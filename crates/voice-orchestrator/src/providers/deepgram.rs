

use crate::stages::stt::SttProvider;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use deepgram::{
    Deepgram,
    DeepgramError,
    common::options::{Encoding, Endpointing, Language, Model, Options},
    common::stream_response::StreamResponse,
};
use flume::Receiver as FlumeReceiver;
use futures::channel::mpsc::{self, Sender as FuturesSender};
use futures::stream::StreamExt;
use futures::SinkExt;
use metrics::{histogram, counter};
use std::collections::VecDeque;
use std::time::Instant;
use tracing::{debug, info, warn, error};
use voice_protocols::{AudioFrame, PipelineEvent};

pub struct DeepgramProvider {
    api_key: String,
    audio_tx: Option<FuturesSender<Result<Bytes, DeepgramError>>>,
    transcript_rx: Option<FlumeReceiver<Result<StreamResponse, DeepgramError>>>,
    transcript_buffer: VecDeque<PipelineEvent>,
    last_transcript: Instant,
    session_type: String,
    language: Option<String>,
}

impl DeepgramProvider {
    pub fn new(session_type: String, language: Option<String>) -> Result<Self> {
        let api_key = std::env::var("VOICE_DEEPGRAM_API_KEY")
            .map_err(|_| anyhow!("VOICE_DEEPGRAM_API_KEY environment variable not set"))?;

        info!("Creating Deepgram provider for session_type: {}", session_type);

        Ok(Self {
            api_key,
            audio_tx: None,
            transcript_rx: None,
            language,
            transcript_buffer: VecDeque::new(),
            last_transcript: Instant::now(),
            session_type,
        })
    }

    async fn process_transcript_message(&mut self, response: StreamResponse) -> Option<PipelineEvent> {
        match response {
            StreamResponse::TranscriptResponse {
                is_final,
                channel,
                ..
            } => {
                if let Some(alternative) = channel.alternatives.first() {
                    let transcript = &alternative.transcript;
                    let confidence = alternative.confidence as f32;

                    debug!("Deepgram TranscriptResponse: transcript='{}', confidence={:.2}, is_final={}, empty={}",
                        transcript, confidence, is_final, transcript.trim().is_empty());

                    if !transcript.trim().is_empty() {
                        let event = if is_final {
                            PipelineEvent::SttFinal {
                                text: transcript.clone(),
                                confidence,
                            }
                        } else {
                            PipelineEvent::SttPartial {
                                text: transcript.clone(),
                                confidence,
                            }
                        };

                        info!("Deepgram transcript: '{}' (confidence: {:.2}, final: {})",
                            transcript, confidence, is_final);

                        counter!("stt_transcripts_total", "provider" => "deepgram", "type" => if is_final { "final" } else { "partial" }).increment(1);
                        histogram!("stt_confidence", "provider" => "deepgram").record(confidence as f64);

                        self.last_transcript = Instant::now();

                        return Some(event);
                    }
                } else {
                    debug!("Deepgram TranscriptResponse with no alternatives");
                }
            }
            StreamResponse::SpeechStartedResponse { .. } => {
                debug!("Deepgram: Speech started");
            }
            StreamResponse::UtteranceEndResponse { .. } => {
                debug!("Deepgram: Utterance ended");
            }
            StreamResponse::TerminalResponse { .. } => {
                info!("Deepgram: Terminal response received");
            }
            _ => {
                debug!("Deepgram: Unknown response type");
            }
        }

        None
    }
}

#[async_trait]
impl SttProvider for DeepgramProvider {
    async fn start_stream(&mut self) -> Result<()> {
        info!("Starting Deepgram STT stream with official SDK...");

        let deepgram = Deepgram::new(&self.api_key)
            .map_err(|e| anyhow!("Failed to create Deepgram client: {}", e))?;

        let (audio_tx, audio_rx) = mpsc::channel::<Result<Bytes, DeepgramError>>(128);

        let sample_rate = if self.session_type.starts_with("browser") { 16000 } else { 8000 };
        info!("Deepgram configured for {} session with sample rate: {}Hz", self.session_type, sample_rate);

        let env_override = std::env::var("VOICE_DEEPGRAM_LANGUAGE").ok();
        let resolved_code = self.language.clone()
            .filter(|s| !s.is_empty())
            .or_else(|| env_override.filter(|s| !s.is_empty()));
        let language_used = match resolved_code.as_deref() {
            Some(code) => {
                info!("Deepgram model=Nova3 language={} (per-call / env override)", code);
                Language::Other(code.to_string())
            }
            None => {
                info!("Deepgram model=Nova3 language=multi (streaming code-switching enabled)");
                Language::multi
            }
        };
        let options = Options::builder()
            .language(language_used)
            .model(Model::Nova3)
            .punctuate(true)
            .smart_format(true)
            .build();

        match deepgram
            .transcription()
            .stream_request_with_options(options)
            .keep_alive()
            .encoding(Encoding::Linear16)
            .sample_rate(sample_rate)
            .channels(1)
            .interim_results(true)
            .endpointing(Endpointing::CustomDurationMs(
                std::env::var("VOICE_DEEPGRAM_ENDPOINTING_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(800),
            ))
            .vad_events(true)
            .stream(audio_rx)
            .await
        {
            Ok(mut results) => {
                info!("Deepgram Request ID: {}", results.request_id());

                let (transcript_tx, transcript_rx) = flume::unbounded();

                tokio::spawn(async move {
                    while let Some(result) = results.next().await {
                        if transcript_tx.send(result).is_err() {
                            break;
                        }
                    }
                    debug!("Deepgram transcript reader task ended");
                });

                self.audio_tx = Some(audio_tx);
                self.transcript_rx = Some(transcript_rx);
                self.last_transcript = Instant::now();
                info!("Deepgram STT stream started successfully with official SDK");
                Ok(())
            }
            Err(e) => {
                error!("Failed to start Deepgram STT stream: {}", e);
                Err(anyhow!("Deepgram stream start failed: {}", e))
            }
        }
    }

    async fn send_audio(&mut self, frame: &AudioFrame) -> Result<()> {
        if let Some(ref mut tx) = self.audio_tx {
            match tx.send(Ok(frame.data.clone())).await {
                Ok(_) => {
                    debug!("Sent {} bytes ({} samples) to Deepgram via SDK",
                        frame.data.len(), frame.data.len() / 2);
                }
                Err(e) => {
                    warn!("Failed to send audio to Deepgram SDK: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn receive_transcript(&mut self) -> Option<PipelineEvent> {
        if let Some(event) = self.transcript_buffer.pop_front() {
            return Some(event);
        }

        if let Some(ref rx) = self.transcript_rx {
            match rx.try_recv() {
                Ok(Ok(response)) => {
                    return self.process_transcript_message(response).await;
                }
                Ok(Err(e)) => {
                    warn!("Deepgram transcript error: {}", e);
                }
                Err(flume::TryRecvError::Disconnected) => {
                    warn!("Deepgram transcript stream ended");
                    self.transcript_rx = None;
                }
                Err(flume::TryRecvError::Empty) => {
                }
            }
        }

        None
    }

    async fn stop_stream(&mut self) -> Result<()> {
        if let Some(mut tx) = self.audio_tx.take() {
            let _ = tx.close().await;
        }
        self.transcript_rx = None;
        debug!("Deepgram STT stream stopped");
        Ok(())
    }
}
