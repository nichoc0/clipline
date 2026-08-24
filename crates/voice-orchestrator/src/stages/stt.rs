

use crate::channel_utils::NonBlockingSend;
use crate::providers::deepgram::DeepgramProvider;
use anyhow::anyhow;
use flume::{Receiver, Sender};
use metrics::gauge;
use std::time::Duration;
use tracing::{debug, info, warn, error};
use voice_protocols::{AudioFrame, PipelineEvent, WorkerEvent};

pub struct SttStage {
    provider: Box<dyn SttProvider + Send + Sync>,
    worker_tx: Option<Sender<WorkerEvent>>,
    session_id: String,
}

#[async_trait::async_trait]
pub trait SttProvider {
    async fn start_stream(&mut self) -> Result<(), anyhow::Error>;
    async fn send_audio(&mut self, frame: &AudioFrame) -> Result<(), anyhow::Error>;
    async fn receive_transcript(&mut self) -> Option<PipelineEvent>;
    async fn stop_stream(&mut self) -> Result<(), anyhow::Error>;
}

impl SttStage {
    pub async fn new(
        session_type: String,
        worker_tx: Option<Sender<WorkerEvent>>,
        session_id: String,
        language: Option<String>,
    ) -> Result<Self, anyhow::Error> {
        let provider_type = std::env::var("VOICE_STT_PROVIDER")
            .map_err(|_| anyhow!("VOICE_STT_PROVIDER environment variable must be set"))?;

        let provider: Box<dyn SttProvider + Send + Sync> = match provider_type.as_str() {
            "deepgram" => {
                Box::new(DeepgramProvider::new(session_type, language)?)
            }
            _ => {
                return Err(anyhow!("Unsupported STT provider '{}'. Supported: deepgram", provider_type));
            }
        };

        Ok(Self {
            provider,
            worker_tx,
            session_id,
        })
    }

    pub async fn run(
        &mut self,
        audio_rx: Receiver<AudioFrame>,
        stt_tx: Sender<PipelineEvent>,
        shutdown_rx: Receiver<()>,
    ) {
        info!("STT stage started, waiting for first audio frame");

        let mut started = false;
        let mut audio_frame_count = 0;
        let mut poll_interval = tokio::time::interval(Duration::from_millis(20));
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv_async() => {
                    debug!("STT stage shutting down");
                    if started {
                        if let Err(e) = self.provider.stop_stream().await {
                            warn!("Failed to stop STT stream: {}", e);
                        }
                    }
                    info!("STT stage finished (processed {} audio frames)", audio_frame_count);
                    return;
                }
                Ok(frame) = audio_rx.recv_async() => {
                    audio_frame_count += 1;

                    if audio_frame_count == 1 {
                        info!("STT stage received first audio frame ({} bytes, {} samples @ {}Hz)",
                            frame.data.len(), frame.data.len() / 2, frame.sample_rate);
                    }

                    if !started {
                        info!("Starting STT provider on first audio frame...");
                        match self.provider.start_stream().await {
                            Ok(_) => {
                                info!("STT stream started successfully, now forwarding audio");
                                started = true;
                            }
                            Err(e) => {
                                error!("Failed to start STT stream: {}, will retry on next audio frame", e);
                                continue;
                            }
                        }
                    }

                    if started {
                        if let Err(e) = self.provider.send_audio(&frame).await {
                            error!("Failed to send audio to STT: {}", e);
                            continue;
                        }

                        if audio_frame_count % 250 == 0 {
                            info!("STT stage processed {} audio frames", audio_frame_count);
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    if started {
                        let _ = self.process_transcripts(&stt_tx).await;
                    }
                }
            }
        }
    }

    async fn process_transcripts(&mut self, stt_tx: &Sender<PipelineEvent>) -> Result<(), ()> {
        if let Some(event) = self.provider.receive_transcript().await {
            match &event {
                PipelineEvent::SttPartial { text, confidence } => {
                    debug!("STT sending SttPartial: '{}' (confidence: {:.2})", text, confidence);
                }
                PipelineEvent::SttFinal { text, confidence } => {
                    info!("STT sending SttFinal: '{}' (confidence: {:.2})", text, confidence);

                    if let Some(ref worker_tx) = self.worker_tx {
                        let worker_event = WorkerEvent::TranscriptUpdate {
                            session_id: self.session_id.clone(),
                            text: text.clone(),
                            is_final: true,
                        };

                        if let Err(e) = worker_tx.try_send(worker_event) {
                            debug!("Failed to send transcript to worker: {}", e);
                        }
                    }
                }
                PipelineEvent::VadDetected { is_speech } => {
                    info!("STT forwarding VAD event: is_speech={}", is_speech);
                }
                _ => {}
            }

            gauge!("pipeline_queue_depth", "stage" => "stt_to_llm").set(stt_tx.len() as f64);

            stt_tx
                .try_send_or_drop(event, "stt_to_llm")
                .map_err(|_| ())?;
        }

        Ok(())
    }
}

