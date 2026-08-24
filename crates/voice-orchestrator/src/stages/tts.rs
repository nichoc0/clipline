

use crate::providers::elevenlabs::ElevenLabsProvider;
use crate::channel_utils::NonBlockingSend;
use anyhow::anyhow;
use flume::{Receiver, Sender};
use metrics::gauge;
use tracing::{debug, info, warn};
use voice_protocols::PipelineEvent;

pub struct TtsStage {
    provider: std::sync::Arc<tokio::sync::Mutex<Box<dyn TtsProvider + Send>>>,
    chunk_counter: u64,
}

#[async_trait::async_trait]
pub trait TtsProvider {
    async fn synthesize_text(&mut self, text: &str) -> Result<Vec<u8>, anyhow::Error>;
    async fn synthesize_streaming(&mut self, tokens: &[String]) -> Result<Vec<Vec<u8>>, anyhow::Error>;

    async fn start_synthesis(&mut self, text: &str) -> Result<(), anyhow::Error>;
    async fn next_chunk(&mut self) -> Option<(Vec<u8>, u32)>;
    fn sample_rate(&self) -> u32;
    fn cancel_current_synthesis(&mut self);
}

impl TtsStage {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Self::new_with_voice_id(None).await
    }

    pub async fn new_with_voice_id(voice_id: Option<&str>) -> Result<Self, anyhow::Error> {
        let provider_type = std::env::var("VOICE_TTS_PROVIDER")
            .map_err(|_| anyhow!("VOICE_TTS_PROVIDER environment variable must be set"))?;

        let provider: Box<dyn TtsProvider + Send> = match provider_type.as_str() {
            "elevenlabs" => {
                match voice_id {
                    Some(vid) => Box::new(ElevenLabsProvider::new_with_voice_id(vid)?),
                    None => Box::new(ElevenLabsProvider::new()?),
                }
            }
            _ => {
                return Err(anyhow!("Unsupported TTS provider '{}'. Supported: elevenlabs", provider_type));
            }
        };

        Ok(Self {
            provider: std::sync::Arc::new(tokio::sync::Mutex::new(provider)),
            chunk_counter: 0,
        })
    }

    pub async fn run(
        &mut self,
        llm_rx: Receiver<PipelineEvent>,
        tts_tx: Sender<PipelineEvent>,
        shutdown_rx: Receiver<()>,
    ) {
        info!("TTS stage started");

        let (chunk_tx, chunk_rx) = flume::unbounded::<Option<(Vec<u8>, u32)>>();
        let mut synthesis_task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv_async() => {
                    info!("TTS stage shutting down");
                    if let Some(task) = synthesis_task {
                        task.abort();
                    }
                    break;
                }
                Ok(event) = llm_rx.recv_async() => {
                    match event {
                        PipelineEvent::LlmComplete { response } => {
                            info!("TTS received LLM response: {}", response);

                            if let Some(task) = synthesis_task.take() {
                                debug!("Cancelling previous synthesis task");
                                task.abort();
                            }
                            {
                                let mut provider = self.provider.lock().await;
                                provider.cancel_current_synthesis();
                            }

                            debug!("Starting TTS synthesis");
                            let provider_clone = self.provider.clone();
                            let chunk_tx_clone = chunk_tx.clone();
                            let response_clone = response.clone();

                            synthesis_task = Some(tokio::spawn(async move {
                                let synthesis_result = {
                                    let mut provider = provider_clone.lock().await;
                                    provider.start_synthesis(&response_clone).await
                                };

                                match synthesis_result {
                                    Ok(_) => {
                                        debug!("TTS synthesis started successfully");
                                        loop {
                                            let chunk_result = {
                                                let mut provider = provider_clone.lock().await;
                                                provider.next_chunk().await
                                            };

                                            if let Some(chunk) = chunk_result {
                                                if chunk_tx_clone.send(Some(chunk)).is_err() {
                                                    break;
                                                }
                                            } else {
                                                let _ = chunk_tx_clone.send(None);
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!("TTS synthesis failed: {}", e);
                                    }
                                }
                            }));
                        }
                        PipelineEvent::Interrupt => {
                            debug!("TTS received interrupt signal, cancelling synthesis");
                            if let Some(task) = synthesis_task.take() {
                                task.abort();
                            }
                            {
                                let mut provider = self.provider.lock().await;
                                provider.cancel_current_synthesis();
                            }
                            let _ = tts_tx.try_send_or_drop(PipelineEvent::Interrupt, "tts_to_egress");
                        }
                        PipelineEvent::VadDetected { is_speech: _ } => {
                        }
                        PipelineEvent::LlmTokens { tokens } => {
                            debug!("TTS received LLM tokens: {:?}", tokens);
                            if let Some(task) = synthesis_task.take() {
                                task.abort();
                            }
                            {
                                let mut provider = self.provider.lock().await;
                                provider.cancel_current_synthesis();
                            }
                            match self.provider.lock().await.synthesize_streaming(&tokens).await {
                                Ok(audio_chunks) => {
                                    debug!("TTS streaming synthesis produced {} audio chunks", audio_chunks.len());
                                    let sample_rate = self.provider.lock().await.sample_rate();
                                    for (i, chunk) in audio_chunks.into_iter().enumerate() {
                                        self.chunk_counter += 1;
                                        debug!("TTS processing streaming chunk {} ({} bytes) at {}Hz", i + 1, chunk.len(), sample_rate);

                                        let event = PipelineEvent::TtsAudio {
                                            data: chunk,
                                            chunk_id: self.chunk_counter,
                                            sample_rate,
                                        };
                                        gauge!("pipeline_queue_depth", "stage" => "tts_to_egress").set(tts_tx.len() as f64);

                                        debug!("TTS sending streaming chunk {} to egress", self.chunk_counter);
                                        if tts_tx.try_send_or_drop(event, "tts_to_egress").is_err() {
                                            debug!("TTS output channel closed");
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("TTS streaming synthesis failed: {}", e);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(chunk_opt) = chunk_rx.recv_async() => {
                    match chunk_opt {
                        Some((audio_data, sample_rate)) => {
                            self.chunk_counter += 1;
                            debug!("TTS received audio chunk {} with {} bytes at {}Hz", self.chunk_counter, audio_data.len(), sample_rate);

                            let event = PipelineEvent::TtsAudio {
                                data: audio_data,
                                chunk_id: self.chunk_counter,
                                sample_rate,
                            };
                            gauge!("pipeline_queue_depth", "stage" => "tts_to_egress").set(tts_tx.len() as f64);

                            if tts_tx.try_send_or_drop(event, "tts_to_egress").is_err() {
                                debug!("TTS output channel closed");
                                return;
                            }
                        }
                        None => {
                            debug!("TTS synthesis stream ended");
                            let complete_event = PipelineEvent::TtsComplete;
                            if tts_tx.try_send_or_drop(complete_event, "tts_to_egress").is_err() {
                                debug!("TTS output channel closed");
                                return;
                            }
                        }
                    }
                }
            }
        }

        debug!("TTS stage finished");
    }
}

