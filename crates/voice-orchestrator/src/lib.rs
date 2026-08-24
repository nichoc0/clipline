pub mod agent_config;
pub mod audio_channel_manager;
pub mod barge_in_bus;
pub mod channel_utils;
pub mod pipeline;
pub mod providers;
pub mod stages;
pub mod transcript_bus;

use crate::channel_utils::NonBlockingSend;
use bytes::Bytes;
use metrics::{gauge, histogram};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use voice_protocols::MediaFormat;

pub struct VoiceOrchestrator {
    call_sid: String,
    session_type: String,
    pipeline: Option<pipeline::AudioPipeline>,
    start_time: Option<Instant>,
}

impl VoiceOrchestrator {
    pub fn new(call_sid: String) -> Self {
        Self::new_with_session_type(call_sid, "telnyx".to_string())
    }

    pub fn new_with_session_type(call_sid: String, session_type: String) -> Self {
        Self {
            call_sid,
            session_type,
            pipeline: None,
            start_time: None,
        }
    }

    pub fn session_type(&self) -> &str {
        &self.session_type
    }

    pub fn validate_production_environment() -> Result<(), anyhow::Error> {
        let required_vars = [
            ("VOICE_STT_PROVIDER", "STT provider must be set"),
            ("VOICE_LLM_PROVIDER", "LLM provider must be set"),
            ("VOICE_TTS_PROVIDER", "TTS provider must be set"),
        ];
        for (var, description) in &required_vars {
            if std::env::var(var).is_err() {
                return Err(anyhow::anyhow!("{}: {}", description, var));
            }
        }

        let stt_provider = std::env::var("VOICE_STT_PROVIDER")?;
        match stt_provider.as_str() {
            "deepgram" => {
                std::env::var("VOICE_DEEPGRAM_API_KEY")
                    .map_err(|_| anyhow::anyhow!("VOICE_DEEPGRAM_API_KEY required for Deepgram STT"))?;
            }
            _ => return Err(anyhow::anyhow!("Unsupported STT provider: {}", stt_provider)),
        }

        let llm_provider = std::env::var("VOICE_LLM_PROVIDER")?;
        match llm_provider.as_str() {
            "repl" => {}
            "groq" => {
                std::env::var("VOICE_GROQ_API_KEY")
                    .map_err(|_| anyhow::anyhow!("VOICE_GROQ_API_KEY required for Groq LLM"))?;
            }
            "openai" => {
                std::env::var("VOICE_OPENAI_API_KEY")
                    .map_err(|_| anyhow::anyhow!("VOICE_OPENAI_API_KEY required for OpenAI LLM"))?;
            }
            _ => return Err(anyhow::anyhow!("Unsupported LLM provider: {}", llm_provider)),
        }

        let tts_provider = std::env::var("VOICE_TTS_PROVIDER")?;
        match tts_provider.as_str() {
            "elevenlabs" => {
                std::env::var("VOICE_ELEVENLABS_API_KEY")
                    .map_err(|_| anyhow::anyhow!("VOICE_ELEVENLABS_API_KEY required for ElevenLabs TTS"))?;
            }
            _ => return Err(anyhow::anyhow!("Unsupported TTS provider: {}", tts_provider)),
        }

        Ok(())
    }

    pub async fn start(&mut self, _media_format: MediaFormat) -> Result<(), anyhow::Error> {
        info!("Starting voice orchestrator for call: {}", self.call_sid);
        Self::validate_production_environment().inspect_err(|e| {
            error!("Environment validation failed: {}", e);
        })?;
        let pipeline = pipeline::AudioPipeline::new(self.call_sid.clone()).await?;
        self.pipeline = Some(pipeline);
        info!("Audio pipeline created for call: {}", self.call_sid);
        Ok(())
    }

    pub async fn stop(&mut self) {
        info!("Stopping voice orchestrator for call: {}", self.call_sid);
        if let Some(pipeline) = self.pipeline.take() {
            pipeline.shutdown().await;
        }
        audio_channel_manager::remove_audio_channel(&self.call_sid);
    }

    pub async fn run_with_audio_channels(
        &mut self,
        mut audio_in_rx: mpsc::Receiver<Bytes>,
        audio_out_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), anyhow::Error> {
        info!("Voice orchestrator running for call: {}", self.call_sid);

        let pipeline = self
            .pipeline
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Pipeline not initialized - call start() first"))?;

        let audio_output_rx = audio_channel_manager::get_receiver(&self.call_sid)
            .expect("Singleton audio receiver should be available");

        self.start_time = Some(Instant::now());

        let audio_input_tx = pipeline.audio_input_tx.clone();
        let audio_input_tx_metrics = pipeline.audio_input_tx.clone();
        let input_task = tokio::spawn(async move {
            let mut frame_count = 0u64;
            while let Some(audio_data) = audio_in_rx.recv().await {
                if !audio_data.is_empty() {
                    frame_count += 1;
                    if let Err(e) = audio_input_tx.try_send_or_drop(audio_data, "audio_input") {
                        error!("Failed to send audio to pipeline: {}", e);
                        break;
                    }
                }
            }
            info!("Orchestrator input task ended after {} frames", frame_count);
        });

        let metrics_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                gauge!("pipeline_queue_depth", "stage" => "raw_audio")
                    .set(audio_input_tx_metrics.len() as f64);
            }
        });

        let start_time = self.start_time;
        let output_task = tokio::spawn(async move {
            let mut chunk_counter = 0u64;
            let mut first_audio_sent = false;
            loop {
                match audio_output_rx.recv_async().await {
                    Ok(audio_data) => {
                        chunk_counter += 1;
                        if !first_audio_sent {
                            if let Some(start) = start_time {
                                histogram!("end_to_end_first_audio_ms")
                                    .record(start.elapsed().as_millis() as f64);
                            }
                            first_audio_sent = true;
                        }
                        if let Err(e) = audio_out_tx.send(audio_data).await {
                            warn!("Outbound audio channel closed: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Orchestrator audio receive error: {}", e);
                        break;
                    }
                }
            }
            info!("Orchestrator output task ended after {} chunks", chunk_counter);
        });

        tokio::select! {
            _ = input_task => {},
            _ = output_task => {},
            _ = metrics_task => {},
        };

        info!("Voice orchestrator finished for call: {}", self.call_sid);
        Ok(())
    }
}
