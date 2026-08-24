
use flume::{Receiver, Sender};
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::{info, error, warn, debug};
use voice_protocols::{AudioFrame, PipelineEvent, WorkerEvent};

use crate::audio_channel_manager;
use crate::agent_config::{AgentConfig, AgentType};
use crate::stages::{ingress, stt, llm, tts, egress};

pub struct AudioPipeline {
    ingress_handle: JoinHandle<()>,
    vad_forwarder_handle: JoinHandle<()>,
    stt_handle: JoinHandle<()>,
    llm_handle: JoinHandle<()>,
    tts_handle: JoinHandle<()>,
    egress_handle: JoinHandle<()>,
    worker_handle: Option<JoinHandle<()>>,

    pub audio_input_tx: Sender<Bytes>,
    pub audio_output_rx: Option<Receiver<Bytes>>,
    shutdown_tx: Sender<()>,

    _audio_channel: std::sync::Arc<crate::audio_channel_manager::AudioChannelPair>,
}

impl AudioPipeline {
    pub async fn new(call_sid: String) -> Result<Self, anyhow::Error> {
        Self::new_with_session_type(call_sid, "telnyx".to_string()).await
    }

    pub async fn new_with_session_type(call_sid: String, session_type: String) -> Result<Self, anyhow::Error> {
        let (raw_audio_tx, raw_audio_rx) = flume::bounded::<Bytes>(5000);

        let (audio_tx, audio_rx) = flume::bounded::<AudioFrame>(1000);
        let (stt_tx, stt_rx) = flume::bounded::<PipelineEvent>(500);
        let (llm_tx, llm_rx) = flume::bounded::<PipelineEvent>(50);
        let (tts_tx, tts_rx) = flume::bounded::<PipelineEvent>(500);
        let (_egress_tx, _egress_rx) = flume::bounded::<PipelineEvent>(200);
        let (vad_tx, vad_rx) = flume::bounded::<PipelineEvent>(50);

        let audio_channel = audio_channel_manager::create_audio_channel(&call_sid);
        info!("Pipeline created singleton audio channel for {} (keeping Arc alive: {:p})", call_sid, &audio_channel);

        let (shutdown_tx, shutdown_rx) = flume::bounded::<()>(1);

        let (worker_tx, _worker_rx) = flume::bounded::<WorkerEvent>(100);
        let worker_handle: Option<JoinHandle<()>> = None;

        let shutdown_rx_clone = shutdown_rx.clone();
        let session_type_clone = session_type.clone();
        let ingress_handle = tokio::spawn(async move {
            info!("Spawned Ingress task, creating stage...");
            let mut ingress_stage = ingress::IngressStage::new(session_type_clone);
            info!("Ingress stage created, starting run loop");
            ingress_stage.run(raw_audio_rx, audio_tx, vad_tx, shutdown_rx_clone).await;
            info!("Ingress stage run loop ended");
        });

        let shutdown_rx_clone = shutdown_rx.clone();
        let stt_tx_for_vad = stt_tx.clone();
        let session_type_for_vad = session_type.clone();
        let vad_forwarder_handle = tokio::spawn(async move {
            if session_type_for_vad.starts_with("browser") {
                info!("VAD forwarder disabled for browser session (using client-side interrupts)");
                let _ = shutdown_rx_clone.recv_async().await;
                info!("VAD forwarder shutting down");
                return;
            }

            info!("VAD forwarder started for {} session", session_type_for_vad);
            let start_time = std::time::Instant::now();
            const WARMUP_PERIOD_MS: u64 = 800;

            loop {
                tokio::select! {
                    _ = shutdown_rx_clone.recv_async() => {
                        info!("VAD forwarder shutting down");
                        break;
                    }
                    Ok(vad_event) = vad_rx.recv_async() => {
                        let elapsed_ms = start_time.elapsed().as_millis() as u64;

                        if elapsed_ms < WARMUP_PERIOD_MS {
                            debug!("VAD forwarder skipping event during warmup ({} ms elapsed)", elapsed_ms);
                            continue;
                        }

                        if let Err(e) = stt_tx_for_vad.try_send(vad_event) {
                            warn!("Failed to forward VAD event to STT channel: {}", e);
                        }
                    }
                }
            }
            info!("VAD forwarder finished");
        });

        let shutdown_rx_clone = shutdown_rx.clone();
        let session_type_clone2 = session_type.clone();
        let worker_tx_for_stt = worker_tx.clone();
        let call_sid_for_stt = call_sid.clone();
        let stt_language: Option<String> = None;
        let stt_handle = tokio::spawn(async move {
            info!(
                "Creating STT stage... (per-call language={})",
                stt_language.as_deref().unwrap_or("<env/multi>")
            );
            match stt::SttStage::new(session_type_clone2, Some(worker_tx_for_stt), call_sid_for_stt, stt_language).await {
                Ok(mut stt_stage) => {
                    info!("STT stage created successfully, starting run loop");
                    stt_stage.run(audio_rx, stt_tx, shutdown_rx_clone).await;
                    info!("STT stage run loop ended");
                }
                Err(e) => {
                    error!("Failed to create STT stage: {}", e);
                    info!("Starting dummy STT consumer to prevent audio backup");
                    loop {
                        tokio::select! {
                            _ = shutdown_rx_clone.recv_async() => break,
                            Ok(_) = audio_rx.recv_async() => {
                            }
                        }
                    }
                }
            }
        });

        let shutdown_rx_clone = shutdown_rx.clone();
        let shutdown_tx_for_llm = shutdown_tx.clone();
        let session_type_for_llm = session_type.clone();
        let worker_tx_for_llm = worker_tx.clone();
        let call_sid_for_llm = call_sid.clone();
        let llm_handle = tokio::spawn(async move {
            info!("Creating LLM stage for session_type: {}", session_type_for_llm);
            let caller_phone: Option<String> = None;
            let stage_result = llm::LlmStage::new_with_session_type(
                &session_type_for_llm,
                Some(worker_tx_for_llm),
                call_sid_for_llm,
                caller_phone,
            )
            .await;
            match stage_result {
                Ok(mut llm_stage) => {
                    info!("LLM stage created successfully, starting run loop");
                    llm_stage.run(stt_rx, llm_tx, shutdown_rx_clone).await;
                    info!("LLM stage run loop ended");
                    let _ = shutdown_tx_for_llm.try_send(());
                }
                Err(e) => {
                    error!("Failed to create LLM stage: {}", e);
                    info!("Starting dummy LLM consumer to prevent STT backup");
                    loop {
                        tokio::select! {
                            _ = shutdown_rx_clone.recv_async() => break,
                            Ok(_) = stt_rx.recv_async() => {
                            }
                        }
                    }
                }
            }
        });

        let agent_config = AgentConfig::for_agent(AgentType::Assistant);

        let voice_id = std::env::var("VOICE_ELEVENLABS_VOICE_ID")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| crate::providers::elevenlabs::TARGET_VOICE_ID.to_string());
        info!(
            "Pipeline using agent '{}' with voice_id: {}",
            agent_config.display_name,
            voice_id
        );

        let shutdown_rx_clone = shutdown_rx.clone();
        let tts_handle = tokio::spawn(async move {
            info!("Creating TTS stage with voice_id: {}", voice_id);
            match tts::TtsStage::new_with_voice_id(Some(&voice_id)).await {
                Ok(mut tts_stage) => {
                    info!("TTS stage created successfully, starting run loop");
                    tts_stage.run(llm_rx, tts_tx, shutdown_rx_clone).await;
                    info!("TTS stage run loop ended");
                }
                Err(e) => {
                    error!("Failed to create TTS stage: {}", e);
                    info!("Starting dummy TTS consumer to prevent LLM backup");
                    loop {
                        tokio::select! {
                            _ = shutdown_rx_clone.recv_async() => break,
                            Ok(_) = llm_rx.recv_async() => {
                            }
                        }
                    }
                }
            }
        });

        let shutdown_rx_clone = shutdown_rx.clone();
        let call_sid_for_egress = call_sid.clone();
        let session_type_for_egress = session_type.clone();
        info!("Pipeline spawning egress task for call: {} (session_type: {})", call_sid_for_egress, session_type_for_egress);
        let egress_handle = tokio::spawn(async move {
            info!("Creating Egress stage...");
            let mut egress_stage = egress::EgressStage::new_with_session_type(session_type_for_egress);
            info!("Egress stage created successfully, starting run loop for call: {}", call_sid_for_egress);
            egress_stage.run(tts_rx, call_sid_for_egress, shutdown_rx_clone).await;
            info!("Egress stage run loop ended");
        });

        info!("Audio pipeline initialized for call: {}", call_sid);

        Ok(Self {
            ingress_handle,
            vad_forwarder_handle,
            stt_handle,
            llm_handle,
            tts_handle,
            egress_handle,
            worker_handle,
            audio_input_tx: raw_audio_tx,
            audio_output_rx: None,
            shutdown_tx,
            _audio_channel: audio_channel,
        })
    }

    pub fn take_audio_output_rx(&mut self) -> Option<Receiver<Bytes>> {
        let rx = self.audio_output_rx.take();
        if let Some(ref receiver) = rx {
            info!("Pipeline handing over RX: {:p}", receiver);
        }
        rx
    }

    pub async fn shutdown(self) {
        info!("Shutting down audio pipeline");

        let _ = self.shutdown_tx.send(());

        if let Some(handle) = self.worker_handle {
            handle.abort();
            info!("Transcript analyzer worker aborted");
        }

        if let Err(e) = tokio::try_join!(
            self.ingress_handle,
            self.vad_forwarder_handle,
            self.stt_handle,
            self.llm_handle,
            self.tts_handle,
            self.egress_handle
        ) {
            error!("Error during pipeline shutdown: {}", e);
        }

        info!("Audio pipeline shutdown complete");
    }
}