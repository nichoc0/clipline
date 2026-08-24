

use audio_utils::{CodecConverter, VoiceActivityDetector};
use bytes::Bytes;
use flume::{Receiver, Sender};
use metrics::{gauge, histogram};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use voice_protocols::{AudioFrame, AudioEncoding, PipelineEvent};

pub struct IngressStage {
    vad: VoiceActivityDetector,
    converter: CodecConverter,
    frame_counter: u64,
    last_activity: Option<Instant>,
    last_packet_time: Option<Instant>,
    session_type: String,
    is_speech_active: bool,
}

impl Default for IngressStage {
    fn default() -> Self {
        Self::new("telnyx".to_string())
    }
}

impl IngressStage {
    pub fn new(session_type: String) -> Self {
        info!("Creating ingress stage for session_type: {}", session_type);
        Self {
            vad: VoiceActivityDetector::new(0.20),
            converter: CodecConverter::new(),
            frame_counter: 0,
            last_activity: None,
            last_packet_time: None,
            session_type,
            is_speech_active: false,
        }
    }

    pub async fn run(
        &mut self,
        raw_audio_rx: Receiver<Bytes>,
        audio_tx: Sender<AudioFrame>,
        vad_tx: Sender<PipelineEvent>,
        shutdown_rx: Receiver<()>,
    ) {
        let source = if self.session_type.starts_with("browser") { "browser" } else { "Telnyx" };
        let format = if self.session_type.starts_with("browser") { "PCM16@16kHz" } else { "μ-law@8kHz" };
        info!("Ingress stage started for {} session, waiting for {} audio", self.session_type, format);
        let mut raw_frame_count = 0;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv_async() => {
                    debug!("Ingress stage shutting down");
                    break;
                }
                audio_result = raw_audio_rx.recv_async() => {
                    match audio_result {
                        Ok(audio_data) => {
                            raw_frame_count += 1;

                            if raw_frame_count == 1 {
                                info!("Ingress received first raw audio frame from {} ({} bytes {})", source, audio_data.len(), format);
                            }

                            let packet_time = Instant::now();

                            if let Some(last_time) = self.last_packet_time {
                                let inter_arrival = packet_time.duration_since(last_time);
                                let expected_interval = Duration::from_millis(20);
                                let jitter = inter_arrival.abs_diff(expected_interval);

                                histogram!("telnyx_packet_jitter_ms").record(jitter.as_millis() as f64);
                            }
                            self.last_packet_time = Some(packet_time);

                            if let Some(frame) = self.process_audio(audio_data, &vad_tx).await {
                                if raw_frame_count % 250 == 0 {
                                    debug!("Ingress processed {} raw audio frames", raw_frame_count);
                                }
                                gauge!("pipeline_queue_depth", "stage" => "ingress_to_stt").set(audio_tx.len() as f64);

                                if let Err(e) = audio_tx.try_send(frame) {
                                    warn!("STT queue full or disconnected, dropping frame: {}", e);
                                }
                            }
                        }
                        Err(_) => {
                            debug!("Raw audio channel closed");
                            break;
                        }
                    }
                }
            }
        }

        info!("Ingress stage finished (processed {} raw audio frames)", raw_frame_count);
    }

    pub async fn process_audio(&mut self, audio_data: Bytes, vad_tx: &Sender<PipelineEvent>) -> Option<AudioFrame> {
        if audio_data.is_empty() {
            return None;
        }

        let (pcm_data, sample_rate) = if self.session_type.starts_with("browser") {
            debug!("Ingress processing browser audio: {} bytes PCM16@16kHz", audio_data.len());
            (audio_data.to_vec(), 16000)
        } else {
            match self.converter.mulaw_to_pcm16(&audio_data) {
                Ok(data) => (data, 8000),
                Err(e) => {
                    warn!("Failed to convert mu-law to PCM: {}", e);
                    return None;
                }
            }
        };

        let is_speech = self.vad.detect_voice_activity(&pcm_data);

        if is_speech && !self.is_speech_active {
            info!("VAD speech START detected — emitting VadDetected{{is_speech:true}}");
            self.is_speech_active = true;
            let _ = vad_tx.try_send(PipelineEvent::VadDetected { is_speech: true });
        } else if !is_speech && self.is_speech_active {
            info!("VAD speech END detected");
            self.is_speech_active = false;
            let _ = vad_tx.try_send(PipelineEvent::VadDetected { is_speech: false });
        }

        if is_speech {
            self.last_activity = Some(Instant::now());
        }

        let frame = AudioFrame {
            data: Bytes::from(pcm_data),
            sample_rate,
            channels: 1,
            encoding: AudioEncoding::PCM16,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            sequence: {
                self.frame_counter += 1;
                self.frame_counter
            },
        };

        Some(frame)
    }

    pub fn has_recent_activity(&self, threshold: Duration) -> bool {
        self.last_activity
            .map(|last| last.elapsed() < threshold)
            .unwrap_or(false)
    }
}
