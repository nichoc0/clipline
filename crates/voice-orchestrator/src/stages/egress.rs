
use crate::audio_channel_manager;
use flume::{Receiver, Sender};
use bytes::Bytes;
use tracing::{debug, info, warn, error};
use voice_protocols::PipelineEvent;
use audio_utils::CodecConverter;

#[allow(dead_code)]
const HANGOVER_MS: u64 = 700;

pub struct EgressStage {
    audio_buffer: Vec<u8>,
    pending_chunks: Vec<(u64, Vec<u8>, u32)>,
    converter: CodecConverter,
    session_type: String,
    remainder_byte: Option<u8>,
}

impl Default for EgressStage {
    fn default() -> Self {
        Self::new()
    }
}

impl EgressStage {
    pub fn new() -> Self {
        Self {
            audio_buffer: Vec::new(),
            pending_chunks: Vec::new(),
            converter: CodecConverter::new(),
            session_type: "telnyx".to_string(),
            remainder_byte: None,
        }
    }

    pub fn new_with_session_type(session_type: String) -> Self {
        Self {
            audio_buffer: Vec::new(),
            pending_chunks: Vec::new(),
            converter: CodecConverter::new(),
            session_type,
            remainder_byte: None,
        }
    }

    pub async fn run(
        &mut self,
        tts_rx: Receiver<PipelineEvent>,
        call_sid: String,
        shutdown_rx: Receiver<()>,
    ) {
        info!("Egress stage started");

        let audio_output_tx = audio_channel_manager::get_sender(&call_sid)
            .expect("Singleton audio sender should be available");
        info!("Egress using singleton audio TX: {:p}", &audio_output_tx);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv_async() => {
                    debug!("Egress stage shutting down");
                    break;
                }
                Ok(event) = tts_rx.recv_async() => {
                    match event {
                        PipelineEvent::TtsAudio { data, chunk_id, sample_rate } => {
                            info!("Egress received TTS audio chunk {} ({} bytes, {}Hz)", chunk_id, data.len(), sample_rate);
                            self.pending_chunks.push((chunk_id, data, sample_rate));

                            while let Ok(next_event) = tts_rx.try_recv() {
                                match next_event {
                                    PipelineEvent::TtsAudio { data, chunk_id, sample_rate } => {
                                        info!("Egress received TTS audio chunk {} ({} bytes, {}Hz)", chunk_id, data.len(), sample_rate);
                                        self.pending_chunks.push((chunk_id, data, sample_rate));
                                    }
                                    PipelineEvent::TtsComplete => {
                                        info!("TTS synthesis complete, processing and sending all pending chunks");
                                        self.process_audio_chunks(&audio_output_tx).await;
                                        self.flush_audio_buffer(&audio_output_tx).await;
                                        continue;
                                    }
                                    _ => {}
                                }
                            }

                            self.process_audio_chunks(&audio_output_tx).await;
                        }
                        PipelineEvent::TtsComplete => {
                            info!("TTS synthesis complete, flushing any remaining buffered audio");
                            self.process_audio_chunks(&audio_output_tx).await;
                            self.flush_audio_buffer(&audio_output_tx).await;
                        }
                        PipelineEvent::VadDetected { is_speech: _ } => {
                        }
                        PipelineEvent::Interrupt => {
                            info!("Egress: interrupt received, clearing audio buffers immediately");
                            self.pending_chunks.clear();
                            self.audio_buffer.clear();
                            self.remainder_byte = None;
                        }
                        _ => {}
                    }
                }
            }
        }

        if !self.audio_buffer.is_empty() {
            self.flush_audio_buffer(&audio_output_tx).await;
        }

        debug!("Egress stage finished");
    }

    async fn process_audio_chunks(&mut self, _audio_output_tx: &Sender<Bytes>) {
        self.pending_chunks.sort_by_key(|(chunk_id, _, _)| *chunk_id);

        for (_chunk_id, mut data, sample_rate) in self.pending_chunks.drain(..) {
            info!("Received {} bytes of PCM16 @ {}kHz from TTS", data.len(), sample_rate / 1000);

            if let Some(remainder) = self.remainder_byte.take() {
                debug!("Prepending remainder byte 0x{:02X} to chunk of {} bytes", remainder, data.len());
                let mut combined = vec![remainder];
                combined.extend_from_slice(&data);
                data = combined;
            }

            if self.audio_buffer.is_empty() && data.len() >= 16 {
                let first_bytes: Vec<String> = data[..16]
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect();
                info!("Egress first chunk bytes: [{}]", first_bytes.join(" "));

                if data.len() >= 2 {
                    let levels = calculate_audio_levels(&data);
                    info!("Egress audio levels - RMS: {:.4}, Peak: {:.4}, Silent: {}",
                          levels.rms, levels.peak, levels.is_silent);

                    if levels.is_silent {
                        warn!("Egress received silent audio (all zeros)");
                    }
                }
            }

            if data.len() % 2 != 0 {
                debug!("Chunk has odd length {} bytes, storing last byte as remainder", data.len());
                self.remainder_byte = Some(data[data.len() - 1]);
                self.audio_buffer.extend_from_slice(&data[..data.len() - 1]);
            } else {
                self.audio_buffer.extend_from_slice(&data);
            }
        }
    }

    async fn flush_audio_buffer(&mut self, audio_output_tx: &Sender<Bytes>) {
        while !self.audio_buffer.is_empty() {
            let chunk_size = std::cmp::min(640, self.audio_buffer.len());

            let aligned_chunk_size = chunk_size & !1;

            if aligned_chunk_size == 0 {
                if self.audio_buffer.len() == 1 {
                    self.audio_buffer.push(0);
                }
                break;
            }

            let chunk: Vec<u8> = self.audio_buffer.drain(..aligned_chunk_size).collect();
            self.send_to_client(chunk, audio_output_tx).await;
        }
    }

    async fn send_to_client(&mut self, audio_chunk: Vec<u8>, audio_output_tx: &Sender<Bytes>) {
        let final_audio = if self.session_type.starts_with("browser") {
            info!("Sending {} bytes PCM16@16kHz to browser (no conversion needed)", audio_chunk.len());

            if audio_chunk.len() >= 16 {
                let first_bytes: Vec<String> = audio_chunk[..16]
                    .iter()
                    .map(|b| format!("{:02X}", b))
                    .collect();
                info!("Egress sending to browser, first bytes: [{}]", first_bytes.join(" "));

                let has_corruption = audio_chunk.chunks_exact(2).take(20).all(|pair| {
                    pair.len() == 2 && pair[0] == 0xAA && pair[1] == 0xAA
                });
                if has_corruption {
                    error!("WARNING - CORRUPTION: Sending 0xAAAA pattern to browser!");
                }
            }

            audio_chunk
        } else {
            info!("Converting {} bytes PCM16@16kHz to μ-law@8kHz for Telnyx", audio_chunk.len());

            let pcm16_8k = match self.converter.resample_16k_to_8k(&audio_chunk) {
                Ok(downsampled_data) => downsampled_data,
                Err(e) => {
                    error!("Failed to downsample PCM16: {}", e);
                    return;
                }
            };

            let mulaw_8k = match self.converter.pcm16_to_mulaw(&pcm16_8k) {
                Ok(mulaw_data) => mulaw_data,
                Err(e) => {
                    error!("Failed to convert PCM16 to μ-law: {}", e);
                    return;
                }
            };

            info!("Converted to {} bytes μ-law@8kHz for Telnyx", mulaw_8k.len());
            mulaw_8k
        };

        info!("Egress attempting to send {} bytes to orchestrator (channel TX: {:p}, queue len: {})", final_audio.len(), audio_output_tx, audio_output_tx.len());
        if let Err(e) = audio_output_tx.send_async(Bytes::from(final_audio)).await {
            error!("Failed to send audio chunk to orchestrator: {}", e);
        } else {
            info!("Successfully sent audio chunk to orchestrator (new queue len: {})", audio_output_tx.len());
        }
    }
}

#[derive(Debug)]
struct AudioLevels {
    rms: f32,
    peak: f32,
    is_silent: bool,
}

fn calculate_audio_levels(pcm_data: &[u8]) -> AudioLevels {
    if pcm_data.len() < 2 {
        return AudioLevels { rms: 0.0, peak: 0.0, is_silent: true };
    }

    let mut sum_squares = 0.0f64;
    let mut peak = 0i16;
    let mut non_zero_count = 0;

    for chunk in pcm_data.chunks_exact(2) {
        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
        if sample != 0 {
            non_zero_count += 1;
        }
        sum_squares += sample as f64 * sample as f64;
        peak = peak.max(sample.saturating_abs());
    }

    let sample_count = pcm_data.len() / 2;
    let rms = if sample_count > 0 {
        (sum_squares / sample_count as f64).sqrt() / 32767.0
    } else {
        0.0
    };

    AudioLevels {
        rms: rms as f32,
        peak: peak as f32 / 32767.0,
        is_silent: non_zero_count == 0,
    }
}

