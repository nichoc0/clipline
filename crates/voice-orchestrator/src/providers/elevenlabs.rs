

use crate::stages::tts::TtsProvider;
use anyhow::{anyhow, Result};
use futures::stream::StreamExt;
use metrics::{histogram, counter};
use reqwest::Client;
use serde_json::json;
use std::time::Instant;
use tracing::{debug, warn, error, info};

pub struct ElevenLabsProvider {
    client: Client,
    api_key: String,
    voice_id: String,
    model_id: String,
    base_url: String,
    current_stream: Option<std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + Sync + 'static>>>,
    stream_started: bool,
    cancelled: bool,
    synthesis_start_time: Option<Instant>,
}

pub const TARGET_VOICE_ID: &str = "OYTbf65OHHFELVut7v2H";

impl ElevenLabsProvider {
    pub fn new() -> Result<Self> {
        let voice_id = std::env::var("VOICE_ELEVENLABS_VOICE_ID")
            .unwrap_or_else(|_| TARGET_VOICE_ID.to_string());
        Self::new_with_voice_id(&voice_id)
    }

    pub fn new_target() -> Result<Self> {
        Self::new_with_voice_id(TARGET_VOICE_ID)
    }

    pub fn new_with_voice_id(voice_id: &str) -> Result<Self> {
        let api_key = std::env::var("VOICE_ELEVENLABS_API_KEY")
            .map_err(|_| anyhow!("VOICE_ELEVENLABS_API_KEY environment variable not set"))?;

        let model_id = std::env::var("VOICE_ELEVENLABS_MODEL_ID")
            .unwrap_or_else(|_| "eleven_turbo_v2_5".to_string());

        let base_url = std::env::var("VOICE_ELEVENLABS_BASE_URL")
            .unwrap_or_else(|_| "https://api.elevenlabs.io".to_string());

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        info!("ElevenLabsProvider initialized with voice_id: {}", voice_id);

        Ok(Self {
            client,
            api_key,
            voice_id: voice_id.to_string(),
            model_id,
            base_url,
            current_stream: None,
            stream_started: false,
            cancelled: false,
            synthesis_start_time: None,
        })
    }

    async fn stream_generate(&self, text: &str) -> Result<Vec<Vec<u8>>> {
        let start_time = Instant::now();

        let request_body = json!({
            "text": text,
            "model_id": self.model_id,
            "optimize_streaming_latency": 0,
            "voice_settings": {
                "stability": 0.55,
                "similarity_boost": 0.75,
                "style": 0,
                "use_speaker_boost": true,
                "speed": 0.85
            }
        });

        let url = format!("{}/v1/text-to-speech/{}/stream?output_format=pcm_16000",
                          self.base_url, self.voice_id);

        let response = self.client
            .post(&url)
            .header("xi-api-key", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("ElevenLabs API error {}: {}", status, error_text));
        }

        let mut chunks = Vec::new();
        let mut stream = response.bytes_stream();
        let mut first_chunk = true;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    if first_chunk {
                        let ttfb = start_time.elapsed();
                        debug!("ElevenLabs TTFB: {}ms", ttfb.as_millis());
                        histogram!("tts_ttfb_ms", "provider" => "elevenlabs").record(ttfb.as_millis() as f64);
                        first_chunk = false;
                    }

                    if !chunk.is_empty() {
                        chunks.push(chunk.to_vec());
                    }
                }
                Err(e) => {
                    warn!("Error reading chunk from ElevenLabs stream: {}", e);
                    break;
                }
            }
        }

        let total_time = start_time.elapsed();
        debug!("ElevenLabs generated {} chunks in {}ms", chunks.len(), total_time.as_millis());
        histogram!("tts_generation_time_ms", "provider" => "elevenlabs").record(total_time.as_millis() as f64);
        counter!("tts_requests_total", "provider" => "elevenlabs").increment(1);
        Ok(chunks)
    }
}

#[async_trait::async_trait]
impl TtsProvider for ElevenLabsProvider {
    async fn synthesize_text(&mut self, text: &str) -> Result<Vec<u8>> {
        let chunks = self.stream_generate(text).await?;

        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        let mut combined = Vec::with_capacity(total_size);

        for chunk in chunks {
            combined.extend(chunk);
        }

        Ok(combined)
    }

    async fn synthesize_streaming(&mut self, tokens: &[String]) -> Result<Vec<Vec<u8>>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let combined_text = tokens.join(" ");
        self.stream_generate(&combined_text).await
    }

    async fn start_synthesis(&mut self, text: &str) -> Result<()> {
        self.cancelled = false;
        self.current_stream = None;
        let request_body = json!({
            "text": text,
            "model_id": self.model_id,
            "optimize_streaming_latency": 0,
            "voice_settings": {
                "stability": 0.55,
                "similarity_boost": 0.75,
                "style": 0,
                "use_speaker_boost": true,
                "speed": 0.85
            }
        });

        let url = format!("{}/v1/text-to-speech/{}/stream?output_format=pcm_16000",
                          self.base_url, self.voice_id);

        let response = self.client
            .post(&url)
            .header("xi-api-key", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("ElevenLabs API error {}: {}", status, error_text));
        }

        self.current_stream = Some(Box::pin(response.bytes_stream()));
        self.stream_started = false;
        self.synthesis_start_time = Some(Instant::now());

        debug!("ElevenLabs streaming synthesis started for text: {}", text);
        tracing::info!("ElevenLabs API request successful, stream initialized");
        Ok(())
    }

    async fn next_chunk(&mut self) -> Option<(Vec<u8>, u32)> {
        if self.cancelled {
            self.current_stream = None;
            return None;
        }

        if let Some(ref mut stream) = self.current_stream {
            let timeout_duration = std::time::Duration::from_secs(30);
            match tokio::time::timeout(timeout_duration, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if !self.stream_started {
                        debug!("ElevenLabs first chunk received");
                        if let Some(start_time) = self.synthesis_start_time {
                            let ttfb = start_time.elapsed();
                            histogram!("tts_ttfb_ms", "provider" => "elevenlabs").record(ttfb.as_millis() as f64);
                        }
                        self.stream_started = true;
                    }

                    if !chunk.is_empty() {
                        let preview_len = std::cmp::min(16, chunk.len());
                        let first_bytes: Vec<String> = chunk[..preview_len]
                            .iter()
                            .map(|b| format!("{:02X}", b))
                            .collect();
                        info!("ElevenLabs chunk: {} bytes, first bytes: [{}]",
                              chunk.len(), first_bytes.join(" "));

                        if chunk.len() >= 3 && chunk[0] == 0x49 && chunk[1] == 0x44 && chunk[2] == 0x33 {
                            error!("FATAL: ElevenLabs sent MP3 data (ID3 header detected)!");
                            error!("API ignored output_format=pcm_22050 and returned MP3 instead.");
                            error!("This should not happen with pcm_22050 format. Check API configuration.");
                            warn!("Returning None to skip corrupted MP3 chunk");
                            return None;
                        }

                        if chunk.len() >= 4 {
                            let has_aaaa_pattern = chunk.chunks_exact(2).take(20).all(|pair| {
                                pair[0] == 0xAA && pair[1] == 0xAA
                            });
                            if has_aaaa_pattern {
                                error!("WARNING - CORRUPTION DETECTED: ElevenLabs sent 0xAAAA pattern!");
                            }
                        }

                        if chunk.len() >= 6 {
                            let samples: Vec<i16> = chunk[..6]
                                .chunks_exact(2)
                                .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
                                .collect();
                            info!("ElevenLabs first 3 PCM16 samples: {:?}", samples);
                        }

                        Some((chunk.to_vec(), 16000))
                    } else {
                        None
                    }
                }
                Ok(Some(Err(e))) => {
                    warn!("ElevenLabs stream error: {}", e);
                    self.current_stream = None;
                    None
                }
                Ok(None) => {
                    tracing::info!("ElevenLabs stream ended - no more chunks available");
                    self.current_stream = None;
                    counter!("tts_requests_total", "provider" => "elevenlabs").increment(1);
                    None
                }
                Err(_timeout) => {
                    warn!("ElevenLabs stream timeout after 30s, marking as stalled");
                    None
                }
            }
        } else {
            None
        }
    }

    fn cancel_current_synthesis(&mut self) {
        debug!("Cancelling ElevenLabs synthesis for barge-in");
        self.cancelled = true;
        self.current_stream = None;
        self.stream_started = false;
        self.synthesis_start_time = None;
    }

    fn sample_rate(&self) -> u32 {
        16000
    }
}
