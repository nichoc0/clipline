

use serde::{Deserialize, Serialize};
use crate::AudioFrame;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEvent {
    Started { session_id: String, call_sid: String },
    Connected,
    SttResult { text: String, is_final: bool },
    LlmResponse { text: String, is_complete: bool },
    TtsGenerated { audio_data: Vec<u8> },
    Error { message: String },
    Ended { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionState {
    Connecting,
    Active,
    Paused,
    Ending,
    Terminated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineEvent {
    AudioReceived { frame: AudioFrame },
    SttPartial { text: String, confidence: f32 },
    SttFinal { text: String, confidence: f32 },
    LlmTokens { tokens: Vec<String> },
    LlmComplete { response: String },
    TtsAudio { data: Vec<u8>, chunk_id: u64, sample_rate: u32 },
    TtsComplete,
    VadDetected { is_speech: bool },
    Interrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProviderEvent {
    Connected,
    Disconnected,
    Error { code: i32, message: String },
    RateLimited { retry_after: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    TranscriptUpdate {
        session_id: String,
        text: String,
        is_final: bool,
    },
    ConversationComplete {
        session_id: String,
        call_sid: String,
        full_transcript: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedContactInfo {
    pub session_id: String,
    pub call_sid: String,
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub full_transcript: String,
}