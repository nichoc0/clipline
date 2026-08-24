

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFrame {
    pub data: Bytes,
    pub sample_rate: u32,
    pub channels: u16,
    pub encoding: AudioEncoding,
    pub timestamp: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AudioEncoding {
    PCM16,
    MuLaw,
    ALaw,
    Opus,
    MP3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaFormat {
    #[serde(rename = "encoding")]
    pub encoding: String,
    #[serde(rename = "sampleRate")]
    pub sample_rate: u32,
    #[serde(rename = "channels")]
    pub channels: u16,
}

impl Default for AudioFrame {
    fn default() -> Self {
        Self {
            data: Bytes::new(),
            sample_rate: 8000,
            channels: 1,
            encoding: AudioEncoding::MuLaw,
            timestamp: 0,
            sequence: 0,
        }
    }
}

impl AudioEncoding {
    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "pcm16" | "pcm" => Some(Self::PCM16),
            "mulaw" | "ulaw" => Some(Self::MuLaw),
            "alaw" => Some(Self::ALaw),
            "opus" => Some(Self::Opus),
            "mp3" => Some(Self::MP3),
            _ => None,
        }
    }

    pub fn to_string(&self) -> &'static str {
        match self {
            Self::PCM16 => "pcm16",
            Self::MuLaw => "mulaw",
            Self::ALaw => "alaw",
            Self::Opus => "opus",
            Self::MP3 => "mp3",
        }
    }
}