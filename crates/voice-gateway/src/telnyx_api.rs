

use reqwest::Client;
use std::time::Duration;
use tracing::{error, info};
use voice_protocols::HangupRequest;

#[derive(Clone)]
pub struct TelnyxClient {
    http: Client,
    api_key: String,
    base_url: String,
}

#[derive(Debug)]
pub enum TelnyxError {
    Http(reqwest::Error),
    Api { code: u16, message: String },
    Config(String),
}

impl std::fmt::Display for TelnyxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TelnyxError::Http(e) => write!(f, "HTTP error: {}", e),
            TelnyxError::Api { code, message } => write!(f, "API error {}: {}", code, message),
            TelnyxError::Config(msg) => write!(f, "Configuration error: {}", msg),
        }
    }
}

impl std::error::Error for TelnyxError {}

impl From<reqwest::Error> for TelnyxError {
    fn from(error: reqwest::Error) -> Self {
        TelnyxError::Http(error)
    }
}

impl TelnyxClient {
    pub fn new() -> Result<Self, TelnyxError> {
        let api_key = std::env::var("VOICE_TELNYX_API_KEY")
            .map_err(|_| TelnyxError::Config("VOICE_TELNYX_API_KEY not set".to_string()))?;

        if api_key.is_empty() {
            return Err(TelnyxError::Config("VOICE_TELNYX_API_KEY is empty".to_string()));
        }

        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            http,
            api_key,
            base_url: "https://api.telnyx.com/v2".to_string(),
        })
    }




    pub async fn hangup(&self, call_control_id: &str) -> Result<(), TelnyxError> {
        let request = HangupRequest {};

        info!("Hanging up call {}", call_control_id);

        let response = self
            .http
            .post(format!("{}/calls/{}/actions/hangup", self.base_url, call_control_id))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            info!("Call {} hung up successfully", call_control_id);
            Ok(())
        } else {
            error!("Hangup call failed with status {}: {}", status, response_text);
            Err(TelnyxError::Api {
                code: status.as_u16(),
                message: response_text,
            })
        }
    }

    pub async fn answer_call(&self, call_control_id: &str) -> Result<(), TelnyxError> {
        info!("Answering call {}", call_control_id);

        let response = self
            .http
            .post(format!("{}/calls/{}/actions/answer", self.base_url, call_control_id))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({}))
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            info!("Call {} answered successfully", call_control_id);
            Ok(())
        } else {
            error!("Answer call failed with status {}: {}", status, response_text);
            Err(TelnyxError::Api {
                code: status.as_u16(),
                message: response_text,
            })
        }
    }

    pub async fn answer_call_with_streaming(&self, call_control_id: &str, stream_url: &str) -> Result<(), TelnyxError> {
        info!("Answering call {} with bidirectional streaming to {}", call_control_id, stream_url);

        let request = serde_json::json!({
            "stream_url": stream_url,
            "stream_track": std::env::var("VOICE_TELNYX_STREAM_TRACK").unwrap_or_else(|_| "inbound_track".to_string()),
            "stream_bidirectional_mode": "rtp",
            "stream_bidirectional_codec": "PCMU",
            "stream_bidirectional_sampling_rate": 8000,
            "stream_bidirectional_target_legs": "self"
        });

        let response = self
            .http
            .post(format!("{}/calls/{}/actions/answer", self.base_url, call_control_id))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            info!("Call {} answered successfully with streaming enabled", call_control_id);
            Ok(())
        } else {
            error!("Answer call with streaming failed with status {}: {}", status, response_text);
            Err(TelnyxError::Api {
                code: status.as_u16(),
                message: response_text,
            })
        }
    }

    pub async fn start_streaming(&self, call_control_id: &str, stream_url: &str) -> Result<(), TelnyxError> {
        info!("Starting streaming for call {} to {}", call_control_id, stream_url);

        let request = serde_json::json!({
            "stream_url": stream_url,
            "stream_track": std::env::var("VOICE_TELNYX_STREAM_TRACK").unwrap_or_else(|_| "inbound_track".to_string()),
            "stream_bidirectional_mode": "rtp",
            "stream_bidirectional_codec": "PCMU",
            "stream_bidirectional_sampling_rate": 8000,
            "stream_bidirectional_target_legs": "self"
        });

        info!("Streaming request payload: {}", serde_json::to_string_pretty(&request).unwrap_or_default());

        let response = self
            .http
            .post(format!("{}/calls/{}/actions/streaming_start", self.base_url, call_control_id))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            info!("Streaming started successfully for call {}", call_control_id);
            Ok(())
        } else {
            error!("Start streaming failed with status {}: {}", status, response_text);
            Err(TelnyxError::Api {
                code: status.as_u16(),
                message: response_text,
            })
        }
    }


}