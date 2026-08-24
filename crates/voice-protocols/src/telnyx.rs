

use serde::{Serialize, Deserialize};

#[derive(Serialize)]
#[serde(tag = "event")]
pub enum TelnyxResponse {
    #[serde(rename = "media")]
    Media {
        #[serde(rename = "stream_id")]
        stream_id: String,
        #[serde(rename = "sequence_number")]
        sequence_number: String,
        media: TelnyxMediaPayload,
    },
    #[serde(rename = "clear")]
    Clear {
        #[serde(rename = "call_control_id")]
        call_control_id: String,
    },
    #[serde(rename = "mark")]
    Mark {
        #[serde(rename = "call_control_id")]
        call_control_id: String,
        name: String,
    },
}

#[derive(Serialize)]
pub struct TelnyxMediaPayload {
    pub track: String,
    pub chunk: String,
    pub timestamp: String,
    pub payload: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "event")]
pub enum TelnyxMessage {
    #[serde(rename = "connected")]
    Connected {
        version: String,
    },
    #[serde(rename = "start")]
    Start {
        #[serde(rename = "stream_id")]
        stream_id: String,
        #[serde(rename = "sequence_number")]
        sequence_number: String,
        start: TelnyxStartPayload,
    },
    #[serde(rename = "media")]
    Media {
        #[serde(rename = "stream_id")]
        stream_id: String,
        #[serde(rename = "sequence_number")]
        sequence_number: String,
        media: TelnyxInboundMediaPayload,
    },
    #[serde(rename = "stop")]
    Stop {
        #[serde(rename = "stream_id")]
        stream_id: String,
        #[serde(rename = "sequence_number")]
        sequence_number: String,
        stop: TelnyxStopPayload,
    },
    #[serde(rename = "mark")]
    Mark {
        name: String,
    },
    #[serde(rename = "clear")]
    Clear {},
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelnyxInboundMediaPayload {
    pub track: String,
    pub chunk: String,
    pub timestamp: String,
    pub payload: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelnyxStartPayload {
    #[serde(rename = "call_control_id")]
    pub call_control_id: String,
    #[serde(rename = "call_session_id")]
    pub call_session_id: String,
    pub to: String,
    pub from: String,
    #[serde(rename = "media_format")]
    pub media_format: TelnyxMediaFormat,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelnyxStopPayload {
    #[serde(rename = "call_control_id")]
    pub call_control_id: String,
    pub user_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelnyxMediaFormat {
    pub encoding: String,
    #[serde(rename = "sample_rate")]
    pub sample_rate: u32,
    pub channels: u16,
}

#[derive(Serialize)]
pub struct OriginateCallRequest {
    #[serde(rename = "connection_id")]
    pub connection_id: String,
    pub to: String,
    pub from: String,
    #[serde(rename = "webhook_url")]
    pub webhook_url: String,
    #[serde(rename = "stream_url", skip_serializing_if = "Option::is_none")]
    pub stream_url: Option<String>,
    #[serde(rename = "stream_track", skip_serializing_if = "Option::is_none")]
    pub stream_track: Option<String>,
    #[serde(rename = "stream_bidirectional_mode", skip_serializing_if = "Option::is_none")]
    pub stream_bidirectional_mode: Option<String>,
    #[serde(rename = "stream_bidirectional_codec", skip_serializing_if = "Option::is_none")]
    pub stream_bidirectional_codec: Option<String>,
    #[serde(rename = "stream_bidirectional_sampling_rate", skip_serializing_if = "Option::is_none")]
    pub stream_bidirectional_sampling_rate: Option<u32>,
    #[serde(rename = "stream_bidirectional_target_legs", skip_serializing_if = "Option::is_none")]
    pub stream_bidirectional_target_legs: Option<String>,
}

#[derive(Deserialize)]
pub struct OriginateCallResponse {
    pub data: CallControlData,
}

#[derive(Deserialize)]
pub struct CallControlData {
    #[serde(rename = "call_control_id")]
    pub call_control_id: String,
    #[serde(rename = "call_session_id", default)]
    pub call_session_id: String,
    #[serde(rename = "call_leg_id", default)]
    pub call_leg_id: String,
    #[serde(default)]
    pub state: String,
}

#[derive(Serialize)]
pub struct TransferCallRequest {
    pub to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

#[derive(Serialize)]
pub struct HangupRequest {
}

#[derive(Serialize)]
pub struct TeXMLResponse {
    #[serde(rename = "Response")]
    pub response: TeXMLResponseInner,
}

#[derive(Serialize)]
pub struct TeXMLResponseInner {
    #[serde(rename = "Say", skip_serializing_if = "Option::is_none")]
    pub say: Option<TeXMLSay>,
    #[serde(rename = "Stream")]
    pub stream: TeXMLStream,
}

#[derive(Serialize)]
pub struct TeXMLSay {
    #[serde(rename = "$text")]
    pub text: String,
    #[serde(rename = "@voice", skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
}

#[derive(Serialize)]
pub struct TeXMLStream {
    #[serde(rename = "@url")]
    pub url: String,
    #[serde(rename = "@track", skip_serializing_if = "Option::is_none")]
    pub track: Option<String>,
}