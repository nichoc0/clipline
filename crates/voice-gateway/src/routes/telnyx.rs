
use axum::{Form, response::Html, http::{StatusCode, HeaderMap}, extract::OriginalUri, body::Bytes};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tracing::{info, warn, error};
use hmac::{Hmac, Mac, digest::KeyInit};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use std::sync::OnceLock;
use voice_orchestrator::VoiceOrchestrator;
use crate::telnyx_api::TelnyxClient;

type OrchestratorSessions = Arc<RwLock<HashMap<String, VoiceOrchestrator>>>;

fn fast_answered() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static S: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    S.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}
static ORCHESTRATOR_SESSIONS: OnceLock<OrchestratorSessions> = OnceLock::new();

pub fn get_orchestrator_sessions() -> &'static OrchestratorSessions {
    ORCHESTRATOR_SESSIONS.get_or_init(|| {
        Arc::new(RwLock::new(HashMap::new()))
    })
}

#[derive(Deserialize, Serialize)]
pub struct VoiceWebhookParams {
    #[serde(rename = "call_control_id")]
    call_control_id: String,
    #[serde(rename = "call_session_id")]
    call_session_id: String,
    #[serde(rename = "call_leg_id")]
    call_leg_id: String,
    #[serde(rename = "from")]
    from: String,
    #[serde(rename = "to")]
    to: String,
    #[serde(rename = "direction")]
    direction: Option<String>,
    #[serde(rename = "state")]
    state: Option<String>,
    #[serde(rename = "event_type")]
    event_type: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize, Serialize)]
pub struct StatusCallbackParams {
    #[serde(rename = "call_control_id")]
    call_control_id: String,
    #[serde(rename = "call_session_id")]
    call_session_id: String,
    #[serde(rename = "call_leg_id")]
    call_leg_id: String,
    #[serde(rename = "event_type")]
    event_type: String,
    #[serde(rename = "from")]
    from: Option<String>,
    #[serde(rename = "to")]
    to: Option<String>,
    #[serde(rename = "duration")]
    duration: Option<String>,
    #[serde(rename = "recording_urls", default)]
    recording_urls: Vec<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

fn validate_telnyx_signature_hmac(
    signing_secret: &str,
    timestamp: &str,
    body: &str,
    signature: &str,
) -> bool {
    let mac = <Hmac<sha2::Sha256> as KeyInit>::new_from_slice(signing_secret.as_bytes());
    let mut mac = match mac {
        Ok(m) => m,
        Err(_) => return false,
    };

    let payload = format!("{}{}", timestamp, body);
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    let expected = hex::encode(result.into_bytes());

    expected.as_bytes().ct_eq(signature.as_bytes()).into()
}

fn validate_telnyx_signature(
    headers: &HeaderMap,
    body: &str,
) -> bool {
    let signing_secret = std::env::var("VOICE_TELNYX_SIGNING_SECRET").unwrap_or_default();
    if signing_secret.is_empty() {
        return true;
    }

    let _signature = match headers.get("telnyx-signature-ed25519") {
        Some(sig) => sig.to_str().unwrap_or(""),
        None => {
            match headers.get("telnyx-signature") {
                Some(sig) => sig.to_str().unwrap_or(""),
                None => return false,
            }
        }
    };

    let timestamp = match headers.get("telnyx-timestamp") {
        Some(ts) => ts.to_str().unwrap_or(""),
        None => return false,
    };

    if let Some(hmac_sig) = headers.get("telnyx-signature") {
        if let Ok(sig_str) = hmac_sig.to_str() {
            return validate_telnyx_signature_hmac(&signing_secret, timestamp, body, sig_str);
        }
    }

    false
}

pub async fn voice_webhook_unified(
    headers: HeaderMap,
    _uri: OriginalUri,
    body: Bytes,
) -> Result<Html<String>, StatusCode> {
    let content_type = headers
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/json") {
        let body_str = String::from_utf8_lossy(&body);
        info!("DEBUG: Received JSON webhook body: {}", body_str);

        let json_value: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| {
                warn!("Failed to parse JSON webhook: {}", e);
                StatusCode::BAD_REQUEST
            })?;

        let payload = json_value.get("data")
            .and_then(|d| d.get("payload"))
            .unwrap_or(&json_value);

        let event_type = json_value.get("data")
            .and_then(|d| d.get("event_type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let call_control_id = payload.get("call_control_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let direction_str = payload.get("direction")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let is_incoming = matches!(direction_str, "incoming" | "inbound" | "");

        if let Some(ref event_type) = event_type {
            if event_type == "call.initiated" && !call_control_id.is_empty() && is_incoming {
                info!("FAST: Answering call.initiated with streaming: {}", call_control_id);
                if let Ok(mut s) = fast_answered().lock() { s.insert(call_control_id.clone()); }
                if let Ok(client) = TelnyxClient::new() {
                    let call_id = call_control_id.clone();

                    let base_url = std::env::var("VOICE_PUBLIC_WS_URL")
                        .or_else(|_| std::env::var("VOICE_PUBLIC_BASE_URL"))
                        .unwrap_or_else(|_| "wss://your-public-host".to_string());

                    let stream_url = if base_url.contains("/telnyx/media") {
                        format!("{}?call_control_id={}", base_url, call_id)
                    } else if base_url.starts_with("wss://") || base_url.starts_with("ws://") {
                        format!("{}/telnyx/media?call_control_id={}", base_url, call_id)
                    } else {
                        let ws_scheme = if base_url.starts_with("https://") { "wss" } else { "ws" };
                        let clean_base = base_url.trim_start_matches("https://").trim_start_matches("http://");
                        format!("{}://{}/telnyx/media?call_control_id={}", ws_scheme, clean_base, call_id)
                    };

                    info!("DEBUG: base_url='{}', constructed stream_url='{}'", base_url, stream_url);

                    tokio::spawn(async move {
                        info!("DEBUG: Calling answer_call_with_streaming with URL: {}", stream_url);
                        if let Err(e) = client.answer_call_with_streaming(&call_id, &stream_url).await {
                            error!("Failed to answer call {} with streaming: {:?}", call_id, e);
                        } else {
                            info!("Successfully answered call {} with streaming", call_id);
                        }
                    });
                }
            }
        }

        let call_session_id = payload.get("call_session_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let call_leg_id = payload.get("call_leg_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let from = payload.get("from")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let to = payload.get("to")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let direction = payload.get("direction")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let state = payload.get("state")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let params = VoiceWebhookParams {
            call_control_id: call_control_id.clone(),
            call_session_id,
            call_leg_id,
            from,
            to,
            direction,
            state,
            event_type,
            extra: BTreeMap::new(),
        };

        if !validate_telnyx_signature(&headers, &body_str) {
            warn!("Invalid Telnyx signature for call {}", params.call_control_id);
            return Err(StatusCode::UNAUTHORIZED);
        }

        info!(
            "Incoming Telnyx call (JSON) - Control ID: {}, Session ID: {}, From: {}, To: {}, State: {:?}, Event: {:?}",
            params.call_control_id, params.call_session_id, params.from, params.to, params.state, params.event_type
        );

        if let Some(ref event_type) = params.event_type {
            if event_type == "call.hangup" {
                info!("Call hangup detected for {}, cleaning up orchestrator", call_control_id);
                if let Ok(mut s) = fast_answered().lock() { s.remove(&call_control_id); }

                let sessions = get_orchestrator_sessions();
                if let Some(mut orchestrator) = sessions.write().await.remove(&call_control_id) {
                    info!("Shutting down orchestrator for call {}", call_control_id);
                    orchestrator.stop().await;
                    info!("Orchestrator shutdown complete for call {}", call_control_id);
                } else {
                    warn!("No orchestrator session found for hangup call {}", call_control_id);
                }
            }

            if event_type == "streaming.failed" && !call_control_id.is_empty() {
                warn!("Streaming failed for {}, hanging up to free the slot", call_control_id);
                if let Ok(client) = TelnyxClient::new() {
                    let call_id = call_control_id.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client.hangup(&call_id).await {
                            error!("Hangup-after-streaming-failed for {} errored: {:?}", call_id, e);
                        }
                    });
                }
                let sessions = get_orchestrator_sessions();
                if let Some(mut orchestrator) = sessions.write().await.remove(&call_control_id) {
                    orchestrator.stop().await;
                }
            }
        }

        Ok(Html("OK".to_string()))
    } else {
        let body_str = String::from_utf8_lossy(&body);
        info!("DEBUG: Received form-encoded webhook body: {}", body_str);
        let params: VoiceWebhookParams = serde_urlencoded::from_str(&body_str)
            .map_err(|_| StatusCode::BAD_REQUEST)?;

        if !validate_telnyx_signature(&headers, &body_str) {
            warn!("Invalid Telnyx signature for call {}", params.call_control_id);
            return Err(StatusCode::UNAUTHORIZED);
        }

        info!(
            "Incoming Telnyx call (Form) - Control ID: {}, Session ID: {}, From: {}, To: {}, State: {:?}, Event: {:?}",
            params.call_control_id, params.call_session_id, params.from, params.to, params.state, params.event_type
        );

        if let Some(ref event_type) = params.event_type {
            if event_type == "call.initiated" {
                info!("Processing call.initiated for {}", params.call_control_id);

                match TelnyxClient::new() {
                    Ok(client) => {
                        let call_control_id = params.call_control_id.clone();
                        info!("Answering call {}", call_control_id);
                        if let Err(e) = client.answer_call(&call_control_id).await {
                            error!("Failed to answer call {}: {:?}", call_control_id, e);
                        } else {
                            info!("Successfully answered call {}", call_control_id);

                            let client_clone = client.clone();
                            let call_control_id_clone = call_control_id.clone();
                            tokio::spawn(async move {
                                let base_url = std::env::var("VOICE_PUBLIC_WS_URL")
                                    .or_else(|_| std::env::var("VOICE_PUBLIC_BASE_URL"))
                                    .unwrap_or_else(|_| "wss://your-public-host".to_string());

                                let stream_url = if base_url.contains("/telnyx/media") {
                                    format!("{}?call_control_id={}", base_url, call_control_id_clone)
                                } else if base_url.starts_with("wss://") || base_url.starts_with("ws://") {
                                    format!("{}/telnyx/media?call_control_id={}", base_url, call_control_id_clone)
                                } else {
                                    let ws_scheme = if base_url.starts_with("https://") { "wss" } else { "ws" };
                                    let clean_base = base_url.trim_start_matches("https://").trim_start_matches("http://");
                                    format!("{}://{}/telnyx/media?call_control_id={}", ws_scheme, clean_base, call_control_id_clone)
                                };

                                info!("Base URL: {}, Final stream URL: {}", base_url, stream_url);
                                info!("Starting media streaming for call {} to {}", call_control_id_clone, stream_url);
                                if let Err(e) = client_clone.start_streaming(&call_control_id_clone, &stream_url).await {
                                    error!("Failed to start streaming for call {}: {:?}", call_control_id_clone, e);
                                } else {
                                    info!("Successfully started streaming for call {}", call_control_id_clone);
                                }
                            });
                        }
                    }
                    Err(e) => {
                        error!("Failed to create Telnyx client: {}", e);
                        warn!("Continuing with webhook response - call will not be answered");
                    }
                }
            }
        }

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="Polly.Joanna">I'm processing your call, please wait.</Say>
    <Stream url="wss://your-public-host/telnyx/media?call_control_id={}" track="both"/>
</Response>"#,
            params.call_control_id
        );

        Ok(Html(xml))
    }
}

pub async fn status_callback(
    headers: HeaderMap,
    _uri: OriginalUri,
    Form(params): Form<StatusCallbackParams>
) -> Result<StatusCode, StatusCode> {
    let body = serde_urlencoded::to_string(&params).unwrap_or_default();

    if !validate_telnyx_signature(&headers, &body) {
        warn!("Invalid Telnyx signature for status callback {}", params.call_control_id);
        return Err(StatusCode::UNAUTHORIZED);
    }

    info!(
        "Telnyx call status update - Control ID: {}, Session ID: {}, Event: {}, From: {:?}, To: {:?}, Duration: {:?}",
        params.call_control_id, params.call_session_id, params.event_type, params.from, params.to, params.duration
    );

    if !params.recording_urls.is_empty() {
        info!(
            "Recording available - CallControlId: {}, URLs: {:?}",
            params.call_control_id, params.recording_urls
        );
    }

    Ok(StatusCode::OK)
}