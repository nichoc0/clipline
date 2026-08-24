
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use voice_protocols::AudioFrame;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

impl SessionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug)]
pub struct SessionContext {
    pub session_id: SessionId,
    pub carrier: &'static str,
    pub carrier_call_id: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub started_at: std::time::SystemTime,
}

#[derive(Clone, Debug, Default)]
pub struct SessionMetrics {
    pub duration_ms: u64,
    pub inbound_frames: u64,
    pub outbound_frames: u64,
    pub inbound_dropped: u64,
    pub outbound_dropped: u64,
    pub first_response_latency_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum IngressError {
    #[error("peer disconnected")]
    Disconnected,
    #[error("auth failed: {0}")]
    AuthFailed(String),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("codec negotiation failed: {0}")]
    CodecNegotiation(String),
    #[error("orchestrator error: {0}")]
    Orchestrator(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub struct OrchestratorChannels {
    pub inbound: mpsc::Sender<Bytes>,
    pub outbound: mpsc::Receiver<Bytes>,
    pub orchestrator_cancel: CancellationToken,
}

#[async_trait]
pub trait OrchestratorFactory: Send + Sync {
    async fn build(
        &self,
        ctx: &SessionContext,
    ) -> Result<OrchestratorChannels, IngressError>;
}

#[async_trait]
pub trait CarrierSession: Send {
    fn carrier_name(&self) -> &'static str;
    fn context(&self) -> &SessionContext;

    async fn run(
        self: Box<Self>,
        factory: Arc<dyn OrchestratorFactory>,
        cancel: CancellationToken,
    ) -> Result<SessionMetrics, IngressError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_uniqueness() {
        let a = SessionId::new();
        let b = SessionId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn session_id_from_string_roundtrips() {
        let s = SessionId::from_string("call_control_abc");
        assert_eq!(s.to_string(), "call_control_abc");
    }
}
