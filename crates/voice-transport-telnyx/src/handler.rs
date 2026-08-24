
use axum::{
    extract::{ws::WebSocketUpgrade, Extension, Query},
    response::Response,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use voice_transport::{CarrierSession, OrchestratorFactory};

use crate::registry::SessionRegistryHandle;
use crate::session::TelnyxSession;

#[derive(Deserialize)]
pub struct MediaStreamParams {
    pub call_control_id: String,
}

pub async fn handle_media_stream(
    ws: WebSocketUpgrade,
    Query(params): Query<MediaStreamParams>,
    Extension(factory): Extension<Arc<dyn OrchestratorFactory>>,
    Extension(registry): Extension<SessionRegistryHandle>,
) -> Response {
    info!(
        "Telnyx WebSocket upgrade requested for call {}",
        params.call_control_id
    );
    ws.on_upgrade(move |socket| async move {
        let session = TelnyxSession::new(socket, params.call_control_id, registry);
        let cancel = CancellationToken::new();
        let session: Box<dyn CarrierSession + Send> = Box::new(session);
        match session.run(factory, cancel).await {
            Ok(metrics) => info!(
                "Telnyx session ended cleanly. duration_ms={} inbound={} outbound={} dropped={}",
                metrics.duration_ms,
                metrics.inbound_frames,
                metrics.outbound_frames,
                metrics.inbound_dropped,
            ),
            Err(e) => error!("Telnyx session ended with error: {}", e),
        }
    })
}
