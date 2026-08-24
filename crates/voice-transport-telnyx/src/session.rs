
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};
use base64::prelude::*;
use bytes::Bytes;
use futures::{sink::SinkExt, stream::StreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use metrics::counter;

const MICROS_PER_BYTE_MULAW_8K: u64 = 125;

fn frame_audio_duration(byte_len: usize) -> Duration {
    Duration::from_micros((byte_len as u64) * MICROS_PER_BYTE_MULAW_8K)
}

use voice_orchestrator::VoiceOrchestrator;
use voice_protocols::{MediaFormat, TelnyxMessage};
use voice_transport::{
    CarrierSession, IngressError, OrchestratorChannels, OrchestratorFactory,
    SessionContext, SessionId, SessionMetrics,
};
use serde_json::json;

use crate::registry::SessionRegistryHandle;

pub struct GatewayOrchestratorFactory;

impl GatewayOrchestratorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GatewayOrchestratorFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OrchestratorFactory for GatewayOrchestratorFactory {
    async fn build(&self, ctx: &SessionContext) -> Result<OrchestratorChannels, IngressError> {
        let (inbound_tx, inbound_rx) = mpsc::channel::<Bytes>(1000);
        let (outbound_tx, outbound_rx) = mpsc::channel::<Bytes>(5000);

        let mut orchestrator = VoiceOrchestrator::new(ctx.carrier_call_id.clone());

        let format = MediaFormat {
            encoding: "mulaw".to_string(),
            sample_rate: 8000,
            channels: 1,
        };

        orchestrator
            .start(format)
            .await
            .map_err(|e| IngressError::Orchestrator(format!("orchestrator.start failed: {e}")))?;

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = orchestrator
                .run_with_audio_channels(inbound_rx, outbound_tx)
                .await
            {
                error!("orchestrator.run_with_audio_channels failed: {e}");
            }
            cancel_for_task.cancel();
            orchestrator.stop().await;
        });

        Ok(OrchestratorChannels {
            inbound: inbound_tx,
            outbound: outbound_rx,
            orchestrator_cancel: cancel,
        })
    }
}

pub struct TelnyxSession {
    ws: WebSocket,
    ctx: SessionContext,
    registry: SessionRegistryHandle,
}

impl TelnyxSession {
    pub fn new(
        ws: WebSocket,
        call_control_id: String,
        registry: SessionRegistryHandle,
    ) -> Self {
        let ctx = SessionContext {
            session_id: SessionId::new(),
            carrier: "telnyx",
            carrier_call_id: call_control_id,
            from: None,
            to: None,
            started_at: SystemTime::now(),
        };
        Self { ws, ctx, registry }
    }
}

#[async_trait]
impl CarrierSession for TelnyxSession {
    fn carrier_name(&self) -> &'static str {
        "telnyx"
    }

    fn context(&self) -> &SessionContext {
        &self.ctx
    }

    async fn run(
        self: Box<Self>,
        factory: Arc<dyn OrchestratorFactory>,
        cancel: CancellationToken,
    ) -> Result<SessionMetrics, IngressError> {
        let Self { ws, ctx, registry } = *self;
        registry.insert(ctx.carrier_call_id.clone());
        let result = run_session_loop(ws, &ctx, factory, cancel).await;
        registry.remove(&ctx.carrier_call_id);
        result
    }
}

async fn run_session_loop(
    ws: WebSocket,
    ctx: &SessionContext,
    factory: Arc<dyn OrchestratorFactory>,
    cancel: CancellationToken,
) -> Result<SessionMetrics, IngressError> {
    let channels = factory.build(ctx).await?;
    let OrchestratorChannels {
        inbound: inbound_tx,
        outbound: mut outbound_rx,
        orchestrator_cancel,
    } = channels;

    let (mut ws_sender, mut ws_receiver) = ws.split();

    let stream_id = Arc::new(RwLock::new(String::new()));
    let started_at = Instant::now();

    let mut metrics = SessionMetrics::default();
    let inbound_metrics = Arc::new(parking_lot_compat::AtomicU64::new(0));
    let outbound_metrics = Arc::new(parking_lot_compat::AtomicU64::new(0));
    let dropped_metrics = Arc::new(parking_lot_compat::AtomicU64::new(0));

    let stream_id_recv = stream_id.clone();
    let inbound_for_recv = inbound_tx.clone();
    let inbound_count_recv = inbound_metrics.clone();
    let dropped_count_recv = dropped_metrics.clone();
    let recv_cancel = cancel.clone();
    let recv_orch_cancel = orchestrator_cancel.clone();
    let call_id_recv = ctx.carrier_call_id.clone();
    let receive_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = recv_cancel.cancelled() => {
                    debug!("Telnyx receive task cancelled (transport side)");
                    break;
                }
                _ = recv_orch_cancel.cancelled() => {
                    debug!("Telnyx receive task cancelled (orchestrator side)");
                    break;
                }
                msg = ws_receiver.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<TelnyxMessage>(&text) {
                                Ok(parsed) => {
                                    handle_inbound(
                                        parsed,
                                        &stream_id_recv,
                                        &inbound_for_recv,
                                        &inbound_count_recv,
                                        &dropped_count_recv,
                                    ).await
                                }
                                Err(e) => warn!("Failed to parse Telnyx message: {} - raw: {}", e, text),
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("Telnyx WebSocket closed for call {}", call_id_recv);
                            break;
                        }
                        Some(Ok(_)) => {  }
                        Some(Err(e)) => {
                            error!("Telnyx WebSocket recv error: {}", e);
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
    });

    let stream_id_send = stream_id.clone();
    let outbound_count_send = outbound_metrics.clone();
    let send_cancel = cancel.clone();
    let send_orch_cancel = orchestrator_cancel.clone();
    let mut barge_in_rx = voice_orchestrator::barge_in_bus::global()
        .subscribe(&ctx.carrier_call_id);
    let stream_id_for_clear = stream_id.clone();
    let send_task = tokio::spawn(async move {
        let mut chunk_counter: u64 = 0;
        let mut next_send_at: Option<tokio::time::Instant> = None;
        loop {
            tokio::select! {
                _ = send_cancel.cancelled() => {
                    debug!("Telnyx send task cancelled (transport side)");
                    break;
                }
                _ = send_orch_cancel.cancelled() => {
                    debug!("Telnyx send task cancelled (orchestrator side)");
                    break;
                }
                _ = barge_in_rx.recv() => {
                    let mut drained = 0usize;
                    while outbound_rx.try_recv().is_ok() {
                        drained += 1;
                    }
                    let stream_id_read = stream_id_for_clear.read().await;
                    if !stream_id_read.is_empty() {
                        let clear_msg = serde_json::json!({
                            "event": "clear",
                            "stream_id": stream_id_read.clone(),
                        }).to_string();
                        drop(stream_id_read);
                        if let Err(e) = ws_sender.send(Message::Text(clear_msg)).await {
                            warn!("Failed to send Telnyx clear: {}", e);
                        } else {
                            info!("Barge-in: drained {} pending frames + sent Telnyx clear", drained);
                        }
                    }
                    next_send_at = None;
                    continue;
                }
                audio_bytes = outbound_rx.recv() => {
                    let Some(audio_bytes) = audio_bytes else {
                        debug!("Outbound channel closed");
                        break;
                    };
                    chunk_counter = chunk_counter.saturating_add(1);

                    let stream_id_read = stream_id_send.read().await;
                    if stream_id_read.is_empty() {
                        debug!("Dropping outbound chunk {}: stream_id not yet known", chunk_counter);
                        continue;
                    }

                    let encoded = BASE64_STANDARD.encode(&audio_bytes);
                    let body = json!({
                        "event": "media",
                        "stream_id": stream_id_read.clone(),
                        "media": { "payload": encoded },
                    })
                    .to_string();
                    drop(stream_id_read);

                    let this_frame_duration = frame_audio_duration(audio_bytes.len());
                    if let Some(deadline) = next_send_at {
                        tokio::time::sleep_until(deadline).await;
                    }
                    let slot = next_send_at
                        .map(|d| d + this_frame_duration)
                        .unwrap_or_else(|| tokio::time::Instant::now() + this_frame_duration);
                    next_send_at = Some(slot);

                    if let Err(e) = ws_sender.send(Message::Text(body)).await {
                        warn!("Telnyx WebSocket send error: {}", e);
                        break;
                    }
                    outbound_count_send.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    });

    tokio::select! {
        _ = receive_task => debug!("Telnyx receive task ended"),
        _ = send_task => debug!("Telnyx send task ended"),
        _ = cancel.cancelled() => debug!("Telnyx session cancelled by gateway"),
        _ = orchestrator_cancel.cancelled() => debug!("Telnyx session cancelled by orchestrator"),
    }

    cancel.cancel();
    orchestrator_cancel.cancel();
    drop(inbound_tx);

    metrics.duration_ms = started_at.elapsed().as_millis() as u64;
    metrics.inbound_frames = inbound_metrics.load(std::sync::atomic::Ordering::Relaxed);
    metrics.outbound_frames = outbound_metrics.load(std::sync::atomic::Ordering::Relaxed);
    metrics.inbound_dropped = dropped_metrics.load(std::sync::atomic::Ordering::Relaxed);

    Ok(metrics)
}

async fn handle_inbound(
    msg: TelnyxMessage,
    stream_id: &Arc<RwLock<String>>,
    inbound_tx: &mpsc::Sender<Bytes>,
    inbound_count: &Arc<parking_lot_compat::AtomicU64>,
    dropped_count: &Arc<parking_lot_compat::AtomicU64>,
) {
    match msg {
        TelnyxMessage::Connected { version } => {
            info!("Telnyx connected, version {}", version);
        }
        TelnyxMessage::Start { stream_id: msg_stream_id, start, .. } => {
            info!(
                "Telnyx stream started — control_id={}, session={}, stream_id={}",
                start.call_control_id, start.call_session_id, msg_stream_id
            );
            *stream_id.write().await = msg_stream_id;
        }
        TelnyxMessage::Media { media, .. } => {
            if media.track != "inbound" {
                debug!("Ignoring track='{}' (echo guard)", media.track);
                return;
            }
            let Ok(audio) = BASE64_STANDARD.decode(&media.payload) else {
                debug!("Failed to decode media payload (base64)");
                return;
            };
            let audio_bytes = Bytes::from(audio);
            match inbound_tx.try_send(audio_bytes) {
                Ok(_) => {
                    inbound_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("Telnyx ingress channel full, dropping frame");
                    dropped_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    counter!("pipeline_drops_total", "channel" => "ws_ingress").increment(1);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!("Telnyx ingress channel closed");
                }
            }
        }
        TelnyxMessage::Stop { stop, .. } => {
            info!("Telnyx stream stopped for call {}", stop.call_control_id);
        }
        TelnyxMessage::Mark { name } => debug!("Telnyx Mark received: {}", name),
        TelnyxMessage::Clear {} => debug!("Telnyx Clear received"),
    }
}

mod parking_lot_compat {
    pub use std::sync::atomic::AtomicU64;
}
