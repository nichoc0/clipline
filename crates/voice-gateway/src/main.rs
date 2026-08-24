
use anyhow::Result;
use axum::{Router, routing::{get, post}};
use config::FileFormat;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod routes;
mod telnyx_api;

use crate::routes::{health, telnyx, metrics};
use std::sync::Arc;
use voice_transport::OrchestratorFactory;
use voice_transport_telnyx::{
    handle_media_stream as telnyx_media_stream,
    GatewayOrchestratorFactory,
    SessionRegistryHandle as TelnyxSessionRegistry,
};

const DEFAULT_CONFIG_TOML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../configs/default.toml"
));

fn main() {
    let _ = dotenvy::dotenv();

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to create tokio runtime: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = rt.block_on(async_main()) {
        eprintln!("Application error: {}", e);
        std::process::exit(1);
    }
}

async fn async_main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "voice_gateway=info,tower_http=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Voice Gateway Server");

    metrics::init_metrics();
    info!("Metrics initialized");

    let config_builder = config::Config::builder()
        .add_source(config::File::from_str(DEFAULT_CONFIG_TOML, FileFormat::Toml))
        .add_source(config::File::with_name("configs/default").required(false))
        .add_source(config::File::with_name("configs/production").required(false))
        .add_source(
            config::Environment::with_prefix("VOICE")
                .separator("_")
                .try_parsing(true),
        );

    let config = match config_builder.build() {
        Ok(cfg) => cfg,
        Err(err) => {
            error!("Failed to load configuration: {}", err);
            error!(
                "Ensure /app/configs/default.toml exists or provide VOICE_* overrides."
            );
            return Err(err.into());
        }
    };

    let port = config.get_int("server.port").unwrap_or(8080) as u16;
    let host = config.get_string("server.host").unwrap_or_else(|_| "0.0.0.0".to_string());

    let cors_layer = CorsLayer::permissive();

    let telnyx_factory: Arc<dyn OrchestratorFactory> =
        Arc::new(GatewayOrchestratorFactory::new());
    let telnyx_registry = TelnyxSessionRegistry::new();

    let app = Router::new()
        .route("/health", get(health::health_check))
        .route("/metrics", get(routes::metrics::metrics))
        .route("/telnyx/voice", post(telnyx::voice_webhook_unified))
        .route("/telnyx/status", post(telnyx::status_callback))
        .route("/telnyx/media", get(telnyx_media_stream))
        .layer(axum::Extension(telnyx_factory))
        .layer(axum::Extension(telnyx_registry))
        .layer(cors_layer);

    let addr = format!("{}:{}", host, port).parse::<SocketAddr>()?;
    info!("Voice Gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>()
    )
        .await
        .map_err(|e| {
            error!("Server error: {}", e);
            anyhow::anyhow!("Server failed: {}", e)
        })?;

    Ok(())
}