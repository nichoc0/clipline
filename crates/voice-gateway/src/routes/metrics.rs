

use axum::{http::StatusCode, response::Response};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init_metrics() {
    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

    if PROMETHEUS_HANDLE.set(handle).is_err() {
        panic!("Failed to set Prometheus handle");
    }
}

pub async fn metrics() -> Result<Response<String>, StatusCode> {
    let handle = PROMETHEUS_HANDLE.get()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let metrics_text = handle.render();

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(metrics_text)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}