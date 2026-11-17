//! Sample Rust backend (axum). Same contract as the other sample backends:
//! GET /health -> 200; anything else -> JSON echo. Query knobs: delay_ms[.<name>], status[.<name>].
use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};

struct App {
    name: String,
}

fn knob(q: &HashMap<String, String>, name: &str, key: &str, default: u64) -> u64 {
    [format!("{key}.{name}"), key.to_string()]
        .iter()
        .find_map(|k| q.get(k).and_then(|v| v.parse().ok()))
        .unwrap_or(default)
}

fn with_headers(app: &App, status: StatusCode, body: serde_json::Value) -> Response {
    let mut resp = (status, Json(body)).into_response();
    let h = resp.headers_mut();
    h.insert("x-backend-name", app.name.parse().unwrap());
    h.insert("x-backend-runtime", "rust".parse().unwrap());
    resp
}

async fn health(State(app): State<Arc<App>>) -> Response {
    with_headers(&app, StatusCode::OK, json!({"status": "ok", "backend": app.name}))
}

async fn echo(
    State(app): State<Arc<App>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let delay = knob(&q, &app.name, "delay_ms", 0);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    let status = StatusCode::from_u16(knob(&q, &app.name, "status", 200) as u16).unwrap_or(StatusCode::OK);
    let hdr = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    with_headers(
        &app,
        status,
        json!({
            "backend": app.name, "runtime": "rust", "method": method.as_str(), "path": uri.path(),
            "query": uri.query().unwrap_or(""), "body_bytes": body.len(),
            "request_id": hdr("x-request-id"), "config_version": hdr("x-mb-config-version"),
            "fanout": hdr("x-mb-fanout") == "true", "delay_ms": delay,
            "time": chrono::Utc::now().to_rfc3339(),
        }),
    )
}

#[tokio::main]
async fn main() {
    let name = std::env::var("BACKEND_NAME").unwrap_or_else(|_| "rust".into());
    let port = std::env::var("PORT").unwrap_or_else(|_| "8004".into());
    let app = Arc::new(App { name: name.clone() });
    let router = Router::new()
        .route("/health", get(health))
        .fallback(echo)
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await.expect("bind");
    println!("rust backend {name} listening on 127.0.0.1:{port}");
    axum::serve(listener, router).await.expect("serve");
}
