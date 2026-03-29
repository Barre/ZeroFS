use crate::config::Settings;
use crate::fs::stats::FileSystemGlobalStats;
use crate::task::spawn_named;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const POSTHOG_API_KEY: &str = "phc_5CFAL4solVNSoOwtMB5DIP3BrVKHtnUv8b6MHn5j9c6";
const POSTHOG_ENDPOINT: &str = "https://eu.i.posthog.com/capture/";

#[derive(Serialize)]
struct PostHogEvent {
    api_key: &'static str,
    event: String,
    distinct_id: String,
    properties: HashMap<String, serde_json::Value>,
}

impl PostHogEvent {
    fn new(event: &str, distinct_id: &str) -> Self {
        Self {
            api_key: POSTHOG_API_KEY,
            event: event.to_string(),
            distinct_id: distinct_id.to_string(),
            properties: HashMap::new(),
        }
    }

    fn insert_prop(&mut self, key: &str, value: impl Serialize) {
        if let Ok(v) = serde_json::to_value(value) {
            self.properties.insert(key.to_string(), v);
        }
    }
}

async fn capture(client: &reqwest::Client, event: PostHogEvent) {
    let payload = match serde_json::to_string(&event) {
        Ok(p) => p,
        Err(e) => {
            tracing::debug!("Telemetry serialization error: {}", e);
            return;
        }
    };

    match client
        .post(POSTHOG_ENDPOINT)
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
    {
        Ok(resp) => {
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::debug!(
                    "Telemetry event '{}' rejected: {} - {}",
                    event.event,
                    status,
                    body
                );
            }
        }
        Err(e) => {
            tracing::debug!("Telemetry event '{}' failed: {}", event.event, e);
        }
    }
}

/// Get or create a stable anonymous ID for telemetry.
/// Stored as a plain text file in the cache directory.
fn get_or_create_anonymous_id(cache_dir: &Path) -> Option<String> {
    let id_path = cache_dir.join("zerofs-telemetry-id");
    if let Ok(id) = std::fs::read_to_string(&id_path) {
        let trimmed = id.trim().to_string();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    let new_id = Uuid::new_v4().to_string();
    let _ = std::fs::create_dir_all(cache_dir);
    let _ = std::fs::write(&id_path, &new_id);
    Some(new_id)
}

/// Fire-and-forget telemetry event on server startup.
/// Only sends if telemetry is enabled in config.
pub fn send_startup_event(settings: &Settings) {
    if !settings.telemetry.as_ref().is_some_and(|t| t.enabled) {
        return;
    }

    let distinct_id = match get_or_create_anonymous_id(&settings.cache.dir) {
        Some(id) => id,
        None => return,
    };

    let backend_type = determine_backend_type(&settings.storage.url);
    let protocols = determine_protocols(&settings.servers);
    let version = env!("CARGO_PKG_VERSION").to_string();
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();

    spawn_named("telemetry-startup", async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let mut event = PostHogEvent::new("server_started", &distinct_id);
        event.insert_prop("version", &version);
        event.insert_prop("os", os);
        event.insert_prop("arch", arch);
        event.insert_prop("backend_type", backend_type);
        event.insert_prop("protocols", protocols);

        capture(&client, event).await;
    });
}

/// Start periodic telemetry reporting (hourly).
/// Sends aggregated filesystem stats to PostHog.
pub fn start_periodic_reporting(
    settings: &Settings,
    global_stats: Arc<FileSystemGlobalStats>,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    if !settings.telemetry.as_ref().is_some_and(|t| t.enabled) {
        return None;
    }

    let distinct_id = get_or_create_anonymous_id(&settings.cache.dir)?;
    let version = env!("CARGO_PKG_VERSION").to_string();
    let max_bytes = settings.max_bytes();
    let compression = format!("{:?}", settings.compression());
    let cache_disk_gb = settings.cache.disk_size_gb;
    let cache_memory_gb = settings.cache.memory_size_gb;

    Some(spawn_named("telemetry-stats", async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let (used_bytes, used_inodes) = global_stats.get_totals();

                    let mut event = PostHogEvent::new("filesystem_stats", &distinct_id);
                    event.insert_prop("version", &version);
                    event.insert_prop("used_bytes", used_bytes);
                    event.insert_prop("used_inodes", used_inodes);
                    event.insert_prop("max_bytes", max_bytes);
                    event.insert_prop("compression", &compression);
                    event.insert_prop("cache_disk_gb", cache_disk_gb);
                    event.insert_prop("cache_memory_gb", cache_memory_gb);

                    capture(&client, event).await;
                }
            }
        }
    }))
}

fn determine_backend_type(url: &str) -> String {
    if url.starts_with("s3://") || url.starts_with("s3a://") {
        "s3".into()
    } else if url.starts_with("gs://") {
        "gcs".into()
    } else if url.starts_with("az://") || url.starts_with("azure://") || url.starts_with("abfs") {
        "azure".into()
    } else if url.starts_with("file://") {
        "local".into()
    } else {
        "unknown".into()
    }
}

fn determine_protocols(servers: &crate::config::ServerConfig) -> Vec<String> {
    let mut protocols = Vec::new();
    if servers.nfs.is_some() {
        protocols.push("nfs".into());
    }
    if servers.ninep.is_some() {
        protocols.push("9p".into());
    }
    if servers.nbd.is_some() {
        protocols.push("nbd".into());
    }
    protocols
}
