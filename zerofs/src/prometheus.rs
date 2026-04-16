use crate::config::PrometheusConfig;
use crate::fs::metrics::FileSystemStats;
use crate::fs::stats::FileSystemGlobalStats;
use crate::task::spawn_named;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Start the Prometheus metrics exporter.
///
/// Installs the global metrics recorder, spawns an HTTP server per configured address
/// serving `/metrics`, and starts a background collector task that bridges existing
/// ZeroFS and SlateDB stats into the metrics crate.
pub fn start(
    config: &PrometheusConfig,
    fs_stats: Arc<FileSystemStats>,
    global_stats: Arc<FileSystemGlobalStats>,
    slatedb_registry: Option<Arc<DefaultMetricsRecorder>>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    metrics::set_global_recorder(recorder).expect("failed to install Prometheus recorder");

    let mut handles = Vec::new();

    for &addr in &config.addresses {
        tracing::info!(
            "Prometheus metrics server listening on http://{}/metrics",
            addr
        );
        let server_handle = handle.clone();
        let server_shutdown = shutdown.clone();
        handles.push(spawn_named("prometheus-http", async move {
            serve_metrics(addr, server_handle, server_shutdown).await;
        }));
    }

    let upkeep_handle = handle.clone();
    handles.push(spawn_named("prometheus-collector", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("Prometheus collector shutting down");
                    break;
                }
                _ = interval.tick() => {
                    collect_fs_stats(&fs_stats);
                    collect_global_stats(&global_stats);
                    if let Some(ref registry) = slatedb_registry {
                        collect_slatedb_stats(registry);
                    }
                    collect_jemalloc_stats();
                    upkeep_handle.run_upkeep();
                }
            }
        }
    }));

    handles
}

type HttpResponse = hyper::Response<http_body_util::Full<bytes::Bytes>>;

fn handle_request(
    req: hyper::Request<impl hyper::body::Body>,
    metrics_handle: &PrometheusHandle,
) -> HttpResponse {
    if req.uri().path() != "/metrics" {
        return hyper::Response::builder()
            .status(404)
            .body(http_body_util::Full::new(bytes::Bytes::from("Not Found")))
            .unwrap();
    }

    hyper::Response::builder()
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(http_body_util::Full::new(bytes::Bytes::from(
            metrics_handle.render(),
        )))
        .unwrap()
}

async fn serve_metrics(addr: SocketAddr, handle: PrometheusHandle, shutdown: CancellationToken) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind Prometheus HTTP server to {}: {}", addr, e);
            return;
        }
    };

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            result = listener.accept() => {
                let (stream, _) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::debug!("Prometheus accept error: {}", e);
                        continue;
                    }
                };
                let handle = handle.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        std::future::ready(Ok::<_, std::convert::Infallible>(
                            handle_request(req, &handle),
                        ))
                    });
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        }
    }
}

fn collect_fs_stats(stats: &FileSystemStats) {
    counter!("zerofs_files_created_total").absolute(stats.files_created.load(Ordering::Relaxed));
    counter!("zerofs_files_deleted_total").absolute(stats.files_deleted.load(Ordering::Relaxed));
    counter!("zerofs_files_renamed_total").absolute(stats.files_renamed.load(Ordering::Relaxed));
    counter!("zerofs_directories_created_total")
        .absolute(stats.directories_created.load(Ordering::Relaxed));
    counter!("zerofs_directories_deleted_total")
        .absolute(stats.directories_deleted.load(Ordering::Relaxed));
    counter!("zerofs_directories_renamed_total")
        .absolute(stats.directories_renamed.load(Ordering::Relaxed));
    counter!("zerofs_links_created_total").absolute(stats.links_created.load(Ordering::Relaxed));
    counter!("zerofs_links_deleted_total").absolute(stats.links_deleted.load(Ordering::Relaxed));
    counter!("zerofs_links_renamed_total").absolute(stats.links_renamed.load(Ordering::Relaxed));
    counter!("zerofs_read_operations_total")
        .absolute(stats.read_operations.load(Ordering::Relaxed));
    counter!("zerofs_write_operations_total")
        .absolute(stats.write_operations.load(Ordering::Relaxed));
    counter!("zerofs_bytes_read_total").absolute(stats.bytes_read.load(Ordering::Relaxed));
    counter!("zerofs_bytes_written_total").absolute(stats.bytes_written.load(Ordering::Relaxed));
    counter!("zerofs_tombstones_created_total")
        .absolute(stats.tombstones_created.load(Ordering::Relaxed));
    counter!("zerofs_tombstones_processed_total")
        .absolute(stats.tombstones_processed.load(Ordering::Relaxed));
    counter!("zerofs_gc_chunks_deleted_total")
        .absolute(stats.gc_chunks_deleted.load(Ordering::Relaxed));
    counter!("zerofs_gc_runs_total").absolute(stats.gc_runs.load(Ordering::Relaxed));
    counter!("zerofs_total_operations").absolute(stats.total_operations.load(Ordering::Relaxed));
}

fn collect_global_stats(stats: &FileSystemGlobalStats) {
    let (used_bytes, used_inodes) = stats.get_totals();
    gauge!("zerofs_used_bytes").set(used_bytes as f64);
    gauge!("zerofs_used_inodes").set(used_inodes as f64);
}

fn collect_jemalloc_stats() {
    let mem = crate::rpc::server::JemallocMemStats::read();
    gauge!("zerofs_jemalloc_allocated_bytes").set(mem.allocated as f64);
    gauge!("zerofs_jemalloc_resident_bytes").set(mem.resident as f64);
    gauge!("zerofs_jemalloc_mapped_bytes").set(mem.mapped as f64);
    gauge!("zerofs_jemalloc_retained_bytes").set(mem.retained as f64);
    gauge!("zerofs_jemalloc_metadata_bytes").set(mem.metadata as f64);
}

fn collect_slatedb_stats(recorder: &DefaultMetricsRecorder) {
    let snapshot = recorder.snapshot();
    for metric in snapshot.all() {
        let prom_name = metric.name.replace('.', "_");
        match &metric.value {
            MetricValue::Counter(v) => {
                counter!(prom_name).absolute(*v);
            }
            MetricValue::Gauge(v) => {
                gauge!(prom_name).set(*v as f64);
            }
            MetricValue::UpDownCounter(v) => {
                gauge!(prom_name).set(*v as f64);
            }
            MetricValue::Histogram { sum, .. } => {
                gauge!(prom_name).set(*sum);
            }
        }
    }
}
