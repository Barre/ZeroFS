use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::Settings;
use crate::key_management;
use crate::length_checked_object_store::LengthCheckedObjectStore;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use slatedb::BlockTransformer;
use slatedb::CompactorBuilder;
use slatedb::config::CompactorOptions;
use slatedb::object_store::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

/// Run standalone compactor for the database.
///
/// This runs compaction operations without starting a full ZeroFS server.
/// Use this to offload compaction to a separate instance from the writer.
/// The writer should be started with `--no-compactor` flag.
pub async fn run_compactor(config_path: PathBuf) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    info!("Starting standalone compactor");

    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let env_vars = settings.cloud_provider_env_vars();
    let (object_store, path_from_url) = parse_url_opts(&settings.storage.url.parse()?, env_vars)?;
    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LengthCheckedObjectStore::new(object_store));
    let db_path = Path::from(path_from_url.to_string());

    info!("Storage URL: {}", settings.storage.url);
    info!("DB Path: {}", db_path);

    let password = settings.storage.encryption_password.clone();
    super::password::validate_password(&password)
        .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

    info!("Loading encryption key from object store");
    let encryption_key =
        key_management::load_or_init_encryption_key(&object_store, &db_path, &password, true)
            .await?;

    let block_transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&encryption_key, settings.compression());

    let segments_enabled =
        crate::segment_extractor::should_enable_segments(&object_store, &db_path, None).await?;
    if segments_enabled {
        info!("Volume uses segment-oriented compaction (RFC-0024)");
    } else {
        info!("Volume uses legacy unsegmented layout");
    }

    let max_concurrent_compactions = settings
        .lsm
        .map(|c| c.max_concurrent_compactions())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS);

    info!("Max concurrent compactions: {}", max_concurrent_compactions);

    let scheduler_options: std::collections::HashMap<String, String> =
        slatedb::config::SizeTieredCompactionSchedulerOptions {
            max_compaction_sources: 16,
            ..Default::default()
        }
        .into();
    let compactor_options = CompactorOptions {
        poll_interval: std::time::Duration::from_secs(1),
        max_concurrent_compactions,
        max_sst_size: 1024 * 1024 * 1024,
        max_fetch_tasks: 8,
        sst_block_size: slatedb::SstBlockSize::Block32Kib,
        scheduler_options,
        ..Default::default()
    };

    let compactor = Arc::new(
        CompactorBuilder::new(db_path, object_store)
            .with_options(compactor_options)
            .with_block_transformer(block_transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies(segments_enabled))
            .build(),
    );

    let compactor_clone = compactor.clone();
    let mut compactor_task = tokio::spawn(async move { compactor_clone.run().await });

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating graceful shutdown...");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, initiating graceful shutdown...");
        }
        result = &mut compactor_task => {
            match result {
                Ok(Ok(())) => {
                    info!("Compactor exited normally");
                    return Ok(());
                }
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("Compactor error: {}", e));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Compactor task panicked: {}", e));
                }
            }
        }
    }

    info!("Stopping compactor...");
    compactor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to stop compactor: {}", e))?;

    // Wait for the run loop to fully exit before returning, so the runtime
    // isn't torn down while the loop is still winding down.
    match compactor_task.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(
            "Compactor run loop exited with error during shutdown: {}",
            e
        ),
        Err(e) => warn!("Compactor task failed to join during shutdown: {}", e),
    }

    info!("Compactor shutdown complete");

    Ok(())
}
