use anyhow::{Context, Result};
use std::io::BufRead;

mod block_transformer;
mod bucket_identity;
mod checkpoint_manager;
mod cli;
mod config;
mod db;
mod dedup;
mod fs;
mod key_management;
mod length_checked_object_store;
#[cfg(target_os = "linux")]
mod mount;
mod nbd;
mod nfs;
mod ninep;
mod object_store_prefetch;
mod parse_object_store;
mod prometheus;
mod redis_conditional_store;
mod replication;
mod rpc;
mod segment_extractor;
mod storage_class_object_store;
mod storage_compatibility;
mod task;
mod telemetry;
#[cfg(feature = "webui")]
mod webui;

#[cfg(test)]
mod fault_store;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod posix_tests;

#[cfg(test)]
mod zerofs_client_tests;

#[cfg(feature = "failpoints")]
mod failpoints;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Install `ring` as the process-wide rustls crypto provider. We build rustls
    // without aws-lc-rs (it doesn't cross-compile to all our release targets), so
    // reqwest — pulled by object_store with `rustls-no-provider` — has no bundled
    // provider and needs the default set explicitly before any TLS connection.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = cli::Cli::parse_args();

    match cli.command {
        cli::Commands::Init { path } => {
            if path.to_str() == Some("-") {
                // Write the config to stdout so it can be piped or redirected, e.g.:
                //   docker run --rm ghcr.io/barre/zerofs:latest init - > zerofs.toml
                print!("{}", config::Settings::render_default_config()?);
            } else {
                eprintln!("Generating configuration file at: {}", path.display());
                config::Settings::write_default_config(&path)?;
                eprintln!("Configuration file created successfully!");
                eprintln!("Edit the file and run: zerofs run -c {}", path.display());
            }
        }
        cli::Commands::ChangePassword { config } => {
            let settings = match config::Settings::from_file(&config) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("✗ Failed to load config: {:#}", e);
                    std::process::exit(1);
                }
            };

            eprintln!("Reading new password from stdin...");
            let mut new_password = String::new();
            std::io::stdin()
                .lock()
                .read_line(&mut new_password)
                .context("Failed to read password from stdin")?;
            let new_password = new_password.trim().to_string();
            eprintln!("New password read successfully.");

            eprintln!("Changing encryption password...");
            match cli::password::change_password(&settings, new_password).await {
                Ok(()) => {
                    println!("✓ Encryption password changed successfully!");
                    println!(
                        "ℹ To use the new password, update your config file or environment variable"
                    );
                }
                Err(e) => {
                    eprintln!("✗ Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        cli::Commands::Run {
            config,
            read_only,
            checkpoint,
            no_compactor,
        } => {
            if let Err(e) =
                cli::server::run_server(config, read_only, checkpoint, no_compactor).await
            {
                eprintln!("✗ Error: {:#}", e);
                std::process::exit(1);
            }
        }
        cli::Commands::Debug { subcommand } => match subcommand {
            cli::DebugCommands::ListKeys { config } => {
                cli::debug::list_keys(config).await?;
            }
        },
        cli::Commands::Checkpoint { subcommand } => match subcommand {
            cli::CheckpointCommands::Create { config, name } => {
                cli::checkpoint::create_checkpoint(&config, &name).await?;
            }
            cli::CheckpointCommands::List { config } => {
                cli::checkpoint::list_checkpoints(&config).await?;
            }
            cli::CheckpointCommands::Delete { config, name } => {
                cli::checkpoint::delete_checkpoint(&config, &name).await?;
            }
            cli::CheckpointCommands::Info { config, name } => {
                cli::checkpoint::get_checkpoint_info(&config, &name).await?;
            }
        },
        cli::Commands::Fatrace { config } => {
            cli::fatrace::run_fatrace(config).await?;
        }
        cli::Commands::Compactor { config } => {
            cli::compactor::run_compactor(config).await?;
        }
        cli::Commands::Flush { config } => {
            cli::flush::flush(&config).await?;
        }
        cli::Commands::Monitor { config, interval } => {
            cli::monitor::run_monitor(config, interval).await?;
        }
        #[cfg(target_os = "linux")]
        cli::Commands::Mount {
            target,
            mountpoint,
            read_only,
            access,
            msize,
            writeback,
            relaxed_consistency,
            aname,
        } => {
            let opts = mount::MountOptions {
                msize,
                read_only,
                access,
                writeback,
                relaxed_consistency,
                aname: aname.unwrap_or_default(),
            };
            if let Err(e) = mount::run(target, mountpoint, opts).await {
                eprintln!("✗ Error: {:#}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
