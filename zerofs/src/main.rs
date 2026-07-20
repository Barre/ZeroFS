use anyhow::{Context, Result};
use std::io::BufRead;

mod block_transformer;
mod bucket_identity;
mod checkpoint_manager;
mod cli;
mod config;
mod db;
mod dedup;
mod frame_codec;
mod fs;
mod key_management;
mod length_checked_object_store;
mod metadata_digest;
#[cfg(target_os = "linux")]
mod mount;
mod nbd;
mod net_util;
mod nfs;
mod ninep;
mod object_store_prefetch;
mod object_trace;
mod parse_object_store;
mod prometheus;
mod redis_conditional_store;
mod replication;
mod retrying_object_store;
mod rpc;
mod segment;
mod segment_extractor;
mod segment_store;
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

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_eager_driver_handoff()
        .build()
        .context("Failed to build Tokio runtime")?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
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
        } => {
            if let Err(e) = cli::server::run_server(config, read_only, checkpoint).await {
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
        cli::Commands::Otrace { config } => {
            cli::otrace::run_otrace(config).await?;
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
