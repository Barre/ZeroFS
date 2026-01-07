use anyhow::{Context, Result};
use std::io::BufRead;

mod block_transformer;
mod bucket_identity;
mod cache;
mod checkpoint_manager;
mod cli;
mod config;
mod db;
mod deku_bytes;
mod fs;
mod key_management;
mod nbd;
mod nfs;
mod ninep;
mod parse_object_store;
mod rpc;
mod storage_compatibility;
mod task;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod posix_tests;

#[cfg(feature = "failpoints")]
mod failpoints;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse_args();

    match cli.command {
        cli::Commands::Init { path } => {
            println!("Generating configuration file at: {}", path.display());
            config::Settings::write_default_config(&path)?;
            println!("Configuration file created successfully!");
            println!("Edit the file and run: zerofs run -c {}", path.display());
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
            cli::server::run_server(config, read_only, checkpoint, no_compactor).await?;
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
    }

    Ok(())
}
