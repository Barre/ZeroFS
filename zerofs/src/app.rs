use anyhow::{Context, Result};
use std::io::BufRead;

pub(crate) async fn run() -> Result<()> {
    let cli = crate::cli::Cli::parse_args();

    match cli.command {
        crate::cli::Commands::Init { path } => {
            if path.to_str() == Some("-") {
                // Write the config to stdout so it can be piped or redirected, e.g.:
                //   docker run --rm ghcr.io/barre/zerofs:latest init - > zerofs.toml
                print!("{}", crate::config::Settings::render_default_config()?);
            } else {
                eprintln!("Generating configuration file at: {}", path.display());
                crate::config::Settings::write_default_config(&path)?;
                eprintln!("Configuration file created successfully!");
                eprintln!("Edit the file and run: zerofs run -c {}", path.display());
            }
        }
        crate::cli::Commands::ChangePassword { config } => {
            let settings = match crate::config::Settings::from_file(&config) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("✗ Failed to load config: {e:#}");
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
            match crate::cli::password::change_password(&settings, new_password).await {
                Ok(()) => {
                    println!("✓ Encryption password changed successfully!");
                    println!(
                        "ℹ To use the new password, update your config file or environment variable"
                    );
                }
                Err(e) => {
                    eprintln!("✗ Error: {e}");
                    std::process::exit(1);
                }
            }
        }
        crate::cli::Commands::Run {
            config,
            read_only,
            checkpoint,
        } => {
            if let Err(e) = crate::cli::server::run_server(config, read_only, checkpoint).await {
                eprintln!("✗ Error: {e:#}");
                std::process::exit(1);
            }
        }
        crate::cli::Commands::Debug { subcommand } => match subcommand {
            crate::cli::DebugCommands::ListKeys { config } => {
                crate::cli::debug::list_keys(config).await?;
            }
        },
        crate::cli::Commands::Checkpoint { subcommand } => match subcommand {
            crate::cli::CheckpointCommands::Create { config, name } => {
                crate::cli::checkpoint::create_checkpoint(&config, &name).await?;
            }
            crate::cli::CheckpointCommands::List { config } => {
                crate::cli::checkpoint::list_checkpoints(&config).await?;
            }
            crate::cli::CheckpointCommands::Delete { config, name } => {
                crate::cli::checkpoint::delete_checkpoint(&config, &name).await?;
            }
            crate::cli::CheckpointCommands::Info { config, name } => {
                crate::cli::checkpoint::get_checkpoint_info(&config, &name).await?;
            }
        },
        crate::cli::Commands::Fatrace { config } => {
            crate::cli::fatrace::run_fatrace(config).await?;
        }
        crate::cli::Commands::Otrace { config } => {
            crate::cli::otrace::run_otrace(config).await?;
        }
        crate::cli::Commands::Flush { config } => {
            crate::cli::flush::flush(&config).await?;
        }
        crate::cli::Commands::Monitor { config, interval } => {
            crate::cli::monitor::run_monitor(config, interval).await?;
        }
        #[cfg(target_os = "linux")]
        crate::cli::Commands::Mount {
            target,
            mountpoint,
            read_only,
            access,
            msize,
            writeback,
            relaxed_consistency,
            aname,
        } => {
            let opts = crate::mount::MountOptions {
                msize,
                read_only,
                access,
                writeback,
                relaxed_consistency,
                aname: aname.unwrap_or_default(),
            };
            if let Err(e) = crate::mount::run(target, mountpoint, opts).await {
                eprintln!("✗ Error: {e:#}");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
