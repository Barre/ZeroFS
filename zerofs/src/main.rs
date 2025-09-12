use anyhow::Result;
use mimalloc::MiMalloc;
use std::io::BufRead;

mod bucket_identity;
mod cli;
mod config;
mod encryption;
mod fs;
mod key_management;
mod nbd;
mod nfs;
mod ninep;
mod parse_object_store;
mod storage_compatibility;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod posix_tests;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse_args();

    match cli.command {
        cli::Commands::Init { path } => {
            println!("Generating configuration file at: {}", path.display());
            config::Settings::write_default_config(path.to_str().unwrap())?;
            println!("Configuration file created successfully!");
            println!("Edit the file and run: zerofs run -c {}", path.display());
        }
        cli::Commands::ChangePassword { config } => {
            let settings = match config::Settings::from_file(config.to_str().unwrap()) {
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
                .unwrap();
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
        cli::Commands::Run { config } => {
            cli::server::run_server(config).await?;
        }
    }

    Ok(())
}
