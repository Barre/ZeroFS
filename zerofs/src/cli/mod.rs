use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub mod checkpoint;
pub mod debug;
pub mod fatrace;
pub mod password;
pub mod remove;
pub mod server;
pub mod truncate;
pub mod ls;

#[derive(Parser)]
#[command(name = "zerofs")]
#[command(author, version, about = "The Filesystem That Makes S3 your Primary Storage", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Generate a default configuration file
    Init {
        #[arg(default_value = "zerofs.toml")]
        path: PathBuf,
    },
    /// Run the filesystem server
    Run {
        #[arg(short, long)]
        config: PathBuf,
        /// Open the filesystem in read-only mode
        #[arg(long, conflicts_with = "checkpoint")]
        read_only: bool,
        /// Open from a specific checkpoint by name (read-only mode)
        #[arg(long, conflicts_with = "read_only")]
        checkpoint: Option<String>,
    },
    /// Change the encryption password
    ///
    /// Reads new password from stdin. Examples:
    ///
    /// echo "newpassword" | zerofs change-password -c config.toml
    ///
    /// zerofs change-password -c config.toml < password.txt
    ChangePassword {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Debug commands for inspecting the database
    Debug {
        #[command(subcommand)]
        subcommand: DebugCommands,
    },
    /// Checkpoint management commands
    Checkpoint {
        #[command(subcommand)]
        subcommand: CheckpointCommands,
    },
    /// Trace file system operations in real-time
    Fatrace {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Filesystem file operations via RPC
    Fs {
        #[command(subcommand)]
        subcommand: FsCommands,
    },
}

#[derive(Subcommand)]
pub enum FsCommands {
    /// Truncate or create a file via RPC
    Truncate {
        #[arg(short, long)]
        config: PathBuf,
        /// Path to the file to truncate (absolute or relative to root)
        path: String,
        /// Desired size in bytes
        size: u64,
    },
    /// Delete a file via RPC
    Delete {
        #[arg(short, long)]
        config: PathBuf,
        /// Path to the file to delete (absolute or relative to root)
        path: String,
    },
    /// List files in a directory via RPC
    Ls {
        #[arg(short, long)]
        config: PathBuf,
        /// Directory prefix to list (empty or "/" lists root)
        prefix: String,
    },
}

#[derive(Subcommand)]
pub enum DebugCommands {
    /// List all keys in the database
    ListKeys {
        #[arg(short, long)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum CheckpointCommands {
    /// Create a new checkpoint
    Create {
        #[arg(short, long)]
        config: PathBuf,
        /// Name for the checkpoint (must be unique)
        name: String,
    },
    /// List all checkpoints
    List {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Delete a checkpoint by name
    Delete {
        #[arg(short, long)]
        config: PathBuf,
        /// Checkpoint name to delete
        name: String,
    },
    /// Get checkpoint information
    Info {
        #[arg(short, long)]
        config: PathBuf,
        /// Checkpoint name to query
        name: String,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
