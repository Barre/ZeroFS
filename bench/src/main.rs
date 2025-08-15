use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

mod benchmark;
mod benchmarks;
mod formatter;
mod runner;

use benchmark::BenchmarkConfig;
use formatter::{CsvFormatter, HumanFormatter, JsonFormatter, ResultFormatter};
use runner::BenchmarkRunner;

#[derive(Parser)]
#[command(name = "bench")]
#[command(about = "File system benchmark tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run benchmarks
    Run {
        /// Directory where benchmark operations will be performed
        #[arg(short = 'd', long, default_value = "./bench_workspace")]
        work_dir: PathBuf,

        /// Number of operations to perform
        #[arg(short = 'n', long, default_value_t = 1000)]
        ops: usize,

        /// Size of data in bytes for write operations
        #[arg(short = 's', long, default_value_t = 4096)]
        size: usize,

        /// Output format
        #[arg(short = 'f', long, value_enum, default_value = "human")]
        format: OutputFormat,

        /// Specific benchmark to run (runs all if not specified)
        #[arg(short = 'b', long)]
        benchmark: Option<String>,

        /// List available benchmarks instead of running
        #[arg(short = 'l', long)]
        list: bool,
    },

    /// Clean up benchmark workspace
    Clean {
        /// Directory to clean
        #[arg(short = 'd', long, default_value = "./bench_workspace")]
        work_dir: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum OutputFormat {
    Human,
    Json,
    Csv,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            work_dir,
            ops,
            size,
            format,
            benchmark,
            list,
        } => {
            let mut runner = BenchmarkRunner::new();
            for bench in benchmarks::get_all_benchmarks() {
                runner.register(bench);
            }

            if list {
                println!("Available benchmarks:");
                for (name, description) in runner.list_benchmarks() {
                    println!("  {} - {}", name, description);
                }
                return Ok(());
            }

            fs::create_dir_all(&work_dir)?;

            let config = BenchmarkConfig {
                work_dir: work_dir.clone(),
                ops,
                size,
            };

            let results = if let Some(bench_name) = benchmark {
                vec![runner.run_single(&bench_name, &config)?]
            } else {
                runner.run_all(&config)
            };

            let formatter: Box<dyn ResultFormatter> = match format {
                OutputFormat::Human => Box::new(HumanFormatter),
                OutputFormat::Json => Box::new(JsonFormatter),
                OutputFormat::Csv => Box::new(CsvFormatter),
            };

            println!("{}", formatter.format(&results));

            if work_dir.exists() {
                fs::remove_dir_all(&work_dir)?;
            }
        }
        Commands::Clean { work_dir } => {
            if work_dir.exists() {
                println!("Cleaning workspace: {:?}", work_dir);
                fs::remove_dir_all(&work_dir)?;
                println!("Workspace cleaned successfully");
            } else {
                println!("Workspace {:?} does not exist", work_dir);
            }
        }
    }

    Ok(())
}
