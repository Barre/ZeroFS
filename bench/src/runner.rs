use crate::benchmark::{Benchmark, BenchmarkConfig, BenchmarkResult};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashSet;
use std::time::{Duration, Instant};

pub struct BenchmarkRunner {
    benchmarks: Vec<Box<dyn Benchmark>>,
}

impl BenchmarkRunner {
    pub fn new() -> Self {
        Self {
            benchmarks: Vec::new(),
        }
    }

    pub fn register(&mut self, benchmark: Box<dyn Benchmark>) {
        self.benchmarks.push(benchmark);
    }

    pub fn run_single(
        &mut self,
        name: &str,
        config: &BenchmarkConfig,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let benchmark = self
            .benchmarks
            .iter_mut()
            .find(|b| b.name() == name)
            .ok_or_else(|| format!("Benchmark '{}' not found", name))?;

        execute_benchmark(benchmark.as_mut(), config)
    }

    pub fn run_all(&mut self, config: &BenchmarkConfig) -> Vec<BenchmarkResult> {
        let mut results = Vec::new();

        for benchmark in &mut self.benchmarks {
            match execute_benchmark(benchmark.as_mut(), config) {
                Ok(result) => results.push(result),
                Err(e) => {
                    eprintln!("Failed to run benchmark '{}': {}", benchmark.name(), e);
                }
            }
        }

        results
    }

    pub fn list_benchmarks(&self) -> Vec<(&str, &str)> {
        self.benchmarks
            .iter()
            .map(|b| (b.name(), b.description()))
            .collect()
    }
}

fn execute_benchmark(
    benchmark: &mut dyn Benchmark,
    config: &BenchmarkConfig,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    println!("\n=== {} ===", benchmark.name());
    println!("{}", benchmark.description());

    print!("Setting up... ");
    benchmark.setup(config)?;
    println!("done");

    let total_start = Instant::now();
    let pb = ProgressBar::new(config.ops as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let mut durations = Vec::new();
    let mut errors = HashSet::new();
    let mut successful_ops = 0;
    let mut failed_ops = 0;

    for i in 0..config.ops {
        let result = benchmark.run_operation(i);

        if result.success {
            successful_ops += 1;
            durations.push(result.duration);
        } else {
            failed_ops += 1;
            if let Some(error) = result.error {
                errors.insert(error);
            }
        }

        pb.inc(1);
    }

    benchmark.finalize()?;

    let total_duration = total_start.elapsed();
    pb.finish_with_message("Complete");
    let (mean_duration, min_duration, max_duration) = if !durations.is_empty() {
        let sum: Duration = durations.iter().sum();
        let mean = sum / durations.len() as u32;
        let min = *durations.iter().min().unwrap();
        let max = *durations.iter().max().unwrap();
        (mean, min, max)
    } else {
        (Duration::ZERO, Duration::ZERO, Duration::ZERO)
    };

    let ops_per_second = if total_duration.as_secs_f64() > 0.0 {
        successful_ops as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    Ok(BenchmarkResult {
        name: benchmark.name().to_string(),
        total_ops: config.ops,
        successful_ops,
        failed_ops,
        total_duration,
        mean_duration,
        min_duration,
        max_duration,
        ops_per_second,
        errors,
    })
}
