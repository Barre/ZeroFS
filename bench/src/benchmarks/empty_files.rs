use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::path::PathBuf;
use std::time::Instant;

pub struct EmptyFilesBenchmark {
    work_dir: PathBuf,
}

impl EmptyFilesBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
        }
    }
}

impl Benchmark for EmptyFilesBenchmark {
    fn name(&self) -> &str {
        "empty-files"
    }

    fn description(&self) -> &str {
        "Creates empty files"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("empty_files");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        Ok(())
    }

    fn run_operation(&mut self, operation_id: usize) -> OperationResult {
        let file_path = self.work_dir.join(format!("empty_{:06}.txt", operation_id));
        let start = Instant::now();

        let result = File::create(&file_path);

        let duration = start.elapsed();

        match result {
            Ok(_) => OperationResult {
                duration,
                success: true,
                error: None,
            },
            Err(e) => OperationResult {
                duration,
                success: false,
                error: Some(e.to_string()),
            },
        }
    }
}
