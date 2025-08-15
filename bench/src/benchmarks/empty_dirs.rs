use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

pub struct EmptyDirsBenchmark {
    work_dir: PathBuf,
}

impl EmptyDirsBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
        }
    }
}

impl Benchmark for EmptyDirsBenchmark {
    fn name(&self) -> &str {
        "empty-dirs"
    }

    fn description(&self) -> &str {
        "Creates empty directories"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("empty_dirs");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        Ok(())
    }

    fn run_operation(&mut self, operation_id: usize) -> OperationResult {
        let dir_path = self.work_dir.join(format!("dir_{:06}", operation_id));
        let start = Instant::now();

        let result = fs::create_dir(&dir_path);

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
