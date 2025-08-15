use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

pub struct FileDeletionBenchmark {
    work_dir: PathBuf,
    data: Vec<u8>,
}

impl FileDeletionBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            data: Vec::new(),
        }
    }
}

impl Benchmark for FileDeletionBenchmark {
    fn name(&self) -> &str {
        "file-deletion"
    }

    fn description(&self) -> &str {
        "Deletes files"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("file_deletion");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        self.data = vec![0u8; config.size];
        for (i, byte) in self.data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        for i in 0..config.ops {
            let file_path = self.work_dir.join(format!("file_{:06}.dat", i));
            let mut file = File::create(&file_path)?;
            file.write_all(&self.data)?;
        }

        Ok(())
    }

    fn run_operation(&mut self, operation_id: usize) -> OperationResult {
        let file_path = self.work_dir.join(format!("file_{:06}.dat", operation_id));

        let start = Instant::now();

        let result = fs::remove_file(&file_path);

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
