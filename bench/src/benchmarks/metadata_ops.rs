use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

pub struct MetadataOpsBenchmark {
    work_dir: PathBuf,
    file_paths: Vec<PathBuf>,
}

impl MetadataOpsBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            file_paths: Vec::new(),
        }
    }
}

impl Benchmark for MetadataOpsBenchmark {
    fn name(&self) -> &str {
        "metadata-ops"
    }

    fn description(&self) -> &str {
        "Gets file metadata (stat operations)"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("metadata_ops");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        self.file_paths.clear();
        let num_files = 100;

        for i in 0..num_files {
            let file_path = self.work_dir.join(format!("file_{:03}.dat", i));
            let size = (i + 1) * 1024; // Varying sizes
            let data = vec![0u8; size];

            let mut file = File::create(&file_path)?;
            file.write_all(&data)?;

            self.file_paths.push(file_path);
        }

        Ok(())
    }

    fn run_operation(&mut self, operation_id: usize) -> OperationResult {
        let file_index = operation_id % self.file_paths.len();
        let file_path = &self.file_paths[file_index];

        let start = Instant::now();

        let result = fs::metadata(file_path);

        let duration = start.elapsed();

        match result {
            Ok(metadata) => {
                // Access fields to ensure syscall completes
                let _ = metadata.len();
                let _ = metadata.is_file();
                let _ = metadata.modified();

                OperationResult {
                    duration,
                    success: true,
                    error: None,
                }
            }
            Err(e) => OperationResult {
                duration,
                success: false,
                error: Some(e.to_string()),
            },
        }
    }
}
