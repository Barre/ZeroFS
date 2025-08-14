use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

pub struct SingleFileAppendBenchmark {
    work_dir: PathBuf,
    file_path: PathBuf,
    data: Vec<u8>,
    appends_since_sync: usize,
    sync_interval: usize,
}

impl SingleFileAppendBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            file_path: PathBuf::new(),
            data: Vec::new(),
            appends_since_sync: 0,
            sync_interval: 100,
        }
    }
}

impl Benchmark for SingleFileAppendBenchmark {
    fn name(&self) -> &str {
        "single-file-append"
    }

    fn description(&self) -> &str {
        "Appends data sequentially to a single file"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("single_file_append");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        self.file_path = self.work_dir.join("append.dat");
        fs::File::create(&self.file_path)?;

        self.data = vec![0u8; config.size];
        for (i, byte) in self.data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        self.appends_since_sync = 0;

        Ok(())
    }

    fn run_operation(&mut self, _operation_id: usize) -> OperationResult {
        let start = Instant::now();

        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let mut file = OpenOptions::new().append(true).open(&self.file_path)?;
            file.write_all(&self.data)?;

            self.appends_since_sync += 1;

            if self.appends_since_sync >= self.sync_interval {
                file.sync_data()?;
                self.appends_since_sync = 0;
            }

            Ok(())
        })();

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

    fn cleanup(&mut self, _config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        if self.appends_since_sync > 0 && self.file_path.exists() {
            let file = OpenOptions::new().append(true).open(&self.file_path)?;
            file.sync_data()?;
        }

        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        Ok(())
    }
}
