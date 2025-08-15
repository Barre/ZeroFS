use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use rand::Rng;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::Instant;

pub struct RandomReadsBenchmark {
    work_dir: PathBuf,
    file_path: PathBuf,
    file_size: usize,
    read_size: usize,
}

impl RandomReadsBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            file_path: PathBuf::new(),
            file_size: 0,
            read_size: 0,
        }
    }
}

impl Benchmark for RandomReadsBenchmark {
    fn name(&self) -> &str {
        "random-reads"
    }

    fn description(&self) -> &str {
        "Reads from random positions in a file"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("random_reads");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        // 100x larger file for realistic random access patterns
        self.file_size = config.size * 100;
        self.read_size = config.size;
        self.file_path = self.work_dir.join("data.dat");

        let data: Vec<u8> = (0..self.file_size).map(|i| (i % 256) as u8).collect();
        fs::write(&self.file_path, data)?;

        Ok(())
    }

    fn run_operation(&mut self, _operation_id: usize) -> OperationResult {
        let mut rng = rand::rng();
        let max_offset = self.file_size.saturating_sub(self.read_size);
        let offset = if max_offset > 0 {
            rng.random_range(0..max_offset)
        } else {
            0
        };

        let start = Instant::now();

        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let mut file = File::open(&self.file_path)?;
            let mut buffer = vec![0u8; self.read_size];
            file.seek(SeekFrom::Start(offset as u64))?;
            file.read_exact(&mut buffer)?;
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
}
