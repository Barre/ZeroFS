use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use rand::Rng;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::Instant;

pub struct DataModificationsBenchmark {
    work_dir: PathBuf,
    file_path: PathBuf,
    modification_data: Vec<u8>,
    file_size: usize,
}

impl DataModificationsBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            file_path: PathBuf::new(),
            modification_data: Vec::new(),
            file_size: 0,
        }
    }
}

impl Benchmark for DataModificationsBenchmark {
    fn name(&self) -> &str {
        "data-modifications"
    }

    fn description(&self) -> &str {
        "Modifies random positions in an existing file"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("data_modifications");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        // 10x larger file for realistic random access patterns
        self.file_size = config.size * 10;
        self.file_path = self.work_dir.join("data.dat");

        let initial_data: Vec<u8> = (0..self.file_size).map(|i| (i % 256) as u8).collect();
        fs::write(&self.file_path, initial_data)?;

        self.modification_data = vec![0u8; config.size];
        let mut rng = rand::rng();
        rng.fill(&mut self.modification_data[..]);

        Ok(())
    }

    fn run_operation(&mut self, _operation_id: usize) -> OperationResult {
        let mut rng = rand::rng();
        let max_offset = self.file_size.saturating_sub(self.modification_data.len());
        let offset = if max_offset > 0 {
            rng.random_range(0..max_offset)
        } else {
            0
        };

        let start = Instant::now();

        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let mut file = OpenOptions::new().write(true).open(&self.file_path)?;
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(&self.modification_data)?;
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
