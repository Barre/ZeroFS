use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

pub struct FileMovesBenchmark {
    work_dir: PathBuf,
    src_dir: PathBuf,
    dst_dir: PathBuf,
    data: Vec<u8>,
}

impl FileMovesBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            src_dir: PathBuf::new(),
            dst_dir: PathBuf::new(),
            data: Vec::new(),
        }
    }
}

impl Benchmark for FileMovesBenchmark {
    fn name(&self) -> &str {
        "file-moves"
    }

    fn description(&self) -> &str {
        "Moves files between directories"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("file_moves");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        self.src_dir = self.work_dir.join("src");
        self.dst_dir = self.work_dir.join("dst");
        fs::create_dir(&self.src_dir)?;
        fs::create_dir(&self.dst_dir)?;

        self.data = vec![0u8; config.size];
        for (i, byte) in self.data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        for i in 0..config.ops {
            let file_path = self.src_dir.join(format!("file_{:06}.dat", i));
            let mut file = File::create(&file_path)?;
            file.write_all(&self.data)?;
        }

        Ok(())
    }

    fn run_operation(&mut self, operation_id: usize) -> OperationResult {
        let src_path = self.src_dir.join(format!("file_{:06}.dat", operation_id));
        let dst_path = self.dst_dir.join(format!("file_{:06}.dat", operation_id));

        let start = Instant::now();

        let result = fs::rename(&src_path, &dst_path);

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
