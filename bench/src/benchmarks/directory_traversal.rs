use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct DirectoryTraversalBenchmark {
    work_dir: PathBuf,
    tree_depth: usize,
    files_per_dir: usize,
}

impl DirectoryTraversalBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            tree_depth: 3,
            files_per_dir: 10,
        }
    }

    fn create_tree(&self, path: &Path, depth: usize) -> Result<(), Box<dyn std::error::Error>> {
        if depth == 0 {
            return Ok(());
        }

        for i in 0..self.files_per_dir {
            let file_path = path.join(format!("file_{}.txt", i));
            File::create(file_path)?;
        }

        for i in 0..3 {
            let dir_path = path.join(format!("dir_{}", i));
            fs::create_dir(&dir_path)?;
            self.create_tree(&dir_path, depth - 1)?;
        }

        Ok(())
    }

    fn count_entries(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
        let mut count = 0;

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            count += 1;

            if entry.file_type()?.is_dir() {
                count += Self::count_entries(&entry.path())?;
            }
        }

        Ok(count)
    }
}

impl Benchmark for DirectoryTraversalBenchmark {
    fn name(&self) -> &str {
        "directory-traversal"
    }

    fn description(&self) -> &str {
        "Traverses directory trees"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("directory_traversal");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        self.create_tree(&self.work_dir, self.tree_depth)?;

        Ok(())
    }

    fn run_operation(&mut self, _operation_id: usize) -> OperationResult {
        let start = Instant::now();

        let result = Self::count_entries(&self.work_dir);

        let duration = start.elapsed();

        match result {
            Ok(_count) => OperationResult {
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
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        Ok(())
    }
}
