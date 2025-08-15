use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

pub struct DirectoryTraversalBenchmark {
    work_dir: PathBuf,
    test_dirs: Vec<PathBuf>,
}

impl DirectoryTraversalBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            test_dirs: Vec::new(),
        }
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

        self.test_dirs.clear();

        for i in 0..config.ops {
            let dir_path = self.work_dir.join(format!("test_{:04}", i));
            fs::create_dir(&dir_path)?;

            let subdirs = ["src", "docs", "tests", "config"];
            for subdir in &subdirs {
                let sub_path = dir_path.join(subdir);
                fs::create_dir(&sub_path)?;

                // Add some files to each subdirectory
                for j in 0..5 {
                    let file_path = sub_path.join(format!("file_{}.txt", j));
                    let mut file = File::create(file_path)?;
                    file.write_all(b"test content")?;
                }
            }

            // Add some files in the root
            for j in 0..3 {
                let file_path = dir_path.join(format!("root_{}.txt", j));
                let mut file = File::create(file_path)?;
                file.write_all(b"root file")?;
            }

            self.test_dirs.push(dir_path);
        }

        Ok(())
    }

    fn run_operation(&mut self, operation_id: usize) -> OperationResult {
        let dir_to_traverse = &self.test_dirs[operation_id];
        let start = Instant::now();

        let result = (|| -> Result<usize, Box<dyn std::error::Error>> {
            let mut total_size = 0;

            for entry in fs::read_dir(dir_to_traverse)? {
                let entry = entry?;
                let metadata = entry.metadata()?;

                if metadata.is_file() {
                    total_size += metadata.len() as usize;
                } else if metadata.is_dir() {
                    for subentry in fs::read_dir(entry.path())? {
                        let subentry = subentry?;
                        let submeta = subentry.metadata()?;
                        if submeta.is_file() {
                            total_size += submeta.len() as usize;
                        }
                    }
                }
            }

            Ok(total_size)
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
