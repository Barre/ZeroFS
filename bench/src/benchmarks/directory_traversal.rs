use crate::benchmark::{Benchmark, BenchmarkConfig, OperationResult};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct DirectoryTraversalBenchmark {
    work_dir: PathBuf,
    hierarchy_root: PathBuf,
}

impl DirectoryTraversalBenchmark {
    pub fn new() -> Self {
        Self {
            work_dir: PathBuf::new(),
            hierarchy_root: PathBuf::new(),
        }
    }

    fn create_directory_hierarchy(
        parent: &Path,
        depth: usize,
        folders_per_level: usize,
        total_created: &mut usize,
        target_total: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if *total_created >= target_total {
            return Ok(());
        }

        for i in 0..folders_per_level {
            if *total_created >= target_total {
                break;
            }

            let dir_path = parent.join(format!("dir_{:03}_{:03}", depth, i));
            fs::create_dir(&dir_path)?;
            *total_created += 1;

            // Add some files to each directory
            for j in 0..3 {
                let file_path = dir_path.join(format!("file_{}.txt", j));
                let mut file = File::create(file_path)?;
                file.write_all(format!("test content at depth {} folder {}", depth, i).as_bytes())?;
            }

            // Recursively create subdirectories
            if depth > 0 && *total_created < target_total {
                Self::create_directory_hierarchy(
                    &dir_path,
                    depth - 1,
                    folders_per_level,
                    total_created,
                    target_total,
                )?;
            }
        }

        Ok(())
    }
}

impl Benchmark for DirectoryTraversalBenchmark {
    fn name(&self) -> &str {
        "directory-traversal"
    }

    fn description(&self) -> &str {
        "Traverses a hierarchy of 200 directories"
    }

    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.work_dir = config.work_dir.join("directory_traversal");
        if self.work_dir.exists() {
            fs::remove_dir_all(&self.work_dir)?;
        }
        fs::create_dir_all(&self.work_dir)?;

        self.hierarchy_root = self.work_dir.join("hierarchy_root");
        fs::create_dir(&self.hierarchy_root)?;

        // Create a hierarchy of approximately 200 folders
        // We'll use a balanced tree structure: depth 4, ~6 folders per level
        // This gives us approximately: 1 + 6 + 36 + 216 ≈ 259 folders (we'll limit to 200)
        let mut total_created = 1; // Count the root
        Self::create_directory_hierarchy(&self.hierarchy_root, 3, 6, &mut total_created, 200)?;

        Ok(())
    }

    fn run_operation(&mut self, _operation_id: usize) -> OperationResult {
        let start = Instant::now();

        let result = (|| -> Result<(usize, usize), Box<dyn std::error::Error>> {
            let mut total_files = 0;
            let mut total_dirs = 0;

            fn traverse_directory(
                path: &Path,
                files: &mut usize,
                dirs: &mut usize,
            ) -> Result<(), Box<dyn std::error::Error>> {
                for entry in fs::read_dir(path)? {
                    let entry = entry?;
                    let metadata = entry.metadata()?;

                    if metadata.is_file() {
                        *files += 1;
                    } else if metadata.is_dir() {
                        *dirs += 1;
                        traverse_directory(&entry.path(), files, dirs)?;
                    }
                }
                Ok(())
            }

            traverse_directory(&self.hierarchy_root, &mut total_files, &mut total_dirs)?;
            Ok((total_files, total_dirs))
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
