use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct BenchmarkConfig {
    pub work_dir: std::path::PathBuf,
    pub ops: usize,
    pub size: usize,
}

#[derive(Clone, Debug)]
pub struct OperationResult {
    pub duration: Duration,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub name: String,
    pub total_ops: usize,
    pub successful_ops: usize,
    pub failed_ops: usize,
    pub total_duration: Duration,
    pub mean_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub ops_per_second: f64,
    pub errors: HashSet<String>,
}

pub trait Benchmark: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn setup(&mut self, config: &BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>>;
    fn run_operation(&mut self, operation_id: usize) -> OperationResult;
    fn finalize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            libc::sync();
        }
        Ok(())
    }
}
