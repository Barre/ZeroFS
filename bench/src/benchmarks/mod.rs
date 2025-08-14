pub mod data_modifications;
pub mod directory_traversal;
pub mod empty_dirs;
pub mod empty_files;
pub mod file_deletion;
pub mod file_moves;
pub mod metadata_ops;
pub mod random_reads;
pub mod sequential_writes;
pub mod single_file_append;

use crate::benchmark::Benchmark;

pub fn get_all_benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![
        Box::new(sequential_writes::SequentialWritesBenchmark::new()),
        Box::new(data_modifications::DataModificationsBenchmark::new()),
        Box::new(single_file_append::SingleFileAppendBenchmark::new()),
        Box::new(empty_files::EmptyFilesBenchmark::new()),
        Box::new(empty_dirs::EmptyDirsBenchmark::new()),
        Box::new(file_moves::FileMovesBenchmark::new()),
        Box::new(random_reads::RandomReadsBenchmark::new()),
        Box::new(file_deletion::FileDeletionBenchmark::new()),
        Box::new(directory_traversal::DirectoryTraversalBenchmark::new()),
        Box::new(metadata_ops::MetadataOpsBenchmark::new()),
    ]
}
