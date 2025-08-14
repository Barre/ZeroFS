use crate::benchmark::BenchmarkResult;

pub trait ResultFormatter {
    fn format(&self, results: &[BenchmarkResult]) -> String;
}

pub struct HumanFormatter;

impl ResultFormatter for HumanFormatter {
    fn format(&self, results: &[BenchmarkResult]) -> String {
        let mut output = String::new();

        for result in results {
            output.push_str(&format!("\n{}\n", "=".repeat(60)));
            output.push_str(&format!("Benchmark: {}\n", result.name));
            output.push_str(&format!("{}\n", "-".repeat(60)));
            output.push_str(&format!("Total operations:    {}\n", result.total_ops));
            output.push_str(&format!("Successful:          {}\n", result.successful_ops));
            output.push_str(&format!("Failed:              {}\n", result.failed_ops));
            output.push_str(&format!(
                "Total time:          {:?}\n",
                result.total_duration
            ));
            output.push_str(&format!(
                "Mean time/op:        {:?}\n",
                result.mean_duration
            ));
            output.push_str(&format!("Min time:            {:?}\n", result.min_duration));
            output.push_str(&format!("Max time:            {:?}\n", result.max_duration));
            output.push_str(&format!(
                "Ops/second:          {:.2}\n",
                result.ops_per_second
            ));

            if !result.errors.is_empty() {
                output.push_str("\nErrors encountered:\n");
                for error in &result.errors {
                    output.push_str(&format!("  - {}\n", error));
                }
            }
            output.push_str(&format!("{}\n", "=".repeat(60)));
        }

        output
    }
}

pub struct JsonFormatter;

impl ResultFormatter for JsonFormatter {
    fn format(&self, results: &[BenchmarkResult]) -> String {
        serde_json::to_string_pretty(results)
            .unwrap_or_else(|e| format!("Failed to serialize results to JSON: {}", e))
    }
}

pub struct CsvFormatter;

impl ResultFormatter for CsvFormatter {
    fn format(&self, results: &[BenchmarkResult]) -> String {
        let mut output = String::new();

        output.push_str("Benchmark,Total Ops,Successful,Failed,Total Duration (ms),Mean Duration (μs),Min Duration (μs),Max Duration (μs),Ops/Second\n");

        for result in results {
            output.push_str(&format!(
                "{},{},{},{},{},{},{},{},{:.2}\n",
                result.name,
                result.total_ops,
                result.successful_ops,
                result.failed_ops,
                result.total_duration.as_millis(),
                result.mean_duration.as_micros(),
                result.min_duration.as_micros(),
                result.max_duration.as_micros(),
                result.ops_per_second
            ));
        }

        output
    }
}
