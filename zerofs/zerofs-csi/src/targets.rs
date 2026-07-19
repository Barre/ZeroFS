//! Shared parsing for comma-separated CSI endpoint parameters.

use ninep_client::Target;

/// Split a comma-separated parameter into trimmed, non-empty segments.
pub fn split(spec: &str) -> Vec<&str> {
    spec.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect()
}

/// Parse the `gateway` parameter using the shared 9P target grammar.
pub fn parse_9p_targets(spec: &str) -> Result<Vec<Target>, String> {
    Target::parse_list(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_trims_and_drops_empties() {
        assert_eq!(split("a:1"), vec!["a:1"]);
        assert_eq!(split(" a:1 , b:2 "), vec!["a:1", "b:2"]);
        assert_eq!(split("a:1, ,b:2,"), vec!["a:1", "b:2"]);
        assert!(split("  ,  ").is_empty());
    }
}
