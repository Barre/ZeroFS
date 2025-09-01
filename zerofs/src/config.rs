use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashSet;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub cache: CacheConfig,
    pub storage: StorageConfig,
    pub servers: ServerConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub dir: PathBuf,
    pub disk_size_gb: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_size_gb: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub encryption_password: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NfsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ninep: Option<NinePConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbd: Option<NbdConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NfsConfig {
    #[serde(default = "default_nfs_addresses")]
    pub addresses: HashSet<SocketAddr>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NinePConfig {
    #[serde(default = "default_9p_addresses")]
    pub addresses: HashSet<SocketAddr>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    #[serde(default = "default_nbd_addresses")]
    pub addresses: HashSet<SocketAddr>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
}

#[derive(Debug, Serialize, Clone)]
pub struct AwsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AwsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AwsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct AzureConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AzureConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AzureConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

fn default_nfs_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        2049,
    ));
    set
}

fn default_9p_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        5564,
    ));
    set
}

fn default_nbd_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        10809,
    ));
    set
}

fn deserialize_expandable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_expandable_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_path<'de, D>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_hashmap<'de, D>(
    deserializer: D,
) -> Result<std::collections::HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
    map.into_iter()
        .map(|(k, v)| match shellexpand::env(&v) {
            Ok(expanded) => Ok((k, expanded.into_owned())),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

impl Settings {
    pub fn from_file(config_path: &str) -> Result<Self> {
        let content = fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {}", config_path))?;

        let settings: Settings = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", config_path))?;

        Ok(settings)
    }

    pub fn generate_default() -> Self {
        let mut aws_config = std::collections::HashMap::new();
        aws_config.insert(
            "access_key_id".to_string(),
            "${AWS_ACCESS_KEY_ID}".to_string(),
        );
        aws_config.insert(
            "secret_access_key".to_string(),
            "${AWS_SECRET_ACCESS_KEY}".to_string(),
        );

        let mut azure_config = std::collections::HashMap::new();
        azure_config.insert(
            "storage_account_name".to_string(),
            "${AZURE_STORAGE_ACCOUNT_NAME}".to_string(),
        );
        azure_config.insert(
            "storage_account_key".to_string(),
            "${AZURE_STORAGE_ACCOUNT_KEY}".to_string(),
        );

        Settings {
            cache: CacheConfig {
                dir: PathBuf::from("${HOME}/.cache/zerofs"),
                disk_size_gb: 10.0,
                memory_size_gb: Some(1.0),
            },
            storage: StorageConfig {
                url: "s3://your-bucket/zerofs-data".to_string(),
                encryption_password: "${ZEROFS_PASSWORD}".to_string(),
            },
            servers: ServerConfig {
                nfs: Some(NfsConfig {
                    addresses: default_nfs_addresses(),
                }),
                ninep: Some(NinePConfig {
                    addresses: default_9p_addresses(),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.9p.sock")),
                }),
                nbd: Some(NbdConfig {
                    addresses: default_nbd_addresses(),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.nbd.sock")),
                }),
            },
            aws: Some(AwsConfig(aws_config)),
            azure: None,
        }
    }

    pub fn write_default_config(path: &str) -> Result<()> {
        let default = Self::generate_default();
        let mut toml_string = toml::to_string_pretty(&default)?;

        toml_string.push_str("\n# Optional AWS S3 settings (uncomment to use):\n");
        toml_string.push_str(
            "# endpoint = \"https://s3.us-east-1.amazonaws.com\"  # For S3-compatible services\n",
        );
        toml_string.push_str("# default_region = \"us-east-1\"\n");
        toml_string.push_str("# allow_http = \"true\"  # For non-HTTPS endpoints\n");

        toml_string.push_str("\n# Optional Azure settings can be added to [azure] section\n");

        // Add commented-out Azure section
        toml_string.push_str("\n# [azure]\n");
        toml_string.push_str("# storage_account_name = \"${AZURE_STORAGE_ACCOUNT_NAME}\"\n");
        toml_string.push_str("# storage_account_key = \"${AZURE_STORAGE_ACCOUNT_KEY}\"\n");
        let commented = format!(
            "# ZeroFS Configuration File\n\
             # Generated by ZeroFS v{}\n\
             #\n\
             # ============================================================================\n\
             # ENVIRONMENT VARIABLE SUBSTITUTION\n\
             # ============================================================================\n\
             # This config file supports environment variable substitution.\n\
             # \n\
             # Supported syntax:\n\
             #   - ${{VAR}} or $VAR  : Environment variable substitution\n\
             # \n\
             # Examples:\n\
             #   encryption_password = \"${{ZEROFS_PASSWORD}}\"\n\
             #   dir = \"${{HOME}}/.cache/zerofs\"\n\
             #   access_key_id = \"${{AWS_ACCESS_KEY_ID}}\"\n\
             #\n\
             # All referenced environment variables must be set, or the config will fail to load.\n\
             #\n\
             # ============================================================================\n\
             # SERVER CONFIGURATION\n\
             # ============================================================================\n\
             # - To disable a server, remove or comment out its entire section\n\
             # - Unix sockets are optional for 9P and NBD servers\n\
             # - NFS only supports TCP connections\n\
             # - Each protocol supports multiple bind addresses\n\
             # \n\
             # Examples:\n\
             #   addresses = [\"127.0.0.1:2049\"]                  # IPv4 localhost only\n\
             #   addresses = [\"0.0.0.0:2049\"]                    # All IPv4 interfaces\n\
             #   addresses = [\"[::]:2049\"]                       # All IPv6 interfaces\n\
             #   addresses = [\"127.0.0.1:2049\", \"[::1]:2049\"]  # Both IPv4 and IPv6 localhost\n\
             #\n\
             # ============================================================================\n\
             # CLOUD STORAGE\n\
             # ============================================================================\n\
             # - For S3: Configure [aws] section with your credentials\n\
             # - For Azure: Configure [azure] section with your credentials\n\
             # - For local storage: Use file:// URLs (no cloud config needed)\n\
             # ============================================================================\n\
             \n{}",
            env!("CARGO_PKG_VERSION"),
            toml_string
        );

        fs::write(path, commented)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::NamedTempFile;

    #[test]
    fn test_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_PASSWORD", "secret123");
            env::set_var("ZEROFS_TEST_BUCKET", "my-bucket");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://${ZEROFS_TEST_BUCKET}/data"
encryption_password = "${ZEROFS_TEST_PASSWORD}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.storage.url, "s3://my-bucket/data");
        assert_eq!(settings.storage.encryption_password, "secret123");
    }

    #[test]
    fn test_home_env_var() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME", home_dir.to_str().unwrap());
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME}/test-cache"
disk_size_gb = 1.0

[storage]
url = "file://${ZEROFS_TEST_HOME}/data"
encryption_password = "test"

[servers]

[servers.ninep]
unix_socket = "${ZEROFS_TEST_HOME}/zerofs.sock"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(settings.cache.dir, home_dir.join("test-cache"));
        assert_eq!(
            settings.storage.url,
            format!("file://{}/data", home_dir.display())
        );
        if let Some(ninep) = settings.servers.ninep {
            assert_eq!(ninep.unix_socket.unwrap(), home_dir.join("zerofs.sock"));
        } else {
            panic!("Expected 9P config");
        }
    }

    #[test]
    fn test_undefined_env_var_error() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "${ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
        let error = format!("{:#}", result.unwrap_err());
        assert!(
            error.contains("ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST"),
            "Error was: {}",
            error
        );
    }

    #[test]
    fn test_mixed_expansion() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME_MIX", home_dir.to_str().unwrap());
            env::set_var("ZEROFS_TEST_DIR_MIX", "mydir");
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME_MIX}/${ZEROFS_TEST_DIR_MIX}/cache"
disk_size_gb = 1.0

[storage]
url = "file:///data"
encryption_password = "test"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.cache.dir, home_dir.join("mydir/cache"));
    }

    #[test]
    fn test_aws_azure_config_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_AWS_KEY", "aws123");
            env::set_var("ZEROFS_TEST_AWS_SECRET", "aws_secret");
            env::set_var("ZEROFS_TEST_AZURE_KEY", "azure456");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "${ZEROFS_TEST_AWS_KEY}"
secret_access_key = "${ZEROFS_TEST_AWS_SECRET}"

[azure]
storage_account_key = "${ZEROFS_TEST_AZURE_KEY}"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        let aws = settings.aws.unwrap();
        assert_eq!(aws.0.get("access_key_id").unwrap(), "aws123");
        assert_eq!(aws.0.get("secret_access_key").unwrap(), "aws_secret");

        let azure = settings.azure.unwrap();
        assert_eq!(azure.0.get("storage_account_key").unwrap(), "azure456");
    }

    #[test]
    fn test_aws_bool_values() {
        let config_with_bool = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "key"
allow_http = true
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_with_bool).unwrap();

        // This should fail because we can't deserialize a bool into a String
        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());

        // Now test with string "true"
        let config_with_string = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "key"
allow_http = "true"
"#;

        std::fs::write(temp_file.path(), config_with_string).unwrap();
        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_ok());
        let settings = result.unwrap();
        assert_eq!(settings.aws.unwrap().0.get("allow_http").unwrap(), "true");
    }
}
