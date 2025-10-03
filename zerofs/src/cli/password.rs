use crate::cli::server::build_slatedb;
use crate::config::Settings;
use crate::fs::CacheConfig;
use crate::key_management;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum PasswordError {
    #[error("Password cannot be empty")]
    EmptyPassword,
    #[error("Password must be at least 8 characters long")]
    TooShort,
    #[error("Please choose a secure password, not 'CHANGEME'")]
    DefaultPassword,
    #[error("Current password is still the default. Please update your config file first")]
    CurrentPasswordIsDefault,
    #[error("Failed to change encryption password: {0}")]
    EncryptionError(String),
    #[error("{0}")]
    Other(String),
}

pub fn validate_password(password: &str) -> Result<(), PasswordError> {
    if password.is_empty() {
        return Err(PasswordError::EmptyPassword);
    }
    if password.len() < 8 {
        return Err(PasswordError::TooShort);
    }
    if password == "CHANGEME" {
        return Err(PasswordError::DefaultPassword);
    }
    Ok(())
}

pub async fn change_password(
    settings: &Settings,
    new_password: String,
) -> Result<(), PasswordError> {
    let current_password = &settings.storage.encryption_password;

    if current_password == "CHANGEME" {
        return Err(PasswordError::CurrentPasswordIsDefault);
    }
    validate_password(&new_password)?;

    let mut env_vars = Vec::new();
    if let Some(aws) = &settings.aws {
        for (k, v) in &aws.0 {
            env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
        }
    }
    if let Some(azure) = &settings.azure {
        for (k, v) in &azure.0 {
            env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
        }
    }

    let (object_store, path_from_url) = object_store::parse_url_opts(
        &settings
            .storage
            .url
            .parse::<url::Url>()
            .map_err(|e| PasswordError::Other(e.to_string()))?,
        env_vars.into_iter(),
    )
    .map_err(|e| PasswordError::Other(e.to_string()))?;

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);
    let actual_db_path = path_from_url.to_string();

    let cache_config = CacheConfig {
        root_folder: settings.cache.dir.to_str().unwrap().to_string(),
        max_cache_size_gb: settings.cache.disk_size_gb,
        memory_cache_size_gb: settings.cache.memory_size_gb,
    };

    let slatedb = build_slatedb(object_store, &cache_config, actual_db_path)
        .await
        .map_err(|e| PasswordError::Other(e.to_string()))?;

    key_management::change_encryption_password(&slatedb, current_password, &new_password)
        .await
        .map_err(|e| PasswordError::EncryptionError(e.to_string()))?;

    slatedb
        .flush()
        .await
        .map_err(|e| PasswordError::Other(e.to_string()))?;
    slatedb
        .close()
        .await
        .map_err(|e| PasswordError::Other(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_password() {
        assert!(validate_password("").is_err());
        assert!(validate_password("short").is_err());
        assert!(validate_password("CHANGEME").is_err());
        assert!(validate_password("goodpassword123").is_ok());
    }
}
