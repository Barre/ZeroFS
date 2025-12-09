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

    let env_vars = settings.cloud_provider_env_vars();

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
        root_folder: settings.cache.dir.clone(),
        max_cache_size_gb: settings.cache.disk_size_gb,
        memory_cache_size_gb: settings.cache.memory_size_gb,
    };

    let (slatedb, _, _) = build_slatedb(
        object_store,
        &cache_config,
        actual_db_path,
        crate::cli::server::DatabaseMode::ReadWrite,
        settings.lsm,
    )
    .await
    .map_err(|e| PasswordError::Other(e.to_string()))?;

    key_management::change_encryption_password(&slatedb, current_password, &new_password)
        .await
        .map_err(|e| PasswordError::EncryptionError(e.to_string()))?;

    match &slatedb {
        crate::encryption::SlateDbHandle::ReadWrite(db) => {
            db.flush()
                .await
                .map_err(|e| PasswordError::Other(e.to_string()))?;
            db.close()
                .await
                .map_err(|e| PasswordError::Other(e.to_string()))?;
        }
        crate::encryption::SlateDbHandle::ReadOnly(_) => {
            return Err(PasswordError::Other(
                "Cannot change password in read-only mode".to_string(),
            ));
        }
    }

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
