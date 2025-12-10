use crate::encryption::SlateDbHandle;
use crate::fs::key_codec::SYSTEM_WRAPPED_ENCRYPTION_KEY;
use crate::task::spawn_blocking_named;
use anyhow::Result;
use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHasher, SaltString},
};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use rand::{RngCore, thread_rng};
use serde::{Deserialize, Serialize};

const ARGON2_MEM_COST: u32 = 65536;
const ARGON2_TIME_COST: u32 = 3;
const ARGON2_PARALLELISM: u32 = 4;

#[derive(Serialize, Deserialize, Debug)]
pub struct WrappedDataKey {
    /// Salt for Argon2 password derivation
    pub salt: String,
    /// Nonce for XChaCha20-Poly1305 encryption of the DEK
    pub nonce: [u8; 12],
    /// Encrypted data encryption key
    pub wrapped_dek: Vec<u8>,
    /// Version for future compatibility
    pub version: u32,
}

pub struct KeyManager {
    argon2: Argon2<'static>,
}

impl KeyManager {
    pub fn new() -> Self {
        let params = Params::new(ARGON2_MEM_COST, ARGON2_TIME_COST, ARGON2_PARALLELISM, None)
            .expect("Valid Argon2 parameters");

        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        Self { argon2 }
    }

    /// Derive a key encryption key (KEK) from a password
    fn derive_kek(&self, password: &str, salt: &SaltString) -> Result<[u8; 32]> {
        let password_hash = self
            .argon2
            .hash_password(password.as_bytes(), salt)
            .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?;

        // Extract the hash bytes
        let hash_bytes = password_hash
            .hash
            .ok_or_else(|| anyhow::anyhow!("No hash in password hash"))?;

        let mut kek = [0u8; 32];
        kek.copy_from_slice(&hash_bytes.as_bytes()[..32]);
        Ok(kek)
    }

    /// Generate a new data encryption key and wrap it with a password
    pub fn generate_and_wrap_key(&self, password: &str) -> Result<(WrappedDataKey, [u8; 32])> {
        // Generate random DEK
        let mut dek = [0u8; 32];
        thread_rng().fill_bytes(&mut dek);

        // Generate random salt for password KDF
        let salt = SaltString::generate(&mut thread_rng());

        // Derive KEK from password
        let kek = self.derive_kek(password, &salt)?;

        // Generate random nonce for wrapping
        let mut nonce_bytes = [0u8; 12];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Encrypt DEK with KEK
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&kek));
        let wrapped_dek = cipher
            .encrypt(nonce, dek.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to wrap DEK: {}", e))?;

        let wrapped_key = WrappedDataKey {
            salt: salt.to_string(),
            nonce: nonce_bytes,
            wrapped_dek,
            version: 1,
        };

        Ok((wrapped_key, dek))
    }

    /// Unwrap a data encryption key using a password
    pub fn unwrap_key(&self, password: &str, wrapped_key: &WrappedDataKey) -> Result<[u8; 32]> {
        if wrapped_key.version != 1 {
            return Err(anyhow::anyhow!(
                "Unsupported wrapped key version: {}",
                wrapped_key.version
            ));
        }

        // Parse salt
        let salt = SaltString::from_b64(&wrapped_key.salt)
            .map_err(|e| anyhow::anyhow!("Invalid salt: {}", e))?;

        // Derive KEK from password
        let kek = self.derive_kek(password, &salt)?;

        // Decrypt DEK with KEK
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&kek));
        let nonce = Nonce::from_slice(&wrapped_key.nonce);

        let dek_vec = cipher
            .decrypt(nonce, wrapped_key.wrapped_dek.as_ref())
            .map_err(|_| {
                anyhow::anyhow!("Failed to unwrap DEK: Invalid password or corrupted key")
            })?;

        let mut dek = [0u8; 32];
        dek.copy_from_slice(&dek_vec);
        Ok(dek)
    }

    /// Re-wrap a DEK with a new password (for password changes)
    pub fn rewrap_key(
        &self,
        old_password: &str,
        new_password: &str,
        wrapped_key: &WrappedDataKey,
    ) -> Result<WrappedDataKey> {
        // First unwrap with old password
        let dek = self.unwrap_key(old_password, wrapped_key)?;

        // Generate new salt and wrap with new password
        let salt = SaltString::generate(&mut thread_rng());
        let kek = self.derive_kek(new_password, &salt)?;

        let mut nonce_bytes = [0u8; 12];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let cipher = ChaCha20Poly1305::new(Key::from_slice(&kek));
        let wrapped_dek = cipher
            .encrypt(nonce, dek.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to rewrap DEK: {}", e))?;

        Ok(WrappedDataKey {
            salt: salt.to_string(),
            nonce: nonce_bytes,
            wrapped_dek,
            version: 1,
        })
    }
}

/// Load or initialize encryption key from database
pub async fn load_or_init_encryption_key(
    db_handle: &SlateDbHandle,
    password: &str,
) -> Result<[u8; 32]> {
    let key_manager = KeyManager::new();

    // Check if wrapped key exists in database
    let existing_key = match db_handle {
        SlateDbHandle::ReadWrite(db) => db.get(SYSTEM_WRAPPED_ENCRYPTION_KEY).await?,
        SlateDbHandle::ReadOnly(reader_swap) => {
            let reader = reader_swap.load();
            reader.get(SYSTEM_WRAPPED_ENCRYPTION_KEY).await?
        }
    };

    match existing_key {
        Some(data) => {
            // Key exists, unwrap it
            let wrapped_key: WrappedDataKey = bincode::deserialize(&data)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize wrapped key: {}", e))?;

            let password = password.to_string();
            spawn_blocking_named("argon2-unwrap", move || {
                key_manager.unwrap_key(&password, &wrapped_key)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
        }
        None => {
            // First time setup - generate new key
            if db_handle.is_read_only() {
                return Err(anyhow::anyhow!(
                    "Cannot initialize encryption key in read-only mode. Please initialize the database in read-write mode first."
                ));
            }

            let password = password.to_string();
            let (wrapped_key, dek) = spawn_blocking_named("argon2-generate", move || {
                key_manager.generate_and_wrap_key(&password)
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

            // Store wrapped key in database
            let serialized = bincode::serialize(&wrapped_key)
                .map_err(|e| anyhow::anyhow!("Failed to serialize wrapped key: {}", e))?;

            match db_handle {
                SlateDbHandle::ReadWrite(db) => {
                    db.put_with_options(
                        SYSTEM_WRAPPED_ENCRYPTION_KEY,
                        &serialized,
                        &slatedb::config::PutOptions::default(),
                        &slatedb::config::WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await?;
                }
                SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
            }

            Ok(dek)
        }
    }
}

/// Change the password used to encrypt the DEK
pub async fn change_encryption_password(
    db_handle: &SlateDbHandle,
    old_password: &str,
    new_password: &str,
) -> Result<()> {
    if db_handle.is_read_only() {
        return Err(anyhow::anyhow!("Cannot change password in read-only mode"));
    }

    let key_manager = KeyManager::new();

    // Load current wrapped key
    let data = match db_handle {
        SlateDbHandle::ReadWrite(db) => db.get(SYSTEM_WRAPPED_ENCRYPTION_KEY).await?,
        SlateDbHandle::ReadOnly(reader_swap) => {
            let reader = reader_swap.load();
            reader.get(SYSTEM_WRAPPED_ENCRYPTION_KEY).await?
        }
    }
    .ok_or_else(|| anyhow::anyhow!("No encryption key found in database"))?;

    let wrapped_key: WrappedDataKey = bincode::deserialize(&data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize wrapped key: {}", e))?;

    let old_password = old_password.to_string();
    let new_password = new_password.to_string();
    let new_wrapped_key = spawn_blocking_named("argon2-rewrap", move || {
        key_manager.rewrap_key(&old_password, &new_password, &wrapped_key)
    })
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

    let serialized = bincode::serialize(&new_wrapped_key)
        .map_err(|e| anyhow::anyhow!("Failed to serialize wrapped key: {}", e))?;

    match db_handle {
        SlateDbHandle::ReadWrite(db) => {
            db.put_with_options(
                SYSTEM_WRAPPED_ENCRYPTION_KEY,
                &serialized,
                &slatedb::config::PutOptions::default(),
                &slatedb::config::WriteOptions {
                    await_durable: false,
                },
            )
            .await?;
        }
        SlateDbHandle::ReadOnly(_) => unreachable!("Already checked read-only above"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_wrap_unwrap() {
        let key_manager = KeyManager::new();
        let password = "test_password_123!";

        // Generate and wrap key
        let (wrapped_key, original_dek) = key_manager
            .generate_and_wrap_key(password)
            .expect("Failed to generate and wrap key");

        // Unwrap key
        let unwrapped_dek = key_manager
            .unwrap_key(password, &wrapped_key)
            .expect("Failed to unwrap key");

        assert_eq!(original_dek, unwrapped_dek);
    }

    #[test]
    fn test_wrong_password() {
        let key_manager = KeyManager::new();
        let password = "correct_password";
        let wrong_password = "wrong_password";

        let (wrapped_key, _) = key_manager
            .generate_and_wrap_key(password)
            .expect("Failed to generate and wrap key");

        // Should fail with wrong password
        assert!(
            key_manager
                .unwrap_key(wrong_password, &wrapped_key)
                .is_err()
        );
    }

    #[test]
    fn test_password_change() {
        let key_manager = KeyManager::new();
        let old_password = "old_password";
        let new_password = "new_password";

        let (wrapped_key, original_dek) = key_manager
            .generate_and_wrap_key(old_password)
            .expect("Failed to generate and wrap key");

        // Change password
        let new_wrapped_key = key_manager
            .rewrap_key(old_password, new_password, &wrapped_key)
            .expect("Failed to rewrap key");

        // Old password should not work
        assert!(
            key_manager
                .unwrap_key(old_password, &new_wrapped_key)
                .is_err()
        );

        // New password should work
        let unwrapped_dek = key_manager
            .unwrap_key(new_password, &new_wrapped_key)
            .expect("Failed to unwrap with new password");

        assert_eq!(original_dek, unwrapped_dek);
    }
}
