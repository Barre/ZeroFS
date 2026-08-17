use crate::secrets::{EncryptionPassword, SecretBytes, SecretVec};
use crate::task::spawn_blocking_named;
use anyhow::{Context, Result};
use argon2::{Algorithm, Argon2, Params, Version, password_hash::SaltString};
use bytes::Bytes;
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use rand::{RngCore, thread_rng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ARGON2_MEM_COST: u32 = 65536;
const ARGON2_TIME_COST: u32 = 3;
const ARGON2_PARALLELISM: u32 = 4;

/// Filename for the wrapped encryption key in object store
const WRAPPED_KEY_FILENAME: &str = "zerofs.key";

#[derive(Serialize, Deserialize, Debug)]
pub struct WrappedDataKey {
    /// Salt for Argon2 password derivation
    pub salt: String,
    /// Nonce for XChaCha20-Poly1305 encryption of the DEK
    pub nonce: [u8; 24],
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
    fn derive_kek(&self, password: &str, salt: &SaltString) -> Result<SecretBytes<32>> {
        // Match PasswordHasher's salt decoding, but derive directly into a
        // locked output instead of creating a copyable PasswordHash value.
        let mut salt_bytes = [0u8; 64];
        let salt_bytes = salt
            .decode_b64(&mut salt_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode salt: {}", e))?;
        let mut kek = SecretBytes::zeroed().context("Failed to protect derived key in memory")?;
        self.argon2
            .hash_password_into(password.as_bytes(), salt_bytes, kek.expose_secret_mut())
            .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?;
        Ok(kek)
    }

    /// Generate a new data encryption key and wrap it with a password
    fn generate_and_wrap_key(&self, password: &str) -> Result<(WrappedDataKey, SecretBytes<32>)> {
        // Generate random DEK
        let mut dek = SecretBytes::zeroed().context("Failed to protect data key in memory")?;
        thread_rng().fill_bytes(dek.expose_secret_mut());

        // Generate random salt for password KDF
        let salt = SaltString::generate(&mut thread_rng());

        // Derive KEK from password
        let kek = self.derive_kek(password, &salt)?;

        // Generate random nonce for wrapping
        let mut nonce_bytes = [0u8; 24];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        // Encrypt DEK with KEK
        let cipher = XChaCha20Poly1305::new(Key::from_slice(kek.expose_secret()));
        let wrapped_dek = cipher
            .encrypt(nonce, dek.expose_secret().as_ref())
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
    fn unwrap_key(&self, password: &str, wrapped_key: &WrappedDataKey) -> Result<SecretBytes<32>> {
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
        let cipher = XChaCha20Poly1305::new(Key::from_slice(kek.expose_secret()));
        let nonce = XNonce::from_slice(&wrapped_key.nonce);

        let dek_vec = SecretVec::new(
            cipher
                .decrypt(nonce, wrapped_key.wrapped_dek.as_ref())
                .map_err(|_| {
                    anyhow::anyhow!("Failed to unwrap DEK: Invalid password or corrupted key")
                })?,
        );

        let mut dek = SecretBytes::zeroed().context("Failed to protect data key in memory")?;
        dek.expose_secret_mut()
            .copy_from_slice(dek_vec.expose_secret());
        Ok(dek)
    }

    /// Re-wrap a DEK with a new password (for password changes)
    fn rewrap_key(
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

        let mut nonce_bytes = [0u8; 24];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        let cipher = XChaCha20Poly1305::new(Key::from_slice(kek.expose_secret()));
        let wrapped_dek = cipher
            .encrypt(nonce, dek.expose_secret().as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to rewrap DEK: {}", e))?;

        Ok(WrappedDataKey {
            salt: salt.to_string(),
            nonce: nonce_bytes,
            wrapped_dek,
            version: 1,
        })
    }
}

/// Get the path for the wrapped key file in object store
fn wrapped_key_path(db_path: &Path) -> Path {
    let mut path = db_path.clone();
    path = path.join(WRAPPED_KEY_FILENAME);
    path
}

/// Load wrapped key from object store
pub async fn load_wrapped_key_from_object_store(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
) -> Result<Option<WrappedDataKey>> {
    let key_path = wrapped_key_path(db_path);

    match object_store.get(&key_path).await {
        Ok(result) => {
            let data = result.bytes().await?;
            let wrapped_key: WrappedDataKey = bincode::deserialize(&data)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize wrapped key: {}", e))?;
            Ok(Some(wrapped_key))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("Failed to load wrapped key: {}", e)),
    }
}

/// Save wrapped key to object store
pub async fn save_wrapped_key_to_object_store(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    wrapped_key: &WrappedDataKey,
) -> Result<()> {
    let key_path = wrapped_key_path(db_path);

    let serialized = bincode::serialize(wrapped_key)
        .map_err(|e| anyhow::anyhow!("Failed to serialize wrapped key: {}", e))?;

    object_store
        .put(&key_path, PutPayload::from(Bytes::from(serialized)))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to save wrapped key: {}", e))?;

    Ok(())
}

/// Load or initialize encryption key from object store.
///
/// This loads the wrapped encryption key from the object store and unwraps it
/// using the provided password. If no key exists, a new one is generated and
/// stored.
pub async fn load_or_init_encryption_key(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    password: EncryptionPassword,
    read_only: bool,
) -> Result<SecretBytes<32>> {
    if let Some(wrapped_key) = load_wrapped_key_from_object_store(object_store, db_path).await? {
        return unwrap_key_blocking(password, wrapped_key).await;
    }

    if read_only {
        return Err(anyhow::anyhow!(
            "Cannot initialize encryption key in read-only mode. Please initialize the database in read-write mode first."
        ));
    }

    // No key yet: generate a candidate, but commit it with a conditional create so
    // only ONE concurrent initializer wins. Nodes sharing a store MUST share one key
    // (else blocks written by one node can't be decrypted by the other); if we lose
    // the race, adopt the winner's key instead of keeping our own.
    let (password, generated) = spawn_blocking_named("argon2-generate", move || {
        let generated = KeyManager::new().generate_and_wrap_key(password.expose_secret());
        (password, generated)
    })
    .map_err(|e| anyhow::anyhow!("Failed to spawn task: {}", e))?
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?;
    let (wrapped_key, dek) = generated?;

    if save_wrapped_key_if_absent(object_store, db_path, &wrapped_key).await? {
        return Ok(dek);
    }

    // Lost the init race: another node wrote its key first. Load and use it so every
    // node that shares this store converges on a single key.
    let wrapped_key = load_wrapped_key_from_object_store(object_store, db_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("wrapped key disappeared right after a concurrent init"))?;
    unwrap_key_blocking(password, wrapped_key).await
}

/// Unwrap a wrapped DEK off the runtime (argon2 is CPU-heavy).
async fn unwrap_key_blocking(
    password: EncryptionPassword,
    wrapped_key: WrappedDataKey,
) -> Result<SecretBytes<32>> {
    spawn_blocking_named("argon2-unwrap", move || {
        KeyManager::new().unwrap_key(password.expose_secret(), &wrapped_key)
    })
    .map_err(|e| anyhow::anyhow!("Failed to spawn task: {}", e))?
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))?
}

/// Persist the wrapped key only if absent (atomic create). Ok(true) = we wrote it,
/// Ok(false) = another node beat us to it (so the caller must adopt that key).
async fn save_wrapped_key_if_absent(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    wrapped_key: &WrappedDataKey,
) -> Result<bool> {
    let key_path = wrapped_key_path(db_path);
    let serialized = bincode::serialize(wrapped_key)
        .map_err(|e| anyhow::anyhow!("Failed to serialize wrapped key: {}", e))?;
    match object_store
        .put_opts(
            &key_path,
            PutPayload::from(Bytes::from(serialized)),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(e) => Err(anyhow::anyhow!("Failed to save wrapped key: {}", e)),
    }
}

/// Change the password used to encrypt the DEK
pub async fn change_encryption_password(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    old_password: EncryptionPassword,
    new_password: EncryptionPassword,
) -> Result<()> {
    let key_manager = KeyManager::new();

    let wrapped_key = load_wrapped_key_from_object_store(object_store, db_path)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No encryption key found"))?;

    let new_wrapped_key = spawn_blocking_named("argon2-rewrap", move || {
        key_manager.rewrap_key(
            old_password.expose_secret(),
            new_password.expose_secret(),
            &wrapped_key,
        )
    })
    .map_err(|e| anyhow::anyhow!("Failed to spawn task: {}", e))?
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;

    save_wrapped_key_to_object_store(object_store, db_path, &new_wrapped_key).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::PasswordHasher;

    #[test]
    fn direct_kdf_matches_the_legacy_password_hash_path() {
        let key_manager = KeyManager::new();
        let password = "existing-volume-password";
        let salt = SaltString::encode_b64(b"fixed-salt-bytes").unwrap();

        let direct = key_manager.derive_kek(password, &salt).unwrap();
        let legacy = key_manager
            .argon2
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .hash
            .unwrap();

        assert_eq!(direct.expose_secret(), legacy.as_bytes());
    }

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

        assert_eq!(original_dek.expose_secret(), unwrapped_dek.expose_secret());
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

        assert_eq!(original_dek.expose_secret(), unwrapped_dek.expose_secret());
    }

    /// Two nodes initializing the same store concurrently must converge on ONE key.
    /// Before the conditional-create init they each generated + kept their own key,
    /// so blocks one wrote couldn't be decrypted by the other.
    #[tokio::test]
    async fn concurrent_init_converges_on_one_key() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db_path = Path::from("data");
        let pw = "shared-cluster-password";

        let (ka, kb) = tokio::join!(
            load_or_init_encryption_key(&store, &db_path, password(pw), false),
            load_or_init_encryption_key(&store, &db_path, password(pw), false),
        );
        let ka = ka.expect("node A init");
        let kb = kb.expect("node B init");
        assert_eq!(
            ka.expose_secret(),
            kb.expose_secret(),
            "concurrent initializers must converge on one key"
        );

        // A later loader gets that same committed key.
        let kc = load_or_init_encryption_key(&store, &db_path, password(pw), false)
            .await
            .expect("later load");
        assert_eq!(
            ka.expose_secret(),
            kc.expose_secret(),
            "a later load must return the committed key"
        );
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn password(value: &str) -> EncryptionPassword {
        EncryptionPassword::try_new(value).unwrap()
    }

    // Password rotation must preserve the DEK (data stays decryptable) and the old
    // password must stop working.
    #[tokio::test]
    async fn change_password_rotates_without_losing_the_dek() {
        let store = store();
        let db_path = Path::from("data");
        let (old_pw, new_pw) = ("old-pw-123", "new-pw-456");

        let dek = load_or_init_encryption_key(&store, &db_path, password(old_pw), false)
            .await
            .unwrap();

        change_encryption_password(&store, &db_path, password(old_pw), password(new_pw))
            .await
            .unwrap();

        assert!(
            load_or_init_encryption_key(&store, &db_path, password(old_pw), false)
                .await
                .is_err(),
            "the old password must stop unwrapping the key"
        );
        let dek2 = load_or_init_encryption_key(&store, &db_path, password(new_pw), false)
            .await
            .unwrap();
        assert_eq!(
            dek.expose_secret(),
            dek2.expose_secret(),
            "rotation must preserve the data key"
        );
    }

    #[tokio::test]
    async fn change_password_without_a_key_errors() {
        let store = store();
        assert!(
            change_encryption_password(&store, &Path::from("data"), password("a"), password("b"),)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn read_only_init_without_a_key_errors() {
        let store = store();
        let err = load_or_init_encryption_key(&store, &Path::from("data"), password("pw"), true)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("read-only"), "got: {err:#}");
    }

    #[tokio::test]
    async fn reload_with_wrong_password_errors() {
        let store = store();
        let db_path = Path::from("data");
        load_or_init_encryption_key(&store, &db_path, password("right"), false)
            .await
            .unwrap();
        assert!(
            load_or_init_encryption_key(&store, &db_path, password("wrong"), false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn load_wrapped_key_rejects_corrupt_bytes() {
        let store = store();
        let db_path = Path::from("data");
        store
            .put(
                &db_path.clone().join(WRAPPED_KEY_FILENAME),
                PutPayload::from(Bytes::from_static(b"not a bincode wrapped key")),
            )
            .await
            .unwrap();
        assert!(
            load_wrapped_key_from_object_store(&store, &db_path)
                .await
                .is_err()
        );
    }

    #[test]
    fn unwrap_rejects_an_unsupported_version() {
        let km = KeyManager::new();
        let (mut wrapped, _) = km.generate_and_wrap_key("pw").unwrap();
        wrapped.version = 2;
        let err = km.unwrap_key("pw", &wrapped).unwrap_err();
        assert!(format!("{err:#}").contains("version"), "got: {err:#}");
    }

    #[test]
    fn unwrap_rejects_a_corrupt_salt() {
        let km = KeyManager::new();
        let (mut wrapped, _) = km.generate_and_wrap_key("pw").unwrap();
        wrapped.salt = "###not-valid-b64###".to_string();
        assert!(km.unwrap_key("pw", &wrapped).is_err());
    }
}
