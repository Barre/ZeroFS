//! In-process storage for secrets.
//!
//! Passwords, key material, and the buffers they arrive through (config text,
//! stdin input) are held in memory that is locked against swapping, excluded
//! from core dumps, and erased before release. Short-lived intermediates that
//! cannot live there are erased on drop. Plaintext is reachable only through
//! an explicit `expose_secret` call.
//!
//! Stack state inside the crypto libraries (HKDF, cipher construction,
//! and Argon2 hold key-derived values in their own stack frames, are neither locked nor excluded
//! from dumps, and residue survives there until later calls overwrite it.
//! Closing the gap would mean running those derivations on locked stacks of our own.

use anyhow::{Context, Result};
use region::{Allocation, LockGuard, Protection};
use std::fmt;
use std::io::Read;
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, thiserror::Error)]
pub(crate) enum LockedMemoryError {
    #[error("failed to allocate secret memory: {0}")]
    Allocate(region::Error),
    #[error("failed to lock secret memory: {0}")]
    Lock(region::Error),
    #[cfg(target_os = "linux")]
    #[error("failed to exclude secret memory from core dumps: {0}")]
    DontDump(std::io::Error),
}

/// A dedicated virtual-memory allocation that is erased before it is unlocked
/// and unmapped.
struct LockedAllocation {
    // Fields drop in declaration order after `drop`: unlock, then unmap.
    _lock: LockGuard,
    allocation: Allocation,
    len: usize,
}

impl LockedAllocation {
    fn new(len: usize) -> Result<Self, LockedMemoryError> {
        let mut allocation = region::alloc(len.max(1), Protection::READ_WRITE)
            .map_err(LockedMemoryError::Allocate)?;
        let lock = region::lock(allocation.as_ptr::<u8>(), allocation.len())
            .map_err(LockedMemoryError::Lock)?;
        exclude_from_core_dumps(&mut allocation)?;
        Ok(Self {
            _lock: lock,
            allocation,
            len,
        })
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.allocation.as_ptr::<u8>(), self.len) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.allocation.as_mut_ptr::<u8>(), self.len) }
    }
}

impl Drop for LockedAllocation {
    fn drop(&mut self) {
        // This runs before `_lock` and `allocation` are dropped.
        zeroize_allocation(&mut self.allocation);
    }
}

// The allocation is uniquely owned and mutation requires exclusive access.
unsafe impl Send for LockedAllocation {}
unsafe impl Sync for LockedAllocation {}

/// A growable byte buffer that lives entirely in locked memory; growth
/// relocates into a larger locked allocation and the old one is wiped on drop.
struct LockedBuf {
    allocation: LockedAllocation,
    len: usize,
}

impl LockedBuf {
    fn with_capacity(capacity: usize) -> Result<Self, LockedMemoryError> {
        Ok(Self {
            allocation: LockedAllocation::new(capacity)?,
            len: 0,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn as_bytes(&self) -> &[u8] {
        &self.allocation.as_bytes()[..self.len]
    }

    /// Spare room of at least `additional` bytes, growing if needed.
    fn spare_mut(&mut self, additional: usize) -> Result<&mut [u8], LockedMemoryError> {
        let required = self
            .len
            .checked_add(additional)
            .expect("locked buffer length overflow");
        if required > self.allocation.len {
            let capacity = required.checked_next_power_of_two().unwrap_or(required);
            let mut replacement = LockedAllocation::new(capacity)?;
            replacement.as_bytes_mut()[..self.len].copy_from_slice(self.as_bytes());
            self.allocation = replacement;
        }
        Ok(&mut self.allocation.as_bytes_mut()[self.len..])
    }

    fn advance(&mut self, read: usize) {
        self.len += read;
        debug_assert!(self.len <= self.allocation.len);
    }

    fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), LockedMemoryError> {
        self.spare_mut(bytes.len())?[..bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(())
    }
}

pub(crate) struct EncryptionPassword(LockedText);

impl EncryptionPassword {
    pub(crate) fn try_new(secret: &str) -> Result<Self, LockedMemoryError> {
        LockedText::try_new(secret).map(Self)
    }

    /// Read one password line into locked memory without an unprotected copy.
    pub(crate) fn read_line(reader: &mut impl Read) -> Result<Self> {
        let mut buf = LockedBuf::with_capacity(256)?;
        loop {
            let start = buf.len();
            match reader.read(buf.spare_mut(256)?) {
                Ok(0) => break,
                Ok(read) => {
                    buf.advance(read);
                    if buf.as_bytes()[start..].contains(&b'\n') {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error).context("Failed to read password"),
            }
        }
        let bytes = buf.as_bytes();
        let line = match bytes.iter().position(|&byte| byte == b'\n') {
            Some(end) => &bytes[..end],
            None => bytes,
        };
        let line = std::str::from_utf8(line).context("Password is not valid UTF-8")?;
        Self::try_new(line.trim()).context("Failed to protect password")
    }

    /// Read the password from stdin, bypassing std's process-global stdin
    /// buffer, which would retain the line unwiped for the process lifetime.
    #[cfg(unix)]
    pub(crate) fn read_line_from_stdin() -> Result<Self> {
        use std::os::fd::FromRawFd;
        // Borrow fd 0; ManuallyDrop skips File's close on drop.
        let mut stdin = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(0) });
        Self::read_line(&mut *stdin)
    }

    #[cfg(not(unix))]
    pub(crate) fn read_line_from_stdin() -> Result<Self> {
        Self::read_line(&mut std::io::stdin().lock())
    }

    /// Expand shell-style environment references, assembling the result
    /// directly in locked memory.
    ///
    /// This preserves the syntax supported by the config's other expandable
    /// strings: `$VAR`, `${VAR}`, `${VAR:-default}`, and `$$` escaping.
    /// Environment values transit as short-lived zeroizing copies; their
    /// contents already live unprotected in `environ` regardless.
    pub(crate) fn expand_environment(self) -> Result<Self> {
        enum Piece<'a> {
            Literal(&'a str),
            Env(Zeroizing<String>),
        }

        let source = self.expose_secret();
        let Some(first_dollar) = source.find('$') else {
            return Ok(self);
        };

        let mut pieces = Vec::new();
        let mut remainder = source;
        let mut next_dollar = first_dollar;

        loop {
            pieces.push(Piece::Literal(&remainder[..next_dollar]));
            remainder = &remainder[next_dollar..];
            if remainder.is_empty() {
                break;
            }

            let after_dollar = &remainder[1..];
            match after_dollar.chars().next() {
                Some('{') => {
                    let Some(closing_brace) = remainder.find('}') else {
                        pieces.push(Piece::Literal("${"));
                        remainder = &remainder[2..];
                        next_dollar = remainder.find('$').unwrap_or(remainder.len());
                        continue;
                    };

                    let expression = &remainder[2..closing_brace];
                    let (name, default) = match expression.find(":-") {
                        Some(split) if split != 0 => {
                            (&expression[..split], Some(&expression[split + 2..]))
                        }
                        _ => (expression, None),
                    };

                    match std::env::var(name) {
                        Ok(value) => pieces.push(Piece::Env(Zeroizing::new(value))),
                        Err(_) if default.is_some() => {
                            pieces.push(Piece::Literal(default.expect("checked above")));
                        }
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!("Failed to expand environment variable `{name}`")
                            });
                        }
                    }

                    remainder = &remainder[closing_brace + 1..];
                }
                Some(character) if is_variable_character(character) => {
                    let name_len = after_dollar
                        .char_indices()
                        .take_while(|(_, character)| is_variable_character(*character))
                        .map(|(index, character)| index + character.len_utf8())
                        .last()
                        .expect("first character was checked");
                    let name = &after_dollar[..name_len];
                    let value = std::env::var(name).with_context(|| {
                        format!("Failed to expand environment variable `{name}`")
                    })?;
                    pieces.push(Piece::Env(Zeroizing::new(value)));
                    remainder = &after_dollar[name_len..];
                }
                Some('$') => {
                    pieces.push(Piece::Literal("$"));
                    remainder = &remainder[2..];
                }
                _ => {
                    pieces.push(Piece::Literal("$"));
                    remainder = after_dollar;
                }
            }

            next_dollar = remainder.find('$').unwrap_or(remainder.len());
        }

        let expanded_len: usize = pieces
            .iter()
            .map(|piece| match piece {
                Piece::Literal(text) => text.len(),
                Piece::Env(value) => value.len(),
            })
            .sum();
        let mut buf = LockedBuf::with_capacity(expanded_len)
            .context("Failed to protect encryption password in memory")?;
        for piece in &pieces {
            let bytes = match piece {
                Piece::Literal(text) => text.as_bytes(),
                Piece::Env(value) => value.as_bytes(),
            };
            buf.push_bytes(bytes)
                .context("Failed to protect encryption password in memory")?;
        }

        Ok(Self(LockedText::from_utf8(buf)?))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for EncryptionPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EncryptionPassword([REDACTED])")
    }
}

/// The password as parsed from the config. The deserialization layer captures
/// it straight into locked memory but must stay infallible, so a lock failure
/// is carried here instead of surfacing through toml's error rendering (which
/// echoes config source). `Settings::from_file` unwraps this.
pub(crate) enum CapturedPassword {
    Locked(EncryptionPassword),
    Failed(LockedMemoryError),
}

impl CapturedPassword {
    pub(crate) fn capture(secret: &str) -> Self {
        match EncryptionPassword::try_new(secret) {
            Ok(password) => Self::Locked(password),
            Err(error) => Self::Failed(error),
        }
    }
}

impl fmt::Debug for CapturedPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapturedPassword([REDACTED])")
    }
}

/// UTF-8 text pinned in locked memory (raw config contents, and the storage
/// behind [`EncryptionPassword`]).
pub(crate) struct LockedText(LockedBuf);

impl LockedText {
    fn try_new(secret: &str) -> Result<Self, LockedMemoryError> {
        let mut buf = LockedBuf::with_capacity(secret.len())?;
        buf.push_bytes(secret.as_bytes())?;
        Ok(Self(buf))
    }

    fn from_utf8(buf: LockedBuf) -> Result<Self> {
        std::str::from_utf8(buf.as_bytes()).context("Text is not valid UTF-8")?;
        Ok(Self(buf))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        std::str::from_utf8(self.0.as_bytes()).expect("constructed from validated UTF-8")
    }
}

impl fmt::Debug for LockedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LockedText([REDACTED])")
    }
}

/// Read a file directly into locked memory.
pub(crate) fn read_file_locked(path: &std::path::Path) -> Result<LockedText> {
    let mut file = std::fs::File::open(path)?;
    let size_hint = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
    let mut buf = LockedBuf::with_capacity(size_hint.saturating_add(1))?;
    loop {
        match file.read(buf.spare_mut(4096)?) {
            Ok(0) => break,
            Ok(read) => buf.advance(read),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    LockedText::from_utf8(buf)
}

/// A fixed-size secret buffer stored in a pointer-stable locked allocation.
///
/// Moving this value moves only the allocation handle, not the secret bytes.
pub(crate) struct SecretBytes<const N: usize>(LockedAllocation);

impl<const N: usize> SecretBytes<N> {
    pub(crate) fn zeroed() -> Result<Self, LockedMemoryError> {
        let mut allocation = LockedAllocation::new(N)?;
        allocation.as_bytes_mut().zeroize();
        Ok(Self(allocation))
    }

    pub(crate) fn expose_secret(&self) -> &[u8; N] {
        self.0
            .as_bytes()
            .try_into()
            .expect("locked allocation has the requested size")
    }

    pub(crate) fn expose_secret_mut(&mut self) -> &mut [u8; N] {
        self.0
            .as_bytes_mut()
            .try_into()
            .expect("locked allocation has the requested size")
    }
}

impl<const N: usize> fmt::Debug for SecretBytes<N> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretBytes([REDACTED])")
    }
}

/// A variable-size secret buffer that is erased on drop.
pub(crate) struct SecretVec(Zeroizing<Vec<u8>>);

impl SecretVec {
    pub(crate) fn new(value: Vec<u8>) -> Self {
        Self(Zeroizing::new(value))
    }

    pub(crate) fn expose_secret(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretVec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretVec([REDACTED])")
    }
}

fn is_variable_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn zeroize_allocation(allocation: &mut Allocation) {
    let allocation =
        unsafe { std::slice::from_raw_parts_mut(allocation.as_mut_ptr::<u8>(), allocation.len()) };
    allocation.zeroize();
}

#[cfg(target_os = "linux")]
fn exclude_from_core_dumps(allocation: &mut Allocation) -> Result<(), LockedMemoryError> {
    let result = unsafe {
        libc::madvise(
            allocation.as_mut_ptr::<libc::c_void>(),
            allocation.len(),
            libc::MADV_DONTDUMP,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(LockedMemoryError::DontDump(std::io::Error::last_os_error()))
    }
}

#[cfg(not(target_os = "linux"))]
fn exclude_from_core_dumps(_allocation: &mut Allocation) -> Result<(), LockedMemoryError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn moving_secret_bytes_does_not_move_the_secret_allocation() {
        let mut secret = SecretBytes::<32>::zeroed().unwrap();
        secret.expose_secret_mut().fill(0x5a);
        let address = secret.expose_secret().as_ptr();
        let moved = secret;
        assert_eq!(address, moved.expose_secret().as_ptr());
        assert_eq!(moved.expose_secret(), &[0x5a; 32]);
    }

    #[test]
    fn encryption_password_is_redacted() {
        let secret = EncryptionPassword::try_new("correct horse battery staple").unwrap();
        assert_eq!(secret.expose_secret(), "correct horse battery staple");
        assert_eq!(format!("{secret:?}"), "EncryptionPassword([REDACTED])");
    }

    #[test]
    fn password_input_is_trimmed_without_an_owned_plaintext_copy() {
        let mut input = Cursor::new(b"  correct horse battery staple  \n");
        let password = EncryptionPassword::read_line(&mut input).unwrap();
        assert_eq!(password.expose_secret(), "correct horse battery staple");
    }

    #[test]
    fn environment_expansion_preserves_config_syntax() {
        unsafe {
            std::env::set_var("ZEROFS_SECRET_EXPANSION_A", "alpha");
            std::env::set_var("ZEROFS_SECRET_EXPANSION_B", "a-longer-secret-value");
            std::env::remove_var("ZEROFS_SECRET_EXPANSION_MISSING");
        }

        let expanded = EncryptionPassword::try_new(
            "${ZEROFS_SECRET_EXPANSION_A}/$ZEROFS_SECRET_EXPANSION_B/$$/${ZEROFS_SECRET_EXPANSION_MISSING:-fallback}",
        )
        .unwrap()
        .expand_environment()
        .unwrap();

        assert_eq!(
            expanded.expose_secret(),
            "alpha/a-longer-secret-value/$/fallback"
        );
    }

    /// Differential parity with `shellexpand::env`, which still handles the
    /// config's non-secret fields; the two implementations must not drift.
    #[test]
    fn expansion_matches_shellexpand() {
        unsafe {
            std::env::set_var("ZEROFS_PARITY_SET", "value");
            std::env::set_var("ZEROFS_PARITY_EMPTY", "");
            std::env::set_var("ZEROFS_PARITY_µ", "mu");
            std::env::remove_var("ZEROFS_PARITY_UNSET");
        }

        let corpus = [
            "plain",
            "${ZEROFS_PARITY_SET}",
            "$ZEROFS_PARITY_SET",
            "${ZEROFS_PARITY_SET:-d}",
            "${ZEROFS_PARITY_EMPTY}",
            "$ZEROFS_PARITY_EMPTY",
            "${ZEROFS_PARITY_EMPTY:-default}",
            "${ZEROFS_PARITY_UNSET:-fallback}",
            "${ZEROFS_PARITY_UNSET:-}",
            "${ZEROFS_PARITY_UNSET:-$ZEROFS_PARITY_SET}",
            "$ZEROFS_PARITY_µ",
            "$$",
            "$$ZEROFS_PARITY_SET",
            "a$$b",
            "$",
            "tail$",
            "$-",
            "$ x",
            "${",
            "a${b",
            "${}",
            "${:-x}",
            "${ZEROFS_PARITY_UNSET}",
            "$ZEROFS_PARITY_UNSET",
            "$1UNSET_DIGIT",
            "${ZEROFS_PARITY_SET:-b:-c}",
            "${ZEROFS_PARITY_UNSET:-b:-c}",
            "${ZEROFS_PARITY_UNSET:-${ZEROFS_PARITY_SET}}",
            "pre${ZEROFS_PARITY_SET}mid$ZEROFS_PARITY_SET.end",
        ];

        for input in corpus {
            let ours = EncryptionPassword::try_new(input)
                .unwrap()
                .expand_environment();
            let theirs = shellexpand::env(input);
            match (&ours, &theirs) {
                (Ok(ours), Ok(theirs)) => {
                    assert_eq!(
                        ours.expose_secret(),
                        theirs.as_ref(),
                        "diverged on {input:?}"
                    )
                }
                (Err(_), Err(_)) => {}
                _ => panic!(
                    "divergence on {input:?}: ours={:?} theirs={theirs:?}",
                    ours.as_ref().map(|p| p.expose_secret())
                ),
            }
        }
    }

    #[test]
    fn long_password_lines_grow_the_locked_buffer() {
        let long = "x".repeat(1000);
        let mut input = Cursor::new(format!("  {long}  \n").into_bytes());
        let password = EncryptionPassword::read_line(&mut input).unwrap();
        assert_eq!(password.expose_secret(), long);
    }

    #[test]
    fn files_read_into_locked_memory() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "line one\nline two\n").unwrap();
        let text = read_file_locked(file.path()).unwrap();
        assert_eq!(text.expose_secret(), "line one\nline two\n");
    }
}
