use crate::error::ZeroFsError;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

/// POSIX caps a single path component at 255 bytes (NAME_MAX); the server
/// enforces the same limit.
pub(crate) const MAX_NAME_LEN: usize = 255;

/// Render a path for error payloads (lossy display; never fed back as input).
pub(crate) fn display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Split a path into validated byte name components. Paths are bytes (on the
/// 9P wire, in POSIX, and in this API), so UTF-8 is never required; rooted at
/// the attach point (a leading `/` is optional), `.` components are ignored,
/// `..` is rejected (no client-side normalization lies).
pub(crate) fn components(path: &Path) -> Result<Vec<&[u8]>, ZeroFsError> {
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                return Err(ZeroFsError::InvalidArgument {
                    message: format!("{path:?}: '..' path components are not supported"),
                });
            }
            Component::Normal(name) => {
                let bytes = os_bytes(name)?;
                validate_name(bytes, &name.to_string_lossy())?;
                out.push(bytes);
            }
            Component::Prefix(_) => {
                return Err(ZeroFsError::InvalidArgument {
                    message: format!("{path:?}: platform path prefixes are not supported"),
                });
            }
        }
    }
    Ok(out)
}

/// Borrow a path as wire bytes. Native Unix preserves arbitrary bytes; browser
/// paths are necessarily UTF-8 because they originate as JavaScript strings.
pub(crate) fn path_bytes(path: &Path) -> Result<&[u8], ZeroFsError> {
    os_bytes(path.as_os_str())
}

#[cfg(unix)]
fn os_bytes(value: &std::ffi::OsStr) -> Result<&[u8], ZeroFsError> {
    Ok(value.as_bytes())
}

#[cfg(not(unix))]
fn os_bytes(value: &std::ffi::OsStr) -> Result<&[u8], ZeroFsError> {
    value
        .to_str()
        .map(str::as_bytes)
        .ok_or_else(|| ZeroFsError::InvalidArgument {
            message: "browser paths must be valid UTF-8".to_string(),
        })
}

pub(crate) fn path_from_bytes(bytes: Vec<u8>) -> Result<std::path::PathBuf, ZeroFsError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(std::path::PathBuf::from(std::ffi::OsString::from_vec(
            bytes,
        )))
    }
    #[cfg(not(unix))]
    {
        String::from_utf8(bytes)
            .map(std::path::PathBuf::from)
            .map_err(|_| ZeroFsError::InvalidArgument {
                message: "server returned a non-UTF-8 path to a browser client".to_string(),
            })
    }
}

/// Validate one byte-exact name component: no `/`, no NUL, not `.`/`..`/empty,
/// at most 255 bytes.
pub(crate) fn validate_name(name: &[u8], display: &str) -> Result<(), ZeroFsError> {
    if name.is_empty() || name == b"." || name == b".." {
        return Err(ZeroFsError::InvalidArgument {
            message: format!("invalid name component: {display:?}"),
        });
    }
    if name.contains(&0) || name.contains(&b'/') {
        return Err(ZeroFsError::InvalidArgument {
            message: format!("name contains '/' or NUL: {display:?}"),
        });
    }
    if name.len() > MAX_NAME_LEN {
        return Err(ZeroFsError::NameTooLong {
            name: display.to_string(),
        });
    }
    Ok(())
}

/// Split validated components into (parent components, final name); errors on
/// the root (no final name to operate on). The name borrows the underlying
/// path data, so it outlives the component vector.
pub(crate) fn split_parent<'a, 'b>(
    names: &'b [&'a [u8]],
    path: &str,
) -> Result<(&'b [&'a [u8]], &'a [u8]), ZeroFsError> {
    names
        .split_last()
        .map(|(last, parents)| (parents, *last))
        .ok_or_else(|| ZeroFsError::InvalidArgument {
            message: format!("{path:?}: operation requires a path with a final name component"),
        })
}

/// Render byte components as a lossy display path for error context.
pub(crate) fn display_path(names: &[&[u8]]) -> String {
    let mut out = String::new();
    for n in names {
        out.push('/');
        out.push_str(&String::from_utf8_lossy(n));
    }
    if out.is_empty() { "/".to_string() } else { out }
}
