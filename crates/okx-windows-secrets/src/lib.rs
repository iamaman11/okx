use std::path::PathBuf;

use thiserror::Error;
use zeroize::Zeroizing;

pub const MACHINE_SECRET_ROOT: &str = r"C:\ProgramData\iamaman11\okx\secrets";

#[derive(Debug, Error)]
pub enum SecretStoreError {
    #[error("Windows machine secret store is unavailable on this platform")]
    UnsupportedPlatform,

    #[error("invalid secret namespace or name")]
    InvalidName,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Windows DPAPI call {api} failed with Win32 error {code}")]
    Dpapi { api: &'static str, code: u32 },

    #[error("Windows DPAPI returned a null output buffer")]
    NullDpapiBuffer,

    #[error("secret ACL hardening failed")]
    AclHardeningFailed,

    #[error("secret was not found")]
    NotFound,
}

pub type SecretStoreResult<T> = Result<T, SecretStoreError>;

pub fn store_machine_secret(namespace: &str, name: &str, secret: &[u8]) -> SecretStoreResult<()> {
    validate_component(namespace)?;
    validate_component(name)?;

    #[cfg(windows)]
    {
        windows::store_machine_secret(namespace, name, secret)
    }

    #[cfg(not(windows))]
    {
        let _ = secret;
        Err(SecretStoreError::UnsupportedPlatform)
    }
}

pub fn load_machine_secret(namespace: &str, name: &str) -> SecretStoreResult<Zeroizing<Vec<u8>>> {
    validate_component(namespace)?;
    validate_component(name)?;

    #[cfg(windows)]
    {
        windows::load_machine_secret(namespace, name)
    }

    #[cfg(not(windows))]
    {
        Err(SecretStoreError::UnsupportedPlatform)
    }
}

pub fn machine_secret_exists(namespace: &str, name: &str) -> SecretStoreResult<bool> {
    validate_component(namespace)?;
    validate_component(name)?;

    #[cfg(windows)]
    {
        Ok(secret_path(namespace, name).is_file())
    }

    #[cfg(not(windows))]
    {
        Err(SecretStoreError::UnsupportedPlatform)
    }
}

fn validate_component(value: &str) -> SecretStoreResult<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(SecretStoreError::InvalidName);
    }

    Ok(())
}

fn secret_path(namespace: &str, name: &str) -> PathBuf {
    PathBuf::from(MACHINE_SECRET_ROOT)
        .join(namespace)
        .join(format!("{name}.bin"))
}

#[cfg(windows)]
mod windows {
    use std::{
        fs,
        process::{Command, Stdio},
        ptr::null_mut,
        slice,
    };

    use windows_sys::Win32::{
        Foundation::{GetLastError, LocalFree},
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_LOCAL_MACHINE, CRYPTPROTECT_UI_FORBIDDEN,
            CryptProtectData, CryptUnprotectData,
        },
    };
    use zeroize::{Zeroize, Zeroizing};

    use super::{MACHINE_SECRET_ROOT, SecretStoreError, SecretStoreResult, secret_path};

    struct DpapiBlob(CRYPT_INTEGER_BLOB);

    impl Default for DpapiBlob {
        fn default() -> Self {
            Self(CRYPT_INTEGER_BLOB {
                cbData: 0,
                pbData: null_mut(),
            })
        }
    }

    impl Drop for DpapiBlob {
        fn drop(&mut self) {
            if !self.0.pbData.is_null() {
                unsafe {
                    let plaintext_or_ciphertext =
                        slice::from_raw_parts_mut(self.0.pbData, self.0.cbData as usize);
                    plaintext_or_ciphertext.zeroize();
                    LocalFree(self.0.pbData as isize);
                }
                self.0.pbData = null_mut();
                self.0.cbData = 0;
            }
        }
    }

    pub fn store_machine_secret(
        namespace: &str,
        name: &str,
        secret: &[u8],
    ) -> SecretStoreResult<()> {
        let protected = protect(secret, &entropy(namespace, name))?;
        let namespace_root = std::path::PathBuf::from(MACHINE_SECRET_ROOT).join(namespace);

        fs::create_dir_all(&namespace_root)?;
        harden_acl(&std::path::PathBuf::from(MACHINE_SECRET_ROOT))?;
        harden_acl(&namespace_root)?;

        let path = secret_path(namespace, name);
        let staging = path.with_extension("bin.new");

        fs::write(&staging, &protected)?;
        if path.exists() {
            fs::remove_file(&path)?;
        }
        fs::rename(&staging, &path)?;
        harden_acl(&path)?;
        Ok(())
    }

    pub fn load_machine_secret(
        namespace: &str,
        name: &str,
    ) -> SecretStoreResult<Zeroizing<Vec<u8>>> {
        let path = secret_path(namespace, name);
        if !path.is_file() {
            return Err(SecretStoreError::NotFound);
        }

        let protected = fs::read(path)?;
        let plaintext = unprotect(&protected, &entropy(namespace, name))?;
        Ok(Zeroizing::new(plaintext))
    }

    fn protect(data: &[u8], entropy: &[u8]) -> SecretStoreResult<Vec<u8>> {
        let mut input = CRYPT_INTEGER_BLOB {
            cbData: data.len() as u32,
            pbData: data.as_ptr() as *mut u8,
        };
        let mut entropy_blob = CRYPT_INTEGER_BLOB {
            cbData: entropy.len() as u32,
            pbData: entropy.as_ptr() as *mut u8,
        };
        let mut output = DpapiBlob::default();

        let success = unsafe {
            CryptProtectData(
                &mut input,
                null_mut(),
                &mut entropy_blob,
                null_mut(),
                null_mut(),
                CRYPTPROTECT_LOCAL_MACHINE | CRYPTPROTECT_UI_FORBIDDEN,
                &mut output.0,
            )
        };
        if success == 0 {
            return Err(SecretStoreError::Dpapi {
                api: "CryptProtectData",
                code: unsafe { GetLastError() },
            });
        }
        if output.0.pbData.is_null() {
            return Err(SecretStoreError::NullDpapiBuffer);
        }

        Ok(unsafe { slice::from_raw_parts(output.0.pbData, output.0.cbData as usize).to_vec() })
    }

    fn unprotect(data: &[u8], entropy: &[u8]) -> SecretStoreResult<Vec<u8>> {
        let mut input = CRYPT_INTEGER_BLOB {
            cbData: data.len() as u32,
            pbData: data.as_ptr() as *mut u8,
        };
        let mut entropy_blob = CRYPT_INTEGER_BLOB {
            cbData: entropy.len() as u32,
            pbData: entropy.as_ptr() as *mut u8,
        };
        let mut output = DpapiBlob::default();

        let success = unsafe {
            CryptUnprotectData(
                &mut input,
                null_mut(),
                &mut entropy_blob,
                null_mut(),
                null_mut(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output.0,
            )
        };
        if success == 0 {
            return Err(SecretStoreError::Dpapi {
                api: "CryptUnprotectData",
                code: unsafe { GetLastError() },
            });
        }
        if output.0.pbData.is_null() {
            return Err(SecretStoreError::NullDpapiBuffer);
        }

        Ok(unsafe {
            slice::from_raw_parts(output.0.pbData, output.0.cbData as usize).to_vec()
        })
    }

    fn entropy(namespace: &str, name: &str) -> Vec<u8> {
        format!("iamaman11/okx/windows-secret/v1/{namespace}/{name}").into_bytes()
    }

    fn harden_acl(path: &std::path::Path) -> SecretStoreResult<()> {
        let status = Command::new("icacls.exe")
            .arg(path)
            .args([
                "/inheritance:r",
                "/grant:r",
                "*S-1-5-18:(OI)(CI)F",
                "*S-1-5-32-544:(OI)(CI)F",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;

        if status.success() {
            Ok(())
        } else {
            Err(SecretStoreError::AclHardeningFailed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_strict_and_path_safe() {
        assert!(validate_component("host-control").is_ok());
        assert!(validate_component("github_token").is_ok());
        assert!(validate_component("../escape").is_err());
        assert!(validate_component("with space").is_err());
    }

    #[cfg(not(windows))]
    #[test]
    fn native_store_fails_closed_off_windows() {
        assert!(matches!(
            load_machine_secret("agent", "github-token"),
            Err(SecretStoreError::UnsupportedPlatform)
        ));
    }
}
