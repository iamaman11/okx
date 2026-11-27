use zeroize::{Zeroize, Zeroizing};

use crate::{HostControlError, HostControlResult};

#[cfg(windows)]
// Keep the already installed Credential Manager target across Cargo renames.
const WINDOWS_CREDENTIAL_SERVICE: &str = "iamaman11.okx-host-control.github";
#[cfg(windows)]
const WINDOWS_CREDENTIAL_ACCOUNT: &str = "control-token";
#[cfg(windows)]
const WINDOWS_ATTESTOR_CREDENTIAL_SERVICE: &str = "iamaman11.okx-host-control.attestor";
#[cfg(windows)]
const WINDOWS_ATTESTOR_CREDENTIAL_ACCOUNT: &str = "control-attestor-token";

/// This credential never falls back to the GitHub, Engine or Research bearers.
/// In an existing installation it is absent, and attestation remains disabled.
pub fn load_native_attestor_token() -> HostControlResult<Option<Zeroizing<String>>> {
    #[cfg(windows)]
    {
        let entry = credential_entry(
            WINDOWS_ATTESTOR_CREDENTIAL_SERVICE,
            WINDOWS_ATTESTOR_CREDENTIAL_ACCOUNT,
        )
        .map_err(secret_store_error)?;

        let secret = match entry.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => return Ok(None),
            Err(error) => return Err(secret_store_error(error)),
        };
        let mut secret = Zeroizing::new(secret);
        let token = match String::from_utf8(std::mem::take(&mut *secret)) {
            Ok(token) => token,
            Err(error) => {
                let mut bytes = error.into_bytes();
                bytes.zeroize();
                return Err(HostControlError::InvalidAttestorToken);
            }
        };
        let token = Zeroizing::new(token);
        validate_attestor_token(&token)?;
        Ok(Some(token))
    }

    #[cfg(not(windows))]
    {
        Ok(None)
    }
}

/// Explicit opt-in provisioning; never called by installation or a lifecycle operation.
pub fn store_native_attestor_token(token: &str) -> HostControlResult<()> {
    validate_attestor_token(token)?;

    #[cfg(windows)]
    {
        let entry = credential_entry(
            WINDOWS_ATTESTOR_CREDENTIAL_SERVICE,
            WINDOWS_ATTESTOR_CREDENTIAL_ACCOUNT,
        )
        .map_err(secret_store_error)?;
        let mut secret = token.as_bytes().to_vec();
        let result = entry.set_secret(&secret).map_err(secret_store_error);
        secret.zeroize();
        result?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        let _ = token;
        Err(HostControlError::UnsupportedPlatform)
    }
}

fn validate_attestor_token(token: &str) -> HostControlResult<()> {
    // Provision a cryptographically random 256-bit bearer as lowercase hex.
    // A fixed format cannot establish entropy; generation is an operator duty.
    if token.len() != 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HostControlError::InvalidAttestorToken);
    }
    Ok(())
}

pub fn store_native_github_token(token: &str) -> HostControlResult<()> {
    validate_token(token)?;

    #[cfg(windows)]
    {
        let entry = credential_entry(WINDOWS_CREDENTIAL_SERVICE, WINDOWS_CREDENTIAL_ACCOUNT)
            .map_err(secret_store_error)?;

        let mut secret = token.as_bytes().to_vec();
        let result = entry.set_secret(&secret).map_err(secret_store_error);
        secret.zeroize();
        result?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        let _ = token;
        Err(HostControlError::UnsupportedPlatform)
    }
}

pub fn load_native_github_token() -> HostControlResult<Zeroizing<String>> {
    #[cfg(windows)]
    {
        let entry = credential_entry(WINDOWS_CREDENTIAL_SERVICE, WINDOWS_CREDENTIAL_ACCOUNT)
            .map_err(secret_store_error)?;

        let secret = match entry.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => return Err(HostControlError::GithubTokenNotFound),
            Err(error) => return Err(secret_store_error(error)),
        };

        let token = match String::from_utf8(secret) {
            Ok(token) => token,
            Err(error) => {
                let mut bytes = error.into_bytes();
                bytes.zeroize();
                return Err(HostControlError::InvalidGithubToken);
            }
        };

        validate_token(&token)?;
        Ok(Zeroizing::new(token))
    }

    #[cfg(not(windows))]
    {
        Err(HostControlError::UnsupportedPlatform)
    }
}

#[cfg(windows)]
fn credential_entry(service: &str, user: &str) -> keyring::Result<keyring::Entry> {
    let store = windows_native_keyring_store::Store::new()?;
    entry_from_store(store.as_ref(), service, user)
}

#[cfg(windows)]
fn entry_from_store(
    store: &dyn keyring::api::CredentialStoreApi,
    service: &str,
    user: &str,
) -> keyring::Result<keyring::Entry> {
    store.build(service, user, None)
}

fn validate_token(token: &str) -> HostControlResult<()> {
    if token.is_empty()
        || token.len() > 1024
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(HostControlError::InvalidGithubToken);
    }
    Ok(())
}

#[cfg(windows)]
fn secret_store_error(error: keyring::Error) -> HostControlError {
    HostControlError::SecretStore(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn native_mapping_and_injected_store_preserve_control_token_entry() {
        let native = windows_native_keyring_store::Store::new().unwrap();
        assert_eq!(native.delimiters, ["", ".", ""]);
        let built = entry_from_store(
            native.as_ref(),
            WINDOWS_CREDENTIAL_SERVICE,
            WINDOWS_CREDENTIAL_ACCOUNT,
        )
        .unwrap();
        assert!(format!("{built:?}").contains("control-token.iamaman11.okx-host-control.github"));
        let mock = keyring::mock::Store::new().unwrap();
        let entry = entry_from_store(
            mock.as_ref(),
            WINDOWS_CREDENTIAL_SERVICE,
            WINDOWS_CREDENTIAL_ACCOUNT,
        )
        .unwrap();
        assert!(matches!(entry.get_secret(), Err(keyring::Error::NoEntry)));
        entry.set_secret(b"test-only-token").unwrap();
        let same = entry_from_store(
            mock.as_ref(),
            WINDOWS_CREDENTIAL_SERVICE,
            WINDOWS_CREDENTIAL_ACCOUNT,
        )
        .unwrap();
        assert_eq!(same.get_secret().unwrap(), b"test-only-token");
    }

    #[test]
    fn token_validation_is_strict() {
        assert!(validate_token("github_pat_example").is_ok());
        assert!(validate_token("").is_err());
        assert!(validate_token("token with whitespace").is_err());
        assert!(validate_attestor_token(&"0123456789abcdef".repeat(4)).is_ok());
        assert!(validate_attestor_token(&"a".repeat(32)).is_err());
        assert!(validate_attestor_token(&"a".repeat(65)).is_err());
        assert!(validate_attestor_token(&"F".repeat(64)).is_err());
        assert!(validate_attestor_token(&format!("{} ", "a".repeat(63))).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn attestor_uses_a_distinct_windows_credential_target() {
        assert_ne!(
            WINDOWS_ATTESTOR_CREDENTIAL_SERVICE,
            WINDOWS_CREDENTIAL_SERVICE
        );
        assert_ne!(
            WINDOWS_ATTESTOR_CREDENTIAL_ACCOUNT,
            WINDOWS_CREDENTIAL_ACCOUNT
        );
        let mock = keyring::mock::Store::new().unwrap();
        let attestor = entry_from_store(
            mock.as_ref(),
            WINDOWS_ATTESTOR_CREDENTIAL_SERVICE,
            WINDOWS_ATTESTOR_CREDENTIAL_ACCOUNT,
        )
        .unwrap();
        let github = entry_from_store(
            mock.as_ref(),
            WINDOWS_CREDENTIAL_SERVICE,
            WINDOWS_CREDENTIAL_ACCOUNT,
        )
        .unwrap();
        attestor
            .set_secret(b"test-attestor-secret-0000000000000")
            .unwrap();
        assert!(matches!(github.get_secret(), Err(keyring::Error::NoEntry)));
    }
}
