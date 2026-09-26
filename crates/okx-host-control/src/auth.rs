use okx_windows_secrets::{load_machine_secret, store_machine_secret};
use zeroize::{Zeroize, Zeroizing};

use crate::{HostControlError, HostControlResult};

#[cfg(windows)]
const WINDOWS_CREDENTIAL_SERVICE: &str = "iamaman11.okx-host-control.github";
#[cfg(windows)]
const WINDOWS_CREDENTIAL_ACCOUNT: &str = "control-token";

const MACHINE_NAMESPACE: &str = "host-control";
const MACHINE_GITHUB_TOKEN: &str = "github-token";

pub fn store_native_github_token(token: &str) -> HostControlResult<()> {
    validate_token(token)?;

    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(WINDOWS_CREDENTIAL_SERVICE, WINDOWS_CREDENTIAL_ACCOUNT)
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
        let entry = keyring::Entry::new(WINDOWS_CREDENTIAL_SERVICE, WINDOWS_CREDENTIAL_ACCOUNT)
            .map_err(secret_store_error)?;

        let secret = match entry.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => return Err(HostControlError::GithubTokenNotFound),
            Err(error) => return Err(secret_store_error(error)),
        };

        decode_token(secret)
    }

    #[cfg(not(windows))]
    {
        Err(HostControlError::UnsupportedPlatform)
    }
}

pub fn migrate_github_token_to_machine() -> HostControlResult<()> {
    let token = load_native_github_token()?;
    store_machine_secret(MACHINE_NAMESPACE, MACHINE_GITHUB_TOKEN, token.as_bytes())?;
    Ok(())
}

pub fn load_machine_github_token() -> HostControlResult<Zeroizing<String>> {
    let secret = load_machine_secret(MACHINE_NAMESPACE, MACHINE_GITHUB_TOKEN)?;
    decode_token(secret.to_vec())
}

fn decode_token(secret: Vec<u8>) -> HostControlResult<Zeroizing<String>> {
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

    #[test]
    fn token_validation_is_strict() {
        assert!(validate_token("github_pat_example").is_ok());
        assert!(validate_token("").is_err());
        assert!(validate_token("token with whitespace").is_err());
    }
}
