use okx_windows_secrets::{SecretStoreError, load_machine_secret, store_machine_secret};
use zeroize::{Zeroize, Zeroizing};

use crate::{AgentError, AgentResult};

#[cfg(windows)]
const WINDOWS_GITHUB_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.github";
#[cfg(windows)]
const WINDOWS_GITHUB_CREDENTIAL_ACCOUNT: &str = "mailbox-token";

const MACHINE_NAMESPACE: &str = "agent";
const MACHINE_GITHUB_TOKEN: &str = "github-token";

pub fn store_native_github_token(token: &str) -> AgentResult<()> {
    validate_token(token)?;

    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(
            WINDOWS_GITHUB_CREDENTIAL_SERVICE,
            WINDOWS_GITHUB_CREDENTIAL_ACCOUNT,
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
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

pub fn load_native_github_token() -> AgentResult<Zeroizing<String>> {
    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(
            WINDOWS_GITHUB_CREDENTIAL_SERVICE,
            WINDOWS_GITHUB_CREDENTIAL_ACCOUNT,
        )
        .map_err(secret_store_error)?;

        let secret = match entry.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => return Err(AgentError::GithubTokenNotFound),
            Err(error) => return Err(secret_store_error(error)),
        };

        decode_token(Zeroizing::new(secret))
    }

    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

pub fn migrate_github_token_to_machine() -> AgentResult<()> {
    let token = load_native_github_token()?;
    store_machine_secret(MACHINE_NAMESPACE, MACHINE_GITHUB_TOKEN, token.as_bytes())?;
    Ok(())
}

pub fn load_runtime_github_token() -> AgentResult<Zeroizing<String>> {
    match load_machine_secret(MACHINE_NAMESPACE, MACHINE_GITHUB_TOKEN) {
        Ok(secret) => decode_token(secret),
        Err(SecretStoreError::NotFound) => load_native_github_token(),
        Err(error) => Err(error.into()),
    }
}

fn decode_token(mut secret: Zeroizing<Vec<u8>>) -> AgentResult<Zeroizing<String>> {
    let bytes = std::mem::take(&mut *secret);
    let token = match String::from_utf8(bytes) {
        Ok(token) => token,
        Err(error) => {
            let mut bytes = error.into_bytes();
            bytes.zeroize();
            return Err(AgentError::InvalidGithubToken);
        }
    };
    validate_token(&token)?;
    Ok(Zeroizing::new(token))
}

fn validate_token(token: &str) -> AgentResult<()> {
    if token.is_empty()
        || token.len() > 1024
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(AgentError::InvalidGithubToken);
    }
    Ok(())
}

#[cfg(windows)]
fn secret_store_error(error: keyring::Error) -> AgentError {
    AgentError::SecretStore(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_validation_rejects_empty_or_whitespace() {
        assert!(matches!(
            validate_token(""),
            Err(AgentError::InvalidGithubToken)
        ));
        assert!(matches!(
            validate_token("token with space"),
            Err(AgentError::InvalidGithubToken)
        ));
        assert!(validate_token("github_pat_example").is_ok());
    }
}
