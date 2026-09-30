use zeroize::{Zeroize, Zeroizing};

use crate::{AgentError, AgentResult};

#[cfg(windows)]
const WINDOWS_CLOUDFLARE_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.cloudflare";
#[cfg(windows)]
const WINDOWS_CLOUDFLARE_CREDENTIAL_ACCOUNT: &str = "runtime-token";

pub fn store_native_cloudflare_token(token: &str) -> AgentResult<()> {
    validate_token(token)?;

    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(
            WINDOWS_CLOUDFLARE_CREDENTIAL_SERVICE,
            WINDOWS_CLOUDFLARE_CREDENTIAL_ACCOUNT,
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

pub fn load_native_cloudflare_token() -> AgentResult<Zeroizing<String>> {
    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(
            WINDOWS_CLOUDFLARE_CREDENTIAL_SERVICE,
            WINDOWS_CLOUDFLARE_CREDENTIAL_ACCOUNT,
        )
        .map_err(secret_store_error)?;

        let secret = match entry.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => return Err(AgentError::CloudflareTokenNotFound),
            Err(error) => return Err(secret_store_error(error)),
        };

        let token = match String::from_utf8(secret) {
            Ok(token) => token,
            Err(error) => {
                let mut bytes = error.into_bytes();
                bytes.zeroize();
                return Err(AgentError::InvalidCloudflareToken);
            }
        };
        validate_token(&token)?;
        Ok(Zeroizing::new(token))
    }

    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

fn validate_token(token: &str) -> AgentResult<()> {
    if token.len() < 32
        || token.len() > 1024
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(AgentError::InvalidCloudflareToken);
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
    fn token_validation_is_bounded_and_whitespace_free() {
        assert!(matches!(
            validate_token("short"),
            Err(AgentError::InvalidCloudflareToken)
        ));
        assert!(matches!(
            validate_token(&format!("{} {}", "a".repeat(32), "b")),
            Err(AgentError::InvalidCloudflareToken)
        ));
        assert!(validate_token(&"a".repeat(64)).is_ok());
    }
}
