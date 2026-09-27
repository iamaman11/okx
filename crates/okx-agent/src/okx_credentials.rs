use okx_api::Credentials;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::{AgentError, AgentResult};

pub const OKX_OBSERVER_CREDENTIAL_SCHEMA_V1: &str = "okx.observer-credentials/v1";

#[cfg(windows)]
const WINDOWS_OKX_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.okx";
#[cfg(windows)]
const WINDOWS_OKX_CREDENTIAL_ACCOUNT: &str = "observer-read-only";

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
struct StoredOkxCredentials {
    schema: String,
    api_key: String,
    secret_key: String,
    passphrase: String,
}

pub fn store_native_okx_credentials(payload: &str) -> AgentResult<()> {
    let stored: StoredOkxCredentials =
        serde_json::from_str(payload).map_err(|_| AgentError::InvalidOkxCredentials)?;
    validate_payload(&stored)?;

    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(
            WINDOWS_OKX_CREDENTIAL_SERVICE,
            WINDOWS_OKX_CREDENTIAL_ACCOUNT,
        )
        .map_err(secret_store_error)?;

        let encoded = Zeroizing::new(
            serde_json::to_vec(&stored).map_err(|_| AgentError::InvalidOkxCredentials)?,
        );
        entry.set_secret(&encoded).map_err(secret_store_error)?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        let _ = stored;
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

pub fn load_native_okx_credentials() -> AgentResult<Credentials> {
    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(
            WINDOWS_OKX_CREDENTIAL_SERVICE,
            WINDOWS_OKX_CREDENTIAL_ACCOUNT,
        )
        .map_err(secret_store_error)?;

        let encoded = match entry.get_secret() {
            Ok(secret) => Zeroizing::new(secret),
            Err(keyring::Error::NoEntry) => return Err(AgentError::OkxCredentialsNotFound),
            Err(error) => return Err(secret_store_error(error)),
        };
        let stored: StoredOkxCredentials =
            serde_json::from_slice(&encoded).map_err(|_| AgentError::InvalidOkxCredentials)?;
        validate_payload(&stored)?;

        Credentials::new(
            stored.api_key.clone(),
            stored.secret_key.clone(),
            stored.passphrase.clone(),
        )
        .map_err(AgentError::from)
    }

    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

fn validate_payload(stored: &StoredOkxCredentials) -> AgentResult<()> {
    if stored.schema != OKX_OBSERVER_CREDENTIAL_SCHEMA_V1 {
        return Err(AgentError::InvalidOkxCredentials);
    }

    Credentials::new(
        stored.api_key.clone(),
        stored.secret_key.clone(),
        stored.passphrase.clone(),
    )
    .map(|_| ())
    .map_err(AgentError::from)
}

#[cfg(windows)]
fn secret_store_error(error: keyring::Error) -> AgentError {
    AgentError::SecretStore(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_schema_is_strict_and_secrets_do_not_appear_in_errors() {
        let invalid = r#"{"schema":"wrong","api_key":"key","secret_key":"super-secret","passphrase":"pass"}"#;
        let error = store_native_okx_credentials(invalid).expect_err("invalid schema");
        let message = error.to_string();
        assert!(!message.contains("super-secret"));
        assert!(message.contains("invalid"));
    }

    #[test]
    fn payload_rejects_unknown_fields_before_native_storage() {
        let invalid = r#"{"schema":"okx.observer-credentials/v1","api_key":"key","secret_key":"secret","passphrase":"pass","trade":true}"#;
        assert!(matches!(
            store_native_okx_credentials(invalid),
            Err(AgentError::InvalidOkxCredentials)
        ));
    }
}
