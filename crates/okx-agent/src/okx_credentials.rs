use crate::{AgentError, AgentResult};
use okx_api::{Credentials, OkxEnvironment};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};
#[cfg(windows)]
fn secret_store_error(error: keyring::Error) -> AgentError {
    AgentError::SecretStore(error.to_string())
}

/// Named account credentials live in the same native protected owner. The
/// catalog contains this reference only; it never contains secret material.
#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
struct NamedAccountSecrets {
    schema: String,
    environment: String,
    region: String,
    observer: SecretMaterial,
    executor: SecretMaterial,
}

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
struct SecretMaterial {
    api_key: String,
    secret_key: String,
    passphrase: String,
}

impl SecretMaterial {
    fn credentials(&self) -> AgentResult<Credentials> {
        Ok(Credentials::new(
            self.api_key.clone(),
            self.secret_key.clone(),
            self.passphrase.clone(),
        )?)
    }
}

pub struct NamedAccountCredentials {
    pub environment: OkxEnvironment,
    pub observer: Credentials,
    pub executor: Credentials,
}

fn decode_named_credentials(payload: &[u8]) -> AgentResult<NamedAccountCredentials> {
    use std::str::FromStr;
    let stored: NamedAccountSecrets =
        serde_json::from_slice(payload).map_err(|_| AgentError::InvalidOkxCredentials)?;
    if stored.schema != "okx.account-credentials/v1"
        || !matches!(stored.environment.as_str(), "real" | "demo")
    {
        return Err(AgentError::InvalidOkxCredentials);
    }
    let region =
        okx_api::Region::from_str(&stored.region).map_err(|_| AgentError::InvalidOkxCredentials)?;
    Ok(NamedAccountCredentials {
        environment: OkxEnvironment::new(region, stored.environment == "demo"),
        observer: stored.observer.credentials()?,
        executor: stored.executor.credentials()?,
    })
}

pub fn store_native_account_credentials(reference: &str, payload: &[u8]) -> AgentResult<()> {
    if !okx_protocol::accounts::valid_reference(reference) {
        return Err(AgentError::InvalidOkxCredentials);
    }
    let _validated = decode_named_credentials(payload)?;
    #[cfg(windows)]
    {
        let service = format!("iamaman11.okx-agent.account.{reference}");
        let entry = keyring::Entry::new(&service, "credentials").map_err(secret_store_error)?;
        entry.set_secret(payload).map_err(secret_store_error)?;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

pub fn load_native_account_credentials(reference: &str) -> AgentResult<NamedAccountCredentials> {
    if !okx_protocol::accounts::valid_reference(reference) {
        return Err(AgentError::InvalidOkxCredentials);
    }
    #[cfg(windows)]
    {
        let service = format!("iamaman11.okx-agent.account.{reference}");
        let entry = keyring::Entry::new(&service, "credentials").map_err(secret_store_error)?;
        let encoded = Zeroizing::new(match entry.get_secret() {
            Ok(value) => value,
            Err(keyring::Error::NoEntry) => return Err(AgentError::OkxCredentialsNotFound),
            Err(error) => return Err(secret_store_error(error)),
        });
        decode_named_credentials(&encoded)
    }
    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn secret_values_never_appear_in_errors() {
        let payload=br#"{"schema":"wrong","environment":"real","region":"global","observer":{"api_key":"test-key","secret_key":"sensitive-secret","passphrase":"sensitive-pass"},"executor":{"api_key":"test-key","secret_key":"sensitive-secret","passphrase":"sensitive-pass"}}"#;
        let error = decode_named_credentials(payload)
            .err()
            .expect("invalid schema");
        assert!(!error.to_string().contains("sensitive"));
    }
    #[test]
    fn named_bundle_is_strict_and_environment_specific() {
        for environment in ["real", "demo"] {
            let value = serde_json::json!({"schema":"okx.account-credentials/v1","environment":environment,"region":"global","observer":{"api_key":"key","secret_key":"secret","passphrase":"pass"},"executor":{"api_key":"key","secret_key":"secret","passphrase":"pass"}});
            assert_eq!(
                decode_named_credentials(&serde_json::to_vec(&value).unwrap())
                    .unwrap()
                    .environment
                    .demo,
                environment == "demo"
            );
            let mut invalid = value;
            invalid["legacy_profile"] = serde_json::json!("selected");
            assert!(decode_named_credentials(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
    }
}
