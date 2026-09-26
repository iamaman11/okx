use base64::{Engine as _, engine::general_purpose::STANDARD};
use okx_protocol::{crypto::public_key_from_private, validate_agent_key_id};
use serde::Serialize;
use zeroize::Zeroize;

use crate::{
    AgentError, AgentResult,
    config::{AGENT_IDENTITY_SCHEMA_V1, DEFAULT_AGENT_KEY_ID},
};

const WINDOWS_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentIdentity {
    pub schema: &'static str,
    pub key_id: String,
    pub public_key: String,
}

impl AgentIdentity {
    pub fn from_private_key(key_id: &str, private_key: &[u8; 32]) -> AgentResult<Self> {
        validate_agent_key_id(key_id)?;
        let public_key = public_key_from_private(*private_key);

        Ok(Self {
            schema: AGENT_IDENTITY_SCHEMA_V1,
            key_id: key_id.to_owned(),
            public_key: STANDARD.encode(public_key),
        })
    }
}

pub fn initialize_native_identity(key_id: &str) -> AgentResult<AgentIdentity> {
    validate_agent_key_id(key_id)?;

    #[cfg(windows)]
    {
        let entry =
            keyring::Entry::new(WINDOWS_CREDENTIAL_SERVICE, key_id).map_err(secret_store_error)?;

        match entry.get_secret() {
            Ok(mut existing) => {
                existing.zeroize();
                return Err(AgentError::IdentityAlreadyExists(key_id.to_owned()));
            }
            Err(keyring::Error::NoEntry) => {}
            Err(error) => return Err(secret_store_error(error)),
        }

        let mut private_key = [0_u8; 32];
        getrandom::fill(&mut private_key).map_err(|error| AgentError::Random(error.to_string()))?;

        let identity = AgentIdentity::from_private_key(key_id, &private_key)?;
        let store_result = entry.set_secret(&private_key).map_err(secret_store_error);
        private_key.zeroize();
        store_result?;
        Ok(identity)
    }

    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

pub fn load_native_private_key(key_id: &str) -> AgentResult<[u8; 32]> {
    validate_agent_key_id(key_id)?;

    #[cfg(windows)]
    {
        let entry =
            keyring::Entry::new(WINDOWS_CREDENTIAL_SERVICE, key_id).map_err(secret_store_error)?;
        let mut secret = match entry.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => {
                return Err(AgentError::IdentityNotFound(key_id.to_owned()));
            }
            Err(error) => return Err(secret_store_error(error)),
        };

        if secret.len() != 32 {
            let len = secret.len();
            secret.zeroize();
            return Err(AgentError::InvalidPrivateKeyLength(len));
        }

        let mut private_key = [0_u8; 32];
        private_key.copy_from_slice(&secret);
        secret.zeroize();
        Ok(private_key)
    }

    #[cfg(not(windows))]
    {
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

pub fn load_native_identity(key_id: &str) -> AgentResult<AgentIdentity> {
    let mut private_key = load_native_private_key(key_id)?;
    let identity = AgentIdentity::from_private_key(key_id, &private_key);
    private_key.zeroize();
    identity
}

#[cfg(windows)]
fn secret_store_error(error: keyring::Error) -> AgentError {
    AgentError::SecretStore(error.to_string())
}

pub fn default_key_id() -> &'static str {
    DEFAULT_AGENT_KEY_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_public_identity_without_exposing_private_key() {
        let private_key = [7_u8; 32];
        let identity =
            AgentIdentity::from_private_key("agent-key-1", &private_key).expect("identity");
        let json = serde_json::to_string(&identity).expect("serialize");

        assert_eq!(identity.schema, "okx.agent.identity/v1");
        assert_eq!(identity.key_id, "agent-key-1");
        assert!(!identity.public_key.is_empty());
        assert!(!json.contains("private"));
    }

    #[cfg(not(windows))]
    #[test]
    fn native_store_fails_closed_off_windows() {
        assert!(matches!(
            load_native_private_key("agent-key-1"),
            Err(AgentError::UnsupportedSecretStorePlatform)
        ));
    }
}
