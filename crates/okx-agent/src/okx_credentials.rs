use okx_api::{Credentials, OkxEnvironment};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::{AgentError, AgentResult};

pub const OKX_OBSERVER_CREDENTIAL_SCHEMA_V1: &str = "okx.observer-credentials/v1";
pub const OKX_EXECUTOR_CREDENTIAL_SCHEMA_V1: &str = "okx.executor-credentials/v1";
pub const OKX_DEMO_OBSERVER_CREDENTIAL_SCHEMA_V1: &str = "okx.demo-observer-credentials/v1";
pub const OKX_DEMO_EXECUTOR_CREDENTIAL_SCHEMA_V1: &str = "okx.demo-executor-credentials/v1";

#[cfg(windows)]
const WINDOWS_OBSERVER_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.okx";
#[cfg(windows)]
const WINDOWS_OBSERVER_CREDENTIAL_ACCOUNT: &str = "observer-read-only";
#[cfg(windows)]
const WINDOWS_EXECUTOR_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.okx-executor";
#[cfg(windows)]
const WINDOWS_EXECUTOR_CREDENTIAL_ACCOUNT: &str = "executor-read-trade";
#[cfg(windows)]
const WINDOWS_DEMO_OBSERVER_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.okx-demo";
#[cfg(windows)]
const WINDOWS_DEMO_OBSERVER_CREDENTIAL_ACCOUNT: &str = "observer-read-only";
#[cfg(windows)]
const WINDOWS_DEMO_EXECUTOR_CREDENTIAL_SERVICE: &str = "iamaman11.okx-agent.okx-demo-executor";
#[cfg(windows)]
const WINDOWS_DEMO_EXECUTOR_CREDENTIAL_ACCOUNT: &str = "executor-read-trade";

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
struct StoredOkxCredentials {
    schema: String,
    api_key: String,
    secret_key: String,
    passphrase: String,
}

#[derive(Clone, Copy)]
enum CredentialProfile {
    Observer,
    Executor,
    DemoObserver,
    DemoExecutor,
}

impl CredentialProfile {
    const fn schema(self) -> &'static str {
        match self {
            Self::Observer => OKX_OBSERVER_CREDENTIAL_SCHEMA_V1,
            Self::Executor => OKX_EXECUTOR_CREDENTIAL_SCHEMA_V1,
            Self::DemoObserver => OKX_DEMO_OBSERVER_CREDENTIAL_SCHEMA_V1,
            Self::DemoExecutor => OKX_DEMO_EXECUTOR_CREDENTIAL_SCHEMA_V1,
        }
    }

    #[cfg(windows)]
    const fn service(self) -> &'static str {
        match self {
            Self::Observer => WINDOWS_OBSERVER_CREDENTIAL_SERVICE,
            Self::Executor => WINDOWS_EXECUTOR_CREDENTIAL_SERVICE,
            Self::DemoObserver => WINDOWS_DEMO_OBSERVER_CREDENTIAL_SERVICE,
            Self::DemoExecutor => WINDOWS_DEMO_EXECUTOR_CREDENTIAL_SERVICE,
        }
    }

    #[cfg(windows)]
    const fn account(self) -> &'static str {
        match self {
            Self::Observer => WINDOWS_OBSERVER_CREDENTIAL_ACCOUNT,
            Self::Executor => WINDOWS_EXECUTOR_CREDENTIAL_ACCOUNT,
            Self::DemoObserver => WINDOWS_DEMO_OBSERVER_CREDENTIAL_ACCOUNT,
            Self::DemoExecutor => WINDOWS_DEMO_EXECUTOR_CREDENTIAL_ACCOUNT,
        }
    }

    fn invalid(self) -> AgentError {
        match self {
            Self::Observer => AgentError::InvalidOkxCredentials,
            Self::Executor => AgentError::InvalidExecutorOkxCredentials,
            Self::DemoObserver => AgentError::InvalidDemoOkxCredentials,
            Self::DemoExecutor => AgentError::InvalidDemoExecutorOkxCredentials,
        }
    }

    fn not_found(self) -> AgentError {
        match self {
            Self::Observer => AgentError::OkxCredentialsNotFound,
            Self::Executor => AgentError::ExecutorOkxCredentialsNotFound,
            Self::DemoObserver => AgentError::DemoOkxCredentialsNotFound,
            Self::DemoExecutor => AgentError::DemoExecutorOkxCredentialsNotFound,
        }
    }
}

pub fn store_native_okx_credentials(payload: &str) -> AgentResult<()> {
    store_native_credentials(CredentialProfile::Observer, payload)
}

pub fn load_native_okx_credentials() -> AgentResult<Credentials> {
    load_native_credentials(CredentialProfile::Observer)
}

pub fn store_native_executor_okx_credentials(payload: &str) -> AgentResult<()> {
    store_native_credentials(CredentialProfile::Executor, payload)
}

pub fn load_native_executor_okx_credentials() -> AgentResult<Credentials> {
    load_native_credentials(CredentialProfile::Executor)
}

pub fn store_native_demo_okx_credentials(payload: &str) -> AgentResult<()> {
    store_native_credentials(CredentialProfile::DemoObserver, payload)
}

pub fn load_native_demo_okx_credentials() -> AgentResult<Credentials> {
    load_native_credentials(CredentialProfile::DemoObserver)
}

pub fn store_native_demo_executor_okx_credentials(payload: &str) -> AgentResult<()> {
    store_native_credentials(CredentialProfile::DemoExecutor, payload)
}

pub fn load_native_demo_executor_okx_credentials() -> AgentResult<Credentials> {
    load_native_credentials(CredentialProfile::DemoExecutor)
}

pub fn load_native_okx_credentials_for_environment(
    environment: OkxEnvironment,
) -> AgentResult<Credentials> {
    if environment.demo {
        load_native_demo_okx_credentials()
    } else {
        load_native_okx_credentials()
    }
}

pub fn load_native_executor_okx_credentials_for_environment(
    environment: OkxEnvironment,
) -> AgentResult<Credentials> {
    if environment.demo {
        load_native_demo_executor_okx_credentials()
    } else {
        load_native_executor_okx_credentials()
    }
}

fn store_native_credentials(profile: CredentialProfile, payload: &str) -> AgentResult<()> {
    let stored: StoredOkxCredentials =
        serde_json::from_str(payload).map_err(|_| profile.invalid())?;
    validate_payload(profile, &stored)?;

    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(profile.service(), profile.account())
            .map_err(secret_store_error)?;
        let encoded = Zeroizing::new(serde_json::to_vec(&stored).map_err(|_| profile.invalid())?);
        entry.set_secret(&encoded).map_err(secret_store_error)?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        let _ = stored;
        Err(AgentError::UnsupportedSecretStorePlatform)
    }
}

fn load_native_credentials(profile: CredentialProfile) -> AgentResult<Credentials> {
    #[cfg(windows)]
    {
        let entry = keyring::Entry::new(profile.service(), profile.account())
            .map_err(secret_store_error)?;

        let encoded = match entry.get_secret() {
            Ok(secret) => Zeroizing::new(secret),
            Err(keyring::Error::NoEntry) => return Err(profile.not_found()),
            Err(error) => return Err(secret_store_error(error)),
        };
        let stored: StoredOkxCredentials =
            serde_json::from_slice(&encoded).map_err(|_| profile.invalid())?;
        validate_payload(profile, &stored)?;

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

fn validate_payload(profile: CredentialProfile, stored: &StoredOkxCredentials) -> AgentResult<()> {
    if stored.schema != profile.schema() {
        return Err(profile.invalid());
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
    fn production_and_demo_credential_schemas_are_not_interchangeable() {
        let observer = r#"{"schema":"okx.observer-credentials/v1","api_key":"key","secret_key":"secret","passphrase":"pass"}"#;
        let executor = r#"{"schema":"okx.executor-credentials/v1","api_key":"key","secret_key":"secret","passphrase":"pass"}"#;
        let demo_observer = r#"{"schema":"okx.demo-observer-credentials/v1","api_key":"key","secret_key":"secret","passphrase":"pass"}"#;
        let demo_executor = r#"{"schema":"okx.demo-executor-credentials/v1","api_key":"key","secret_key":"secret","passphrase":"pass"}"#;

        let observer_stored: StoredOkxCredentials =
            serde_json::from_str(observer).expect("observer");
        let executor_stored: StoredOkxCredentials =
            serde_json::from_str(executor).expect("executor");
        let demo_observer_stored: StoredOkxCredentials =
            serde_json::from_str(demo_observer).expect("demo observer");
        let demo_executor_stored: StoredOkxCredentials =
            serde_json::from_str(demo_executor).expect("demo executor");

        assert!(validate_payload(CredentialProfile::Observer, &observer_stored).is_ok());
        assert!(validate_payload(CredentialProfile::Executor, &executor_stored).is_ok());
        assert!(validate_payload(CredentialProfile::DemoObserver, &demo_observer_stored).is_ok());
        assert!(validate_payload(CredentialProfile::DemoExecutor, &demo_executor_stored).is_ok());

        assert!(matches!(
            validate_payload(CredentialProfile::Observer, &demo_observer_stored),
            Err(AgentError::InvalidOkxCredentials)
        ));
        assert!(matches!(
            validate_payload(CredentialProfile::Executor, &demo_executor_stored),
            Err(AgentError::InvalidExecutorOkxCredentials)
        ));
        assert!(matches!(
            validate_payload(CredentialProfile::DemoObserver, &observer_stored),
            Err(AgentError::InvalidDemoOkxCredentials)
        ));
        assert!(matches!(
            validate_payload(CredentialProfile::DemoExecutor, &executor_stored),
            Err(AgentError::InvalidDemoExecutorOkxCredentials)
        ));
    }

    #[test]
    fn secret_values_never_appear_in_schema_errors() {
        let stored = StoredOkxCredentials {
            schema: "wrong".to_owned(),
            api_key: "key".to_owned(),
            secret_key: "super-secret".to_owned(),
            passphrase: "super-pass".to_owned(),
        };
        let error =
            validate_payload(CredentialProfile::Executor, &stored).expect_err("invalid schema");
        let message = error.to_string();
        assert!(!message.contains("super-secret"));
        assert!(!message.contains("super-pass"));
    }

    #[test]
    fn payload_rejects_unknown_fields_before_native_storage() {
        let invalid = r#"{"schema":"okx.executor-credentials/v1","api_key":"key","secret_key":"secret","passphrase":"pass","withdraw":false}"#;
        assert!(serde_json::from_str::<StoredOkxCredentials>(invalid).is_err());
    }
}
