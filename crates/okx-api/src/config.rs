use std::{env, str::FromStr};

use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::{auth::sign, error::OkxError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Region {
    Global,
    Eea,
    UsAu,
}

impl FromStr for Region {
    type Err = OkxError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "global" => Ok(Self::Global),
            "eea" | "eu" => Ok(Self::Eea),
            "us-au" | "us_au" | "usau" => Ok(Self::UsAu),
            other => Err(OkxError::Config(format!(
                "unsupported OKX region '{other}'; expected global, eea, or us-au"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct OkxEnvironment {
    pub region: Region,
    pub demo: bool,
}

impl OkxEnvironment {
    pub const fn new(region: Region, demo: bool) -> Self {
        Self { region, demo }
    }

    pub const fn rest_base_url(self) -> &'static str {
        match self.region {
            Region::Global => "https://openapi.okx.com",
            Region::Eea => "https://eea.okx.com",
            Region::UsAu => "https://us.okx.com",
        }
    }

    pub const fn public_ws_url(self) -> &'static str {
        match (self.region, self.demo) {
            (Region::Global, false) => "wss://ws.okx.com:8443/ws/v5/public",
            (Region::Global, true) => "wss://wspap.okx.com:8443/ws/v5/public",
            (Region::Eea, false) => "wss://wseea.okx.com:8443/ws/v5/public",
            (Region::Eea, true) => "wss://wseeapap.okx.com:8443/ws/v5/public",
            (Region::UsAu, false) => "wss://wsus.okx.com:8443/ws/v5/public",
            (Region::UsAu, true) => "wss://wsuspap.okx.com:8443/ws/v5/public",
        }
    }

    pub const fn private_ws_url(self) -> &'static str {
        match (self.region, self.demo) {
            (Region::Global, false) => "wss://ws.okx.com:8443/ws/v5/private",
            (Region::Global, true) => "wss://wspap.okx.com:8443/ws/v5/private",
            (Region::Eea, false) => "wss://wseea.okx.com:8443/ws/v5/private",
            (Region::Eea, true) => "wss://wseeapap.okx.com:8443/ws/v5/private",
            (Region::UsAu, false) => "wss://wsus.okx.com:8443/ws/v5/private",
            (Region::UsAu, true) => "wss://wsuspap.okx.com:8443/ws/v5/private",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct WsLoginMaterial {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub passphrase: String,
    pub timestamp: String,
    pub sign: String,
}

#[derive(Clone)]
pub struct Credentials {
    api_key: Zeroizing<String>,
    secret_key: Zeroizing<String>,
    passphrase: Zeroizing<String>,
}

impl Credentials {
    pub fn new(api_key: String, secret_key: String, passphrase: String) -> Result<Self, OkxError> {
        validate_secret_component("API key", &api_key)?;
        validate_secret_component("API secret", &secret_key)?;
        validate_secret_component("API passphrase", &passphrase)?;

        Ok(Self {
            api_key: Zeroizing::new(api_key),
            secret_key: Zeroizing::new(secret_key),
            passphrase: Zeroizing::new(passphrase),
        })
    }

    pub fn websocket_login_material(
        &self,
        timestamp_seconds: &str,
    ) -> Result<WsLoginMaterial, OkxError> {
        if timestamp_seconds.trim().is_empty()
            || !timestamp_seconds.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(OkxError::Config(
                "websocket login timestamp must be Unix epoch seconds".to_owned(),
            ));
        }

        let signature = sign(
            timestamp_seconds,
            "GET",
            "/users/self/verify",
            "",
            self.secret_key(),
        )?;

        Ok(WsLoginMaterial {
            api_key: self.api_key().to_owned(),
            passphrase: self.passphrase().to_owned(),
            timestamp: timestamp_seconds.to_owned(),
            sign: signature,
        })
    }

    pub fn from_env() -> Result<Self, OkxError> {
        fn required(name: &str) -> Result<String, OkxError> {
            env::var(name)
                .map_err(|_| {
                    OkxError::Config(format!("required environment variable {name} is missing"))
                })
                .and_then(|value| {
                    if value.trim().is_empty() {
                        Err(OkxError::Config(format!(
                            "required environment variable {name} is empty"
                        )))
                    } else {
                        Ok(value)
                    }
                })
        }

        Self::new(
            required("OKX_API_KEY")?,
            required("OKX_API_SECRET")?,
            required("OKX_API_PASSPHRASE")?,
        )
    }

    pub(crate) fn api_key(&self) -> &str {
        self.api_key.as_str()
    }

    pub(crate) fn secret_key(&self) -> &str {
        self.secret_key.as_str()
    }

    pub(crate) fn passphrase(&self) -> &str {
        self.passphrase.as_str()
    }
}

fn validate_secret_component(label: &str, value: &str) -> Result<(), OkxError> {
    if value.trim().is_empty() || value.len() > 4096 || value.bytes().any(|byte| byte == 0) {
        return Err(OkxError::Config(format!("{label} is invalid")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_login_material_uses_okx_verify_path() {
        let credentials = Credentials::new(
            "key".to_owned(),
            "secret".to_owned(),
            "pass".to_owned(),
        )
        .expect("credentials");
        let material = credentials
            .websocket_login_material("1538054050")
            .expect("login material");

        assert_eq!(material.api_key, "key");
        assert_eq!(material.passphrase, "pass");
        assert_eq!(material.timestamp, "1538054050");
        assert_eq!(
            material.sign,
            sign("1538054050", "GET", "/users/self/verify", "", "secret").expect("signature")
        );
    }

    #[test]
    fn credentials_reject_empty_components_without_echoing_secret() {
        let error = match Credentials::new("api-key".to_owned(), String::new(), "pass".to_owned()) {
            Ok(_) => panic!("empty secret must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "configuration error: API secret is invalid"
        );
    }
}
