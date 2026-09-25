use std::{env, str::FromStr};

use serde::Serialize;

use crate::error::OkxError;

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

#[derive(Clone)]
pub struct Credentials {
    api_key: String,
    secret_key: String,
    passphrase: String,
}

impl Credentials {
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

        Ok(Self {
            api_key: required("OKX_API_KEY")?,
            secret_key: required("OKX_API_SECRET")?,
            passphrase: required("OKX_API_PASSPHRASE")?,
        })
    }

    pub(crate) fn api_key(&self) -> &str {
        &self.api_key
    }

    pub(crate) fn secret_key(&self) -> &str {
        &self.secret_key
    }

    pub(crate) fn passphrase(&self) -> &str {
        &self.passphrase
    }
}
