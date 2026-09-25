use reqwest::Client;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::{
    auth::{sign, timestamp_now},
    config::{Credentials, OkxEnvironment},
    error::OkxError,
};

#[derive(Debug, Deserialize)]
struct ApiEnvelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    data: Vec<T>,
}

#[derive(Clone)]
pub struct OkxRestClient {
    http: Client,
    environment: OkxEnvironment,
    credentials: Credentials,
}

impl OkxRestClient {
    pub fn new(environment: OkxEnvironment, credentials: Credentials) -> Result<Self, OkxError> {
        let http = Client::builder().user_agent("iamaman11-okx/0.1").build()?;

        Ok(Self {
            http,
            environment,
            credentials,
        })
    }

    pub fn environment(&self) -> OkxEnvironment {
        self.environment
    }

    pub(crate) async fn private_get<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in params {
            serializer.append_pair(key, value);
        }
        let query = serializer.finish();

        let request_path = if query.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query}")
        };

        let timestamp = timestamp_now();
        let signature = sign(
            &timestamp,
            "GET",
            &request_path,
            "",
            self.credentials.secret_key(),
        )?;
        let url = format!("{}{}", self.environment.rest_base_url(), request_path);

        let mut request = self
            .http
            .get(url)
            .header("OK-ACCESS-KEY", self.credentials.api_key())
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", self.credentials.passphrase());

        if self.environment.demo {
            request = request.header("x-simulated-trading", "1");
        }

        let response = request.send().await?.error_for_status()?;
        let envelope: ApiEnvelope<T> = response.json().await?;

        if envelope.code != "0" {
            return Err(OkxError::Api {
                code: envelope.code,
                message: envelope.msg,
            });
        }

        Ok(envelope.data)
    }
}
