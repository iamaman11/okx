use std::time::Duration;

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{
    auth::{sign, timestamp_now},
    config::{Credentials, OkxEnvironment},
    error::OkxError,
};

#[derive(Debug, Deserialize)]
pub(crate) struct ApiEnvelope<T> {
    pub(crate) code: String,
    #[serde(default)]
    pub(crate) msg: String,
    pub(crate) data: Vec<T>,
    #[serde(rename = "inTime", default)]
    pub(crate) in_time: String,
    #[serde(rename = "outTime", default)]
    pub(crate) out_time: String,
}

const OKX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const OKX_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

fn build_http_client() -> Result<Client, reqwest::Error> {
    Client::builder()
        .user_agent("iamaman11-okx/0.1")
        .connect_timeout(OKX_CONNECT_TIMEOUT)
        .timeout(OKX_REQUEST_TIMEOUT)
        .build()
}

#[derive(Clone)]
pub struct OkxPublicClient {
    http: Client,
    environment: OkxEnvironment,
}

impl OkxPublicClient {
    pub fn new(environment: OkxEnvironment) -> Result<Self, OkxError> {
        let http = build_http_client()?;
        Ok(Self { http, environment })
    }

    pub fn environment(&self) -> OkxEnvironment {
        self.environment
    }

    pub(crate) async fn public_get<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let request_path = request_path_with_query(path, params);
        let url = format!("{}{}", self.environment.rest_base_url(), request_path);

        let mut request = self.http.get(url).header("Accept", "application/json");

        if self.environment.demo {
            request = request.header("x-simulated-trading", "1");
        }

        decode(request.send().await?).await
    }
}

#[derive(Clone)]
pub struct OkxRestClient {
    http: Client,
    environment: OkxEnvironment,
    credentials: Credentials,
}

impl OkxRestClient {
    pub fn new(environment: OkxEnvironment, credentials: Credentials) -> Result<Self, OkxError> {
        let http = build_http_client()?;

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
        let request_path = request_path_with_query(path, params);

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

        decode(request.send().await?).await
    }

    pub(crate) async fn private_post<T, B>(
        &self,
        path: &str,
        body: &B,
        exp_time_ms: Option<u64>,
    ) -> Result<ApiEnvelope<T>, OkxError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let encoded = serde_json::to_string(body)?;
        let timestamp = timestamp_now();
        let signature = sign(
            &timestamp,
            "POST",
            path,
            &encoded,
            self.credentials.secret_key(),
        )?;
        let url = format!("{}{}", self.environment.rest_base_url(), path);

        let mut request = self
            .http
            .post(url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("OK-ACCESS-KEY", self.credentials.api_key())
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", self.credentials.passphrase())
            .body(encoded);

        if let Some(exp_time_ms) = exp_time_ms {
            request = request.header("expTime", exp_time_ms.to_string());
        }
        if self.environment.demo {
            request = request.header("x-simulated-trading", "1");
        }

        decode_envelope(request.send().await?).await
    }
}

fn request_path_with_query(path: &str, params: &[(&str, String)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    let query = serializer.finish();
    if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    }
}

async fn decode<T>(response: reqwest::Response) -> Result<Vec<T>, OkxError>
where
    T: DeserializeOwned,
{
    let envelope = decode_envelope(response).await?;

    if envelope.code != "0" {
        return Err(OkxError::Api {
            code: envelope.code,
            message: envelope.msg,
        });
    }

    Ok(envelope.data)
}

async fn decode_envelope<T>(response: reqwest::Response) -> Result<ApiEnvelope<T>, OkxError>
where
    T: DeserializeOwned,
{
    let response = response.error_for_status()?;
    Ok(response.json().await?)
}
