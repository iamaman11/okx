use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{
    auth::{sign, timestamp_now},
    clock::ClockEvidence,
    config::{Credentials, OkxEnvironment},
    error::OkxError,
    rate::{GENERAL_RATE_LIMIT_CODE, RateBudget, RateRequestPlan, SUBACCOUNT_RATE_LIMIT_CODE},
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
const OKX_PUBLIC_TIME_PATH: &str = "/api/v5/public/time";

#[derive(Debug, Deserialize)]
struct ServerTime {
    ts: String,
}

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
    rate_budget: RateBudget,
}

impl OkxPublicClient {
    pub fn new(environment: OkxEnvironment) -> Result<Self, OkxError> {
        Self::with_rate_budget(environment, RateBudget::new())
    }

    pub fn with_rate_budget(
        environment: OkxEnvironment,
        rate_budget: RateBudget,
    ) -> Result<Self, OkxError> {
        let http = build_http_client()?;
        Ok(Self {
            http,
            environment,
            rate_budget,
        })
    }

    pub fn environment(&self) -> OkxEnvironment {
        self.environment
    }

    pub fn rate_budget(&self) -> RateBudget {
        self.rate_budget.clone()
    }

    pub async fn clock_evidence(&self) -> Result<ClockEvidence, OkxError> {
        fetch_clock_evidence(&self.http, self.environment, &self.rate_budget).await
    }

    pub(crate) async fn public_get<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let plan = self.rate_budget.public_rest_plan(path, params);
        admit(&self.rate_budget, &plan)?;

        let request_path = request_path_with_query(path, params);
        let url = format!("{}{}", self.environment.rest_base_url(), request_path);

        let mut request = self.http.get(url).header("Accept", "application/json");

        if self.environment.demo {
            request = request.header("x-simulated-trading", "1");
        }

        decode(request.send().await?, &self.rate_budget, &plan).await
    }
}

#[derive(Clone)]
pub struct OkxRestClient {
    http: Client,
    environment: OkxEnvironment,
    credentials: Credentials,
    rate_budget: RateBudget,
}

impl OkxRestClient {
    pub fn new(environment: OkxEnvironment, credentials: Credentials) -> Result<Self, OkxError> {
        Self::with_rate_budget(environment, credentials, RateBudget::new())
    }

    pub fn with_rate_budget(
        environment: OkxEnvironment,
        credentials: Credentials,
        rate_budget: RateBudget,
    ) -> Result<Self, OkxError> {
        let http = build_http_client()?;

        Ok(Self {
            http,
            environment,
            credentials,
            rate_budget,
        })
    }

    pub fn environment(&self) -> OkxEnvironment {
        self.environment
    }

    pub fn rate_budget(&self) -> RateBudget {
        self.rate_budget.clone()
    }

    pub async fn clock_evidence(&self) -> Result<ClockEvidence, OkxError> {
        fetch_clock_evidence(&self.http, self.environment, &self.rate_budget).await
    }

    pub(crate) async fn private_get<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let plan = self.rate_budget.private_rest_plan(path, params);
        admit(&self.rate_budget, &plan)?;

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

        decode(request.send().await?, &self.rate_budget, &plan).await
    }

    pub(crate) async fn private_post<T, B>(
        &self,
        path: &str,
        body: &B,
        request_timestamp: &str,
        exp_time_ms: Option<u64>,
        rate_plan: &RateRequestPlan,
    ) -> Result<ApiEnvelope<T>, OkxError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        admit(&self.rate_budget, rate_plan)?;
        self.private_post_after_admission(path, body, request_timestamp, exp_time_ms, rate_plan)
            .await
    }

    pub(crate) async fn private_post_after_admission<T, B>(
        &self,
        path: &str,
        body: &B,
        request_timestamp: &str,
        exp_time_ms: Option<u64>,
        rate_plan: &RateRequestPlan,
    ) -> Result<ApiEnvelope<T>, OkxError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let encoded = serde_json::to_string(body)?;

        let signature = sign(
            request_timestamp,
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
            .header("OK-ACCESS-TIMESTAMP", request_timestamp)
            .header("OK-ACCESS-PASSPHRASE", self.credentials.passphrase())
            .body(encoded);

        if let Some(exp_time_ms) = exp_time_ms {
            request = request.header("expTime", exp_time_ms.to_string());
        }
        if self.environment.demo {
            request = request.header("x-simulated-trading", "1");
        }

        decode_envelope(request.send().await?, &self.rate_budget, rate_plan).await
    }
}

async fn fetch_clock_evidence(
    http: &Client,
    environment: OkxEnvironment,
    rate_budget: &RateBudget,
) -> Result<ClockEvidence, OkxError> {
    let plan = rate_budget.public_rest_plan(OKX_PUBLIC_TIME_PATH, &[]);
    admit(rate_budget, &plan)?;

    let local_started_ms = system_unix_ms()?;
    let started = Instant::now();
    let url = format!("{}{}", environment.rest_base_url(), OKX_PUBLIC_TIME_PATH);
    let rows: Vec<ServerTime> = decode(
        http.get(url)
            .header("Accept", "application/json")
            .send()
            .await?,
        rate_budget,
        &plan,
    )
    .await?;
    let round_trip = started.elapsed();
    let [row] = rows.as_slice() else {
        return Err(OkxError::Clock(format!(
            "expected exactly one OKX server-time row, found {}",
            rows.len()
        )));
    };
    let server_time_ms = row.ts.parse::<u64>().map_err(|_| {
        OkxError::Clock("OKX server time is not a Unix millisecond timestamp".to_owned())
    })?;
    ClockEvidence::from_sample(server_time_ms, local_started_ms, round_trip, Instant::now())
}

fn system_unix_ms() -> Result<u64, OkxError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OkxError::Clock("local wall clock is before Unix epoch".to_owned()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| OkxError::Clock("local Unix millisecond clock overflowed u64".to_owned()))
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

fn admit(rate_budget: &RateBudget, plan: &RateRequestPlan) -> Result<(), OkxError> {
    rate_budget
        .admit(plan)
        .map_err(|evidence| OkxError::RateLimited { evidence })
}

async fn decode<T>(
    response: reqwest::Response,
    rate_budget: &RateBudget,
    plan: &RateRequestPlan,
) -> Result<Vec<T>, OkxError>
where
    T: DeserializeOwned,
{
    let envelope = decode_envelope(response, rate_budget, plan).await?;

    if envelope.code != "0" {
        return Err(OkxError::Api {
            code: envelope.code,
            message: envelope.msg,
        });
    }

    Ok(envelope.data)
}

async fn decode_envelope<T>(
    response: reqwest::Response,
    rate_budget: &RateBudget,
    plan: &RateRequestPlan,
) -> Result<ApiEnvelope<T>, OkxError>
where
    T: DeserializeOwned,
{
    let server_retry_after_ms = retry_after_ms(&response);

    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let body = response.bytes().await?;
        let exchange_code = throttle_code_from_http_429_body(&body);
        let evidence =
            rate_budget.record_exchange_throttle(plan, &exchange_code, server_retry_after_ms);
        return Err(OkxError::RateLimited {
            evidence: Box::new(evidence),
        });
    }

    let response = response.error_for_status()?;
    let envelope: ApiEnvelope<T> = response.json().await?;

    if matches!(
        envelope.code.as_str(),
        GENERAL_RATE_LIMIT_CODE | SUBACCOUNT_RATE_LIMIT_CODE
    ) {
        let evidence =
            rate_budget.record_exchange_throttle(plan, &envelope.code, server_retry_after_ms);
        return Err(OkxError::RateLimited {
            evidence: Box::new(evidence),
        });
    }

    Ok(envelope)
}

fn throttle_code_from_http_429_body(body: &[u8]) -> String {
    serde_json::from_slice::<ApiEnvelope<serde_json::Value>>(body)
        .ok()
        .map(|envelope| envelope.code)
        .filter(|code| {
            matches!(
                code.as_str(),
                GENERAL_RATE_LIMIT_CODE | SUBACCOUNT_RATE_LIMIT_CODE
            )
        })
        .unwrap_or_else(|| "HTTP_429".to_owned())
}

fn retry_after_ms(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?
        .checked_mul(1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_code_from_http_429_body_preserves_okx_code() {
        assert_eq!(
            throttle_code_from_http_429_body(
                br#"{"code":"50061","msg":"sub-account rate limit","data":[]}"#
            ),
            "50061"
        );
        assert_eq!(
            throttle_code_from_http_429_body(
                br#"{"code":"50011","msg":"rate limit reached","data":[]}"#
            ),
            "50011"
        );
        assert_eq!(throttle_code_from_http_429_body(b"not-json"), "HTTP_429");
        assert_eq!(
            throttle_code_from_http_429_body(br#"{"code":"51000","msg":"other error","data":[]}"#),
            "HTTP_429"
        );
    }

    #[test]
    fn request_query_encoding_is_deterministic() {
        assert_eq!(
            request_path_with_query(
                "/api/v5/account/instruments",
                &[
                    ("instType", "SWAP".to_owned()),
                    ("instId", "DOGE-USDT-SWAP".to_owned()),
                ],
            ),
            "/api/v5/account/instruments?instType=SWAP&instId=DOGE-USDT-SWAP"
        );
    }
}
