use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, StatusCode, header::RETRY_AFTER};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{
    auth::{sign, timestamp_now},
    clock::ClockEvidence,
    config::{Credentials, OkxEnvironment},
    error::OkxError,
    rate::{GENERAL_RATE_LIMIT_CODE, RateBudget, RateRequestPlan, SUBACCOUNT_RATE_LIMIT_CODE},
    transport::{HttpTransport, RawHttpResponse, ReqwestHttpTransport},
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
        .no_proxy()
        .user_agent("iamaman11-okx/0.1")
        .connect_timeout(OKX_CONNECT_TIMEOUT)
        .timeout(OKX_REQUEST_TIMEOUT)
        .build()
}

#[derive(Debug, Clone)]
pub struct CapturedPublicRows<T> {
    pub rows: Vec<T>,
    pub raw_body: Vec<u8>,
}

#[derive(Clone)]
pub struct OkxPublicClient {
    http: Client,
    transport: Arc<dyn HttpTransport>,
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
        let transport: Arc<dyn HttpTransport> = Arc::new(ReqwestHttpTransport::new(http.clone()));
        Ok(Self {
            http,
            transport,
            environment,
            rate_budget,
        })
    }

    /// Same signed production requests with an injected HTTP delivery boundary.
    pub fn with_http_transport(
        environment: OkxEnvironment,
        rate_budget: RateBudget,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<Self, OkxError> {
        Ok(Self {
            http: build_http_client()?,
            transport,
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
        fetch_clock_evidence(
            &self.http,
            &self.transport,
            self.environment,
            &self.rate_budget,
        )
        .await
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

        decode(
            send_request(&self.transport, request).await?,
            &self.rate_budget,
            &plan,
        )
        .await
    }

    pub(crate) async fn public_get_captured<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<CapturedPublicRows<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        self.public_get_captured_with_admission(path, params, None)
            .await
    }

    /// Only bounded analytics history scans opt into queued public admission.
    /// Existing regular reads, all private reads and every trading action stay fail-fast.
    pub(crate) async fn public_get_captured_queued<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<CapturedPublicRows<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        self.public_get_captured_with_admission(path, params, Some(Duration::from_secs(15)))
            .await
    }

    async fn public_get_captured_with_admission<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
        queue_timeout: Option<Duration>,
    ) -> Result<CapturedPublicRows<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let plan = self.rate_budget.public_rest_plan(path, params);
        if let Some(timeout) = queue_timeout {
            self.rate_budget
                .admit_wait(&plan, timeout)
                .await
                .map_err(|evidence| OkxError::RateLimited { evidence })?;
        } else {
            admit(&self.rate_budget, &plan)?;
        }

        let request_path = request_path_with_query(path, params);
        let url = format!("{}{}", self.environment.rest_base_url(), request_path);

        let mut request = self.http.get(url).header("Accept", "application/json");
        if self.environment.demo {
            request = request.header("x-simulated-trading", "1");
        }
        let (envelope, raw_body) = decode_envelope_captured(
            send_request(&self.transport, request).await?,
            &self.rate_budget,
            &plan,
        )
        .await?;
        if envelope.code != "0" {
            return Err(OkxError::Api {
                code: envelope.code,
                message: envelope.msg,
            });
        }
        Ok(CapturedPublicRows {
            rows: envelope.data,
            raw_body,
        })
    }
}

#[derive(Clone)]
pub struct OkxRestClient {
    http: Client,
    transport: Arc<dyn HttpTransport>,
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
        let transport: Arc<dyn HttpTransport> = Arc::new(ReqwestHttpTransport::new(http.clone()));

        Ok(Self {
            http,
            transport,
            environment,
            credentials,
            rate_budget,
        })
    }

    /// Same private signer and parser, regardless of delivery implementation.
    pub fn with_http_transport(
        environment: OkxEnvironment,
        credentials: Credentials,
        rate_budget: RateBudget,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<Self, OkxError> {
        Ok(Self {
            http: build_http_client()?,
            transport,
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
        fetch_clock_evidence(
            &self.http,
            &self.transport,
            self.environment,
            &self.rate_budget,
        )
        .await
    }

    pub(crate) async fn private_get<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        self.private_get_with_queue_budget(path, params, None).await
    }

    /// An opt-in rate-admitted read: wait only for local no-send capacity.
    /// Trading POST, exchange throttles and already-sent requests are never retried.
    pub(crate) async fn private_get_wait<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
        max_wait: Duration,
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        self.private_get_with_queue_budget(path, params, Some(max_wait))
            .await
    }

    async fn private_get_with_queue_budget<T>(
        &self,
        path: &str,
        params: &[(&str, String)],
        max_wait: Option<Duration>,
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let plan = self.rate_budget.private_rest_plan(path, params);
        if let Some(wait) = max_wait {
            self.rate_budget
                .admit_wait(&plan, wait)
                .await
                .map_err(|evidence| OkxError::RateLimited { evidence })?;
        } else {
            admit(&self.rate_budget, &plan)?;
        }

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

        decode(
            send_request(&self.transport, request).await?,
            &self.rate_budget,
            &plan,
        )
        .await
    }

    pub(crate) async fn private_read_post<T, B>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<Vec<T>, OkxError>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        if path != "/api/v5/account/position-builder" {
            return Err(OkxError::Config(format!(
                "private read-only POST path is not allowlisted: {path}"
            )));
        }
        let rate_plan = self.rate_budget.private_rest_plan(path, &[]);
        let timestamp = timestamp_now();
        let envelope = self
            .private_post(path, body, &timestamp, None, &rate_plan)
            .await?;
        if envelope.code != "0" {
            return Err(OkxError::Api {
                code: envelope.code,
                message: envelope.msg,
            });
        }
        Ok(envelope.data)
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

        decode_envelope(
            send_request(&self.transport, request).await?,
            &self.rate_budget,
            rate_plan,
        )
        .await
    }
}

async fn fetch_clock_evidence(
    http: &Client,
    transport: &Arc<dyn HttpTransport>,
    environment: OkxEnvironment,
    rate_budget: &RateBudget,
) -> Result<ClockEvidence, OkxError> {
    let plan = rate_budget.public_rest_plan(OKX_PUBLIC_TIME_PATH, &[]);
    admit(rate_budget, &plan)?;

    let local_started_ms = system_unix_ms()?;
    let started = Instant::now();
    let url = format!("{}{}", environment.rest_base_url(), OKX_PUBLIC_TIME_PATH);
    let rows: Vec<ServerTime> = decode(
        send_request(
            transport,
            http.get(url).header("Accept", "application/json"),
        )
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

async fn send_request(
    transport: &Arc<dyn HttpTransport>,
    request: reqwest::RequestBuilder,
) -> Result<RawHttpResponse, OkxError> {
    transport
        .send(request.build()?, Instant::now() + OKX_REQUEST_TIMEOUT)
        .await
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
    response: RawHttpResponse,
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
    response: RawHttpResponse,
    rate_budget: &RateBudget,
    plan: &RateRequestPlan,
) -> Result<ApiEnvelope<T>, OkxError>
where
    T: DeserializeOwned,
{
    let (envelope, _) = decode_envelope_captured(response, rate_budget, plan).await?;
    Ok(envelope)
}

async fn decode_envelope_captured<T>(
    response: RawHttpResponse,
    rate_budget: &RateBudget,
    plan: &RateRequestPlan,
) -> Result<(ApiEnvelope<T>, Vec<u8>), OkxError>
where
    T: DeserializeOwned,
{
    let server_retry_after_ms = retry_after_ms(&response);

    if response.status == StatusCode::TOO_MANY_REQUESTS {
        let exchange_code = throttle_code_from_http_429_body(&response.body);
        let evidence =
            rate_budget.record_exchange_throttle(plan, &exchange_code, server_retry_after_ms);
        return Err(OkxError::RateLimited {
            evidence: Box::new(evidence),
        });
    }

    // reqwest::error_for_status() discards the exchange error envelope.
    // Preserve ONLY a validated numeric OKX code for native operator diagnosis;
    // never return a raw authentication response, headers or any secret.
    if matches!(
        response.status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
    ) {
        let status = response.status.as_u16();
        let code = auth_rejection_code(&response.body).unwrap_or_else(|| "NOT_PROVIDED".to_owned());
        return Err(OkxError::AuthenticationRejected {
            status,
            okx_code: code,
        });
    }

    if response.status.is_client_error() || response.status.is_server_error() {
        return Err(OkxError::HttpStatus {
            status: response.status.as_u16(),
        });
    }
    let body = response.body;
    let envelope: ApiEnvelope<T> = serde_json::from_slice(&body)?;

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

    Ok((envelope, body))
}

/// The response is untrusted and may be an HTML gateway page rather than
/// an OKX JSON envelope. Only an exact 5-digit code is safe to surface.
fn auth_rejection_code(body: &[u8]) -> Option<String> {
    if body.len() > 4096 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let code = value.get("code")?.as_str()?;
    (code.len() == 5 && code.bytes().all(|byte| byte.is_ascii_digit())).then(|| code.to_owned())
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

fn retry_after_ms(response: &RawHttpResponse) -> Option<u64> {
    response
        .headers
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
    fn auth_rejection_exposes_only_a_bounded_numeric_okx_code() {
        assert_eq!(
            auth_rejection_code(br#"{"code":"50119","msg":"key invalid","data":[]}"#),
            Some("50119".to_owned())
        );
        assert_eq!(auth_rejection_code(b"<html>Unauthorized</html>"), None);
        assert_eq!(
            auth_rejection_code(br#"{"code":"INVALID_KEY","msg":"sensitive"}"#),
            None
        );
        assert_eq!(
            auth_rejection_code(br#"{"code":"50119","secret":"sensitive","nested":[]}"#),
            Some("50119".to_owned())
        );
        assert_eq!(auth_rejection_code(&vec![b'a'; 4097]), None);
    }

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

    /// Transcript feeds raw responses, without a fake exchange or risk engine.
    #[derive(Default)]
    struct ScriptedDelivery {
        responses: std::sync::Mutex<std::collections::VecDeque<(&'static str, RawHttpResponse)>>,
        requests: std::sync::Mutex<Vec<(String, reqwest::header::HeaderMap)>>,
    }
    impl HttpTransport for ScriptedDelivery {
        fn send(
            &self,
            request: reqwest::Request,
            _deadline: Instant,
        ) -> crate::transport::HttpSend<'_> {
            Box::pin(async move {
                let path = request.url().path().to_owned();
                self.requests
                    .lock()
                    .unwrap()
                    .push((path.clone(), request.headers().clone()));
                let (expected, reply) = self
                    .responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or_else(|| OkxError::Response("UNEXPECTED_HTTP_REQUEST".to_owned()))?;
                if expected != path {
                    return Err(OkxError::Response("HTTP_TRANSCRIPT_MISMATCH".to_owned()));
                }
                Ok(reply)
            })
        }
    }
    fn reply(status: StatusCode, body: &[u8]) -> RawHttpResponse {
        RawHttpResponse {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: body.to_vec(),
        }
    }
    #[tokio::test]
    async fn signed_private_request_and_real_decode_use_the_same_delivery_port() {
        let wire = Arc::new(ScriptedDelivery::default());
        wire.responses.lock().unwrap().push_back((
            "/api/v5/account/config",
            reply(
                StatusCode::OK,
                br#"{"code":"0","msg":"","data":[{"uid":"123"}]}"#,
            ),
        ));
        let creds = Credentials::new("key".into(), "secret".into(), "pass".into()).unwrap();
        let api = OkxRestClient::with_http_transport(
            OkxEnvironment::new(crate::config::Region::Global, false),
            creds,
            RateBudget::new(),
            wire.clone(),
        )
        .unwrap();
        let rows: Vec<serde_json::Value> = api
            .private_get("/api/v5/account/config", &[])
            .await
            .unwrap();
        assert_eq!(rows[0]["uid"], "123");
        let sent = wire.requests.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].1.contains_key("OK-ACCESS-SIGN"));
        assert_eq!(wire.responses.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn bounded_private_balance_read_waits_only_before_http_and_fails_closed() {
        let rate = RateBudget::new();
        let plan = rate.private_rest_plan("/api/v5/account/balance", &[]);
        for _ in 0..10 {
            rate.admit(&plan).expect("within documented balance budget");
        }
        let wire = Arc::new(ScriptedDelivery::default());
        wire.responses.lock().unwrap().push_back((
            "/api/v5/account/balance",
            reply(
                StatusCode::OK,
                br#"{"code":"0","msg":"","data":[{"details":[]}]}"#,
            ),
        ));
        let creds = Credentials::new("key".into(), "secret".into(), "pass".into()).unwrap();
        let api = OkxRestClient::with_http_transport(
            OkxEnvironment::new(crate::config::Region::Global, false),
            creds,
            rate,
            wire.clone(),
        )
        .unwrap();

        // Exhausted admission + expired caller deadline must not call the
        // HTTP transport. The scripted reply must remain untouched.
        let denied = api
            .private_get_wait::<serde_json::Value>("/api/v5/account/balance", &[], Duration::ZERO)
            .await
            .unwrap_err();
        assert!(matches!(
            denied,
            OkxError::RateLimited { evidence }
                if !evidence.request_sent && evidence.local_defer_ms > 0
        ));
        assert!(wire.requests.lock().unwrap().is_empty());
        assert_eq!(wire.responses.lock().unwrap().len(), 1);

        // The same typed budget releases its real admission window and sends
        // exactly one authenticated GET; there is no HTTP retry or fake data.
        let started = Instant::now();
        let rows: Vec<serde_json::Value> = api
            .private_get_wait("/api/v5/account/balance", &[], Duration::from_secs(3))
            .await
            .expect("local wait then one recorded HTTP response");
        assert!(started.elapsed() >= Duration::from_millis(1500));
        assert_eq!(rows.len(), 1);
        assert_eq!(wire.requests.lock().unwrap().len(), 1);
        assert!(
            wire.requests.lock().unwrap()[0]
                .1
                .contains_key("OK-ACCESS-SIGN")
        );
        assert!(wire.responses.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bounded_private_balance_does_not_retry_exchange_throttle() {
        let wire = Arc::new(ScriptedDelivery::default());
        wire.responses.lock().unwrap().push_back((
            "/api/v5/account/balance",
            reply(
                StatusCode::TOO_MANY_REQUESTS,
                br#"{"code":"50011","data":[]}"#,
            ),
        ));
        let api = OkxRestClient::with_http_transport(
            OkxEnvironment::new(crate::config::Region::Global, false),
            Credentials::new("key".into(), "secret".into(), "pass".into()).unwrap(),
            RateBudget::new(),
            wire.clone(),
        )
        .unwrap();
        let denied = api
            .private_get_wait::<serde_json::Value>(
                "/api/v5/account/balance",
                &[],
                Duration::from_secs(3),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            denied,
            OkxError::RateLimited { evidence }
                if evidence.request_sent && evidence.exchange_code.as_deref() == Some("50011")
        ));
        assert_eq!(wire.requests.lock().unwrap().len(), 1);
        assert!(wire.responses.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn raw_http_throttle_auth_and_status_are_typed_without_live_calls() {
        for (status, body) in [
            (
                StatusCode::TOO_MANY_REQUESTS,
                br#"{"code":"50011","data":[]}"#.as_slice(),
            ),
            (
                StatusCode::UNAUTHORIZED,
                br#"{"code":"50119","secret":"hidden"}"#.as_slice(),
            ),
            (StatusCode::SERVICE_UNAVAILABLE, b"gateway".as_slice()),
        ] {
            let wire = Arc::new(ScriptedDelivery::default());
            wire.responses
                .lock()
                .unwrap()
                .push_back(("/api/v5/public/instruments", reply(status, body)));
            let api = OkxPublicClient::with_http_transport(
                OkxEnvironment::new(crate::config::Region::Global, false),
                RateBudget::new(),
                wire.clone(),
            )
            .unwrap();
            let failure = api
                .public_get::<serde_json::Value>("/api/v5/public/instruments", &[])
                .await
                .unwrap_err();
            match status {
                StatusCode::TOO_MANY_REQUESTS => {
                    assert!(matches!(failure, OkxError::RateLimited { .. }))
                }
                StatusCode::UNAUTHORIZED => assert!(matches!(
                    failure,
                    OkxError::AuthenticationRejected { status: 401, .. }
                )),
                _ => assert!(matches!(failure, OkxError::HttpStatus { status: 503 })),
            }
            assert_eq!(wire.requests.lock().unwrap().len(), 1);
            assert_eq!(wire.responses.lock().unwrap().len(), 0);
        }
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
