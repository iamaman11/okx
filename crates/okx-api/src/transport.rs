//! One delivery boundary for raw HTTP bytes; no venue or account emulator.
use crate::error::OkxError;
use reqwest::{Client, StatusCode, header::HeaderMap};
pub use reqwest::{Request as HttpRequest, StatusCode as HttpStatusCode};
use std::{future::Future, pin::Pin, time::Instant};

pub struct RawHttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

pub type HttpSend<'a> =
    Pin<Box<dyn Future<Output = Result<RawHttpResponse, OkxError>> + Send + 'a>>;

pub trait HttpTransport: Send + Sync {
    fn send(&self, request: HttpRequest, deadline: Instant) -> HttpSend<'_>;
}

#[derive(Clone)]
pub struct ReqwestHttpTransport {
    client: Client,
}

impl ReqwestHttpTransport {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl HttpTransport for ReqwestHttpTransport {
    fn send(&self, request: HttpRequest, deadline: Instant) -> HttpSend<'_> {
        Box::pin(async move {
            let delivery = async {
                let mut response = self.client.execute(request).await?;
                let status = response.status();
                let headers = response.headers().clone();
                let auth = status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN;
                let max_bytes = if auth { 4096 } else { 32 * 1024 * 1024 };
                let mut body = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    if body.len().saturating_add(chunk.len()) > max_bytes {
                        if auth {
                            body.clear();
                            break;
                        }
                        return Err(OkxError::Response("HTTP_RESPONSE_TOO_LARGE".to_owned()));
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(RawHttpResponse {
                    status,
                    headers,
                    body,
                })
            };
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), delivery)
                .await
                .map_err(|_| OkxError::Response("HTTP_TRANSPORT_DELIVERY_UNCERTAIN".to_owned()))?
        })
    }
}
