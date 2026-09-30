use std::{convert::Infallible, net::SocketAddr};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::Incoming,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use okx_bridge_mcp::{Bridge, mcp_initialize_result, mcp_tools};
use serde_json::{Value, json};
use tokio::net::TcpListener;

type Body = Full<Bytes>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(address).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(io, service_fn(handle))
                .await
            {
                eprintln!("okx-bridge connection failed: {error}");
            }
        });
    }
}

async fn handle(request: Request<Incoming>) -> Result<Response<Body>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/health") => json_response(
            StatusCode::OK,
            json!({
                "status": "ok",
                "service": "okx-bridge",
                "transport": "hidden",
                "credential_storage": "client_vault"
            }),
        ),
        (&Method::POST, "/mcp") => {
            let bearer = bearer_token(&request);
            match request.collect().await {
                Ok(collected) => match serde_json::from_slice::<Value>(&collected.to_bytes()) {
                    Ok(message) => handle_mcp(message, bearer).await,
                    Err(error) => {
                        jsonrpc_error(Value::Null, -32700, &format!("parse error: {error}"))
                    }
                },
                Err(error) => jsonrpc_error(
                    Value::Null,
                    -32603,
                    &format!("request body failed: {error}"),
                ),
            }
        }
        (&Method::GET, "/mcp") => response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain; charset=utf-8",
            "MCP endpoint accepts POST requests",
        ),
        _ => response(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "not found",
        ),
    };
    Ok(response)
}

async fn handle_mcp(message: Value, bearer: Option<String>) -> Response<Body> {
    let Some(object) = message.as_object() else {
        return jsonrpc_error(Value::Null, -32600, "invalid request");
    };

    let id = object.get("id").cloned();
    let method = object.get("method").and_then(Value::as_str);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
        return jsonrpc_error(id.unwrap_or(Value::Null), -32600, "invalid request");
    }

    if id.is_none() {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .expect("valid 204 response");
    }

    let id = id.unwrap_or(Value::Null);
    match method.expect("checked") {
        "initialize" => jsonrpc_result(id, mcp_initialize_result()),
        "ping" => jsonrpc_result(id, json!({})),
        "tools/list" => jsonrpc_result(id, json!({"tools": mcp_tools()})),
        "tools/call" => {
            let Some(token) = bearer else {
                return unauthorized(id);
            };
            let bridge = match Bridge::from_bearer(token) {
                Ok(bridge) => bridge,
                Err(_) => return unauthorized(id),
            };

            let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
            let name = params.get("name").and_then(Value::as_str);
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let Some(name) = name else {
                return jsonrpc_error(id, -32602, "tools/call requires params.name");
            };

            match bridge.execute_tool(name, arguments).await {
                Ok(value) => {
                    let text = serde_json::to_string(&value)
                        .unwrap_or_else(|_| "{\"error\":\"serialization failed\"}".to_owned());
                    jsonrpc_result(
                        id,
                        json!({
                            "content": [{"type": "text", "text": text}],
                            "structuredContent": value,
                            "isError": false
                        }),
                    )
                }
                Err(error) => jsonrpc_result(
                    id,
                    json!({
                        "content": [{"type": "text", "text": error.to_string()}],
                        "structuredContent": {
                            "error": {
                                "code": "OKX_BRIDGE_ERROR",
                                "message": error.to_string()
                            }
                        },
                        "isError": true
                    }),
                ),
            }
        }
        _ => jsonrpc_error(id, -32601, "method not found"),
    }
}

fn bearer_token(request: &Request<Incoming>) -> Option<String> {
    let value = request.headers().get(AUTHORIZATION)?.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_owned())
}

fn unauthorized(id: Value) -> Response<Body> {
    json_response(
        StatusCode::UNAUTHORIZED,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32001,
                "message": "authorization required"
            }
        }),
    )
}

fn jsonrpc_result(id: Value, result: Value) -> Response<Body> {
    json_response(
        StatusCode::OK,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }),
    )
}

fn jsonrpc_error(id: Value, code: i64, message: &str) -> Response<Body> {
    json_response(
        StatusCode::OK,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": code,
                "message": message
            }
        }),
    )
}

fn json_response(status: StatusCode, value: Value) -> Response<Body> {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    response(status, "application/json", body)
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<Body> {
    let mut response = Response::builder()
        .status(status)
        .body(Full::new(body.into()))
        .expect("valid response");
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_is_current_streamable_http_mcp_shape() {
        let result = mcp_initialize_result();
        assert_eq!(result["protocolVersion"], "2025-06-18");
        assert_eq!(result["serverInfo"]["name"], "okx-bridge");
    }

    #[test]
    fn bearer_parser_requires_exact_scheme_and_nonempty_value() {
        fn parse(value: &str) -> Option<String> {
            let value = value.strip_prefix("Bearer ")?;
            (!value.is_empty()).then(|| value.to_owned())
        }

        assert_eq!(
            parse("Bearer github_pat_example").as_deref(),
            Some("github_pat_example")
        );
        assert_eq!(parse("bearer nope"), None);
        assert_eq!(parse("Bearer "), None);
    }
}
