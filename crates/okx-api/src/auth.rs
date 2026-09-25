use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::OkxError;

type HmacSha256 = Hmac<Sha256>;

pub fn timestamp_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn sign(
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &str,
    secret: &str,
) -> Result<String, OkxError> {
    let prehash = format!(
        "{timestamp}{}{request_path}{body}",
        method.to_ascii_uppercase()
    );

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| OkxError::Crypto(e.to_string()))?;
    mac.update(prehash.as_bytes());

    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::sign;

    #[test]
    fn signing_is_deterministic_and_includes_query_string() {
        let signature = sign(
            "2020-12-08T09:08:57.715Z",
            "GET",
            "/api/v5/account/balance?ccy=BTC",
            "",
            "test-secret",
        )
        .expect("signature");

        assert_eq!(signature, "5KlCItRxE039QKll2OJlbYeUcSiPGR/z10UR7bbl68o=");
    }
}
