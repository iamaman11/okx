use serde::{Deserialize, Serialize};

use crate::{OkxError, OkxRestClient};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FundingBalance {
    #[serde(default)]
    pub ccy: String,
    #[serde(default)]
    pub bal: String,
    #[serde(rename = "frozenBal", default)]
    pub frozen_balance: String,
    #[serde(rename = "availBal", default)]
    pub available_balance: String,
}

#[derive(Clone)]
pub struct AssetApi {
    client: OkxRestClient,
}

impl AssetApi {
    pub fn new(client: OkxRestClient) -> Self {
        Self { client }
    }

    pub async fn funding_balances(&self) -> Result<Vec<FundingBalance>, OkxError> {
        self.client.private_get("/api/v5/asset/balances", &[]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_funding_balance() {
        let row: FundingBalance = serde_json::from_str(
            r#"{"ccy":"USDT","bal":"12.5","frozenBal":"2","availBal":"10.5"}"#,
        )
        .expect("funding balance");
        assert_eq!(row.ccy, "USDT");
        assert_eq!(row.bal, "12.5");
        assert_eq!(row.frozen_balance, "2");
        assert_eq!(row.available_balance, "10.5");
    }
}
