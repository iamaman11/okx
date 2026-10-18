//! Account cash transfers are venue facts, never evidence of strategy PnL
//! or of an allocation to a particular strategy run.
//! OKX account bill type=1, subType=11/12 (Get bill types).
use std::str::FromStr;

use rust_decimal::Decimal;

#[derive(Clone, Copy)]
pub(crate) struct AccountBillSource<'a> {
    pub bill_type: &'a str,
    pub sub_type: &'a str,
    pub currency: &'a str,
    pub balance_change: Option<&'a str>,
    pub instrument_id: &'a str,
    pub order_id: Option<&'a str>,
    pub trade_id: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
    pub fee: Option<&'a str>,
    pub pnl: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountBillClass {
    TransferIn(Decimal),
    TransferOut(Decimal),
    UnverifiedTransfer,
    Other,
}

impl AccountBillClass {
    pub fn label(self) -> &'static str {
        match self {
            Self::TransferIn(_) => "ACCOUNT_TRANSFER_IN",
            Self::TransferOut(_) => "ACCOUNT_TRANSFER_OUT",
            Self::UnverifiedTransfer => "UNVERIFIED_ACCOUNT_TRANSFER",
            Self::Other => "UNLINKED_OTHER_BILL",
        }
    }
}

fn absent_native_id(value: Option<&str>) -> bool {
    value.is_none_or(|v| v.trim().is_empty() || v == "0")
}

fn zero_or_absent(value: Option<&str>) -> bool {
    value.is_none_or(|v| v.trim().is_empty() || Decimal::from_str(v).ok() == Some(Decimal::ZERO))
}

/// Classification is deliberately strict and account-scoped.
/// Numeric amount is the signed native balance delta; there is no inference
/// from a bill timestamp, order symbol, or the strategy's declared capital.
/// A malformed/unknown type=1 bill stays unverified, never silently becomes PnL.
pub(crate) fn classify_native_account_bill(b: AccountBillSource<'_>) -> AccountBillClass {
    if b.bill_type != "1" {
        return AccountBillClass::Other;
    }
    if !matches!(b.sub_type, "11" | "12")
        || b.currency.trim().is_empty()
        || !b.instrument_id.trim().is_empty()
        || !absent_native_id(b.order_id)
        || !absent_native_id(b.trade_id)
        || !absent_native_id(b.client_order_id)
        || !zero_or_absent(b.fee)
        || !zero_or_absent(b.pnl)
    {
        return AccountBillClass::UnverifiedTransfer;
    }
    let Some(value) = b.balance_change.and_then(|v| Decimal::from_str(v).ok()) else {
        return AccountBillClass::UnverifiedTransfer;
    };
    match (b.sub_type, value.cmp(&Decimal::ZERO)) {
        ("11", std::cmp::Ordering::Greater) => AccountBillClass::TransferIn(value),
        ("12", std::cmp::Ordering::Less) => AccountBillClass::TransferOut(value),
        _ => AccountBillClass::UnverifiedTransfer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> AccountBillSource<'static> {
        AccountBillSource {
            bill_type: "1",
            sub_type: "11",
            currency: "USDT",
            balance_change: Some("127"),
            instrument_id: "",
            order_id: None,
            trade_id: Some("0"),
            client_order_id: None,
            fee: Some("0"),
            pnl: Some("0"),
        }
    }

    #[test]
    fn venue_transfer_is_account_cashflow_not_trading_pnl() {
        assert_eq!(
            classify_native_account_bill(source()),
            AccountBillClass::TransferIn(Decimal::from(127))
        );
        let outgoing = AccountBillSource {
            sub_type: "12",
            balance_change: Some("-7.5"),
            ..source()
        };
        assert_eq!(
            classify_native_account_bill(outgoing),
            AccountBillClass::TransferOut(Decimal::new(-75, 1))
        );
    }

    #[test]
    fn malformed_or_trade_linked_transfer_fails_closed() {
        for case in [
            AccountBillSource {
                sub_type: "12",
                ..source()
            },
            AccountBillSource {
                balance_change: Some("-1"),
                ..source()
            },
            AccountBillSource {
                balance_change: None,
                ..source()
            },
            AccountBillSource {
                balance_change: Some("NaN"),
                ..source()
            },
            AccountBillSource {
                order_id: Some("1234"),
                ..source()
            },
            AccountBillSource {
                trade_id: Some("55"),
                ..source()
            },
            AccountBillSource {
                client_order_id: Some("external"),
                ..source()
            },
            AccountBillSource {
                instrument_id: "BTC-USDT-SWAP",
                ..source()
            },
            AccountBillSource {
                fee: Some("-0.1"),
                ..source()
            },
            AccountBillSource {
                pnl: Some("1"),
                ..source()
            },
            AccountBillSource {
                sub_type: "200",
                ..source()
            },
            AccountBillSource {
                currency: "",
                ..source()
            },
        ] {
            assert_eq!(
                classify_native_account_bill(case),
                AccountBillClass::UnverifiedTransfer
            );
        }
        assert_eq!(
            classify_native_account_bill(AccountBillSource {
                bill_type: "2",
                ..source()
            }),
            AccountBillClass::Other
        );
    }
}
