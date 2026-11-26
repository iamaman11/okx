//! Typed identity and lot reads, shared by both storage layouts.
use super::*;

#[derive(Debug, Clone)]
pub enum TradeIntentLookup {
    IntentId(String),
    IdempotencyKey(String),
    ClientOrderId(String),
    ExchangeOrderId(String),
    /// Preserve the original Status precedence and eager validation of all keys.
    Reference(String),
}

/// Explicit subject kinds for read-only account diagnostics. A public
/// request-id is not an operation-id, and a replacement's cancellation is not
/// necessarily a proven parent of its new intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeDiagnosticSubject {
    Cancel,
    Close,
    Replace,
}

/// One account-scoped SQL observation. No venue readback, trade or recovery
/// capability can be obtained from this snapshot.
#[derive(Debug, Clone)]
pub struct TradeSubjectDiagnosticSnapshot {
    pub observed_at_ms: i64,
    pub operation: Option<OperationReceipt>,
    pub intent: Option<StoredIntent>,
    /// A key-derived cancellation candidate, not a proven replace relation.
    pub unverified_cancel_candidate: Option<OperationReceipt>,
}

impl AccountStore {
    pub(crate) fn trade_subject_diagnostic_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        subject: TradeDiagnosticSubject,
        key: &str,
        observed_at_ms: i64,
    ) -> StoreResult<TradeSubjectDiagnosticSnapshot> {
        if !(8..=128).contains(&key.len())
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(StoreError::Invalid("INVALID_TRADE_SUBJECT_REFERENCE"));
        }
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let mut snapshot = TradeSubjectDiagnosticSnapshot {
            observed_at_ms,
            operation: None,
            intent: None,
            unverified_cancel_candidate: None,
        };
        match subject {
            TradeDiagnosticSubject::Cancel => {
                let receipt = Self::operation_sql(db, scope, key)?;
                if let Some(ref receipt) = receipt {
                    if receipt.kind != "cancel" {
                        return Err(StoreError::Invalid("CANCEL_RECEIPT_KIND_MISMATCH"));
                    }
                    if receipt.result_ref != "POSTING" {
                        let intent = Self::lookup_trade_intent_sql(
                            db,
                            scope,
                            &TradeIntentLookup::IntentId(receipt.result_ref.clone()),
                        )?
                        .ok_or(StoreError::Invalid("CANCEL_RECEIPT_INTENT_MISSING"))?;
                        snapshot.intent = Some(intent);
                    }
                }
                snapshot.operation = receipt;
            }
            TradeDiagnosticSubject::Close => {
                snapshot.intent = Self::lookup_trade_intent_sql(
                    db,
                    scope,
                    &TradeIntentLookup::IdempotencyKey(key.to_owned()),
                )?;
                if snapshot
                    .intent
                    .as_ref()
                    .is_some_and(|intent| intent.plan.operation != "close")
                {
                    return Err(StoreError::Invalid("CLOSE_INTENT_OPERATION_MISMATCH"));
                }
            }
            TradeDiagnosticSubject::Replace => {
                snapshot.intent = Self::lookup_trade_intent_sql(
                    db,
                    scope,
                    &TradeIntentLookup::IdempotencyKey(key.to_owned()),
                )?;
                if let Some(ref intent) = snapshot.intent {
                    if intent.plan.operation != "open"
                        || intent.request_hash.len() != 64
                        || !intent
                            .request_hash
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                    {
                        return Err(StoreError::Invalid("REPLACE_INTENT_EVIDENCE_INVALID"));
                    }
                    // The original order.reference was removed before the
                    // replacement intent was persisted. This hash-derived
                    // key might also be supplied by an unrelated cancellation.
                    // Expose it ONLY as an unverified candidate; never assert
                    // that it targeted the old order or authorized a new POST.
                    let candidate = Self::operation_sql(
                        db,
                        scope,
                        &format!("replace_cancel_{}", &intent.request_hash[..40]),
                    )?;
                    if let Some(ref candidate) = candidate
                        && candidate.kind != "cancel"
                    {
                        return Err(StoreError::Invalid("REPLACE_CANDIDATE_KIND_MISMATCH"));
                    }
                    snapshot.unverified_cancel_candidate = candidate;
                }
            }
        }
        Ok(snapshot)
    }

    pub fn lookup_trade_intent(
        &self,
        lookup: &TradeIntentLookup,
    ) -> StoreResult<Option<StoredIntent>> {
        Self::lookup_trade_intent_sql(&self.connection, None, lookup)
    }

    pub(crate) fn lookup_trade_intent_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        lookup: &TradeIntentLookup,
    ) -> StoreResult<Option<StoredIntent>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let find = |column, key: &str| Self::find_intent_sql(db, scope, column, key);
        match lookup {
            TradeIntentLookup::IntentId(id) => find("intent_id", id),
            TradeIntentLookup::IdempotencyKey(key) => find("idempotency_key", key),
            TradeIntentLookup::ClientOrderId(key) => find("client_order_id", key),
            TradeIntentLookup::ExchangeOrderId(key) => find("exchange_order_id", key),
            TradeIntentLookup::Reference(key) => Ok(find("intent_id", key)?
                .or(find("client_order_id", key)?)
                .or(find("exchange_order_id", key)?)
                .or(find("idempotency_key", key)?)),
        }
    }
}
