//! Original immutable quote/admission validation in the caller's SQL scope.
//! A known quote is not ALL-IN admission or an exchange-send capability.
use super::super::*;
use sha2::{Digest, Sha256};
impl AccountStore {
    fn entry_source_identity_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<(String, String)> {
        match scope {
            None => Ok(db.query_row(
                "SELECT account_key,installation_id FROM account_meta WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?),
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                Ok(db.query_row("SELECT account_key,installation_id FROM account_meta WHERE singleton=1 AND installation_id=?1 AND account_key=?2",params![installation,account],|r|Ok((r.get(0)?,r.get(1)?)))?)
            }
        }
    }
    pub(crate) fn verified_stored_entry_quote_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        quote: &EntryQuoteEvidence,
    ) -> StoreResult<StoredIntent> {
        let (account_key, installation_id) = Self::entry_source_identity_sql(db, scope)?;

        let intent = Self::find_intent_sql(db, scope, "intent_id", &quote.intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_INTENT_MISSING"))?;
        let maker = Decimal::from_str(&quote.maker_rate)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_FEE_RATE_INVALID"))?;
        let taker = Decimal::from_str(&quote.taker_rate)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_FEE_RATE_INVALID"))?;
        if quote.schema != "okx.entry-known-quote/v1"
            || quote.account_key != account_key
            || quote.installation_id != installation_id
            || quote.plan_hash != intent.plan_hash
            || quote.policy_revision != intent.policy_revision
            || quote.instrument_id != intent.instrument_id
            || !intent.risk_increase
            || quote.quantity != intent.plan.quantity
            || quote.limit_price != intent.plan.limit_price
            || quote.contract_value != intent.plan.contract_value
            || quote.quantity <= Decimal::ZERO
            || quote.limit_price <= Decimal::ZERO
            || quote.contract_value <= Decimal::ZERO
            || quote.fee_rate < Decimal::ZERO
            || quote.fee_rate != maker.abs().max(taker.abs())
            || quote.fee_level.is_empty()
            || quote.fee_exchange_at <= 0
            || quote.usd_per_usdt <= Decimal::ZERO
            || quote.fee_received_at <= 0
            || quote.conversion_received_at <= 0
            || quote.observed_at <= 0
            || quote.fee_received_at > quote.observed_at
            || quote.fee_exchange_at > quote.observed_at
            || quote.conversion_received_at > quote.observed_at
            || quote.observed_at - quote.fee_received_at > 5_000
            || quote.observed_at - quote.fee_exchange_at > 5_000
            || quote.observed_at - quote.conversion_received_at > 5_000
            || quote.fee_generation.len() != 71
            || !quote.fee_generation.starts_with("sha256:")
            || !quote.fee_generation[7..]
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
            || quote.reference_generation.is_empty()
            || quote.conversion_digest.len() != 64
            || !quote
                .conversion_digest
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(StoreError::Invalid("ENTRY_QUOTE_SCOPE_UNVERIFIED"));
        }
        let tariff_bytes = serde_json::to_vec(&(
            &quote.instrument_id,
            &quote.reference_generation,
            &quote.fee_level,
            &quote.maker_rate,
            &quote.taker_rate,
            true,
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"))?;
        let tariff_digest = format!("sha256:{:x}", Sha256::digest(tariff_bytes));
        if tariff_digest != quote.fee_generation {
            return Err(StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"));
        }
        let notional = quote
            .quantity
            .checked_mul(quote.contract_value)
            .and_then(|v| v.checked_mul(quote.limit_price))
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?;
        let fee = notional
            .checked_mul(quote.fee_rate)
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToPositiveInfinity);
        let total_usdt = notional
            .checked_add(fee)
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?;
        let usd = total_usdt
            .checked_mul(quote.usd_per_usdt)
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?
            .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity);
        if (notional, fee, total_usdt, usd)
            != (
                quote.notional_usdt,
                quote.fee_usdt,
                quote.total_usdt,
                quote.total_usd,
            )
        {
            return Err(StoreError::Invalid("ENTRY_QUOTE_ARITHMETIC_MISMATCH"));
        }
        Ok(intent)
    }
    pub(crate) fn entry_quote_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        intent_id: &str,
    ) -> StoreResult<Option<EntryQuoteEvidence>> {
        let name = format!("entry-quote:{intent_id}");
        let Some(receipt) = Self::operation_sql(db, scope, &name)? else {
            return Ok(None);
        };
        if receipt.kind != "ENTRY_QUOTE_UNPROVEN" {
            return Err(StoreError::Invalid("ENTRY_QUOTE_INVALID_RECEIPT"));
        }
        let quote: EntryQuoteEvidence = serde_json::from_str(&receipt.result_ref)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_CORRUPT"))?;
        if quote.intent_id != intent_id
            || quote.plan_hash != receipt.request_hash
            || quote.observed_at != receipt.committed_at
        {
            return Err(StoreError::Invalid("ENTRY_QUOTE_CORRUPT"));
        }
        // Revalidate against the actual durable plan and exact arithmetic on
        // EVERY read, including after Windows restart and v2 DB reopen.
        Self::verified_stored_entry_quote_sql(db, scope, &quote)?;
        Ok(Some(quote))
    }
    pub(crate) fn verified_entry_admission_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        proof: &EntryAdmissionEvidence,
    ) -> StoreResult<()> {
        let (account_key, installation_id) = Self::entry_source_identity_sql(db, scope)?;

        let quote = Self::entry_quote_sql(db, scope, &proof.intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_QUOTE_MISSING"))?;
        let bytes = serde_json::to_vec(&quote)
            .map_err(|_| StoreError::Invalid("ENTRY_ADMISSION_QUOTE_DIGEST_INVALID"))?;
        let expected_quote_digest = format!("{:x}", Sha256::digest(bytes));
        let computed = quote
            .total_usdt
            .checked_mul(proof.admission_usd_per_usdt)
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_UPPER_OVERFLOW"))?
            .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity)
            .checked_add(proof.other_entry_costs_upper_usd)
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_UPPER_OVERFLOW"))?;
        let digest_valid = |v: &str| {
            v.len() == 64
                && v.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        };
        if proof.schema != "okx.entry-admission/v2"
            || proof.account_key != account_key
            || proof.installation_id != installation_id
            || proof.intent_id != quote.intent_id
            || proof.plan_hash != quote.plan_hash
            || !digest_valid(&proof.quote_digest)
            || proof.quote_digest != expected_quote_digest
            || !digest_valid(&proof.entry_proof_digest)
            || proof.admission_usd_per_usdt != quote.usd_per_usdt
            || proof.admission_usd_per_usdt <= Decimal::ZERO
            || proof.other_entry_costs_upper_usd < Decimal::ZERO
            || proof.entry_upper_usd != computed
            || proof.entry_upper_usd < quote.total_usd
            || proof.entry_upper_usd <= Decimal::ZERO
            || proof.observed_at < quote.observed_at
            || proof.observed_at - quote.observed_at > 5_000
        {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_SOURCE_UNVERIFIED"));
        }
        Ok(())
    }
    pub(crate) fn entry_admission_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<Option<EntryAdmissionEvidence>> {
        let key = format!("entry-admission:{id}");
        let Some(receipt) = Self::operation_sql(db, scope, &key)? else {
            return Ok(None);
        };
        if receipt.kind != "ENTRY_ADMISSION_PROVEN" {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_INVALID_RECEIPT"));
        }
        let proof: EntryAdmissionEvidence = serde_json::from_str(&receipt.result_ref)
            .map_err(|_| StoreError::Invalid("ENTRY_ADMISSION_CORRUPT"))?;
        if proof.intent_id != id
            || proof.plan_hash != receipt.request_hash
            || proof.observed_at != receipt.committed_at
        {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_CORRUPT"));
        }
        Self::verified_entry_admission_sql(db, scope, &proof)?;
        Ok(Some(proof))
    }
}
