//! Actual producer observations are data; original SQL sources and activation
//! remain mandatory. No public constructor for VerifiedCensus or send claims.
use super::*;
use crate::{AccountStore, CentralReceipt, CommittedVenueSend, StoreError, StoreResult};
use okx_kernel::risk::{
    model::{PostCausalProof, RiskProposal},
    money::RiskAmount,
};
use std::collections::BTreeMap;
#[derive(Debug, Clone)]
pub struct ReportedRiskCensus {
    pub source_digest: String,
    pub generated_at_ms: i64,
    pub read_floor_ms: i64,
    pub occupied: BTreeMap<String, String>,
    pub cumulative_loss_usd: BTreeMap<String, String>,
}
#[derive(Debug, Clone)]
pub struct NativeReserveRequest {
    pub venue: String,
    pub uid: String,
    pub run_id: String,
    pub intent_id: String,
    pub strategy_id: String,
    pub instrument_id: String,
    pub plan_sha256: String,
    pub entry_proof_digest: String,
    pub conversion_digest: String,
    pub conversion_rate: String,
    pub loss_usdt: String,
    pub loss_usd: String,
    pub entry_upper_usd: String,
    pub native_incarnation: String,
    pub conversion_observed_at_ms: i64,
    pub policy_revision: u64,
}
#[derive(Debug, Clone)]
pub struct NativePostRequest {
    pub reservation_key: String,
    pub plan_sha256: String,
    pub source_proof_digest: String,
    pub policy_revision: u64,
    pub expected_sequence: u64,
    pub census: ReportedRiskCensus,
    pub causal: PostCausalProof,
}
pub enum NativeReserveDecision {
    Held {
        receipt: CentralReceipt,
        book_sequence: u64,
        policy_revision: u64,
    },
    Denied {
        reason: String,
    },
}
pub enum NativePostDecision {
    Authorized {
        claim: CommittedVenueSend,
        book_sequence: u64,
        existing_modelled_loss_usdt: String,
    },
    Denied {
        reason: String,
    },
}
fn amount(v: &str) -> StoreResult<RiskAmount> {
    RiskAmount::parse(v).map_err(|_| StoreError::Invalid("NATIVE_ADMISSION_MONEY_INVALID"))
}
fn verify_reserved_loss(
    planned_loss_usd: rust_decimal::Decimal,
    rate: rust_decimal::Decimal,
    loss_usdt: &str,
    loss_usd: &str,
) -> StoreResult<()> {
    let (expected_usdt, expected_usd) = crate::risk::native_reserved_loss(planned_loss_usd, rate)?;
    if amount(loss_usdt)? != amount(&expected_usdt)? || amount(loss_usd)? != amount(&expected_usd)?
    {
        return Err(StoreError::Invalid("NATIVE_RESERVE_ORIGINAL_LOSS_MISMATCH"));
    }
    Ok(())
}
fn bind_census(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    reported: &ReportedRiskCensus,
    now: i64,
) -> StoreResult<crate::risk::VerifiedCensus> {
    if reported.generated_at_ms <= 0
        || reported.generated_at_ms > now
        || now - reported.generated_at_ms > 5000
        || reported.read_floor_ms <= 0
        || reported.read_floor_ms > reported.generated_at_ms
        || reported.source_digest.len() != 64
        || !reported
            .source_digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(StoreError::Invalid("NATIVE_ADMISSION_CENSUS_INVALID"));
    }
    let (generation, policy_revision):(i64,i64)=tx.query_row("SELECT generation,policy_revision FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2 AND singleton=1",rusqlite::params![installation,campaign],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let mut q=tx.prepare("SELECT account_key FROM risk_policy_account WHERE installation_id=?1 AND campaign_id=?2 ORDER BY account_key")?;
    let accounts = q
        .query_map(rusqlite::params![installation, campaign], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if accounts.is_empty()
        || accounts.len() > 32
        || accounts.iter().ne(reported.occupied.keys())
        || accounts.iter().ne(reported.cumulative_loss_usd.keys())
    {
        return Err(StoreError::Invalid("NATIVE_ADMISSION_COHORT_MISMATCH"));
    }
    for account in &accounts {
        let scope = Some((installation, account.as_str()));
        let policy = AccountStore::account_policy_sql(tx, scope)?;
        if policy.campaign_id != campaign || policy.revision != policy_revision {
            return Err(StoreError::Invalid(
                "NATIVE_ADMISSION_ACCOUNT_POLICY_MISMATCH",
            ));
        }
        let financial = AccountStore::financial_risk_sql(tx, scope, now, None)?;
        if !financial.nav_complete || financial.source_as_of < reported.read_floor_ms {
            return Err(StoreError::Invalid(
                "NATIVE_ADMISSION_FINANCIAL_SOURCE_INCOMPLETE",
            ));
        }
        if amount(&policy.campaign_loss.to_string())?
            != amount(&reported.cumulative_loss_usd[account])?
        {
            return Err(StoreError::Invalid("NATIVE_ADMISSION_LOSS_SOURCE_MISMATCH"));
        }
        // The current certified producer accepts managed positions only;
        // their occupancy is already represented by local risk reservations.
        // Nonzero external occupancy needs a future independently bound source.
        if !amount(&reported.occupied[account])?.is_zero() {
            return Err(StoreError::Invalid(
                "NATIVE_ADMISSION_EXTERNAL_OCCUPANCY_UNPROVEN",
            ));
        }
    }
    let parse = |m: &BTreeMap<String, String>| {
        m.iter()
            .map(|(k, v)| Ok((k.clone(), amount(v)?)))
            .collect::<StoreResult<BTreeMap<_, _>>>()
    };
    Ok(crate::risk::VerifiedCensus {
        complete: true,
        generation: u64::try_from(generation)
            .ok()
            .filter(|g| *g > 0)
            .ok_or(StoreError::Invalid("NATIVE_ADMISSION_GENERATION_INVALID"))?,
        source_digest: reported.source_digest.clone(),
        generated_at_ms: reported.generated_at_ms,
        read_floor_ms: reported.read_floor_ms,
        occupied: parse(&reported.occupied)?,
        cumulative_loss_usd: parse(&reported.cumulative_loss_usd)?,
    })
}
pub(crate) fn reserve_reported_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    request: NativeReserveRequest,
    reported: ReportedRiskCensus,
    now: i64,
) -> StoreResult<NativeReserveDecision> {
    let account = format!("{}:real:{}", request.venue, request.uid);
    let source =
        AccountStore::reserve_source_sql(tx, Some((installation, &account)), &request.intent_id)?;
    if source.intent.plan_hash != request.plan_sha256
        || source.intent.plan.strategy_instance != request.strategy_id
        || source.intent.instrument_id != request.instrument_id
        || source.intent.policy_revision as u64 != request.policy_revision
        || amount(&source.admission.entry_upper_usd.to_string())?
            != amount(&request.entry_upper_usd)?
        || source.admission.entry_proof_digest != request.entry_proof_digest
        || amount(&source.admission.admission_usd_per_usdt.to_string())?
            != amount(&request.conversion_rate)?
    {
        return Err(StoreError::Invalid(
            "NATIVE_RESERVE_ORIGINAL_SOURCE_MISMATCH",
        ));
    }
    verify_reserved_loss(
        source.intent.plan.planned_loss_usd,
        source.admission.admission_usd_per_usdt,
        &request.loss_usdt,
        &request.loss_usd,
    )?;
    let census = bind_census(tx, installation, campaign, &reported, now)?;
    let input = crate::risk::ReserveInput {
        venue: request.venue,
        uid: request.uid,
        run_id: request.run_id,
        intent_id: request.intent_id,
        strategy_id: request.strategy_id,
        instrument_id: request.instrument_id,
        plan_sha256: request.plan_sha256,
        entry_proof_digest: request.entry_proof_digest,
        conversion_digest: request.conversion_digest,
        conversion_observed_at_ms: request.conversion_observed_at_ms,
        conversion_rate: amount(&request.conversion_rate)?,
        native_incarnation: request.native_incarnation,
        proposal: RiskProposal {
            loss_usdt: amount(&request.loss_usdt)?,
            loss_usd: amount(&request.loss_usd)?,
            entry_upper_usd: amount(&request.entry_upper_usd)?,
        },
        policy_revision: request.policy_revision,
        now_ms: now,
    };
    let receipt = super::native_reservation_receipt_sql(tx, installation, campaign, &input)?;
    match super::apply_original_unified_finance_transaction(
        tx,
        installation,
        UnifiedFinanceWrite::ReserveNative {
            campaign: campaign.into(),
            input,
            census,
        },
    )? {
        UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::Reserved {
            book_sequence,
            ..
        }) => Ok(NativeReserveDecision::Held {
            receipt,
            book_sequence,
            policy_revision: request.policy_revision,
        }),
        UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::Denied { reason }) => {
            Ok(NativeReserveDecision::Denied { reason })
        }
        _ => Err(StoreError::Invalid("NATIVE_RESERVE_RECEIPT_UNEXPECTED")),
    }
}
impl UnifiedFinanceHandle {
    pub async fn reserve_reported(
        &self,
        campaign: String,
        request: NativeReserveRequest,
        census: ReportedRiskCensus,
    ) -> StoreResult<NativeReserveDecision> {
        let installation = self.installation.clone();
        self.submit_before(
            Some(std::time::Instant::now() + std::time::Duration::from_secs(10)),
            move |db| {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                require_verified_live_finance_authority(&tx, &installation)?;
                let result = reserve_reported_sql(
                    &tx,
                    &installation,
                    &campaign,
                    request,
                    census,
                    super::opening_ingress::owner_now_ms()?,
                )?;
                tx.commit().map_err(|_| {
                    StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
                })?;
                Ok(result)
            },
        )
        .await
    }
    pub async fn begin_post_reported(
        &self,
        campaign: String,
        request: NativePostRequest,
    ) -> StoreResult<NativePostDecision> {
        let installation = self.installation.clone();
        self.submit_before(
            Some(std::time::Instant::now() + std::time::Duration::from_secs(10)),
            move |db| {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                require_verified_live_finance_authority(&tx, &installation)?;
                let now = super::opening_ingress::owner_now_ms()?;
                let census = bind_census(&tx, &installation, &campaign, &request.census, now)?;
                let input = crate::risk::PostFenceInput {
                    reservation_key: request.reservation_key.clone(),
                    plan_sha256: request.plan_sha256.clone(),
                    policy_revision: request.policy_revision,
                    expected_sequence: request.expected_sequence,
                    now_ms: now,
                    source_proof_digest: request.source_proof_digest,
                    census,
                    causal: request.causal,
                };
                let decision = apply_original_unified_finance_transaction(
                    &tx,
                    &installation,
                    UnifiedFinanceWrite::BeginPost {
                        campaign: campaign.clone(),
                        input,
                    },
                )?;
                match decision {
                    UnifiedFinanceWriteReceipt::Risk(
                        crate::risk::UnifiedRiskDecision::PostAuthorized {
                            book_sequence,
                            existing_modelled_loss_usdt,
                            ..
                        },
                    ) => {
                        let (account, intent): (String, String) = tx.query_row(
                            "SELECT account_key,intent_id FROM risk_entries
                         WHERE installation_id=?1 AND campaign_id=?2
                         AND reservation_key=?3 AND plan_sha256=?4 AND state='POSTED'",
                            rusqlite::params![
                                installation,
                                campaign,
                                request.reservation_key,
                                request.plan_sha256
                            ],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )?;
                        tx.commit().map_err(|_| {
                            StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
                        })?;
                        Ok(NativePostDecision::Authorized {
                            claim: CommittedVenueSend::after_commit(
                                installation,
                                account,
                                intent,
                                request.plan_sha256,
                            ),
                            book_sequence,
                            existing_modelled_loss_usdt,
                        })
                    }
                    UnifiedFinanceWriteReceipt::Risk(
                        crate::risk::UnifiedRiskDecision::Denied { reason },
                    ) => {
                        tx.commit().map_err(|_| {
                            StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
                        })?;
                        Ok(NativePostDecision::Denied { reason })
                    }
                    _ => Err(StoreError::Invalid("NATIVE_POST_RECEIPT_UNEXPECTED")),
                }
            },
        )
        .await
    }
}

#[cfg(test)]
mod sizing_tests {
    use super::*;
    fn dec(value: &str) -> rust_decimal::Decimal {
        value.parse().unwrap()
    }

    #[test]
    fn original_plan_rejects_self_consistent_under_reserve() {
        assert!(matches!(
            verify_reserved_loss(dec("5"), dec("1"), "1", "1"),
            Err(StoreError::Invalid("NATIVE_RESERVE_ORIGINAL_LOSS_MISMATCH"))
        ));
        assert!(verify_reserved_loss(dec("5"), dec("1"), "5.00", "5.0").is_ok());
    }

    #[test]
    fn non_parity_fx_requires_same_exact_native_product() {
        let (usdt, usd) = crate::risk::native_reserved_loss(dec("1"), dec("1.123456789")).unwrap();
        assert!(verify_reserved_loss(dec("1"), dec("1.123456789"), &usdt, &usd).is_ok());
        assert!(verify_reserved_loss(dec("1"), dec("1.123456789"), &usdt, "1.000000001").is_err());
        assert!(verify_reserved_loss(dec("1"), dec("1.123456789"), "0.1", "0.1123456789").is_err());
    }
}

#[cfg(test)]
mod census_tests {
    use super::*;
    const INSTALLATION: &str = "census-installation";
    const CAMPAIGN: &str = "census-campaign";
    const ACCOUNT: &str = "okx:real:account-a";
    const NOW: i64 = 20_000;

    fn original_sources() -> rusqlite::Connection {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        // Use the original schema and original AccountStore read functions.
        // No finance activation receipt is created; these are read-only tests.
        db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        db.execute(
            "INSERT INTO account_meta VALUES(1,?1,?2,2,1,'test','init','test')",
            rusqlite::params![ACCOUNT, INSTALLATION],
        )
        .unwrap();
        db.execute(
            "INSERT INTO account_policy VALUES(?1,?2,1,7,0,?3,'10','2.5')",
            rusqlite::params![INSTALLATION, ACCOUNT, CAMPAIGN],
        )
        .unwrap();
        db.execute(
            "INSERT INTO nav_watermarks VALUES(?1,?2,'account','100','100',?3,'COMPLETE')",
            rusqlite::params![INSTALLATION, ACCOUNT, NOW],
        )
        .unwrap();
        db.execute("INSERT INTO risk_meta VALUES(1,?1,?2,7,3,0,1,'10','0','10','0','0','10','0','0','0','0')",
            rusqlite::params![CAMPAIGN, INSTALLATION]).unwrap();
        db.execute(
            "INSERT INTO risk_account_occupied VALUES(?1,?2,?3,'0')",
            rusqlite::params![INSTALLATION, CAMPAIGN, ACCOUNT],
        )
        .unwrap();
        db.execute(
            "INSERT INTO risk_policy_account VALUES(?1,?2,?3,'10','10','10')",
            rusqlite::params![INSTALLATION, CAMPAIGN, ACCOUNT],
        )
        .unwrap();
        db
    }

    fn reported() -> ReportedRiskCensus {
        ReportedRiskCensus {
            source_digest: "a".repeat(64),
            generated_at_ms: NOW,
            read_floor_ms: NOW - 1,
            occupied: BTreeMap::from([(ACCOUNT.into(), "0".into())]),
            cumulative_loss_usd: BTreeMap::from([(ACCOUNT.into(), "2.5".into())]),
        }
    }

    fn check(
        db: &mut rusqlite::Connection,
        source: &ReportedRiskCensus,
    ) -> StoreResult<crate::risk::VerifiedCensus> {
        let tx = db.transaction().unwrap();
        let before = tx.total_changes();
        let result = bind_census(&tx, INSTALLATION, CAMPAIGN, source, NOW);
        assert_eq!(
            tx.total_changes(),
            before,
            "census verification must not write"
        );
        result
    }

    #[test]
    fn native_census_accepts_original_financial_and_policy_sources_without_writes() {
        let mut db = original_sources();
        let verified = check(&mut db, &reported()).unwrap();
        assert!(verified.complete);
        assert_eq!(verified.generation, 3);
        assert_eq!(
            verified.cumulative_loss_usd[ACCOUNT].canonical().unwrap(),
            "2.5"
        );
    }

    #[test]
    fn native_census_rejects_missing_stale_incomplete_or_pre_floor_nav() {
        for sql in [
            "DELETE FROM nav_watermarks",
            "UPDATE nav_watermarks SET time=14999",
            "UPDATE nav_watermarks SET coverage='PARTIAL'",
            "UPDATE nav_watermarks SET time=19998",
        ] {
            let mut db = original_sources();
            db.execute_batch(sql).unwrap();
            assert!(matches!(
                check(&mut db, &reported()),
                Err(StoreError::Invalid(
                    "NATIVE_ADMISSION_FINANCIAL_SOURCE_INCOMPLETE"
                ))
            ));
        }
    }

    #[test]
    fn native_census_rejects_wrong_loss_foreign_roster_or_unproven_external_occupancy() {
        let mut db = original_sources();
        let mut source = reported();
        source
            .cumulative_loss_usd
            .insert(ACCOUNT.into(), "1".into());
        assert!(matches!(
            check(&mut db, &source),
            Err(StoreError::Invalid("NATIVE_ADMISSION_LOSS_SOURCE_MISMATCH"))
        ));
        source = reported();
        source.occupied.insert("foreign-account".into(), "0".into());
        assert!(matches!(
            check(&mut db, &source),
            Err(StoreError::Invalid("NATIVE_ADMISSION_COHORT_MISMATCH"))
        ));
        source = reported();
        source.occupied.insert(ACCOUNT.into(), "1".into());
        assert!(matches!(
            check(&mut db, &source),
            Err(StoreError::Invalid(
                "NATIVE_ADMISSION_EXTERNAL_OCCUPANCY_UNPROVEN"
            ))
        ));
    }

    #[test]
    fn native_census_rejects_campaign_or_revision_mismatch() {
        for sql in [
            "UPDATE account_policy SET campaign_id='foreign'",
            "UPDATE account_policy SET revision=6",
        ] {
            let mut db = original_sources();
            db.execute_batch(sql).unwrap();
            assert!(matches!(
                check(&mut db, &reported()),
                Err(StoreError::Invalid(
                    "NATIVE_ADMISSION_ACCOUNT_POLICY_MISMATCH"
                ))
            ));
        }
    }
}
