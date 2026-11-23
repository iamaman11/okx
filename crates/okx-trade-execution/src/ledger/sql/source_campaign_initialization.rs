//! Original scoped RiskBook tables, one caller-owned transaction.
use crate::account_worker::{NativeCampaignPolicy, NativeCampaignPolicyReceipt};
use crate::{AccountStore, StoreError, StoreResult};
use okx_domain_core::risk::money::RiskAmount;
use rusqlite::{OptionalExtension, Transaction, params};

fn require(value: bool, reason: &'static str) -> StoreResult<()> {
    if value {
        Ok(())
    } else {
        Err(StoreError::Invalid(reason))
    }
}
fn amount(raw: &str) -> StoreResult<RiskAmount> {
    RiskAmount::parse(raw).map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_MONEY_INVALID"))
}

impl AccountStore {
    pub(crate) fn apply_native_campaign_policy_sql(
        tx: &Transaction<'_>,
        installation: &str,
        p: &NativeCampaignPolicy,
    ) -> StoreResult<NativeCampaignPolicyReceipt> {
        let roster=tx.prepare("SELECT account_key FROM account_meta WHERE installation_id=?1 ORDER BY account_key")?
            .query_map([installation],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        require(
            roster
                == p.accounts
                    .iter()
                    .map(|a| a.account_key.clone())
                    .collect::<Vec<_>>(),
            "NATIVE_CAMPAIGN_COHORT_MISMATCH",
        )?;
        for a in &p.accounts {
            let source:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM account_meta m
                JOIN operation_receipts i ON i.installation_id=m.installation_id AND i.account_key=m.account_key
                    AND i.operation_id=m.initialize_operation_id AND i.kind='INITIALIZE'
                JOIN operation_receipts c ON c.installation_id=m.installation_id AND c.account_key=m.account_key
                    AND c.operation_id='account-connect:'||m.initialize_operation_id AND c.kind='ACCOUNT_CONNECTED'
                WHERE m.installation_id=?1 AND m.account_key=?2)",params![installation,a.account_key],|r|r.get(0))?;
            require(source, "NATIVE_CAMPAIGN_ACCOUNT_SOURCE_MISSING")?;
        }
        let id = format!(
            "native-campaign-policy:{}:{}",
            p.campaign_id, p.policy_revision
        );
        let hash = p.digest()?;
        if let Some(receipt) = Self::operation_sql(tx, Some((installation, &roster[0])), &id)? {
            require(
                receipt.kind == "NATIVE_CAMPAIGN_POLICY" && receipt.request_hash == hash,
                "NATIVE_CAMPAIGN_POLICY_CONFLICT",
            )?;
            crate::risk::verify_unified_import_campaign(tx, installation, &p.campaign_id)
                .map_err(StoreError::Invalid)?;
            let seq: i64 = tx.query_row(
                "SELECT book_sequence FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2",
                params![installation, p.campaign_id],
                |r| r.get(0),
            )?;
            return Ok(NativeCampaignPolicyReceipt {
                policy_revision: p.policy_revision,
                book_sequence: u64::try_from(seq)
                    .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_SEQUENCE_INVALID"))?,
                replayed: true,
            });
        }
        let previous:Option<(i64,i64,String,String,String)>=tx.query_row(
            "SELECT policy_revision,book_sequence,global_limit_usdt,loss_limit_usd,entry_limit_usd
             FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2",
            params![installation,p.campaign_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        let seq = if let Some((rev, seq, global, loss, entry)) = previous {
            require(
                p.policy_revision
                    > u64::try_from(rev)
                        .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_REVISION_INVALID"))?,
                "POLICY_REVISION_CONFLICT",
            )?;
            // Existing obligations remain primary. A policy cannot replenish money.
            require(
                amount(&p.global_limit_usdt)? <= amount(&global)?
                    && amount(&p.loss_limit_usd)? <= amount(&loss)?
                    && amount(&p.entry_limit_usd)? <= amount(&entry)?,
                "NATIVE_CAMPAIGN_LIMIT_INCREASE_DENIED",
            )?;
            update_existing(tx, installation, p)?;
            seq
        } else {
            let assigned:i64=tx.query_row("SELECT COUNT(*) FROM account_policy WHERE installation_id=?1 AND campaign_id<>'unconfigured'",[installation],|r|r.get(0))?;
            require(assigned == 0, "CAMPAIGN_ID_IMMUTABLE")?;
            tx.execute("INSERT INTO risk_meta VALUES(1,?1,?2,?3,?4,0,1,?5,'0',?6,'0','0',?7,'0','0','0','0')",
                params![p.campaign_id,installation,sql_integer(p.policy_revision)?,sql_integer(p.generation)?,p.global_limit_usdt,p.loss_limit_usd,p.entry_limit_usd])?;
            for a in &p.accounts {
                tx.execute(
                    "INSERT INTO risk_account_occupied VALUES(?1,?2,?3,'0')",
                    params![installation, p.campaign_id, a.account_key],
                )?;
                tx.execute(
                    "INSERT INTO risk_policy_account VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        installation,
                        p.campaign_id,
                        a.account_key,
                        a.limit_usdt,
                        a.order_limit_usdt,
                        a.entry_limit_usd
                    ],
                )?;
                tx.execute(
                    "INSERT INTO risk_loss_source VALUES(?1,?2,?3,'0','')",
                    params![installation, p.campaign_id, a.account_key],
                )?;
                for st in &a.strategies {
                    tx.execute(
                        "INSERT INTO risk_policy_strategy VALUES(?1,?2,?3,?4,?5)",
                        params![
                            installation,
                            p.campaign_id,
                            a.account_key,
                            st.strategy_id,
                            st.limit_usdt
                        ],
                    )?;
                    tx.execute(
                        "INSERT INTO risk_strategy_occupied VALUES(?1,?2,?3,?4,'0')",
                        params![installation, p.campaign_id, a.account_key, st.strategy_id],
                    )?;
                }
            }
            tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,?2,?3,'CAMPAIGN_CREATED',?4,?5,?6)",
                params![
                    installation,
                    roster[0],
                    format!("campaign-created:{}", p.campaign_id),
                    hash,
                    serde_json::to_string(p)
                        .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_POLICY_ENCODING"))?,
                    now_ms()?
                ],
            )?;
            0
        };
        for a in &p.accounts {
            let updated=tx.execute("UPDATE account_policy SET revision=?3,paused=?4,campaign_id=?5,campaign_limit=?6
                WHERE installation_id=?1 AND account_key=?2 AND revision<?3 AND (campaign_id='unconfigured' OR campaign_id=?5)",
                params![installation,a.account_key,sql_integer(p.policy_revision)?,a.paused,p.campaign_id,p.loss_limit_usd])?;
            require(updated == 1, "POLICY_REVISION_CONFLICT")?;
        }
        tx.execute(
            "INSERT INTO operation_receipts VALUES(?1,?2,?3,'NATIVE_CAMPAIGN_POLICY',?4,?5,?6)",
            params![
                installation,
                roster[0],
                id,
                hash,
                serde_json::to_string(p)
                    .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_POLICY_ENCODING"))?,
                now_ms()?
            ],
        )?;
        crate::risk::verify_unified_import_campaign(tx, installation, &p.campaign_id)
            .map_err(StoreError::Invalid)?;
        Ok(NativeCampaignPolicyReceipt {
            policy_revision: p.policy_revision,
            book_sequence: u64::try_from(seq)
                .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_SEQUENCE_INVALID"))?,
            replayed: false,
        })
    }
}

fn update_existing(
    tx: &Transaction<'_>,
    installation: &str,
    p: &NativeCampaignPolicy,
) -> StoreResult<()> {
    let saved_limits:(String,String,String,String,String)=tx.query_row("SELECT global_reserved_usdt,loss_consumed_usd,loss_reserved_usd,entry_consumed_usd,entry_reserved_usd
        FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2",params![installation,p.campaign_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    require(
        amount(&p.global_limit_usdt)? >= amount(&saved_limits.0)?
            && amount(&p.loss_limit_usd)?
                >= amount(&saved_limits.1)?
                    .checked_add(&amount(&saved_limits.2)?)
                    .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_MONEY_INVALID"))?
            && amount(&p.entry_limit_usd)?
                >= amount(&saved_limits.3)?
                    .checked_add(&amount(&saved_limits.4)?)
                    .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_MONEY_INVALID"))?,
        "NATIVE_CAMPAIGN_LIMIT_BELOW_OBLIGATIONS",
    )?;
    let saved=tx.prepare("SELECT account_key FROM risk_policy_account WHERE installation_id=?1 AND campaign_id=?2 ORDER BY account_key")?
        .query_map(params![installation,p.campaign_id],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
    require(
        saved
            == p.accounts
                .iter()
                .map(|a| a.account_key.clone())
                .collect::<Vec<_>>(),
        "NATIVE_CAMPAIGN_COHORT_MISMATCH",
    )?;
    for a in &p.accounts {
        let strategies=tx.prepare("SELECT strategy_id FROM risk_policy_strategy WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3 ORDER BY strategy_id")?
            .query_map(params![installation,p.campaign_id,a.account_key],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        require(
            strategies
                == a.strategies
                    .iter()
                    .map(|s| s.strategy_id.clone())
                    .collect::<Vec<_>>(),
            "NATIVE_CAMPAIGN_STRATEGY_COHORT_MISMATCH",
        )?;
        let occupied:String=tx.query_row("SELECT reserved_usdt FROM risk_account_occupied WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3",params![installation,p.campaign_id,a.account_key],|r|r.get(0))?;
        require(
            amount(&a.limit_usdt)? >= amount(&occupied)?,
            "NATIVE_CAMPAIGN_LIMIT_BELOW_OBLIGATIONS",
        )?;
        for st in &a.strategies {
            let occupied:String=tx.query_row("SELECT reserved_usdt FROM risk_strategy_occupied WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3 AND strategy_id=?4",params![installation,p.campaign_id,a.account_key,st.strategy_id],|r|r.get(0))?;
            require(
                amount(&st.limit_usdt)? >= amount(&occupied)?,
                "NATIVE_CAMPAIGN_LIMIT_BELOW_OBLIGATIONS",
            )?;
        }
        tx.execute("UPDATE risk_policy_account SET limit_usdt=?4,order_limit_usdt=?5,entry_limit_usd=?6 WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3",
            params![installation,p.campaign_id,a.account_key,a.limit_usdt,a.order_limit_usdt,a.entry_limit_usd])?;
        for st in &a.strategies {
            tx.execute("UPDATE risk_policy_strategy SET limit_usdt=?5 WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3 AND strategy_id=?4",
            params![installation,p.campaign_id,a.account_key,st.strategy_id,st.limit_usdt])?;
        }
    }
    tx.execute("UPDATE risk_meta SET policy_revision=?3,global_limit_usdt=?4,loss_limit_usd=?5,entry_limit_usd=?6
        WHERE installation_id=?1 AND campaign_id=?2",params![installation,p.campaign_id,sql_integer(p.policy_revision)?,p.global_limit_usdt,p.loss_limit_usd,p.entry_limit_usd])?;
    Ok(())
}

fn now_ms() -> StoreResult<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_CLOCK_INVALID"))?
            .as_millis(),
    )
    .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_CLOCK_INVALID"))
}

fn sql_integer(value: u64) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_INTEGER_INVALID"))
}
