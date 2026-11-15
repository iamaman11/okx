//! Original storage audit projection, shared by account-local and scoped SQL.
use super::super::*;
impl AccountStore {
    pub(crate) fn recovery_snapshot_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        now: i64,
    ) -> StoreResult<RecoverySnapshot> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let args = || {
            scope
                .map(|(i, a)| vec![Value::from(i.to_owned()), Value::from(a.to_owned())])
                .unwrap_or_default()
        };
        let (required,backup_at):(bool,Option<i64>)=db.query_row(match scope {
            None=>"SELECT requires_reconcile,backup_at FROM storage_recovery",
            Some(_)=>"SELECT requires_reconcile,backup_at FROM storage_recovery WHERE installation_id=?1 AND account_key=?2 AND singleton=1"
        },params_from_iter(args()),|r|Ok((r.get(0)?,r.get(1)?)))?;
        let initialized_at:i64=db.query_row(match scope {
            None=>"SELECT initialized_at FROM account_meta",Some(_)=>"SELECT initialized_at FROM account_meta WHERE installation_id=?1 AND account_key=?2 AND singleton=1"
        },params_from_iter(args()),|r|r.get(0))?;
        let threshold = backup_at.unwrap_or(initialized_at);
        let cursor =
            |source: &str, part: &str| Self::entry_settlement_cursor_sql(db, scope, source, part);
        let generation = cursor("account_snapshot", "account")?.map(|c| c.generation);
        let audit = cursor("recovery_audit", "account")?;
        let audit_watermark = audit.as_ref().map(|c| c.watermark);
        let mut coverage_complete = generation.is_some()
            && audit.is_some_and(|c| {
                Some(c.generation) == generation
                    && c.watermark >= threshold
                    && c.watermark <= now
                    && c.cursor.is_empty()
                    && c.coverage_gaps.is_empty()
            });
        for (source, part) in [
            ("fills", "SPOT"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
            ("orders", "account"),
            ("account_snapshot", "account"),
        ] {
            let historical = matches!(source, "fills" | "bills");
            coverage_complete &= cursor(source, part)?.is_some_and(|c| {
                Some(c.generation) == generation
                    && c.watermark >= threshold
                    && c.watermark <= now
                    && c.coverage_gaps.is_empty()
                    && c.cursor.is_empty()
                    && audit_watermark.is_some_and(|w| c.watermark >= w)
                    && (historical || now - c.watermark <= 5000)
            });
        }
        let mut q=db.prepare(match scope {
            None=>"SELECT intent_id FROM risk_receipts WHERE state IN('HELD','POSTED','UNKNOWN','OPEN','TERMINAL_PENDING_CENTRAL','UNSENT_PENDING_CENTRAL') ORDER BY state,intent_id LIMIT 257",
            Some(_)=>"SELECT intent_id FROM risk_receipts WHERE installation_id=?1 AND account_key=?2 AND state IN('HELD','POSTED','UNKNOWN','OPEN','TERMINAL_PENDING_CENTRAL','UNSENT_PENDING_CENTRAL') ORDER BY state,intent_id LIMIT 257"
        })?;
        let ids = q
            .query_map(params_from_iter(args()), |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if ids.len() > 256 {
            return Err(StoreError::Invalid("STORAGE_RECOVERY_RECEIPT_LIMIT"));
        }
        let receipts = ids
            .iter()
            .map(|id| {
                Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::Retained)?
                    .ok_or(StoreError::Invalid("CENTRAL_RECEIPT_MISSING"))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        let unresolved=db.query_row(match scope {
            None=>"SELECT EXISTS(SELECT 1 FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED'))",
            Some(_)=>"SELECT EXISTS(SELECT 1 FROM intents WHERE installation_id=?1 AND account_key=?2 AND state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED'))"
        },params_from_iter(args()),|r|r.get(0))?;
        Ok(RecoverySnapshot {
            required,
            backup_at,
            coverage_complete,
            active_receipts: receipts,
            unresolved_intents: unresolved,
            cumulative_loss_usd: Self::account_policy_sql(db, scope)?.campaign_loss,
        })
    }
    pub(crate) fn mark_storage_reconciled_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        observed_at: i64,
        exchange: &str,
        central: &str,
    ) -> StoreResult<()> {
        if exchange.is_empty() || central.is_empty() {
            return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
        }
        let n=match scope {
            None=>db.execute("UPDATE storage_recovery SET requires_reconcile=0,reconciled_evidence=?2 WHERE backup_at IS NULL OR backup_at<=?1",params![observed_at,format!("{exchange}|{central}")])?,
            Some((i,a))=>{
                Self::verify_source_account_sql(db,i,a)?;
                db.execute("UPDATE storage_recovery SET requires_reconcile=0,reconciled_evidence=?2 WHERE installation_id=?3 AND account_key=?4 AND singleton=1 AND (backup_at IS NULL OR backup_at<=?1)",params![observed_at,format!("{exchange}|{central}"),i,a])?
            }
        };
        if n != 1 {
            return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
        }
        Ok(())
    }
}
