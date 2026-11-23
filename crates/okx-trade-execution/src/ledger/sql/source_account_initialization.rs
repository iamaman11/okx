//! One original flat-opening seed for account-local fixtures and Native scopes.
//! No Connection/file creation, financial grant, venue access or own COMMIT.
use super::super::*;
use rusqlite::{Transaction, params_from_iter, types::Value};
use sha2::{Digest, Sha256};

fn write(
    tx: &Transaction<'_>,
    scope: Option<(&str, &str)>,
    local: &str,
    native: &str,
    mut values: Vec<Value>,
) -> StoreResult<()> {
    let sql = if let Some((installation, account)) = scope {
        values.push(installation.to_owned().into());
        values.push(account.to_owned().into());
        native
    } else {
        local
    };
    tx.execute(sql, params_from_iter(values))?;
    Ok(())
}

impl AccountStore {
    pub(crate) fn validate_account_opening(
        meta: &AccountMeta,
        snapshot: &OpeningSnapshot,
    ) -> StoreResult<()> {
        if snapshot.positions != 0
            || snapshot.orders != 0
            || snapshot.conditional_orders != 0
            || snapshot.liabilities != Decimal::ZERO
        {
            return Err(StoreError::Invalid("ACCOUNT_NOT_FLAT"));
        }
        if snapshot.source_ref != meta.opening_source
            || snapshot.observed_at != meta.initialized_at
            || snapshot.balances.is_empty()
        {
            return Err(StoreError::Invalid("OPENING_EVIDENCE_MISMATCH"));
        }
        if meta.account_key.is_empty()
            || meta.installation_id.is_empty()
            || meta.opening_source.is_empty()
            || meta.initialized_at <= 0
            || meta.initialize_operation_id.is_empty()
            || meta.release_id.is_empty()
        {
            return Err(StoreError::Invalid("INVALID_ACCOUNT_META"));
        }
        if snapshot
            .balances
            .iter()
            .any(|(currency, money)| currency.is_empty() || *money < Decimal::ZERO)
        {
            return Err(StoreError::Invalid("INVALID_OPENING_BALANCE"));
        }
        Ok(())
    }
    pub(crate) fn initialization_request_hash(
        scope: Option<(&str, &str)>,
        meta: &AccountMeta,
        snapshot: &OpeningSnapshot,
    ) -> StoreResult<String> {
        if scope.is_none() {
            return Ok(format!(
                "{}|{}|{}",
                meta.account_key, meta.installation_id, meta.release_id
            ));
        }
        let raw = serde_json::to_vec(&("okx.native-account-initialization/v1", meta, snapshot))
            .map_err(|_| StoreError::Invalid("INITIALIZE_SOURCE_ENCODING"))?;
        Ok(format!("{:x}", Sha256::digest(raw)))
    }

    pub(crate) fn initialize_account_rows_sql(
        tx: &Transaction<'_>,
        scope: Option<(&str, &str)>,
        meta: &AccountMeta,
        snapshot: &OpeningSnapshot,
    ) -> StoreResult<()> {
        Self::validate_account_opening(meta, snapshot)?;
        if scope.is_some_and(|(installation, account)| {
            installation != meta.installation_id || account != meta.account_key
        }) {
            return Err(StoreError::Invalid("ACCOUNT_IDENTITY_MISMATCH"));
        }
        tx.execute(
            "INSERT INTO account_meta VALUES(1,?1,?2,2,?3,?4,?5,?6)",
            params![
                meta.account_key,
                meta.installation_id,
                meta.initialized_at,
                meta.opening_source,
                meta.initialize_operation_id,
                meta.release_id
            ],
        )?;
        if let Some((installation, account)) = scope {
            tx.execute(
                "INSERT INTO campaign_source_epoch VALUES(?1,?2,1,0)",
                params![installation, account],
            )?;
            tx.execute(
                "INSERT INTO campaign_late_head_epoch VALUES(?1,?2,1,0)",
                params![installation, account],
            )?;
        }
        write(
            tx,
            scope,
            "INSERT INTO account_policy VALUES(1,0,0,'unconfigured','0','0')",
            "INSERT INTO account_policy VALUES(?1,?2,1,0,0,'unconfigured','0','0')",
            vec![],
        )?;
        for (currency, money) in &snapshot.balances {
            write(
                tx,
                scope,
                "INSERT INTO cash_flows VALUES(?1,'OPENING',?2,?3,?4,?5)",
                "INSERT INTO cash_flows VALUES(?6,?7,?1,'OPENING',?2,?3,?4,?5)",
                vec![
                    format!("opening:{currency}").into(),
                    currency.clone().into(),
                    exact(*money).into(),
                    snapshot.observed_at.into(),
                    snapshot.source_ref.clone().into(),
                ],
            )?;
        }
        write(
            tx,
            scope,
            "INSERT INTO source_cursors VALUES('opening','account',?1,?2,0,'')",
            "INSERT INTO source_cursors VALUES(?3,?4,'opening','account',?1,?2,0,'')",
            vec![
                snapshot.source_ref.clone().into(),
                snapshot.observed_at.into(),
            ],
        )?;
        write(
            tx,
            scope,
            "INSERT INTO operation_receipts VALUES(?1,'INITIALIZE',?2,?3,?4)",
            "INSERT INTO operation_receipts VALUES(?5,?6,?1,'INITIALIZE',?2,?3,?4)",
            vec![
                meta.initialize_operation_id.clone().into(),
                Self::initialization_request_hash(scope, meta, snapshot)?.into(),
                meta.opening_source.clone().into(),
                meta.initialized_at.into(),
            ],
        )?;
        write(
            tx,
            scope,
            "INSERT INTO storage_recovery VALUES(1,0,NULL,NULL)",
            "INSERT INTO storage_recovery VALUES(?1,?2,1,0,NULL,NULL)",
            vec![],
        )?;
        write(
            tx,
            scope,
            "INSERT INTO nav_points VALUES('account',?1,'opening',?2,'0','0','USD','COMPLETE',?3)",
            "INSERT INTO nav_points VALUES(?4,?5,'account',?1,'opening',?2,'0','0','USD','COMPLETE',?3)",
            vec![
                meta.initialized_at.into(),
                exact(snapshot.opening_equity_usd).into(),
                snapshot.source_ref.clone().into(),
            ],
        )?;
        write(
            tx,
            scope,
            "INSERT INTO nav_watermarks VALUES('account',?1,?1,?2,'COMPLETE')",
            "INSERT INTO nav_watermarks VALUES(?3,?4,'account',?1,?1,?2,'COMPLETE')",
            vec![
                exact(snapshot.opening_equity_usd).into(),
                meta.initialized_at.into(),
            ],
        )?;
        write(
            tx,
            scope,
            "INSERT INTO nav_scopes VALUES('account','USD')",
            "INSERT INTO nav_scopes VALUES(?1,?2,'account','USD')",
            vec![],
        )?;
        for (currency, money) in &snapshot.balances {
            let nav_scope = format!("account:{currency}");
            write(
                tx,
                scope,
                "INSERT INTO nav_points VALUES(?1,?2,'opening',?3,'0','0',?4,'COMPLETE',?5)",
                "INSERT INTO nav_points VALUES(?6,?7,?1,?2,'opening',?3,'0','0',?4,'COMPLETE',?5)",
                vec![
                    nav_scope.clone().into(),
                    meta.initialized_at.into(),
                    exact(*money).into(),
                    currency.clone().into(),
                    snapshot.source_ref.clone().into(),
                ],
            )?;
            write(
                tx,
                scope,
                "INSERT INTO nav_scopes VALUES(?1,?2)",
                "INSERT INTO nav_scopes VALUES(?3,?4,?1,?2)",
                vec![nav_scope.into(), currency.clone().into()],
            )?;
        }
        Ok(())
    }

    pub(crate) fn account_metadata_sql(
        db: &Connection,
        installation: &str,
        account: &str,
    ) -> StoreResult<Option<AccountMeta>> {
        Ok(db.query_row("SELECT account_key,installation_id,initialized_at,opening_source,initialize_operation_id,release_id
            FROM account_meta WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
            params![installation,account],|r|Ok(AccountMeta{account_key:r.get(0)?,installation_id:r.get(1)?,
                initialized_at:r.get(2)?,opening_source:r.get(3)?,initialize_operation_id:r.get(4)?,release_id:r.get(5)?})).optional()?)
    }
}
