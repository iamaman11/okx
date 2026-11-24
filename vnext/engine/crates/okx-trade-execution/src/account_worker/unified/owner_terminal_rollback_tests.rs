//! Successful terminal Resolve staged/rolled back on the actual NEW SQL FIFO.
//! Scoped terminal/source rows are explicit component fixtures, not a public
//! signed producer or live execution grant. Fresh public lifecycle witnesses
//! separately establish the producer chain. No imported schema or second owner.
use super::{
    UnifiedFinanceHandle, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction,
    owner_financial_reply_tests::{INSTALLATION, account, campaign, fresh},
};
use crate::{EntrySettlementEvidence, StoreResult};
use rusqlite::{Connection, params, types::Value};
use rust_decimal::Decimal;
use std::{collections::BTreeMap, time::Duration};

const INTENT: &str = "terminal_rollback_001";
const RESERVATION: &str = "terminal_rollback_reservation";
fn proof() -> EntrySettlementEvidence {
    EntrySettlementEvidence {
        schema: "okx.entry-terminal-settlement/v1".into(),
        account_key: account(),
        intent_id: INTENT.into(),
        plan_hash: "b".repeat(64),
        reservation_key: RESERVATION.into(),
        venue_order_id: "component-terminal-order".into(),
        venue_update_ms: 202,
        source_covered_through_ms: 210,
        venue_filled_quantity: Decimal::ONE,
        booked_notional_and_fee_usdt: Decimal::ONE,
        cumulative_entry_usd: Decimal::new(4, 1),
        settlement_revision: 1,
        source_digest: "d".repeat(64),
    }
}
fn input() -> crate::risk::TerminalResolveInput {
    let amount = |v| okx_engine_risk::risk::money::RiskAmount::parse(v).unwrap();
    crate::risk::TerminalResolveInput {
        account: account(),
        intent: INTENT.into(),
        reservation: RESERVATION.into(),
        plan_sha256: proof().plan_hash,
        generation: 1,
        expected_sequence: 2,
        now_ms: 221,
        census: crate::risk::VerifiedCensus {
            complete: true,
            generation: 1,
            source_digest: "e".repeat(64),
            generated_at_ms: 220,
            read_floor_ms: 211,
            occupied: BTreeMap::from([(account(), amount("0"))]),
            cumulative_loss_usd: BTreeMap::from([(account(), amount("0"))]),
        },
    }
}
fn seed(db: &mut Connection) -> StoreResult<()> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let p = proof();
    tx.execute("INSERT INTO decisions(installation_id,account_key,decision_id,instance_id,decision_at,input_digest,policy_revision,code_version)
        VALUES(?1,?2,'terminal_component_decision',NULL,199,'component-input',1,'component')", params![INSTALLATION,account()])?;
    tx.execute("INSERT INTO intents(installation_id,account_key,intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at,begin_post_at,exchange_order_id,incident)
        VALUES(?1,?2,?3,'terminal_component_request',?4,'terminal_component_decision','FILLED',3,'terminalcomponentclient',1000,?5,1,1,'BTC-USDT-SWAP',199,202,201,?6,NULL)",
        params![INSTALLATION,account(),INTENT,"c".repeat(64),p.plan_hash,p.venue_order_id])?;
    tx.execute("INSERT INTO risk_receipts(installation_id,account_key,reservation_key,intent_id,instance_id,plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state)
        VALUES(?1,?2,?3,?4,NULL,?5,1,?6,'1',?7,'OPEN')", params![INSTALLATION,account(),RESERVATION,INTENT,p.plan_hash,"f".repeat(64),"a".repeat(64)])?;
    tx.execute("INSERT INTO risk_entries(installation_id,campaign_id,reservation_key,account_key,strategy_id,intent_id,plan_sha256,request_digest,policy_revision,state,loss_usdt,loss_usd,entry_upper_usd,reserved_usdt,reserved_loss_usd,reserved_entry_usd,entry_settled_usd,entry_settlement_revision,entry_settlement_digest,entry_proof_digest,source_digest,book_sequence,held_at_ms,reserve_read_floor_ms,native_incarnation,post_authorized_at_ms,post_sequence,post_proof_digest,post_source_digest)
        VALUES(?1,?2,?3,?4,'manual',?5,?6,?7,1,'OPEN','1','1','1','1','1','0.6','0.4',1,?8,?9,?9,1,200,199,?10,201,2,?9,?9)",
        params![INSTALLATION,campaign(),RESERVATION,account(),INTENT,p.plan_hash,"a".repeat(64),p.source_digest,"f".repeat(64),"f".repeat(32)])?;
    tx.execute("UPDATE risk_meta SET book_sequence=2,global_reserved_usdt='1',loss_reserved_usd='1',entry_consumed_usd='0.4',entry_reserved_usd='0.6' WHERE installation_id=?1 AND campaign_id=?2", params![INSTALLATION,campaign()])?;
    tx.execute("UPDATE risk_account_occupied SET reserved_usdt='1' WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3", params![INSTALLATION,campaign(),account()])?;
    tx.execute("UPDATE risk_strategy_occupied SET reserved_usdt='1' WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3 AND strategy_id='manual'", params![INSTALLATION,campaign(),account()])?;
    for (operation, kind, hash, encoded, time) in [
        (
            format!("entry-settlement:{INTENT}"),
            "ENTRY_SETTLEMENT_PROVEN",
            p.plan_hash.clone(),
            serde_json::to_string(&p).unwrap(),
            202,
        ),
        (
            format!("entry-terminal-settled:{INTENT}:r1"),
            "ENTRY_TERMINAL_SETTLED",
            p.plan_hash,
            p.source_digest,
            210,
        ),
        (
            format!("terminal:{INTENT}"),
            "TERMINAL",
            "c".repeat(64),
            "component-verified-terminal".into(),
            202,
        ),
    ] {
        tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5,?6,?7)", params![INSTALLATION,account(),operation,kind,hash,encoded,time])?;
    }
    crate::risk::verify_unified_import_campaign(&tx, INSTALLATION, &campaign())
        .map_err(crate::StoreError::Invalid)?;
    tx.commit()?;
    Ok(())
}
type Snapshot = Vec<(&'static str, Vec<Vec<Value>>)>;
fn snapshot(db: &Connection) -> StoreResult<Snapshot> {
    let mut result = Vec::new();
    for table in [
        "risk_entries",
        "risk_meta",
        "risk_account_occupied",
        "risk_strategy_occupied",
        "risk_loss_source",
        "risk_receipts",
        "operation_receipts",
        "intents",
        "finance_entries",
        "finance_source_links",
    ] {
        let mut query = db.prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))?;
        let columns = query.column_count();
        let rows = query
            .query_map([], |row| {
                (0..columns)
                    .map(|i| row.get(i))
                    .collect::<std::result::Result<Vec<Value>, _>>()
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        result.push((table, rows));
    }
    Ok(result)
}
fn resolve(tx: &rusqlite::Transaction<'_>, replay: bool) -> StoreResult<()> {
    let receipt = apply_original_unified_finance_transaction(
        tx,
        INSTALLATION,
        UnifiedFinanceWrite::ResolveTerminal {
            campaign: campaign(),
            input: input(),
        },
    )?;
    assert!(
        matches!(receipt, UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::TerminalReleased { book_sequence:3, replayed, .. }) if replayed == replay)
    );
    assert_released(tx)?;
    Ok(())
}
fn assert_released(db: &Connection) -> StoreResult<()> {
    let states: (String,String,String,String,String,String,i64) = db.query_row(
        "SELECT e.state,r.state,m.global_reserved_usdt,m.loss_reserved_usd,m.entry_reserved_usd,m.entry_consumed_usd,m.book_sequence
         FROM risk_entries e JOIN risk_receipts r ON r.installation_id=e.installation_id AND r.account_key=e.account_key AND r.intent_id=e.intent_id
         JOIN risk_meta m ON m.installation_id=e.installation_id AND m.campaign_id=e.campaign_id
         WHERE e.installation_id=?1 AND e.campaign_id=?2 AND e.reservation_key=?3", params![INSTALLATION,campaign(),RESERVATION],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
    assert_eq!(
        states,
        (
            "RELEASED".into(),
            "RELEASED".into(),
            "0".into(),
            "0".into(),
            "0".into(),
            "0.4".into(),
            3
        )
    );
    let (count, kind, hash, digest, at): (i64,String,String,String,i64) = db.query_row(
        "SELECT COUNT(*),kind,request_hash,result_ref,committed_at FROM operation_receipts WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
        params![INSTALLATION,account(),format!("central-release:{RESERVATION}")],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    assert_eq!(
        (count, kind, hash, at),
        (1, "CENTRAL_RELEASE".into(), "c".repeat(64), 221)
    );
    assert_eq!(digest.len(), 64);
    let (account_reserved, strategy_reserved): (String, String) = db.query_row(
        "SELECT a.reserved_usdt,s.reserved_usdt FROM risk_account_occupied a
         JOIN risk_strategy_occupied s USING(installation_id,campaign_id,account_key)
         WHERE a.installation_id=?1 AND a.campaign_id=?2 AND a.account_key=?3 AND s.strategy_id='manual'",
        params![INSTALLATION,campaign(),account()], |r| Ok((r.get(0)?,r.get(1)?)))?;
    assert_eq!(
        (account_reserved, strategy_reserved),
        ("0".into(), "0".into())
    );
    Ok(())
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(5), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(*stopped.borrow());
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_terminal_resolve_success_rollback_then_commit_reopen_once()
{
    let (root, owner) = fresh().await;
    owner.submit(seed).await.unwrap();
    let before = owner.submit(|db| snapshot(db)).await.unwrap();
    let expected = before.clone();
    owner
        .submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            resolve(&tx, false)?;
            assert_ne!(
                snapshot(&tx)?,
                expected,
                "successful release must really stage changes before rollback"
            );
            tx.rollback()?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(owner.submit(|db| snapshot(db)).await.unwrap(), before);
    let committed = owner
        .submit(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            resolve(&tx, false)?;
            tx.commit()?;
            snapshot(db)
        })
        .await
        .unwrap();
    // Terminal Resolve changes capacity and the local release ACK. It must
    // preserve all original source rows and all already booked finance legs.
    for table in ["finance_entries", "finance_source_links", "intents"] {
        assert_eq!(
            before.iter().find(|(name, _)| *name == table),
            committed.iter().find(|(name, _)| *name == table)
        );
    }
    owner
        .submit(|db| {
            let tx = db.transaction()?;
            resolve(&tx, true)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(owner.submit(|db| snapshot(db)).await.unwrap(), committed);
    close(owner).await;
    let owner = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    assert_eq!(owner.submit(|db| snapshot(db)).await.unwrap(), committed);
    owner
        .submit(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            resolve(&tx, true)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(owner.submit(|db| snapshot(db)).await.unwrap(), committed);
    close(owner).await;
    assert_eq!(
        root.parent().unwrap().canonicalize().unwrap(),
        std::env::temp_dir().canonicalize().unwrap()
    );
    assert!(
        root.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("okx-fresh-financial-reply-")
    );
    std::fs::remove_dir_all(root).unwrap();
}
