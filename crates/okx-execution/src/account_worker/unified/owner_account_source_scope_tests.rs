//! Actual captured-source ingestion on a fresh one-file Native owner.
//! Parsed authenticated-producer DTOs are explicit component inputs, not HTTP
//! authentication evidence or permission to trade. No financial/source INSERTs.
use super::UnifiedFinanceHandle;
use crate::{
    AccountMeta, CapturedBill, CapturedBillPage, CapturedFill, CapturedFillPage, Fill,
    NewFinanceInstallation, OpeningSnapshot, SourceCursor, StoreError, UsdConversionEvidence,
};
use rust_decimal::Decimal;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const INSTALLATION: &str = "installation_identical_venue_sources";
const INSTRUMENT: &str = "BTC-USDT-SWAP";
const FILL_ID: &str = "BTC-USDT-SWAP:same-trade:same-order";
fn key(label: char) -> String {
    format!("okx:real:{}", label.to_string().repeat(64))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn cursor(source: &str, partition: &str) -> SourceCursor {
    SourceCursor {
        source: source.into(),
        stream_partition: partition.into(),
        cursor: "same-source-cursor".into(),
        watermark: 200,
        generation: 1,
        coverage_gaps: String::new(),
    }
}
fn conversion() -> UsdConversionEvidence {
    UsdConversionEvidence {
        observed_at: 200,
        source_digest: "f".repeat(64),
        usd_per_currency: BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    }
}
fn fills(fee: Decimal) -> CapturedFillPage {
    CapturedFillPage {
        partition: "SWAP".into(),
        cursor: cursor("fills", "SWAP"),
        rows: vec![CapturedFill {
            fill: Fill {
                fill_id: FILL_ID.into(),
                instrument_id: INSTRUMENT.into(),
                trade_id: "same-trade".into(),
                exchange_order_id: "same-order".into(),
                intent_id: None,
                time: 150,
                qty: Decimal::ONE,
                price: Decimal::from(100),
                fee,
                currency: "USDT".into(),
                side: "buy".into(),
            },
            client_order_id: "same-client".into(),
            bill_id: "same-bill".into(),
            instrument_type: "SWAP".into(),
            sub_type: "3".into(),
            position_side: "long".into(),
        }],
    }
}
fn bills(fee: Decimal) -> CapturedBillPage {
    CapturedBillPage {
        cursor: cursor("bills", "account"),
        rows: vec![CapturedBill {
            bill_id: "same-bill".into(),
            client_order_id: "same-client".into(),
            order_id: "same-order".into(),
            instrument_id: INSTRUMENT.into(),
            trade_id: "same-trade".into(),
            bill_type: "2".into(),
            bill_sub_type: "3".into(),
            currency: "USDT".into(),
            time: 151,
            fee: Some(fee),
            pnl: Some(Decimal::ZERO),
            balance_change: Some(fee),
        }],
    }
}
async fn initialize(owner: &UnifiedFinanceHandle, label: char) {
    let opening = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: label.to_string().repeat(64),
        observed_at: 100,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    owner
        .initialize_account(
            deadline(),
            AccountMeta {
                account_key: key(label),
                installation_id: INSTALLATION.into(),
                initialized_at: 100,
                opening_source: opening.source_ref.clone(),
                initialize_operation_id: format!("connect_source_scope_{label}"),
                release_id: "a".repeat(40),
            },
            opening,
            label.to_string().repeat(64),
        )
        .await
        .unwrap();
}
type Facts = Vec<(String, String, String, i64, i64, i64)>;
async fn facts(owner: &UnifiedFinanceHandle) -> Facts {
    owner.submit(|db| {
        let mut query = db.prepare("SELECT p.account_key,p.campaign_loss,t.amount,
            (SELECT COUNT(*) FROM fills f WHERE f.installation_id=p.installation_id AND f.account_key=p.account_key),
            (SELECT COUNT(*) FROM bills b WHERE b.installation_id=p.installation_id AND b.account_key=p.account_key),
            (SELECT COUNT(*) FROM finance_entries e WHERE e.installation_id=p.installation_id AND e.account_key=p.account_key AND e.component='FEE')
            FROM account_policy p JOIN finance_totals t ON t.installation_id=p.installation_id AND t.account_key=p.account_key AND t.currency='USDT' AND t.component='FEE' ORDER BY p.account_key")?;
        Ok(query.query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?.collect::<Result<Vec<_>,_>>()?)
    }).await.unwrap()
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(3), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_identical_venue_ids_are_account_scoped_and_deduplicated() {
    let root = std::env::temp_dir().join(format!(
        "okx-identical-venue-source-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let owner = UnifiedFinanceHandle::initialize_new(
        root.clone(),
        NewFinanceInstallation {
            installation_id: INSTALLATION.into(),
            initialize_request_id: "initialize_source_scope_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    initialize(&owner, 'a').await;
    initialize(&owner, 'b').await;
    // Exactly the same order/trade/fill/bill IDs, but deliberately different
    // amounts. A global identity or accidental cross-account JOIN cannot pass.
    for (label, fee) in [('a', Decimal::new(-1, 1)), ('b', Decimal::new(-2, 1))] {
        owner
            .ingest_captured_fills(key(label), fills(fee), conversion())
            .await
            .unwrap();
        owner
            .ingest_captured_bills(key(label), bills(fee), conversion())
            .await
            .unwrap();
    }
    let expected = vec![
        (key('a'), "0.1".into(), "-0.1".into(), 1, 1, 1),
        (key('b'), "0.2".into(), "-0.2".into(), 1, 1, 1),
    ];
    assert_eq!(facts(&owner).await, expected);
    for (label, fee) in [('b', Decimal::new(-2, 1)), ('a', Decimal::new(-1, 1))] {
        owner
            .ingest_captured_bills(key(label), bills(fee), conversion())
            .await
            .unwrap();
        owner
            .ingest_captured_fills(key(label), fills(fee), conversion())
            .await
            .unwrap();
    }
    assert_eq!(
        facts(&owner).await,
        expected,
        "fill+bill source replay is one fee, not two fees per account"
    );
    // Reusing B's observed amount under A is a conflicting A replay, not a
    // lookup of B's source. The transaction must leave both accounts intact.
    assert!(matches!(
        owner
            .ingest_captured_fills(key('a'), fills(Decimal::new(-2, 1)), conversion())
            .await,
        Err(StoreError::Invalid("IMMUTABLE_FILL_CONFLICT"))
    ));
    assert!(matches!(
        owner
            .ingest_captured_bills(key('a'), bills(Decimal::new(-2, 1)), conversion())
            .await,
        Err(StoreError::Invalid(_))
    ));
    assert!(
        owner
            .ingest_captured_fills(key('f'), fills(Decimal::new(-1, 1)), conversion())
            .await
            .is_err()
    );
    assert_eq!(facts(&owner).await, expected);
    // These observations are intentionally unowned; ingestion cannot create a
    // trade intent, reservation, receipt or a live campaign grant.
    let authority_rows = owner
        .submit(|db| {
            Ok((
                db.query_row::<i64, _, _>("SELECT COUNT(*) FROM intents", [], |r| r.get(0))?,
                db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(authority_rows, (0, 0));
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root, INSTALLATION.into())
        .await
        .unwrap();
    assert_eq!(facts(&reopened).await, expected);
    reopened
        .ingest_captured_bills(key('a'), bills(Decimal::new(-1, 1)), conversion())
        .await
        .unwrap();
    reopened
        .ingest_captured_fills(key('b'), fills(Decimal::new(-2, 1)), conversion())
        .await
        .unwrap();
    assert_eq!(facts(&reopened).await, expected);
    close(reopened).await;
}
