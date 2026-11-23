use super::*;

#[test]
fn terminal_diagnostics_reopen_retains_only_original_sql_stages() {
    let mut s = store();
    assert!(
        s.recent_terminal_release_diagnostics(128)
            .unwrap()
            .is_empty()
    );
    intent(&mut s);
    reserve(&mut s);
    assert!(
        s.recent_terminal_release_diagnostics(128)
            .unwrap()
            .is_empty()
    );
    s.connection
        .execute("UPDATE intents SET state='CANCELLED',revision=7", [])
        .unwrap();
    s.connection
        .execute(
            "UPDATE risk_receipts SET state='TERMINAL_PENDING_CENTRAL'",
            [],
        )
        .unwrap();
    let pending = s.recent_terminal_release_diagnostics(128).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent_revision, 7);
    assert_eq!(pending[0].intent_state, IntentState::Cancelled);
    assert_eq!(pending[0].reservation_key, "central-key");
    assert_eq!(pending[0].policy_revision, 1);
    assert_eq!(pending[0].receipt_state, "TERMINAL_PENDING_CENTRAL");
    assert_eq!(pending[0].central_release_committed_at, None);
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s =
        AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(s.recent_terminal_release_diagnostics(128).unwrap(), pending);
    s.acknowledge_central_release("central-key", "durably-recorded-worker-response")
        .unwrap();
    let released = s.recent_terminal_release_diagnostics(128).unwrap();
    assert_eq!(released[0].receipt_state, "RELEASED");
    assert!(
        released[0]
            .central_release_committed_at
            .is_some_and(|v| v > 0)
    );
    drop(s);
    let s = AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(
        s.recent_terminal_release_diagnostics(128).unwrap(),
        released
    );
    // A wrong hash does not manufacture a matching SQL ACK timestamp.
    s.connection
        .execute(
            "UPDATE operation_receipts SET request_hash='wrong' WHERE kind='CENTRAL_RELEASE'",
            [],
        )
        .unwrap();
    assert_eq!(
        s.recent_terminal_release_diagnostics(128).unwrap()[0].central_release_committed_at,
        None
    );
    s.connection
        .execute(
            "UPDATE risk_receipts SET state='LATE_SETTLEMENT_PENDING'",
            [],
        )
        .unwrap();
    assert_eq!(
        s.recent_terminal_release_diagnostics(128).unwrap()[0].receipt_state,
        "LATE_SETTLEMENT_PENDING"
    );
    for update in [
        "UPDATE risk_receipts SET account_key='wrong'",
        "UPDATE risk_receipts SET account_key='okx:real:verified',installation_id='wrong'",
        "UPDATE risk_receipts SET installation_id='installation',plan_hash='wrong'",
        "UPDATE risk_receipts SET plan_hash='plan',policy_revision=99",
    ] {
        s.connection.execute(update, []).unwrap();
        assert!(
            s.recent_terminal_release_diagnostics(128)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn terminal_diagnostics_caps_candidates_before_identity_joins() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.connection
        .execute("UPDATE intents SET state='FILLED'", [])
        .unwrap();
    s.connection
        .execute("UPDATE risk_receipts SET state='RELEASED'", [])
        .unwrap();
    // Real persisted rows retain the original decision FK. Each receipt is
    // scoped independently; unrelated live receipts must not enlarge the scan.
    for n in 0..150 {
        let id = format!("bounded-{n:03}");
        s.connection.execute(
            "INSERT INTO intents SELECT ?1,?1,request_hash,decision_id,state,revision,?1,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at,begin_post_at,exchange_order_id,incident FROM intents WHERE intent_id='intent'",
            [&id],
        ).unwrap();
        s.connection.execute(
            "INSERT INTO risk_receipts SELECT ?1,?1,account_key,installation_id,instance_id,plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state FROM risk_receipts WHERE intent_id='intent'",
            [&id],
        ).unwrap();
    }
    assert!(s.recent_terminal_release_diagnostics(0).unwrap().is_empty());
    assert_eq!(s.recent_terminal_release_diagnostics(1).unwrap().len(), 1);
    assert_eq!(
        s.recent_terminal_release_diagnostics(usize::MAX)
            .unwrap()
            .len(),
        128
    );
    s.connection
        .execute(
            "UPDATE risk_receipts SET account_key='wrong' WHERE intent_id LIKE 'bounded-%'",
            [],
        )
        .unwrap();
    // All 128 candidates fail scope validation. The API intentionally does
    // not scan farther to backfill from the valid receipt outside this subset.
    assert!(
        s.recent_terminal_release_diagnostics(usize::MAX)
            .unwrap()
            .is_empty()
    );
}
