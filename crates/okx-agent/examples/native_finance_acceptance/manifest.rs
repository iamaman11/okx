//! Reviewed property map for #510 r7; this data alone makes no PASS claim.
//! Witnesses are exact --lib names, without crate prefixes. The collector must
//! discover and execute them on the current run/SHA; ignored, missing, foreign,
//! failed or incomplete results cannot satisfy an atom. Component witnesses on
//! a row with `missing` remain partial evidence, never completion of that row.
//!
//! Scope: fresh isolated CODE installations, OKX linear USDT-SWAP, census32,
//! writer queue128. New venues/products/unbounded workload are NOT_PROVEN and
//! outside this supported slice. No installed data, deployment or REAL trading
//! is authorized. Physical process tests retain their actual boundary; capital
//! transfer kills cannot stand in for Reserve/BeginPost or power-loss proof.
//!
//! The executing collector enforces current-run discovery/result completeness;
//! its example-target self-tests are separate from these library witnesses.
//! Existing architecture/SQL/transport and final candidate CI remain mandatory
//! external gates. Their gaps below cannot be silently counted as Rust PASS.

pub struct Witness {
    pub package: &'static str,
    pub test: &'static str,
}
const TERMINAL_CONFIG_FRONTIER: Witness = Witness {
    package: "okx-agent",
    test: "account_bootstrap::tests::terminal_census_fresh_config_follows_settlement_frontier_without_relabeling_cache",
};
const EMERGENCY_REMEDIATION: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_failed_stop_emergency_close_and_verified_remediation",
};
const CANCEL_ACK_LOSS: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_partial_protection::risk_book_contract_native_safety_cancel_unavailable_ack_is_not_reposted_or_released",
};
const ENGINE_COMPOSITION: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_engine::risk_book_contract_native_engine_tasks_initialize_opening_and_drain_original_owner",
};
const RETAINED_TRANSPORT: Witness = Witness {
    package: "repository",
    test: "retained_workerd_auth_session_transport_no_financial_owner",
};
const SEQUENTIAL_B: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_public_cohort::risk_book_contract_native_public_b_uses_remaining_budget_after_a_settlement",
};

pub struct Contract {
    pub id: &'static str,
    pub witnesses: &'static [Witness],
    pub missing: Option<&'static str>,
}

const CONNECT: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_connect_contracts::risk_book_contract_native_connect_two_accounts_and_reopen_one_original_sqlite",
};
const UID_REFUSAL: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_connect_contracts::risk_book_contract_native_connect_wrong_uid_does_not_create_account_or_publish_catalog",
};
const CATALOG_RECOVERY: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_connect_contracts::risk_book_contract_native_catalog_failure_retains_sql_and_retries_original_connection",
};
const POLICY: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::campaign_initialization::tests::risk_book_contract_native_campaign_policy_two_accounts_replay_and_no_opening_grant",
};
const POLICY_REFUSAL: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::campaign_initialization::tests::risk_book_contract_native_campaign_policy_rejects_bad_limits_roster_and_refill_atomically",
};
const MAINTENANCE_OPENING: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::risk_book_contract_native_maintenance_installs_original_opening_before_market_admission",
};
const BACKUP: Witness = Witness {
    package: "okx-agent",
    test: "storage_commands::native_backup::tests::native_whole_backup_wrapper_requires_original_metadata_catalog_and_stopped_engine",
};
const DECIMAL: Witness = Witness {
    package: "okx-kernel",
    test: "risk::money::tests::risk_book_contract_legacy_decimal_syntax_and_range",
};
const ARITHMETIC: Witness = Witness {
    package: "okx-kernel",
    test: "risk::money::tests::risk_book_contract_exact_ceiling_conversion_and_checked_arithmetic",
};
const SETTLEMENT_GOLDEN: Witness = Witness {
    package: "okx-kernel",
    test: "risk::settlement::tests::risk_book_contract_settlement_independent_golden_transitions",
};
const SETTLEMENT_INVALID: Witness = Witness {
    package: "okx-kernel",
    test: "risk::settlement::tests::risk_book_contract_settlement_rejects_corrupt_reserve_and_regression",
};
const SECOND_SPEC_COMPONENT: Witness = Witness {
    package: "okx-agent",
    test: "trade_service::plan::tests::b_second_linear_contract_spec_uses_existing_precision_gate",
};
const SECOND_SPEC_PUBLIC: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_second_instrument::risk_book_contract_native_public_second_instrument_spec_precision_and_permission",
};
const ACCOUNT_SOURCE_SCOPE: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_account_source_scope_tests::risk_book_contract_fresh_owner_identical_venue_ids_are_account_scoped_and_deduplicated",
};
const BUSY_OWNER: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_diagnostic_busy_tests::risk_book_contract_native_diagnostic_busy_deadline_preserves_identity_across_reopen",
};
const BUSY_DIAGNOSTIC: Witness = Witness {
    package: "okx-agent",
    test: "diagnostics::native_intent::tests::risk_book_contract_native_diagnostic_busy_read_has_bounded_unknown_result",
};
const STALE_DIAGNOSTIC: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_stale_diagnostic::risk_book_contract_native_stale_post_held_tariff_diagnostic_is_exact_and_read_only",
};
const SOURCE_FEE: Witness = Witness {
    package: "okx-agent",
    test: "trade_service::entry_budget::tests::e1_fee_source_must_be_current_exact_and_content_bound",
};
const SOURCE_FX: Witness = Witness {
    package: "okx-agent",
    test: "trade_service::entry_budget::tests::e1_known_quote_rejects_bad_fx_identity_and_stale_fee_equally",
};
const SOURCE_HELD: Witness = Witness {
    package: "okx-agent",
    test: "trade_service::execute::tests::e1_held_entry_sources_reject_tampering_and_stale_tariffs",
};
const SOURCE_CENSUS: Witness = Witness {
    package: "okx-agent",
    test: "trade_service::execute::tests::e2_census_uses_oldest_verified_account_and_financial_source",
};
const SOURCE_NAV: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::native_admission::census_tests::native_census_rejects_missing_stale_incomplete_or_pre_floor_nav",
};
const TERMINAL_ROLLBACK: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_terminal_rollback_tests::risk_book_contract_fresh_owner_terminal_resolve_success_rollback_then_commit_reopen_once",
};
const JOINT_ROLLBACK: Witness = Witness {
    package: "okx-execution",
    test: "risk::book::tests::risk_book_contract_scoped_original_reserve_shared_budget_and_high_water_on_denial",
};
const B_STATUS_CANCEL: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_public_fairness::risk_book_contract_native_public_b_status_and_cancel_leave_blocked_a_held",
};
const SHARED_CAP_PUBLIC: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_public_cohort::risk_book_contract_native_public_cohort_denied_b_does_not_block_held_a_post",
};
const SHARED_CAP_OWNER: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_multiaccount_reserve::risk_book_contract_native_two_accounts_concurrent_owner_reserves_share_entry_cap",
};
const CAPITAL_SCOPE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_multiaccount::risk_book_contract_native_two_accounts_share_budget_and_isolate_public_capital_receipts",
};
const FOREIGN_CAMPAIGN: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::risk_book_contract_native_capture_and_public_execute_require_original_opening",
};
const CAMPAIGN_SCOPE_COMPONENT: Witness = Witness {
    package: "okx-execution",
    test: "account_store::tests::risk_book_contract_scoped_original_risk_aggregate_checker_is_campaign_isolated",
};
const SEMANTIC_HASH: Witness = Witness {
    package: "okx-protocol",
    test: "trading::typed_strategy_parameters_tests::s2_06_same_golden_preimage_and_sha256_for_reordered_semantics",
};
const SEMANTIC_DECIMAL: Witness = Witness {
    package: "okx-protocol",
    test: "trading::typed_strategy_parameters_tests::s2_01_execute_semantic_decimal_equivalence",
};
const CONCURRENT_REPLAY: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_execute_safety::risk_book_contract_native_execute_100_callers_one_send_policy_cas_and_payload_conflict",
};
const STRATEGY_CAS: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_strategies::risk_book_contract_native_public_strategies_crud_claim_no_trade_share_original_owner",
};
const RESERVE_REPLY: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_reserve_reply_tests::risk_book_contract_fresh_owner_reserve_commit_lost_reply_exact_readback_once_send",
};
const POST_REPLY: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_reserve_reply_tests::risk_book_contract_fresh_owner_begin_post_commit_lost_reply_no_second_claim_or_send",
};
const BOUND_REJECTION: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_execute_safety::risk_book_contract_native_execute_bound_rejection_preserves_code_and_reserve",
};
const FAILED_ENVELOPE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_execute_safety::risk_book_contract_native_execute_all_failed_envelope_preserves_bound_rejection",
};
const FOREIGN_ACK: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_execute_safety::risk_book_contract_native_execute_foreign_rejection_remains_unknown",
};
const LOST_ACK: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once",
};
const HELD_RECOVERY: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_reserve_recovery::risk_book_contract_native_public_cancel_after_held_replays_and_cold_reopens_without_send",
};
const VENUE_CRASH: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_crash::risk_book_contract_native_os_kill_after_create_recovers_original_unknown",
};
const PARTIAL_BILL: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_partial_entry::risk_book_contract_native_partial_sources_missing_bill_then_terminal_status_and_replay",
};
const PARTIAL_STOP: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_partial_protection::risk_book_contract_native_partial_stop_and_late_fill_keep_durable_failure_fence",
};
const REDUCE_STOP: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_partial_protection::risk_book_contract_native_partial_reduce_stop_resize_needs_signed_readback_and_sends_once",
};
const MULTI_CLOSE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_multi_close::risk_book_contract_native_two_closes_use_remaining_lot_and_retire_partial_terminal",
};
const CORRECTION: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_fee_correction::risk_book_contract_native_late_fee_rebate_and_bound_breach_use_immutable_revisions",
};
const CAPITAL_CRASH: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_financial_crash_tests::risk_book_contract_fresh_owner_os_kill_before_and_after_financial_commit",
};
const INITIALIZE: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_startup_tests::risk_book_contract_new_owner_exact_replay_reopen_and_no_trade_permission",
};
const UNKNOWN_FILE: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_startup_tests::risk_book_contract_new_owner_refuses_unknown_file_and_old_state_without_reset",
};
const FOREIGN_WAL: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_startup_tests::risk_book_contract_new_owner_rejects_foreign_sqlite_without_checkpointing_its_wal",
};
const INVALID_INIT: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_startup_tests::risk_book_contract_new_owner_invalid_request_and_open_never_autocreate",
};
const WAL_SNAPSHOT: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_finance_snapshot_tests::risk_book_contract_native_whole_snapshot_includes_uncheckpointed_wal_and_restores_recovery_gate",
};
const SNAPSHOT_REFUSAL: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_finance_snapshot_tests::risk_book_contract_native_snapshot_refuses_tampered_foreign_or_existing_targets",
};
const RESTORE_EXECUTE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_snapshot::risk_book_contract_native_restored_snapshot_blocks_public_execute_until_storage_reconciled",
};
const DIAGNOSTIC: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_diagnostics::risk_book_contract_native_unknown_intent_diagnostic_reads_sql_without_network_or_resend",
};
const DIAGNOSTIC_SCOPE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_diagnostics::risk_book_contract_native_intent_diagnostic_is_account_scoped_and_refuses_foreign_installation",
};
const DIAGNOSTIC_BUSY_COMPONENT: Witness = Witness {
    package: "okx-agent",
    test: "diagnostics::protection_trace::tests::contention_is_counted_and_readback_marks_a_gap",
};
const DIAGNOSTIC_EVICTION_COMPONENT: Witness = Witness {
    package: "okx-agent",
    test: "diagnostics::protection_trace::tests::eviction_is_counted_and_readback_marks_projection_incomplete",
};
const LIFECYCLE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen",
};
const STOP_CLOSE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_stop_after_fill_blocks_open_but_closes_and_releases",
};
const ADD_OWNED: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_add::risk_book_contract_native_public_add_preserves_original_lot_and_protects_increment",
};
const ADD_UNOWNED: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_add::risk_book_contract_native_public_add_rejects_fresh_unowned_venue_increment",
};
const CHANGED_FEE: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_changed_source::risk_book_contract_native_changed_post_held_fee_releases_unsent_and_replans",
};
const POSITIVE_B: Witness = Witness {
    package: "okx-agent",
    test: "accounts::native_trade_contracts::native_public_fairness::risk_book_contract_native_public_b_executes_and_keeps_reserve_while_a_source_blocked",
};
const ARCHITECTURE: Witness = Witness {
    package: "repository",
    test: "architecture_and_research_negative_guards",
};
const REFERENCE_REFRESH: Witness = Witness {
    package: "okx-runtime",
    test: "public::reference_refresh::tests::risk_book_contract_reference_unchanged_rest_preserves_books_and_fresh_reads_are_local",
};
const REFERENCE_CHANGED: Witness = Witness {
    package: "okx-runtime",
    test: "public::reference_refresh::tests::risk_book_contract_reference_changed_rest_denies_plan_and_reconnects",
};
const REFERENCE_RACE: Witness = Witness {
    package: "okx-runtime",
    test: "public::reference_refresh::tests::risk_book_contract_reference_intervening_ws_event_fences_rest_even_when_content_is_unchanged",
};
const REFERENCE_FAILURE: Witness = Witness {
    package: "okx-runtime",
    test: "public::reference_refresh::tests::risk_book_contract_reference_failed_partition_or_deadline_preserves_original_source",
};
const QUEUE_FIFO: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_queue_tests::risk_book_contract_fresh_owner_queue_bounded_128_fifo_and_shutdown",
};
const QUEUE_DEADLINES: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_queue_tests::risk_book_contract_fresh_owner_queue_deadlines_and_dropped_caller",
};
const FINANCIAL_CANCEL: Witness = Witness {
    package: "okx-execution",
    test: "account_worker::unified::owner_financial_reply_tests::risk_book_contract_fresh_owner_financial_cancel_after_enqueue_still_commits_once",
};
const COVERAGE_COMPLETE: Witness = Witness {
    package: "okx-agent",
    test: "native_acceptance_coverage::coverage_verifier_accepts_complete_runtime_result_set_and_labels_subset",
};
const COVERAGE_MISSING: Witness = Witness {
    package: "okx-agent",
    test: "native_acceptance_coverage::coverage_verifier_rejects_missing_zero_duplicate_and_foreign_results",
};
const COVERAGE_FAILURE: Witness = Witness {
    package: "okx-agent",
    test: "native_acceptance_coverage::coverage_verifier_rejects_skips_child_failure_timeout_and_incomplete_teardown",
};
const COVERAGE_SYMBOLS: Witness = Witness {
    package: "okx-agent",
    test: "native_acceptance_coverage::coverage_verifier_requires_each_symbol_for_a_property_and_valid_manifest_context",
};

pub const CONTRACTS: &[Contract] = &[
    Contract {
        id: "RB02.real_reference_refresh_and_coherent_spec_generation",
        witnesses: &[
            REFERENCE_REFRESH,
            REFERENCE_CHANGED,
            REFERENCE_RACE,
            REFERENCE_FAILURE,
        ],
        missing: None,
    },
    Contract {
        id: "RB15.public_b_execute_and_cancel_a_preserve_independent_reserve",
        witnesses: &[POSITIVE_B],
        missing: None,
    },
    Contract {
        id: "RB01.verified_account_connection",
        witnesses: &[CONNECT, UID_REFUSAL, CATALOG_RECOVERY],
        missing: None,
    },
    Contract {
        id: "RB01.strict_campaign_policy",
        witnesses: &[POLICY, POLICY_REFUSAL],
        missing: None,
    },
    Contract {
        // Logical capabilities, not acceptance of historical wire mutators:
        // Summary -> request/campaign_summary (opening and lifecycle readback).
        // InstallEntryOpening -> ProducedOpening/install_reported_opening.
        // AccountSummary -> stopped_account_summary (live and cold readback).
        // Inspect -> request/stopped_inspect_reservation (entry settlement).
        // Reserve -> ProducedReserve/reserve_reported (public Execute).
        // BeginPost -> ProducedPost/begin_post_reported (public single send).
        // SettleEntry -> settle_bound_entry (source-backed public Status).
        // Resolve -> resolve_terminal_status (verified public terminal release).
        // ReleaseUnsent -> reject_and_release_unsent/release_unsent_status.
        id: "RB01.logical_financial_capabilities",
        witnesses: &[MAINTENANCE_OPENING, LIFECYCLE, CHANGED_FEE],
        missing: None,
    },
    Contract {
        id: "RB01.stopped_native_backup_caller",
        witnesses: &[BACKUP],
        missing: None,
    },
    Contract {
        id: "RB02.checked_decimal_syntax_range_and_rounding",
        witnesses: &[DECIMAL, ARITHMETIC],
        missing: None,
    },
    Contract {
        id: "RB02.independent_cumulative_settlement_goldens",
        witnesses: &[SETTLEMENT_GOLDEN, SETTLEMENT_INVALID],
        missing: None,
    },
    Contract {
        id: "RB02.second_instrument_public_spec_permission_and_precision",
        witnesses: &[SECOND_SPEC_COMPONENT, SECOND_SPEC_PUBLIC],
        missing: None,
    },
    Contract {
        id: "RB03.concurrent_public_shared_entry_cap",
        witnesses: &[SHARED_CAP_PUBLIC, SHARED_CAP_OWNER],
        missing: None,
    },
    Contract {
        id: "RB03.public_capital_receipt_account_scope",
        witnesses: &[CAPITAL_SCOPE],
        missing: None,
    },
    Contract {
        id: "RB03.sequential_public_remaining_after_spend",
        witnesses: &[SEQUENTIAL_B],
        missing: None,
    },
    Contract {
        id: "RB03.identical_venue_ids_across_accounts",
        // Actual ingestion DTO and SQL uniqueness boundary, not an additional
        // claim of HTTP authentication or live receipt permission.
        witnesses: &[ACCOUNT_SOURCE_SCOPE, CONNECT, RESERVE_REPLY],
        missing: None,
    },
    Contract {
        // This product has one immutable configured campaign per installation.
        // A foreign campaign is refused, rather than creating a second owner.
        id: "RB03.foreign_campaign_refused_and_scoped_aggregates_isolated",
        witnesses: &[FOREIGN_CAMPAIGN, CAMPAIGN_SCOPE_COMPONENT],
        missing: None,
    },
    Contract {
        id: "RB03.stop_preserves_other_account_reservation",
        witnesses: &[POSITIVE_B],
        missing: None,
    },
    Contract {
        id: "RB04.canonical_semantic_identity",
        witnesses: &[SEMANTIC_HASH, SEMANTIC_DECIMAL],
        missing: None,
    },
    Contract {
        id: "RB04.concurrent_public_replay_payload_conflict_and_policy_cas",
        witnesses: &[CONCURRENT_REPLAY],
        missing: None,
    },
    Contract {
        id: "RB04.public_strategy_revision_cas",
        witnesses: &[STRATEGY_CAS],
        missing: None,
    },
    Contract {
        id: "RB05.committed_begin_post_reply_loss_and_single_claim",
        witnesses: &[POST_REPLY],
        missing: None,
    },
    Contract {
        id: "RB05.bound_rejection_versus_unknown_venue_outcome",
        witnesses: &[BOUND_REJECTION, FAILED_ENVELOPE, FOREIGN_ACK, LOST_ACK],
        missing: None,
    },
    Contract {
        id: "RB05.same_identity_recovery_after_possible_venue_send",
        witnesses: &[VENUE_CRASH],
        missing: None,
    },
    Contract {
        id: "RB06.partial_fill_requires_independent_bill",
        witnesses: &[PARTIAL_BILL],
        missing: None,
    },
    Contract {
        id: "RB06.partial_stop_late_fill_failure_fence",
        witnesses: &[PARTIAL_STOP],
        missing: None,
    },
    Contract {
        id: "RB06.reduced_stop_needs_independent_readback",
        witnesses: &[REDUCE_STOP],
        missing: None,
    },
    Contract {
        id: "RB06.multiple_closes_use_actual_remaining_lot_and_bills",
        witnesses: &[MULTI_CLOSE],
        missing: None,
    },
    Contract {
        id: "RB06.late_fee_rebate_duplicate_and_entry_bound_breach",
        witnesses: &[CORRECTION],
        missing: None,
    },
    Contract {
        id: "RB06.cancel_ack_loss_and_successful_emergency_remediation",
        witnesses: &[
            CANCEL_ACK_LOSS,
            EMERGENCY_REMEDIATION,
            TERMINAL_CONFIG_FRONTIER,
        ],
        missing: None,
    },
    Contract {
        id: "RB06.post_release_economic_correction",
        witnesses: &[LIFECYCLE],
        missing: None,
    },
    Contract {
        id: "RB07.reserve_and_original_local_receipt_one_commit",
        witnesses: &[RESERVE_REPLY],
        missing: None,
    },
    Contract {
        id: "RB07.begin_post_and_original_send_claim_one_commit",
        witnesses: &[POST_REPLY],
        missing: None,
    },
    Contract {
        id: "RB07.remaining_financial_action_rollback_atomicity",
        // Scoped production SQL rollback components establish transaction
        // atomicity; fresh public sources and the common real SQLite crash
        // witnesses establish producer and physical boundaries separately.
        witnesses: &[
            JOINT_ROLLBACK,
            TERMINAL_ROLLBACK,
            LIFECYCLE,
            CHANGED_FEE,
            CAPITAL_CRASH,
        ],
        missing: None,
    },
    Contract {
        id: "RB08.capital_transaction_os_kill_before_and_after_commit",
        witnesses: &[CAPITAL_CRASH],
        missing: None,
    },
    Contract {
        id: "RB08.physical_second_writer_exclusion_after_reopen",
        witnesses: &[CAPITAL_CRASH],
        missing: None,
    },
    Contract {
        id: "RB08.venue_create_before_outcome_os_kill",
        witnesses: &[VENUE_CRASH],
        missing: None,
    },
    Contract {
        // Shared fresh-owner SQLite/WAL before/after-COMMIT process kills
        // establish physical rollback/durability; the Native Create kill
        // reopens the actual durable Reserve and BeginPost. Exact production
        // Reserve/BeginPost lost-reply tests separately establish their paired
        // primary/receipt/claim effects and cold replay. No external side
        // effect lies between Reserve COMMIT and the next SQL operation.
        // Retire duplicate per-action OS-kill schedules, retaining atomicity,
        // original identity, no second grant/send and real process loss. This
        // composed proof makes no power-loss or capital-only admission claim.
        id: "RB08.admission_durability_composed_physical_and_logical",
        witnesses: &[CAPITAL_CRASH, VENUE_CRASH, RESERVE_REPLY, POST_REPLY],
        missing: None,
    },
    Contract {
        id: "RB08.retained_transport_and_retired_remote_finance_crash",
        // The remote financial commit/reply boundary ceased to exist. Its OS
        // kill schedule is RETIRED, not proved by dispose(). Actual workerd
        // retains auth/session/delivery and refuses former money commands.
        // Native real SQLite OS-kill tests retain the financial guarantee.
        witnesses: &[RETAINED_TRANSPORT, CAPITAL_CRASH, VENUE_CRASH],
        missing: None,
    },
    Contract {
        id: "RB09.explicit_initialize_exact_replay_and_reopen",
        witnesses: &[INITIALIZE],
        missing: None,
    },
    Contract {
        id: "RB09.unknown_foreign_or_invalid_target_never_resets",
        witnesses: &[UNKNOWN_FILE, FOREIGN_WAL, INVALID_INIT],
        missing: None,
    },
    Contract {
        id: "RB09.whole_wal_snapshot_and_recovery_fence",
        witnesses: &[WAL_SNAPSHOT, SNAPSHOT_REFUSAL, RESTORE_EXECUTE],
        missing: None,
    },
    Contract {
        id: "RB09.standard_native_startup_composition",
        // Actual shipped Engine task composition/capture/opening/drain.
        // Real Ctrl-C/stdin, installed CONTROL and production WSS identity
        // remain #449 INSTALLED/PHYSICAL, not implicit CODE test claims.
        witnesses: &[ENGINE_COMPOSITION, INITIALIZE, UNKNOWN_FILE],
        missing: None,
    },
    Contract {
        id: "RB10.scoped_durable_intent_diagnostic_without_network",
        witnesses: &[DIAGNOSTIC, DIAGNOSTIC_SCOPE, VENUE_CRASH],
        missing: None,
    },
    Contract {
        id: "RB10.busy_eviction_and_restart_report_incompleteness",
        // Financial intent facts read SQL directly and have no volatile LRU.
        // Cache eviction is tested only for the protection projection that
        // actually owns that cache; busy SQL never means intent not found.
        witnesses: &[
            BUSY_OWNER,
            BUSY_DIAGNOSTIC,
            DIAGNOSTIC,
            HELD_RECOVERY,
            DIAGNOSTIC_BUSY_COMPONENT,
            DIAGNOSTIC_EVICTION_COMPONENT,
        ],
        missing: None,
    },
    Contract {
        id: "RB11.research_has_no_financial_writer_or_venue_capability",
        witnesses: &[ARCHITECTURE],
        missing: None,
    },
    Contract {
        id: "RB11.trade_lifecycle_requires_no_research_job",
        witnesses: &[LIFECYCLE],
        missing: None,
    },
    Contract {
        id: "RB12.public_execute_status_close_release_reopen",
        witnesses: &[LIFECYCLE],
        missing: None,
    },
    Contract {
        id: "RB12.public_add_preserves_owned_original_and_increment",
        witnesses: &[ADD_OWNED],
        missing: None,
    },
    Contract {
        id: "RB12.public_add_denies_external_or_unknown_exposure",
        witnesses: &[ADD_UNOWNED],
        missing: None,
    },
    Contract {
        id: "RB12.stop_denies_new_risk_but_allows_verified_close",
        witnesses: &[STOP_CLOSE],
        missing: None,
    },
    Contract {
        id: "RB12.changed_post_held_fee_exact_unsent_release_and_replan",
        witnesses: &[CHANGED_FEE],
        missing: None,
    },
    Contract {
        id: "RB12.missing_or_stale_source_before_and_after_held",
        // Compose the actual per-source rule functions with the public generic
        // fence-error -> durable no-send/release -> new-plan orchestration.
        // This does not claim an outage test for every field/service pair.
        witnesses: &[
            SOURCE_FEE,
            SOURCE_FX,
            SOURCE_HELD,
            SOURCE_CENSUS,
            SOURCE_NAV,
            CHANGED_FEE,
            REFERENCE_REFRESH,
            REFERENCE_CHANGED,
            REFERENCE_RACE,
            REFERENCE_FAILURE,
        ],
        missing: None,
    },
    Contract {
        id: "RB13.layer_visibility_and_negative_architecture_guards",
        witnesses: &[ARCHITECTURE],
        missing: None,
    },
    Contract {
        id: "RB14.direct_budget_denied_diagnostic_drill",
        witnesses: &[SHARED_CAP_PUBLIC],
        // Actual read-only diagnostic asserts owner, refusal, no receipt/send
        // and no extra HTTP while the other account remains held.
        missing: None,
    },
    Contract {
        id: "RB14.direct_stale_source_diagnostic_drill",
        witnesses: &[STALE_DIAGNOSTIC],
        missing: None,
    },
    Contract {
        id: "RB14.direct_commit_reply_lost_diagnostic_drill",
        witnesses: &[RESERVE_REPLY, POST_REPLY, DIAGNOSTIC],
        // The actual lost-COMMIT-reply tests now invoke the SAME coherent
        // inspection operation used by the public projection, before/after
        // reopen. Public projection separately proves zero HTTP/POST.
        missing: None,
    },
    Contract {
        id: "RB15.queue_capacity_128_fifo_and_shutdown",
        witnesses: &[QUEUE_FIFO],
        missing: None,
    },
    Contract {
        id: "RB15.queue_deadline_and_dropped_read_caller",
        witnesses: &[QUEUE_DEADLINES],
        missing: None,
    },
    Contract {
        id: "RB15.financial_caller_cancel_after_enqueue_commits_once",
        witnesses: &[FINANCIAL_CANCEL],
        missing: None,
    },
    Contract {
        id: "RB15.public_b_status_and_cancel_under_blocked_a_source",
        witnesses: &[B_STATUS_CANCEL, POSITIVE_B],
        missing: None,
    },
    Contract {
        id: "RB16.coverage_verifier_negative_controls",
        witnesses: &[
            COVERAGE_COMPLETE,
            COVERAGE_MISSING,
            COVERAGE_FAILURE,
            COVERAGE_SYMBOLS,
        ],
        missing: None,
    },
    Contract {
        id: "RB16.declared_numeric_workload_deadline_and_resource_budgets",
        witnesses: &[],
        missing: Some(
            "Census32 and queue128 are product bounds, not a complete acceptance workload. Numerical operation deadlines, repetitions and SQL/queue/I/O/CPU/RAM budgets must be declared and justified before full acceptance.",
        ),
    },
    Contract {
        id: "RB16.comparable_wall_time_and_resource_measurements",
        witnesses: &[],
        missing: Some(
            "Missing sufficient same-candidate setup-through-physical-through-teardown samples and comparable baseline with p50/p95/max and SQL/queue/I/O/CPU/RAM evidence; build time must be separate. One run cannot establish p95.",
        ),
    },
];
