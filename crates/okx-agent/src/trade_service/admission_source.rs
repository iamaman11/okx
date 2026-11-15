//! Internal admission packets created at actual production source boundaries.
use super::*;
pub(crate) struct ProducedReserve {
    command: RiskCommand,
    incarnation: String,
    native_loss_usd: String,
}
pub(crate) struct ProducedPost {
    command: RiskCommand,
}
impl ProducedReserve {
    pub(super) fn from_production(
        command: RiskCommand,
        incarnation: String,
        planned_loss_usd: Decimal,
    ) -> Result<Self, String> {
        if !command.valid() || !matches!(command.action, RiskCommandAction::Reserve { .. }) {
            return Err("NATIVE_RESERVE_PRODUCER_INVALID".into());
        }
        let RiskCommandAction::Reserve {
            requested_loss_usdt,
            usd_conversion,
            ..
        } = &command.action
        else {
            return Err("NATIVE_RESERVE_PRODUCER_INVALID".into());
        };
        // The legacy transport envelope has a nine-place USD limit. Native
        // admission carries the exact product separately; SQL independently
        // rederives it from the immutable plan before reserving any money.
        let (native_usdt, native_loss_usd) = okx_execution::risk::native_reserved_loss(
            planned_loss_usd,
            dec(&usd_conversion.usd_per_usdt)?,
        )
        .map_err(err)?;
        if dec(&native_usdt)? != dec(requested_loss_usdt)? {
            return Err("NATIVE_RESERVE_PRODUCER_LOSS_MISMATCH".into());
        }
        Ok(Self {
            command,
            incarnation,
            native_loss_usd,
        })
    }
    pub(crate) fn into_parts(self) -> (RiskCommand, String, String) {
        (self.command, self.incarnation, self.native_loss_usd)
    }
}
impl ProducedPost {
    pub(super) fn from_production(command: RiskCommand) -> Result<Self, String> {
        if !command.valid() || !matches!(command.action, RiskCommandAction::BeginPost { .. }) {
            return Err("NATIVE_POST_PRODUCER_INVALID".into());
        }
        Ok(Self { command })
    }
    pub(crate) fn into_parts(self) -> RiskCommand {
        self.command
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn command() -> RiskCommand {
        let uid = "a".repeat(64);
        let account = format!("okx:real:{uid}");
        RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "reserve_12345678".into(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Reserve {
                venue: "okx".into(),
                account_uid: uid,
                run_id: "run_12345678".into(),
                intent_id: "intent_12345678".into(),
                strategy_id: "momentum_A".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                plan_sha256: "c".repeat(64),
                requested_loss_usdt: "0.890109892".into(),
                requested_loss_usd: "1.000000002".into(),
                entry_upper_usd: Some("2".into()),
                entry_proof_digest: Some("b".repeat(64)),
                usd_conversion: UsdConversion {
                    usd_per_usdt: "1.123456789".into(),
                    source_digest: "a".repeat(64),
                    observed_at_ms: 1,
                },
                policy_revision: 1,
                census: NativeRiskCensus {
                    account_keys: vec![account.clone()],
                    occupied: vec![RiskAccountOccupancy {
                        account_key: account,
                        occupied_usdt: "0".into(),
                        cumulative_loss_usd: "0".into(),
                    }],
                    generated_at_ms: 1,
                    source_read_floor_ms: 1,
                    source_digest: "b".repeat(64),
                    content_digest: "c".repeat(64),
                    complete: true,
                    native_account_evidence_complete: true,
                    native_order_evidence_complete: true,
                    native_ledger_evidence_complete: true,
                },
            },
        }
    }
    #[test]
    fn native_packet_keeps_exact_product_without_loosening_wire_contract() {
        let wire = command();
        assert!(wire.valid());
        let packet = ProducedReserve::from_production(wire, "d".repeat(32), Decimal::ONE).unwrap();
        let (wire, _, native_usd) = packet.into_parts();
        assert!(wire.valid());
        assert_eq!(native_usd, "1.000000001123456788");
        let RiskCommandAction::Reserve {
            requested_loss_usd, ..
        } = wire.action
        else {
            panic!()
        };
        assert_eq!(requested_loss_usd, "1.000000002");
    }
    #[test]
    fn native_packet_cannot_size_a_different_plan_or_partial_census() {
        assert!(ProducedReserve::from_production(command(), "d".repeat(32), Decimal::TEN).is_err());
        let mut wire = command();
        if let RiskCommandAction::Reserve { census, .. } = &mut wire.action {
            census.native_ledger_evidence_complete = false;
        }
        assert!(ProducedReserve::from_production(wire, "d".repeat(32), Decimal::ONE).is_err());
    }
    #[tokio::test]
    async fn produced_native_reserve_cannot_fall_back_to_cloudflare() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let client = NativeRiskClient::new(tx);
        let packet =
            ProducedReserve::from_production(command(), "d".repeat(32), Decimal::ONE).unwrap();
        assert_eq!(
            client.reserve_produced(packet).await.err(),
            Some("NATIVE_FINANCE_ROUTE_REQUIRED")
        );
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}
