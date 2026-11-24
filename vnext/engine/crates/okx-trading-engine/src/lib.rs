//! vNext trading command boundary.
//!
//! This layer only validates transport values. It cannot authorize risk,
//! reserve funds, mint a `CommittedVenueSend`, or submit an order. Those
//! operations belong to the financial owner in `okx-trade-execution`.
//! A production command handler must require a separate durable financial
//! admission result before performing any venue effect.

use rust_decimal::Decimal;
use serde::Deserialize;

/// A field is trusted only after the complete payload has been validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationStage {
    Wire,
    Identity,
    Quantity,
    Time,
    Evidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationCode {
    InvalidWire,
    InvalidIdentifier,
    InvalidQuantity,
    InvalidTime,
    Expired,
    InvalidDigest,
    InvalidSequence,
    InvalidVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationError {
    pub stage: ValidationStage,
    pub code: ValidationCode,
}

impl ValidationError {
    const fn new(stage: ValidationStage, code: ValidationCode) -> Self {
        Self { stage, code }
    }
}

fn bounded_identifier(raw: String, max_len: usize) -> Result<Box<str>, ValidationError> {
    if raw.is_empty()
        || raw.len() > max_len
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
    {
        return Err(ValidationError::new(
            ValidationStage::Identity,
            ValidationCode::InvalidIdentifier,
        ));
    }
    Ok(raw.into_boxed_str())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountId(Box<str>);

impl AccountId {
    pub fn parse(raw: String) -> Result<Self, ValidationError> {
        Ok(Self(bounded_identifier(raw, 160)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentId(Box<str>);

impl InstrumentId {
    pub fn parse(raw: String) -> Result<Self, ValidationError> {
        Ok(Self(bounded_identifier(raw, 80)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationId(Box<str>);

impl OperationId {
    pub fn parse(raw: String) -> Result<Self, ValidationError> {
        Ok(Self(bounded_identifier(raw, 128)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest(Box<str>);

impl Digest {
    pub fn parse(raw: String) -> Result<Self, ValidationError> {
        if raw.len() != 64
            || !raw
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ValidationError::new(
                ValidationStage::Evidence,
                ValidationCode::InvalidDigest,
            ));
        }
        Ok(Self(raw.into_boxed_str()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositiveQuantity(Decimal);

impl PositiveQuantity {
    pub fn parse(raw: &str) -> Result<Self, ValidationError> {
        let valid_decimal = !raw.is_empty()
            && raw.len() <= 64
            && raw.as_bytes().first().is_some_and(u8::is_ascii_digit)
            && raw.as_bytes().last().is_some_and(u8::is_ascii_digit)
            && raw.bytes().all(|b| b.is_ascii_digit() || b == b'.')
            && raw.bytes().filter(|b| *b == b'.').count() <= 1;
        if !valid_decimal {
            return Err(ValidationError::new(
                ValidationStage::Quantity,
                ValidationCode::InvalidQuantity,
            ));
        }
        let value = Decimal::from_str_exact(raw).map_err(|_| {
            ValidationError::new(ValidationStage::Quantity, ValidationCode::InvalidQuantity)
        })?;
        if value <= Decimal::ZERO {
            return Err(ValidationError::new(
                ValidationStage::Quantity,
                ValidationCode::InvalidQuantity,
            ));
        }
        Ok(Self(value))
    }

    pub fn amount(self) -> Decimal {
        self.0
    }
}

/// This is a target, not a request to issue a venue POST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionTarget {
    Flat,
    Long(PositiveQuantity),
    Short(PositiveQuantity),
}

/// Hold explicitly differs from setting a flat target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyDecision {
    Hold,
    SetTarget(PositionTarget),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalSequence(u64);

impl SignalSequence {
    pub fn new(raw: u64) -> Result<Self, ValidationError> {
        if raw == 0 {
            return Err(ValidationError::new(
                ValidationStage::Evidence,
                ValidationCode::InvalidSequence,
            ));
        }
        Ok(Self(raw))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyVersion(u64);

impl StrategyVersion {
    pub fn new(raw: u64) -> Result<Self, ValidationError> {
        if raw == 0 {
            return Err(ValidationError::new(
                ValidationStage::Evidence,
                ValidationCode::InvalidVersion,
            ));
        }
        Ok(Self(raw))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmissionWindow {
    observed_at_ms: i64,
    expires_at_ms: i64,
}

impl SubmissionWindow {
    pub fn new(observed_at_ms: i64, expires_at_ms: i64) -> Result<Self, ValidationError> {
        if observed_at_ms <= 0 || expires_at_ms <= observed_at_ms {
            return Err(ValidationError::new(
                ValidationStage::Time,
                ValidationCode::InvalidTime,
            ));
        }
        Ok(Self { observed_at_ms, expires_at_ms })
    }

    pub fn is_expired(self, now_ms: i64) -> bool {
        now_ms >= self.expires_at_ms
    }

    pub fn observed_at_ms(self) -> i64 {
        self.observed_at_ms
    }

    pub fn expires_at_ms(self) -> i64 {
        self.expires_at_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualTargetRequest {
    pub account: AccountId,
    pub instrument: InstrumentId,
    pub operation: OperationId,
    pub target: PositionTarget,
    pub window: SubmissionWindow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategySignal {
    pub account: AccountId,
    pub instrument: InstrumentId,
    pub strategy_instance: OperationId,
    pub version: StrategyVersion,
    pub closed_bar_id: OperationId,
    pub sequence: SignalSequence,
    pub source_digest: Digest,
    pub decision: StrategyDecision,
    pub window: SubmissionWindow,
}

/// A valid command is NOT a financially admitted instruction or a venue send grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedCommand {
    Manual(ManualTargetRequest),
    Strategy(StrategySignal),
}

impl ValidatedCommand {
    /// A second, authoritative expiry check must also occur at durable admission.
    pub fn require_fresh(&self, now_ms: i64) -> Result<(), ValidationError> {
        let window = match self {
            Self::Manual(command) => command.window,
            Self::Strategy(signal) => signal.window,
        };
        if window.is_expired(now_ms) {
            return Err(ValidationError::new(
                ValidationStage::Time,
                ValidationCode::Expired,
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum UntrustedTarget {
    Flat,
    Long { quantity: String },
    Short { quantity: String },
}

impl TryFrom<UntrustedTarget> for PositionTarget {
    type Error = ValidationError;

    fn try_from(target: UntrustedTarget) -> Result<Self, Self::Error> {
        Ok(match target {
            UntrustedTarget::Flat => Self::Flat,
            UntrustedTarget::Long { quantity } => Self::Long(PositiveQuantity::parse(&quantity)?),
            UntrustedTarget::Short { quantity } => Self::Short(PositiveQuantity::parse(&quantity)?),
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum UntrustedDecision {
    Hold,
    SetTarget { target: UntrustedTarget },
}

impl TryFrom<UntrustedDecision> for StrategyDecision {
    type Error = ValidationError;

    fn try_from(decision: UntrustedDecision) -> Result<Self, Self::Error> {
        match decision {
            UntrustedDecision::Hold => Ok(Self::Hold),
            UntrustedDecision::SetTarget { target } => {
                Ok(Self::SetTarget(PositionTarget::try_from(target)?))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum UntrustedCommand {
    Manual {
        account: String,
        instrument: String,
        operation: String,
        target: UntrustedTarget,
        observed_at_ms: i64,
        expires_at_ms: i64,
    },
    Strategy {
        account: String,
        instrument: String,
        strategy_instance: String,
        version: u64,
        closed_bar_id: String,
        sequence: u64,
        source_digest: String,
        decision: UntrustedDecision,
        observed_at_ms: i64,
        expires_at_ms: i64,
    },
}

/// Parses untrusted wire JSON and validates all domain fields before exposure.
/// It performs NO business/risk admission and has NO external side effects.
pub fn validate_wire_command(wire_json: &str) -> Result<ValidatedCommand, ValidationError> {
    let wire: UntrustedCommand = serde_json::from_str(wire_json).map_err(|_| {
        ValidationError::new(ValidationStage::Wire, ValidationCode::InvalidWire)
    })?;
    Ok(match wire {
        UntrustedCommand::Manual {
            account,
            instrument,
            operation,
            target,
            observed_at_ms,
            expires_at_ms,
        } => ValidatedCommand::Manual(ManualTargetRequest {
            account: AccountId::parse(account)?,
            instrument: InstrumentId::parse(instrument)?,
            operation: OperationId::parse(operation)?,
            target: PositionTarget::try_from(target)?,
            window: SubmissionWindow::new(observed_at_ms, expires_at_ms)?,
        }),
        UntrustedCommand::Strategy {
            account,
            instrument,
            strategy_instance,
            version,
            closed_bar_id,
            sequence,
            source_digest,
            decision,
            observed_at_ms,
            expires_at_ms,
        } => ValidatedCommand::Strategy(StrategySignal {
            account: AccountId::parse(account)?,
            instrument: InstrumentId::parse(instrument)?,
            strategy_instance: OperationId::parse(strategy_instance)?,
            version: StrategyVersion::new(version)?,
            closed_bar_id: OperationId::parse(closed_bar_id)?,
            sequence: SignalSequence::new(sequence)?,
            source_digest: Digest::parse(source_digest)?,
            decision: StrategyDecision::try_from(decision)?,
            window: SubmissionWindow::new(observed_at_ms, expires_at_ms)?,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manual(target: &str) -> String {
        format!(
            r#"{{"kind":"manual","account":"okx:demo:account1","instrument":"BTC-USDT-SWAP","operation":"cmd-1","target":{target},"observed_at_ms":100,"expires_at_ms":200}}"#
        )
    }

    fn strategy(decision: &str) -> String {
        format!(
            r#"{{"kind":"strategy","account":"okx:demo:account1","instrument":"BTC-USDT-SWAP","strategy_instance":"strategy-1","version":1,"closed_bar_id":"BTC-1m-100","sequence":7,"source_digest":"{}","decision":{decision},"observed_at_ms":100,"expires_at_ms":200}}"#,
            "a".repeat(64)
        )
    }

    #[test]
    fn hold_is_not_flat() {
        let hold = validate_wire_command(&strategy(r#"{"kind":"hold"}"#)).unwrap();
        let flat = validate_wire_command(&strategy(
            r#"{"kind":"set_target","target":{"kind":"flat"}}"#,
        ))
        .unwrap();
        assert!(matches!(
            hold,
            ValidatedCommand::Strategy(StrategySignal {
                decision: StrategyDecision::Hold,
                ..
            })
        ));
        assert!(matches!(
            flat,
            ValidatedCommand::Strategy(StrategySignal {
                decision: StrategyDecision::SetTarget(PositionTarget::Flat),
                ..
            })
        ));
        assert_ne!(hold, flat);
    }

    #[test]
    fn manual_command_only_proposes_target() {
        let parsed =
            validate_wire_command(&manual(r#"{"kind":"long","quantity":"0.010"}"#))
                .unwrap();
        let ValidatedCommand::Manual(command) = parsed else { panic!("manual") };
        assert_eq!(command.account.as_str(), "okx:demo:account1");
        assert_eq!(command.instrument.as_str(), "BTC-USDT-SWAP");
        assert_eq!(command.operation.as_str(), "cmd-1");
        assert!(matches!(command.target, PositionTarget::Long(_)));
    }

    #[test]
    fn rejects_float_or_nonpositive_quantity_before_financial_admission() {
        for amount in ["0", "-1", "1e3", "1.", "1.2.3"] {
            let input = manual(&format!(r#"{{"kind":"long","quantity":"{amount}"}}"#));
            assert_eq!(
                validate_wire_command(&input).unwrap_err().stage,
                ValidationStage::Quantity
            );
        }
        let excessive_precision = format!("0.{}1", "0".repeat(30));
        let input = manual(&format!(
            r#"{{"kind":"long","quantity":"{excessive_precision}"}}"#
        ));
        assert_eq!(
            validate_wire_command(&input).unwrap_err().stage,
            ValidationStage::Quantity
        );
        let float_input = manual(r#"{"kind":"long","quantity":1.1}"#);
        assert_eq!(
            validate_wire_command(&float_input).unwrap_err().stage,
            ValidationStage::Wire
        );
    }

    #[test]
    fn rejects_unknown_or_forged_fields() {
        let input = manual(r#"{"kind":"flat","risk_approved":true}"#);
        assert_eq!(
            validate_wire_command(&input).unwrap_err().code,
            ValidationCode::InvalidWire
        );
        let input = manual(r#"{"kind":"execute_post"}"#);
        assert_eq!(
            validate_wire_command(&input).unwrap_err().code,
            ValidationCode::InvalidWire
        );
    }

    #[test]
    fn rejects_invalid_account_and_digest() {
        let invalid_account =
            manual(r#"{"kind":"flat"}"#).replace("okx:demo:account1", "other account");
        assert_eq!(
            validate_wire_command(&invalid_account).unwrap_err().code,
            ValidationCode::InvalidIdentifier
        );
        let invalid_digest =
            strategy(r#"{"kind":"hold"}"#).replace(&"a".repeat(64), "not-a-digest");
        assert_eq!(
            validate_wire_command(&invalid_digest).unwrap_err().code,
            ValidationCode::InvalidDigest
        );
    }

    #[test]
    fn rejects_zero_signal_sequence_and_expiration() {
        let zero = strategy(r#"{"kind":"hold"}"#).replace("\"sequence\":7", "\"sequence\":0");
        assert_eq!(
            validate_wire_command(&zero).unwrap_err().code,
            ValidationCode::InvalidSequence
        );
        let valid = validate_wire_command(&strategy(r#"{"kind":"hold"}"#)).unwrap();
        assert!(valid.require_fresh(199).is_ok());
        assert_eq!(
            valid.require_fresh(200).unwrap_err().code,
            ValidationCode::Expired
        );
        let invalid = manual(r#"{"kind":"flat"}"#)
            .replace("\"expires_at_ms\":200", "\"expires_at_ms\":99");
        assert_eq!(
            validate_wire_command(&invalid).unwrap_err().code,
            ValidationCode::InvalidTime
        );
    }
}
