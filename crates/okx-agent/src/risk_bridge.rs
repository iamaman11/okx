//! One bounded native requester over the existing authenticated WS ownership chain.
//! Timeout is UNKNOWN. Never retry a risk mutation or an exchange POST.
use okx_protocol::risk_authority::{RiskCommand, RiskResult};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub struct OutboundRiskCommand {
    pub command: RiskCommand,
    pub return_to: oneshot::Sender<RiskResult>,
}
#[derive(Clone)]
pub struct NativeRiskClient {
    tx: mpsc::Sender<OutboundRiskCommand>,
}
impl NativeRiskClient {
    pub fn new(tx: mpsc::Sender<OutboundRiskCommand>) -> Self {
        Self { tx }
    }
    pub async fn request(&self, command: RiskCommand) -> Result<RiskResult, &'static str> {
        if !command.valid() {
            return Err("INVALID_RISK_COMMAND");
        }
        let id = command.command_id.clone();
        let (reply, rx) = oneshot::channel();
        self.tx
            .try_send(OutboundRiskCommand {
                command,
                return_to: reply,
            })
            .map_err(|_| "RISK_TRANSPORT_UNAVAILABLE")?;
        let response = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY")?
            .map_err(|_| "RISK_TRANSPORT_LOST_DO_NOT_REPLAY")?;
        if !response.valid() || response.command_id != id {
            return Err("RISK_RESPONSE_IDENTITY_MISMATCH");
        }
        Ok(response)
    }
}
