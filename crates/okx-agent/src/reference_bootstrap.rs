use chrono::{SecondsFormat, Utc};
use okx_api::{OkxPublicClient, PublicDataApi};
use okx_observation::ReferenceRegistry;

use crate::AgentResult;

pub async fn bootstrap_reference(client: OkxPublicClient) -> AgentResult<ReferenceRegistry> {
    let public = PublicDataApi::new(client);
    let instruments = public.derivative_instruments().await?;
    let received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    Ok(ReferenceRegistry::from_public(received_at, instruments)?)
}
