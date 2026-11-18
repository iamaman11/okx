use jiff::Timestamp;
use okx_api::{OkxPublicClient, PublicDataApi};
use okx_observation::ReferenceRegistry;

use crate::AgentResult;

pub async fn bootstrap_reference(client: OkxPublicClient) -> AgentResult<ReferenceRegistry> {
    let public = PublicDataApi::new(client);
    let instruments = public.derivative_instruments().await?;
    let received_at = format!("{:.3}", Timestamp::now());
    Ok(ReferenceRegistry::from_public(received_at, instruments)?)
}
