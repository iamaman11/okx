// The canonical checked-in proto/host_state.proto defines these generated
// message fields. Explicit prost derive avoids a build.rs/protoc dependency,
// keeping release builds reproducible on native Windows and Linux.
use prost::Message;

pub const HOST_STATE_VERSION: u32 = 1;
pub const MAX_HOST_STATE_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, Message)]
pub struct HostStateWire {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub desired_agent: u32,
    #[prost(uint32, tag = "3")]
    pub selected_environment: u32,
    #[prost(bool, tag = "4")]
    pub read_only: bool,
    #[prost(message, optional, tag = "5")]
    pub legacy_production_policy: Option<LegacyProductionPolicyWire>,
    #[prost(message, optional, tag = "6")]
    pub transition: Option<TransitionWire>,
    #[prost(message, optional, tag = "7")]
    pub switch_hold: Option<SourceDrainHoldWire>,
}
#[derive(Clone, PartialEq, Message)]
pub struct LegacyProductionPolicyWire {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(uint64, tag = "2")]
    pub revision: u64,
    #[prost(uint32, tag = "3")]
    pub permission: u32,
    #[prost(string, tag = "4")]
    pub account_uid_fingerprint: String,
    #[prost(string, tag = "5")]
    pub strategy_run_id: String,
    #[prost(string, tag = "6")]
    pub operator_authority_id: String,
    #[prost(string, repeated, tag = "7")]
    pub allowed_instruments: Vec<String>,
    #[prost(string, tag = "8")]
    pub max_total_modelled_loss_usdt: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct SourceDrainHoldWire {
    #[prost(uint32, tag = "1")]
    pub source_environment: u32,
    #[prost(uint32, tag = "2")]
    pub destination_environment: u32,
    #[prost(uint32, tag = "3")]
    pub source_pid: u32,
}
#[derive(Clone, PartialEq, Message)]
pub struct SourceWitnessWire {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(uint32, tag = "2")]
    pub source_environment: u32,
    #[prost(uint32, tag = "3")]
    pub destination_environment: u32,
    #[prost(uint32, tag = "4")]
    pub source_pid: u32,
    #[prost(string, tag = "5")]
    pub launch_lease_sha256: String,
    #[prost(string, tag = "6")]
    pub account_uid_fingerprint: String,
    #[prost(uint64, tag = "7")]
    pub private_ws_generation: u64,
    #[prost(string, tag = "8")]
    pub ledger_manifest_sha256: String,
    #[prost(string, tag = "9")]
    pub source_tree: String,
    #[prost(string, tag = "10")]
    pub agent_sha256: String,
    #[prost(string, tag = "11")]
    pub controller_sha256: String,
    #[prost(uint64, tag = "12")]
    pub source_observed_at_ms: u64,
    #[prost(uint64, tag = "13")]
    pub issued_at_ms: u64,
    #[prost(uint64, tag = "14")]
    pub expires_at_ms: u64,
}
#[derive(Clone, PartialEq, Message)]
pub struct TransitionWire {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(uint32, tag = "2")]
    pub phase: u32,
    #[prost(message, optional, tag = "3")]
    pub witness: Option<SourceWitnessWire>,
    #[prost(uint64, optional, tag = "4")]
    pub consumed_at_ms: Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    pub source_exited_at_ms: Option<u64>,
    #[prost(string, tag = "6")]
    pub checksum_sha256: String,
}

pub fn decode_host_state(bytes: &[u8]) -> Option<HostStateWire> {
    if bytes.is_empty() || bytes.len() > MAX_HOST_STATE_BYTES {
        return None;
    }
    let decoded = HostStateWire::decode(bytes).ok()?;
    // Canonical representation also rejects duplicate/unknown fields and
    // corrupted trailing framing, rather than ignoring potential authority.
    if decoded.version != HOST_STATE_VERSION || decoded.encode_to_vec() != bytes {
        return None;
    }
    Some(decoded)
}

pub fn encode_host_state(state: &HostStateWire) -> Option<Vec<u8>> {
    if state.version != HOST_STATE_VERSION {
        return None;
    }
    let encoded = state.encode_to_vec();
    if encoded.is_empty() || encoded.len() > MAX_HOST_STATE_BYTES {
        return None;
    }
    Some(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_codec_refuses_unknown_fields_and_malformed_snapshots() {
        let state = HostStateWire {
            version: HOST_STATE_VERSION,
            desired_agent: 2,
            selected_environment: 1,
            read_only: true,
            legacy_production_policy: None,
            transition: None,
            switch_hold: None,
        };
        let wire = encode_host_state(&state).unwrap();
        assert_eq!(decode_host_state(&wire), Some(state));
        assert!(decode_host_state(&[]).is_none());
        assert!(decode_host_state(&[0xff]).is_none());
        let mut unknown = wire.clone();
        unknown.extend([0x98, 0x06, 0x01]);
        assert!(decode_host_state(&unknown).is_none());
        assert!(decode_host_state(&vec![0; MAX_HOST_STATE_BYTES + 1]).is_none());
    }
}
