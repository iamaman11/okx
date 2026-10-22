//! Canonical process desired state. Version 2 never decodes previous host journals.
use prost::Message;
pub const HOST_STATE_VERSION: u32 = 2;
pub const MAX_HOST_STATE_BYTES: usize = 64;
#[derive(Clone, PartialEq, Message)]
pub struct HostStateWire {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub desired_agent: u32,
}
pub fn decode_host_state(bytes: &[u8]) -> Option<HostStateWire> {
    if bytes.is_empty() || bytes.len() > MAX_HOST_STATE_BYTES {
        return None;
    }
    let v = HostStateWire::decode(bytes).ok()?;
    if v.version != HOST_STATE_VERSION
        || !matches!(v.desired_agent, 1 | 2)
        || v.encode_to_vec() != bytes
    {
        return None;
    }
    Some(v)
}
pub fn encode_host_state(v: &HostStateWire) -> Option<Vec<u8>> {
    if v.version != HOST_STATE_VERSION || !matches!(v.desired_agent, 1 | 2) {
        None
    } else {
        Some(v.encode_to_vec())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_and_previous_versions_are_rejected() {
        let v = HostStateWire {
            version: 2,
            desired_agent: 1,
        };
        let bytes = encode_host_state(&v).unwrap();
        assert_eq!(decode_host_state(&bytes), Some(v));
        assert!(decode_host_state(&[8, 1, 16, 1]).is_none());
        let mut extended = bytes;
        extended.extend([24, 1]);
        assert!(decode_host_state(&extended).is_none());
    }
}
