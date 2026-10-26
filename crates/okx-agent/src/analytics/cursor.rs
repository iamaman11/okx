//! Durable, authenticated research result cursors. All state lives in Jobs SQLite.
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use okx_protocol::analytics::ResearchResultView;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    version: u8,
    pub job: String,
    pub root: String,
    pub leaf: String,
    pub view: ResearchResultView,
    pub offset: usize,
    pub instrument: Option<u64>,
}

fn mac(key: &[u8], payload: &[u8]) -> Result<[u8; 32], String> {
    if key.len() != 32 {
        return Err("RESEARCH_CURSOR_KEY_INVALID".into());
    }
    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for (i, byte) in key.iter().enumerate() {
        inner_pad[i] ^= byte;
        outer_pad[i] ^= byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(payload);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    Ok(outer.finalize().into())
}

impl Cursor {
    pub fn new(
        job: &str,
        root: &str,
        leaf: &str,
        view: ResearchResultView,
        offset: usize,
        instrument: Option<u64>,
    ) -> Self {
        Self {
            version: 1,
            job: job.into(),
            root: root.into(),
            leaf: leaf.into(),
            view,
            offset,
            instrument,
        }
    }
    pub fn encode(&self, key: &[u8]) -> Result<String, String> {
        let payload = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        let signature = mac(key, &payload)?;
        Ok(format!(
            "r1.{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature),
        ))
    }
    pub fn decode(raw: &str, key: &[u8]) -> Result<Self, String> {
        if raw.len() > 800 {
            return Err("INVALID_CURSOR".into());
        }
        let parts = raw.split('.').collect::<Vec<_>>();
        if parts.len() != 3 || parts[0] != "r1" {
            return Err("INVALID_CURSOR".into());
        }
        let payload = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|_| "INVALID_CURSOR")?;
        let provided = URL_SAFE_NO_PAD.decode(parts[2]).map_err(|_| "INVALID_CURSOR")?;
        let expected = mac(key, &payload)?;
        if provided.len() != expected.len() {
            return Err("INVALID_CURSOR".into());
        }
        let difference = provided
            .iter()
            .zip(expected)
            .fold(0u8, |diff, (left, right)| diff | (*left ^ right));
        if difference != 0 {
            return Err("INVALID_CURSOR".into());
        }
        let cursor: Self = serde_json::from_slice(&payload).map_err(|_| "INVALID_CURSOR")?;
        if cursor.version != 1 || cursor.offset == 0 {
            return Err("INVALID_CURSOR".into());
        }
        Ok(cursor)
    }
    pub fn matches(
        &self,
        job: &str,
        root: &str,
        leaf: &str,
        view: ResearchResultView,
    ) -> Result<(), String> {
        if self.job != job || self.root != root || self.leaf != leaf || self.view != view {
            return Err("CURSOR_MISMATCH".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn c17_authenticates_cursor_and_rejects_tampering_or_cross_view() {
        let key = [17u8; 32];
        let cursor = Cursor::new("job", "root", "leaf", ResearchResultView::Trades, 50, None);
        let text = cursor.encode(&key).unwrap();
        let parsed = Cursor::decode(&text, &key).unwrap();
        assert_eq!(parsed.offset, 50);
        parsed.matches("job", "root", "leaf", ResearchResultView::Trades).unwrap();
        assert_eq!(
            parsed.matches("job", "root", "leaf", ResearchResultView::Equity).unwrap_err(),
            "CURSOR_MISMATCH"
        );
        assert_eq!(Cursor::decode(&text, &[18u8; 32]).unwrap_err(), "INVALID_CURSOR");
        let mut forged = text.into_bytes();
        let last = forged.len() - 1;
        forged[last] = if forged[last] == b'A' { b'B' } else { b'A' };
        assert_eq!(
            Cursor::decode(std::str::from_utf8(&forged).unwrap(), &key).unwrap_err(),
            "INVALID_CURSOR"
        );
    }
}
