use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;

/// Identity of the actual executable running this worker. Source values are
/// optional build-time attestations; the executable digest is authoritative.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "UncheckedProducer")]
pub struct Producer {
    binary_sha256: String,
    source_head: Option<String>,
    source_tree: Option<String>,
}
#[derive(Deserialize)]
struct UncheckedProducer {
    binary_sha256: String,
    source_head: Option<String>,
    source_tree: Option<String>,
}
impl TryFrom<UncheckedProducer> for Producer {
    type Error = &'static str;
    fn try_from(value: UncheckedProducer) -> Result<Self, Self::Error> {
        Self::new(value.binary_sha256, value.source_head, value.source_tree)
    }
}
impl Producer {
    pub fn binary_sha256(&self) -> &str {
        &self.binary_sha256
    }
    pub fn new(
        binary_sha256: String,
        source_head: Option<String>,
        source_tree: Option<String>,
    ) -> Result<Self, &'static str> {
        fn valid(s: &str, n: usize) -> bool {
            s.len() == n
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }
        if !valid(&binary_sha256, 64)
            || source_head.as_deref().is_some_and(|v| !valid(v, 40))
            || source_tree.as_deref().is_some_and(|v| !valid(v, 40))
        {
            return Err("INVALID_WORKER_PRODUCER");
        }
        Ok(Self {
            binary_sha256,
            source_head,
            source_tree,
        })
    }
    pub fn current() -> Result<Self, String> {
        let mut file = std::fs::File::open(std::env::current_exe().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            digest.update(&buffer[..n]);
        }
        let head = option_env!("OKX_BUILD_SOURCE_HEAD")
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let tree = option_env!("OKX_BUILD_SOURCE_TREE")
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        Self::new(format!("{:x}", digest.finalize()), head, tree).map_err(str::to_owned)
    }
}
