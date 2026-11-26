use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;

/// The source identity compiled into the small, executable entry point.
///
/// The service library deliberately does not use `env!`/`option_env!` for
/// per-PR source attestation: changing only the accepted source tree must not
/// force unrelated Research library code generation. The actual executable
/// bytes remain bound by the SHA-256 calculated below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompiledSource {
    head: Option<&'static str>,
    tree: Option<&'static str>,
}

impl CompiledSource {
    pub const fn new(head: Option<&'static str>, tree: Option<&'static str>) -> Self {
        Self { head, tree }
    }
}

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
    pub fn current_with_source(source: CompiledSource) -> Result<Self, String> {
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
        let head = source.head.filter(|s| !s.is_empty()).map(str::to_owned);
        let tree = source.tree.filter(|s| !s.is_empty()).map(str::to_owned);
        Self::new(format!("{:x}", digest.finalize()), head, tree).map_err(str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::{CompiledSource, Producer};

    #[test]
    fn explicit_compiled_source_binds_actual_executable_and_exact_tree() {
        let producer = Producer::current_with_source(CompiledSource::new(
            Some("aabbccddee0011223344556677889900aabbccdd"),
            Some("00112233445566778899aabbccddeeff00112233"),
        ))
        .unwrap();
        assert_eq!(producer.binary_sha256().len(), 64);
        assert_eq!(
            producer.source_head.as_deref(),
            Some("aabbccddee0011223344556677889900aabbccdd")
        );
        assert_eq!(
            producer.source_tree.as_deref(),
            Some("00112233445566778899aabbccddeeff00112233")
        );
    }

    #[test]
    fn invalid_explicit_compiled_source_is_rejected_not_silently_rewritten() {
        assert_eq!(
            Producer::current_with_source(CompiledSource::new(Some("bad"), None)).unwrap_err(),
            "INVALID_WORKER_PRODUCER"
        );
    }
}
