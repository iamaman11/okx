//! Research-only Windows Credential Manager identity. Never reuse the Engine
//! credential target, even when both components run under the same OS user.
#[cfg(windows)]
use keyring::api::CredentialStoreApi;
use zeroize::Zeroizing;

pub const RESEARCH_CLOUDFLARE_SERVICE: &str = "iamaman11.okx-research-service.cloudflare";
pub const RESEARCH_CLOUDFLARE_USER: &str = "runtime-token";

pub fn valid_token(token: &[u8]) -> bool {
    (32..=1024).contains(&token.len()) && token.iter().all(u8::is_ascii_graphic)
}

/// Native one-shot provisioning. The caller supplies bytes through stdin and
/// zeroizes them after this function returns; nothing is logged or echoed.
pub fn store(token: &[u8]) -> Result<(), String> {
    if !valid_token(token) {
        return Err("RESEARCH_EDGE_TOKEN_INVALID".into());
    }
    #[cfg(windows)]
    {
        entry()?
            .set_secret(token)
            .map_err(|_| "RESEARCH_EDGE_CREDENTIAL_STORE_FAILED".to_owned())
    }
    #[cfg(not(windows))]
    {
        let _ = token;
        Err("RESEARCH_EDGE_CREDENTIAL_PLATFORM_UNSUPPORTED".into())
    }
}

/// Windows runtime reads only its dedicated installed secret; it never
/// inherits Engine bearer state or accepts a token through command-line args.
pub fn load() -> Result<Zeroizing<String>, String> {
    #[cfg(windows)]
    {
        let secret = Zeroizing::new(match entry()?.get_secret() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => {
                return Err("RESEARCH_EDGE_CREDENTIAL_NOT_FOUND".into());
            }
            Err(_) => return Err("RESEARCH_EDGE_CREDENTIAL_READ_FAILED".into()),
        });
        if !valid_token(&secret) {
            return Err("RESEARCH_EDGE_TOKEN_INVALID".into());
        }
        let token = std::str::from_utf8(&secret)
            .map_err(|_| "RESEARCH_EDGE_TOKEN_INVALID")?
            .to_owned();
        Ok(Zeroizing::new(token))
    }
    #[cfg(not(windows))]
    {
        Err("RESEARCH_EDGE_CREDENTIAL_PLATFORM_UNSUPPORTED".into())
    }
}

#[cfg(windows)]
fn entry() -> Result<keyring::Entry, String> {
    let store = windows_native_keyring_store::Store::new()
        .map_err(|_| "RESEARCH_EDGE_CREDENTIAL_STORE_UNAVAILABLE")?;
    entry_from_store(store.as_ref())
}

#[cfg(windows)]
fn entry_from_store(store: &dyn CredentialStoreApi) -> Result<keyring::Entry, String> {
    store
        .build(RESEARCH_CLOUDFLARE_SERVICE, RESEARCH_CLOUDFLARE_USER, None)
        .map_err(|_| "RESEARCH_EDGE_CREDENTIAL_STORE_UNAVAILABLE".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_token_validation_is_bounded_and_ascii_only() {
        assert!(valid_token(&[b'A'; 32]));
        assert!(valid_token(&[b'F'; 1024]));
        let mut newline = vec![b'x'; 32];
        newline.push(b'\n');
        let mut zero = vec![b'x'; 32];
        zero.push(0);
        let mut invalid_utf8 = vec![b'x'; 32];
        invalid_utf8.push(0xc0);
        for invalid in [
            vec![],
            vec![b'x'; 31],
            vec![b'x'; 1025],
            newline,
            zero,
            invalid_utf8,
        ] {
            assert!(!valid_token(&invalid));
        }
        assert_ne!(
            RESEARCH_CLOUDFLARE_SERVICE,
            "iamaman11.okx-agent.cloudflare"
        );
    }

    #[cfg(windows)]
    #[test]
    fn research_native_target_is_distinct_from_engine() {
        let native = windows_native_keyring_store::Store::new().unwrap();
        let entry = entry_from_store(native.as_ref()).unwrap();
        let target = format!("{entry:?}");
        assert!(target.contains("runtime-token.iamaman11.okx-research-service.cloudflare"));
        assert!(!target.contains("runtime-token.iamaman11.okx-agent.cloudflare"));
    }

    #[cfg(windows)]
    #[test]
    fn mock_store_never_exposes_research_bearer_to_engine_target() {
        let store = keyring::mock::Store::new().unwrap();
        let research = entry_from_store(store.as_ref()).unwrap();
        research
            .set_secret(b"research-only-secret-value-0123456789")
            .unwrap();
        assert_eq!(
            entry_from_store(store.as_ref())
                .unwrap()
                .get_secret()
                .unwrap(),
            b"research-only-secret-value-0123456789"
        );
        let engine = store
            .build("iamaman11.okx-agent.cloudflare", "runtime-token", None)
            .unwrap();
        assert!(matches!(engine.get_secret(), Err(keyring::Error::NoEntry)));
    }
}
