//! Windows Credential Manager entry construction, using the same native store
//! and default service/user mapping as keyring v1.
use keyring::api::CredentialStoreApi;

// Persistent Windows Credential Manager targets. These remain bound to the
// installed okx-agent identity even when the source Cargo crate is renamed.
// Changing them would hide existing identity keys and API tokens.
pub(crate) const IDENTITY_SERVICE: &str = "iamaman11.okx-agent";
pub(crate) const GITHUB_SERVICE: &str = "iamaman11.okx-agent.github";
pub(crate) const CLOUDFLARE_SERVICE: &str = "iamaman11.okx-agent.cloudflare";
pub(crate) const ACCOUNT_SERVICE_PREFIX: &str = "iamaman11.okx-agent.account.";

pub(crate) fn entry(service: &str, user: &str) -> keyring::Result<keyring::Entry> {
    let store = windows_native_keyring_store::Store::new()?;
    entry_from_store(store.as_ref(), service, user)
}

fn entry_from_store(
    store: &dyn CredentialStoreApi,
    service: &str,
    user: &str,
) -> keyring::Result<keyring::Entry> {
    store.build(service, user, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injected_store_preserves_entry_names_and_bytes() {
        let store = keyring::mock::Store::new().unwrap();
        let first = entry_from_store(
            store.as_ref(),
            GITHUB_SERVICE,
            "mailbox-token",
        )
        .unwrap();
        assert!(matches!(first.get_secret(), Err(keyring::Error::NoEntry)));
        first.set_secret(b"test-only-bytes\0").unwrap();
        let same = entry_from_store(
            store.as_ref(),
            GITHUB_SERVICE,
            "mailbox-token",
        )
        .unwrap();
        assert_eq!(same.get_secret().unwrap(), b"test-only-bytes\0");
        let different = entry_from_store(
            store.as_ref(),
            GITHUB_SERVICE,
            "other",
        )
        .unwrap();
        assert!(matches!(
            different.get_secret(),
            Err(keyring::Error::NoEntry)
        ));
    }

    #[test]
    fn native_store_constructs_original_target_without_accessing_credentials() {
        let store = windows_native_keyring_store::Store::new().unwrap();
        assert_eq!(store.delimiters, ["", ".", ""]);
        let account_service = format!("{ACCOUNT_SERVICE_PREFIX}example");
        assert_eq!(account_service, "iamaman11.okx-agent.account.example");
        for (service, user, target) in [
            (
                IDENTITY_SERVICE,
                "agent-key",
                "agent-key.iamaman11.okx-agent",
            ),
            (
                GITHUB_SERVICE,
                "mailbox-token",
                "mailbox-token.iamaman11.okx-agent.github",
            ),
            (
                CLOUDFLARE_SERVICE,
                "runtime-token",
                "runtime-token.iamaman11.okx-agent.cloudflare",
            ),
            (
                account_service.as_str(),
                "credentials",
                "credentials.iamaman11.okx-agent.account.example",
            ),
        ] {
            let built = entry_from_store(store.as_ref(), service, user).unwrap();
            // Native entry construction is local; Debug renders its computed target.
            assert!(format!("{built:?}").contains(target));
        }
    }
}
