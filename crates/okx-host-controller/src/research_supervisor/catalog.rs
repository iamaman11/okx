//! Select the single catalog CONTROL may pass to the Research process.
use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::{is_reparse, valid_sha, validate_state_ancestors};
use crate::{HostControlError, HostControlResult};

pub(super) const CATALOG_DB: &str = "research-catalog.sqlite";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum CatalogBinding {
    HostScoped,
    ExistingScoped { scope_sha256: String },
}

pub(super) fn independent_root(runtime: &Path) -> PathBuf {
    runtime.join("research").join("edge-catalog")
}

pub(super) fn existing_root(runtime: &Path) -> PathBuf {
    runtime.join("analytics")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatalogOnDisk {
    Empty,
    Independent,
    Existing,
}

impl CatalogOnDisk {
    pub(super) fn inspect(runtime: &Path) -> HostControlResult<Self> {
        match (
            catalog_present(&independent_root(runtime))?,
            catalog_present(&existing_root(runtime))?,
        ) {
            (true, true) => Err(HostControlError::ResearchCatalogConflict),
            (true, false) => Ok(Self::Independent),
            (false, true) => Ok(Self::Existing),
            (false, false) => Ok(Self::Empty),
        }
    }

    pub(super) fn select(
        self,
        previous: Option<&CatalogBinding>,
        has_host_scope: bool,
        expected_scope: Option<&str>,
    ) -> HostControlResult<CatalogBinding> {
        use CatalogBinding::{ExistingScoped, HostScoped};
        use CatalogOnDisk::{Empty, Existing, Independent};

        match (previous, expected_scope, self) {
            (Some(HostScoped), Some(_), _)
            | (Some(HostScoped), None, Existing)
            | (Some(ExistingScoped { .. }), _, Independent) => {
                Err(HostControlError::ResearchCatalogConflict)
            }
            (_, Some(scope), Existing) if valid_sha(scope) => Ok(ExistingScoped {
                scope_sha256: scope.to_owned(),
            }),
            (Some(binding @ ExistingScoped { .. }), None, Existing) => Ok(binding.clone()),
            (Some(ExistingScoped { .. }), _, Empty) | (_, Some(_), Empty) => {
                Err(HostControlError::ResearchCatalogMissing)
            }
            (Some(HostScoped), None, _) | (None, None, Empty) => Ok(HostScoped),
            (None, None, Independent) if has_host_scope => Ok(HostScoped),
            (_, _, Independent) => Err(HostControlError::ResearchCatalogConflict),
            _ => Err(HostControlError::ResearchCatalogScopeRequired),
        }
    }
}

fn catalog_entry(path: &Path, directory: bool) -> HostControlResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(m) if !is_reparse(&m) && if directory { m.is_dir() } else { m.is_file() } => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Ok(_) => Err(HostControlError::ResearchCatalogConflict),
        Err(error) => Err(error.into()),
    }
}

fn catalog_present(root: &Path) -> HostControlResult<bool> {
    validate_state_ancestors(root, true).map_err(|_| HostControlError::ResearchCatalogConflict)?;
    let present = catalog_entry(&root.join(CATALOG_DB), false)?;
    for name in [
        "research-catalog.sqlite-wal",
        "research-catalog.sqlite-shm",
        "research-catalog.sqlite-journal",
    ] {
        if catalog_entry(&root.join(name), false)? && !present {
            return Err(HostControlError::ResearchCatalogConflict);
        }
    }
    for name in ["evidence", "source-cache"] {
        if catalog_entry(&root.join(name), true)? && !present {
            return Err(HostControlError::ResearchCatalogConflict);
        }
    }
    catalog_entry(&root.join("research-worker.lock"), false)?;
    Ok(present)
}
