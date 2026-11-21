//! The Research-owned durable signal journal. Commit precedes any IPC delivery.
//! The Engine alone decides whether an accepted signal changes risk or orders.
use okx_protocol::research_ipc::{
    GraphPublicationPage, GraphPublicationRequest, GraphSignalPublication,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// A full journal rejects new events instead of silently evicting unseen data.
/// Compaction requires a future explicit, durable consumer acknowledgement.
const MAX_EVENTS: i64 = 100_000;

#[derive(Clone)]
pub(super) struct GraphOutbox {
    db: Arc<Mutex<Connection>>,
}

impl GraphOutbox {
    pub(super) fn open(path: PathBuf) -> Result<Self, String> {
        Self::initialize(Connection::open(path).map_err(|e| e.to_string())?)
    }

    #[cfg(test)]
    pub(super) fn memory() -> Self {
        Self::initialize(Connection::open_in_memory().unwrap()).unwrap()
    }

    fn initialize(connection: Connection) -> Result<Self, String> {
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA busy_timeout=100;
             CREATE TABLE IF NOT EXISTS graph_outbox (
                 session_id TEXT NOT NULL,
                 graph_sha256 TEXT NOT NULL,
                 sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 100000),
                 event_json TEXT NOT NULL,
                 PRIMARY KEY(session_id,graph_sha256,sequence)
             ) STRICT, WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS graph_outbox_metadata (
                 name TEXT PRIMARY KEY,
                 value INTEGER NOT NULL CHECK(value BETWEEN 0 AND 100000)
             ) STRICT;
             INSERT OR IGNORE INTO graph_outbox_metadata(name,value) VALUES('count',0);",
            )
            .map_err(|e| e.to_string())?;
        Ok(Self {
            db: Arc::new(Mutex::new(connection)),
        })
    }

    /// Returns true only if this event was newly persisted. A crash after
    /// commit but before IPC delivery is recovered by replay, never re-emitted.
    pub(super) fn record(&self, event: &GraphSignalPublication) -> Result<bool, String> {
        if !event.valid() {
            return Err("GRAPH_OUTBOX_EVENT_INVALID".into());
        }
        let mut db = self.db.lock().map_err(|_| "GRAPH_OUTBOX_POISONED")?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT event_json FROM graph_outbox
             WHERE session_id=?1 AND graph_sha256=?2 AND sequence=?3",
                params![
                    event.session_id.as_str(),
                    event.graph_sha256,
                    i64::try_from(event.sequence).expect("validated sequence")
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if let Some(existing) = existing {
            let previous: GraphSignalPublication =
                serde_json::from_str(&existing).map_err(|_| "GRAPH_OUTBOX_CORRUPT_EVENT")?;
            return if previous == *event {
                Ok(false)
            } else {
                Err("GRAPH_OUTBOX_CONFLICTING_PUBLICATION".into())
            };
        }
        let count: i64 = tx
            .query_row(
                "SELECT value FROM graph_outbox_metadata WHERE name='count'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        if count >= MAX_EVENTS {
            return Err("GRAPH_OUTBOX_FULL".into());
        }
        let encoded = serde_json::to_string(event).map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT INTO graph_outbox(session_id,graph_sha256,sequence,event_json)
             VALUES(?1,?2,?3,?4)",
            params![
                event.session_id.as_str(),
                event.graph_sha256,
                i64::try_from(event.sequence).expect("validated sequence"),
                encoded
            ],
        )
        .map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE graph_outbox_metadata SET value=value+1 WHERE name='count'",
            [],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(true)
    }

    /// Used when a session is restored from a possibly stale historical prefix.
    /// An already committed sequence may never be rewritten with another close.
    pub(super) fn verify_restored(&self, event: &GraphSignalPublication) -> Result<(), String> {
        let db = self.db.lock().map_err(|_| "GRAPH_OUTBOX_POISONED")?;
        let existing: Option<String> = db
            .query_row(
                "SELECT event_json FROM graph_outbox
             WHERE session_id=?1 AND graph_sha256=?2 AND sequence=?3",
                params![
                    event.session_id.as_str(),
                    event.graph_sha256,
                    i64::try_from(event.sequence).expect("validated sequence")
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if let Some(existing) = existing {
            let previous: GraphSignalPublication =
                serde_json::from_str(&existing).map_err(|_| "GRAPH_OUTBOX_CORRUPT_EVENT")?;
            if previous != *event {
                return Err("GRAPH_OUTBOX_RESTORE_CONFLICT".into());
            }
        }
        Ok(())
    }

    /// Can run without a live in-memory graph session after a service restart.
    pub(super) fn page(
        &self,
        request: &GraphPublicationRequest,
    ) -> Result<GraphPublicationPage, String> {
        if !request.valid() {
            return Err("GRAPH_PUBLICATION_REQUEST_INVALID".into());
        }
        let db = self.db.lock().map_err(|_| "GRAPH_OUTBOX_POISONED")?;
        let mut statement = db
            .prepare(
                "SELECT event_json FROM graph_outbox
             WHERE session_id=?1 AND graph_sha256=?2 AND sequence>?3
             ORDER BY sequence LIMIT ?4",
            )
            .map_err(|e| e.to_string())?;
        let rows = statement
            .query_map(
                params![
                    request.session_id.as_str(),
                    request.graph_sha256,
                    i64::try_from(request.after_sequence).expect("validated cursor"),
                    i64::from(request.limit) + 1
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|e| e.to_string())?;
        let mut publications = Vec::with_capacity(request.limit as usize + 1);
        for row in rows {
            let data = row.map_err(|e| e.to_string())?;
            let event: GraphSignalPublication =
                serde_json::from_str(&data).map_err(|_| "GRAPH_OUTBOX_CORRUPT_EVENT")?;
            publications.push(event);
        }
        let has_more = publications.len() > request.limit as usize;
        publications.truncate(request.limit as usize);
        let page = GraphPublicationPage {
            session_id: request.session_id.clone(),
            graph_sha256: request.graph_sha256.clone(),
            after_sequence: request.after_sequence,
            publications,
            has_more,
        };
        if !page.matches_request(request) {
            return Err("GRAPH_OUTBOX_SEQUENCE_GAP_OR_CORRUPTION".into());
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::research_ipc::{
        GraphPosition, GraphSessionId, GraphSessionReply, GraphSignalIntent,
    };

    fn event(sequence: u64, closed_ms: u64) -> GraphSignalPublication {
        GraphSignalPublication {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id: GraphSessionId::new("research:durable:btc".into()).unwrap(),
            graph_sha256: "a".repeat(64),
            closed_ms,
            sequence,
            intent: GraphSignalIntent::SetTarget(GraphPosition::Flat),
        }
    }

    #[test]
    fn durable_replay_is_ordered_bounded_and_conflict_checked() {
        let root =
            std::env::temp_dir().join(format!("okx-graph-outbox-test-{}", std::process::id()));
        // This test owns a unique path in a new temporary directory.
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let root = root.with_extension(format!("{:032x}", u128::from_le_bytes(nonce)));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("signals.sqlite");
        let first = event(3, 180_000);
        let second = event(4, 240_000);
        {
            let outbox = GraphOutbox::open(path.clone()).unwrap();
            assert!(outbox.record(&first).unwrap());
            assert!(!outbox.record(&first).unwrap());
            assert!(outbox.record(&second).unwrap());
            let mut conflict = first.clone();
            conflict.intent = GraphSignalIntent::Hold;
            assert_eq!(
                outbox.record(&conflict).unwrap_err(),
                "GRAPH_OUTBOX_CONFLICTING_PUBLICATION"
            );
            assert_eq!(
                outbox.verify_restored(&conflict).unwrap_err(),
                "GRAPH_OUTBOX_RESTORE_CONFLICT"
            );
        }
        let outbox = GraphOutbox::open(path).unwrap();
        let mut request = GraphPublicationRequest {
            session_id: first.session_id.clone(),
            graph_sha256: first.graph_sha256.clone(),
            after_sequence: 2,
            limit: 1,
        };
        let page = outbox.page(&request).unwrap();
        assert!(page.matches_request(&request));
        assert_eq!(page.publications, vec![first.clone()]);
        assert!(page.has_more);
        request.after_sequence = 3;
        assert_eq!(outbox.page(&request).unwrap().publications, vec![second]);
        request.after_sequence = 1;
        assert_eq!(
            outbox.page(&request).unwrap_err(),
            "GRAPH_OUTBOX_SEQUENCE_GAP_OR_CORRUPTION"
        );
        request.limit = 65;
        assert_eq!(
            outbox.page(&request).unwrap_err(),
            "GRAPH_PUBLICATION_REQUEST_INVALID"
        );
        drop(outbox);
        std::fs::remove_dir_all(root).unwrap();
    }
}
