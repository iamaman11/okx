use super::super::*;
impl AccountStore {
    pub fn cursor(&self, source: &str, partition: &str) -> StoreResult<Option<SourceCursor>> {
        Ok(self.connection.query_row("SELECT cursor,watermark,generation,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2",params![source,partition],|r|Ok(SourceCursor{source:source.into(),stream_partition:partition.into(),cursor:r.get(0)?,watermark:r.get(1)?,generation:r.get(2)?,coverage_gaps:r.get(3)?})).optional()?)
    }

    /// A WebSocket connection starts at generation zero after each process restart.
    /// Its persistent source generation must resume AFTER all durable SQL cursors,
    /// without resetting any watermark or weakening cursor-regression enforcement.
    pub fn source_generation_base(&self) -> StoreResult<u64> {
        let generation: i64 = self.connection.query_row(
            "SELECT COALESCE(MAX(generation),0) FROM source_cursors",
            [],
            |r| r.get(0),
        )?;
        if generation < 0 || generation == i64::MAX {
            return Err(StoreError::Invalid("SOURCE_GENERATION_INVALID"));
        }
        Ok(generation as u64)
    }
}
