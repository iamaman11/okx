use super::super::*;
impl AccountStore {
    pub fn cursor(&self, source: &str, partition: &str) -> StoreResult<Option<SourceCursor>> {
        Ok(self.connection.query_row("SELECT cursor,watermark,generation,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2",params![source,partition],|r|Ok(SourceCursor{source:source.into(),stream_partition:partition.into(),cursor:r.get(0)?,watermark:r.get(1)?,generation:r.get(2)?,coverage_gaps:r.get(3)?})).optional()?)
    }
}
