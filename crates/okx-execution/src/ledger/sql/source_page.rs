//! One financial-history page in the caller's original transaction.
//! All IngestPage fields are handled in the same caller-owned transaction.
use super::super::*;
impl AccountStore {
    pub(crate) fn ingest_unified_source_page_sql(
        tx: &rusqlite::Transaction<'_>,
        installation: &str,
        account: &str,
        page: &IngestPage,
    ) -> StoreResult<()> {
        // No wildcard: adding an IngestPage component must require an
        // explicit ingestion decision here instead of silently dropping it.
        let IngestPage {
            funding_events,
            attributed_funding_ids,
            fills,
            swap_fill_sources,
            bills,
            bill_order_sources,
            entry_order_fee_links,
            finance,
            cash_flows,
            allocations,
            lots,
            nav,
            projected_fill_ids,
            cursor,
        } = page;
        Self::verify_source_account_sql(tx, installation, account)?;
        if cursor
            .as_ref()
            .is_some_and(|c| c.source == "finance_provenance")
        {
            return Err(StoreError::Invalid("FINANCE_PROVENANCE_CURSOR_INTERNAL"));
        }
        let scope = Some((installation, account));
        let previous = Self::finance_rowid_sql(tx, scope)?;
        Self::record_source_fills_sql(tx, scope, fills)?;
        Self::record_swap_sources_sql(tx, scope, swap_fill_sources)?;
        Self::record_source_bills_sql(tx, scope, bills, bill_order_sources)?;
        Self::record_funding_sources_sql(tx, scope, funding_events)?;
        Self::record_source_flows_sql(tx, scope, cash_flows, allocations)?;
        let added_loss = Self::record_finance_entries_sql(tx, scope, finance)?;
        Self::link_page_swap_fees_sql(tx, scope, page)?;
        if added_loss > Decimal::ZERO {
            let old:String=tx.query_row("SELECT campaign_loss FROM account_policy WHERE installation_id=?1 AND account_key=?2 AND singleton=1",params![installation,account],|r|r.get(0))?;
            let total = decimal(old)?
                .checked_add(added_loss)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            tx.execute("UPDATE account_policy SET campaign_loss=?1 WHERE installation_id=?2 AND account_key=?3 AND singleton=1",params![exact(total),installation,account])?;
        }
        Self::record_entry_fee_links_sql(tx, scope, entry_order_fee_links)?;
        Self::record_position_lots_sql(tx, scope, lots)?;
        Self::record_nav_points_sql(tx, scope, nav)?;
        if let Some(cursor) = cursor {
            Self::upsert_source_cursor_sql(tx, scope, cursor)?;
        }
        Self::acknowledge_projected_fills_sql(tx, scope, projected_fill_ids)?;
        Self::acknowledge_funding_sources_sql(tx, scope, attributed_funding_ids)?;
        Self::advance_finance_provenance_sql(tx, scope, previous)?;
        Ok(())
    }
}
