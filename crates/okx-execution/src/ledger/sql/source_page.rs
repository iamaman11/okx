//! One financial-history page in the caller's original transaction.
//! Unsupported projections fail BEFORE writes; no supplied field is ignored.
use super::super::*;
impl AccountStore {
    pub(crate) fn ingest_unified_source_page_sql(
        tx: &rusqlite::Transaction<'_>,
        installation: &str,
        account: &str,
        page: &IngestPage,
    ) -> StoreResult<()> {
        Self::verify_source_account_sql(tx, installation, account)?;
        // These components still require their original scoped projection
        // paths. Never advance a page containing a silently dropped fact.
        if !page.entry_order_fee_links.is_empty() {
            return Err(StoreError::Invalid(
                "UNIFIED_SOURCE_PAGE_COMPONENT_NOT_PORTED",
            ));
        }
        if page
            .cursor
            .as_ref()
            .is_some_and(|c| c.source == "finance_provenance")
        {
            return Err(StoreError::Invalid("FINANCE_PROVENANCE_CURSOR_INTERNAL"));
        }
        let scope = Some((installation, account));
        let previous = Self::finance_rowid_sql(tx, scope)?;
        Self::record_source_fills_sql(tx, scope, &page.fills)?;
        Self::record_swap_sources_sql(tx, scope, &page.swap_fill_sources)?;
        Self::record_source_bills_sql(tx, scope, &page.bills, &page.bill_order_sources)?;
        Self::record_funding_sources_sql(tx, scope, &page.funding_events)?;
        Self::record_source_flows_sql(tx, scope, &page.cash_flows, &page.allocations)?;
        let added_loss = Self::record_finance_entries_sql(tx, scope, &page.finance)?;
        Self::link_page_swap_fees_sql(tx, scope, page)?;
        if added_loss > Decimal::ZERO {
            let old:String=tx.query_row("SELECT campaign_loss FROM account_policy WHERE installation_id=?1 AND account_key=?2 AND singleton=1",params![installation,account],|r|r.get(0))?;
            let total = decimal(old)?
                .checked_add(added_loss)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            tx.execute("UPDATE account_policy SET campaign_loss=?1 WHERE installation_id=?2 AND account_key=?3 AND singleton=1",params![exact(total),installation,account])?;
        }
        Self::record_position_lots_sql(tx, scope, &page.lots)?;
        Self::record_nav_points_sql(tx, scope, &page.nav)?;
        if let Some(cursor) = &page.cursor {
            Self::upsert_source_cursor_sql(tx, scope, cursor)?;
        }
        Self::acknowledge_projected_fills_sql(tx, scope, &page.projected_fill_ids)?;
        Self::acknowledge_funding_sources_sql(tx, scope, &page.attributed_funding_ids)?;
        Self::advance_finance_provenance_sql(tx, scope, previous)?;
        Ok(())
    }
}
