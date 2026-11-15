//! Parsed venue fill facts. Ownership and fee identity are bound by the SQL owner.
use super::*;
#[derive(Debug, Clone)]
pub struct CapturedFill {
    pub fill: Fill,
    pub client_order_id: String,
    pub bill_id: String,
    pub instrument_type: String,
    pub sub_type: String,
    pub position_side: String,
}
#[derive(Debug, Clone)]
pub struct CapturedFillPage {
    pub partition: String,
    pub cursor: SourceCursor,
    pub rows: Vec<CapturedFill>,
}
impl AccountStore {
    pub fn ingest_captured_fills(
        &mut self,
        input: &CapturedFillPage,
        fx: &UsdConversionEvidence,
    ) -> StoreResult<()> {
        let page = Self::bind_captured_fills_sql(&self.connection, None, input)?;
        self.ingest_page_with_usd(&page, fx)
    }
    pub(crate) fn bind_captured_fills_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        input: &CapturedFillPage,
    ) -> StoreResult<IngestPage> {
        if input.rows.len() > 100 {
            return Err(StoreError::Invalid("FILL_HISTORY_PAGE_TOO_LARGE"));
        }
        if !matches!(
            input.partition.as_str(),
            "SPOT" | "MARGIN" | "SWAP" | "FUTURES" | "OPTION"
        ) {
            return Err(StoreError::Invalid("FILL_HISTORY_PARTITION_INVALID"));
        }
        if input.cursor.source != "fills" || input.cursor.stream_partition != input.partition {
            return Err(StoreError::Invalid("SOURCE_CAPTURE_CURSOR_MISMATCH"));
        }
        let initialized_at = Self::source_capture_state_sql(db, scope)?.initialized_at;
        let mut page = IngestPage {
            cursor: Some(input.cursor.clone()),
            ..Default::default()
        };
        for row in &input.rows {
            let fill = &row.fill;
            if fill.time < initialized_at {
                continue;
            }
            if fill.intent_id.is_some() {
                return Err(StoreError::Invalid("CAPTURE_FILL_OWNER_MUST_BE_RESOLVED"));
            }
            if fill.currency.is_empty() {
                return Err(StoreError::Invalid("FILL_FEE_CURRENCY_UNAVAILABLE"));
            }
            let id = format!(
                "{}:{}:{}",
                fill.instrument_id, fill.trade_id, fill.exchange_order_id
            );
            if fill.fill_id != id {
                return Err(StoreError::Invalid("CAPTURE_FILL_IDENTITY_INVALID"));
            }
            let owner = Self::source_order_owner_sql(
                db,
                scope,
                &row.client_order_id,
                &fill.exchange_order_id,
            )?;
            let key = if row.bill_id.is_empty() {
                format!("fill:{id}")
            } else {
                format!("bill:{}", row.bill_id)
            };
            if fill.fee != Decimal::ZERO && !(input.partition == "SWAP" && row.bill_id.is_empty()) {
                page.finance.push(FinanceEntry {
                    entry_id: format!("{key}:fee"),
                    economic_event_key: key,
                    component: "FEE".into(),
                    allocation_leg: "account".into(),
                    amount: fill.fee,
                    currency: fill.currency.clone(),
                    instance_id: owner.as_ref().map(|i| i.plan.strategy_instance.clone()),
                    source_ref: format!("fill:{id}"),
                    time: fill.time,
                    reverses_entry_id: None,
                });
            }
            if input.partition == "SWAP" {
                if row.instrument_type != "SWAP" {
                    return Err(StoreError::Invalid("SWAP_FILL_SOURCE_TYPE_MISMATCH"));
                }
                let direction = swap_fill_direction(&row.sub_type, &fill.side, &row.position_side)?;
                page.swap_fill_sources.push(SwapFillSource {
                    fill_id: id,
                    bill_id: row.bill_id.clone(),
                    sub_type: row.sub_type.clone(),
                    position_side: row.position_side.clone(),
                    direction: direction.into(),
                });
            }
            let mut bound = fill.clone();
            bound.intent_id = owner.map(|i| i.intent_id);
            page.fills.push(bound);
        }
        Ok(page)
    }
}
