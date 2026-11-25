//! Temporary adapter from Engine captures to the Research source protocol.

use super::MarketBootstrapper;
use jiff::Timestamp;
use okx_api::CapturedHttpDelivery;

fn source_delivery(
    receipt: CapturedHttpDelivery,
) -> okx_ipc_contracts::research_ipc::PublicSourceHttpDelivery {
    okx_ipc_contracts::research_ipc::PublicSourceHttpDelivery {
        transport_send_invocations: receipt.transport_send_invocations,
        response_status: receipt.response_status,
        response_body_bytes: receipt.response_body_bytes,
    }
}

impl MarketBootstrapper {
    /// Bounded public source operations; the host retains the same rate budget.
    /// No paths, credentials, financial queries or arbitrary HTTP requests.
    pub async fn research_source(
        &self,
        request: okx_ipc_contracts::research_ipc::PublicSourceRequest,
    ) -> Result<okx_ipc_contracts::research_ipc::PublicSourceReply, String> {
        use okx_ipc_contracts::research_ipc::*;
        if !request.valid() {
            return Err("INVALID_RESEARCH_SOURCE_REQUEST".into());
        }
        match request {
            PublicSourceRequest::CatalogPage {
                route,
                instrument,
                after,
                before,
                limit,
            } => {
                let value = self
                    .api
                    .research_catalog_page(
                        &route,
                        &instrument,
                        after.as_deref(),
                        before.as_deref(),
                        limit,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                // JSON encodes raw Vec<u8> as numbers and also includes parsed
                // rows. Keep enough headroom for both copies in the IPC frame.
                if value.raw_body.len() > okx_ipc_contracts::research_ipc::MAX_FRAME_BYTES / 8 {
                    return Err("RESEARCH_CATALOG_PAGE_TOO_LARGE".into());
                }
                let reply = PublicSourceReply::CatalogPage(PublicCatalogCapture {
                    route,
                    raw_body: value.raw_body,
                    rows: value.rows,
                    acquired_at_ms: Timestamp::now().as_millisecond().max(1).to_string(),
                    http_delivery: source_delivery(value.http_delivery),
                });
                // Leave room for the surrounding HostFrame and request identity.
                let encoded = serde_json::to_vec(&reply).map_err(|e| e.to_string())?;
                if encoded.len() >= okx_ipc_contracts::research_ipc::MAX_FRAME_BYTES - 4096 {
                    return Err("RESEARCH_CATALOG_FRAME_TOO_LARGE".into());
                }
                Ok(reply)
            }
            PublicSourceRequest::Candles {
                instrument,
                bar,
                after,
                before,
                limit,
                queued,
            } => {
                let value = self
                    .capture_research_candles(
                        &instrument,
                        &bar,
                        after.as_deref(),
                        before.as_deref(),
                        limit,
                        queued,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(PublicSourceReply::Candles(PublicCandleCapture {
                    raw_body: value.raw_body,
                    rows: value
                        .rows
                        .into_iter()
                        .map(|row| HistoryCandle {
                            open_time_ms: row.open_time_ms,
                            open: row.open,
                            high: row.high,
                            low: row.low,
                            close: row.close,
                            volume: row.volume,
                            volume_currency: row.volume_currency,
                            volume_quote: row.volume_quote,
                            confirmed: row.confirmed,
                        })
                        .collect(),
                    acquired_at_ms: value.acquired_at_ms,
                    http_delivery: Some(source_delivery(value.http_delivery)),
                }))
            }
            PublicSourceRequest::Funding {
                instrument,
                after,
                before,
                limit,
            } => {
                let value = self
                    .research_funding_page(&instrument, after.as_deref(), before.as_deref(), limit)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(PublicSourceReply::Funding(PublicFundingCapture {
                    raw_body: value.raw_body,
                    rows: value
                        .rows
                        .into_iter()
                        .map(|row| FundingHistoryEvent {
                            funding_time_ms: row.funding_time_ms,
                            funding_rate: row.funding_rate,
                            realized_rate: row.realized_rate,
                            formula_type: row.formula_type,
                            method: row.method,
                        })
                        .collect(),
                    acquired_at_ms: value.acquired_at_ms,
                    http_delivery: Some(source_delivery(value.http_delivery)),
                }))
            }
            PublicSourceRequest::Reference { instrument } => {
                let value = self
                    .research_current_reference(&instrument)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(PublicSourceReply::Reference(PublicReferenceCapture {
                    raw_body: value.raw_body,
                    instrument: PublicReferenceInstrument {
                        contract_type: value.instrument.contract_type,
                        contract_value: value.instrument.contract_value,
                        contract_value_currency: value.instrument.contract_value_currency,
                        settle_currency: value.instrument.settle_currency,
                    },
                    reference_generation: value.reference_generation,
                    acquired_at_ms: value.acquired_at_ms,
                    http_delivery: Some(source_delivery(value.http_delivery)),
                }))
            }
            PublicSourceRequest::Ticker { instrument } => {
                let value = self
                    .analytics_ticker(&instrument)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(PublicSourceReply::Ticker(ResearchTicker {
                    bid_price: value.bid_price,
                    ask_price: value.ask_price,
                }))
            }
            PublicSourceRequest::OpenInterest { instrument, period } => {
                let value = self
                    .analytics_oi(&instrument, &period)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(PublicSourceReply::OpenInterest(
                    value
                        .into_iter()
                        .map(|v| ResearchOpenInterest {
                            oi: v.oi,
                            oi_currency: v.oi_currency,
                            ts: v.ts,
                        })
                        .collect(),
                ))
            }
        }
    }
}
