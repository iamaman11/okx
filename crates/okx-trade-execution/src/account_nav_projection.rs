//! Account NAV arithmetic shared by the collector and installation SQL owner.
//! These values describe a captured snapshot; they never grant execution rights.
use super::*;
#[derive(Debug, Clone)]
pub struct AccountNavSnapshot {
    pub as_of: i64,
    pub generation: i64,
    pub source_version: String,
    pub source_ref: String,
    pub equity_usd: Decimal,
    pub balances: Vec<(String, Decimal)>,
    pub complete: bool,
}

/// The same production arithmetic without I/O, suitable for direct tests.
pub fn build_account_nav_page(
    input: &AccountNavSnapshot,
    flows: &std::collections::BTreeMap<(String, String), Decimal>,
    opening_usd: Decimal,
    external_usd: Decimal,
) -> StoreResult<IngestPage> {
    if input.as_of <= 0 || input.source_ref.is_empty() || input.source_version.is_empty() {
        return Err(StoreError::Invalid("ACCOUNT_NAV_SOURCE_INVALID"));
    }
    let coverage = if input.complete {
        "COMPLETE"
    } else {
        "PARTIAL"
    };
    let point = |scope_id,
                 equity: Decimal,
                 opening: Decimal,
                 external: Decimal,
                 currency|
     -> StoreResult<NavPoint> {
        Ok(NavPoint {
            scope_id,
            time: input.as_of,
            source_version: input.source_version.clone(),
            equity,
            pnl: equity
                .checked_sub(opening)
                .and_then(|v| v.checked_sub(external))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            flows: external,
            currency,
            coverage: coverage.into(),
            mark_source: input.source_ref.clone(),
        })
    };
    let mut page = IngestPage::default();
    page.nav.push(point(
        "account".into(),
        input.equity_usd,
        opening_usd,
        external_usd,
        "USD".into(),
    )?);
    let mut currencies = std::collections::BTreeSet::new();
    for (currency, equity) in &input.balances {
        if currency.is_empty() || !currencies.insert(currency) {
            return Err(StoreError::Invalid("ACCOUNT_NAV_CURRENCY_INVALID"));
        }
        let opening = *flows
            .get(&(currency.clone(), "OPENING".into()))
            .unwrap_or(&Decimal::ZERO);
        let external = flows
            .iter()
            .filter(|((c, k), _)| c == currency && k != "OPENING")
            .try_fold(Decimal::ZERO, |a, (_, v)| {
                a.checked_add(*v)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
            })?;
        page.nav.push(point(
            format!("account:{currency}"),
            *equity,
            opening,
            external,
            currency.clone(),
        )?);
    }
    page.cursor = Some(SourceCursor {
        source: "account_snapshot".into(),
        stream_partition: "account".into(),
        cursor: String::new(),
        watermark: input.as_of,
        generation: input.generation,
        coverage_gaps: if input.complete {
            String::new()
        } else {
            "FINANCIAL_BACKFILL_PENDING".into()
        },
    });
    Ok(page)
}
