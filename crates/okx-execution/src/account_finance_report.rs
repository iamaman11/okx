//! One typed portfolio read: health, ledger revision and data share a SQL snapshot.
//! Reporting does not authorize execution or mutate account/risk state.
use super::*;
#[derive(Debug, Clone)]
pub enum FinanceReportView {
    Summary,
    History {
        after_time: i64,
        after_id: String,
        limit: usize,
    },
}
#[derive(Debug, Clone)]
pub struct FinanceReportQuery {
    pub from: i64,
    pub through: i64,
    pub instances: Vec<String>,
    pub currencies: Vec<String>,
    pub view: FinanceReportView,
}
#[derive(Debug, Clone)]
pub struct FinanceReportSnapshot {
    pub entries: Vec<FinanceEntry>,
    pub attribution: FinanceAttribution,
    pub financial_risk: FinancialRisk,
    pub nav: Vec<NavWindow>,
    pub initialized_at: i64,
    pub revision: i64,
}
impl AccountStore {
    pub fn finance_report(
        &mut self,
        query: &FinanceReportQuery,
    ) -> StoreResult<FinanceReportSnapshot> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let report = Self::finance_report_sql(&tx, None, query)?;
        tx.commit()?;
        Ok(report)
    }
    pub(crate) fn finance_report_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        query: &FinanceReportQuery,
    ) -> StoreResult<FinanceReportSnapshot> {
        if query.from > query.through {
            return Err(StoreError::Invalid("NAV_RANGE_FILTER_INVALID"));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|v| i64::try_from(v.as_millis()).ok())
            .ok_or(StoreError::Invalid("ACCOUNT_REPORT_CLOCK_UNAVAILABLE"))?;
        let health = Self::financial_risk_sql(db, scope, now, None)
            .map_err(|e| e.with_interruption_stage("ACCOUNT_SQL_PORTFOLIO_RISK_INTERRUPTED"))?;
        let (entries, attribution, nav) = match &query.view {
            FinanceReportView::History {
                after_time,
                after_id,
                limit,
            } => (
                Self::finance_history_filtered_sql(
                    db,
                    scope,
                    query.from,
                    query.through,
                    *after_time,
                    after_id,
                    *limit,
                    &query.instances,
                    &query.currencies,
                )
                .map_err(|e| {
                    e.with_interruption_stage("ACCOUNT_SQL_PORTFOLIO_HISTORY_INTERRUPTED")
                })?,
                Default::default(),
                Vec::new(),
            ),
            FinanceReportView::Summary => (
                Vec::new(),
                Self::finance_attribution_filtered_sql(
                    db,
                    scope,
                    query.from,
                    query.through,
                    &query.instances,
                    &query.currencies,
                )
                .map_err(|e| {
                    e.with_interruption_stage("ACCOUNT_SQL_PORTFOLIO_ATTRIBUTION_INTERRUPTED")
                })?,
                Self::nav_summary_sql(
                    db,
                    scope,
                    query.from,
                    query.through,
                    &query.instances,
                    &query.currencies,
                )
                .map_err(|e| e.with_interruption_stage("ACCOUNT_SQL_PORTFOLIO_NAV_INTERRUPTED"))?,
            ),
        };
        let metadata = Self::source_capture_state_sql(db, scope)?;
        Ok(FinanceReportSnapshot {
            entries,
            attribution,
            financial_risk: health,
            nav,
            initialized_at: metadata.initialized_at,
            revision: Self::finance_rowid_sql(db, scope)?,
        })
    }
}
