use std::path::PathBuf;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum AccountKind {
    Cash,
    Checking,
    Savings,
    Credit,
}

impl AccountKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Cash => "cash",
            Self::Checking => "checking",
            Self::Savings => "savings",
            Self::Credit => "credit",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "cash" => Ok(Self::Cash),
            "checking" => Ok(Self::Checking),
            "savings" => Ok(Self::Savings),
            "credit" => Ok(Self::Credit),
            _ => Err(AppError::Config(format!(
                "unsupported account kind in database: {value}"
            ))),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum CategoryKind {
    Income,
    Expense,
}

impl CategoryKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Income => "income",
            Self::Expense => "expense",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "income" => Ok(Self::Income),
            "expense" => Ok(Self::Expense),
            _ => Err(AppError::Config(format!(
                "unsupported category kind in database: {value}"
            ))),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TransactionKind {
    Income,
    Expense,
    Transfer,
}

impl TransactionKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Income => "income",
            Self::Expense => "expense",
            Self::Transfer => "transfer",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "income" => Ok(Self::Income),
            "expense" => Ok(Self::Expense),
            "transfer" => Ok(Self::Transfer),
            _ => Err(AppError::Config(format!(
                "unsupported transaction kind in database: {value}"
            ))),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    Transactions,
    Summary,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ImportKind {
    Csv,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum RecurringCadence {
    Weekly,
    Monthly,
}

impl RecurringCadence {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "weekly" => Ok(Self::Weekly),
            "monthly" => Ok(Self::Monthly),
            _ => Err(AppError::Config(format!(
                "unsupported recurring cadence in database: {value}"
            ))),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum PlanningGoalKind {
    SinkingFund,
    BalanceTarget,
}

impl PlanningGoalKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::SinkingFund => "sinking_fund",
            Self::BalanceTarget => "balance_target",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "sinking_fund" => Ok(Self::SinkingFund),
            "balance_target" => Ok(Self::BalanceTarget),
            _ => Err(AppError::Config(format!(
                "unsupported planning goal kind in database: {value}"
            ))),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Weekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl Weekday {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Mon => "mon",
            Self::Tue => "tue",
            Self::Wed => "wed",
            Self::Thu => "thu",
            Self::Fri => "fri",
            Self::Sat => "sat",
            Self::Sun => "sun",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "mon" => Ok(Self::Mon),
            "tue" => Ok(Self::Tue),
            "wed" => Ok(Self::Wed),
            "thu" => Ok(Self::Thu),
            "fri" => Ok(Self::Fri),
            "sat" => Ok(Self::Sat),
            "sun" => Ok(Self::Sun),
            _ => Err(AppError::Config(format!(
                "unsupported recurring weekday in database: {value}"
            ))),
        }
    }

    pub fn short_label(&self) -> &'static str {
        match self {
            Self::Mon => "Mon",
            Self::Tue => "Tue",
            Self::Wed => "Wed",
            Self::Thu => "Thu",
            Self::Fri => "Fri",
            Self::Sat => "Sat",
            Self::Sun => "Sun",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccurrenceStatus {
    Pending,
    Posted,
    Skipped,
}

impl OccurrenceStatus {
    pub fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "pending" => Ok(Self::Pending),
            "posted" => Ok(Self::Posted),
            "skipped" => Ok(Self::Skipped),
            _ => Err(AppError::Config(format!(
                "unsupported recurring occurrence status in database: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Account {
    pub id: i64,
    pub name: String,
    pub kind: AccountKind,
    pub opening_balance_cents: i64,
    pub opened_on: String,
    pub archived: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Category {
    pub id: i64,
    pub name: String,
    pub kind: CategoryKind,
    pub archived: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TransactionRecord {
    pub id: i64,
    pub txn_date: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account_id: i64,
    pub account_name: String,
    pub to_account_id: Option<i64>,
    pub to_account_name: Option<String>,
    pub category_id: Option<i64>,
    pub category_name: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
    pub reconciliation_id: Option<i64>,
    pub recurring_rule_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BalanceRecord {
    pub account_id: i64,
    pub account_name: String,
    pub account_kind: AccountKind,
    pub current_balance_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SummaryRecord {
    pub from: String,
    pub to: String,
    pub account_id: Option<i64>,
    pub account_name: Option<String>,
    pub transaction_count: i64,
    pub income_cents: i64,
    pub expense_cents: i64,
    pub net_cents: i64,
    pub transfer_in_cents: i64,
    pub transfer_out_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReconciliationRecord {
    pub id: i64,
    pub account_id: i64,
    pub account_name: String,
    pub statement_ending_on: String,
    pub statement_balance_cents: i64,
    pub cleared_balance_cents: i64,
    pub transaction_count: i64,
    pub created_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecurringRuleRecord {
    pub id: i64,
    pub name: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account_id: i64,
    pub account_name: String,
    pub to_account_id: Option<i64>,
    pub to_account_name: Option<String>,
    pub category_id: Option<i64>,
    pub category_name: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub cadence: RecurringCadence,
    pub interval: i64,
    pub day_of_month: Option<u32>,
    pub weekday: Option<Weekday>,
    pub start_on: String,
    pub end_on: Option<String>,
    pub next_due_on: String,
    pub paused: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecurringOccurrenceRecord {
    pub id: i64,
    pub rule_id: i64,
    pub rule_name: String,
    pub due_on: String,
    pub transaction_id: Option<i64>,
    pub status: OccurrenceStatus,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account_name: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BudgetRecord {
    pub id: i64,
    pub month: String,
    pub category_id: i64,
    pub category_name: String,
    pub scenario_id: Option<i64>,
    pub scenario_name: Option<String>,
    pub is_override: bool,
    pub account_id: Option<i64>,
    pub account_name: Option<String>,
    pub amount_cents: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BudgetStatusRecord {
    pub month: String,
    pub category_id: i64,
    pub category_name: String,
    pub scenario_id: Option<i64>,
    pub scenario_name: Option<String>,
    pub is_override: bool,
    pub account_id: Option<i64>,
    pub account_name: Option<String>,
    pub budget_cents: i64,
    pub spent_cents: i64,
    pub remaining_cents: i64,
    pub over_budget: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlanningItemRecord {
    pub id: i64,
    pub scenario_id: Option<i64>,
    pub scenario_name: Option<String>,
    pub title: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account_id: i64,
    pub account_name: String,
    pub to_account_id: Option<i64>,
    pub to_account_name: Option<String>,
    pub category_id: Option<i64>,
    pub category_name: Option<String>,
    pub due_on: String,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub linked_transaction_id: Option<i64>,
    pub archived: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlanningScenarioRecord {
    pub id: i64,
    pub name: String,
    pub note: Option<String>,
    pub archived: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PlanningGoalRecord {
    pub id: i64,
    pub name: String,
    pub kind: PlanningGoalKind,
    pub account_id: i64,
    pub account_name: String,
    pub target_amount_cents: Option<i64>,
    pub minimum_balance_cents: Option<i64>,
    pub due_on: Option<String>,
    pub archived: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ForecastSelection {
    pub id: Option<i64>,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ForecastDailyPoint {
    pub date: String,
    pub opening_balance_cents: i64,
    pub inflow_cents: i64,
    pub outflow_cents: i64,
    pub net_cents: i64,
    pub closing_balance_cents: i64,
    pub alerts: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ForecastMonthlyPoint {
    pub month: String,
    pub inflow_cents: i64,
    pub outflow_cents: i64,
    pub net_cents: i64,
    pub ending_balance_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BillCalendarItem {
    pub date: String,
    pub title: String,
    pub source: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account_id: i64,
    pub account_name: String,
    pub category_name: Option<String>,
    pub scenario_name: Option<String>,
    pub linked_transaction_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GoalStatusRecord {
    pub id: i64,
    pub name: String,
    pub kind: PlanningGoalKind,
    pub account_id: i64,
    pub account_name: String,
    pub target_amount_cents: Option<i64>,
    pub minimum_balance_cents: Option<i64>,
    pub due_on: Option<String>,
    pub current_balance_cents: i64,
    pub projected_balance_cents: i64,
    pub remaining_cents: i64,
    pub suggested_monthly_contribution_cents: i64,
    pub on_track: bool,
    pub breach_date: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ForecastSnapshot {
    pub scenario: ForecastSelection,
    pub as_of: String,
    pub account: ForecastSelection,
    pub warnings: Vec<String>,
    pub alerts: Vec<String>,
    pub daily: Vec<ForecastDailyPoint>,
    pub monthly: Vec<ForecastMonthlyPoint>,
    pub goal_status: Vec<GoalStatusRecord>,
    pub bill_calendar: Vec<BillCalendarItem>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MonthlyCashFlowPoint {
    pub month: String,
    pub income_cents: i64,
    pub expense_cents: i64,
    pub net_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CategorySpendingPoint {
    pub category_id: i64,
    pub category_name: String,
    pub spent_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BalanceTrendPoint {
    pub month: String,
    pub balance_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WeeklyBalancePoint {
    pub week_start: String,
    pub opening_balance_cents: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ImportedTransactionRow {
    pub line_number: usize,
    pub txn_date: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account_name: String,
    pub category_name: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CsvImportResult {
    pub dry_run: bool,
    pub imported_count: usize,
    pub duplicate_count: usize,
    pub preview: Vec<ImportedTransactionRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionFilters {
    pub from: Option<String>,
    pub to: Option<String>,
    pub account: Option<String>,
    pub category: Option<String>,
    pub search: Option<String>,
    pub limit: Option<usize>,
    pub include_deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewTransaction {
    pub txn_date: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account: String,
    pub to_account: Option<String>,
    pub category: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub recurring_rule_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateTransaction {
    pub id: i64,
    pub txn_date: Option<String>,
    pub kind: Option<TransactionKind>,
    pub amount_cents: Option<i64>,
    pub account: Option<String>,
    pub to_account: Option<String>,
    pub category: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub clear_to_account: bool,
    pub clear_category: bool,
    pub clear_payee: bool,
    pub clear_note: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewRecurringRule {
    pub name: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account: String,
    pub to_account: Option<String>,
    pub category: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub cadence: RecurringCadence,
    pub interval: i64,
    pub day_of_month: Option<u32>,
    pub weekday: Option<Weekday>,
    pub start_on: String,
    pub next_due_on: Option<String>,
    pub end_on: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewPlanningItem {
    pub scenario: Option<String>,
    pub title: String,
    pub kind: TransactionKind,
    pub amount_cents: i64,
    pub account: String,
    pub to_account: Option<String>,
    pub category: Option<String>,
    pub due_on: String,
    pub payee: Option<String>,
    pub note: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatePlanningItem {
    pub id: i64,
    pub scenario: Option<String>,
    pub title: Option<String>,
    pub kind: Option<TransactionKind>,
    pub amount_cents: Option<i64>,
    pub account: Option<String>,
    pub to_account: Option<String>,
    pub category: Option<String>,
    pub due_on: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub clear_scenario: bool,
    pub clear_to_account: bool,
    pub clear_category: bool,
    pub clear_payee: bool,
    pub clear_note: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewPlanningScenario {
    pub name: String,
    pub note: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatePlanningScenario {
    pub id: i64,
    pub name: Option<String>,
    pub note: Option<String>,
    pub clear_note: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewPlanningGoal {
    pub name: String,
    pub kind: PlanningGoalKind,
    pub account: String,
    pub target_amount_cents: Option<i64>,
    pub minimum_balance_cents: Option<i64>,
    pub due_on: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatePlanningGoal {
    pub id: i64,
    pub name: Option<String>,
    pub kind: Option<PlanningGoalKind>,
    pub account: Option<String>,
    pub target_amount_cents: Option<i64>,
    pub minimum_balance_cents: Option<i64>,
    pub due_on: Option<String>,
    pub clear_target_amount: bool,
    pub clear_minimum_balance: bool,
    pub clear_due_on: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateRecurringRule {
    pub id: i64,
    pub name: Option<String>,
    pub kind: Option<TransactionKind>,
    pub amount_cents: Option<i64>,
    pub account: Option<String>,
    pub to_account: Option<String>,
    pub category: Option<String>,
    pub payee: Option<String>,
    pub note: Option<String>,
    pub cadence: Option<RecurringCadence>,
    pub interval: Option<i64>,
    pub day_of_month: Option<u32>,
    pub weekday: Option<Weekday>,
    pub start_on: Option<String>,
    pub next_due_on: Option<String>,
    pub end_on: Option<String>,
    pub clear_to_account: bool,
    pub clear_category: bool,
    pub clear_payee: bool,
    pub clear_note: bool,
    pub clear_day_of_month: bool,
    pub clear_weekday: bool,
    pub clear_next_due_on: bool,
    pub clear_end_on: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CsvImportPlan {
    pub path: PathBuf,
    pub account: String,
    pub date_column: String,
    pub amount_column: String,
    pub description_column: String,
    pub category_column: Option<String>,
    pub category: Option<String>,
    pub payee_column: Option<String>,
    pub note_column: Option<String>,
    pub type_column: Option<String>,
    pub default_kind: Option<TransactionKind>,
    pub date_format: String,
    pub delimiter: u8,
    pub dry_run: bool,
    pub allow_duplicates: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchlistItem {
    pub id: i64,
    pub ticker: String,
    pub label: Option<String>,
    pub added_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketQuote {
    pub ticker: String,
    pub name: String,
    pub exchange: Option<String>,
    pub currency: Option<String>,
    pub last_price: f64,
    pub change: Option<f64>,
    pub change_percent: Option<f64>,
    pub market_state: Option<String>,
    pub fetched_at: String,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketNewsItem {
    pub title: String,
    pub source: String,
    pub url: String,
    pub published_at: Option<String>,
    pub summary: String,
    pub ticker: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketNewsFeed {
    pub ticker: String,
    pub fetched_at: String,
    pub stale: bool,
    pub items: Vec<MarketNewsItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketBrief {
    pub ticker: String,
    pub summary: String,
    pub tone: String,
    pub why_it_matters: Vec<String>,
    pub watch_items: Vec<String>,
    pub headline_count: usize,
    pub source_count: usize,
    pub fetched_at: String,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WatchlistEntry {
    pub id: i64,
    pub ticker: String,
    pub label: Option<String>,
    pub added_at: String,
    pub quote: Option<MarketQuote>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum MarketRefreshKind {
    Quotes,
    News,
    Both,
}

impl MarketRefreshKind {
    pub fn includes_quotes(self) -> bool {
        matches!(self, Self::Quotes | Self::Both)
    }

    pub fn includes_news(self) -> bool {
        matches!(self, Self::News | Self::Both)
    }

    pub fn as_label(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::News => "news",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketSettings {
    pub quote_refresh_hours: i64,
    pub news_refresh_hours: i64,
    pub auto_refresh_quotes: bool,
    pub auto_refresh_news: bool,
    pub weekday_only: bool,
    pub max_quote_cost_usd: f64,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketRefreshSummary {
    pub requested_kind: MarketRefreshKind,
    pub tickers_considered: usize,
    pub quote_refreshed: usize,
    pub news_refreshed: usize,
    pub quote_skipped_not_due: usize,
    pub news_skipped_not_due: usize,
    pub quote_skipped_disabled: usize,
    pub news_skipped_disabled: usize,
    pub quote_skipped_cost_cap: usize,
    pub weekend_blocked: bool,
    pub failure_count: usize,
    pub estimated_cost_usd: f64,
    pub ran_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolyWatchItem {
    pub id: i64,
    pub slug: String,
    pub label: Option<String>,
    pub added_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolyMarket {
    pub slug: String,
    pub question: String,
    pub subtitle: Option<String>,
    pub event_title: Option<String>,
    pub end_date: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub liquidity: Option<f64>,
    pub volume_24h: Option<f64>,
    pub volume_total: Option<f64>,
    pub yes_price: Option<f64>,
    pub no_price: Option<f64>,
    pub fetched_at: String,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolySearchResult {
    pub slug: String,
    pub question: String,
    pub event_title: Option<String>,
    pub end_date: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub yes_price: Option<f64>,
    pub no_price: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolyWatchEntry {
    pub id: i64,
    pub slug: String,
    pub label: Option<String>,
    pub added_at: String,
    pub market: Option<PolyMarket>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolyBrief {
    pub slug: String,
    pub summary: String,
    pub tone: String,
    pub why_it_matters: Vec<String>,
    pub watch_items: Vec<String>,
    pub fetched_at: String,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolyRefreshSummary {
    pub slugs_considered: usize,
    pub markets_refreshed: usize,
    pub movers_refreshed: bool,
    pub failure_count: usize,
    pub ran_at: String,
}
