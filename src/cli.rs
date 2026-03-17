use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::model::{
    AccountKind, CategoryKind, ExportKind, MarketRefreshKind, PlanningGoalKind, RecurringCadence,
    TransactionKind, Weekday,
};

#[derive(Debug, Parser)]
#[command(name = "helius", version, about = "Personal finance tracker CLI")]
pub struct Cli {
    #[arg(long, global = true)]
    pub db: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Shell,
    Init(InitArgs),
    Account {
        #[command(subcommand)]
        command: AccountCommand,
    },
    Category {
        #[command(subcommand)]
        command: CategoryCommand,
    },
    Tx {
        #[command(subcommand)]
        command: TransactionCommand,
    },
    Balance(BalanceArgs),
    Summary {
        #[command(subcommand)]
        command: SummaryCommand,
    },
    Export {
        #[command(subcommand)]
        command: ExportCommand,
    },
    Import {
        #[command(subcommand)]
        command: ImportCommand,
    },
    Budget {
        #[command(subcommand)]
        command: BudgetCommand,
    },
    Forecast {
        #[command(subcommand)]
        command: ForecastCommand,
    },
    Plan {
        #[command(subcommand)]
        command: PlanCommand,
    },
    Scenario {
        #[command(subcommand)]
        command: ScenarioCommand,
    },
    Goal {
        #[command(subcommand)]
        command: GoalCommand,
    },
    Reconcile {
        #[command(subcommand)]
        command: ReconcileCommand,
    },
    Recurring {
        #[command(subcommand)]
        command: RecurringCommand,
    },
    Watchlist {
        #[command(subcommand)]
        command: WatchlistCommand,
    },
    Market {
        #[command(subcommand)]
        command: MarketCommand,
    },
    Poly {
        #[command(subcommand)]
        command: PolyCommand,
    },
}

#[derive(Debug, Args)]
pub struct InitArgs {
    #[arg(long)]
    pub currency: String,
}

#[derive(Debug, Subcommand)]
pub enum AccountCommand {
    Add(AccountAddArgs),
    Edit(AccountEditArgs),
    Delete(AccountDeleteArgs),
    List(AccountListArgs),
}

#[derive(Debug, Args)]
pub struct AccountAddArgs {
    pub name: String,
    #[arg(long = "type", value_enum)]
    pub kind: AccountKind,
    #[arg(long)]
    pub opening_balance: Option<String>,
    #[arg(long)]
    pub opened_on: Option<String>,
}

#[derive(Debug, Args)]
pub struct AccountEditArgs {
    pub account: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long = "type", value_enum)]
    pub kind: Option<AccountKind>,
    #[arg(long)]
    pub opening_balance: Option<String>,
    #[arg(long)]
    pub opened_on: Option<String>,
}

#[derive(Debug, Args)]
pub struct AccountDeleteArgs {
    pub account: String,
}

#[derive(Debug, Args)]
pub struct AccountListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum CategoryCommand {
    Add(CategoryAddArgs),
    Edit(CategoryEditArgs),
    Delete(CategoryDeleteArgs),
    List(CategoryListArgs),
}

#[derive(Debug, Args)]
pub struct CategoryAddArgs {
    pub name: String,
    #[arg(long, value_enum)]
    pub kind: CategoryKind,
}

#[derive(Debug, Args)]
pub struct CategoryEditArgs {
    pub category: String,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long, value_enum)]
    pub kind: Option<CategoryKind>,
}

#[derive(Debug, Args)]
pub struct CategoryDeleteArgs {
    pub category: String,
}

#[derive(Debug, Args)]
pub struct CategoryListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum TransactionCommand {
    Add(TransactionAddArgs),
    Edit(TransactionEditArgs),
    Delete(TransactionDeleteArgs),
    Restore(TransactionRestoreArgs),
    List(TransactionListArgs),
}

#[derive(Debug, Args)]
pub struct TransactionAddArgs {
    #[arg(long = "type", value_enum)]
    pub kind: TransactionKind,
    #[arg(long)]
    pub amount: String,
    #[arg(long)]
    pub date: String,
    #[arg(long)]
    pub account: String,
    #[arg(long)]
    pub to_account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
}

#[derive(Debug, Args)]
pub struct TransactionEditArgs {
    pub id: i64,
    #[arg(long = "type", value_enum)]
    pub kind: Option<TransactionKind>,
    #[arg(long)]
    pub amount: Option<String>,
    #[arg(long)]
    pub date: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub to_account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long)]
    pub clear_to_account: bool,
    #[arg(long)]
    pub clear_category: bool,
    #[arg(long)]
    pub clear_payee: bool,
    #[arg(long)]
    pub clear_note: bool,
}

#[derive(Debug, Args)]
pub struct TransactionDeleteArgs {
    pub id: i64,
}

#[derive(Debug, Args)]
pub struct TransactionRestoreArgs {
    pub id: i64,
}

#[derive(Debug, Args)]
pub struct TransactionListArgs {
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub search: Option<String>,
    #[arg(long)]
    pub limit: Option<usize>,
    #[arg(long)]
    pub include_deleted: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct BalanceArgs {
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum SummaryCommand {
    Month(SummaryMonthArgs),
    Range(SummaryRangeArgs),
}

#[derive(Debug, Args)]
pub struct SummaryMonthArgs {
    pub month: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SummaryRangeArgs {
    #[arg(long)]
    pub from: String,
    #[arg(long)]
    pub to: String,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum ExportCommand {
    Csv(ExportCsvArgs),
}

#[derive(Debug, Args)]
pub struct ExportCsvArgs {
    #[arg(long, value_enum)]
    pub kind: ExportKind,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long)]
    pub month: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum ImportCommand {
    Csv(ImportCsvArgs),
}

#[derive(Debug, Args)]
pub struct ImportCsvArgs {
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub account: String,
    #[arg(long)]
    pub date_column: String,
    #[arg(long)]
    pub amount_column: String,
    #[arg(long)]
    pub description_column: String,
    #[arg(long)]
    pub category_column: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee_column: Option<String>,
    #[arg(long)]
    pub note_column: Option<String>,
    #[arg(long)]
    pub type_column: Option<String>,
    #[arg(long, value_enum)]
    pub default_type: Option<TransactionKind>,
    #[arg(long, default_value = "%Y-%m-%d")]
    pub date_format: String,
    #[arg(long, default_value = ",")]
    pub delimiter: char,
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub allow_duplicates: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum BudgetCommand {
    Set(BudgetSetArgs),
    Delete(BudgetDeleteArgs),
    List(BudgetListArgs),
    Status(BudgetStatusArgs),
}

#[derive(Debug, Args)]
pub struct BudgetSetArgs {
    pub category: String,
    #[arg(long)]
    pub month: String,
    #[arg(long)]
    pub amount: String,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub scenario: Option<String>,
}

#[derive(Debug, Args)]
pub struct BudgetDeleteArgs {
    pub category: String,
    #[arg(long)]
    pub month: String,
    #[arg(long)]
    pub scenario: Option<String>,
}

#[derive(Debug, Args)]
pub struct BudgetListArgs {
    #[arg(long)]
    pub month: Option<String>,
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct BudgetStatusArgs {
    pub month: Option<String>,
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum ForecastCommand {
    Show(ForecastShowArgs),
    Bills(ForecastBillsArgs),
}

#[derive(Debug, Args)]
pub struct ForecastShowArgs {
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long, default_value_t = 90)]
    pub days: usize,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ForecastBillsArgs {
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long, default_value_t = 30)]
    pub days: usize,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum PlanCommand {
    Item {
        #[command(subcommand)]
        command: PlanItemCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum PlanItemCommand {
    Add(PlanItemAddArgs),
    Edit(PlanItemEditArgs),
    List(PlanItemListArgs),
    Delete(PlanItemIdArgs),
    Post(PlanItemIdArgs),
}

#[derive(Debug, Args)]
pub struct PlanItemAddArgs {
    pub title: String,
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long = "type", value_enum)]
    pub kind: TransactionKind,
    #[arg(long)]
    pub amount: String,
    #[arg(long = "date")]
    pub due_on: String,
    #[arg(long)]
    pub account: String,
    #[arg(long)]
    pub to_account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
}

#[derive(Debug, Args)]
pub struct PlanItemEditArgs {
    pub id: i64,
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long)]
    pub title: Option<String>,
    #[arg(long = "type", value_enum)]
    pub kind: Option<TransactionKind>,
    #[arg(long)]
    pub amount: Option<String>,
    #[arg(long = "date")]
    pub due_on: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub to_account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long)]
    pub clear_scenario: bool,
    #[arg(long)]
    pub clear_to_account: bool,
    #[arg(long)]
    pub clear_category: bool,
    #[arg(long)]
    pub clear_payee: bool,
    #[arg(long)]
    pub clear_note: bool,
}

#[derive(Debug, Args)]
pub struct PlanItemListArgs {
    #[arg(long)]
    pub scenario: Option<String>,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PlanItemIdArgs {
    pub id: i64,
}

#[derive(Debug, Subcommand)]
pub enum ScenarioCommand {
    Add(ScenarioAddArgs),
    List(ScenarioListArgs),
    Edit(ScenarioEditArgs),
    Delete(ScenarioDeleteArgs),
}

#[derive(Debug, Args)]
pub struct ScenarioAddArgs {
    pub name: String,
    #[arg(long)]
    pub note: Option<String>,
}

#[derive(Debug, Args)]
pub struct ScenarioListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ScenarioEditArgs {
    pub id: i64,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long)]
    pub clear_note: bool,
}

#[derive(Debug, Args)]
pub struct ScenarioDeleteArgs {
    pub id: i64,
}

#[derive(Debug, Subcommand)]
pub enum GoalCommand {
    Add(GoalAddArgs),
    List(GoalListArgs),
    Edit(GoalEditArgs),
    Delete(GoalDeleteArgs),
}

#[derive(Debug, Args)]
pub struct GoalAddArgs {
    pub name: String,
    #[arg(long, value_enum)]
    pub kind: PlanningGoalKind,
    #[arg(long)]
    pub account: String,
    #[arg(long)]
    pub target_amount: Option<String>,
    #[arg(long)]
    pub minimum_balance: Option<String>,
    #[arg(long)]
    pub due_on: Option<String>,
}

#[derive(Debug, Args)]
pub struct GoalListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct GoalEditArgs {
    pub id: i64,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long, value_enum)]
    pub kind: Option<PlanningGoalKind>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub target_amount: Option<String>,
    #[arg(long)]
    pub minimum_balance: Option<String>,
    #[arg(long)]
    pub due_on: Option<String>,
    #[arg(long)]
    pub clear_target_amount: bool,
    #[arg(long)]
    pub clear_minimum_balance: bool,
    #[arg(long)]
    pub clear_due_on: bool,
}

#[derive(Debug, Args)]
pub struct GoalDeleteArgs {
    pub id: i64,
}

#[derive(Debug, Subcommand)]
pub enum ReconcileCommand {
    Start(ReconcileStartArgs),
    List(ReconcileListArgs),
    Delete(ReconcileDeleteArgs),
}

#[derive(Debug, Args)]
pub struct ReconcileStartArgs {
    #[arg(long)]
    pub account: String,
    #[arg(long = "to")]
    pub statement_ending_on: String,
    #[arg(long = "statement-balance")]
    pub statement_balance: String,
    #[arg(long = "transaction-id")]
    pub transaction_ids: Vec<i64>,
}

#[derive(Debug, Args)]
pub struct ReconcileListArgs {
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReconcileDeleteArgs {
    pub id: i64,
}

#[derive(Debug, Subcommand)]
pub enum RecurringCommand {
    Add(RecurringAddArgs),
    Edit(RecurringEditArgs),
    List(RecurringListArgs),
    Pause(RecurringIdArgs),
    Resume(RecurringIdArgs),
    Delete(RecurringIdArgs),
    Run(RecurringRunArgs),
}

#[derive(Debug, Args)]
pub struct RecurringAddArgs {
    pub name: String,
    #[arg(long = "type", value_enum)]
    pub kind: TransactionKind,
    #[arg(long)]
    pub amount: String,
    #[arg(long)]
    pub account: String,
    #[arg(long)]
    pub to_account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long, value_enum)]
    pub cadence: RecurringCadence,
    #[arg(long, default_value_t = 1)]
    pub interval: i64,
    #[arg(long)]
    pub day_of_month: Option<u32>,
    #[arg(long, value_enum)]
    pub weekday: Option<Weekday>,
    #[arg(long)]
    pub start_on: String,
    #[arg(long)]
    pub next_due_on: Option<String>,
    #[arg(long)]
    pub end_on: Option<String>,
}

#[derive(Debug, Args)]
pub struct RecurringEditArgs {
    pub id: i64,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long = "type", value_enum)]
    pub kind: Option<TransactionKind>,
    #[arg(long)]
    pub amount: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub to_account: Option<String>,
    #[arg(long)]
    pub category: Option<String>,
    #[arg(long)]
    pub payee: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long, value_enum)]
    pub cadence: Option<RecurringCadence>,
    #[arg(long)]
    pub interval: Option<i64>,
    #[arg(long)]
    pub day_of_month: Option<u32>,
    #[arg(long, value_enum)]
    pub weekday: Option<Weekday>,
    #[arg(long)]
    pub start_on: Option<String>,
    #[arg(long)]
    pub next_due_on: Option<String>,
    #[arg(long)]
    pub end_on: Option<String>,
    #[arg(long)]
    pub clear_to_account: bool,
    #[arg(long)]
    pub clear_category: bool,
    #[arg(long)]
    pub clear_payee: bool,
    #[arg(long)]
    pub clear_note: bool,
    #[arg(long)]
    pub clear_day_of_month: bool,
    #[arg(long)]
    pub clear_weekday: bool,
    #[arg(long)]
    pub clear_next_due_on: bool,
    #[arg(long)]
    pub clear_end_on: bool,
}

#[derive(Debug, Args)]
pub struct RecurringListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RecurringIdArgs {
    pub id: i64,
}

#[derive(Debug, Args)]
pub struct RecurringRunArgs {
    #[arg(long)]
    pub through: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum WatchlistCommand {
    Add(WatchlistAddArgs),
    List(WatchlistListArgs),
    Remove(WatchlistRemoveArgs),
    Refresh(WatchlistRefreshArgs),
}

#[derive(Debug, Args)]
pub struct WatchlistAddArgs {
    pub ticker: String,
    #[arg(long)]
    pub label: Option<String>,
}

#[derive(Debug, Args)]
pub struct WatchlistListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct WatchlistRemoveArgs {
    pub ticker: String,
}

#[derive(Debug, Args)]
pub struct WatchlistRefreshArgs {
    pub ticker: Option<String>,
    #[arg(long)]
    pub all: bool,
    #[arg(long, value_enum, default_value = "both")]
    pub kind: MarketRefreshKind,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum MarketCommand {
    Quote(MarketQuoteArgs),
    Brief(MarketBriefArgs),
    News(MarketNewsArgs),
    AutoRefresh(MarketAutoRefreshArgs),
    Settings {
        #[command(subcommand)]
        command: MarketSettingsCommand,
    },
}

#[derive(Debug, Args)]
pub struct MarketQuoteArgs {
    pub ticker: String,
    #[arg(long)]
    pub cached_only: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct MarketBriefArgs {
    pub ticker: Option<String>,
    #[arg(long, default_value_t = 6)]
    pub limit: usize,
    #[arg(long)]
    pub cached_only: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct MarketNewsArgs {
    pub ticker: Option<String>,
    #[arg(long, default_value_t = 6)]
    pub limit: usize,
    #[arg(long)]
    pub cached_only: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct MarketAutoRefreshArgs {
    #[arg(long, value_enum, default_value = "both")]
    pub kind: MarketRefreshKind,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum MarketSettingsCommand {
    Show(MarketSettingsShowArgs),
    Set(MarketSettingsSetArgs),
}

#[derive(Debug, Args)]
pub struct MarketSettingsShowArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct MarketSettingsSetArgs {
    #[arg(long)]
    pub quote_hours: Option<i64>,
    #[arg(long)]
    pub news_hours: Option<i64>,
    #[arg(long)]
    pub auto_quotes: Option<bool>,
    #[arg(long)]
    pub auto_news: Option<bool>,
    #[arg(long)]
    pub weekday_only: Option<bool>,
    #[arg(long)]
    pub max_quote_cost: Option<f64>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum PolyCommand {
    Search(PolySearchArgs),
    Market(PolyMarketArgs),
    Brief(PolyBriefArgs),
    Movers(PolyMoversArgs),
    Watch {
        #[command(subcommand)]
        command: PolyWatchCommand,
    },
}

#[derive(Debug, Args)]
pub struct PolySearchArgs {
    pub query: String,
    #[arg(long, default_value_t = 8)]
    pub limit: usize,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PolyMarketArgs {
    pub slug: String,
    #[arg(long)]
    pub cached_only: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PolyBriefArgs {
    pub slug: Option<String>,
    #[arg(long)]
    pub cached_only: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PolyMoversArgs {
    #[arg(long, default_value_t = 8)]
    pub limit: usize,
    #[arg(long)]
    pub cached_only: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum PolyWatchCommand {
    Add(PolyWatchAddArgs),
    List(PolyWatchListArgs),
    Remove(PolyWatchRemoveArgs),
    Refresh(PolyWatchRefreshArgs),
}

#[derive(Debug, Args)]
pub struct PolyWatchAddArgs {
    pub slug: String,
    #[arg(long)]
    pub label: Option<String>,
}

#[derive(Debug, Args)]
pub struct PolyWatchListArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct PolyWatchRemoveArgs {
    pub slug: String,
}

#[derive(Debug, Args)]
pub struct PolyWatchRefreshArgs {
    pub slug: Option<String>,
    #[arg(long)]
    pub all: bool,
    #[arg(long)]
    pub json: bool,
}
