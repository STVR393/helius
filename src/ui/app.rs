use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::NaiveDate;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use crate::amount::{parse_amount_to_cents, parse_balance_to_cents};
use crate::db::Db;
use crate::error::AppError;
use crate::market::MarketService;
use crate::model::{
    Account, BalanceRecord, BalanceTrendPoint, BudgetStatusRecord, Category, CategorySpendingPoint,
    CsvImportPlan, CsvImportResult, ForecastSnapshot, MarketNewsFeed, MarketQuote,
    MarketRefreshKind, MonthlyCashFlowPoint, NewPlanningGoal, NewPlanningItem, NewPlanningScenario,
    NewRecurringRule, PlanningGoalKind, PlanningGoalRecord, PlanningItemRecord,
    PlanningScenarioRecord, PolyMarket, PolySearchResult, PolyWatchEntry, ReconciliationRecord,
    RecurringCadence, RecurringOccurrenceRecord, RecurringRuleRecord, SummaryRecord,
    TransactionFilters, TransactionKind, TransactionRecord, UpdatePlanningGoal, UpdatePlanningItem,
    UpdatePlanningScenario, UpdateRecurringRule, UpdateTransaction, WatchlistEntry, Weekday,
    WeeklyBalancePoint,
};
use crate::poly::PolyService;
use crate::today_iso;

use super::{clamp_index, clean_output, command_template, empty_summary, shift_index};

const MAX_COMMAND_LOG_LINES: usize = 18;

#[derive(Copy, Clone)]
pub(super) enum View {
    Dashboard,
    Transactions,
    Accounts,
    Categories,
    Summary,
    Budgets,
    Planning,
    #[allow(dead_code)]
    Markets,
    #[allow(dead_code)]
    Poly,
    Recurring,
    Reconcile,
}

impl View {
    pub(super) fn all() -> [View; 9] {
        [
            View::Dashboard,
            View::Transactions,
            View::Accounts,
            View::Categories,
            View::Summary,
            View::Budgets,
            View::Planning,
            View::Recurring,
            View::Reconcile,
        ]
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            View::Dashboard => "DASHBOARD",
            View::Transactions => "TRANSACTIONS",
            View::Accounts => "ACCOUNTS",
            View::Categories => "CATEGORIES",
            View::Summary => "SUMMARY",
            View::Budgets => "BUDGETS",
            View::Planning => "PLANNING",
            View::Markets => "MARKETS",
            View::Poly => "POLY",
            View::Recurring => "RECURRING",
            View::Reconcile => "RECONCILE",
        }
    }
}

#[derive(Copy, Clone)]
pub(super) enum PlanningSubview {
    Forecast,
    Calendar,
    Goals,
    Scenarios,
}

impl PlanningSubview {
    fn all() -> [PlanningSubview; 4] {
        [
            PlanningSubview::Forecast,
            PlanningSubview::Calendar,
            PlanningSubview::Goals,
            PlanningSubview::Scenarios,
        ]
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            PlanningSubview::Forecast => "FORECAST",
            PlanningSubview::Calendar => "CALENDAR",
            PlanningSubview::Goals => "GOALS",
            PlanningSubview::Scenarios => "SCENARIOS",
        }
    }
}

#[derive(Clone)]
pub(super) enum FormKind {
    TransactionAdd,
    TransactionEdit { id: i64 },
    AccountAdd,
    AccountEdit { id: i64 },
    CategoryAdd,
    CategoryEdit { id: i64 },
    BudgetSet,
    PlanningItemAdd,
    PlanningItemEdit { id: i64 },
    PlanningGoalAdd,
    PlanningGoalEdit { id: i64 },
    PlanningScenarioAdd,
    PlanningScenarioEdit { id: i64 },
    TransactionFilter,
    ImportCsv,
    RecurringAdd,
    RecurringEdit { id: i64 },
    ReconcileStart,
    WatchlistAdd,
    PolyWatchAdd,
    PolySearch,
}

#[derive(Clone)]
pub(super) struct FormField {
    pub(super) label: &'static str,
    pub(super) value: String,
    pub(super) required: bool,
}

#[derive(Clone)]
pub(super) struct FormState {
    pub(super) kind: FormKind,
    pub(super) title: &'static str,
    pub(super) hint: &'static str,
    pub(super) fields: Vec<FormField>,
    pub(super) active: usize,
}

impl FormState {
    fn current_field_mut(&mut self) -> &mut FormField {
        &mut self.fields[self.active]
    }

    fn next_field(&mut self) {
        self.active = (self.active + 1) % self.fields.len();
    }

    fn previous_field(&mut self) {
        if self.active == 0 {
            self.active = self.fields.len() - 1;
        } else {
            self.active -= 1;
        }
    }
}

#[derive(Clone)]
pub(super) struct ReconcileSelectionState {
    pub(super) account_id: i64,
    pub(super) account_name: String,
    pub(super) statement_ending_on: String,
    pub(super) statement_balance_cents: i64,
    pub(super) opening_balance_cents: i64,
    pub(super) eligible_transactions: Vec<TransactionRecord>,
    pub(super) selected_ids: HashSet<i64>,
    pub(super) active: usize,
}

impl ReconcileSelectionState {
    pub(super) fn selected_count(&self) -> usize {
        self.selected_ids.len()
    }

    pub(super) fn selected_balance_cents(&self) -> i64 {
        self.opening_balance_cents
            + self
                .eligible_transactions
                .iter()
                .filter(|transaction| self.selected_ids.contains(&transaction.id))
                .map(|transaction| transaction_effect_for_account(self.account_id, transaction))
                .sum::<i64>()
    }

    pub(super) fn difference_cents(&self) -> i64 {
        self.statement_balance_cents - self.selected_balance_cents()
    }
}

#[derive(Clone)]
pub(super) struct CsvImportReviewState {
    pub(super) plan: CsvImportPlan,
    pub(super) preview: CsvImportResult,
    pub(super) active: usize,
}

#[derive(Clone)]
pub(super) struct PolySearchState {
    pub(super) query: String,
    pub(super) limit: usize,
    pub(super) results: Vec<PolySearchResult>,
    pub(super) active: usize,
}

enum FormOutcome {
    Refresh(String),
    OpenReconcile(ReconcileSelectionState, String),
    OpenImportReview(CsvImportReviewState, String),
    OpenPolySearch(PolySearchState, String),
}

pub(super) struct App {
    pub(super) db_path: PathBuf,
    pub(super) db: Db,
    pub(super) currency: String,
    pub(super) current_view: usize,
    pub(super) planning_subview: usize,
    pub(super) tx_index: usize,
    pub(super) account_index: usize,
    pub(super) category_index: usize,
    pub(super) budget_index: usize,
    pub(super) planning_day_index: usize,
    pub(super) planning_item_index: usize,
    pub(super) planning_goal_index: usize,
    pub(super) planning_scenario_index: usize,
    pub(super) market_index: usize,
    pub(super) poly_index: usize,
    pub(super) recurring_index: usize,
    pub(super) reconciliation_index: usize,
    pub(super) balances: Vec<BalanceRecord>,
    pub(super) accounts: Vec<Account>,
    pub(super) categories: Vec<Category>,
    pub(super) recent_transactions: Vec<TransactionRecord>,
    pub(super) transactions: Vec<TransactionRecord>,
    pub(super) summary: SummaryRecord,
    pub(super) cash_flow_trend: Vec<MonthlyCashFlowPoint>,
    pub(super) category_spending: Vec<CategorySpendingPoint>,
    pub(super) balance_trend: Vec<BalanceTrendPoint>,
    pub(super) budgets: Vec<BudgetStatusRecord>,
    pub(super) planning_actual_weekly: Vec<WeeklyBalancePoint>,
    pub(super) planning_baseline: ForecastSnapshot,
    pub(super) planning_forecast: ForecastSnapshot,
    pub(super) planning_items: Vec<PlanningItemRecord>,
    pub(super) planning_goals: Vec<PlanningGoalRecord>,
    pub(super) planning_scenarios: Vec<PlanningScenarioRecord>,
    pub(super) selected_planning_scenario_id: Option<i64>,
    pub(super) watchlist: Vec<WatchlistEntry>,
    pub(super) market_quote: Option<MarketQuote>,
    pub(super) market_news: Option<MarketNewsFeed>,
    pub(super) poly_watchlist: Vec<PolyWatchEntry>,
    pub(super) poly_market: Option<PolyMarket>,
    pub(super) poly_movers: Vec<PolyMarket>,
    pub(super) recurring_rules: Vec<RecurringRuleRecord>,
    pub(super) due_occurrences: Vec<RecurringOccurrenceRecord>,
    pub(super) reconciliations: Vec<ReconciliationRecord>,
    pub(super) show_help: bool,
    pub(super) input_mode: bool,
    pub(super) input_buffer: String,
    pub(super) tx_filters: TransactionFilters,
    pub(super) form: Option<FormState>,
    pub(super) form_replace_on_input: bool,
    pub(super) form_error: Option<String>,
    pub(super) reconcile_flow: Option<ReconcileSelectionState>,
    pub(super) import_review: Option<CsvImportReviewState>,
    pub(super) poly_search: Option<PolySearchState>,
    pub(super) command_log: Vec<String>,
    pub(super) status: String,
}

impl App {
    pub(super) fn new(db_path: PathBuf) -> Result<Self, AppError> {
        let db = Db::open_existing(&db_path)?;
        let mut app = Self {
            db_path,
            currency: db.currency_code()?,
            db,
            current_view: 0,
            planning_subview: 0,
            tx_index: 0,
            account_index: 0,
            category_index: 0,
            budget_index: 0,
            planning_day_index: 0,
            planning_item_index: 0,
            planning_goal_index: 0,
            planning_scenario_index: 0,
            market_index: 0,
            poly_index: 0,
            recurring_index: 0,
            reconciliation_index: 0,
            balances: Vec::new(),
            accounts: Vec::new(),
            categories: Vec::new(),
            recent_transactions: Vec::new(),
            transactions: Vec::new(),
            summary: empty_summary(),
            cash_flow_trend: Vec::new(),
            category_spending: Vec::new(),
            balance_trend: Vec::new(),
            budgets: Vec::new(),
            planning_actual_weekly: Vec::new(),
            planning_baseline: empty_forecast(),
            planning_forecast: empty_forecast(),
            planning_items: Vec::new(),
            planning_goals: Vec::new(),
            planning_scenarios: Vec::new(),
            selected_planning_scenario_id: None,
            watchlist: Vec::new(),
            market_quote: None,
            market_news: None,
            poly_watchlist: Vec::new(),
            poly_market: None,
            poly_movers: Vec::new(),
            recurring_rules: Vec::new(),
            due_occurrences: Vec::new(),
            reconciliations: Vec::new(),
            show_help: true,
            input_mode: false,
            input_buffer: String::new(),
            tx_filters: default_transaction_filters(),
            form: None,
            form_replace_on_input: false,
            form_error: None,
            reconcile_flow: None,
            import_review: None,
            poly_search: None,
            command_log: vec![
                "N opens a direct form for the current panel.".to_string(),
                "E edits the selected item when that panel supports it.".to_string(),
                "I opens CSV import from TRANSACTIONS or ACCOUNTS.".to_string(),
                "F or / opens transaction filters inside TRANSACTIONS.".to_string(),
                "S still opens raw command mode for power use.".to_string(),
            ],
            status: String::from(
                "Tab moves panels. N/E/F/I/D/R/A/P act on the current panel. Press ? for help.",
            ),
        };
        app.refresh()?;
        Ok(app)
    }

    pub(super) fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> Result<(), AppError> {
        loop {
            terminal.draw(|frame| self.render(frame))?;
            if event::poll(Duration::from_millis(200))? {
                if let Event::Key(key) = event::read()? {
                    if !should_handle_key_event(key) {
                        continue;
                    }
                    if self.handle_key(key)? {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn refresh(&mut self) -> Result<(), AppError> {
        let (from, to) = crate::current_month_range();
        let today = today_iso();
        self.currency = self.db.currency_code()?;
        self.balances = self.db.balances(None)?;
        self.accounts = self.db.list_accounts()?;
        self.categories = self.db.list_categories()?;
        self.recent_transactions = self.db.recent_transactions(6)?;
        self.transactions = self.db.list_transactions(&self.tx_filters)?;
        self.summary = self.db.summary(&from, &to, None)?;
        self.cash_flow_trend = self.db.monthly_cash_flow_trend(6)?;
        self.category_spending = self.db.category_spending(&from, &to, 6)?;
        self.balance_trend = self.db.total_balance_trend(6)?;
        self.planning_scenarios = self.db.list_planning_scenarios()?;
        if self
            .selected_planning_scenario_id
            .is_some_and(|selected_id| {
                !self
                    .planning_scenarios
                    .iter()
                    .any(|scenario| scenario.id == selected_id)
            })
        {
            self.selected_planning_scenario_id = None;
        }
        let selected_scenario = self.selected_planning_scenario_id.map(|id| id.to_string());
        self.budgets = self
            .db
            .budget_status(&today[..7], selected_scenario.as_deref())?;
        self.planning_actual_weekly = self.db.weekly_opening_balance_history(10)?;
        self.planning_baseline = self.db.forecast(None, None, 90)?;
        self.planning_forecast = self.db.forecast(selected_scenario.as_deref(), None, 90)?;
        self.planning_items =
            self.db
                .list_planning_items(selected_scenario.as_deref(), None, None)?;
        self.planning_goals = self.db.list_planning_goals()?;
        self.watchlist.clear();
        self.market_quote = None;
        self.market_news = None;
        self.poly_watchlist.clear();
        self.poly_market = None;
        self.poly_movers.clear();
        self.recurring_rules = self.db.list_recurring_rules()?;
        self.due_occurrences = self.db.list_due_occurrences(&today)?;
        self.reconciliations = self.db.list_reconciliations(None)?;
        self.clamp_indices();
        Ok(())
    }

    fn clamp_indices(&mut self) {
        self.tx_index = clamp_index(self.tx_index, self.transactions.len());
        self.account_index = clamp_index(self.account_index, self.accounts.len());
        self.category_index = clamp_index(self.category_index, self.categories.len());
        self.budget_index = clamp_index(self.budget_index, self.budgets.len());
        self.planning_day_index =
            clamp_index(self.planning_day_index, self.planning_forecast.daily.len());
        self.planning_item_index = clamp_index(self.planning_item_index, self.planning_items.len());
        self.planning_goal_index = clamp_index(self.planning_goal_index, self.planning_goals.len());
        self.planning_scenario_index = clamp_index(
            self.planning_scenario_index,
            self.planning_scenarios.len() + 1,
        );
        self.market_index = clamp_index(self.market_index, self.watchlist.len());
        self.poly_index = clamp_index(self.poly_index, self.poly_watchlist.len());
        self.recurring_index = clamp_index(self.recurring_index, self.recurring_rules.len());
        self.reconciliation_index =
            clamp_index(self.reconciliation_index, self.reconciliations.len());
        if let Some(flow) = self.reconcile_flow.as_mut() {
            flow.active = clamp_index(flow.active, flow.eligible_transactions.len());
        }
        if let Some(review) = self.import_review.as_mut() {
            review.active = clamp_index(review.active, review.preview.preview.len());
        }
        if let Some(search) = self.poly_search.as_mut() {
            search.active = clamp_index(search.active, search.results.len());
        }
    }
    fn handle_key(&mut self, key: KeyEvent) -> Result<bool, AppError> {
        if let KeyCode::Char(ch) = key.code {
            if key.modifiers.contains(KeyModifiers::CONTROL) && ch.eq_ignore_ascii_case(&'c') {
                return Ok(true);
            }
        }

        if self.reconcile_flow.is_some() {
            return self.handle_reconcile_flow_key(key);
        }
        if self.import_review.is_some() {
            return self.handle_import_review_key(key);
        }
        if self.poly_search.is_some() {
            return self.handle_poly_search_key(key);
        }
        if self.form.is_some() {
            return self.handle_form_key(key);
        }
        if self.input_mode {
            return self.handle_input_key(key);
        }

        match key.code {
            KeyCode::Left => {
                if matches!(self.current_view(), View::Planning) {
                    self.cycle_planning_subview(-1);
                }
            }
            KeyCode::Right => {
                if matches!(self.current_view(), View::Planning) {
                    self.cycle_planning_subview(1);
                }
            }
            KeyCode::Tab => {
                self.current_view = (self.current_view + 1) % View::all().len();
                self.status = format!("Switched to {}.", self.current_view().label());
            }
            KeyCode::BackTab => {
                if self.current_view == 0 {
                    self.current_view = View::all().len() - 1;
                } else {
                    self.current_view -= 1;
                }
                self.status = format!("Switched to {}.", self.current_view().label());
            }
            KeyCode::Esc => {
                if self.show_help {
                    self.show_help = false;
                    self.status = String::from("Help hidden. Press ? if you need it again.");
                }
            }
            KeyCode::Enter => {
                let result = if matches!(self.current_view(), View::Markets) {
                    self.refresh_selected_market(true)
                } else if matches!(self.current_view(), View::Poly) {
                    self.refresh_selected_poly(true)
                } else if matches!(self.current_view(), View::Planning) {
                    self.activate_selected_planning_entry()
                } else {
                    self.open_edit_form_for_current_view()
                };
                if let Err(error) = result {
                    self.status = error.to_string();
                }
            }
            KeyCode::Down => self.move_selection(1),
            KeyCode::Up => self.move_selection(-1),
            KeyCode::Char(ch) => match ch.to_ascii_lowercase() {
                'q' => return Ok(true),
                '?' | 'h' => {
                    self.show_help = !self.show_help;
                    self.status = if self.show_help {
                        String::from("Help open. Press Esc, ?, or H to close it.")
                    } else {
                        String::from("Help hidden. Press N or E to keep working.")
                    };
                }
                'j' => self.move_selection(1),
                'k' => self.move_selection(-1),
                'a' => {
                    let result = match self.current_view() {
                        View::Markets => self.refresh_all_watchlist_markets(),
                        View::Poly => self.refresh_all_poly_watchlist(),
                        _ => Ok(()),
                    };
                    if let Err(error) = result {
                        self.status = error.to_string();
                    }
                }
                'g' => {
                    let result = self.db.run_due_recurring(&today_iso());
                    match result {
                        Ok(count) => {
                            self.refresh()?;
                            self.status = format!("Posted {count} due recurring transactions.");
                        }
                        Err(error) => self.status = error.to_string(),
                    }
                }
                'd' => {
                    let result = match self.current_view() {
                        View::Transactions => self.toggle_delete_selected_transaction(),
                        View::Accounts => self.delete_selected_account(),
                        View::Categories => self.delete_selected_category(),
                        View::Budgets => self.delete_selected_budget(),
                        View::Planning => self.delete_selected_planning_entry(),
                        View::Markets => self.remove_selected_watchlist_item(),
                        View::Poly => self.remove_selected_poly_watch_item(),
                        View::Recurring => self.delete_selected_recurring_rule(),
                        View::Reconcile => self.delete_selected_reconciliation(),
                        _ => Ok(()),
                    };
                    if let Err(error) = result {
                        self.status = error.to_string();
                    }
                }
                'e' => {
                    if let Err(error) = self.open_edit_form_for_current_view() {
                        self.status = error.to_string();
                    }
                }
                'f' => {
                    let result = match self.current_view() {
                        View::Transactions => self.open_transaction_filter_form(),
                        View::Poly => self.open_poly_search_form(),
                        _ => Ok(()),
                    };
                    if let Err(error) = result {
                        self.status = error.to_string();
                    }
                }
                'p' => {
                    if matches!(self.current_view(), View::Recurring) {
                        if let Err(error) = self.toggle_pause_selected_recurring_rule() {
                            self.status = error.to_string();
                        }
                    }
                }
                'r' => {
                    let result = match self.current_view() {
                        View::Planning => self.refresh_planning_view(),
                        View::Accounts | View::Reconcile => {
                            self.open_reconcile_form_for_current_view()
                        }
                        View::Markets => self.refresh_selected_market(true),
                        View::Poly => self.refresh_selected_poly(true),
                        _ => Ok(()),
                    };
                    if let Err(error) = result {
                        self.status = error.to_string();
                    }
                }
                'i' => {
                    if let Err(error) = self.open_import_form_for_current_view() {
                        self.status = error.to_string();
                    }
                }
                'm' => {
                    if matches!(self.current_view(), View::Poly) {
                        if let Err(error) = self.refresh_poly_movers() {
                            self.status = error.to_string();
                        }
                    }
                }
                'n' => {
                    if let Err(error) = self.open_form_for_current_view() {
                        self.status = error.to_string();
                    }
                }
                'c' => {
                    if matches!(self.current_view(), View::Transactions) {
                        if let Err(error) = self.clear_transaction_filters() {
                            self.status = error.to_string();
                        }
                    }
                }
                's' | 'w' => self.open_input_mode(),
                '/' => {
                    let result = match self.current_view() {
                        View::Transactions => self.open_transaction_filter_form(),
                        View::Poly => self.open_poly_search_form(),
                        _ => {
                            self.open_input_mode_with_text("tx list --limit 20".to_string());
                            self.status = String::from(
                                "Command mode opened with a transaction-list starter. Edit it and press Enter.",
                            );
                            Ok(())
                        }
                    };
                    if let Err(error) = result {
                        self.status = error.to_string();
                    }
                }
                _ => {}
            },
            _ => {}
        }
        Ok(false)
    }

    fn handle_reconcile_flow_key(&mut self, key: KeyEvent) -> Result<bool, AppError> {
        match key.code {
            KeyCode::Esc => {
                self.reconcile_flow = None;
                self.status = String::from("Reconciliation review cancelled.");
            }
            KeyCode::Down => {
                if let Some(flow) = self.reconcile_flow.as_mut() {
                    shift_index(&mut flow.active, flow.eligible_transactions.len(), 1);
                }
            }
            KeyCode::Up => {
                if let Some(flow) = self.reconcile_flow.as_mut() {
                    shift_index(&mut flow.active, flow.eligible_transactions.len(), -1);
                }
            }
            KeyCode::Enter | KeyCode::Char(' ') => self.toggle_reconcile_selection(),
            _ if is_form_submit_key(key) => {
                if let Err(error) = self.save_reconcile_flow() {
                    self.status = error.to_string();
                }
            }
            KeyCode::Char(ch) => match ch.to_ascii_lowercase() {
                'j' => {
                    if let Some(flow) = self.reconcile_flow.as_mut() {
                        shift_index(&mut flow.active, flow.eligible_transactions.len(), 1);
                    }
                }
                'k' => {
                    if let Some(flow) = self.reconcile_flow.as_mut() {
                        shift_index(&mut flow.active, flow.eligible_transactions.len(), -1);
                    }
                }
                'a' => {
                    if let Some(flow) = self.reconcile_flow.as_mut() {
                        flow.selected_ids = flow
                            .eligible_transactions
                            .iter()
                            .map(|transaction| transaction.id)
                            .collect();
                        self.status = String::from("Selected all eligible transactions.");
                    }
                }
                'c' => {
                    if let Some(flow) = self.reconcile_flow.as_mut() {
                        flow.selected_ids.clear();
                        self.status = String::from("Cleared the reconciliation selection.");
                    }
                }
                _ => {}
            },
            _ => {}
        }
        Ok(false)
    }

    fn handle_import_review_key(&mut self, key: KeyEvent) -> Result<bool, AppError> {
        match key.code {
            KeyCode::Esc => {
                self.import_review = None;
                self.status = String::from("Import preview cancelled.");
            }
            KeyCode::Down => {
                if let Some(review) = self.import_review.as_mut() {
                    shift_index(&mut review.active, review.preview.preview.len(), 1);
                }
            }
            KeyCode::Up => {
                if let Some(review) = self.import_review.as_mut() {
                    shift_index(&mut review.active, review.preview.preview.len(), -1);
                }
            }
            _ if is_form_submit_key(key) => {
                if let Err(error) = self.confirm_import_review() {
                    self.status = error.to_string();
                }
            }
            KeyCode::Char(ch) => match ch.to_ascii_lowercase() {
                'j' => {
                    if let Some(review) = self.import_review.as_mut() {
                        shift_index(&mut review.active, review.preview.preview.len(), 1);
                    }
                }
                'k' => {
                    if let Some(review) = self.import_review.as_mut() {
                        shift_index(&mut review.active, review.preview.preview.len(), -1);
                    }
                }
                _ => {}
            },
            _ => {}
        }
        Ok(false)
    }
    fn handle_poly_search_key(&mut self, key: KeyEvent) -> Result<bool, AppError> {
        match key.code {
            KeyCode::Esc => {
                self.poly_search = None;
                self.status = String::from("Polymarket search closed.");
            }
            KeyCode::Down => {
                if let Some(search) = self.poly_search.as_mut() {
                    shift_index(&mut search.active, search.results.len(), 1);
                }
            }
            KeyCode::Up => {
                if let Some(search) = self.poly_search.as_mut() {
                    shift_index(&mut search.active, search.results.len(), -1);
                }
            }
            KeyCode::Enter => {
                if let Err(error) = self.add_selected_poly_search_result() {
                    self.status = error.to_string();
                }
            }
            KeyCode::Char(ch) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if ch.to_ascii_lowercase() == 's' {
                    if let Err(error) = self.rerun_poly_search() {
                        self.status = error.to_string();
                    }
                }
            }
            KeyCode::Char(ch) => match ch.to_ascii_lowercase() {
                'j' => {
                    if let Some(search) = self.poly_search.as_mut() {
                        shift_index(&mut search.active, search.results.len(), 1);
                    }
                }
                'k' => {
                    if let Some(search) = self.poly_search.as_mut() {
                        shift_index(&mut search.active, search.results.len(), -1);
                    }
                }
                'a' => {
                    if let Err(error) = self.add_selected_poly_search_result() {
                        self.status = error.to_string();
                    }
                }
                'r' => {
                    if let Err(error) = self.rerun_poly_search() {
                        self.status = error.to_string();
                    }
                }
                _ => {}
            },
            _ => {}
        }
        Ok(false)
    }
    fn handle_form_key(&mut self, key: KeyEvent) -> Result<bool, AppError> {
        match key.code {
            KeyCode::Esc => {
                self.clear_form_state();
                self.status = String::from("Form closed. Press N to open it again.");
            }
            KeyCode::Tab | KeyCode::Down => {
                if let Some(form) = self.form.as_mut() {
                    form.next_field();
                    self.form_replace_on_input = true;
                    self.form_error = None;
                }
            }
            KeyCode::BackTab | KeyCode::Up => {
                if let Some(form) = self.form.as_mut() {
                    form.previous_field();
                    self.form_replace_on_input = true;
                    self.form_error = None;
                }
            }
            KeyCode::Enter => {
                if let Err(error) = self.execute_form() {
                    self.form_error = Some(error.to_string());
                    self.status = error.to_string();
                }
            }
            KeyCode::Backspace => {
                if let Some(form) = self.form.as_mut() {
                    apply_form_backspace(
                        &mut form.current_field_mut().value,
                        &mut self.form_replace_on_input,
                    );
                    self.form_error = None;
                }
            }
            _ if is_form_submit_key(key) => {
                if let Err(error) = self.execute_form() {
                    self.form_error = Some(error.to_string());
                    self.status = error.to_string();
                }
            }
            KeyCode::Char(ch) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                match ch.to_ascii_lowercase() {
                    'u' => {
                        if let Some(form) = self.form.as_mut() {
                            form.current_field_mut().value.clear();
                            self.form_replace_on_input = false;
                            self.form_error = None;
                        }
                    }
                    _ => {}
                }
            }
            KeyCode::Char(ch) => {
                if let Some(form) = self.form.as_mut() {
                    apply_form_text_input(
                        &mut form.current_field_mut().value,
                        &mut self.form_replace_on_input,
                        ch,
                    );
                    self.form_error = None;
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_input_key(&mut self, key: KeyEvent) -> Result<bool, AppError> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = false;
                self.status = String::from("Command mode closed. Press S to open it again.");
            }
            KeyCode::Enter => self.execute_input_command()?,
            KeyCode::Backspace => {
                self.input_buffer.pop();
            }
            KeyCode::Tab => self.input_buffer.push(' '),
            KeyCode::Char(ch) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if ch.to_ascii_lowercase() == 'u' {
                    self.input_buffer.clear();
                    self.status = String::from("Input cleared.");
                }
            }
            KeyCode::Char(ch) => self.input_buffer.push(ch),
            _ => {}
        }
        Ok(false)
    }

    fn move_selection(&mut self, delta: isize) {
        match self.current_view() {
            View::Transactions => shift_index(&mut self.tx_index, self.transactions.len(), delta),
            View::Accounts => shift_index(&mut self.account_index, self.accounts.len(), delta),
            View::Categories => shift_index(&mut self.category_index, self.categories.len(), delta),
            View::Budgets => shift_index(&mut self.budget_index, self.budgets.len(), delta),
            View::Planning => match self.current_planning_subview() {
                PlanningSubview::Forecast => {
                    shift_index(
                        &mut self.planning_day_index,
                        self.planning_forecast.daily.len(),
                        delta,
                    );
                }
                PlanningSubview::Calendar => {
                    shift_index(
                        &mut self.planning_item_index,
                        self.planning_items.len(),
                        delta,
                    );
                }
                PlanningSubview::Goals => {
                    shift_index(
                        &mut self.planning_goal_index,
                        self.planning_goals.len(),
                        delta,
                    );
                }
                PlanningSubview::Scenarios => {
                    shift_index(
                        &mut self.planning_scenario_index,
                        self.planning_scenarios.len() + 1,
                        delta,
                    );
                }
            },
            View::Markets => {
                shift_index(&mut self.market_index, self.watchlist.len(), delta);
                if let Err(error) = self.load_selected_market_from_cache() {
                    self.status = error.to_string();
                }
            }
            View::Poly => {
                shift_index(&mut self.poly_index, self.poly_watchlist.len(), delta);
                if let Err(error) = self.load_selected_poly_from_cache() {
                    self.status = error.to_string();
                }
            }
            View::Recurring => {
                shift_index(&mut self.recurring_index, self.recurring_rules.len(), delta)
            }
            View::Reconcile => shift_index(
                &mut self.reconciliation_index,
                self.reconciliations.len(),
                delta,
            ),
            View::Dashboard | View::Summary => {}
        }
    }

    fn toggle_delete_selected_transaction(&mut self) -> Result<(), AppError> {
        let Some(transaction) = self.transactions.get(self.tx_index).cloned() else {
            return Err(AppError::Validation("no transaction selected".to_string()));
        };
        if transaction.deleted_at.is_some() {
            self.db.restore_transaction(transaction.id)?;
            self.status = format!("Restored transaction {}.", transaction.id);
        } else {
            self.db.delete_transaction(transaction.id)?;
            self.status = format!("Deleted transaction {}.", transaction.id);
        }
        self.refresh()?;
        Ok(())
    }

    fn delete_selected_category(&mut self) -> Result<(), AppError> {
        let category_id = self
            .categories
            .get(self.category_index)
            .map(|category| category.id)
            .ok_or_else(|| AppError::Validation("no category selected".to_string()))?;
        self.db.delete_category(&category_id.to_string())?;
        self.refresh()?;
        self.status = format!("Archived category {}.", category_id);
        Ok(())
    }

    fn delete_selected_account(&mut self) -> Result<(), AppError> {
        let account = self
            .accounts
            .get(self.account_index)
            .cloned()
            .ok_or_else(|| AppError::Validation("no account selected".to_string()))?;
        self.db.delete_account(&account.id.to_string())?;
        self.refresh()?;
        self.status = format!("Archived account {}.", account.name);
        Ok(())
    }

    fn delete_selected_budget(&mut self) -> Result<(), AppError> {
        let row = self
            .budgets
            .get(self.budget_index)
            .cloned()
            .ok_or_else(|| AppError::Validation("no budget selected".to_string()))?;
        if row.scenario_id.is_some() && !row.is_override {
            return Err(AppError::Validation(
                "selected budget is inherited from baseline; switch to baseline to delete it"
                    .to_string(),
            ));
        }
        self.db.delete_budget(
            &row.month,
            &row.category_name,
            row.scenario_name.as_deref().filter(|_| row.is_override),
        )?;
        self.refresh()?;
        self.status = if row.is_override {
            format!(
                "Reset scenario budget for {} in {}.",
                row.category_name, row.month
            )
        } else {
            format!("Deleted budget for {} in {}.", row.category_name, row.month)
        };
        Ok(())
    }

    pub(super) fn current_planning_subview(&self) -> PlanningSubview {
        PlanningSubview::all()[self.planning_subview]
    }

    fn cycle_planning_subview(&mut self, delta: isize) {
        let total = PlanningSubview::all().len() as isize;
        let next = (self.planning_subview as isize + delta).rem_euclid(total) as usize;
        self.planning_subview = next;
        self.status = format!(
            "Planning view switched to {}.",
            self.current_planning_subview().label()
        );
    }

    pub(super) fn selected_planning_scenario(&self) -> Option<&PlanningScenarioRecord> {
        if self.planning_scenario_index == 0 {
            None
        } else {
            self.planning_scenarios
                .get(self.planning_scenario_index - 1)
        }
    }

    fn activate_selected_planning_entry(&mut self) -> Result<(), AppError> {
        match self.current_planning_subview() {
            PlanningSubview::Forecast => {
                self.status = String::from(
                    "Forecast rows are read-only. Use CALENDAR, GOALS, or SCENARIOS for actions.",
                );
                Ok(())
            }
            PlanningSubview::Calendar => {
                let item = self
                    .planning_items
                    .get(self.planning_item_index)
                    .ok_or_else(|| AppError::Validation("no planning item selected".to_string()))?;
                let item_id = item.id;
                let linked_transaction_id = item.linked_transaction_id;
                if linked_transaction_id.is_some() {
                    self.status = format!(
                        "Planning item {} is already posted as transaction {}.",
                        item_id,
                        linked_transaction_id.unwrap_or_default()
                    );
                    Ok(())
                } else {
                    let transaction_id = self.db.post_planning_item(item_id)?;
                    self.refresh()?;
                    self.status = format!(
                        "Posted planning item {} as transaction {}.",
                        item_id, transaction_id
                    );
                    Ok(())
                }
            }
            PlanningSubview::Goals => {
                self.status = String::from(
                    "Goal detail is already shown. Press E to edit it or D to archive it.",
                );
                Ok(())
            }
            PlanningSubview::Scenarios => {
                self.selected_planning_scenario_id = self
                    .selected_planning_scenario()
                    .map(|scenario| scenario.id);
                self.refresh()?;
                self.status = match self.selected_planning_scenario() {
                    Some(scenario) => format!("Selected planning scenario {}.", scenario.name),
                    None => String::from("Selected the baseline planning forecast."),
                };
                Ok(())
            }
        }
    }

    fn delete_selected_planning_entry(&mut self) -> Result<(), AppError> {
        match self.current_planning_subview() {
            PlanningSubview::Forecast => Err(AppError::Validation(
                "forecast rows cannot be archived".to_string(),
            )),
            PlanningSubview::Calendar => {
                let item_id = self
                    .planning_items
                    .get(self.planning_item_index)
                    .map(|item| item.id)
                    .ok_or_else(|| AppError::Validation("no planning item selected".to_string()))?;
                self.db.delete_planning_item(item_id)?;
                self.refresh()?;
                self.status = format!("Archived planning item {}.", item_id);
                Ok(())
            }
            PlanningSubview::Goals => {
                let goal_id = self
                    .planning_goals
                    .get(self.planning_goal_index)
                    .map(|goal| goal.id)
                    .ok_or_else(|| AppError::Validation("no goal selected".to_string()))?;
                self.db.delete_planning_goal(goal_id)?;
                self.refresh()?;
                self.status = format!("Archived goal {}.", goal_id);
                Ok(())
            }
            PlanningSubview::Scenarios => {
                let Some(scenario) = self.selected_planning_scenario() else {
                    self.status = String::from(
                        "Baseline is built in. Press N to create a scenario, then D to archive it.",
                    );
                    return Ok(());
                };
                let scenario_id = scenario.id;
                let scenario_name = scenario.name.clone();
                self.db.delete_planning_scenario(scenario_id)?;
                if self.selected_planning_scenario_id == Some(scenario_id) {
                    self.selected_planning_scenario_id = None;
                }
                self.refresh()?;
                self.status = format!("Archived scenario {}.", scenario_name);
                Ok(())
            }
        }
    }

    fn refresh_planning_view(&mut self) -> Result<(), AppError> {
        self.refresh()?;
        self.status = format!(
            "Planning forecast refreshed for {}.",
            self.selected_planning_scenario()
                .map(|scenario| scenario.name.as_str())
                .unwrap_or("baseline")
        );
        Ok(())
    }

    fn delete_selected_recurring_rule(&mut self) -> Result<(), AppError> {
        let rule_id = self
            .recurring_rules
            .get(self.recurring_index)
            .map(|rule| rule.id)
            .ok_or_else(|| AppError::Validation("no recurring rule selected".to_string()))?;
        self.db.delete_recurring_rule(rule_id)?;
        self.refresh()?;
        self.status = format!("Deleted recurring rule {}.", rule_id);
        Ok(())
    }

    fn toggle_pause_selected_recurring_rule(&mut self) -> Result<(), AppError> {
        let Some(rule) = self.recurring_rules.get(self.recurring_index) else {
            return Err(AppError::Validation(
                "no recurring rule selected".to_string(),
            ));
        };
        if rule.paused {
            self.db.resume_recurring_rule(rule.id)?;
            self.status = format!("Resumed recurring rule {}.", rule.id);
        } else {
            self.db.pause_recurring_rule(rule.id)?;
            self.status = format!("Paused recurring rule {}.", rule.id);
        }
        self.refresh()?;
        Ok(())
    }

    fn delete_selected_reconciliation(&mut self) -> Result<(), AppError> {
        let reconciliation_id = self
            .reconciliations
            .get(self.reconciliation_index)
            .map(|reconciliation| reconciliation.id)
            .ok_or_else(|| AppError::Validation("no reconciliation selected".to_string()))?;
        self.db.delete_reconciliation(reconciliation_id)?;
        self.refresh()?;
        self.status = format!("Removed reconciliation {}.", reconciliation_id);
        Ok(())
    }

    fn load_selected_market_from_cache(&mut self) -> Result<(), AppError> {
        self.market_quote = None;
        self.market_news = None;
        self.market_index = clamp_index(self.market_index, self.watchlist.len());
        let Some(entry) = self.watchlist.get(self.market_index) else {
            return Ok(());
        };
        self.market_quote = self.db.cached_quote(&entry.ticker)?;
        self.market_news = self.db.cached_news(&entry.ticker)?;
        Ok(())
    }

    fn selected_watchlist_ticker(&self) -> Result<String, AppError> {
        self.watchlist
            .get(self.market_index)
            .map(|entry| entry.ticker.clone())
            .ok_or_else(|| AppError::Validation("no watchlist ticker selected".to_string()))
    }

    fn refresh_selected_market(&mut self, fetch_live: bool) -> Result<(), AppError> {
        let ticker = self.selected_watchlist_ticker()?;
        if fetch_live {
            let service = MarketService::from_env();
            let summary = service.refresh_watchlist(
                &self.db,
                Some(&ticker),
                false,
                MarketRefreshKind::Both,
            )?;
            self.watchlist = service.list_watchlist(&self.db)?;
            self.market_index = self
                .watchlist
                .iter()
                .position(|entry| entry.ticker.eq_ignore_ascii_case(&ticker))
                .unwrap_or(0);
            self.load_selected_market_from_cache()?;
            self.status = format!(
                "Refreshed {} quote(s) and {} news feed(s) for {}.",
                summary.quote_refreshed, summary.news_refreshed, ticker
            );
        } else {
            self.load_selected_market_from_cache()?;
        }
        Ok(())
    }

    fn refresh_all_watchlist_markets(&mut self) -> Result<(), AppError> {
        let service = MarketService::from_env();
        let summary = service.refresh_watchlist(&self.db, None, true, MarketRefreshKind::Both)?;
        self.refresh()?;
        self.status = format!(
            "Refreshed {} quote(s) and {} news feed(s) across {} ticker(s).",
            summary.quote_refreshed, summary.news_refreshed, summary.tickers_considered
        );
        Ok(())
    }

    fn remove_selected_watchlist_item(&mut self) -> Result<(), AppError> {
        let ticker = self.selected_watchlist_ticker()?;
        MarketService::from_env().remove_watchlist(&self.db, &ticker)?;
        self.refresh()?;
        self.status = format!("Removed {} from the watchlist.", ticker);
        Ok(())
    }
    fn load_selected_poly_from_cache(&mut self) -> Result<(), AppError> {
        self.poly_market = None;
        self.poly_index = clamp_index(self.poly_index, self.poly_watchlist.len());
        let Some(entry) = self.poly_watchlist.get(self.poly_index) else {
            return Ok(());
        };
        self.poly_market = self.db.cached_poly_market(&entry.slug)?;
        Ok(())
    }

    fn selected_poly_slug(&self) -> Result<String, AppError> {
        self.poly_watchlist
            .get(self.poly_index)
            .map(|entry| entry.slug.clone())
            .ok_or_else(|| AppError::Validation("no Polymarket watch item selected".to_string()))
    }

    fn refresh_selected_poly(&mut self, fetch_live: bool) -> Result<(), AppError> {
        let slug = self.selected_poly_slug()?;
        if fetch_live {
            let service = PolyService::from_env();
            let summary = service.refresh_watchlist(&self.db, Some(&slug), false)?;
            self.poly_watchlist = service.list_watchlist(&self.db)?;
            self.poly_index = self
                .poly_watchlist
                .iter()
                .position(|entry| entry.slug.eq_ignore_ascii_case(&slug))
                .unwrap_or(0);
            self.load_selected_poly_from_cache()?;
            self.status = format!(
                "Refreshed {} Polymarket market(s) for {}.",
                summary.markets_refreshed, slug
            );
        } else {
            self.load_selected_poly_from_cache()?;
        }
        Ok(())
    }

    fn refresh_poly_movers(&mut self) -> Result<(), AppError> {
        self.poly_movers = PolyService::from_env().movers(&self.db, 8, false)?;
        self.status = format!("Refreshed {} Polymarket mover(s).", self.poly_movers.len());
        Ok(())
    }

    fn refresh_all_poly_watchlist(&mut self) -> Result<(), AppError> {
        let service = PolyService::from_env();
        let summary = service.refresh_watchlist(&self.db, None, true)?;
        self.refresh()?;
        self.status = format!(
            "Refreshed {} Polymarket market(s) across {} watched slug(s).",
            summary.markets_refreshed, summary.slugs_considered
        );
        Ok(())
    }

    fn remove_selected_poly_watch_item(&mut self) -> Result<(), AppError> {
        let slug = self.selected_poly_slug()?;
        PolyService::from_env().remove_watchlist(&self.db, &slug)?;
        self.refresh()?;
        self.status = format!("Removed {} from the Polymarket watchlist.", slug);
        Ok(())
    }
    fn rerun_poly_search(&mut self) -> Result<(), AppError> {
        let (query, limit) = self
            .poly_search
            .as_ref()
            .map(|search| (search.query.clone(), search.limit))
            .ok_or_else(|| AppError::Validation("Polymarket search is not active".to_string()))?;
        let results = PolyService::from_env().search(&query, limit)?;
        let result_count = results.len();
        self.poly_search = Some(PolySearchState {
            query,
            limit,
            results,
            active: 0,
        });
        self.status = format!(
            "Loaded {} Polymarket result(s). Press Enter or A to add the selected market.",
            result_count
        );
        Ok(())
    }

    fn add_selected_poly_search_result(&mut self) -> Result<(), AppError> {
        let Some(search) = self.poly_search.as_ref() else {
            return Err(AppError::Validation(
                "Polymarket search is not active".to_string(),
            ));
        };
        let selected = search.results.get(search.active).ok_or_else(|| {
            AppError::Validation("No Polymarket search result is selected".to_string())
        })?;
        let slug = selected.slug.clone();
        let label = Some(selected.question.clone());
        let service = PolyService::from_env();
        let _ = service.add_watchlist(&self.db, &slug, label.as_deref())?;
        self.refresh()?;
        self.poly_index = self
            .poly_watchlist
            .iter()
            .position(|entry| entry.slug.eq_ignore_ascii_case(&slug))
            .unwrap_or(self.poly_index);
        self.load_selected_poly_from_cache()?;
        self.poly_search = None;
        self.status = format!(
            "Added {} to the Polymarket watchlist. Press R to refresh live data.",
            slug
        );
        Ok(())
    }
    fn open_form_for_current_view(&mut self) -> Result<(), AppError> {
        self.show_help = false;
        self.input_mode = false;
        self.reconcile_flow = None;
        self.poly_search = None;
        let form = match self.current_view() {
            View::Dashboard | View::Transactions => Some(self.transaction_add_form()),
            View::Accounts => Some(self.account_form()),
            View::Categories => Some(self.category_form()),
            View::Summary | View::Budgets => Some(self.budget_form_for_selected()),
            View::Planning => Some(self.planning_form_for_current_subview()?),
            View::Markets => Some(self.watchlist_add_form()),
            View::Poly => Some(self.poly_watch_add_form()),
            View::Recurring => Some(self.recurring_add_form()),
            View::Reconcile => Some(self.reconcile_start_form(self.selected_account_name())),
        };
        self.open_form(
            form,
            "Form mode active. Typing replaces the selected value. Enter, Ctrl+S, or F2 saves.",
        );
        Ok(())
    }

    fn open_edit_form_for_current_view(&mut self) -> Result<(), AppError> {
        self.show_help = false;
        self.input_mode = false;
        self.reconcile_flow = None;
        self.poly_search = None;
        let form = match self.current_view() {
            View::Transactions => Some(self.transaction_edit_form()?),
            View::Accounts => Some(self.account_edit_form()?),
            View::Categories => Some(self.category_edit_form()?),
            View::Budgets => Some(self.budget_form_for_selected()),
            View::Planning => Some(self.planning_edit_form_for_current_subview()?),
            View::Recurring => Some(self.recurring_edit_form()?),
            View::Reconcile => {
                return self.open_reconcile_form_for_current_view();
            }
            View::Dashboard | View::Summary | View::Markets | View::Poly => {
                return Err(AppError::Validation(
                    "this panel does not support inline edit yet".to_string(),
                ));
            }
        };
        self.open_form(
            form,
            "Edit form active. Typing replaces the selected value. Enter, Ctrl+S, or F2 saves.",
        );
        Ok(())
    }

    fn open_reconcile_form_for_current_view(&mut self) -> Result<(), AppError> {
        self.show_help = false;
        self.input_mode = false;
        self.reconcile_flow = None;
        self.poly_search = None;
        self.open_form(
            Some(self.reconcile_start_form(self.selected_account_name())),
            "Reconciliation setup active. Enter account/date/balance, then press Enter, Ctrl+S, or F2.",
        );
        Ok(())
    }

    fn open_import_form_for_current_view(&mut self) -> Result<(), AppError> {
        self.show_help = false;
        self.input_mode = false;
        self.reconcile_flow = None;
        self.poly_search = None;
        self.import_review = None;
        let form = Some(match self.current_view() {
            View::Transactions | View::Accounts => self.import_form(),
            _ => {
                return Err(AppError::Validation(
                    "CSV import is available from TRANSACTIONS or ACCOUNTS".to_string(),
                ));
            }
        });
        self.open_form(
            form,
            "Import setup active. Fill the CSV fields and press Enter, Ctrl+S, or F2 for a dry-run preview.",
        );
        Ok(())
    }

    fn open_transaction_filter_form(&mut self) -> Result<(), AppError> {
        if !matches!(self.current_view(), View::Transactions) {
            return Err(AppError::Validation(
                "transaction filters are only available from TRANSACTIONS".to_string(),
            ));
        }
        self.show_help = false;
        self.input_mode = false;
        self.reconcile_flow = None;
        self.import_review = None;
        self.poly_search = None;
        self.open_form(
            Some(self.transaction_filter_form()),
            "Transaction filter form active. Leave fields blank to disable them, then press Enter, Ctrl+S, or F2.",
        );
        Ok(())
    }
    fn open_poly_search_form(&mut self) -> Result<(), AppError> {
        if !matches!(self.current_view(), View::Poly) {
            return Err(AppError::Validation(
                "Polymarket search is only available from POLY".to_string(),
            ));
        }
        self.show_help = false;
        self.input_mode = false;
        self.reconcile_flow = None;
        self.import_review = None;
        self.poly_search = None;
        self.open_form(
            Some(self.poly_search_form()),
            "Polymarket search active. Fill query/limit and press Ctrl+S to fetch markets.",
        );
        Ok(())
    }

    fn open_form(&mut self, form: Option<FormState>, status: &str) {
        self.form = form;
        self.form_replace_on_input = self.form.is_some();
        self.form_error = None;
        self.status = String::from(status);
    }

    fn clear_form_state(&mut self) {
        self.form = None;
        self.form_replace_on_input = false;
        self.form_error = None;
    }

    fn clear_transaction_filters(&mut self) -> Result<(), AppError> {
        self.tx_filters = default_transaction_filters();
        self.refresh()?;
        self.status = String::from("Transaction filters cleared. Showing recent activity again.");
        Ok(())
    }
    fn poly_search_form(&self) -> FormState {
        let (query, limit) = self
            .poly_search
            .as_ref()
            .map(|search| (search.query.clone(), search.limit.to_string()))
            .unwrap_or_else(|| {
                let default_query = self
                    .poly_market
                    .as_ref()
                    .map(|market| market.question.clone())
                    .or_else(|| {
                        self.poly_watchlist
                            .get(self.poly_index)
                            .map(|entry| entry.slug.clone())
                    })
                    .unwrap_or_default();
                (default_query, "8".to_string())
            });
        FormState {
            kind: FormKind::PolySearch,
            title: "SEARCH POLYMARKET",
            hint: "Enter a query and result limit, then press Ctrl+S. In results: Enter/A adds selected market.",
            fields: vec![
                FormField {
                    label: "QUERY",
                    value: query,
                    required: true,
                },
                FormField {
                    label: "LIMIT",
                    value: limit,
                    required: true,
                },
            ],
            active: 0,
        }
    }
    fn watchlist_add_form(&self) -> FormState {
        FormState {
            kind: FormKind::WatchlistAdd,
            title: "ADD WATCHLIST TICKER",
            hint: "Add a ticker symbol and optional label. Use R or Enter from MARKETS to refresh live data.",
            fields: vec![
                FormField { label: "TICKER", value: String::new(), required: true },
                FormField { label: "LABEL", value: String::new(), required: false },
            ],
            active: 0,
        }
    }

    fn poly_watch_add_form(&self) -> FormState {
        FormState {
            kind: FormKind::PolyWatchAdd,
            title: "ADD POLYMARKET MARKET",
            hint: "Add a Polymarket market slug or full event URL. Use R or Enter from POLY to refresh live data.",
            fields: vec![
                FormField { label: "SLUG", value: String::new(), required: true },
                FormField { label: "LABEL", value: String::new(), required: false },
            ],
            active: 0,
        }
    }
    fn import_form(&self) -> FormState {
        FormState {
            kind: FormKind::ImportCsv,
            title: "IMPORT CSV",
            hint: "Load a bank CSV, preview what will be created, then confirm the import from the review screen.",
            fields: vec![
                FormField { label: "FILE", value: String::new(), required: true },
                FormField { label: "ACCOUNT", value: self.selected_account_name(), required: true },
                FormField { label: "DATE COLUMN", value: "Date".to_string(), required: true },
                FormField { label: "AMOUNT COLUMN", value: "Amount".to_string(), required: true },
                FormField { label: "DESCRIPTION COL", value: "Description".to_string(), required: true },
                FormField { label: "CATEGORY COL", value: String::new(), required: false },
                FormField { label: "DEFAULT CATEGORY", value: self.default_expense_category(), required: false },
                FormField { label: "TYPE COLUMN", value: String::new(), required: false },
                FormField { label: "DEFAULT TYPE", value: String::new(), required: false },
                FormField { label: "DATE FORMAT", value: "%Y-%m-%d".to_string(), required: true },
                FormField { label: "ALLOW DUPES", value: "no".to_string(), required: true },
            ],
            active: 0,
        }
    }
    fn transaction_filter_form(&self) -> FormState {
        FormState {
            kind: FormKind::TransactionFilter,
            title: "FILTER TRANSACTIONS",
            hint: "Set date, account, category, or free-text search filters. Blank fields are ignored.",
            fields: vec![
                FormField { label: "FROM", value: self.tx_filters.from.clone().unwrap_or_default(), required: false },
                FormField { label: "TO", value: self.tx_filters.to.clone().unwrap_or_default(), required: false },
                FormField { label: "ACCOUNT", value: self.tx_filters.account.clone().unwrap_or_default(), required: false },
                FormField { label: "CATEGORY", value: self.tx_filters.category.clone().unwrap_or_default(), required: false },
                FormField { label: "SEARCH", value: self.tx_filters.search.clone().unwrap_or_default(), required: false },
                FormField { label: "LIMIT", value: self.tx_filters.limit.map(|value| value.to_string()).unwrap_or_default(), required: false },
                FormField { label: "INCLUDE DELETED", value: if self.tx_filters.include_deleted { "yes".to_string() } else { "no".to_string() }, required: true },
            ],
            active: 0,
        }
    }

    fn transaction_add_form(&self) -> FormState {
        FormState {
            kind: FormKind::TransactionAdd,
            title: "NEW TRANSACTION",
            hint: "Type, amount, date, account, category, payee, note. For transfers, fill TO ACCOUNT and leave CATEGORY blank.",
            fields: vec![
                FormField { label: "TYPE", value: "expense".to_string(), required: true },
                FormField { label: "AMOUNT", value: "0.00".to_string(), required: true },
                FormField { label: "DATE", value: today_iso(), required: true },
                FormField { label: "ACCOUNT", value: self.default_account_name(), required: true },
                FormField { label: "CATEGORY", value: self.default_expense_category(), required: false },
                FormField { label: "TO ACCOUNT", value: String::new(), required: false },
                FormField { label: "PAYEE", value: String::new(), required: false },
                FormField { label: "NOTE", value: String::new(), required: false },
            ],
            active: 0,
        }
    }

    fn transaction_edit_form(&self) -> Result<FormState, AppError> {
        let transaction = self
            .transactions
            .get(self.tx_index)
            .ok_or_else(|| AppError::Validation("no transaction selected".to_string()))?;
        if transaction.deleted_at.is_some() {
            return Err(AppError::Validation(
                "restore the selected transaction before editing it".to_string(),
            ));
        }
        if transaction.reconciliation_id.is_some() {
            return Err(AppError::Validation(
                "reconciled transactions are locked until that reconciliation is removed"
                    .to_string(),
            ));
        }

        Ok(FormState {
            kind: FormKind::TransactionEdit { id: transaction.id },
            title: "EDIT TRANSACTION",
            hint: "Update any field. For transfers, TO ACCOUNT is required and CATEGORY must stay blank.",
            fields: vec![
                FormField { label: "TYPE", value: transaction.kind.as_db_str().to_string(), required: true },
                FormField { label: "AMOUNT", value: format_money_input(transaction.amount_cents), required: true },
                FormField { label: "DATE", value: transaction.txn_date.clone(), required: true },
                FormField { label: "ACCOUNT", value: transaction.account_name.clone(), required: true },
                FormField { label: "CATEGORY", value: transaction.category_name.clone().unwrap_or_default(), required: false },
                FormField { label: "TO ACCOUNT", value: transaction.to_account_name.clone().unwrap_or_default(), required: false },
                FormField { label: "PAYEE", value: transaction.payee.clone().unwrap_or_default(), required: false },
                FormField { label: "NOTE", value: transaction.note.clone().unwrap_or_default(), required: false },
            ],
            active: 0,
        })
    }

    fn account_form(&self) -> FormState {
        FormState {
            kind: FormKind::AccountAdd,
            title: "NEW ACCOUNT",
            hint: "Create a cash, checking, savings, or credit account. Opening can be zero or negative.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: "New Account".to_string(),
                    required: true,
                },
                FormField {
                    label: "TYPE",
                    value: "checking".to_string(),
                    required: true,
                },
                FormField {
                    label: "OPENING",
                    value: "0.00".to_string(),
                    required: true,
                },
                FormField {
                    label: "OPENED ON",
                    value: today_iso(),
                    required: true,
                },
            ],
            active: 0,
        }
    }

    fn account_edit_form(&self) -> Result<FormState, AppError> {
        let account = self
            .accounts
            .get(self.account_index)
            .ok_or_else(|| AppError::Validation("no account selected".to_string()))?;
        Ok(FormState {
            kind: FormKind::AccountEdit { id: account.id },
            title: "EDIT ACCOUNT",
            hint:
                "Update the account name, type, or opening values. Opening can be zero or negative.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: account.name.clone(),
                    required: true,
                },
                FormField {
                    label: "TYPE",
                    value: account.kind.as_db_str().to_string(),
                    required: true,
                },
                FormField {
                    label: "OPENING",
                    value: format_money_input(account.opening_balance_cents),
                    required: true,
                },
                FormField {
                    label: "OPENED ON",
                    value: account.opened_on.clone(),
                    required: true,
                },
            ],
            active: 0,
        })
    }

    fn category_form(&self) -> FormState {
        FormState {
            kind: FormKind::CategoryAdd,
            title: "NEW CATEGORY",
            hint: "Create an income or expense category.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: "New Category".to_string(),
                    required: true,
                },
                FormField {
                    label: "KIND",
                    value: "expense".to_string(),
                    required: true,
                },
            ],
            active: 0,
        }
    }

    fn category_edit_form(&self) -> Result<FormState, AppError> {
        let category = self
            .categories
            .get(self.category_index)
            .ok_or_else(|| AppError::Validation("no category selected".to_string()))?;
        Ok(FormState {
            kind: FormKind::CategoryEdit { id: category.id },
            title: "EDIT CATEGORY",
            hint: "Rename the category or change its kind. Kind changes are blocked once the category is in use.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: category.name.clone(),
                    required: true,
                },
                FormField {
                    label: "KIND",
                    value: category.kind.as_db_str().to_string(),
                    required: true,
                },
            ],
            active: 0,
        })
    }

    fn budget_form_for_selected(&self) -> FormState {
        let category = self
            .budgets
            .get(self.budget_index)
            .map(|row| row.category_name.clone())
            .unwrap_or_else(|| self.default_budget_category());
        let month = self
            .budgets
            .get(self.budget_index)
            .map(|row| row.month.clone())
            .unwrap_or_else(|| today_iso()[..7].to_string());
        let amount = self
            .budgets
            .get(self.budget_index)
            .map(|row| format_money_input(row.budget_cents))
            .unwrap_or_else(|| "0.00".to_string());
        let account = self
            .budgets
            .get(self.budget_index)
            .and_then(|row| row.account_name.clone())
            .unwrap_or_else(|| self.default_account_name());
        let scenario = self
            .budgets
            .get(self.budget_index)
            .and_then(|row| row.scenario_name.clone())
            .or_else(|| {
                self.selected_planning_scenario()
                    .map(|entry| entry.name.clone())
            })
            .filter(|value| value != "baseline")
            .unwrap_or_default();
        FormState {
            kind: FormKind::BudgetSet,
            title: "SET BUDGET",
            hint: "Set a monthly budget for an expense category using YYYY-MM. Fill SCENARIO to create an override for the active planning case.",
            fields: vec![
                FormField {
                    label: "CATEGORY",
                    value: category,
                    required: true,
                },
                FormField {
                    label: "MONTH",
                    value: month,
                    required: true,
                },
                FormField {
                    label: "AMOUNT",
                    value: amount,
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: account,
                    required: false,
                },
                FormField {
                    label: "SCENARIO",
                    value: scenario,
                    required: false,
                },
            ],
            active: 0,
        }
    }

    fn planning_form_for_current_subview(&self) -> Result<FormState, AppError> {
        Ok(match self.current_planning_subview() {
            PlanningSubview::Forecast | PlanningSubview::Calendar => self.planning_item_add_form(),
            PlanningSubview::Goals => self.planning_goal_add_form(),
            PlanningSubview::Scenarios => self.planning_scenario_add_form(),
        })
    }

    fn planning_edit_form_for_current_subview(&self) -> Result<FormState, AppError> {
        match self.current_planning_subview() {
            PlanningSubview::Forecast => Err(AppError::Validation(
                "forecast rows are read-only; switch to CALENDAR, GOALS, or SCENARIOS to edit"
                    .to_string(),
            )),
            PlanningSubview::Calendar => self.planning_item_edit_form(),
            PlanningSubview::Goals => self.planning_goal_edit_form(),
            PlanningSubview::Scenarios => self.planning_scenario_edit_form(),
        }
    }

    fn planning_item_add_form(&self) -> FormState {
        let scenario = self
            .selected_planning_scenario()
            .map(|entry| entry.name.clone())
            .unwrap_or_default();
        FormState {
            kind: FormKind::PlanningItemAdd,
            title: "NEW PLANNING ITEM",
            hint:
                "Use baseline by leaving SCENARIO blank. Transfers need TO ACCOUNT and no CATEGORY.",
            fields: vec![
                FormField {
                    label: "TITLE",
                    value: "Upcoming Bill".to_string(),
                    required: true,
                },
                FormField {
                    label: "SCENARIO",
                    value: scenario,
                    required: false,
                },
                FormField {
                    label: "TYPE",
                    value: "expense".to_string(),
                    required: true,
                },
                FormField {
                    label: "AMOUNT",
                    value: String::new(),
                    required: true,
                },
                FormField {
                    label: "DATE",
                    value: today_iso(),
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: self.default_account_name(),
                    required: true,
                },
                FormField {
                    label: "CATEGORY",
                    value: self.default_expense_category(),
                    required: false,
                },
                FormField {
                    label: "TO ACCOUNT",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "PAYEE",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "NOTE",
                    value: String::new(),
                    required: false,
                },
            ],
            active: 0,
        }
    }

    fn planning_item_edit_form(&self) -> Result<FormState, AppError> {
        let item = self
            .planning_items
            .get(self.planning_item_index)
            .ok_or_else(|| AppError::Validation("no planning item selected".to_string()))?;
        Ok(FormState {
            kind: FormKind::PlanningItemEdit { id: item.id },
            title: "EDIT PLANNING ITEM",
            hint: "Update the planned item fields. Posted items can still be refined, but the real transaction stays unchanged.",
            fields: vec![
                FormField {
                    label: "TITLE",
                    value: item.title.clone(),
                    required: true,
                },
                FormField {
                    label: "SCENARIO",
                    value: item.scenario_name.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "TYPE",
                    value: item.kind.as_db_str().to_string(),
                    required: true,
                },
                FormField {
                    label: "AMOUNT",
                    value: format_money_input(item.amount_cents),
                    required: true,
                },
                FormField {
                    label: "DATE",
                    value: item.due_on.clone(),
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: item.account_name.clone(),
                    required: true,
                },
                FormField {
                    label: "CATEGORY",
                    value: item.category_name.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "TO ACCOUNT",
                    value: item.to_account_name.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "PAYEE",
                    value: item.payee.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "NOTE",
                    value: item.note.clone().unwrap_or_default(),
                    required: false,
                },
            ],
            active: 0,
        })
    }

    fn planning_goal_add_form(&self) -> FormState {
        FormState {
            kind: FormKind::PlanningGoalAdd,
            title: "NEW GOAL",
            hint: "BALANCE TARGET uses MINIMUM BALANCE only. SINKING FUND uses TARGET AMOUNT and DUE ON.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: "Cash Buffer".to_string(),
                    required: true,
                },
                FormField {
                    label: "KIND",
                    value: "balance_target".to_string(),
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: self.default_account_name(),
                    required: true,
                },
                FormField {
                    label: "TARGET AMOUNT",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "MINIMUM BAL",
                    value: "0.00".to_string(),
                    required: false,
                },
                FormField {
                    label: "DUE ON",
                    value: String::new(),
                    required: false,
                },
            ],
            active: 0,
        }
    }

    fn planning_goal_edit_form(&self) -> Result<FormState, AppError> {
        let goal = self
            .planning_goals
            .get(self.planning_goal_index)
            .ok_or_else(|| AppError::Validation("no goal selected".to_string()))?;
        Ok(FormState {
            kind: FormKind::PlanningGoalEdit { id: goal.id },
            title: "EDIT GOAL",
            hint:
                "Switch KIND carefully. BALANCE TARGET and SINKING FUND require different fields.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: goal.name.clone(),
                    required: true,
                },
                FormField {
                    label: "KIND",
                    value: goal.kind.as_db_str().to_string(),
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: goal.account_name.clone(),
                    required: true,
                },
                FormField {
                    label: "TARGET AMOUNT",
                    value: goal
                        .target_amount_cents
                        .map(format_money_input)
                        .unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "MINIMUM BAL",
                    value: goal
                        .minimum_balance_cents
                        .map(format_money_input)
                        .unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "DUE ON",
                    value: goal.due_on.clone().unwrap_or_default(),
                    required: false,
                },
            ],
            active: 0,
        })
    }

    fn planning_scenario_add_form(&self) -> FormState {
        FormState {
            kind: FormKind::PlanningScenarioAdd,
            title: "NEW SCENARIO",
            hint: "Scenarios layer extra planning items on top of the baseline forecast.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: "Stress Case".to_string(),
                    required: true,
                },
                FormField {
                    label: "NOTE",
                    value: String::new(),
                    required: false,
                },
            ],
            active: 0,
        }
    }

    fn planning_scenario_edit_form(&self) -> Result<FormState, AppError> {
        let scenario = self.selected_planning_scenario().ok_or_else(|| {
            AppError::Validation(
                "baseline is built in; press N to create a scenario to edit".to_string(),
            )
        })?;
        Ok(FormState {
            kind: FormKind::PlanningScenarioEdit { id: scenario.id },
            title: "EDIT SCENARIO",
            hint: "Rename the scenario or change its note.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: scenario.name.clone(),
                    required: true,
                },
                FormField {
                    label: "NOTE",
                    value: scenario.note.clone().unwrap_or_default(),
                    required: false,
                },
            ],
            active: 0,
        })
    }

    fn recurring_add_form(&self) -> FormState {
        FormState {
            kind: FormKind::RecurringAdd,
            title: "NEW RECURRING RULE",
            hint: "Use monthly with DAY OF MONTH, or weekly with WEEKDAY. Leave NEXT DUE ON blank to use the schedule automatically.",
            fields: vec![
                FormField { label: "NAME", value: "Monthly Rent".to_string(), required: true },
                FormField { label: "TYPE", value: "expense".to_string(), required: true },
                FormField { label: "AMOUNT", value: String::new(), required: true },
                FormField { label: "ACCOUNT", value: self.default_account_name(), required: true },
                FormField { label: "CATEGORY", value: self.default_expense_category(), required: false },
                FormField { label: "TO ACCOUNT", value: String::new(), required: false },
                FormField { label: "PAYEE", value: String::new(), required: false },
                FormField { label: "NOTE", value: String::new(), required: false },
                FormField { label: "CADENCE", value: "monthly".to_string(), required: true },
                FormField { label: "INTERVAL", value: "1".to_string(), required: true },
                FormField { label: "DAY OF MONTH", value: "1".to_string(), required: false },
                FormField { label: "WEEKDAY", value: String::new(), required: false },
                FormField { label: "START ON", value: today_iso(), required: true },
                FormField { label: "NEXT DUE ON", value: String::new(), required: false },
                FormField { label: "END ON", value: String::new(), required: false },
            ],
            active: 0,
        }
    }

    fn recurring_edit_form(&self) -> Result<FormState, AppError> {
        let rule = self
            .recurring_rules
            .get(self.recurring_index)
            .ok_or_else(|| AppError::Validation("no recurring rule selected".to_string()))?;
        Ok(FormState {
            kind: FormKind::RecurringEdit { id: rule.id },
            title: "EDIT RECURRING RULE",
            hint: "Keep DAY OF MONTH blank for weekly rules. NEXT DUE ON can override the next generated occurrence.",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: rule.name.clone(),
                    required: true,
                },
                FormField {
                    label: "TYPE",
                    value: rule.kind.as_db_str().to_string(),
                    required: true,
                },
                FormField {
                    label: "AMOUNT",
                    value: format_money_input(rule.amount_cents),
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: rule.account_name.clone(),
                    required: true,
                },
                FormField {
                    label: "CATEGORY",
                    value: rule.category_name.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "TO ACCOUNT",
                    value: rule.to_account_name.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "PAYEE",
                    value: rule.payee.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "NOTE",
                    value: rule.note.clone().unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "CADENCE",
                    value: rule.cadence.as_db_str().to_string(),
                    required: true,
                },
                FormField {
                    label: "INTERVAL",
                    value: rule.interval.to_string(),
                    required: true,
                },
                FormField {
                    label: "DAY OF MONTH",
                    value: rule
                        .day_of_month
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "WEEKDAY",
                    value: rule
                        .weekday
                        .map(|value| value.as_db_str().to_string())
                        .unwrap_or_default(),
                    required: false,
                },
                FormField {
                    label: "START ON",
                    value: rule.start_on.clone(),
                    required: true,
                },
                FormField {
                    label: "NEXT DUE ON",
                    value: rule.next_due_on.clone(),
                    required: false,
                },
                FormField {
                    label: "END ON",
                    value: rule.end_on.clone().unwrap_or_default(),
                    required: false,
                },
            ],
            active: 0,
        })
    }

    fn reconcile_start_form(&self, account_name: String) -> FormState {
        let statement_balance = self
            .balance_for_account_name(&account_name)
            .map(format_money_input)
            .unwrap_or_else(|| "0.00".to_string());
        FormState {
            kind: FormKind::ReconcileStart,
            title: "START RECONCILIATION",
            hint:
                "Ctrl+S or F2 loads eligible transactions. Then use Space to toggle and Ctrl+S or F2 to save.",
            fields: vec![
                FormField {
                    label: "ACCOUNT",
                    value: account_name,
                    required: true,
                },
                FormField {
                    label: "TO",
                    value: today_iso(),
                    required: true,
                },
                FormField {
                    label: "STATEMENT BALANCE",
                    value: statement_balance,
                    required: true,
                },
            ],
            active: 0,
        }
    }
    fn default_account_name(&self) -> String {
        self.accounts
            .get(self.account_index)
            .or_else(|| self.accounts.first())
            .map(|account| account.name.clone())
            .unwrap_or_default()
    }

    fn selected_account_name(&self) -> String {
        if matches!(self.current_view(), View::Transactions) {
            if let Some(transaction) = self.transactions.get(self.tx_index) {
                return transaction.account_name.clone();
            }
        }
        if let Some(account) = self.accounts.get(self.account_index) {
            account.name.clone()
        } else if let Some(reconciliation) = self.reconciliations.get(self.reconciliation_index) {
            reconciliation.account_name.clone()
        } else {
            self.default_account_name()
        }
    }

    fn default_expense_category(&self) -> String {
        self.categories
            .iter()
            .find(|category| category.kind.as_db_str() == "expense")
            .map(|category| category.name.clone())
            .unwrap_or_default()
    }

    fn default_budget_category(&self) -> String {
        self.categories
            .get(self.category_index)
            .filter(|category| category.kind.as_db_str() == "expense")
            .or_else(|| {
                self.categories
                    .iter()
                    .find(|category| category.kind.as_db_str() == "expense")
            })
            .map(|category| category.name.clone())
            .unwrap_or_default()
    }

    fn balance_for_account_name(&self, account_name: &str) -> Option<i64> {
        self.balances
            .iter()
            .find(|row| row.account_name.eq_ignore_ascii_case(account_name.trim()))
            .map(|row| row.current_balance_cents)
    }

    fn account_by_ref(&self, reference: &str) -> Option<&Account> {
        let trimmed = reference.trim();
        if let Ok(id) = trimmed.parse::<i64>() {
            if let Some(account) = self.accounts.iter().find(|account| account.id == id) {
                return Some(account);
            }
        }
        self.accounts
            .iter()
            .find(|account| account.name.eq_ignore_ascii_case(trimmed))
    }

    fn toggle_reconcile_selection(&mut self) {
        let Some(flow) = self.reconcile_flow.as_mut() else {
            return;
        };
        let Some(transaction) = flow.eligible_transactions.get(flow.active) else {
            self.status = String::from("No reconciliation transaction selected.");
            return;
        };
        if !flow.selected_ids.remove(&transaction.id) {
            flow.selected_ids.insert(transaction.id);
        }
        self.status = format!(
            "Selected {} transactions. Cleared balance now {}.",
            flow.selected_count(),
            crate::amount::format_cents(flow.selected_balance_cents())
        );
    }

    fn save_reconcile_flow(&mut self) -> Result<(), AppError> {
        let Some(flow) = self.reconcile_flow.as_ref() else {
            return Ok(());
        };
        let mut transaction_ids: Vec<i64> = flow.selected_ids.iter().copied().collect();
        transaction_ids.sort_unstable();
        let reconciliation_id = self.db.start_reconciliation(
            &flow.account_name,
            &flow.statement_ending_on,
            flow.statement_balance_cents,
            &transaction_ids,
        )?;
        self.reconcile_flow = None;
        self.refresh()?;
        self.status = format!("Created reconciliation {}.", reconciliation_id);
        Ok(())
    }

    fn execute_form(&mut self) -> Result<(), AppError> {
        let Some(form) = self.form.clone() else {
            return Ok(());
        };
        if let Some(field) = form
            .fields
            .iter()
            .find(|field| field.required && field.value.trim().is_empty())
        {
            return Err(AppError::Validation(format!(
                "{} is required before saving",
                field.label
            )));
        }

        match self.apply_form(&form)? {
            FormOutcome::Refresh(message) => {
                self.clear_form_state();
                self.refresh()?;
                self.status = message;
            }
            FormOutcome::OpenReconcile(flow, message) => {
                self.clear_form_state();
                self.reconcile_flow = Some(flow);
                self.status = message;
            }
            FormOutcome::OpenImportReview(review, message) => {
                self.clear_form_state();
                self.import_review = Some(review);
                self.status = message;
            }
            FormOutcome::OpenPolySearch(search, message) => {
                self.clear_form_state();
                self.poly_search = Some(search);
                self.status = message;
            }
        }
        Ok(())
    }

    fn apply_form(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        match form.kind.clone() {
            FormKind::TransactionAdd => self.save_new_transaction(form),
            FormKind::TransactionEdit { id } => self.save_transaction_edit(id, form),
            FormKind::AccountAdd => self.save_new_account(form),
            FormKind::AccountEdit { id } => self.save_account_edit(id, form),
            FormKind::CategoryAdd => self.save_new_category(form),
            FormKind::CategoryEdit { id } => self.save_category_edit(id, form),
            FormKind::BudgetSet => self.save_budget(form),
            FormKind::PlanningItemAdd => self.save_new_planning_item(form),
            FormKind::PlanningItemEdit { id } => self.save_planning_item_edit(id, form),
            FormKind::PlanningGoalAdd => self.save_new_planning_goal(form),
            FormKind::PlanningGoalEdit { id } => self.save_planning_goal_edit(id, form),
            FormKind::PlanningScenarioAdd => self.save_new_planning_scenario(form),
            FormKind::PlanningScenarioEdit { id } => self.save_planning_scenario_edit(id, form),
            FormKind::TransactionFilter => self.save_transaction_filters(form),
            FormKind::ImportCsv => self.save_import_preview(form),
            FormKind::RecurringAdd => self.save_new_recurring_rule(form),
            FormKind::RecurringEdit { id } => self.save_recurring_edit(id, form),
            FormKind::ReconcileStart => self.start_reconciliation_review(form),
            FormKind::WatchlistAdd => self.save_watchlist_add(form),
            FormKind::PolyWatchAdd => self.save_poly_watch_add(form),
            FormKind::PolySearch => self.save_poly_search(form),
        }
    }

    fn save_new_transaction(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let transaction = crate::model::NewTransaction {
            txn_date: normalize_date_input(form_value(form, 2))?,
            kind: parse_transaction_kind(form_value(form, 0))?,
            amount_cents: parse_amount_to_cents(form_value(form, 1))?,
            account: form_value(form, 3).trim().to_string(),
            category: optional_field(form, 4),
            to_account: optional_field(form, 5),
            payee: optional_field(form, 6),
            note: optional_field(form, 7),
            recurring_rule_id: None,
        };
        let transaction_id = self.db.add_transaction(&transaction)?;
        Ok(FormOutcome::Refresh(format!(
            "Created transaction {}.",
            transaction_id
        )))
    }

    fn save_transaction_edit(
        &mut self,
        id: i64,
        form: &FormState,
    ) -> Result<FormOutcome, AppError> {
        let kind = parse_transaction_kind(form_value(form, 0))?;
        let patch = UpdateTransaction {
            id,
            txn_date: Some(normalize_date_input(form_value(form, 2))?),
            kind: Some(kind),
            amount_cents: Some(parse_amount_to_cents(form_value(form, 1))?),
            account: Some(form_value(form, 3).trim().to_string()),
            to_account: optional_field(form, 5),
            category: optional_field(form, 4),
            payee: optional_field(form, 6),
            note: optional_field(form, 7),
            clear_to_account: optional_field(form, 5).is_none(),
            clear_category: optional_field(form, 4).is_none(),
            clear_payee: optional_field(form, 6).is_none(),
            clear_note: optional_field(form, 7).is_none(),
        };
        self.db.edit_transaction(&patch)?;
        Ok(FormOutcome::Refresh(format!("Updated transaction {}.", id)))
    }

    fn save_new_account(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let kind = parse_account_kind(form_value(form, 1))?;
        let account_id = self.db.add_account(
            form_value(form, 0).trim(),
            &kind,
            parse_balance_to_cents(form_value(form, 2))?,
            &normalize_date_input(form_value(form, 3))?,
        )?;
        Ok(FormOutcome::Refresh(format!(
            "Created account {}.",
            account_id
        )))
    }

    fn save_account_edit(&mut self, id: i64, form: &FormState) -> Result<FormOutcome, AppError> {
        let kind = parse_account_kind(form_value(form, 1))?;
        self.db.edit_account(
            &id.to_string(),
            Some(form_value(form, 0).trim()),
            Some(&kind),
            Some(parse_balance_to_cents(form_value(form, 2))?),
            Some(&normalize_date_input(form_value(form, 3))?),
        )?;
        Ok(FormOutcome::Refresh(format!("Updated account {}.", id)))
    }

    fn save_new_category(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let kind = parse_category_kind(form_value(form, 1))?;
        let category_id = self.db.add_category(form_value(form, 0).trim(), &kind)?;
        Ok(FormOutcome::Refresh(format!(
            "Created category {}.",
            category_id
        )))
    }

    fn save_category_edit(&mut self, id: i64, form: &FormState) -> Result<FormOutcome, AppError> {
        let kind = parse_category_kind(form_value(form, 1))?;
        self.db.edit_category(
            &id.to_string(),
            Some(form_value(form, 0).trim()),
            Some(&kind),
        )?;
        Ok(FormOutcome::Refresh(format!("Updated category {}.", id)))
    }

    fn save_budget(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let budget_id = self.db.set_budget(
            form_value(form, 1).trim(),
            form_value(form, 0).trim(),
            parse_amount_to_cents(form_value(form, 2))?,
            optional_field(form, 3).as_deref(),
            optional_field(form, 4).as_deref(),
        )?;
        Ok(FormOutcome::Refresh(format!("Saved budget {}.", budget_id)))
    }

    fn save_new_planning_item(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let item = NewPlanningItem {
            title: form_value(form, 0).trim().to_string(),
            scenario: optional_field(form, 1),
            kind: parse_transaction_kind(form_value(form, 2))?,
            amount_cents: parse_amount_to_cents(form_value(form, 3))?,
            due_on: normalize_date_input(form_value(form, 4))?,
            account: form_value(form, 5).trim().to_string(),
            category: optional_field(form, 6),
            to_account: optional_field(form, 7),
            payee: optional_field(form, 8),
            note: optional_field(form, 9),
        };
        let item_id = self.db.add_planning_item(&item)?;
        Ok(FormOutcome::Refresh(format!(
            "Created planning item {}.",
            item_id
        )))
    }

    fn save_planning_item_edit(
        &mut self,
        id: i64,
        form: &FormState,
    ) -> Result<FormOutcome, AppError> {
        let patch = UpdatePlanningItem {
            id,
            title: Some(form_value(form, 0).trim().to_string()),
            scenario: optional_field(form, 1),
            kind: Some(parse_transaction_kind(form_value(form, 2))?),
            amount_cents: Some(parse_amount_to_cents(form_value(form, 3))?),
            due_on: Some(normalize_date_input(form_value(form, 4))?),
            account: Some(form_value(form, 5).trim().to_string()),
            category: optional_field(form, 6),
            to_account: optional_field(form, 7),
            payee: optional_field(form, 8),
            note: optional_field(form, 9),
            clear_scenario: optional_field(form, 1).is_none(),
            clear_to_account: optional_field(form, 7).is_none(),
            clear_category: optional_field(form, 6).is_none(),
            clear_payee: optional_field(form, 8).is_none(),
            clear_note: optional_field(form, 9).is_none(),
        };
        self.db.edit_planning_item(&patch)?;
        Ok(FormOutcome::Refresh(format!(
            "Updated planning item {}.",
            id
        )))
    }

    fn save_new_planning_goal(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let goal = NewPlanningGoal {
            name: form_value(form, 0).trim().to_string(),
            kind: parse_planning_goal_kind(form_value(form, 1))?,
            account: form_value(form, 2).trim().to_string(),
            target_amount_cents: optional_field(form, 3)
                .map(|value| parse_amount_to_cents(&value))
                .transpose()?,
            minimum_balance_cents: optional_field(form, 4)
                .map(|value| parse_amount_to_cents(&value))
                .transpose()?,
            due_on: optional_field(form, 5)
                .map(|value| normalize_date_input(&value))
                .transpose()?,
        };
        let goal_id = self.db.add_planning_goal(&goal)?;
        Ok(FormOutcome::Refresh(format!("Created goal {}.", goal_id)))
    }

    fn save_planning_goal_edit(
        &mut self,
        id: i64,
        form: &FormState,
    ) -> Result<FormOutcome, AppError> {
        let patch = UpdatePlanningGoal {
            id,
            name: Some(form_value(form, 0).trim().to_string()),
            kind: Some(parse_planning_goal_kind(form_value(form, 1))?),
            account: Some(form_value(form, 2).trim().to_string()),
            target_amount_cents: optional_field(form, 3)
                .map(|value| parse_amount_to_cents(&value))
                .transpose()?,
            minimum_balance_cents: optional_field(form, 4)
                .map(|value| parse_amount_to_cents(&value))
                .transpose()?,
            due_on: optional_field(form, 5)
                .map(|value| normalize_date_input(&value))
                .transpose()?,
            clear_target_amount: optional_field(form, 3).is_none(),
            clear_minimum_balance: optional_field(form, 4).is_none(),
            clear_due_on: optional_field(form, 5).is_none(),
        };
        self.db.edit_planning_goal(&patch)?;
        Ok(FormOutcome::Refresh(format!("Updated goal {}.", id)))
    }

    fn save_new_planning_scenario(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let scenario_name = form_value(form, 0).trim().to_string();
        let scenario_id = self.db.add_planning_scenario(&NewPlanningScenario {
            name: scenario_name.clone(),
            note: optional_field(form, 1),
        })?;
        Ok(FormOutcome::Refresh(format!(
            "Created scenario {} (id {}).",
            scenario_name, scenario_id
        )))
    }

    fn save_planning_scenario_edit(
        &mut self,
        id: i64,
        form: &FormState,
    ) -> Result<FormOutcome, AppError> {
        let scenario_name = form_value(form, 0).trim().to_string();
        self.db.edit_planning_scenario(&UpdatePlanningScenario {
            id,
            name: Some(scenario_name.clone()),
            note: optional_field(form, 1),
            clear_note: optional_field(form, 1).is_none(),
        })?;
        Ok(FormOutcome::Refresh(format!(
            "Updated scenario {} (id {}).",
            scenario_name, id
        )))
    }

    fn save_transaction_filters(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        self.tx_filters = TransactionFilters {
            from: optional_field(form, 0)
                .map(|value| normalize_date_input(&value))
                .transpose()?,
            to: optional_field(form, 1)
                .map(|value| normalize_date_input(&value))
                .transpose()?,
            account: optional_field(form, 2),
            category: optional_field(form, 3),
            search: optional_field(form, 4),
            limit: parse_optional_limit(optional_field(form, 5), "LIMIT")?,
            include_deleted: parse_yes_no(form_value(form, 6), "INCLUDE DELETED")?,
        };
        Ok(FormOutcome::Refresh(format!(
            "Applied transaction filters. Loaded {} rows.",
            self.db.list_transactions(&self.tx_filters)?.len()
        )))
    }

    fn save_import_preview(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let plan = build_import_plan(form, true)?;
        let preview = self.db.import_csv_transactions(&plan)?;
        Ok(FormOutcome::OpenImportReview(
            CsvImportReviewState {
                plan,
                preview,
                active: 0,
            },
            String::from(
                "Import preview loaded. Review the rows and press Ctrl+S or F2 to confirm the real import.",
            ),
        ))
    }

    fn confirm_import_review(&mut self) -> Result<(), AppError> {
        let Some(review) = self.import_review.clone() else {
            return Ok(());
        };
        let mut plan = review.plan;
        plan.dry_run = false;
        let result = self.db.import_csv_transactions(&plan)?;
        self.import_review = None;
        self.refresh()?;
        self.status = format!(
            "Imported {} rows and skipped {} duplicates.",
            result.imported_count, result.duplicate_count
        );
        Ok(())
    }

    fn save_watchlist_add(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let ticker = form_value(form, 0).trim().to_string();
        let label = optional_field(form, 1);
        let watchlist_id =
            MarketService::from_env().add_watchlist(&self.db, &ticker, label.as_deref())?;
        Ok(FormOutcome::Refresh(format!(
            "Added {} to the watchlist as item {}.",
            ticker.to_ascii_uppercase(),
            watchlist_id
        )))
    }
    fn save_poly_watch_add(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let slug = form_value(form, 0).trim().to_string();
        let label = optional_field(form, 1);
        let watch_id = PolyService::from_env().add_watchlist(&self.db, &slug, label.as_deref())?;
        Ok(FormOutcome::Refresh(format!(
            "Added {} to the Polymarket watchlist as item {}.",
            slug.to_ascii_lowercase(),
            watch_id
        )))
    }
    fn save_poly_search(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let query = form_value(form, 0).trim().to_string();
        let limit = parse_optional_limit(Some(form_value(form, 1).trim().to_string()), "LIMIT")?
            .unwrap_or(8);
        let results = PolyService::from_env().search(&query, limit)?;
        let count = results.len();
        Ok(FormOutcome::OpenPolySearch(
            PolySearchState {
                query,
                limit,
                results,
                active: 0,
            },
            format!(
                "Loaded {} Polymarket result(s). Press Enter or A to add the selected market.",
                count
            ),
        ))
    }
    fn save_new_recurring_rule(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let rule = build_new_recurring_rule(form)?;
        let rule_id = self.db.add_recurring_rule(&rule)?;
        Ok(FormOutcome::Refresh(format!(
            "Created recurring rule {}.",
            rule_id
        )))
    }

    fn save_recurring_edit(&mut self, id: i64, form: &FormState) -> Result<FormOutcome, AppError> {
        let cadence = parse_recurring_cadence(form_value(form, 8))?;
        let day_of_month = parse_optional_u32(optional_field(form, 10))?;
        let weekday = parse_optional_weekday(optional_field(form, 11))?;
        let patch = UpdateRecurringRule {
            id,
            name: Some(form_value(form, 0).trim().to_string()),
            kind: Some(parse_transaction_kind(form_value(form, 1))?),
            amount_cents: Some(parse_amount_to_cents(form_value(form, 2))?),
            account: Some(form_value(form, 3).trim().to_string()),
            to_account: optional_field(form, 5),
            category: optional_field(form, 4),
            payee: optional_field(form, 6),
            note: optional_field(form, 7),
            cadence: Some(cadence),
            interval: Some(parse_positive_i64(form_value(form, 9), "INTERVAL")?),
            day_of_month,
            weekday,
            start_on: Some(normalize_date_input(form_value(form, 12))?),
            next_due_on: optional_field(form, 13)
                .map(|value| normalize_date_input(&value))
                .transpose()?,
            end_on: optional_field(form, 14)
                .map(|value| normalize_date_input(&value))
                .transpose()?,
            clear_to_account: optional_field(form, 5).is_none(),
            clear_category: optional_field(form, 4).is_none(),
            clear_payee: optional_field(form, 6).is_none(),
            clear_note: optional_field(form, 7).is_none(),
            clear_day_of_month: matches!(cadence, RecurringCadence::Weekly)
                || day_of_month.is_none(),
            clear_weekday: matches!(cadence, RecurringCadence::Monthly) || weekday.is_none(),
            clear_next_due_on: optional_field(form, 13).is_none(),
            clear_end_on: optional_field(form, 14).is_none(),
        };
        self.db.edit_recurring_rule(&patch)?;
        Ok(FormOutcome::Refresh(format!(
            "Updated recurring rule {}.",
            id
        )))
    }

    fn start_reconciliation_review(&mut self, form: &FormState) -> Result<FormOutcome, AppError> {
        let account_ref = form_value(form, 0).trim().to_string();
        let statement_ending_on = normalize_date_input(form_value(form, 1))?;
        let statement_balance_cents = parse_amount_to_cents(form_value(form, 2))?;
        let eligible = self
            .db
            .list_eligible_reconciliation_transactions(&account_ref, &statement_ending_on)?;
        if eligible.is_empty() {
            return Err(AppError::Validation(
                "no eligible transactions were found for that reconciliation window".to_string(),
            ));
        }
        let account = self.account_by_ref(&account_ref).ok_or_else(|| {
            AppError::Validation(
                "selected reconciliation account could not be resolved".to_string(),
            )
        })?;
        let flow = ReconcileSelectionState {
            account_id: account.id,
            account_name: account.name.clone(),
            statement_ending_on,
            statement_balance_cents,
            opening_balance_cents: account.opening_balance_cents,
            selected_ids: eligible.iter().map(|transaction| transaction.id).collect(),
            eligible_transactions: eligible,
            active: 0,
        };
        Ok(FormOutcome::OpenReconcile(
            flow,
            String::from(
                "Reconciliation review loaded. Space toggles rows and Ctrl+S or F2 saves.",
            ),
        ))
    }
    fn open_input_mode(&mut self) {
        self.input_mode = true;
        self.clear_form_state();
        self.input_buffer.clear();
        self.reconcile_flow = None;
        self.poly_search = None;
        self.show_help = false;
        self.status = String::from(
            "Command mode active. Type `help`, `balance`, or any full CLI command and press Enter.",
        );
    }

    fn open_input_mode_with_text(&mut self, text: String) {
        self.open_input_mode();
        self.input_buffer = text;
    }

    fn execute_input_command(&mut self) -> Result<(), AppError> {
        let command = self.input_buffer.trim().to_string();
        if command.is_empty() {
            self.status = String::from("Type a command first. `help` shows examples.");
            return Ok(());
        }

        let lowercase = command.to_ascii_lowercase();
        if matches!(lowercase.as_str(), "exit" | "quit" | "close") {
            self.input_mode = false;
            self.input_buffer.clear();
            self.status = String::from("Command mode closed. Press S to open it again.");
            return Ok(());
        }

        if lowercase == "clear" {
            self.command_log.clear();
            self.input_buffer.clear();
            self.status = String::from("Command log cleared.");
            return Ok(());
        }

        if lowercase == "help" {
            self.append_command_result(
                "help",
                "Shortcuts: income, expense, transfer, account, category, budget, balance, transactions, summary, recurring, reconcile, forecast, scenario, goal\nFull commands also work here, for example:\n  tx list --limit 10\n  balance\n  forecast show\n  scenario list\n  recurring list\n  import csv --input bank.csv --account Checking --date-column Date --amount-column Amount --description-column Description --category Groceries --dry-run",
            );
            self.input_buffer.clear();
            self.status = String::from("Examples added to the command log.");
            return Ok(());
        }

        if let Some(template) = command_template(&lowercase) {
            self.input_buffer = template;
            self.status = String::from(
                "Template loaded. Replace the placeholder values, then press Enter again.",
            );
            self.append_command_result(&command, "Template loaded into the input line.");
            return Ok(());
        }

        self.execute_command_text(&command)
    }

    fn execute_command_text(&mut self, command: &str) -> Result<(), AppError> {
        let tokens = match shlex::split(command) {
            Some(tokens) => tokens,
            None => {
                self.append_command_result(command, "ERROR: invalid quoting in command.");
                self.status = String::from("Command failed. Check your quotes and try again.");
                return Ok(());
            }
        };
        let tokens = normalize_input_command_tokens(tokens);

        if tokens.is_empty() {
            self.append_command_result(
                command,
                "ERROR: type a command such as `balance`, `tx list`, or `recurring list`.",
            );
            self.status = String::from("Write mode expects a command after the executable name.");
            return Ok(());
        }

        if tokens.first().map(String::as_str) == Some("shell") {
            self.append_command_result(
                command,
                "ERROR: `shell` is not available inside the TUI. Type commands directly here.",
            );
            self.status = String::from("Type commands directly into command mode.");
            return Ok(());
        }

        let mut argv = vec![
            "helius".to_string(),
            "--db".to_string(),
            self.db_path.display().to_string(),
        ];
        argv.extend(tokens);

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        match crate::run_app(argv, &mut stdout, &mut stderr) {
            Ok(()) => {
                let output = clean_output(&stdout);
                if output.trim().is_empty() {
                    self.append_command_result(command, "Command completed.");
                } else {
                    self.append_command_result(command, &output);
                }
                self.input_buffer.clear();
                self.status = String::from(
                    "Command completed. Keep working or press Esc to close the overlay.",
                );
                self.refresh()?;
            }
            Err(error) => {
                let error_text = if stderr.is_empty() {
                    error.to_string()
                } else {
                    clean_output(&stderr)
                };
                self.append_command_result(command, &format!("ERROR: {error_text}"));
                self.status = String::from("Command failed. Fix it and try again.");
            }
        }

        Ok(())
    }

    fn append_command_result(&mut self, command: &str, output: &str) {
        self.command_log.push(format!("> {command}"));
        for line in output.lines() {
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                self.command_log.push(String::new());
            } else {
                self.command_log.push(trimmed.to_string());
            }
        }
        if output.lines().next().is_none() {
            self.command_log.push(String::from("(no output)"));
        }
        if self.command_log.len() > MAX_COMMAND_LOG_LINES {
            let overflow = self.command_log.len() - MAX_COMMAND_LOG_LINES;
            self.command_log.drain(0..overflow);
        }
    }

    pub(super) fn command_bar_text(&self) -> String {
        if self.reconcile_flow.is_some() {
            String::from(
                "RECONCILE MODE | Space: toggle | A: all | C: clear | Ctrl+S/F2: save | Esc: cancel",
            )
        } else if self.import_review.is_some() {
            String::from("IMPORT PREVIEW | Up/Down: browse | Ctrl+S/F2: import | Esc: cancel")
        } else if self.form.is_some() {
            String::from(
                "FORM MODE | Type: replace field | Tab: next | Enter/Ctrl+S/F2: save | Esc: cancel",
            )
        } else if self.input_mode {
            String::from(
                "COMMAND MODE | Enter: run | Esc: close | Ctrl+U: clear input | help: examples",
            )
        } else {
            format!("{} | {}", self.view_action_text(), self.status)
        }
    }

    pub(super) fn current_view(&self) -> View {
        View::all()[self.current_view]
    }

    pub(super) fn view_action_text(&self) -> String {
        match self.current_view() {
            View::Dashboard => String::from("N new transaction | S commands | ?: help | Q quit"),
            View::Transactions => String::from(
                "N new | E edit | F or / filter | C clear | I import CSV | D delete/restore | S commands",
            ),
            View::Accounts => String::from(
                "N new | E edit | D archive unused | I import CSV | R reconcile selected | S commands",
            ),
            View::Categories => String::from("N new | E edit | D archive | S commands"),
            View::Summary => String::from("N set budget | charts are read-only here"),
            View::Budgets => String::from("N new budget | E edit budget | D delete/reset | S commands"),
            View::Planning => String::from(
                "Left/Right subview | N add | E edit | D archive | Enter select/post | R refresh",
            ),
            View::Markets => String::from(
                "N add ticker | Enter/R refresh | A refresh all | D remove | S commands",
            ),
            View::Poly => String::from(
                "N add slug | F or / search | Enter/R refresh | A refresh all | M movers | D remove | S commands",
            ),
            View::Recurring => {
                String::from("N new | E edit | P pause/resume | D delete | G post due | S commands")
            }
            View::Reconcile => {
                String::from("R/N start reconcile | D remove record | selected detail shown below")
            }
        }
    }

    pub(super) fn transaction_filters_active(&self) -> bool {
        self.tx_filters != default_transaction_filters()
    }
}

fn parse_optional_limit(raw: Option<String>, label: &str) -> Result<Option<usize>, AppError> {
    match raw {
        Some(value) => {
            let parsed = value.trim().parse::<usize>().map_err(|_| {
                AppError::Validation(format!("{} must be a whole positive number", label))
            })?;
            if parsed == 0 {
                return Err(AppError::Validation(format!(
                    "{} must be a whole positive number",
                    label
                )));
            }
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
}

fn default_transaction_filters() -> TransactionFilters {
    TransactionFilters {
        from: None,
        to: None,
        account: None,
        category: None,
        search: None,
        limit: Some(50),
        include_deleted: false,
    }
}
fn build_import_plan(form: &FormState, dry_run: bool) -> Result<CsvImportPlan, AppError> {
    let path_text = form_value(form, 0).trim();
    if path_text.is_empty() {
        return Err(AppError::Validation("FILE is required".to_string()));
    }
    let delimiter = b',';
    Ok(CsvImportPlan {
        path: PathBuf::from(path_text),
        account: form_value(form, 1).trim().to_string(),
        date_column: form_value(form, 2).trim().to_string(),
        amount_column: form_value(form, 3).trim().to_string(),
        description_column: form_value(form, 4).trim().to_string(),
        category_column: optional_field(form, 5),
        category: optional_field(form, 6),
        payee_column: None,
        note_column: None,
        type_column: optional_field(form, 7),
        default_kind: parse_optional_default_kind(optional_field(form, 8))?,
        date_format: form_value(form, 9).trim().to_string(),
        delimiter,
        dry_run,
        allow_duplicates: parse_yes_no(form_value(form, 10), "ALLOW DUPES")?,
    })
}
fn build_new_recurring_rule(form: &FormState) -> Result<NewRecurringRule, AppError> {
    let cadence = parse_recurring_cadence(form_value(form, 8))?;
    Ok(NewRecurringRule {
        name: form_value(form, 0).trim().to_string(),
        kind: parse_transaction_kind(form_value(form, 1))?,
        amount_cents: parse_amount_to_cents(form_value(form, 2))?,
        account: form_value(form, 3).trim().to_string(),
        category: optional_field(form, 4),
        to_account: optional_field(form, 5),
        payee: optional_field(form, 6),
        note: optional_field(form, 7),
        cadence,
        interval: parse_positive_i64(form_value(form, 9), "INTERVAL")?,
        day_of_month: match cadence {
            RecurringCadence::Monthly => parse_optional_u32(optional_field(form, 10))?,
            RecurringCadence::Weekly => None,
        },
        weekday: match cadence {
            RecurringCadence::Weekly => parse_optional_weekday(optional_field(form, 11))?,
            RecurringCadence::Monthly => None,
        },
        start_on: normalize_date_input(form_value(form, 12))?,
        next_due_on: optional_field(form, 13)
            .map(|value| normalize_date_input(&value))
            .transpose()?,
        end_on: optional_field(form, 14)
            .map(|value| normalize_date_input(&value))
            .transpose()?,
    })
}

fn form_value(form: &FormState, index: usize) -> &str {
    form.fields[index].value.as_str()
}

fn optional_field(form: &FormState, index: usize) -> Option<String> {
    let trimmed = form.fields[index].value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_date_input(raw: &str) -> Result<String, AppError> {
    let parsed = NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")?;
    Ok(parsed.format("%Y-%m-%d").to_string())
}

fn parse_positive_i64(raw: &str, label: &str) -> Result<i64, AppError> {
    let parsed = raw
        .trim()
        .parse::<i64>()
        .map_err(|_| AppError::Validation(format!("{} must be a whole positive number", label)))?;
    if parsed <= 0 {
        return Err(AppError::Validation(format!(
            "{} must be a whole positive number",
            label
        )));
    }
    Ok(parsed)
}

fn parse_optional_u32(raw: Option<String>) -> Result<Option<u32>, AppError> {
    match raw {
        Some(value) => value.trim().parse::<u32>().map(Some).map_err(|_| {
            AppError::Validation("DAY OF MONTH must be a number between 1 and 28".to_string())
        }),
        None => Ok(None),
    }
}

fn parse_optional_default_kind(raw: Option<String>) -> Result<Option<TransactionKind>, AppError> {
    match raw {
        Some(value) => parse_transaction_kind(&value).map(Some),
        None => Ok(None),
    }
}

fn parse_yes_no(raw: &str, label: &str) -> Result<bool, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "yes" | "y" | "true" | "1" => Ok(true),
        "no" | "n" | "false" | "0" => Ok(false),
        _ => Err(AppError::Validation(format!("{} must be yes or no", label))),
    }
}
fn parse_transaction_kind(raw: &str) -> Result<TransactionKind, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "income" => Ok(TransactionKind::Income),
        "expense" => Ok(TransactionKind::Expense),
        "transfer" => Ok(TransactionKind::Transfer),
        _ => Err(AppError::Validation(
            "TYPE must be income, expense, or transfer".to_string(),
        )),
    }
}

fn parse_account_kind(raw: &str) -> Result<crate::model::AccountKind, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "cash" => Ok(crate::model::AccountKind::Cash),
        "checking" => Ok(crate::model::AccountKind::Checking),
        "savings" => Ok(crate::model::AccountKind::Savings),
        "credit" => Ok(crate::model::AccountKind::Credit),
        _ => Err(AppError::Validation(
            "TYPE must be cash, checking, savings, or credit".to_string(),
        )),
    }
}

fn parse_category_kind(raw: &str) -> Result<crate::model::CategoryKind, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "income" => Ok(crate::model::CategoryKind::Income),
        "expense" => Ok(crate::model::CategoryKind::Expense),
        _ => Err(AppError::Validation(
            "KIND must be income or expense".to_string(),
        )),
    }
}

fn parse_planning_goal_kind(raw: &str) -> Result<PlanningGoalKind, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "sinking_fund" | "sinking fund" => Ok(PlanningGoalKind::SinkingFund),
        "balance_target" | "balance target" => Ok(PlanningGoalKind::BalanceTarget),
        _ => Err(AppError::Validation(
            "KIND must be balance_target or sinking_fund".to_string(),
        )),
    }
}

fn parse_recurring_cadence(raw: &str) -> Result<RecurringCadence, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "weekly" => Ok(RecurringCadence::Weekly),
        "monthly" => Ok(RecurringCadence::Monthly),
        _ => Err(AppError::Validation(
            "CADENCE must be weekly or monthly".to_string(),
        )),
    }
}

fn parse_optional_weekday(raw: Option<String>) -> Result<Option<Weekday>, AppError> {
    match raw {
        Some(value) => parse_weekday(&value).map(Some),
        None => Ok(None),
    }
}

fn parse_weekday(raw: &str) -> Result<Weekday, AppError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Ok(Weekday::Mon),
        "tue" | "tues" | "tuesday" => Ok(Weekday::Tue),
        "wed" | "wednesday" => Ok(Weekday::Wed),
        "thu" | "thur" | "thurs" | "thursday" => Ok(Weekday::Thu),
        "fri" | "friday" => Ok(Weekday::Fri),
        "sat" | "saturday" => Ok(Weekday::Sat),
        "sun" | "sunday" => Ok(Weekday::Sun),
        _ => Err(AppError::Validation(
            "WEEKDAY must be mon, tue, wed, thu, fri, sat, or sun".to_string(),
        )),
    }
}

fn apply_form_text_input(value: &mut String, replace_on_input: &mut bool, ch: char) {
    if *replace_on_input {
        value.clear();
        *replace_on_input = false;
    }
    value.push(ch);
}

fn apply_form_backspace(value: &mut String, replace_on_input: &mut bool) {
    if *replace_on_input {
        value.clear();
        *replace_on_input = false;
    } else {
        value.pop();
    }
}

fn should_handle_key_event(key: KeyEvent) -> bool {
    !matches!(key.kind, KeyEventKind::Release)
}

fn normalize_input_command_tokens(mut tokens: Vec<String>) -> Vec<String> {
    if tokens
        .first()
        .map(|token| is_helius_program_token(token))
        .unwrap_or(false)
    {
        tokens.remove(0);
    }
    tokens
}

fn is_helius_program_token(token: &str) -> bool {
    let file_name = Path::new(token)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(token)
        .to_ascii_lowercase();
    file_name == "helius" || file_name == "helius.exe"
}

fn is_form_submit_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::F(2))
        || matches!(
            key.code,
            KeyCode::Char(ch)
                if key.modifiers.contains(KeyModifiers::CONTROL)
                    && ch.eq_ignore_ascii_case(&'s')
        )
}

fn empty_forecast() -> ForecastSnapshot {
    ForecastSnapshot {
        scenario: crate::model::ForecastSelection {
            id: None,
            name: Some("baseline".to_string()),
        },
        as_of: today_iso(),
        account: crate::model::ForecastSelection {
            id: None,
            name: None,
        },
        warnings: Vec::new(),
        alerts: Vec::new(),
        daily: Vec::new(),
        monthly: Vec::new(),
        goal_status: Vec::new(),
        bill_calendar: Vec::new(),
    }
}

fn format_money_input(amount_cents: i64) -> String {
    crate::amount::format_cents(amount_cents)
}

fn transaction_effect_for_account(account_id: i64, transaction: &TransactionRecord) -> i64 {
    match transaction.kind {
        TransactionKind::Income if transaction.account_id == account_id => transaction.amount_cents,
        TransactionKind::Expense if transaction.account_id == account_id => {
            -transaction.amount_cents
        }
        TransactionKind::Transfer if transaction.account_id == account_id => {
            -transaction.amount_cents
        }
        TransactionKind::Transfer if transaction.to_account_id == Some(account_id) => {
            transaction.amount_cents
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_form_backspace, apply_form_text_input, build_new_recurring_rule, is_form_submit_key,
        normalize_input_command_tokens, should_handle_key_event, FormField, FormKind, FormState,
        View,
    };
    use crate::model::RecurringCadence;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

    #[test]
    fn accepts_ctrl_s_as_form_submit_key() {
        assert!(is_form_submit_key(KeyEvent::new(
            KeyCode::Char('s'),
            KeyModifiers::CONTROL,
        )));
    }

    #[test]
    fn accepts_f2_as_form_submit_key() {
        assert!(is_form_submit_key(KeyEvent::new(
            KeyCode::F(2),
            KeyModifiers::NONE,
        )));
    }

    #[test]
    fn handles_press_and_repeat_key_events() {
        let mut press = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        press.kind = KeyEventKind::Press;
        assert!(should_handle_key_event(press));

        let mut repeat = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        repeat.kind = KeyEventKind::Repeat;
        assert!(should_handle_key_event(repeat));

        let mut release = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert!(!should_handle_key_event(release));
    }

    #[test]
    fn first_form_input_replaces_prefilled_value() {
        let mut value = String::from("0.00");
        let mut replace_on_input = true;
        apply_form_text_input(&mut value, &mut replace_on_input, '5');
        apply_form_text_input(&mut value, &mut replace_on_input, '0');
        assert_eq!(value, "50");
    }

    #[test]
    fn backspace_clears_prefilled_value_before_editing() {
        let mut value = String::from("Monthly Rent");
        let mut replace_on_input = true;
        apply_form_backspace(&mut value, &mut replace_on_input);
        assert!(value.is_empty());
        assert!(!replace_on_input);
    }

    #[test]
    fn recurring_add_ignores_day_of_month_for_weekly_rules() {
        let form = FormState {
            kind: FormKind::RecurringAdd,
            title: "NEW RECURRING RULE",
            hint: "",
            fields: vec![
                FormField {
                    label: "NAME",
                    value: "Weekly Rent".to_string(),
                    required: true,
                },
                FormField {
                    label: "TYPE",
                    value: "expense".to_string(),
                    required: true,
                },
                FormField {
                    label: "AMOUNT",
                    value: "50.00".to_string(),
                    required: true,
                },
                FormField {
                    label: "ACCOUNT",
                    value: "Checking".to_string(),
                    required: true,
                },
                FormField {
                    label: "CATEGORY",
                    value: "Rent".to_string(),
                    required: false,
                },
                FormField {
                    label: "TO ACCOUNT",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "PAYEE",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "NOTE",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "CADENCE",
                    value: "weekly".to_string(),
                    required: true,
                },
                FormField {
                    label: "INTERVAL",
                    value: "1".to_string(),
                    required: true,
                },
                FormField {
                    label: "DAY OF MONTH",
                    value: "1".to_string(),
                    required: false,
                },
                FormField {
                    label: "WEEKDAY",
                    value: "mon".to_string(),
                    required: false,
                },
                FormField {
                    label: "START ON",
                    value: "2026-03-16".to_string(),
                    required: true,
                },
                FormField {
                    label: "NEXT DUE ON",
                    value: String::new(),
                    required: false,
                },
                FormField {
                    label: "END ON",
                    value: String::new(),
                    required: false,
                },
            ],
            active: 0,
        };

        let rule = build_new_recurring_rule(&form).expect("weekly rule should build");
        assert!(matches!(rule.cadence, RecurringCadence::Weekly));
        assert_eq!(rule.day_of_month, None);
        assert!(rule.weekday.is_some());
    }

    #[test]
    fn t_ui_navigation_excludes_market_and_poly_tabs() {
        let labels: Vec<&str> = View::all().iter().map(|view| view.label()).collect();
        assert!(!labels.contains(&"MARKETS"));
        assert!(!labels.contains(&"POLY"));
    }

    #[test]
    fn strips_optional_helius_prefix_from_write_mode_commands() {
        assert_eq!(
            normalize_input_command_tokens(vec![
                "helius".to_string(),
                "balance".to_string(),
                "--json".to_string()
            ]),
            vec!["balance".to_string(), "--json".to_string()]
        );
        assert_eq!(
            normalize_input_command_tokens(vec![
                r"C:\Tools\helius.exe".to_string(),
                "recurring".to_string(),
                "list".to_string()
            ]),
            vec!["recurring".to_string(), "list".to_string()]
        );
    }
}
