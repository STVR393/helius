use std::fs;
use std::io::Write;
use std::path::Path;

use comfy_table::{presets::UTF8_BORDERS_ONLY, Cell, ContentArrangement, Table};
use serde::Serialize;

use crate::amount::format_cents;
use crate::error::AppError;
use crate::model::{
    Account, BalanceRecord, BillCalendarItem, BudgetRecord, BudgetStatusRecord, Category,
    CsvImportResult, ForecastSnapshot, GoalStatusRecord, ImportedTransactionRow, MarketBrief,
    MarketNewsFeed, MarketQuote, MarketRefreshSummary, MarketSettings, PlanningGoalRecord,
    PlanningItemRecord, PlanningScenarioRecord, PolyBrief, PolyMarket, PolyRefreshSummary,
    PolySearchResult, PolyWatchEntry, ReconciliationRecord, RecurringRuleRecord, SummaryRecord,
    TransactionRecord, WatchlistEntry,
};
use crate::theme::{paint, Tone};

pub fn success_text(message: &str) -> String {
    paint(message, Tone::Positive)
}

pub fn warning_text(message: &str) -> String {
    paint(message, Tone::Warning)
}

pub fn error_text(message: &str) -> String {
    paint(message, Tone::Negative)
}

pub fn write_accounts(
    writer: &mut dyn Write,
    accounts: &[Account],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, accounts);
    }

    let mut table = new_table();
    table.set_header(header_row(["ID", "Name", "Type", "Opening", "Opened"]));
    for account in accounts {
        table.add_row([
            Cell::new(account.id),
            Cell::new(&account.name),
            Cell::new(account.kind.as_db_str()),
            Cell::new(format_cents(account.opening_balance_cents)),
            Cell::new(&account.opened_on),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_categories(
    writer: &mut dyn Write,
    categories: &[Category],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, categories);
    }

    let mut table = new_table();
    table.set_header(header_row(["ID", "Name", "Kind"]));
    for category in categories {
        table.add_row([
            Cell::new(category.id),
            Cell::new(&category.name),
            Cell::new(category.kind.as_db_str()),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_transactions(
    writer: &mut dyn Write,
    transactions: &[TransactionRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, transactions);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Date", "Type", "Amount", "Account", "To", "Category", "Payee", "Note", "Status",
    ]));
    for transaction in transactions {
        table.add_row([
            Cell::new(transaction.id),
            Cell::new(&transaction.txn_date),
            Cell::new(transaction.kind.as_db_str()),
            Cell::new(money_text(transaction.amount_cents)),
            Cell::new(&transaction.account_name),
            Cell::new(transaction.to_account_name.as_deref().unwrap_or("-")),
            Cell::new(transaction.category_name.as_deref().unwrap_or("-")),
            Cell::new(transaction.payee.as_deref().unwrap_or("-")),
            Cell::new(transaction.note.as_deref().unwrap_or("-")),
            Cell::new(transaction_status(transaction)),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_balances(
    writer: &mut dyn Write,
    balances: &[BalanceRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, balances);
    }

    let mut table = new_table();
    table.set_header(header_row(["Account", "Type", "Balance"]));
    for balance in balances {
        table.add_row([
            Cell::new(&balance.account_name),
            Cell::new(balance.account_kind.as_db_str()),
            Cell::new(money_text(balance.current_balance_cents)),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_summary(
    writer: &mut dyn Write,
    summary: &SummaryRecord,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, summary);
    }

    let mut table = new_table();
    table.set_header(header_row(["Metric", "Value"]));
    table.add_row([Cell::new(label_text("From")), Cell::new(&summary.from)]);
    table.add_row([Cell::new(label_text("To")), Cell::new(&summary.to)]);
    table.add_row([
        Cell::new(label_text("Account")),
        Cell::new(summary.account_name.as_deref().unwrap_or("all accounts")),
    ]);
    table.add_row([
        Cell::new(label_text("Transactions")),
        Cell::new(summary.transaction_count),
    ]);
    table.add_row([
        Cell::new(label_text("Income")),
        Cell::new(money_text(summary.income_cents)),
    ]);
    table.add_row([
        Cell::new(label_text("Expense")),
        Cell::new(money_text(-summary.expense_cents)),
    ]);
    table.add_row([
        Cell::new(label_text("Net")),
        Cell::new(money_text(summary.net_cents)),
    ]);
    table.add_row([
        Cell::new(label_text("Transfers In")),
        Cell::new(money_text(summary.transfer_in_cents)),
    ]);
    table.add_row([
        Cell::new(label_text("Transfers Out")),
        Cell::new(money_text(-summary.transfer_out_cents)),
    ]);

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_reconciliations(
    writer: &mut dyn Write,
    reconciliations: &[ReconciliationRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, reconciliations);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID",
        "Account",
        "Ending",
        "Statement",
        "Cleared",
        "Txns",
        "Created",
    ]));
    for reconciliation in reconciliations {
        table.add_row([
            Cell::new(reconciliation.id),
            Cell::new(&reconciliation.account_name),
            Cell::new(&reconciliation.statement_ending_on),
            Cell::new(money_text(reconciliation.statement_balance_cents)),
            Cell::new(money_text(reconciliation.cleared_balance_cents)),
            Cell::new(reconciliation.transaction_count),
            Cell::new(&reconciliation.created_at),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_recurring_rules(
    writer: &mut dyn Write,
    rules: &[RecurringRuleRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, rules);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Name", "Type", "Amount", "Account", "Target", "Category", "Schedule", "Next Due",
        "State",
    ]));
    for rule in rules {
        table.add_row([
            Cell::new(rule.id),
            Cell::new(&rule.name),
            Cell::new(rule.kind.as_db_str()),
            Cell::new(money_text(rule.amount_cents)),
            Cell::new(&rule.account_name),
            Cell::new(rule.to_account_name.as_deref().unwrap_or("-")),
            Cell::new(rule.category_name.as_deref().unwrap_or("-")),
            Cell::new(schedule_text(rule)),
            Cell::new(&rule.next_due_on),
            Cell::new(if rule.paused {
                warning_text("paused")
            } else {
                success_text("active")
            }),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_budgets(
    writer: &mut dyn Write,
    budgets: &[BudgetRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, budgets);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Month", "Category", "Account", "Scenario", "Mode", "Amount",
    ]));
    for budget in budgets {
        table.add_row([
            Cell::new(budget.id),
            Cell::new(&budget.month),
            Cell::new(&budget.category_name),
            Cell::new(budget.account_name.as_deref().unwrap_or("-")),
            Cell::new(budget.scenario_name.as_deref().unwrap_or("baseline")),
            Cell::new(budget_mode_label(budget.scenario_id, budget.is_override)),
            Cell::new(money_text(budget.amount_cents)),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_budget_status(
    writer: &mut dyn Write,
    status_rows: &[BudgetStatusRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, status_rows);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Month",
        "Category",
        "Account",
        "Scenario",
        "Mode",
        "Budget",
        "Spent",
        "Remaining",
        "State",
    ]));
    for row in status_rows {
        table.add_row([
            Cell::new(&row.month),
            Cell::new(&row.category_name),
            Cell::new(row.account_name.as_deref().unwrap_or("-")),
            Cell::new(row.scenario_name.as_deref().unwrap_or("baseline")),
            Cell::new(budget_mode_label(row.scenario_id, row.is_override)),
            Cell::new(money_text(row.budget_cents)),
            Cell::new(money_text(-row.spent_cents)),
            Cell::new(money_text(row.remaining_cents)),
            Cell::new(if row.over_budget {
                error_text("over")
            } else if row.budget_cents == 0 && row.spent_cents > 0 {
                warning_text("unbudgeted")
            } else {
                success_text("ok")
            }),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_planning_items(
    writer: &mut dyn Write,
    items: &[PlanningItemRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, items);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Date", "Title", "Type", "Amount", "Account", "Scenario", "Status",
    ]));
    for item in items {
        table.add_row([
            Cell::new(item.id),
            Cell::new(&item.due_on),
            Cell::new(&item.title),
            Cell::new(item.kind.as_db_str()),
            Cell::new(money_text(item.amount_cents)),
            Cell::new(&item.account_name),
            Cell::new(item.scenario_name.as_deref().unwrap_or("baseline")),
            Cell::new(if item.linked_transaction_id.is_some() {
                success_text("posted")
            } else {
                warning_text("planned")
            }),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_planning_scenarios(
    writer: &mut dyn Write,
    scenarios: &[PlanningScenarioRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, scenarios);
    }

    let mut table = new_table();
    table.set_header(header_row(["ID", "Name", "Note", "Updated"]));
    for scenario in scenarios {
        table.add_row([
            Cell::new(scenario.id),
            Cell::new(&scenario.name),
            Cell::new(scenario.note.as_deref().unwrap_or("-")),
            Cell::new(&scenario.updated_at),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_planning_goals(
    writer: &mut dyn Write,
    goals: &[PlanningGoalRecord],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, goals);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Name", "Kind", "Account", "Target", "Floor", "Due",
    ]));
    for goal in goals {
        table.add_row([
            Cell::new(goal.id),
            Cell::new(&goal.name),
            Cell::new(goal.kind.as_db_str()),
            Cell::new(&goal.account_name),
            Cell::new(
                goal.target_amount_cents
                    .map(money_text)
                    .unwrap_or_else(|| "-".to_string()),
            ),
            Cell::new(
                goal.minimum_balance_cents
                    .map(money_text)
                    .unwrap_or_else(|| "-".to_string()),
            ),
            Cell::new(goal.due_on.as_deref().unwrap_or("-")),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_bill_calendar(
    writer: &mut dyn Write,
    items: &[BillCalendarItem],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, items);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Date", "Title", "Source", "Account", "Category", "Amount", "Status",
    ]));
    for item in items {
        table.add_row([
            Cell::new(&item.date),
            Cell::new(&item.title),
            Cell::new(&item.source),
            Cell::new(&item.account_name),
            Cell::new(item.category_name.as_deref().unwrap_or("-")),
            Cell::new(money_text(item.amount_cents)),
            Cell::new(if item.linked_transaction_id.is_some() {
                success_text("posted")
            } else {
                warning_text("upcoming")
            }),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_forecast(
    writer: &mut dyn Write,
    snapshot: &ForecastSnapshot,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, snapshot);
    }

    let mut summary = new_table();
    summary.set_header(header_row(["Field", "Value"]));
    summary.add_row([Cell::new(label_text("As Of")), Cell::new(&snapshot.as_of)]);
    summary.add_row([
        Cell::new(label_text("Scenario")),
        Cell::new(snapshot.scenario.name.as_deref().unwrap_or("baseline")),
    ]);
    summary.add_row([
        Cell::new(label_text("Account")),
        Cell::new(snapshot.account.name.as_deref().unwrap_or("all accounts")),
    ]);
    summary.add_row([
        Cell::new(label_text("Warnings")),
        Cell::new(snapshot.warnings.len()),
    ]);
    summary.add_row([
        Cell::new(label_text("Alerts")),
        Cell::new(snapshot.alerts.len()),
    ]);
    if let Some(first_warning) = snapshot.warnings.first() {
        summary.add_row([
            Cell::new(label_text("First Warning")),
            Cell::new(first_warning),
        ]);
    }
    if let Some(first_alert) = snapshot.alerts.first() {
        summary.add_row([Cell::new(label_text("First Alert")), Cell::new(first_alert)]);
    }

    let mut daily = new_table();
    daily.set_header(header_row([
        "Date", "Open", "In", "Out", "Net", "Close", "Alerts",
    ]));
    for point in &snapshot.daily {
        daily.add_row([
            Cell::new(&point.date),
            Cell::new(money_text(point.opening_balance_cents)),
            Cell::new(money_text(point.inflow_cents)),
            Cell::new(money_text(-point.outflow_cents)),
            Cell::new(money_text(point.net_cents)),
            Cell::new(money_text(point.closing_balance_cents)),
            Cell::new(if point.alerts.is_empty() {
                "-".to_string()
            } else {
                point.alerts.join(" | ")
            }),
        ]);
    }

    let mut monthly = new_table();
    monthly.set_header(header_row(["Month", "In", "Out", "Net", "Ending"]));
    for point in &snapshot.monthly {
        monthly.add_row([
            Cell::new(&point.month),
            Cell::new(money_text(point.inflow_cents)),
            Cell::new(money_text(-point.outflow_cents)),
            Cell::new(money_text(point.net_cents)),
            Cell::new(money_text(point.ending_balance_cents)),
        ]);
    }

    let mut goals = new_table();
    goals.set_header(header_row([
        "Goal",
        "Kind",
        "Account",
        "Current",
        "Projected",
        "Remaining",
        "Suggested",
        "State",
    ]));
    for goal in &snapshot.goal_status {
        goals.add_row(goal_status_row(goal));
    }

    writeln!(writer, "{}", paint("FORECAST", Tone::Header))?;
    writeln!(writer, "{summary}")?;
    writeln!(writer)?;
    writeln!(writer, "{}", paint("DAILY", Tone::Header))?;
    writeln!(writer, "{daily}")?;
    writeln!(writer)?;
    writeln!(writer, "{}", paint("MONTHLY", Tone::Header))?;
    writeln!(writer, "{monthly}")?;
    if !snapshot.goal_status.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "{}", paint("GOALS", Tone::Header))?;
        writeln!(writer, "{goals}")?;
    }
    if !snapshot.bill_calendar.is_empty() {
        writeln!(writer)?;
        writeln!(writer, "{}", paint("BILLS", Tone::Header))?;
        write_bill_calendar(writer, &snapshot.bill_calendar, false)?;
    }
    Ok(())
}

pub fn write_csv_import_result(
    writer: &mut dyn Write,
    result: &CsvImportResult,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, result);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Line", "Date", "Type", "Amount", "Category", "Payee", "Status",
    ]));
    for row in &result.preview {
        table.add_row(import_preview_row(row));
    }

    writeln!(writer, "{table}")?;
    writeln!(
        writer,
        "{}",
        if result.dry_run {
            warning_text(&format!(
                "Dry run: {} ready, {} duplicates.",
                result.imported_count, result.duplicate_count
            ))
        } else {
            success_text(&format!(
                "Imported {} rows, skipped {} duplicates.",
                result.imported_count, result.duplicate_count
            ))
        }
    )?;
    Ok(())
}

pub fn write_watchlist(
    writer: &mut dyn Write,
    watchlist: &[WatchlistEntry],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, watchlist);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Ticker", "Label", "Price", "State", "Fetched",
    ]));
    for entry in watchlist {
        table.add_row([
            Cell::new(entry.id),
            Cell::new(&entry.ticker),
            Cell::new(entry.label.as_deref().unwrap_or("-")),
            Cell::new(
                entry
                    .quote
                    .as_ref()
                    .map(market_price_text)
                    .unwrap_or_else(|| warning_text("missing")),
            ),
            Cell::new(
                entry
                    .quote
                    .as_ref()
                    .map(market_quote_state)
                    .unwrap_or_else(|| warning_text("stale")),
            ),
            Cell::new(
                entry
                    .quote
                    .as_ref()
                    .map(|quote| quote.fetched_at.clone())
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_market_settings(
    writer: &mut dyn Write,
    settings: &MarketSettings,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, settings);
    }

    let mut table = new_table();
    table.set_header(header_row(["Setting", "Value"]));
    table.add_row([
        Cell::new(label_text("Quote Refresh")),
        Cell::new(format!("every {}h", settings.quote_refresh_hours)),
    ]);
    table.add_row([
        Cell::new(label_text("News Refresh")),
        Cell::new(format!("every {}h", settings.news_refresh_hours)),
    ]);
    table.add_row([
        Cell::new(label_text("Auto Quotes")),
        Cell::new(on_off_text(settings.auto_refresh_quotes)),
    ]);
    table.add_row([
        Cell::new(label_text("Auto News")),
        Cell::new(on_off_text(settings.auto_refresh_news)),
    ]);
    table.add_row([
        Cell::new(label_text("Weekday Only")),
        Cell::new(on_off_text(settings.weekday_only)),
    ]);
    table.add_row([
        Cell::new(label_text("Max Quote Cost")),
        Cell::new(format!("${:.3}", settings.max_quote_cost_usd)),
    ]);
    table.add_row([
        Cell::new(label_text("Updated")),
        Cell::new(&settings.updated_at),
    ]);
    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_market_refresh_summary(
    writer: &mut dyn Write,
    summary: &MarketRefreshSummary,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, summary);
    }

    let mut table = new_table();
    table.set_header(header_row(["Metric", "Value"]));
    table.add_row([
        Cell::new(label_text("Requested")),
        Cell::new(summary.requested_kind.as_label()),
    ]);
    table.add_row([
        Cell::new(label_text("Tickers Considered")),
        Cell::new(summary.tickers_considered),
    ]);
    table.add_row([
        Cell::new(label_text("Quotes Refreshed")),
        Cell::new(summary.quote_refreshed),
    ]);
    table.add_row([
        Cell::new(label_text("News Refreshed")),
        Cell::new(summary.news_refreshed),
    ]);
    table.add_row([
        Cell::new(label_text("Quotes Not Due")),
        Cell::new(summary.quote_skipped_not_due),
    ]);
    table.add_row([
        Cell::new(label_text("News Not Due")),
        Cell::new(summary.news_skipped_not_due),
    ]);
    table.add_row([
        Cell::new(label_text("Quotes Disabled")),
        Cell::new(summary.quote_skipped_disabled),
    ]);
    table.add_row([
        Cell::new(label_text("News Disabled")),
        Cell::new(summary.news_skipped_disabled),
    ]);
    table.add_row([
        Cell::new(label_text("Cost Cap Skips")),
        Cell::new(summary.quote_skipped_cost_cap),
    ]);
    table.add_row([
        Cell::new(label_text("Failures")),
        Cell::new(summary.failure_count),
    ]);
    table.add_row([
        Cell::new(label_text("Weekend Blocked")),
        Cell::new(on_off_text(summary.weekend_blocked)),
    ]);
    table.add_row([
        Cell::new(label_text("Estimated Cost")),
        Cell::new(format!("${:.3}", summary.estimated_cost_usd)),
    ]);
    table.add_row([Cell::new(label_text("Ran At")), Cell::new(&summary.ran_at)]);
    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_market_quote(
    writer: &mut dyn Write,
    quote: &MarketQuote,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, quote);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Ticker", "Name", "Exchange", "Price", "Change", "State", "Fetched",
    ]));
    table.add_row([
        Cell::new(&quote.ticker),
        Cell::new(&quote.name),
        Cell::new(quote.exchange.as_deref().unwrap_or("-")),
        Cell::new(market_price_text(quote)),
        Cell::new(market_change_text(quote)),
        Cell::new(market_quote_state(quote)),
        Cell::new(&quote.fetched_at),
    ]);
    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_market_brief(
    writer: &mut dyn Write,
    brief: &MarketBrief,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, brief);
    }

    writeln!(
        writer,
        "{} {}",
        label_text("BRIEF"),
        paint(&brief.ticker, Tone::Header)
    )?;
    writeln!(
        writer,
        "{} {}",
        label_text("Tone:"),
        paint(&brief.tone.to_ascii_uppercase(), Tone::Info)
    )?;
    writeln!(writer, "{} {}", label_text("Summary:"), brief.summary)?;
    writeln!(
        writer,
        "{} {}",
        label_text("Sources:"),
        format!(
            "{} article(s) from {} source(s)",
            brief.headline_count, brief.source_count
        )
    )?;
    writeln!(writer, "{} {}", label_text("Fetched:"), &brief.fetched_at)?;
    if brief.stale {
        writeln!(
            writer,
            "{}",
            warning_text("Using stale cached brief input.")
        )?;
    }
    writeln!(writer)?;
    writeln!(writer, "{}", label_text("Why It Matters"))?;
    for item in &brief.why_it_matters {
        writeln!(writer, "- {item}")?;
    }
    writeln!(writer)?;
    writeln!(writer, "{}", label_text("Watch Next"))?;
    for item in &brief.watch_items {
        writeln!(writer, "- {item}")?;
    }
    Ok(())
}
pub fn write_market_news(
    writer: &mut dyn Write,
    feed: &MarketNewsFeed,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, feed);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Ticker",
        "Source",
        "Published",
        "Headline",
        "Url",
    ]));
    for item in &feed.items {
        table.add_row([
            Cell::new(&item.ticker),
            Cell::new(&item.source),
            Cell::new(item.published_at.as_deref().unwrap_or("-")),
            Cell::new(&item.title),
            Cell::new(&item.url),
        ]);
    }

    writeln!(writer, "{table}")?;
    writeln!(
        writer,
        "{}",
        if feed.stale {
            warning_text(&format!(
                "Showing stale cached news for {} from {}.",
                feed.ticker, feed.fetched_at
            ))
        } else {
            success_text(&format!(
                "Showing {} cached/current news items for {}.",
                feed.items.len(),
                feed.ticker
            ))
        }
    )?;
    Ok(())
}
pub fn write_poly_search_results(
    writer: &mut dyn Write,
    results: &[PolySearchResult],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, results);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Slug", "Question", "Event", "YES", "NO", "State",
    ]));
    for result in results {
        table.add_row([
            Cell::new(&result.slug),
            Cell::new(truncate_label(&result.question, 42)),
            Cell::new(result.event_title.as_deref().unwrap_or("-")),
            Cell::new(probability_text(result.yes_price)),
            Cell::new(probability_text(result.no_price)),
            Cell::new(poly_market_state(result.active, result.closed, false)),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_poly_watchlist(
    writer: &mut dyn Write,
    watchlist: &[PolyWatchEntry],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, watchlist);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "ID", "Slug", "Label", "YES", "NO", "State", "Fetched",
    ]));
    for entry in watchlist {
        table.add_row([
            Cell::new(entry.id),
            Cell::new(&entry.slug),
            Cell::new(entry.label.as_deref().unwrap_or("-")),
            Cell::new(
                entry
                    .market
                    .as_ref()
                    .map(|market| probability_text(market.yes_price))
                    .unwrap_or_else(|| warning_text("missing")),
            ),
            Cell::new(
                entry
                    .market
                    .as_ref()
                    .map(|market| probability_text(market.no_price))
                    .unwrap_or_else(|| warning_text("missing")),
            ),
            Cell::new(
                entry
                    .market
                    .as_ref()
                    .map(|market| poly_market_state(market.active, market.closed, market.stale))
                    .unwrap_or_else(|| warning_text("stale")),
            ),
            Cell::new(
                entry
                    .market
                    .as_ref()
                    .map(|market| market.fetched_at.clone())
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_poly_market(
    writer: &mut dyn Write,
    market: &PolyMarket,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, market);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Slug",
        "Question",
        "YES",
        "NO",
        "24H Vol",
        "Liquidity",
        "State",
        "Fetched",
    ]));
    table.add_row([
        Cell::new(&market.slug),
        Cell::new(truncate_label(&market.question, 48)),
        Cell::new(probability_text(market.yes_price)),
        Cell::new(probability_text(market.no_price)),
        Cell::new(optional_dollar_text(market.volume_24h)),
        Cell::new(optional_dollar_text(market.liquidity)),
        Cell::new(poly_market_state(
            market.active,
            market.closed,
            market.stale,
        )),
        Cell::new(&market.fetched_at),
    ]);
    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_poly_movers(
    writer: &mut dyn Write,
    movers: &[PolyMarket],
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, movers);
    }

    let mut table = new_table();
    table.set_header(header_row([
        "Slug",
        "Question",
        "YES",
        "NO",
        "24H Vol",
        "Liquidity",
        "State",
    ]));
    for market in movers {
        table.add_row([
            Cell::new(&market.slug),
            Cell::new(truncate_label(&market.question, 42)),
            Cell::new(probability_text(market.yes_price)),
            Cell::new(probability_text(market.no_price)),
            Cell::new(optional_dollar_text(market.volume_24h)),
            Cell::new(optional_dollar_text(market.liquidity)),
            Cell::new(poly_market_state(
                market.active,
                market.closed,
                market.stale,
            )),
        ]);
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

pub fn write_poly_brief(
    writer: &mut dyn Write,
    brief: &PolyBrief,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, brief);
    }

    writeln!(
        writer,
        "{} {}",
        label_text("POLY"),
        paint(&brief.slug, Tone::Header)
    )?;
    writeln!(
        writer,
        "{} {}",
        label_text("Tone:"),
        paint(&brief.tone.to_ascii_uppercase(), Tone::Info)
    )?;
    writeln!(writer, "{} {}", label_text("Summary:"), brief.summary)?;
    writeln!(writer, "{} {}", label_text("Fetched:"), &brief.fetched_at)?;
    if brief.stale {
        writeln!(
            writer,
            "{}",
            warning_text("Using stale cached Polymarket input.")
        )?;
    }
    writeln!(writer)?;
    writeln!(writer, "{}", label_text("Why It Matters"))?;
    for item in &brief.why_it_matters {
        writeln!(writer, "- {item}")?;
    }
    writeln!(writer)?;
    writeln!(writer, "{}", label_text("Watch Next"))?;
    for item in &brief.watch_items {
        writeln!(writer, "- {item}")?;
    }
    Ok(())
}

pub fn write_poly_refresh_summary(
    writer: &mut dyn Write,
    summary: &PolyRefreshSummary,
    as_json: bool,
) -> Result<(), AppError> {
    if as_json {
        return write_json(writer, summary);
    }

    let mut table = new_table();
    table.set_header(header_row(["Metric", "Value"]));
    table.add_row([
        Cell::new(label_text("Slugs Considered")),
        Cell::new(summary.slugs_considered),
    ]);
    table.add_row([
        Cell::new(label_text("Markets Refreshed")),
        Cell::new(summary.markets_refreshed),
    ]);
    table.add_row([
        Cell::new(label_text("Movers Refreshed")),
        Cell::new(on_off_text(summary.movers_refreshed)),
    ]);
    table.add_row([
        Cell::new(label_text("Failures")),
        Cell::new(summary.failure_count),
    ]);
    table.add_row([Cell::new(label_text("Ran At")), Cell::new(&summary.ran_at)]);
    writeln!(writer, "{table}")?;
    Ok(())
}
pub fn export_transactions_csv(
    path: &Path,
    transactions: &[TransactionRecord],
) -> Result<(), AppError> {
    ensure_parent_dir(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    writer.write_record([
        "id",
        "txn_date",
        "kind",
        "amount",
        "amount_cents",
        "account_id",
        "account_name",
        "to_account_id",
        "to_account_name",
        "category_id",
        "category_name",
        "payee",
        "note",
        "created_at",
        "updated_at",
        "deleted_at",
        "reconciliation_id",
        "recurring_rule_id",
    ])?;

    for transaction in transactions {
        writer.write_record([
            transaction.id.to_string(),
            transaction.txn_date.clone(),
            transaction.kind.as_db_str().to_string(),
            format_cents(transaction.amount_cents),
            transaction.amount_cents.to_string(),
            transaction.account_id.to_string(),
            transaction.account_name.clone(),
            transaction
                .to_account_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
            transaction.to_account_name.clone().unwrap_or_default(),
            transaction
                .category_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
            transaction.category_name.clone().unwrap_or_default(),
            transaction.payee.clone().unwrap_or_default(),
            transaction.note.clone().unwrap_or_default(),
            transaction.created_at.clone(),
            transaction.updated_at.clone(),
            transaction.deleted_at.clone().unwrap_or_default(),
            transaction
                .reconciliation_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
            transaction
                .recurring_rule_id
                .map(|value| value.to_string())
                .unwrap_or_default(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}

pub fn export_summary_csv(path: &Path, summary: &SummaryRecord) -> Result<(), AppError> {
    ensure_parent_dir(path)?;
    let mut writer = csv::Writer::from_path(path)?;
    writer.write_record([
        "from",
        "to",
        "account_id",
        "account_name",
        "transaction_count",
        "income",
        "income_cents",
        "expense",
        "expense_cents",
        "net",
        "net_cents",
        "transfer_in",
        "transfer_in_cents",
        "transfer_out",
        "transfer_out_cents",
    ])?;
    writer.write_record([
        summary.from.clone(),
        summary.to.clone(),
        summary
            .account_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        summary.account_name.clone().unwrap_or_default(),
        summary.transaction_count.to_string(),
        format_cents(summary.income_cents),
        summary.income_cents.to_string(),
        format_cents(summary.expense_cents),
        summary.expense_cents.to_string(),
        format_cents(summary.net_cents),
        summary.net_cents.to_string(),
        format_cents(summary.transfer_in_cents),
        summary.transfer_in_cents.to_string(),
        format_cents(summary.transfer_out_cents),
        summary.transfer_out_cents.to_string(),
    ])?;
    writer.flush()?;
    Ok(())
}

fn write_json<T: Serialize + ?Sized>(writer: &mut dyn Write, value: &T) -> Result<(), AppError> {
    serde_json::to_writer_pretty(&mut *writer, value)?;
    writeln!(writer)?;
    Ok(())
}

fn new_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_BORDERS_ONLY)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}

fn header_row<const N: usize>(labels: [&str; N]) -> Vec<Cell> {
    labels
        .into_iter()
        .map(|label| Cell::new(paint(label, Tone::Header)))
        .collect()
}

fn import_preview_row(row: &ImportedTransactionRow) -> [Cell; 7] {
    [
        Cell::new(row.line_number),
        Cell::new(&row.txn_date),
        Cell::new(row.kind.as_db_str()),
        Cell::new(money_text(match row.kind {
            crate::model::TransactionKind::Expense => -row.amount_cents,
            _ => row.amount_cents,
        })),
        Cell::new(row.category_name.as_deref().unwrap_or("-")),
        Cell::new(row.payee.as_deref().unwrap_or("-")),
        Cell::new(if row.duplicate {
            warning_text("duplicate")
        } else {
            success_text("ready")
        }),
    ]
}

fn money_text(amount_cents: i64) -> String {
    if amount_cents < 0 {
        paint(&format_cents(amount_cents), Tone::Negative)
    } else if amount_cents == 0 {
        label_text(&format_cents(amount_cents))
    } else {
        paint(&format_cents(amount_cents), Tone::Positive)
    }
}

fn label_text(text: &str) -> String {
    paint(text, Tone::Primary)
}

fn goal_status_row(goal: &GoalStatusRecord) -> [Cell; 8] {
    [
        Cell::new(&goal.name),
        Cell::new(goal.kind.as_db_str()),
        Cell::new(&goal.account_name),
        Cell::new(money_text(goal.current_balance_cents)),
        Cell::new(money_text(goal.projected_balance_cents)),
        Cell::new(money_text(goal.remaining_cents)),
        Cell::new(money_text(goal.suggested_monthly_contribution_cents)),
        Cell::new(if goal.on_track {
            success_text("on track")
        } else {
            error_text(goal.breach_date.as_deref().unwrap_or("behind"))
        }),
    ]
}

fn budget_mode_label(scenario_id: Option<i64>, is_override: bool) -> String {
    if is_override {
        "override".to_string()
    } else if scenario_id.is_some() {
        "inherited".to_string()
    } else {
        "base".to_string()
    }
}

fn transaction_status(transaction: &TransactionRecord) -> String {
    let mut labels = Vec::new();
    if transaction.deleted_at.is_some() {
        labels.push("deleted");
    }
    if transaction.reconciliation_id.is_some() {
        labels.push("reconciled");
    }
    if transaction.recurring_rule_id.is_some() {
        labels.push("recurring");
    }
    if labels.is_empty() {
        paint("open", Tone::Info)
    } else {
        label_text(&labels.join(","))
    }
}

fn schedule_text(rule: &RecurringRuleRecord) -> String {
    match rule.cadence {
        crate::model::RecurringCadence::Weekly => {
            let weekday = rule.weekday.map(|value| value.short_label()).unwrap_or("?");
            format!("weekly/{}/{}", rule.interval, weekday)
        }
        crate::model::RecurringCadence::Monthly => {
            let day = rule.day_of_month.unwrap_or(0);
            format!("monthly/{}/d{}", rule.interval, day)
        }
    }
}

fn market_price_text(quote: &MarketQuote) -> String {
    let value = match quote.currency.as_deref() {
        Some(currency) => format!("{} {:.2}", currency, quote.last_price),
        None => format!("{:.2}", quote.last_price),
    };

    if quote.stale {
        warning_text(&value)
    } else if matches!(quote.change, Some(change) if change < 0.0) {
        paint(&value, Tone::Negative)
    } else if matches!(quote.change, Some(change) if change > 0.0) {
        paint(&value, Tone::Positive)
    } else {
        label_text(&value)
    }
}

fn market_change_text(quote: &MarketQuote) -> String {
    let value = match (quote.change, quote.change_percent) {
        (Some(change), Some(change_percent)) => format!("{:+.2} ({:+.2}%)", change, change_percent),
        (Some(change), None) => format!("{:+.2}", change),
        (None, Some(change_percent)) => format!("{:+.2}%", change_percent),
        (None, None) => return label_text("-"),
    };

    let direction = quote.change.or(quote.change_percent).unwrap_or(0.0);
    if direction < 0.0 {
        paint(&value, Tone::Negative)
    } else if direction > 0.0 {
        paint(&value, Tone::Positive)
    } else {
        label_text(&value)
    }
}

fn market_quote_state(quote: &MarketQuote) -> String {
    if quote.stale {
        warning_text("stale")
    } else if let Some(state) = quote.market_state.as_deref() {
        paint(state, Tone::Info)
    } else {
        label_text("-")
    }
}

fn probability_text(value: Option<f64>) -> String {
    match value {
        Some(value) => label_text(&format!("{:.0}%", value * 100.0)),
        None => label_text("-"),
    }
}

fn optional_dollar_text(value: Option<f64>) -> String {
    match value {
        Some(value) => label_text(&format!("${:.0}", value)),
        None => label_text("-"),
    }
}

fn poly_market_state(active: bool, closed: bool, stale: bool) -> String {
    if stale {
        warning_text("stale")
    } else if closed {
        label_text("closed")
    } else if active {
        success_text("active")
    } else {
        warning_text("inactive")
    }
}

fn truncate_label(value: &str, limit: usize) -> String {
    let count = value.chars().count();
    if count <= limit {
        value.to_string()
    } else {
        let mut truncated = value
            .chars()
            .take(limit.saturating_sub(1))
            .collect::<String>();
        truncated.push_str("...");
        truncated
    }
}

fn on_off_text(value: bool) -> String {
    if value {
        success_text("on")
    } else {
        warning_text("off")
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}
