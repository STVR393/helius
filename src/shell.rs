use std::io::{self, Write};
use std::path::PathBuf;

use chrono::Local;

use crate::db::Db;
use crate::error::AppError;

pub fn run_interactive_shell(db_path: PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    writeln!(stdout, "Helius interactive shell")?;
    writeln!(stdout, "Database: {}", db_path.display())?;
    writeln!(
        stdout,
        "Shortcuts: init, account, category, income, expense, transfer, balance, transactions, summary, budget, reconcile, recurring, watchlist, quote, brief, news, exit"
    )?;
    writeln!(
        stdout,
        "You can also type full commands like `tx list --account Checking`, `recurring list`, or `market quote AAPL`."
    )?;

    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        write!(stdout, "helius> ")?;
        stdout.flush()?;

        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            writeln!(stdout)?;
            break;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if matches!(input, "exit" | "quit") {
            writeln!(stdout, "Bye.")?;
            break;
        }

        if input == "help" {
            print_help(stdout)?;
            continue;
        }

        let result = match input {
            "init" => prompt_init(&db_path, stdout),
            "account" | "account add" => prompt_account(&db_path, stdout),
            "category" | "category add" => prompt_category(&db_path, stdout),
            "income" => prompt_transaction(&db_path, "income", stdout),
            "expense" => prompt_transaction(&db_path, "expense", stdout),
            "transfer" => prompt_transaction(&db_path, "transfer", stdout),
            "balance" => execute_cli(&db_path, vec!["balance".to_string()], stdout),
            "transactions" | "tx" => prompt_transactions(&db_path, stdout),
            "summary" | "summary month" => prompt_summary_month(&db_path, stdout),
            "budget" | "budget set" => prompt_budget(&db_path, stdout),
            "summary range" => prompt_summary_range(&db_path, stdout),
            "reconcile" | "reconcile start" => prompt_reconcile(&db_path, stdout),
            "recurring" | "recurring add" => prompt_recurring(&db_path, stdout),
            "recurring run" => prompt_recurring_run(&db_path, stdout),
            "watchlist" | "watchlist add" => prompt_watchlist_add(&db_path, stdout),
            "quote" | "market quote" => prompt_market_quote(&db_path, stdout),
            "brief" | "market brief" => prompt_market_brief(&db_path, stdout),
            "news" | "market news" => prompt_market_news(&db_path, stdout),
            _ => execute_raw_cli(&db_path, input, stdout),
        };

        if let Err(error) = result {
            writeln!(stdout, "{error}")?;
        }
    }

    Ok(())
}

fn print_help(stdout: &mut dyn Write) -> Result<(), AppError> {
    writeln!(stdout, "Commands:")?;
    writeln!(stdout, "  init            Initialize the database")?;
    writeln!(stdout, "  account         Guided account creation")?;
    writeln!(stdout, "  category        Guided category creation")?;
    writeln!(stdout, "  income          Guided income transaction")?;
    writeln!(stdout, "  expense         Guided expense transaction")?;
    writeln!(stdout, "  transfer        Guided transfer transaction")?;
    writeln!(stdout, "  balance         Show current balances")?;
    writeln!(
        stdout,
        "  transactions    List transactions with optional filters"
    )?;
    writeln!(stdout, "  summary         Show a monthly summary")?;
    writeln!(stdout, "  budget          Guided monthly budget entry")?;
    writeln!(stdout, "  reconcile       Guided reconciliation flow")?;
    writeln!(stdout, "  recurring       Guided recurring rule creation")?;
    writeln!(stdout, "  recurring run   Post due recurring transactions")?;
    writeln!(stdout, "  watchlist       Guided watchlist ticker creation")?;
    writeln!(stdout, "  quote           Guided market quote lookup")?;
    writeln!(stdout, "  brief           Guided market brief lookup")?;
    writeln!(stdout, "  news            Guided raw market news lookup")?;
    writeln!(stdout, "  help            Show this help")?;
    writeln!(stdout, "  exit            Close the app")?;
    writeln!(stdout)?;
    writeln!(stdout, "You can also type raw CLI commands, for example:")?;
    writeln!(stdout, "  tx edit 3 --note \"fixed note\"")?;
    writeln!(stdout, "  reconcile list")?;
    writeln!(stdout, "  recurring list")?;
    writeln!(stdout, "  watchlist refresh --all")?;
    writeln!(stdout, "  market brief AAPL")?;
    writeln!(stdout, "  market news AAPL --limit 5")?;
    writeln!(stdout, "  budget status 2026-03")?;
    Ok(())
}

fn prompt_init(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let currency = prompt_required(stdout, "Currency code", Some("USD"))?;
    execute_cli(
        db_path,
        vec!["init".to_string(), "--currency".to_string(), currency],
        stdout,
    )
}

fn prompt_account(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let name = prompt_required(stdout, "Account name", None)?;
    let kind = prompt_required(
        stdout,
        "Account type (cash/checking/savings/credit)",
        Some("checking"),
    )?;
    let opening_balance = prompt_optional(stdout, "Opening balance", Some("0.00"))?;
    let opened_on = prompt_optional(stdout, "Opened on (YYYY-MM-DD)", Some(&today_iso()))?;

    let mut args = vec![
        "account".to_string(),
        "add".to_string(),
        name,
        "--type".to_string(),
        kind,
    ];

    if let Some(value) = opening_balance {
        args.push("--opening-balance".to_string());
        args.push(value);
    }

    if let Some(value) = opened_on {
        args.push("--opened-on".to_string());
        args.push(value);
    }

    execute_cli(db_path, args, stdout)
}

fn prompt_category(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let name = prompt_required(stdout, "Category name", None)?;
    let kind = prompt_required(stdout, "Category kind (income/expense)", Some("expense"))?;
    execute_cli(
        db_path,
        vec![
            "category".to_string(),
            "add".to_string(),
            name,
            "--kind".to_string(),
            kind,
        ],
        stdout,
    )
}

fn prompt_transaction(
    db_path: &PathBuf,
    kind: &str,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    let amount = prompt_required(stdout, "Amount", None)?;
    let date = prompt_required(stdout, "Date (YYYY-MM-DD)", Some(&today_iso()))?;
    let account = prompt_required(stdout, "Account", None)?;
    let mut args = vec![
        "tx".to_string(),
        "add".to_string(),
        "--type".to_string(),
        kind.to_string(),
        "--amount".to_string(),
        amount,
        "--date".to_string(),
        date,
        "--account".to_string(),
        account,
    ];

    if kind == "transfer" {
        let to_account = prompt_required(stdout, "To account", None)?;
        args.push("--to-account".to_string());
        args.push(to_account);
    } else {
        let category = prompt_required(stdout, "Category", None)?;
        args.push("--category".to_string());
        args.push(category);
    }

    if let Some(payee) = prompt_optional(stdout, "Payee", None)? {
        args.push("--payee".to_string());
        args.push(payee);
    }

    if let Some(note) = prompt_optional(stdout, "Note", None)? {
        args.push("--note".to_string());
        args.push(note);
    }

    execute_cli(db_path, args, stdout)
}

fn prompt_transactions(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let account = prompt_optional(stdout, "Filter account", None)?;
    let from = prompt_optional(stdout, "From date (YYYY-MM-DD)", None)?;
    let to = prompt_optional(stdout, "To date (YYYY-MM-DD)", None)?;
    let limit = prompt_optional(stdout, "Limit", Some("20"))?;
    let include_deleted = prompt_optional(stdout, "Include deleted? (yes/no)", Some("no"))?;

    let mut args = vec!["tx".to_string(), "list".to_string()];
    if let Some(value) = account {
        args.push("--account".to_string());
        args.push(value);
    }
    if let Some(value) = from {
        args.push("--from".to_string());
        args.push(value);
    }
    if let Some(value) = to {
        args.push("--to".to_string());
        args.push(value);
    }
    if let Some(value) = limit {
        args.push("--limit".to_string());
        args.push(value);
    }
    if matches!(include_deleted.as_deref(), Some("yes") | Some("y")) {
        args.push("--include-deleted".to_string());
    }

    execute_cli(db_path, args, stdout)
}

fn prompt_summary_month(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let month = prompt_optional(stdout, "Month (YYYY-MM, blank = current month)", None)?;
    let account = prompt_optional(stdout, "Filter account", None)?;

    let mut args = vec!["summary".to_string(), "month".to_string()];
    if let Some(value) = month {
        args.push(value);
    }
    if let Some(value) = account {
        args.push("--account".to_string());
        args.push(value);
    }

    execute_cli(db_path, args, stdout)
}

fn prompt_summary_range(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let from = prompt_required(stdout, "From date (YYYY-MM-DD)", None)?;
    let to = prompt_required(stdout, "To date (YYYY-MM-DD)", None)?;
    let account = prompt_optional(stdout, "Filter account", None)?;

    let mut args = vec![
        "summary".to_string(),
        "range".to_string(),
        "--from".to_string(),
        from,
        "--to".to_string(),
        to,
    ];
    if let Some(value) = account {
        args.push("--account".to_string());
        args.push(value);
    }

    execute_cli(db_path, args, stdout)
}

fn prompt_budget(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let category = prompt_required(stdout, "Expense category", None)?;
    let month = prompt_required(stdout, "Month (YYYY-MM)", Some(&today_iso()[..7]))?;
    let amount = prompt_required(stdout, "Budget amount", None)?;
    execute_cli(
        db_path,
        vec![
            "budget".to_string(),
            "set".to_string(),
            category,
            "--month".to_string(),
            month,
            "--amount".to_string(),
            amount,
        ],
        stdout,
    )
}

fn prompt_reconcile(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let account = prompt_required(stdout, "Account", None)?;
    let ending_on = prompt_required(
        stdout,
        "Statement ending on (YYYY-MM-DD)",
        Some(&today_iso()),
    )?;
    let statement_balance = prompt_required(stdout, "Statement balance", None)?;
    let db = Db::open_existing(db_path)?;
    let eligible = db.list_eligible_reconciliation_transactions(&account, &ending_on)?;
    if eligible.is_empty() {
        writeln!(stdout, "No eligible transactions found.")?;
        return Ok(());
    }
    crate::output::write_transactions(stdout, &eligible, false)?;
    let ids = prompt_required(stdout, "Transaction ids (comma separated)", None)?;

    let mut args = vec![
        "reconcile".to_string(),
        "start".to_string(),
        "--account".to_string(),
        account,
        "--to".to_string(),
        ending_on,
        "--statement-balance".to_string(),
        statement_balance,
    ];
    for raw_id in ids
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        args.push("--transaction-id".to_string());
        args.push(raw_id.to_string());
    }
    execute_cli(db_path, args, stdout)
}

fn prompt_recurring(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let name = prompt_required(stdout, "Rule name", None)?;
    let kind = prompt_required(stdout, "Type (income/expense/transfer)", Some("expense"))?;
    let amount = prompt_required(stdout, "Amount", None)?;
    let account = prompt_required(stdout, "Account", None)?;
    let cadence = prompt_required(stdout, "Cadence (weekly/monthly)", Some("monthly"))?;
    let interval = prompt_optional(stdout, "Interval", Some("1"))?;
    let start_on = prompt_required(stdout, "Start on (YYYY-MM-DD)", Some(&today_iso()))?;
    let next_due_on = prompt_optional(stdout, "Next due on (YYYY-MM-DD)", None)?;
    let end_on = prompt_optional(stdout, "End on (YYYY-MM-DD)", None)?;

    let mut args = vec![
        "recurring".to_string(),
        "add".to_string(),
        name,
        "--type".to_string(),
        kind.clone(),
        "--amount".to_string(),
        amount,
        "--account".to_string(),
        account,
        "--cadence".to_string(),
        cadence.clone(),
        "--start-on".to_string(),
        start_on,
    ];

    if let Some(value) = interval {
        args.push("--interval".to_string());
        args.push(value);
    }
    if let Some(value) = next_due_on {
        args.push("--next-due-on".to_string());
        args.push(value);
    }

    if kind == "transfer" {
        let to_account = prompt_required(stdout, "To account", None)?;
        args.push("--to-account".to_string());
        args.push(to_account);
    } else {
        let category = prompt_required(stdout, "Category", None)?;
        args.push("--category".to_string());
        args.push(category);
    }

    if cadence == "weekly" {
        let weekday =
            prompt_required(stdout, "Weekday (mon/tue/wed/thu/fri/sat/sun)", Some("mon"))?;
        args.push("--weekday".to_string());
        args.push(weekday);
    } else {
        let day = prompt_required(stdout, "Day of month (1-28)", Some("1"))?;
        args.push("--day-of-month".to_string());
        args.push(day);
    }

    if let Some(payee) = prompt_optional(stdout, "Payee", None)? {
        args.push("--payee".to_string());
        args.push(payee);
    }
    if let Some(note) = prompt_optional(stdout, "Note", None)? {
        args.push("--note".to_string());
        args.push(note);
    }
    if let Some(value) = end_on {
        args.push("--end-on".to_string());
        args.push(value);
    }

    execute_cli(db_path, args, stdout)
}

fn prompt_recurring_run(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let through = prompt_optional(
        stdout,
        "Post due items through (YYYY-MM-DD)",
        Some(&today_iso()),
    )?;
    let mut args = vec!["recurring".to_string(), "run".to_string()];
    if let Some(value) = through {
        args.push("--through".to_string());
        args.push(value);
    }
    execute_cli(db_path, args, stdout)
}

fn prompt_watchlist_add(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let ticker = prompt_required(stdout, "Ticker", None)?;
    let label = prompt_optional(stdout, "Label", None)?;
    let mut args = vec!["watchlist".to_string(), "add".to_string(), ticker];
    if let Some(value) = label {
        args.push("--label".to_string());
        args.push(value);
    }
    execute_cli(db_path, args, stdout)
}

fn prompt_market_quote(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let ticker = prompt_required(stdout, "Ticker", None)?;
    execute_cli(
        db_path,
        vec!["market".to_string(), "quote".to_string(), ticker],
        stdout,
    )
}

fn prompt_market_brief(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let ticker = prompt_optional(stdout, "Ticker (blank uses single watchlist item)", None)?;
    let limit = prompt_optional(stdout, "Limit", Some("5"))?;
    let mut args = vec!["market".to_string(), "brief".to_string()];
    if let Some(value) = ticker {
        args.push(value);
    }
    if let Some(value) = limit {
        args.push("--limit".to_string());
        args.push(value);
    }
    execute_cli(db_path, args, stdout)
}
fn prompt_market_news(db_path: &PathBuf, stdout: &mut dyn Write) -> Result<(), AppError> {
    let ticker = prompt_optional(stdout, "Ticker (blank uses single watchlist item)", None)?;
    let limit = prompt_optional(stdout, "Limit", Some("5"))?;
    let mut args = vec!["market".to_string(), "news".to_string()];
    if let Some(value) = ticker {
        args.push(value);
    }
    if let Some(value) = limit {
        args.push("--limit".to_string());
        args.push(value);
    }
    execute_cli(db_path, args, stdout)
}
fn execute_raw_cli(db_path: &PathBuf, input: &str, stdout: &mut dyn Write) -> Result<(), AppError> {
    let tokens = shlex::split(input)
        .ok_or_else(|| AppError::Validation("invalid quoting in command".to_string()))?;
    execute_cli(db_path, tokens, stdout)
}

fn execute_cli(
    db_path: &PathBuf,
    args: Vec<String>,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    let mut argv = vec![
        "helius".to_string(),
        "--db".to_string(),
        db_path.display().to_string(),
    ];
    argv.extend(args);
    let mut stderr = io::sink();
    crate::run_app(argv, stdout, &mut stderr)
}

fn prompt_required(
    stdout: &mut dyn Write,
    label: &str,
    default: Option<&str>,
) -> Result<String, AppError> {
    loop {
        let value = prompt_value(stdout, label, default)?;
        if !value.is_empty() {
            return Ok(value);
        }
        writeln!(stdout, "A value is required.")?;
    }
}

fn prompt_optional(
    stdout: &mut dyn Write,
    label: &str,
    default: Option<&str>,
) -> Result<Option<String>, AppError> {
    let value = prompt_value(stdout, label, default)?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn prompt_value(
    stdout: &mut dyn Write,
    label: &str,
    default: Option<&str>,
) -> Result<String, AppError> {
    match default {
        Some(value) => write!(stdout, "{label} [{value}]: ")?,
        None => write!(stdout, "{label}: ")?,
    }
    stdout.flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let trimmed = input.trim();

    if trimmed.is_empty() {
        Ok(default.unwrap_or_default().to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn today_iso() -> String {
    Local::now().date_naive().format("%Y-%m-%d").to_string()
}
