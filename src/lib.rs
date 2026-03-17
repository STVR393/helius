mod amount;
mod cli;
mod db;
mod error;
mod model;
mod output;
mod shell;
mod theme;
mod ui;

use std::ffi::OsString;
use std::io::{BufRead, IsTerminal, Write};
use std::path::Path;

use chrono::{Datelike, Local, NaiveDate};
use clap::{CommandFactory, Parser};

use crate::amount::{parse_amount_to_cents, parse_balance_to_cents};
use crate::cli::{
    AccountAddArgs, AccountCommand, AccountDeleteArgs, AccountEditArgs, BalanceArgs, BudgetCommand,
    BudgetDeleteArgs, BudgetListArgs, BudgetSetArgs, BudgetStatusArgs, CategoryAddArgs,
    CategoryCommand, CategoryDeleteArgs, CategoryEditArgs, Cli, Command, ExportCommand,
    ExportCsvArgs, ForecastBillsArgs, ForecastCommand, ForecastShowArgs, GoalAddArgs, GoalCommand,
    GoalDeleteArgs, GoalEditArgs, GoalListArgs, ImportCommand, ImportCsvArgs, InitArgs,
    PlanCommand, PlanItemAddArgs, PlanItemCommand, PlanItemEditArgs, PlanItemIdArgs,
    PlanItemListArgs, ReconcileCommand, ReconcileDeleteArgs, ReconcileListArgs, ReconcileStartArgs,
    RecurringAddArgs, RecurringCommand, RecurringEditArgs, RecurringIdArgs, RecurringListArgs,
    RecurringRunArgs, ScenarioAddArgs, ScenarioCommand, ScenarioDeleteArgs, ScenarioEditArgs,
    ScenarioListArgs, SummaryCommand, SummaryMonthArgs, SummaryRangeArgs, TransactionAddArgs,
    TransactionCommand, TransactionDeleteArgs, TransactionEditArgs, TransactionListArgs,
    TransactionRestoreArgs,
};
use crate::db::{db_requires_init, resolve_db_path, Db};
pub use crate::error::AppError;
use crate::model::{
    CsvImportPlan, ExportKind, NewPlanningGoal, NewPlanningItem, NewPlanningScenario,
    NewRecurringRule, NewTransaction, TransactionFilters, UpdatePlanningGoal, UpdatePlanningItem,
    UpdatePlanningScenario, UpdateRecurringRule, UpdateTransaction,
};

const DEFAULT_ONBOARDING_CURRENCY: &str = "USD";

pub fn run_app<I, T>(
    args: I,
    stdout: &mut dyn Write,
    _stderr: &mut dyn Write,
) -> Result<(), AppError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(args)?;
    let db_path = resolve_db_path(cli.db.clone())?;

    match cli.command {
        Some(command) => run_command(&db_path, command, stdout),
        None => {
            if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
                let stdin = std::io::stdin();
                let mut stdin = stdin.lock();
                ensure_interactive_db_ready(&db_path, &mut stdin, stdout)?;
                ui::run_tui(db_path)
            } else {
                let mut command = Cli::command();
                let mut help = Vec::new();
                command.write_long_help(&mut help)?;
                stdout.write_all(&help)?;
                writeln!(stdout)?;
                Ok(())
            }
        }
    }
}

pub fn format_error_message(message: &str) -> String {
    output::error_text(message)
}

fn ensure_interactive_db_ready(
    db_path: &Path,
    stdin: &mut dyn BufRead,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    if !db_requires_init(db_path)? {
        return Ok(());
    }

    writeln!(stdout, "Helius is starting for the first time.")?;
    writeln!(stdout, "No database was found at {}.", db_path.display())?;
    writeln!(
        stdout,
        "Enter a 3-letter currency code to initialize your local database [{}].",
        DEFAULT_ONBOARDING_CURRENCY
    )?;
    writeln!(
        stdout,
        "Press Enter to accept the default, or type `quit` to cancel."
    )?;

    loop {
        write!(stdout, "Currency> ")?;
        stdout.flush()?;

        let mut input = String::new();
        if stdin.read_line(&mut input)? == 0 {
            return Err(AppError::Config(
                "startup cancelled before database initialization".to_string(),
            ));
        }

        let trimmed = input.trim();
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            return Err(AppError::Config(
                "startup cancelled before database initialization".to_string(),
            ));
        }

        let currency = if trimmed.is_empty() {
            DEFAULT_ONBOARDING_CURRENCY
        } else {
            trimmed
        };

        let db = Db::open_for_init(db_path)?;
        match db.init(currency) {
            Ok(()) => {
                writeln!(
                    stdout,
                    "Initialized database at {} with currency {}.",
                    db_path.display(),
                    currency.trim().to_ascii_uppercase()
                )?;
                writeln!(stdout, "Starting Helius...")?;
                return Ok(());
            }
            Err(AppError::Validation(message)) => {
                writeln!(stdout, "{message}")?;
                writeln!(stdout, "Enter a 3-letter code such as USD or EUR.")?;
            }
            Err(AppError::AlreadyExists(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
    }
}

fn with_existing_db<T, F>(db_path: &Path, run: F) -> Result<T, AppError>
where
    F: FnOnce(Db) -> Result<T, AppError>,
{
    let db = Db::open_existing(db_path)?;
    run(db)
}

fn run_command(db_path: &Path, command: Command, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        Command::Shell => shell::run_interactive_shell(db_path.to_path_buf(), stdout),
        Command::Init(args) => handle_init(db_path, args, stdout),
        Command::Account { command } => {
            with_existing_db(db_path, |db| handle_account(db, command, stdout))
        }
        Command::Category { command } => {
            with_existing_db(db_path, |db| handle_category(db, command, stdout))
        }
        Command::Tx { command } => {
            with_existing_db(db_path, |db| handle_transaction(db, command, stdout))
        }
        Command::Balance(args) => with_existing_db(db_path, |db| handle_balance(db, args, stdout)),
        Command::Summary { command } => {
            with_existing_db(db_path, |db| handle_summary(db, command, stdout))
        }
        Command::Export { command } => {
            with_existing_db(db_path, |db| handle_export(db, command, stdout))
        }
        Command::Import { command } => {
            with_existing_db(db_path, |db| handle_import(db, command, stdout))
        }
        Command::Budget { command } => {
            with_existing_db(db_path, |db| handle_budget(db, command, stdout))
        }
        Command::Forecast { command } => {
            with_existing_db(db_path, |db| handle_forecast(db, command, stdout))
        }
        Command::Plan { command } => {
            with_existing_db(db_path, |db| handle_plan(db, command, stdout))
        }
        Command::Scenario { command } => {
            with_existing_db(db_path, |db| handle_scenario(db, command, stdout))
        }
        Command::Goal { command } => {
            with_existing_db(db_path, |db| handle_goal(db, command, stdout))
        }
        Command::Reconcile { command } => {
            with_existing_db(db_path, |db| handle_reconcile(db, command, stdout))
        }
        Command::Recurring { command } => {
            with_existing_db(db_path, |db| handle_recurring(db, command, stdout))
        }
    }
}

fn handle_init(db_path: &Path, args: InitArgs, stdout: &mut dyn Write) -> Result<(), AppError> {
    let db = Db::open_for_init(db_path)?;
    db.init(&args.currency)?;
    writeln!(
        stdout,
        "{}",
        output::success_text(&format!(
            "Initialized database at {} with currency {}.",
            db_path.display(),
            args.currency.trim().to_ascii_uppercase()
        ))
    )?;
    Ok(())
}

fn handle_account(db: Db, command: AccountCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        AccountCommand::Add(AccountAddArgs {
            name,
            kind,
            opening_balance,
            opened_on,
        }) => {
            let opening_balance_cents = match opening_balance {
                Some(value) => parse_balance_to_cents(&value)?,
                None => 0,
            };
            let opened_on = match opened_on {
                Some(value) => normalize_date(&value)?,
                None => today_iso(),
            };
            let account_id = db.add_account(&name, &kind, opening_balance_cents, &opened_on)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created account {account_id} ({name})."))
            )?;
            Ok(())
        }
        AccountCommand::Edit(AccountEditArgs {
            account,
            name,
            kind,
            opening_balance,
            opened_on,
        }) => {
            let name = normalize_optional_string(name);
            let opened_on = normalize_optional_date(opened_on)?;
            let opening_balance_cents = opening_balance
                .map(|value| parse_balance_to_cents(&value))
                .transpose()?;
            if name.is_none()
                && kind.is_none()
                && opening_balance_cents.is_none()
                && opened_on.is_none()
            {
                return Err(AppError::Validation(
                    "account edit requires at least one field change".to_string(),
                ));
            }
            let account_id = db.edit_account(
                &account,
                name.as_deref(),
                kind.as_ref(),
                opening_balance_cents,
                opened_on.as_deref(),
            )?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Updated account {account_id}."))
            )?;
            Ok(())
        }
        AccountCommand::Delete(AccountDeleteArgs { account }) => {
            let account_id = db.delete_account(&account)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Archived account {account_id}."))
            )?;
            Ok(())
        }
        AccountCommand::List(args) => {
            let accounts = db.list_accounts()?;
            output::write_accounts(stdout, &accounts, args.json)
        }
    }
}

fn handle_category(
    db: Db,
    command: CategoryCommand,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    match command {
        CategoryCommand::Add(CategoryAddArgs { name, kind }) => {
            let category_id = db.add_category(&name, &kind)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created category {category_id} ({name})."))
            )?;
            Ok(())
        }
        CategoryCommand::Edit(CategoryEditArgs {
            category,
            name,
            kind,
        }) => {
            if name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
                && kind.is_none()
            {
                return Err(AppError::Validation(
                    "category edit requires --name, --kind, or both".to_string(),
                ));
            }
            let category_id = db.edit_category(
                &category,
                normalize_optional_string(name).as_deref(),
                kind.as_ref(),
            )?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Updated category {category_id}."))
            )?;
            Ok(())
        }
        CategoryCommand::Delete(CategoryDeleteArgs { category }) => {
            let category_id = db.delete_category(&category)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Archived category {category_id}."))
            )?;
            Ok(())
        }
        CategoryCommand::List(args) => {
            let categories = db.list_categories()?;
            output::write_categories(stdout, &categories, args.json)
        }
    }
}

fn handle_transaction(
    db: Db,
    command: TransactionCommand,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    match command {
        TransactionCommand::Add(TransactionAddArgs {
            kind,
            amount,
            date,
            account,
            to_account,
            category,
            payee,
            note,
        }) => {
            let transaction = NewTransaction {
                txn_date: normalize_date(&date)?,
                kind,
                amount_cents: parse_amount_to_cents(&amount)?,
                account,
                to_account,
                category,
                payee: normalize_optional_string(payee),
                note: normalize_optional_string(note),
                recurring_rule_id: None,
            };
            let transaction_id = db.add_transaction(&transaction)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created transaction {transaction_id}."))
            )?;
            Ok(())
        }
        TransactionCommand::Edit(TransactionEditArgs {
            id,
            kind,
            amount,
            date,
            account,
            to_account,
            category,
            payee,
            note,
            clear_to_account,
            clear_category,
            clear_payee,
            clear_note,
        }) => {
            let patch = UpdateTransaction {
                id,
                txn_date: normalize_optional_date(date)?,
                kind,
                amount_cents: match amount {
                    Some(value) => Some(parse_amount_to_cents(&value)?),
                    None => None,
                },
                account: normalize_optional_string(account),
                to_account: normalize_optional_string(to_account),
                category: normalize_optional_string(category),
                payee: normalize_optional_string(payee),
                note: normalize_optional_string(note),
                clear_to_account,
                clear_category,
                clear_payee,
                clear_note,
            };
            db.edit_transaction(&patch)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Updated transaction {id}."))
            )?;
            Ok(())
        }
        TransactionCommand::Delete(TransactionDeleteArgs { id }) => {
            db.delete_transaction(id)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Deleted transaction {id}."))
            )?;
            Ok(())
        }
        TransactionCommand::Restore(TransactionRestoreArgs { id }) => {
            db.restore_transaction(id)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Restored transaction {id}."))
            )?;
            Ok(())
        }
        TransactionCommand::List(TransactionListArgs {
            from,
            to,
            account,
            category,
            search,
            limit,
            include_deleted,
            json,
        }) => {
            let filters = TransactionFilters {
                from: normalize_optional_date(from)?,
                to: normalize_optional_date(to)?,
                account: normalize_optional_string(account),
                category: normalize_optional_string(category),
                search: normalize_optional_string(search),
                limit,
                include_deleted,
            };
            let transactions = db.list_transactions(&filters)?;
            output::write_transactions(stdout, &transactions, json)
        }
    }
}

fn handle_balance(db: Db, args: BalanceArgs, stdout: &mut dyn Write) -> Result<(), AppError> {
    let balances = db.balances(
        args.account
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
    )?;
    output::write_balances(stdout, &balances, args.json)
}

fn handle_summary(db: Db, command: SummaryCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        SummaryCommand::Month(SummaryMonthArgs {
            month,
            account,
            json,
        }) => {
            let (from, to) = match month {
                Some(value) => month_range(&value)?,
                None => current_month_range(),
            };
            let summary = db.summary(
                &from,
                &to,
                account
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            )?;
            output::write_summary(stdout, &summary, json)
        }
        SummaryCommand::Range(SummaryRangeArgs {
            from,
            to,
            account,
            json,
        }) => {
            let summary = db.summary(
                &normalize_date(&from)?,
                &normalize_date(&to)?,
                account
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            )?;
            output::write_summary(stdout, &summary, json)
        }
    }
}

fn handle_export(db: Db, command: ExportCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        ExportCommand::Csv(ExportCsvArgs {
            kind,
            output,
            from,
            to,
            month,
            account,
            category,
        }) => match kind {
            ExportKind::Transactions => {
                let (from, to) = resolve_export_range(month, from, to, false)?;
                let filters = TransactionFilters {
                    from,
                    to,
                    account: normalize_optional_string(account),
                    category: normalize_optional_string(category),
                    search: None,
                    limit: None,
                    include_deleted: true,
                };
                let transactions = db.list_transactions(&filters)?;
                output::export_transactions_csv(&output, &transactions)?;
                writeln!(
                    stdout,
                    "{}",
                    output::success_text(&format!(
                        "Exported {} transactions to {}.",
                        transactions.len(),
                        output.display()
                    ))
                )?;
                Ok(())
            }
            ExportKind::Summary => {
                if category
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_some()
                {
                    return Err(AppError::Validation(
                        "summary export does not support category filters".to_string(),
                    ));
                }
                let (from, to) = resolve_export_range(month, from, to, true)?;
                let summary = db.summary(
                    &from.ok_or_else(|| {
                        AppError::Validation(
                            "summary export requires --month or both --from and --to".to_string(),
                        )
                    })?,
                    &to.ok_or_else(|| {
                        AppError::Validation(
                            "summary export requires --month or both --from and --to".to_string(),
                        )
                    })?,
                    account
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty()),
                )?;
                output::export_summary_csv(&output, &summary)?;
                writeln!(
                    stdout,
                    "{}",
                    output::success_text(&format!("Exported summary to {}.", output.display()))
                )?;
                Ok(())
            }
        },
    }
}

fn handle_import(db: Db, command: ImportCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        ImportCommand::Csv(ImportCsvArgs {
            input,
            account,
            date_column,
            amount_column,
            description_column,
            category_column,
            category,
            payee_column,
            note_column,
            type_column,
            default_type,
            date_format,
            delimiter,
            dry_run,
            allow_duplicates,
            json,
        }) => {
            let delimiter_u8 = if delimiter.is_ascii() {
                delimiter as u8
            } else {
                return Err(AppError::Validation(
                    "CSV delimiter must be a single ASCII character".to_string(),
                ));
            };
            let plan = CsvImportPlan {
                path: input,
                account,
                date_column,
                amount_column,
                description_column,
                category_column,
                category,
                payee_column,
                note_column,
                type_column,
                default_kind: default_type,
                date_format,
                delimiter: delimiter_u8,
                dry_run,
                allow_duplicates,
            };
            let result = db.import_csv_transactions(&plan)?;
            output::write_csv_import_result(stdout, &result, json)
        }
    }
}

fn handle_budget(db: Db, command: BudgetCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        BudgetCommand::Set(BudgetSetArgs {
            category,
            month,
            amount,
            account,
            scenario,
        }) => {
            let month = normalize_month(&month)?;
            let amount_cents = parse_amount_to_cents(&amount)?;
            let budget_id = db.set_budget(
                &month,
                &category,
                amount_cents,
                normalize_optional_string(account).as_deref(),
                normalize_optional_string(scenario).as_deref(),
            )?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!(
                    "Saved budget {budget_id} for {category} in {month}."
                ))
            )?;
            Ok(())
        }
        BudgetCommand::Delete(BudgetDeleteArgs {
            category,
            month,
            scenario,
        }) => {
            let month = normalize_month(&month)?;
            let scenario = normalize_optional_string(scenario);
            db.delete_budget(&month, &category, scenario.as_deref())?;
            let message = match scenario {
                Some(name) => {
                    format!("Reset scenario budget for {category} in {month} ({name}).")
                }
                None => format!("Deleted budget for {category} in {month}."),
            };
            writeln!(stdout, "{}", output::warning_text(&message))?;
            Ok(())
        }
        BudgetCommand::List(BudgetListArgs {
            month,
            scenario,
            json,
        }) => {
            let month = match month {
                Some(value) => Some(normalize_month(&value)?),
                None => None,
            };
            let budgets = db.list_budgets(
                month.as_deref(),
                normalize_optional_string(scenario).as_deref(),
            )?;
            output::write_budgets(stdout, &budgets, json)
        }
        BudgetCommand::Status(BudgetStatusArgs {
            month,
            scenario,
            json,
        }) => {
            let month = match month {
                Some(value) => normalize_month(&value)?,
                None => Local::now().date_naive().format("%Y-%m").to_string(),
            };
            let rows = db.budget_status(&month, normalize_optional_string(scenario).as_deref())?;
            output::write_budget_status(stdout, &rows, json)
        }
    }
}

fn handle_forecast(
    db: Db,
    command: ForecastCommand,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    match command {
        ForecastCommand::Show(ForecastShowArgs {
            scenario,
            account,
            days,
            json,
        }) => {
            let snapshot = db.forecast(
                normalize_optional_string(scenario).as_deref(),
                normalize_optional_string(account).as_deref(),
                days,
            )?;
            output::write_forecast(stdout, &snapshot, json)
        }
        ForecastCommand::Bills(ForecastBillsArgs {
            scenario,
            account,
            days,
            json,
        }) => {
            let snapshot = db.forecast(
                normalize_optional_string(scenario).as_deref(),
                normalize_optional_string(account).as_deref(),
                days,
            )?;
            output::write_bill_calendar(stdout, &snapshot.bill_calendar, json)
        }
    }
}

fn handle_plan(db: Db, command: PlanCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        PlanCommand::Item { command } => match command {
            PlanItemCommand::Add(PlanItemAddArgs {
                title,
                scenario,
                kind,
                amount,
                due_on,
                account,
                to_account,
                category,
                payee,
                note,
            }) => {
                let item = NewPlanningItem {
                    scenario: normalize_optional_string(scenario),
                    title,
                    kind,
                    amount_cents: parse_amount_to_cents(&amount)?,
                    account,
                    to_account: normalize_optional_string(to_account),
                    category: normalize_optional_string(category),
                    due_on: normalize_date(&due_on)?,
                    payee: normalize_optional_string(payee),
                    note: normalize_optional_string(note),
                };
                let item_id = db.add_planning_item(&item)?;
                writeln!(
                    stdout,
                    "{}",
                    output::success_text(&format!("Created planning item {item_id}."))
                )?;
                Ok(())
            }
            PlanItemCommand::Edit(PlanItemEditArgs {
                id,
                scenario,
                title,
                kind,
                amount,
                due_on,
                account,
                to_account,
                category,
                payee,
                note,
                clear_scenario,
                clear_to_account,
                clear_category,
                clear_payee,
                clear_note,
            }) => {
                if title
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                    && kind.is_none()
                    && amount.is_none()
                    && due_on.is_none()
                    && account
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    && to_account
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    && category
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    && payee
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    && note
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    && scenario
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    && !clear_scenario
                    && !clear_to_account
                    && !clear_category
                    && !clear_payee
                    && !clear_note
                {
                    return Err(AppError::Validation(
                        "plan item edit requires at least one field change".to_string(),
                    ));
                }
                let patch = UpdatePlanningItem {
                    id,
                    scenario: normalize_optional_string(scenario),
                    title: normalize_optional_string(title),
                    kind,
                    amount_cents: amount
                        .map(|value| parse_amount_to_cents(&value))
                        .transpose()?,
                    account: normalize_optional_string(account),
                    to_account: normalize_optional_string(to_account),
                    category: normalize_optional_string(category),
                    due_on: normalize_optional_date(due_on)?,
                    payee: normalize_optional_string(payee),
                    note: normalize_optional_string(note),
                    clear_scenario,
                    clear_to_account,
                    clear_category,
                    clear_payee,
                    clear_note,
                };
                db.edit_planning_item(&patch)?;
                writeln!(
                    stdout,
                    "{}",
                    output::success_text(&format!("Updated planning item {id}."))
                )?;
                Ok(())
            }
            PlanItemCommand::List(PlanItemListArgs {
                scenario,
                from,
                to,
                json,
            }) => {
                let items = db.list_planning_items(
                    normalize_optional_string(scenario).as_deref(),
                    normalize_optional_date(from)?.as_deref(),
                    normalize_optional_date(to)?.as_deref(),
                )?;
                output::write_planning_items(stdout, &items, json)
            }
            PlanItemCommand::Delete(PlanItemIdArgs { id }) => {
                db.delete_planning_item(id)?;
                writeln!(
                    stdout,
                    "{}",
                    output::warning_text(&format!("Archived planning item {id}."))
                )?;
                Ok(())
            }
            PlanItemCommand::Post(PlanItemIdArgs { id }) => {
                let transaction_id = db.post_planning_item(id)?;
                writeln!(
                    stdout,
                    "{}",
                    output::success_text(&format!(
                        "Posted planning item {id} as transaction {transaction_id}."
                    ))
                )?;
                Ok(())
            }
        },
    }
}

fn handle_scenario(
    db: Db,
    command: ScenarioCommand,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    match command {
        ScenarioCommand::Add(ScenarioAddArgs { name, note }) => {
            let scenario_id = db.add_planning_scenario(&NewPlanningScenario {
                name,
                note: normalize_optional_string(note),
            })?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created scenario {scenario_id}."))
            )?;
            Ok(())
        }
        ScenarioCommand::List(ScenarioListArgs { json }) => {
            let scenarios = db.list_planning_scenarios()?;
            output::write_planning_scenarios(stdout, &scenarios, json)
        }
        ScenarioCommand::Edit(ScenarioEditArgs {
            id,
            name,
            note,
            clear_note,
        }) => {
            if name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
                && note
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                && !clear_note
            {
                return Err(AppError::Validation(
                    "scenario edit requires --name, --note, or --clear-note".to_string(),
                ));
            }
            db.edit_planning_scenario(&UpdatePlanningScenario {
                id,
                name: normalize_optional_string(name),
                note: normalize_optional_string(note),
                clear_note,
            })?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Updated scenario {id}."))
            )?;
            Ok(())
        }
        ScenarioCommand::Delete(ScenarioDeleteArgs { id }) => {
            db.delete_planning_scenario(id)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Archived scenario {id}."))
            )?;
            Ok(())
        }
    }
}

fn handle_goal(db: Db, command: GoalCommand, stdout: &mut dyn Write) -> Result<(), AppError> {
    match command {
        GoalCommand::Add(GoalAddArgs {
            name,
            kind,
            account,
            target_amount,
            minimum_balance,
            due_on,
        }) => {
            let goal_id = db.add_planning_goal(&NewPlanningGoal {
                name,
                kind,
                account,
                target_amount_cents: target_amount
                    .map(|value| parse_amount_to_cents(&value))
                    .transpose()?,
                minimum_balance_cents: minimum_balance
                    .map(|value| parse_amount_to_cents(&value))
                    .transpose()?,
                due_on: normalize_optional_date(due_on)?,
            })?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created goal {goal_id}."))
            )?;
            Ok(())
        }
        GoalCommand::List(GoalListArgs { json }) => {
            let goals = db.list_planning_goals()?;
            output::write_planning_goals(stdout, &goals, json)
        }
        GoalCommand::Edit(GoalEditArgs {
            id,
            name,
            kind,
            account,
            target_amount,
            minimum_balance,
            due_on,
            clear_target_amount,
            clear_minimum_balance,
            clear_due_on,
        }) => {
            if name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
                && kind.is_none()
                && account
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                && target_amount.is_none()
                && minimum_balance.is_none()
                && due_on.is_none()
                && !clear_target_amount
                && !clear_minimum_balance
                && !clear_due_on
            {
                return Err(AppError::Validation(
                    "goal edit requires at least one field change".to_string(),
                ));
            }
            db.edit_planning_goal(&UpdatePlanningGoal {
                id,
                name: normalize_optional_string(name),
                kind,
                account: normalize_optional_string(account),
                target_amount_cents: target_amount
                    .map(|value| parse_amount_to_cents(&value))
                    .transpose()?,
                minimum_balance_cents: minimum_balance
                    .map(|value| parse_amount_to_cents(&value))
                    .transpose()?,
                due_on: normalize_optional_date(due_on)?,
                clear_target_amount,
                clear_minimum_balance,
                clear_due_on,
            })?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Updated goal {id}."))
            )?;
            Ok(())
        }
        GoalCommand::Delete(GoalDeleteArgs { id }) => {
            db.delete_planning_goal(id)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Archived goal {id}."))
            )?;
            Ok(())
        }
    }
}

fn handle_reconcile(
    db: Db,
    command: ReconcileCommand,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    match command {
        ReconcileCommand::Start(ReconcileStartArgs {
            account,
            statement_ending_on,
            statement_balance,
            transaction_ids,
        }) => {
            if transaction_ids.is_empty() {
                return Err(AppError::Validation(
                    "reconcile start requires at least one --transaction-id when used directly from the CLI"
                        .to_string(),
                ));
            }
            let reconciliation_id = db.start_reconciliation(
                &account,
                &normalize_date(&statement_ending_on)?,
                parse_amount_to_cents(&statement_balance)?,
                &transaction_ids,
            )?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created reconciliation {reconciliation_id}."))
            )?;
            Ok(())
        }
        ReconcileCommand::List(ReconcileListArgs { account, json }) => {
            let reconciliations = db.list_reconciliations(
                account
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            )?;
            output::write_reconciliations(stdout, &reconciliations, json)
        }
        ReconcileCommand::Delete(ReconcileDeleteArgs { id }) => {
            db.delete_reconciliation(id)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Removed reconciliation {id}."))
            )?;
            Ok(())
        }
    }
}

fn handle_recurring(
    db: Db,
    command: RecurringCommand,
    stdout: &mut dyn Write,
) -> Result<(), AppError> {
    match command {
        RecurringCommand::Add(RecurringAddArgs {
            name,
            kind,
            amount,
            account,
            to_account,
            category,
            payee,
            note,
            cadence,
            interval,
            day_of_month,
            weekday,
            start_on,
            next_due_on,
            end_on,
        }) => {
            let rule = NewRecurringRule {
                name,
                kind,
                amount_cents: parse_amount_to_cents(&amount)?,
                account,
                to_account,
                category,
                payee: normalize_optional_string(payee),
                note: normalize_optional_string(note),
                cadence,
                interval,
                day_of_month,
                weekday,
                start_on: normalize_date(&start_on)?,
                next_due_on: normalize_optional_date(next_due_on)?,
                end_on: normalize_optional_date(end_on)?,
            };
            let rule_id = db.add_recurring_rule(&rule)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Created recurring rule {rule_id}."))
            )?;
            Ok(())
        }
        RecurringCommand::Edit(RecurringEditArgs {
            id,
            name,
            kind,
            amount,
            account,
            to_account,
            category,
            payee,
            note,
            cadence,
            interval,
            day_of_month,
            weekday,
            start_on,
            next_due_on,
            end_on,
            clear_to_account,
            clear_category,
            clear_payee,
            clear_note,
            clear_day_of_month,
            clear_weekday,
            clear_next_due_on,
            clear_end_on,
        }) => {
            let patch = UpdateRecurringRule {
                id,
                name: normalize_optional_string(name),
                kind,
                amount_cents: match amount {
                    Some(value) => Some(parse_amount_to_cents(&value)?),
                    None => None,
                },
                account: normalize_optional_string(account),
                to_account: normalize_optional_string(to_account),
                category: normalize_optional_string(category),
                payee: normalize_optional_string(payee),
                note: normalize_optional_string(note),
                cadence,
                interval,
                day_of_month,
                weekday,
                start_on: normalize_optional_date(start_on)?,
                next_due_on: normalize_optional_date(next_due_on)?,
                end_on: normalize_optional_date(end_on)?,
                clear_to_account,
                clear_category,
                clear_payee,
                clear_note,
                clear_day_of_month,
                clear_weekday,
                clear_next_due_on,
                clear_end_on,
            };
            db.edit_recurring_rule(&patch)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Updated recurring rule {id}."))
            )?;
            Ok(())
        }
        RecurringCommand::List(RecurringListArgs { json }) => {
            let rules = db.list_recurring_rules()?;
            output::write_recurring_rules(stdout, &rules, json)
        }
        RecurringCommand::Pause(RecurringIdArgs { id }) => {
            db.pause_recurring_rule(id)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Paused recurring rule {id}."))
            )?;
            Ok(())
        }
        RecurringCommand::Resume(RecurringIdArgs { id }) => {
            db.resume_recurring_rule(id)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!("Resumed recurring rule {id}."))
            )?;
            Ok(())
        }
        RecurringCommand::Delete(RecurringIdArgs { id }) => {
            db.delete_recurring_rule(id)?;
            writeln!(
                stdout,
                "{}",
                output::warning_text(&format!("Deleted recurring rule {id}."))
            )?;
            Ok(())
        }
        RecurringCommand::Run(RecurringRunArgs { through }) => {
            let through = match through {
                Some(value) => normalize_date(&value)?,
                None => today_iso(),
            };
            let posted = db.run_due_recurring(&through)?;
            writeln!(
                stdout,
                "{}",
                output::success_text(&format!(
                    "Posted {posted} recurring transactions through {through}."
                ))
            )?;
            Ok(())
        }
    }
}
fn resolve_export_range(
    month: Option<String>,
    from: Option<String>,
    to: Option<String>,
    require_bounded_range: bool,
) -> Result<(Option<String>, Option<String>), AppError> {
    if let Some(month) = month {
        let (from, to) = month_range(&month)?;
        return Ok((Some(from), Some(to)));
    }

    let from = normalize_optional_date(from)?;
    let to = normalize_optional_date(to)?;

    if require_bounded_range && (from.is_none() || to.is_none()) {
        return Err(AppError::Validation(
            "summary export requires --month or both --from and --to".to_string(),
        ));
    }

    Ok((from, to))
}

fn normalize_optional_date(value: Option<String>) -> Result<Option<String>, AppError> {
    match value {
        Some(raw) => Ok(Some(normalize_date(&raw)?)),
        None => Ok(None),
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_date(raw: &str) -> Result<String, AppError> {
    let parsed = NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")?;
    Ok(parsed.format("%Y-%m-%d").to_string())
}

fn month_range(raw: &str) -> Result<(String, String), AppError> {
    let trimmed = raw.trim();
    let mut parts = trimmed.split('-');
    let year = parts
        .next()
        .ok_or_else(|| AppError::Validation("month must use YYYY-MM format".to_string()))?
        .parse::<i32>()
        .map_err(|_| AppError::Validation("month must use YYYY-MM format".to_string()))?;
    let month = parts
        .next()
        .ok_or_else(|| AppError::Validation("month must use YYYY-MM format".to_string()))?
        .parse::<u32>()
        .map_err(|_| AppError::Validation("month must use YYYY-MM format".to_string()))?;
    if parts.next().is_some() {
        return Err(AppError::Validation(
            "month must use YYYY-MM format".to_string(),
        ));
    }

    let first = NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
        AppError::Validation("month must use a real calendar month in YYYY-MM format".to_string())
    })?;
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .ok_or_else(|| {
        AppError::Validation("month must use a real calendar month in YYYY-MM format".to_string())
    })?;
    let last = next_month.pred_opt().ok_or_else(|| {
        AppError::Validation("month must use a real calendar month in YYYY-MM format".to_string())
    })?;

    Ok((
        first.format("%Y-%m-%d").to_string(),
        last.format("%Y-%m-%d").to_string(),
    ))
}

fn normalize_month(raw: &str) -> Result<String, AppError> {
    let (from, _) = month_range(raw)?;
    Ok(from[..7].to_string())
}

fn current_month_range() -> (String, String) {
    let today = Local::now().date_naive();
    let first = NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
        .expect("current month should always be valid");
    let next_month = if today.month() == 12 {
        NaiveDate::from_ymd_opt(today.year() + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(today.year(), today.month() + 1, 1)
    }
    .expect("next month should always be valid");
    let last = next_month.pred_opt().expect("previous day should exist");

    (
        first.format("%Y-%m-%d").to_string(),
        last.format("%Y-%m-%d").to_string(),
    )
}

pub fn today_iso() -> String {
    Local::now().date_naive().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tempfile::TempDir;

    use super::{ensure_interactive_db_ready, month_range, normalize_date};
    use crate::db::Db;

    #[test]
    fn normalizes_valid_dates() {
        assert_eq!(normalize_date("2026-03-13").unwrap(), "2026-03-13");
    }

    #[test]
    fn rejects_invalid_dates() {
        assert!(normalize_date("2026-02-30").is_err());
    }

    #[test]
    fn expands_month_to_date_range() {
        let (from, to) = month_range("2026-02").unwrap();
        assert_eq!(from, "2026-02-01");
        assert_eq!(to, "2026-02-28");
    }

    #[test]
    fn interactive_setup_initializes_missing_database_with_default_currency() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("tracker.db");
        let mut stdin = Cursor::new(b"\n");
        let mut stdout = Vec::new();

        ensure_interactive_db_ready(&db_path, &mut stdin, &mut stdout).unwrap();

        let db = Db::open_existing(&db_path).unwrap();
        assert_eq!(db.currency_code().unwrap(), "USD");

        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("No database was found"));
        assert!(output.contains("Initialized database"));
    }

    #[test]
    fn interactive_setup_retries_until_currency_is_valid() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("tracker.db");
        let mut stdin = Cursor::new(b"US\nEUR\n");
        let mut stdout = Vec::new();

        ensure_interactive_db_ready(&db_path, &mut stdin, &mut stdout).unwrap();

        let db = Db::open_existing(&db_path).unwrap();
        assert_eq!(db.currency_code().unwrap(), "EUR");

        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Enter a 3-letter code such as USD or EUR."));
    }
}
