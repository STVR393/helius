use std::fs;
use std::path::PathBuf;
use std::thread;

use assert_cmd::Command;
use predicates::prelude::*;
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;
use tiny_http::{Header, Response, Server, StatusCode};

fn db_path(temp_dir: &TempDir) -> PathBuf {
    temp_dir.path().join("tracker.db")
}

fn run_ok(temp_dir: &TempDir, args: &[&str]) -> String {
    let output = Command::cargo_bin("helius")
        .unwrap()
        .arg("--db")
        .arg(db_path(temp_dir))
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    String::from_utf8(output).unwrap()
}

fn run_err(temp_dir: &TempDir, args: &[&str]) -> String {
    let output = Command::cargo_bin("helius")
        .unwrap()
        .arg("--db")
        .arg(db_path(temp_dir))
        .args(args)
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();

    String::from_utf8(output).unwrap()
}

fn run_ok_env(temp_dir: &TempDir, args: &[&str], envs: &[(&str, &str)]) -> String {
    let mut command = Command::cargo_bin("helius").unwrap();
    command.arg("--db").arg(db_path(temp_dir)).args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command.assert().success().get_output().stdout.clone();
    String::from_utf8(output).unwrap()
}

struct MockMarketServer {
    base_url: String,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockMarketServer {
    fn start(expected_requests: usize) -> Self {
        let server = Server::http("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", server.server_addr());
        let handle = thread::spawn(move || {
            for _ in 0..expected_requests {
                let mut request = server.recv().unwrap();
                let mut body = String::new();
                request.as_reader().read_to_string(&mut body).unwrap();
                let ticker = if body.contains("MSFT") {
                    "MSFT"
                } else if body.contains("AAPL") {
                    "AAPL"
                } else {
                    "SPY"
                };
                let payload = if request.url().contains("/answer") {
                    mock_valyu_response(ticker)
                } else {
                    mock_tavily_response(ticker)
                };
                let response = Response::from_string(payload)
                    .with_status_code(StatusCode(200))
                    .with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    );
                request.respond(response).unwrap();
            }
        });

        Self {
            base_url,
            handle: Some(handle),
        }
    }

    fn valyu_base(&self) -> String {
        format!("{}/v1", self.base_url)
    }

    fn tavily_base(&self) -> &str {
        &self.base_url
    }

    fn finish(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

fn mock_valyu_response(ticker: &str) -> String {
    let (name, price, change, change_percent) = match ticker {
        "MSFT" => ("Microsoft Corp.", 410.12, -1.23, -0.30),
        "AAPL" => ("Apple Inc.", 210.25, 1.75, 0.84),
        _ => ("SPDR S&P 500 ETF Trust", 520.50, 0.55, 0.11),
    };
    format!(
        "{{\"structured_output\":{{\"ticker\":\"{ticker}\",\"name\":\"{name}\",\"exchange\":\"NASDAQ\",\"currency\":\"USD\",\"last_price\":{price},\"change\":{change},\"change_percent\":{change_percent},\"market_state\":\"regular\"}}}}"
    )
}

fn mock_tavily_response(ticker: &str) -> String {
    format!(
        "{{\"results\":[{{\"title\":\"{ticker} headline one\",\"url\":\"https://example.com/{ticker}/1\",\"content\":\"{ticker} summary one\",\"published_at\":\"2026-03-13T10:00:00Z\",\"source\":\"Example News\"}},{{\"title\":\"{ticker} headline two\",\"url\":\"https://example.com/{ticker}/2\",\"content\":\"{ticker} summary two\",\"published_at\":\"2026-03-13T12:00:00Z\",\"source\":\"Example Wire\"}}]}}"
    )
}

fn seed_basic_data(temp_dir: &TempDir) {
    run_ok(temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        temp_dir,
        &[
            "account",
            "add",
            "Checking",
            "--type",
            "checking",
            "--opening-balance",
            "1000.00",
            "--opened-on",
            "2026-01-01",
        ],
    );
    run_ok(temp_dir, &["account", "add", "Cash", "--type", "cash"]);
    run_ok(temp_dir, &["category", "add", "Salary", "--kind", "income"]);
    run_ok(
        temp_dir,
        &["category", "add", "Groceries", "--kind", "expense"],
    );
    run_ok(
        temp_dir,
        &[
            "tx",
            "add",
            "--type",
            "income",
            "--amount",
            "2500.00",
            "--date",
            "2026-02-10",
            "--account",
            "Checking",
            "--category",
            "Salary",
            "--payee",
            "Employer",
        ],
    );
    run_ok(
        temp_dir,
        &[
            "tx",
            "add",
            "--type",
            "expense",
            "--amount",
            "125.40",
            "--date",
            "2026-02-11",
            "--account",
            "Checking",
            "--category",
            "Groceries",
            "--payee",
            "Supermarket",
        ],
    );
    run_ok(
        temp_dir,
        &[
            "tx",
            "add",
            "--type",
            "transfer",
            "--amount",
            "200.00",
            "--date",
            "2026-02-12",
            "--account",
            "Checking",
            "--to-account",
            "Cash",
            "--note",
            "ATM withdrawal",
        ],
    );
}

fn transaction_map(temp_dir: &TempDir) -> Vec<Value> {
    serde_json::from_str::<Value>(&run_ok(temp_dir, &["tx", "list", "--json"]))
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn help_exits_successfully() {
    Command::cargo_bin("helius")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: helius"));
}

#[test]
fn no_args_prints_help_when_not_tty() {
    Command::cargo_bin("helius")
        .unwrap()
        .assert()
        .success()
        .stdout(predicate::str::contains("Personal finance tracker CLI"))
        .stdout(predicate::str::contains("Commands:"));
}

#[test]
fn shell_subcommand_enters_interactive_shell() {
    Command::cargo_bin("helius")
        .unwrap()
        .arg("shell")
        .write_stdin("exit\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("Helius interactive shell"))
        .stdout(predicate::str::contains("Bye."));
}

#[test]
fn init_creates_database_and_missing_db_fails_cleanly() {
    let temp_dir = TempDir::new().unwrap();
    let db = db_path(&temp_dir);

    Command::cargo_bin("helius")
        .unwrap()
        .arg("--db")
        .arg(&db)
        .arg("balance")
        .assert()
        .failure()
        .stderr(predicate::str::contains("run `helius init` first"));

    let stdout = run_ok(&temp_dir, &["init", "--currency", "USD"]);
    assert!(stdout.contains("Initialized database"));
    assert!(db.exists());
}

#[test]
fn duplicate_accounts_and_categories_are_rejected() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    run_ok(&temp_dir, &["account", "add", "Wallet", "--type", "cash"]);
    let account_error = run_err(&temp_dir, &["account", "add", "Wallet", "--type", "cash"]);
    assert!(account_error.contains("account `Wallet` already exists"));

    run_ok(
        &temp_dir,
        &["category", "add", "Salary", "--kind", "income"],
    );
    let category_error = run_err(
        &temp_dir,
        &["category", "add", "Salary", "--kind", "income"],
    );
    assert!(category_error.contains("category `Salary` already exists"));
}

#[test]
fn accounts_can_be_edited_and_only_unused_accounts_can_be_archived() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &["account", "add", "Travel Fund", "--type", "savings"],
    );
    run_ok(
        &temp_dir,
        &[
            "account",
            "edit",
            "Travel Fund",
            "--name",
            "Emergency Fund",
            "--type",
            "savings",
            "--opening-balance",
            "500.00",
            "--opened-on",
            "2026-02-01",
        ],
    );

    let accounts: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["account", "list", "--json"])).unwrap();
    let emergency_fund = accounts
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "Emergency Fund")
        .cloned()
        .unwrap();
    assert_eq!(emergency_fund["kind"], "savings");
    assert_eq!(emergency_fund["opening_balance_cents"], 50000);
    assert_eq!(emergency_fund["opened_on"], "2026-02-01");

    let checking_delete_error = run_err(&temp_dir, &["account", "delete", "Checking"]);
    assert!(checking_delete_error
        .contains("cannot archive account while transactions still reference it"));

    run_ok(&temp_dir, &["account", "delete", "Emergency Fund"]);

    let remaining: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["account", "list", "--json"])).unwrap();
    assert!(remaining
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["name"] != "Emergency Fund"));
}

#[test]
fn categories_can_be_edited_and_archived() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Groceries", "--kind", "expense"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Salary", "--kind", "income"],
    );

    run_ok(
        &temp_dir,
        &["category", "edit", "Groceries", "--name", "Food"],
    );

    let edited: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["category", "list", "--json"])).unwrap();
    let categories = edited.as_array().unwrap();
    assert!(categories
        .iter()
        .any(|row| { row["name"] == "Food" && row["kind"] == "expense" }));

    run_ok(&temp_dir, &["category", "delete", "Food"]);

    let after_delete: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["category", "list", "--json"])).unwrap();
    let categories = after_delete.as_array().unwrap();
    assert_eq!(categories.len(), 1);
    assert_eq!(categories[0]["name"], "Salary");

    let archived_ref_error = run_err(
        &temp_dir,
        &[
            "tx",
            "add",
            "--type",
            "expense",
            "--amount",
            "10.00",
            "--date",
            "2026-02-10",
            "--account",
            "Checking",
            "--category",
            "Food",
        ],
    );
    assert!(archived_ref_error.contains("category `Food` was not found"));
}

#[test]
fn category_kind_change_is_blocked_once_category_is_used() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    let error = run_err(
        &temp_dir,
        &["category", "edit", "Groceries", "--kind", "income"],
    );
    assert!(error.contains("cannot change category kind"));
}

#[test]
fn ledger_flow_updates_balances_transactions_and_summary() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    let balances: Value = serde_json::from_str(&run_ok(&temp_dir, &["balance", "--json"])).unwrap();
    let balances = balances.as_array().unwrap();
    assert_eq!(balances.len(), 2);
    assert_eq!(balances[0]["account_name"], "Cash");
    assert_eq!(balances[0]["current_balance_cents"], 20000);
    assert_eq!(balances[1]["account_name"], "Checking");
    assert_eq!(balances[1]["current_balance_cents"], 317460);

    let transactions: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "tx",
            "list",
            "--account",
            "Checking",
            "--limit",
            "10",
            "--json",
        ],
    ))
    .unwrap();
    let transactions = transactions.as_array().unwrap();
    assert_eq!(transactions.len(), 3);
    assert_eq!(transactions[0]["kind"], "transfer");
    assert_eq!(transactions[1]["kind"], "expense");
    assert_eq!(transactions[2]["kind"], "income");

    let summary: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "summary",
            "range",
            "--from",
            "2026-02-01",
            "--to",
            "2026-02-28",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(summary["transaction_count"], 3);
    assert_eq!(summary["income_cents"], 250000);
    assert_eq!(summary["expense_cents"], 12540);
    assert_eq!(summary["net_cents"], 237460);
    assert_eq!(summary["transfer_in_cents"], 20000);
    assert_eq!(summary["transfer_out_cents"], 20000);
}

#[test]
fn tx_list_search_filters_across_text_fields() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    let payee_matches: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["tx", "list", "--search", "market", "--json"],
    ))
    .unwrap();
    let payee_matches = payee_matches.as_array().unwrap();
    assert_eq!(payee_matches.len(), 1);
    assert_eq!(payee_matches[0]["kind"], "expense");

    let note_matches: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["tx", "list", "--search", "atm", "--json"],
    ))
    .unwrap();
    let note_matches = note_matches.as_array().unwrap();
    assert_eq!(note_matches.len(), 1);
    assert_eq!(note_matches[0]["kind"], "transfer");

    let category_matches: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["tx", "list", "--search", "grocer", "--json"],
    ))
    .unwrap();
    let category_matches = category_matches.as_array().unwrap();
    assert_eq!(category_matches.len(), 1);
    assert_eq!(category_matches[0]["category_name"], "Groceries");
}

#[test]
fn tx_edit_delete_restore_and_reconcile_flow() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    let transactions = transaction_map(&temp_dir);
    let expense_id = transactions
        .iter()
        .find(|tx| tx["kind"] == "expense")
        .unwrap()["id"]
        .as_i64()
        .unwrap();
    let ids: Vec<String> = transactions
        .iter()
        .filter(|tx| tx["account_name"] == "Checking")
        .map(|tx| tx["id"].as_i64().unwrap().to_string())
        .collect();

    run_ok(
        &temp_dir,
        &[
            "tx",
            "edit",
            &expense_id.to_string(),
            "--note",
            "Weekly groceries",
        ],
    );
    let edited = transaction_map(&temp_dir);
    let edited_expense = edited.iter().find(|tx| tx["id"] == expense_id).unwrap();
    assert_eq!(edited_expense["note"], "Weekly groceries");

    run_ok(&temp_dir, &["tx", "delete", &expense_id.to_string()]);
    let after_delete: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["balance", "--json"])).unwrap();
    let checking_balance = after_delete
        .as_array()
        .unwrap()
        .iter()
        .find(|balance| balance["account_name"] == "Checking")
        .unwrap();
    assert_eq!(checking_balance["current_balance_cents"], 330000);

    run_ok(&temp_dir, &["tx", "restore", &expense_id.to_string()]);
    let restored = transaction_map(&temp_dir);
    let restored_expense = restored.iter().find(|tx| tx["id"] == expense_id).unwrap();
    assert_eq!(restored_expense["deleted_at"], Value::Null);

    let mut reconcile_args = vec![
        "reconcile",
        "start",
        "--account",
        "Checking",
        "--to",
        "2026-02-12",
        "--statement-balance",
        "3174.60",
    ];
    for id in &ids {
        reconcile_args.push("--transaction-id");
        reconcile_args.push(id);
    }
    run_ok(&temp_dir, &reconcile_args);

    let reconciliation_list: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["reconcile", "list", "--json"])).unwrap();
    let reconciliation_id = reconciliation_list.as_array().unwrap()[0]["id"]
        .as_i64()
        .unwrap();

    let reconcile_edit_error = run_err(
        &temp_dir,
        &["tx", "edit", &expense_id.to_string(), "--note", "Locked"],
    );
    assert!(reconcile_edit_error.contains("reconciled transactions cannot be edited"));

    run_ok(
        &temp_dir,
        &["reconcile", "delete", &reconciliation_id.to_string()],
    );
    run_ok(
        &temp_dir,
        &[
            "tx",
            "edit",
            &expense_id.to_string(),
            "--note",
            "Unlocked again",
        ],
    );
}

#[test]
fn recurring_rule_run_posts_transactions() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Salary", "--kind", "income"],
    );

    run_ok(
        &temp_dir,
        &[
            "recurring",
            "add",
            "Monthly Salary",
            "--type",
            "income",
            "--amount",
            "2500.00",
            "--account",
            "Checking",
            "--category",
            "Salary",
            "--cadence",
            "monthly",
            "--interval",
            "1",
            "--day-of-month",
            "1",
            "--start-on",
            "2026-02-01",
        ],
    );
    run_ok(&temp_dir, &["recurring", "run", "--through", "2026-03-31"]);

    let transactions = transaction_map(&temp_dir);
    assert_eq!(transactions.len(), 2);
    assert!(transactions
        .iter()
        .all(|tx| tx["recurring_rule_id"].is_number()));

    let balances: Value = serde_json::from_str(&run_ok(&temp_dir, &["balance", "--json"])).unwrap();
    assert_eq!(
        balances.as_array().unwrap()[0]["current_balance_cents"],
        500000
    );
}

#[test]
fn export_writes_transaction_and_summary_csv_files() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    let tx_export = temp_dir.path().join("exports").join("transactions.csv");
    let summary_export = temp_dir.path().join("exports").join("summary.csv");

    run_ok(
        &temp_dir,
        &[
            "export",
            "csv",
            "--kind",
            "transactions",
            "--output",
            tx_export.to_str().unwrap(),
            "--month",
            "2026-02",
        ],
    );
    run_ok(
        &temp_dir,
        &[
            "export",
            "csv",
            "--kind",
            "summary",
            "--output",
            summary_export.to_str().unwrap(),
            "--from",
            "2026-02-01",
            "--to",
            "2026-02-28",
        ],
    );

    let tx_csv = fs::read_to_string(&tx_export).unwrap();
    assert!(tx_csv.contains("txn_date,kind,amount"));
    assert!(tx_csv.contains("2026-02-10,income,2500.00"));
    assert!(tx_csv.contains("recurring_rule_id"));

    let summary_csv = fs::read_to_string(&summary_export).unwrap();
    assert!(summary_csv.contains("from,to,account_id,account_name,transaction_count"));
    assert!(summary_csv.contains("2026-02-01,2026-02-28"));
    assert!(summary_csv.contains("2374.60"));
}

#[test]
fn invalid_transactions_fail_with_clear_messages() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Salary", "--kind", "income"],
    );

    let missing_category = run_err(
        &temp_dir,
        &[
            "tx",
            "add",
            "--type",
            "income",
            "--amount",
            "100.00",
            "--date",
            "2026-02-10",
            "--account",
            "Checking",
        ],
    );
    assert!(missing_category.contains("require a category"));

    let same_account_transfer = run_err(
        &temp_dir,
        &[
            "tx",
            "add",
            "--type",
            "transfer",
            "--amount",
            "25.00",
            "--date",
            "2026-02-10",
            "--account",
            "Checking",
            "--to-account",
            "Checking",
        ],
    );
    assert!(same_account_transfer.contains("different from the source account"));

    let summary_category_error = run_err(
        &temp_dir,
        &[
            "export",
            "csv",
            "--kind",
            "summary",
            "--output",
            temp_dir.path().join("summary.csv").to_str().unwrap(),
            "--from",
            "2026-02-01",
            "--to",
            "2026-02-28",
            "--category",
            "Salary",
        ],
    );
    assert!(summary_category_error.contains("does not support category filters"));
}

#[test]
fn budget_commands_track_monthly_status() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-02",
            "--amount",
            "300.00",
        ],
    );

    let budgets: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["budget", "list", "--json"])).unwrap();
    let budget_row = budgets.as_array().unwrap().first().unwrap();
    assert_eq!(budget_row["month"], "2026-02");
    assert_eq!(budget_row["category_name"], "Groceries");
    assert_eq!(budget_row["amount_cents"], 30000);

    let status: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["budget", "status", "2026-02", "--json"],
    ))
    .unwrap();
    let groceries = status
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["category_name"] == "Groceries")
        .unwrap();
    assert_eq!(groceries["budget_cents"], 30000);
    assert_eq!(groceries["spent_cents"], 12540);
    assert_eq!(groceries["remaining_cents"], 17460);
    assert_eq!(groceries["over_budget"], false);
}

#[test]
fn budget_delete_removes_baseline_budget_entry() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-02",
            "--amount",
            "300.00",
            "--account",
            "Checking",
        ],
    );
    run_ok(
        &temp_dir,
        &["budget", "delete", "Groceries", "--month", "2026-02"],
    );

    let budgets: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["budget", "list", "--month", "2026-02", "--json"],
    ))
    .unwrap();
    assert!(budgets.as_array().unwrap().is_empty());
}

#[test]
fn planning_schema_migration_from_v7_to_v8_recreates_planning_tables() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    conn.execute_batch(
        "DROP TABLE planning_items;
         DROP TABLE planning_scenarios;
         DROP TABLE scenario_budget_overrides;
         DROP TABLE planning_goals;
         UPDATE metadata SET schema_version = 7 WHERE id = 1;",
    )
    .unwrap();
    drop(conn);

    let scenarios: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["scenario", "list", "--json"])).unwrap();
    assert_eq!(scenarios.as_array().unwrap().len(), 0);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT schema_version FROM metadata WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let planning_items_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'planning_items'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let planning_goals_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'planning_goals'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 8);
    assert_eq!(planning_items_exists, 1);
    assert_eq!(planning_goals_exists, 1);
}

#[test]
fn open_existing_repairs_budget_account_column_when_metadata_is_already_v8() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);
    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-03",
            "--amount",
            "200.00",
        ],
    );

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;
         DROP INDEX IF EXISTS idx_budgets_month;
         DROP INDEX IF EXISTS idx_budgets_account;
         ALTER TABLE budgets RENAME TO budgets_old;
         CREATE TABLE budgets (
             id INTEGER PRIMARY KEY,
             month TEXT NOT NULL,
             category_id INTEGER NOT NULL REFERENCES categories(id),
             amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
             created_at TEXT NOT NULL,
             updated_at TEXT NOT NULL,
             UNIQUE(month, category_id)
         );
         INSERT INTO budgets (id, month, category_id, amount_cents, created_at, updated_at)
         SELECT id, month, category_id, amount_cents, created_at, updated_at
         FROM budgets_old;
         DROP TABLE budgets_old;
         UPDATE metadata SET schema_version = 8 WHERE id = 1;
         PRAGMA foreign_keys = ON;",
    )
    .unwrap();
    drop(conn);

    let budgets: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["budget", "list", "--json"])).unwrap();
    assert_eq!(budgets.as_array().unwrap().len(), 1);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    let mut statement = conn.prepare("PRAGMA table_info(budgets)").unwrap();
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(columns.iter().any(|column| column == "account_id"));
}

#[test]
fn budget_account_mapping_and_plan_item_post_round_trip() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-03",
            "--amount",
            "300.00",
            "--account",
            "Checking",
        ],
    );

    let budgets: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["budget", "list", "--json"])).unwrap();
    let budget_row = budgets.as_array().unwrap().first().unwrap();
    assert_eq!(budget_row["account_name"], "Checking");

    run_ok(
        &temp_dir,
        &[
            "plan",
            "item",
            "add",
            "Insurance",
            "--type",
            "expense",
            "--amount",
            "240.00",
            "--date",
            "2026-03-20",
            "--account",
            "Checking",
            "--category",
            "Groceries",
            "--note",
            "Quarterly premium",
        ],
    );

    let items: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["plan", "item", "list", "--json"])).unwrap();
    let item_id = items[0]["id"].as_i64().unwrap();
    assert_eq!(items[0]["linked_transaction_id"], Value::Null);

    run_ok(&temp_dir, &["plan", "item", "post", &item_id.to_string()]);

    let items_after: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["plan", "item", "list", "--json"])).unwrap();
    assert!(items_after[0]["linked_transaction_id"].as_i64().is_some());

    let txs: Value = serde_json::from_str(&run_ok(&temp_dir, &["tx", "list", "--json"])).unwrap();
    assert!(txs
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["note"] == "Quarterly premium"));
}

#[test]
fn forecast_show_includes_bills_goals_and_recurring_items() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-03",
            "--amount",
            "200.00",
            "--account",
            "Checking",
        ],
    );
    run_ok(
        &temp_dir,
        &[
            "goal",
            "add",
            "Buffer",
            "--kind",
            "balance-target",
            "--account",
            "Checking",
            "--minimum-balance",
            "1500.00",
        ],
    );
    run_ok(
        &temp_dir,
        &[
            "recurring",
            "add",
            "Gym",
            "--type",
            "expense",
            "--amount",
            "50.00",
            "--account",
            "Checking",
            "--category",
            "Groceries",
            "--cadence",
            "monthly",
            "--day-of-month",
            "20",
            "--start-on",
            "2026-03-01",
        ],
    );
    run_ok(
        &temp_dir,
        &[
            "plan",
            "item",
            "add",
            "Insurance",
            "--type",
            "expense",
            "--amount",
            "240.00",
            "--date",
            "2026-03-22",
            "--account",
            "Checking",
            "--category",
            "Groceries",
        ],
    );

    let forecast: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["forecast", "show", "--json"])).unwrap();
    assert_eq!(forecast["scenario"]["name"], "baseline");
    assert_eq!(forecast["daily"].as_array().unwrap().len(), 90);
    assert!(forecast["goal_status"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["name"] == "Buffer"));
    assert!(forecast["bill_calendar"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["title"] == "Insurance"));
    assert!(forecast["bill_calendar"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["title"] == "Gym"));
}

#[test]
fn scenario_specific_planning_items_change_the_forecast() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(&temp_dir, &["scenario", "add", "Downside"]);
    run_ok(
        &temp_dir,
        &[
            "plan",
            "item",
            "add",
            "Emergency Repair",
            "--scenario",
            "Downside",
            "--type",
            "expense",
            "--amount",
            "700.00",
            "--date",
            "2026-03-24",
            "--account",
            "Checking",
            "--category",
            "Groceries",
        ],
    );

    let baseline: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["forecast", "show", "--json"])).unwrap();
    let downside: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["forecast", "show", "--scenario", "Downside", "--json"],
    ))
    .unwrap();

    let baseline_net: i64 = baseline["daily"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["net_cents"].as_i64().unwrap())
        .sum();
    let downside_net: i64 = downside["daily"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["net_cents"].as_i64().unwrap())
        .sum();

    assert_eq!(downside["scenario"]["name"], "Downside");
    assert!(downside_net < baseline_net);
    assert_eq!(baseline_net - downside_net, 70000);
}

#[test]
fn scenario_commands_edit_and_archive_cleanly() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &[
            "scenario",
            "add",
            "Stress Case",
            "--note",
            "High expense month",
        ],
    );

    let scenarios: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["scenario", "list", "--json"])).unwrap();
    let scenario = scenarios
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "Stress Case")
        .cloned()
        .unwrap();
    let scenario_id = scenario["id"].as_i64().unwrap();
    assert_eq!(scenario["note"], "High expense month");

    let scenario_id_text = scenario_id.to_string();
    run_ok(
        &temp_dir,
        &[
            "scenario",
            "edit",
            &scenario_id_text,
            "--name",
            "Downside",
            "--note",
            "Updated stress case",
        ],
    );

    let edited: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["scenario", "list", "--json"])).unwrap();
    let edited_row = edited
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"].as_i64() == Some(scenario_id) && row["name"] == "Downside")
        .cloned()
        .unwrap();
    assert_eq!(edited_row["note"], "Updated stress case");

    run_ok(&temp_dir, &["scenario", "delete", &scenario_id_text]);

    let after_delete: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["scenario", "list", "--json"])).unwrap();
    assert!(after_delete
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["id"].as_i64() != Some(scenario_id)));
    assert!(
        run_err(&temp_dir, &["forecast", "show", "--scenario", "Downside"],)
            .contains("scenario `Downside` was not found")
    );
}

#[test]
fn scenario_budget_overrides_change_budget_views_and_forecast() {
    let temp_dir = TempDir::new().unwrap();
    seed_basic_data(&temp_dir);

    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-03",
            "--amount",
            "200.00",
            "--account",
            "Checking",
        ],
    );
    run_ok(&temp_dir, &["scenario", "add", "Stress"]);
    run_ok(
        &temp_dir,
        &[
            "budget",
            "set",
            "Groceries",
            "--month",
            "2026-03",
            "--amount",
            "500.00",
            "--scenario",
            "Stress",
        ],
    );

    let baseline_budgets: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["budget", "list", "--month", "2026-03", "--json"],
    ))
    .unwrap();
    let baseline_row = baseline_budgets.as_array().unwrap().first().unwrap();
    assert_eq!(baseline_row["amount_cents"], 20000);
    assert_eq!(baseline_row["scenario_name"], Value::Null);
    assert_eq!(baseline_row["is_override"], false);

    let stress_budgets: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "budget",
            "list",
            "--month",
            "2026-03",
            "--scenario",
            "Stress",
            "--json",
        ],
    ))
    .unwrap();
    let stress_row = stress_budgets.as_array().unwrap().first().unwrap();
    assert_eq!(stress_row["amount_cents"], 50000);
    assert_eq!(stress_row["account_name"], "Checking");
    assert_eq!(stress_row["scenario_name"], "Stress");
    assert_eq!(stress_row["is_override"], true);

    let stress_status: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "budget",
            "status",
            "2026-03",
            "--scenario",
            "Stress",
            "--json",
        ],
    ))
    .unwrap();
    let groceries = stress_status
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["category_name"] == "Groceries")
        .unwrap();
    assert_eq!(groceries["budget_cents"], 50000);
    assert_eq!(groceries["account_name"], "Checking");
    assert_eq!(groceries["scenario_name"], "Stress");
    assert_eq!(groceries["is_override"], true);

    let baseline_forecast: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["forecast", "show", "--json"])).unwrap();
    let stress_forecast: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["forecast", "show", "--scenario", "Stress", "--json"],
    ))
    .unwrap();

    let baseline_net: i64 = baseline_forecast["daily"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["net_cents"].as_i64().unwrap())
        .sum();
    let stress_net: i64 = stress_forecast["daily"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["net_cents"].as_i64().unwrap())
        .sum();

    assert_eq!(stress_forecast["scenario"]["name"], "Stress");
    assert_eq!(baseline_net - stress_net, 30000);

    run_ok(
        &temp_dir,
        &[
            "budget",
            "delete",
            "Groceries",
            "--month",
            "2026-03",
            "--scenario",
            "Stress",
        ],
    );

    let reset_budgets: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "budget",
            "list",
            "--month",
            "2026-03",
            "--scenario",
            "Stress",
            "--json",
        ],
    ))
    .unwrap();
    let reset_row = reset_budgets.as_array().unwrap().first().unwrap();
    assert_eq!(reset_row["amount_cents"], 20000);
    assert_eq!(reset_row["is_override"], false);

    let reset_forecast: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["forecast", "show", "--scenario", "Stress", "--json"],
    ))
    .unwrap();
    let reset_net: i64 = reset_forecast["daily"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["net_cents"].as_i64().unwrap())
        .sum();
    assert_eq!(reset_net, baseline_net);
}

#[test]
fn csv_import_supports_dry_run_and_duplicate_skips() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Groceries", "--kind", "expense"],
    );

    let csv_path = temp_dir.path().join("bank.csv");
    fs::write(
        &csv_path,
        "Date,Amount,Description,Category\n2026-03-01,-15.25,Coffee,Groceries\n2026-03-02,-42.10,Market,Groceries\n",
    )
    .unwrap();

    let dry_run: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "import",
            "csv",
            "--input",
            csv_path.to_str().unwrap(),
            "--account",
            "Checking",
            "--date-column",
            "Date",
            "--amount-column",
            "Amount",
            "--description-column",
            "Description",
            "--category-column",
            "Category",
            "--dry-run",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["imported_count"], 2);
    assert_eq!(dry_run["duplicate_count"], 0);

    let imported: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "import",
            "csv",
            "--input",
            csv_path.to_str().unwrap(),
            "--account",
            "Checking",
            "--date-column",
            "Date",
            "--amount-column",
            "Amount",
            "--description-column",
            "Description",
            "--category-column",
            "Category",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(imported["dry_run"], false);
    assert_eq!(imported["imported_count"], 2);
    assert_eq!(imported["duplicate_count"], 0);

    let transactions = transaction_map(&temp_dir);
    assert_eq!(transactions.len(), 2);
    assert!(transactions.iter().all(|tx| tx["kind"] == "expense"));

    let duplicate_run: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "import",
            "csv",
            "--input",
            csv_path.to_str().unwrap(),
            "--account",
            "Checking",
            "--date-column",
            "Date",
            "--amount-column",
            "Amount",
            "--description-column",
            "Description",
            "--category-column",
            "Category",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(duplicate_run["imported_count"], 0);
    assert_eq!(duplicate_run["duplicate_count"], 2);
}

#[test]
fn recurring_edit_can_switch_from_monthly_to_weekly() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Salary", "--kind", "income"],
    );

    run_ok(
        &temp_dir,
        &[
            "recurring",
            "add",
            "Monthly Salary",
            "--type",
            "income",
            "--amount",
            "2500.00",
            "--account",
            "Checking",
            "--category",
            "Salary",
            "--cadence",
            "monthly",
            "--interval",
            "1",
            "--day-of-month",
            "1",
            "--start-on",
            "2026-02-01",
        ],
    );

    let rules: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    let rule_id = rules.as_array().unwrap()[0]["id"].as_i64().unwrap();

    run_ok(
        &temp_dir,
        &[
            "recurring",
            "edit",
            &rule_id.to_string(),
            "--cadence",
            "weekly",
            "--weekday",
            "fri",
            "--clear-day-of-month",
        ],
    );

    let updated: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    let rule = &updated.as_array().unwrap()[0];
    assert_eq!(rule["cadence"], "weekly");
    assert_eq!(rule["weekday"], "fri");
    assert!(rule["day_of_month"].is_null());
}

#[test]
fn recurring_rules_accept_explicit_next_due_on() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Housing", "--kind", "expense"],
    );

    run_ok(
        &temp_dir,
        &[
            "recurring",
            "add",
            "Rent",
            "--type",
            "expense",
            "--amount",
            "900.00",
            "--account",
            "Checking",
            "--category",
            "Housing",
            "--cadence",
            "monthly",
            "--interval",
            "1",
            "--day-of-month",
            "6",
            "--start-on",
            "2026-03-01",
            "--next-due-on",
            "2026-04-06",
        ],
    );

    let rules: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    let rule = &rules.as_array().unwrap()[0];
    let rule_id = rule["id"].as_i64().unwrap();
    assert_eq!(rule["next_due_on"], "2026-04-06");

    run_ok(
        &temp_dir,
        &[
            "recurring",
            "edit",
            &rule_id.to_string(),
            "--next-due-on",
            "2026-05-06",
        ],
    );

    let updated: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    assert_eq!(updated.as_array().unwrap()[0]["next_due_on"], "2026-05-06");
}

#[test]
fn forecast_show_does_not_advance_recurring_next_due_on() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Housing", "--kind", "expense"],
    );
    run_ok(
        &temp_dir,
        &[
            "recurring",
            "add",
            "Rent",
            "--type",
            "expense",
            "--amount",
            "900.00",
            "--account",
            "Checking",
            "--category",
            "Housing",
            "--cadence",
            "monthly",
            "--day-of-month",
            "6",
            "--start-on",
            "2026-03-17",
        ],
    );

    let before: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    assert_eq!(before.as_array().unwrap()[0]["next_due_on"], "2026-04-06");

    let _forecast: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["forecast", "show", "--json"])).unwrap();

    let after: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    assert_eq!(after.as_array().unwrap()[0]["next_due_on"], "2026-04-06");
}

#[test]
fn recurring_list_prefers_pending_occurrence_over_stored_next_pointer() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Housing", "--kind", "expense"],
    );
    run_ok(
        &temp_dir,
        &[
            "recurring",
            "add",
            "Rent",
            "--type",
            "expense",
            "--amount",
            "900.00",
            "--account",
            "Checking",
            "--category",
            "Housing",
            "--cadence",
            "monthly",
            "--day-of-month",
            "6",
            "--start-on",
            "2026-03-17",
        ],
    );

    let connection = Connection::open(db_path(&temp_dir)).unwrap();
    connection
        .execute(
            "INSERT INTO recurring_occurrences (rule_id, due_on, transaction_id, status, created_at)
             VALUES (1, '2026-04-06', NULL, 'pending', '2026-03-17T12:00:00+02:00')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE recurring_rules
             SET next_due_on = '2027-03-06',
                 updated_at = '2026-03-17T12:00:01+02:00'
             WHERE id = 1",
            [],
        )
        .unwrap();

    let rules: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["recurring", "list", "--json"])).unwrap();
    assert_eq!(rules.as_array().unwrap()[0]["next_due_on"], "2026-04-06");
}

#[test]
fn csv_import_supports_default_category_without_category_column() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &["account", "add", "Checking", "--type", "checking"],
    );
    run_ok(
        &temp_dir,
        &["category", "add", "Groceries", "--kind", "expense"],
    );

    let csv_path = temp_dir.path().join("bank-no-category.csv");
    fs::write(
        &csv_path,
        "Date,Amount,Description\n2026-03-04,-18.50,Coffee\n2026-03-05,-65.00,Market\n",
    )
    .unwrap();

    let imported: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "import",
            "csv",
            "--input",
            csv_path.to_str().unwrap(),
            "--account",
            "Checking",
            "--date-column",
            "Date",
            "--amount-column",
            "Amount",
            "--description-column",
            "Description",
            "--category",
            "Groceries",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(imported["imported_count"], 2);
    assert_eq!(imported["duplicate_count"], 0);

    let transactions = transaction_map(&temp_dir);
    assert_eq!(transactions.len(), 2);
    assert!(transactions
        .iter()
        .all(|tx| tx["category_name"] == "Groceries" && tx["kind"] == "expense"));
}

#[test]
fn market_schema_migration_from_v4_to_v6_recreates_market_tables() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    conn.execute_batch(
        "DROP TABLE watchlist_items;
         DROP TABLE market_cache;
         DROP TABLE market_settings;
         DROP TABLE market_refresh_runs;
         UPDATE metadata SET schema_version = 4 WHERE id = 1;",
    )
    .unwrap();
    drop(conn);

    let watchlist: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["watchlist", "list", "--json"])).unwrap();
    assert_eq!(watchlist.as_array().unwrap().len(), 0);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT schema_version FROM metadata WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let watchlist_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'watchlist_items'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let cache_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'market_cache'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let settings_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'market_settings'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let refresh_runs_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'market_refresh_runs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 8);
    assert_eq!(watchlist_exists, 1);
    assert_eq!(cache_exists, 1);
    assert_eq!(settings_exists, 1);
    assert_eq!(refresh_runs_exists, 1);
}

#[test]
fn open_existing_repairs_missing_recurring_tables_before_running_commands() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;
         DROP TABLE recurring_occurrences;
         DROP TABLE recurring_rules;",
    )
    .unwrap();
    drop(conn);

    let watchlist: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["watchlist", "list", "--json"])).unwrap();
    assert_eq!(watchlist.as_array().unwrap().len(), 0);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    let recurring_rules_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'recurring_rules'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let recurring_occurrences_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'recurring_occurrences'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recurring_rules_exists, 1);
    assert_eq!(recurring_occurrences_exists, 1);
}
#[test]
fn watchlist_refresh_populates_cached_quotes_and_remove_updates_list() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(&temp_dir, &["watchlist", "add", "AAPL", "--label", "Apple"]);
    run_ok(
        &temp_dir,
        &["watchlist", "add", "MSFT", "--label", "Microsoft"],
    );

    let before: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["watchlist", "list", "--json"])).unwrap();
    assert!(before
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["quote"].is_null()));

    let server = MockMarketServer::start(4);
    let valyu_base = server.valyu_base();
    let envs = [
        ("HELIUS_VALYU_API_KEY", "demo-valyu"),
        ("HELIUS_TAVILY_API_KEY", "demo-tavily"),
        ("HELIUS_VALYU_BASE_URL", valyu_base.as_str()),
        ("HELIUS_TAVILY_BASE_URL", server.tavily_base()),
    ];
    run_ok_env(&temp_dir, &["watchlist", "refresh", "--all"], &envs);
    server.finish();

    let refreshed: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["watchlist", "list", "--json"])).unwrap();
    let rows = refreshed.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row["quote"].is_object()));

    run_ok(&temp_dir, &["watchlist", "remove", "MSFT"]);
    let remaining: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["watchlist", "list", "--json"])).unwrap();
    assert_eq!(remaining.as_array().unwrap().len(), 1);
    assert_eq!(remaining.as_array().unwrap()[0]["ticker"], "AAPL");
}

#[test]
fn market_quote_and_news_support_live_fetch_and_cached_only_mode() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let server = MockMarketServer::start(2);
    let valyu_base = server.valyu_base();
    let envs = [
        ("HELIUS_VALYU_API_KEY", "demo-valyu"),
        ("HELIUS_TAVILY_API_KEY", "demo-tavily"),
        ("HELIUS_VALYU_BASE_URL", valyu_base.as_str()),
        ("HELIUS_TAVILY_BASE_URL", server.tavily_base()),
    ];

    let quote_live: Value = serde_json::from_str(&run_ok_env(
        &temp_dir,
        &["market", "quote", "AAPL", "--json"],
        &envs,
    ))
    .unwrap();
    assert_eq!(quote_live["ticker"], "AAPL");
    assert_eq!(quote_live["exchange"], "NASDAQ");
    assert_eq!(quote_live["currency"], "USD");
    assert_eq!(quote_live["stale"], false);

    let news_live: Value = serde_json::from_str(&run_ok_env(
        &temp_dir,
        &["market", "news", "AAPL", "--limit", "2", "--json"],
        &envs,
    ))
    .unwrap();
    assert_eq!(news_live["ticker"], "AAPL");
    assert_eq!(news_live["items"].as_array().unwrap().len(), 2);

    let brief_live: Value = serde_json::from_str(&run_ok_env(
        &temp_dir,
        &["market", "brief", "AAPL", "--limit", "2", "--json"],
        &envs,
    ))
    .unwrap();
    assert_eq!(brief_live["ticker"], "AAPL");
    assert_eq!(brief_live["headline_count"], 2);
    assert!(brief_live["summary"].as_str().unwrap().len() > 10);
    server.finish();

    let quote_cached: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["market", "quote", "AAPL", "--cached-only", "--json"],
    ))
    .unwrap();
    assert_eq!(quote_cached["ticker"], "AAPL");
    assert_eq!(quote_cached["stale"], false);

    let news_cached: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["market", "news", "AAPL", "--cached-only", "--json"],
    ))
    .unwrap();
    assert_eq!(news_cached["ticker"], "AAPL");
    assert_eq!(news_cached["items"].as_array().unwrap().len(), 2);

    let brief_cached: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["market", "brief", "AAPL", "--cached-only", "--json"],
    ))
    .unwrap();
    assert_eq!(brief_cached["ticker"], "AAPL");
    assert_eq!(brief_cached["headline_count"], 2);
}

#[test]
fn market_settings_show_and_set_round_trip() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let defaults: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["market", "settings", "show", "--json"],
    ))
    .unwrap();
    assert_eq!(defaults["quote_refresh_hours"], 4);
    assert_eq!(defaults["news_refresh_hours"], 12);
    assert_eq!(defaults["auto_refresh_quotes"], true);
    assert_eq!(defaults["auto_refresh_news"], true);
    assert_eq!(defaults["weekday_only"], true);

    let updated: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &[
            "market",
            "settings",
            "set",
            "--quote-hours",
            "6",
            "--news-hours",
            "24",
            "--auto-quotes",
            "false",
            "--auto-news",
            "true",
            "--weekday-only",
            "false",
            "--max-quote-cost",
            "0.05",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(updated["quote_refresh_hours"], 6);
    assert_eq!(updated["news_refresh_hours"], 24);
    assert_eq!(updated["auto_refresh_quotes"], false);
    assert_eq!(updated["auto_refresh_news"], true);
    assert_eq!(updated["weekday_only"], false);
    assert_eq!(updated["max_quote_cost_usd"], 0.05);
}

#[test]
fn market_auto_refresh_respects_due_windows_and_quote_cost_cap() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(&temp_dir, &["watchlist", "add", "AAPL", "--label", "Apple"]);
    run_ok(
        &temp_dir,
        &[
            "market",
            "settings",
            "set",
            "--quote-hours",
            "4",
            "--news-hours",
            "12",
            "--auto-quotes",
            "true",
            "--auto-news",
            "true",
            "--weekday-only",
            "false",
            "--max-quote-cost",
            "0.05",
        ],
    );

    let server = MockMarketServer::start(1);
    let valyu_base = server.valyu_base();
    let envs = [
        ("HELIUS_VALYU_API_KEY", "demo-valyu"),
        ("HELIUS_TAVILY_API_KEY", "demo-tavily"),
        ("HELIUS_VALYU_BASE_URL", valyu_base.as_str()),
        ("HELIUS_TAVILY_BASE_URL", server.tavily_base()),
    ];

    let first: Value = serde_json::from_str(&run_ok_env(
        &temp_dir,
        &["market", "auto-refresh", "--json"],
        &envs,
    ))
    .unwrap();
    server.finish();

    assert_eq!(first["tickers_considered"], 1);
    assert_eq!(first["quote_refreshed"], 0);
    assert_eq!(first["quote_skipped_cost_cap"], 1);
    assert_eq!(first["news_refreshed"], 1);
    assert_eq!(first["news_skipped_not_due"], 0);
    assert!((first["estimated_cost_usd"].as_f64().unwrap() - 0.016).abs() < 0.0001);

    let second: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["market", "auto-refresh", "--json"])).unwrap();
    assert_eq!(second["tickers_considered"], 1);
    assert_eq!(second["quote_skipped_cost_cap"], 1);
    assert_eq!(second["news_skipped_not_due"], 1);
    assert_eq!(second["news_refreshed"], 0);
}

#[test]
fn market_commands_fail_cleanly_when_api_keys_are_missing() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let quote_output = Command::cargo_bin("helius")
        .unwrap()
        .arg("--db")
        .arg(db_path(&temp_dir))
        .args(["market", "quote", "AAPL", "--json"])
        .env_remove("HELIUS_VALYU_API_KEY")
        .env_remove("HELIUS_TAVILY_API_KEY")
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();
    let quote_error = String::from_utf8(quote_output).unwrap();
    assert!(quote_error.contains("missing HELIUS_VALYU_API_KEY"));

    let news_output = Command::cargo_bin("helius")
        .unwrap()
        .arg("--db")
        .arg(db_path(&temp_dir))
        .args(["market", "news", "AAPL", "--json"])
        .env_remove("HELIUS_VALYU_API_KEY")
        .env_remove("HELIUS_TAVILY_API_KEY")
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();
    let news_error = String::from_utf8(news_output).unwrap();
    assert!(news_error.contains("missing HELIUS_TAVILY_API_KEY"));
}

struct MockPolyServer {
    base_url: String,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockPolyServer {
    fn start(expected_requests: usize) -> Self {
        let server = Server::http("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", server.server_addr());
        let handle = thread::spawn(move || {
            for _ in 0..expected_requests {
                let request = server.recv().unwrap();
                let url = request.url().to_string();
                let payload = if url.starts_with("/public-search") {
                    mock_poly_search_response()
                } else if url.contains("order=volume24hr") {
                    mock_poly_movers_response()
                } else if let Some((_, slug)) = url.split_once("slug=") {
                    mock_poly_market_response(slug.split('&').next().unwrap_or(slug))
                } else {
                    "[]".to_string()
                };
                let response = Response::from_string(payload)
                    .with_status_code(StatusCode(200))
                    .with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    );
                request.respond(response).unwrap();
            }
        });

        Self {
            base_url,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn finish(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

fn mock_poly_market_response(slug: &str) -> String {
    format!(
        "[{{\"slug\":\"{slug}\",\"question\":\"Will {} resolve YES?\",\"description\":\"Macro event market\",\"active\":true,\"closed\":false,\"liquidityNum\":125000,\"volume24hr\":54000,\"volumeNum\":880000,\"endDate\":\"2026-06-30\",\"outcomes\":\"[\\\"Yes\\\",\\\"No\\\"]\",\"outcomePrices\":\"[\\\"0.61\\\",\\\"0.39\\\"]\"}}]",
        slug.replace('-', " ")
    )
}

fn mock_poly_movers_response() -> String {
    "[{\"slug\":\"fed-cut-june\",\"question\":\"Will the Fed cut in June?\",\"active\":true,\"closed\":false,\"liquidityNum\":99000,\"volume24hr\":45000,\"volumeNum\":780000,\"outcomes\":[\"Yes\",\"No\"],\"outcomePrices\":[0.58,0.42]},{\"slug\":\"ecb-cut-july\",\"question\":\"Will the ECB cut in July?\",\"active\":true,\"closed\":false,\"liquidityNum\":64000,\"volume24hr\":21000,\"volumeNum\":360000,\"outcomes\":[\"Yes\",\"No\"],\"outcomePrices\":[0.47,0.53]}]".to_string()
}

fn mock_poly_search_response() -> String {
    "{\"events\":[{\"title\":\"Rates\",\"markets\":[{\"slug\":\"fed-cut-june\",\"question\":\"Will the Fed cut in June?\",\"active\":true,\"closed\":false,\"outcomes\":[\"Yes\",\"No\"],\"outcomePrices\":[0.58,0.42]}]}]}".to_string()
}

#[test]
fn poly_schema_migration_from_v6_to_v7_recreates_poly_tables() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    conn.execute_batch(
        "DROP TABLE poly_watchlist_items;
         DROP TABLE poly_cache;
         UPDATE metadata SET schema_version = 6 WHERE id = 1;",
    )
    .unwrap();
    drop(conn);

    let watchlist: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["poly", "watch", "list", "--json"])).unwrap();
    assert_eq!(watchlist.as_array().unwrap().len(), 0);

    let conn = Connection::open(db_path(&temp_dir)).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT schema_version FROM metadata WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let poly_watchlist_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'poly_watchlist_items'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let poly_cache_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'poly_cache'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 8);
    assert_eq!(poly_watchlist_exists, 1);
    assert_eq!(poly_cache_exists, 1);
}

#[test]
fn poly_commands_support_live_fetch_and_cached_only_mode() {
    let temp_dir = TempDir::new().unwrap();
    run_ok(&temp_dir, &["init", "--currency", "USD"]);
    run_ok(
        &temp_dir,
        &[
            "poly",
            "watch",
            "add",
            "fed-cut-june",
            "--label",
            "Fed June",
        ],
    );

    let server = MockPolyServer::start(3);
    let envs = [("HELIUS_POLYMARKET_BASE_URL", server.base_url())];

    let refresh: Value = serde_json::from_str(&run_ok_env(
        &temp_dir,
        &["poly", "watch", "refresh", "--all", "--json"],
        &envs,
    ))
    .unwrap();
    assert_eq!(refresh["markets_refreshed"], 1);
    assert_eq!(refresh["movers_refreshed"], true);

    let search: Value = serde_json::from_str(&run_ok_env(
        &temp_dir,
        &["poly", "search", "fed", "--limit", "5", "--json"],
        &envs,
    ))
    .unwrap();
    assert_eq!(search.as_array().unwrap().len(), 1);
    assert_eq!(search[0]["slug"], "fed-cut-june");

    let market: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["poly", "market", "fed-cut-june", "--cached-only", "--json"],
    ))
    .unwrap();
    assert_eq!(market["slug"], "fed-cut-june");
    assert_eq!(market["yes_price"], 0.61);

    let brief: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["poly", "brief", "fed-cut-june", "--cached-only", "--json"],
    ))
    .unwrap();
    assert_eq!(brief["slug"], "fed-cut-june");
    assert_eq!(brief["tone"], "close");

    let movers: Value = serde_json::from_str(&run_ok(
        &temp_dir,
        &["poly", "movers", "--cached-only", "--json"],
    ))
    .unwrap();
    assert_eq!(movers.as_array().unwrap().len(), 2);

    let watchlist: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["poly", "watch", "list", "--json"])).unwrap();
    assert_eq!(watchlist.as_array().unwrap().len(), 1);
    assert_eq!(watchlist[0]["slug"], "fed-cut-june");

    run_ok(&temp_dir, &["poly", "watch", "remove", "fed-cut-june"]);
    let watchlist: Value =
        serde_json::from_str(&run_ok(&temp_dir, &["poly", "watch", "list", "--json"])).unwrap();
    assert_eq!(watchlist.as_array().unwrap().len(), 0);

    server.finish();
}
