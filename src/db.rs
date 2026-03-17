use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{Datelike, Duration, Local, NaiveDate, Weekday as ChronoWeekday};
use directories::ProjectDirs;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};

use crate::error::AppError;
use crate::model::{
    Account, AccountKind, BalanceRecord, BalanceTrendPoint, BillCalendarItem, BudgetRecord,
    BudgetStatusRecord, Category, CategoryKind, CategorySpendingPoint, CsvImportPlan,
    CsvImportResult, ForecastDailyPoint, ForecastMonthlyPoint, ForecastSelection, ForecastSnapshot,
    GoalStatusRecord, ImportedTransactionRow, MonthlyCashFlowPoint, NewPlanningGoal,
    NewPlanningItem, NewPlanningScenario, NewRecurringRule, NewTransaction, OccurrenceStatus,
    PlanningGoalKind, PlanningGoalRecord, PlanningItemRecord, PlanningScenarioRecord,
    ReconciliationRecord, RecurringCadence, RecurringOccurrenceRecord, RecurringRuleRecord,
    SummaryRecord, TransactionFilters, TransactionKind, TransactionRecord, UpdatePlanningGoal,
    UpdatePlanningItem, UpdatePlanningScenario, UpdateRecurringRule, UpdateTransaction, Weekday,
    WeeklyBalancePoint,
};

pub const CURRENT_SCHEMA_VERSION: i64 = 8;

pub struct Db {
    conn: Connection,
    path: PathBuf,
}

#[derive(Clone, Debug)]
struct StoredTransaction {
    id: i64,
    txn_date: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    category_id: Option<i64>,
    payee: Option<String>,
    note: Option<String>,
    created_at: String,
    updated_at: String,
    deleted_at: Option<String>,
    reconciliation_id: Option<i64>,
    recurring_rule_id: Option<i64>,
}

#[derive(Clone, Debug)]
struct ResolvedTransaction {
    txn_date: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    category_id: Option<i64>,
    payee: Option<String>,
    note: Option<String>,
    recurring_rule_id: Option<i64>,
}

#[derive(Clone, Debug)]
struct StoredRecurringRule {
    id: i64,
    name: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    category_id: Option<i64>,
    payee: Option<String>,
    note: Option<String>,
    cadence: RecurringCadence,
    interval: i64,
    day_of_month: Option<u32>,
    weekday: Option<Weekday>,
    start_on: String,
    end_on: Option<String>,
    next_due_on: String,
    paused: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Clone, Debug)]
struct ResolvedRecurringRule {
    name: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    category_id: Option<i64>,
    payee: Option<String>,
    note: Option<String>,
    cadence: RecurringCadence,
    interval: i64,
    day_of_month: Option<u32>,
    weekday: Option<Weekday>,
    start_on: String,
    end_on: Option<String>,
    next_due_on: String,
}

#[derive(Clone, Debug)]
struct StoredPlanningItem {
    scenario_id: Option<i64>,
    title: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    category_id: Option<i64>,
    due_on: String,
    payee: Option<String>,
    note: Option<String>,
    linked_transaction_id: Option<i64>,
    archived: bool,
}

#[derive(Clone, Debug)]
struct ResolvedPlanningItem {
    scenario_id: Option<i64>,
    title: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    category_id: Option<i64>,
    due_on: String,
    payee: Option<String>,
    note: Option<String>,
}

#[derive(Clone, Debug)]
struct StoredPlanningGoal {
    name: String,
    kind: PlanningGoalKind,
    account_id: i64,
    target_amount_cents: Option<i64>,
    minimum_balance_cents: Option<i64>,
    due_on: Option<String>,
    archived: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Clone, Debug)]
struct ForecastEvent {
    inflow_cents: i64,
    outflow_cents: i64,
    per_account_delta: Vec<(i64, i64)>,
}

#[derive(Clone, Debug)]
struct BudgetForecastRow {
    month: String,
    category_id: i64,
    category_name: String,
    account_id: Option<i64>,
    amount_cents: i64,
}

#[derive(Clone, Debug)]
struct RecurringForecastOccurrence {
    rule_id: i64,
    due_on: String,
    rule_name: String,
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    account_name: String,
    to_account_id: Option<i64>,
    category_name: Option<String>,
}

impl Db {
    pub fn open_existing(path: &Path) -> Result<Self, AppError> {
        if !path.exists() {
            return Err(AppError::missing_db(path));
        }

        let mut db = Self::open(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        if !db.is_initialized()? {
            return Err(AppError::missing_db(path));
        }

        // Older local databases can be missing newer tables even when metadata exists.
        // Recreate any absent schema objects before applying versioned migrations.
        db.conn.execute_batch(FULL_SCHEMA_SQL)?;
        db.migrate_if_needed()?;
        db.repair_schema_if_needed()?;
        db.ensure_indexes()?;
        Ok(db)
    }

    pub fn open_for_init(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        Self::open(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
    }

    fn open(path: &Path, flags: OpenFlags) -> Result<Self, AppError> {
        let conn = Connection::open_with_flags(path, flags)?;
        conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
        Ok(Self {
            conn,
            path: path.to_path_buf(),
        })
    }

    pub fn init(&self, currency: &str) -> Result<(), AppError> {
        let normalized_currency = normalize_currency_code(currency)?;
        if self.is_initialized()? {
            return Err(AppError::AlreadyExists(
                "database is already initialized".to_string(),
            ));
        }

        self.conn.execute_batch(FULL_SCHEMA_SQL)?;
        self.ensure_indexes()?;
        self.conn.execute(
            "INSERT INTO metadata (id, currency, schema_version, created_at) VALUES (1, ?1, ?2, ?3)",
            params![normalized_currency, CURRENT_SCHEMA_VERSION, now_timestamp()],
        )?;
        Ok(())
    }

    pub fn currency_code(&self) -> Result<String, AppError> {
        self.conn
            .query_row("SELECT currency FROM metadata WHERE id = 1", [], |row| {
                row.get(0)
            })
            .map_err(Into::into)
    }

    pub fn add_account(
        &self,
        name: &str,
        kind: &AccountKind,
        opening_balance_cents: i64,
        opened_on: &str,
    ) -> Result<i64, AppError> {
        let normalized_name = normalize_name("account", name)?;
        match self.conn.execute(
            "INSERT INTO accounts (name, kind, opening_balance_cents, opened_on) VALUES (?1, ?2, ?3, ?4)",
            params![normalized_name, kind.as_db_str(), opening_balance_cents, opened_on],
        ) {
            Ok(_) => Ok(self.conn.last_insert_rowid()),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("account", &normalized_name))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn list_accounts(&self) -> Result<Vec<Account>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT id, name, kind, opening_balance_cents, opened_on, archived
             FROM accounts
             WHERE archived = 0
             ORDER BY name COLLATE NOCASE",
        )?;

        let rows = statement.query_map([], |row| {
            let kind: String = row.get(2)?;
            Ok(Account {
                id: row.get(0)?,
                name: row.get(1)?,
                kind: AccountKind::from_db(&kind).map_err(map_db_error)?,
                opening_balance_cents: row.get(3)?,
                opened_on: row.get(4)?,
                archived: row.get::<_, i64>(5)? == 1,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn edit_account(
        &self,
        reference: &str,
        name: Option<&str>,
        kind: Option<&AccountKind>,
        opening_balance_cents: Option<i64>,
        opened_on: Option<&str>,
    ) -> Result<i64, AppError> {
        let account_id = self.resolve_account_ref(reference)?;
        let current = self.load_account(account_id)?;
        let next_name = match name {
            Some(value) => normalize_name("account", value)?,
            None => current.name.clone(),
        };
        let next_kind = kind.copied().unwrap_or(current.kind);
        let next_opening_balance_cents =
            opening_balance_cents.unwrap_or(current.opening_balance_cents);
        let next_opened_on = opened_on.unwrap_or(&current.opened_on).trim().to_string();
        if next_opened_on.is_empty() {
            return Err(AppError::Validation(
                "account opened_on date cannot be empty".to_string(),
            ));
        }

        match self.conn.execute(
            "UPDATE accounts
             SET name = ?1,
                 kind = ?2,
                 opening_balance_cents = ?3,
                 opened_on = ?4
             WHERE id = ?5",
            params![
                next_name,
                next_kind.as_db_str(),
                next_opening_balance_cents,
                next_opened_on,
                account_id
            ],
        ) {
            Ok(0) => Err(AppError::invalid_ref("account", reference.trim())),
            Ok(_) => Ok(account_id),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("account", &next_name))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn delete_account(&self, reference: &str) -> Result<i64, AppError> {
        let account_id = self.resolve_account_ref(reference)?;
        if let Some(blocker) = self.account_archive_blocker(account_id)? {
            return Err(AppError::Validation(format!(
                "cannot archive account while {} still reference it",
                blocker
            )));
        }
        let changed = self.conn.execute(
            "UPDATE accounts
             SET archived = 1
             WHERE id = ?1
               AND archived = 0",
            params![account_id],
        )?;
        if changed == 0 {
            return Err(AppError::invalid_ref("account", reference.trim()));
        }
        Ok(account_id)
    }

    pub fn add_category(&self, name: &str, kind: &CategoryKind) -> Result<i64, AppError> {
        let normalized_name = normalize_name("category", name)?;
        match self.conn.execute(
            "INSERT INTO categories (name, kind) VALUES (?1, ?2)",
            params![normalized_name, kind.as_db_str()],
        ) {
            Ok(_) => Ok(self.conn.last_insert_rowid()),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("category", &normalized_name))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn list_categories(&self) -> Result<Vec<Category>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT id, name, kind, archived
             FROM categories
             WHERE archived = 0
             ORDER BY kind ASC, name COLLATE NOCASE",
        )?;

        let rows = statement.query_map([], |row| {
            let kind: String = row.get(2)?;
            Ok(Category {
                id: row.get(0)?,
                name: row.get(1)?,
                kind: CategoryKind::from_db(&kind).map_err(map_db_error)?,
                archived: row.get::<_, i64>(3)? == 1,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn edit_category(
        &self,
        reference: &str,
        name: Option<&str>,
        kind: Option<&CategoryKind>,
    ) -> Result<i64, AppError> {
        let category_id = self.resolve_category_ref(reference, None)?;
        let (current_name, current_kind) = self.load_category(category_id)?;
        let next_name = match name {
            Some(value) => normalize_name("category", value)?,
            None => current_name.clone(),
        };
        let next_kind = kind.copied().unwrap_or(current_kind);

        if next_kind != current_kind && self.category_has_dependencies(category_id)? {
            return Err(AppError::Validation(
                "cannot change category kind while transactions, budgets, or recurring rules still reference it"
                    .to_string(),
            ));
        }

        match self.conn.execute(
            "UPDATE categories
             SET name = ?1,
                 kind = ?2
             WHERE id = ?3",
            params![next_name, next_kind.as_db_str(), category_id],
        ) {
            Ok(0) => Err(AppError::invalid_ref("category", reference.trim())),
            Ok(_) => Ok(category_id),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("category", &next_name))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn delete_category(&self, reference: &str) -> Result<i64, AppError> {
        let category_id = self.resolve_category_ref(reference, None)?;
        let changed = self.conn.execute(
            "UPDATE categories
             SET archived = 1
             WHERE id = ?1
               AND archived = 0",
            params![category_id],
        )?;
        if changed == 0 {
            return Err(AppError::invalid_ref("category", reference.trim()));
        }
        Ok(category_id)
    }
    pub fn add_transaction(&self, transaction: &NewTransaction) -> Result<i64, AppError> {
        let resolved = self.resolve_transaction_input(transaction)?;
        self.insert_transaction(&resolved)
    }

    pub fn edit_transaction(&self, patch: &UpdateTransaction) -> Result<(), AppError> {
        let existing = self.load_transaction(patch.id)?;
        let _ = (&existing.id, &existing.created_at, &existing.updated_at);
        if existing.deleted_at.is_some() {
            return Err(AppError::Validation(
                "deleted transactions must be restored before they can be edited".to_string(),
            ));
        }
        if existing.reconciliation_id.is_some() {
            return Err(AppError::Validation(
                "reconciled transactions cannot be edited until the reconciliation is removed"
                    .to_string(),
            ));
        }

        let kind = patch.kind.unwrap_or(existing.kind);
        let account_id = match patch.account.as_deref() {
            Some(reference) => self.resolve_account_ref(reference)?,
            None => existing.account_id,
        };
        let to_account_id = if patch.clear_to_account {
            None
        } else if let Some(reference) = patch.to_account.as_deref() {
            Some(self.resolve_account_ref(reference)?)
        } else {
            existing.to_account_id
        };
        let category_id = if patch.clear_category {
            None
        } else if let Some(reference) = patch.category.as_deref() {
            let expected = expected_category_kind(kind);
            match expected.as_ref() {
                Some(expected_kind) => {
                    Some(self.resolve_category_ref(reference, Some(expected_kind))?)
                }
                None => Some(self.resolve_category_ref(reference, None)?),
            }
        } else {
            existing.category_id
        };

        let resolved = ResolvedTransaction {
            txn_date: patch.txn_date.clone().unwrap_or(existing.txn_date),
            kind,
            amount_cents: patch.amount_cents.unwrap_or(existing.amount_cents),
            account_id,
            to_account_id,
            category_id,
            payee: resolve_optional_patch(existing.payee, &patch.payee, patch.clear_payee),
            note: resolve_optional_patch(existing.note, &patch.note, patch.clear_note),
            recurring_rule_id: existing.recurring_rule_id,
        };

        self.validate_resolved_transaction(&resolved)?;
        self.conn.execute(
            "UPDATE transactions
             SET txn_date = ?1,
                 kind = ?2,
                 amount_cents = ?3,
                 account_id = ?4,
                 to_account_id = ?5,
                 category_id = ?6,
                 payee = ?7,
                 note = ?8,
                 updated_at = ?9
             WHERE id = ?10",
            params![
                resolved.txn_date,
                resolved.kind.as_db_str(),
                resolved.amount_cents,
                resolved.account_id,
                resolved.to_account_id,
                resolved.category_id,
                normalize_optional_text(&resolved.payee),
                normalize_optional_text(&resolved.note),
                now_timestamp(),
                patch.id
            ],
        )?;
        Ok(())
    }

    pub fn delete_transaction(&self, id: i64) -> Result<(), AppError> {
        let existing = self.load_transaction(id)?;
        if existing.deleted_at.is_some() {
            return Err(AppError::Validation(
                "transaction is already deleted".to_string(),
            ));
        }
        if existing.reconciliation_id.is_some() {
            return Err(AppError::Validation(
                "reconciled transactions cannot be deleted until the reconciliation is removed"
                    .to_string(),
            ));
        }

        self.conn.execute(
            "UPDATE transactions SET deleted_at = ?1, updated_at = ?1 WHERE id = ?2",
            params![now_timestamp(), id],
        )?;
        Ok(())
    }

    pub fn restore_transaction(&self, id: i64) -> Result<(), AppError> {
        let existing = self.load_transaction(id)?;
        if existing.deleted_at.is_none() {
            return Err(AppError::Validation(
                "transaction is not deleted".to_string(),
            ));
        }

        let resolved = ResolvedTransaction {
            txn_date: existing.txn_date,
            kind: existing.kind,
            amount_cents: existing.amount_cents,
            account_id: existing.account_id,
            to_account_id: existing.to_account_id,
            category_id: existing.category_id,
            payee: existing.payee,
            note: existing.note,
            recurring_rule_id: existing.recurring_rule_id,
        };
        self.validate_resolved_transaction(&resolved)?;

        self.conn.execute(
            "UPDATE transactions SET deleted_at = NULL, updated_at = ?1 WHERE id = ?2",
            params![now_timestamp(), id],
        )?;
        Ok(())
    }

    pub fn list_transactions(
        &self,
        filters: &TransactionFilters,
    ) -> Result<Vec<TransactionRecord>, AppError> {
        let account_id = match filters.account.as_deref() {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };
        let category_id = match filters.category.as_deref() {
            Some(reference) => Some(self.resolve_category_ref(reference, None)?),
            None => None,
        };
        let search_pattern = filters
            .search
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("%{}%", value.to_lowercase()));
        let limit = filters.limit.unwrap_or(1_000_000) as i64;
        let include_deleted = if filters.include_deleted {
            1_i64
        } else {
            0_i64
        };

        let mut statement = self.conn.prepare(
            "SELECT
                t.id,
                t.txn_date,
                t.kind,
                t.amount_cents,
                source.id,
                source.name,
                target.id,
                target.name,
                category.id,
                category.name,
                t.payee,
                t.note,
                t.created_at,
                t.updated_at,
                t.deleted_at,
                t.reconciliation_id,
                t.recurring_rule_id
             FROM transactions t
             JOIN accounts source ON source.id = t.account_id
             LEFT JOIN accounts target ON target.id = t.to_account_id
             LEFT JOIN categories category ON category.id = t.category_id
             WHERE (?1 IS NULL OR t.txn_date >= ?1)
               AND (?2 IS NULL OR t.txn_date <= ?2)
               AND (?3 IS NULL OR t.account_id = ?3 OR t.to_account_id = ?3)
               AND (?4 IS NULL OR t.category_id = ?4)
               AND (
                    ?5 IS NULL
                    OR LOWER(COALESCE(t.payee, '')) LIKE ?5
                    OR LOWER(COALESCE(t.note, '')) LIKE ?5
                    OR LOWER(source.name) LIKE ?5
                    OR LOWER(COALESCE(target.name, '')) LIKE ?5
                    OR LOWER(COALESCE(category.name, '')) LIKE ?5
               )
               AND (?6 = 1 OR t.deleted_at IS NULL)
             ORDER BY t.txn_date DESC, t.id DESC
             LIMIT ?7",
        )?;

        let rows = statement.query_map(
            params![
                filters.from.as_deref(),
                filters.to.as_deref(),
                account_id,
                category_id,
                search_pattern.as_deref(),
                include_deleted,
                limit
            ],
            |row| map_transaction_row(row),
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn recent_transactions(&self, limit: usize) -> Result<Vec<TransactionRecord>, AppError> {
        self.list_transactions(&TransactionFilters {
            from: None,
            to: None,
            account: None,
            category: None,
            search: None,
            limit: Some(limit),
            include_deleted: false,
        })
    }

    pub fn list_eligible_reconciliation_transactions(
        &self,
        account_ref: &str,
        statement_ending_on: &str,
    ) -> Result<Vec<TransactionRecord>, AppError> {
        let account_id = self.resolve_account_ref(account_ref)?;
        self.list_eligible_reconciliation_transactions_by_id(account_id, statement_ending_on)
    }

    pub fn balances(&self, account_ref: Option<&str>) -> Result<Vec<BalanceRecord>, AppError> {
        let account_id = match account_ref {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };

        let mut statement = self.conn.prepare(
            "SELECT
                a.id,
                a.name,
                a.kind,
                a.opening_balance_cents + COALESCE(SUM(
                    CASE
                        WHEN t.kind = 'income' AND t.account_id = a.id THEN t.amount_cents
                        WHEN t.kind = 'expense' AND t.account_id = a.id THEN -t.amount_cents
                        WHEN t.kind = 'transfer' AND t.account_id = a.id THEN -t.amount_cents
                        WHEN t.kind = 'transfer' AND t.to_account_id = a.id THEN t.amount_cents
                        ELSE 0
                    END
                ), 0) AS current_balance_cents
             FROM accounts a
             LEFT JOIN transactions t
               ON (t.account_id = a.id OR t.to_account_id = a.id)
              AND t.deleted_at IS NULL
             WHERE a.archived = 0
               AND (?1 IS NULL OR a.id = ?1)
             GROUP BY a.id, a.name, a.kind, a.opening_balance_cents
             ORDER BY a.name COLLATE NOCASE",
        )?;

        let rows = statement.query_map(params![account_id], |row| {
            let kind: String = row.get(2)?;
            Ok(BalanceRecord {
                account_id: row.get(0)?,
                account_name: row.get(1)?,
                account_kind: AccountKind::from_db(&kind).map_err(map_db_error)?,
                current_balance_cents: row.get(3)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn summary(
        &self,
        from: &str,
        to: &str,
        account_ref: Option<&str>,
    ) -> Result<SummaryRecord, AppError> {
        match account_ref {
            Some(reference) => self.summary_for_account(from, to, reference),
            None => self.summary_all_accounts(from, to),
        }
    }

    pub fn monthly_cash_flow_trend(
        &self,
        months: usize,
    ) -> Result<Vec<MonthlyCashFlowPoint>, AppError> {
        let months = months.max(1);
        let current = Local::now().date_naive();
        let current_start = NaiveDate::from_ymd_opt(current.year(), current.month(), 1)
            .expect("current month should always be valid");
        let first_start = add_months_with_day(current_start, -((months - 1) as i32), 1)?;
        let mut points = Vec::with_capacity(months);

        for step in 0..months {
            let month_start = add_months_with_day(first_start, step as i32, 1)?;
            let next_start = add_months_with_day(month_start, 1, 1)?;
            let month_end = next_start
                .pred_opt()
                .expect("previous day should exist for month boundary");
            let (income_cents, expense_cents): (i64, i64) = self.conn.query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN kind = 'income' THEN amount_cents ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'expense' THEN amount_cents ELSE 0 END), 0)
                 FROM transactions
                 WHERE deleted_at IS NULL
                   AND txn_date >= ?1
                   AND txn_date <= ?2",
                params![
                    month_start.format("%Y-%m-%d").to_string(),
                    month_end.format("%Y-%m-%d").to_string()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            points.push(MonthlyCashFlowPoint {
                month: month_start.format("%Y-%m").to_string(),
                income_cents,
                expense_cents,
                net_cents: income_cents - expense_cents,
            });
        }

        Ok(points)
    }

    pub fn category_spending(
        &self,
        from: &str,
        to: &str,
        limit: usize,
    ) -> Result<Vec<CategorySpendingPoint>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                c.id,
                c.name,
                SUM(t.amount_cents) AS spent_cents
             FROM transactions t
             JOIN categories c ON c.id = t.category_id
             WHERE t.deleted_at IS NULL
               AND t.kind = 'expense'
               AND t.txn_date >= ?1
               AND t.txn_date <= ?2
             GROUP BY c.id, c.name
             ORDER BY spent_cents DESC, c.name COLLATE NOCASE
             LIMIT ?3",
        )?;

        let rows = statement.query_map(params![from, to, limit as i64], |row| {
            Ok(CategorySpendingPoint {
                category_id: row.get(0)?,
                category_name: row.get(1)?,
                spent_cents: row.get(2)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn total_balance_trend(&self, months: usize) -> Result<Vec<BalanceTrendPoint>, AppError> {
        let months = months.max(1);
        let current = Local::now().date_naive();
        let current_start = NaiveDate::from_ymd_opt(current.year(), current.month(), 1)
            .expect("current month should always be valid");
        let first_start = add_months_with_day(current_start, -((months - 1) as i32), 1)?;
        let opening_total: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(opening_balance_cents), 0) FROM accounts WHERE archived = 0",
            [],
            |row| row.get(0),
        )?;
        let mut points = Vec::with_capacity(months);

        for step in 0..months {
            let month_start = add_months_with_day(first_start, step as i32, 1)?;
            let next_start = add_months_with_day(month_start, 1, 1)?;
            let month_end = next_start
                .pred_opt()
                .expect("previous day should exist for month boundary");
            let (income_cents, expense_cents): (i64, i64) = self.conn.query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN kind = 'income' THEN amount_cents ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'expense' THEN amount_cents ELSE 0 END), 0)
                 FROM transactions
                 WHERE deleted_at IS NULL
                   AND txn_date <= ?1",
                params![month_end.format("%Y-%m-%d").to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            points.push(BalanceTrendPoint {
                month: month_start.format("%Y-%m").to_string(),
                balance_cents: opening_total + income_cents - expense_cents,
            });
        }

        Ok(points)
    }

    pub fn weekly_opening_balance_history(
        &self,
        weeks: usize,
    ) -> Result<Vec<WeeklyBalancePoint>, AppError> {
        let weeks = weeks.max(1);
        let today = Local::now().date_naive();
        let current_week_start =
            today - Duration::days(today.weekday().num_days_from_monday() as i64);
        let first_week_start = current_week_start - Duration::weeks((weeks - 1) as i64);
        let opening_total: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(opening_balance_cents), 0) FROM accounts WHERE archived = 0",
            [],
            |row| row.get(0),
        )?;
        let mut points = Vec::with_capacity(weeks);

        for step in 0..weeks {
            let week_start = first_week_start + Duration::weeks(step as i64);
            let week_start_iso = week_start.format("%Y-%m-%d").to_string();
            let (income_cents, expense_cents): (i64, i64) = self.conn.query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN kind = 'income' THEN amount_cents ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN kind = 'expense' THEN amount_cents ELSE 0 END), 0)
                 FROM transactions
                 WHERE deleted_at IS NULL
                   AND txn_date < ?1",
                params![&week_start_iso],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            points.push(WeeklyBalancePoint {
                week_start: week_start_iso,
                opening_balance_cents: opening_total + income_cents - expense_cents,
            });
        }

        Ok(points)
    }

    pub fn start_reconciliation(
        &self,
        account_ref: &str,
        statement_ending_on: &str,
        statement_balance_cents: i64,
        transaction_ids: &[i64],
    ) -> Result<i64, AppError> {
        if transaction_ids.is_empty() {
            return Err(AppError::Validation(
                "reconciliation requires at least one transaction selection".to_string(),
            ));
        }

        let account_id = self.resolve_account_ref(account_ref)?;
        let eligible =
            self.list_eligible_reconciliation_transactions_by_id(account_id, statement_ending_on)?;
        if eligible.is_empty() {
            return Err(AppError::Validation(
                "no eligible transactions were found for reconciliation".to_string(),
            ));
        }

        let eligible_ids: HashSet<i64> =
            eligible.iter().map(|transaction| transaction.id).collect();
        for transaction_id in transaction_ids {
            if !eligible_ids.contains(transaction_id) {
                return Err(AppError::Validation(format!(
                    "transaction {transaction_id} is not eligible for this reconciliation"
                )));
            }
        }

        let opening_balance_cents = self.lookup_account_opening_balance(account_id)?;
        let selected_lookup: HashSet<i64> = transaction_ids.iter().copied().collect();
        let mut cleared_delta = 0_i64;
        let mut selected_count = 0_i64;
        for transaction in &eligible {
            if selected_lookup.contains(&transaction.id) {
                selected_count += 1;
                cleared_delta += transaction_effect_for_account(
                    account_id,
                    transaction.kind,
                    transaction.amount_cents,
                    transaction.account_id,
                    transaction.to_account_id,
                );
            }
        }
        let cleared_balance_cents = opening_balance_cents + cleared_delta;
        if cleared_balance_cents != statement_balance_cents {
            return Err(AppError::Validation(format!(
                "selected transactions clear to {}, but the statement balance is {}",
                crate::amount::format_cents(cleared_balance_cents),
                crate::amount::format_cents(statement_balance_cents)
            )));
        }

        self.conn.execute(
            "INSERT INTO reconciliations (account_id, statement_ending_on, statement_balance_cents, cleared_balance_cents, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                account_id,
                statement_ending_on,
                statement_balance_cents,
                cleared_balance_cents,
                now_timestamp()
            ],
        )?;
        let reconciliation_id = self.conn.last_insert_rowid();
        for transaction_id in transaction_ids {
            self.conn.execute(
                "UPDATE transactions SET reconciliation_id = ?1, updated_at = ?2 WHERE id = ?3",
                params![reconciliation_id, now_timestamp(), transaction_id],
            )?;
        }

        if selected_count == 0 {
            return Err(AppError::Validation(
                "reconciliation requires at least one transaction selection".to_string(),
            ));
        }

        Ok(reconciliation_id)
    }

    pub fn list_reconciliations(
        &self,
        account_ref: Option<&str>,
    ) -> Result<Vec<ReconciliationRecord>, AppError> {
        let account_id = match account_ref {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };

        let mut statement = self.conn.prepare(
            "SELECT
                r.id,
                r.account_id,
                a.name,
                r.statement_ending_on,
                r.statement_balance_cents,
                r.cleared_balance_cents,
                COUNT(t.id) AS transaction_count,
                r.created_at
             FROM reconciliations r
             JOIN accounts a ON a.id = r.account_id
             LEFT JOIN transactions t ON t.reconciliation_id = r.id
             WHERE (?1 IS NULL OR r.account_id = ?1)
             GROUP BY r.id, r.account_id, a.name, r.statement_ending_on, r.statement_balance_cents, r.cleared_balance_cents, r.created_at
             ORDER BY r.statement_ending_on DESC, r.id DESC",
        )?;

        let rows = statement.query_map(params![account_id], |row| {
            Ok(ReconciliationRecord {
                id: row.get(0)?,
                account_id: row.get(1)?,
                account_name: row.get(2)?,
                statement_ending_on: row.get(3)?,
                statement_balance_cents: row.get(4)?,
                cleared_balance_cents: row.get(5)?,
                transaction_count: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn delete_reconciliation(&self, id: i64) -> Result<(), AppError> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM reconciliations WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(AppError::NotFound(format!(
                "reconciliation `{id}` was not found"
            )));
        }

        self.conn.execute(
            "UPDATE transactions SET reconciliation_id = NULL, updated_at = ?1 WHERE reconciliation_id = ?2",
            params![now_timestamp(), id],
        )?;
        self.conn
            .execute("DELETE FROM reconciliations WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn add_recurring_rule(&self, rule: &NewRecurringRule) -> Result<i64, AppError> {
        let resolved = self.resolve_recurring_rule_input(rule)?;
        self.insert_recurring_rule(&resolved)
    }

    pub fn edit_recurring_rule(&self, patch: &UpdateRecurringRule) -> Result<(), AppError> {
        let existing = self.load_recurring_rule(patch.id)?;
        let _ = (
            &existing.next_due_on,
            existing.paused,
            &existing.created_at,
            &existing.updated_at,
        );
        let kind = patch.kind.unwrap_or(existing.kind);
        let cadence = patch.cadence.unwrap_or(existing.cadence);
        let account_id = match patch.account.as_deref() {
            Some(reference) => self.resolve_account_ref(reference)?,
            None => existing.account_id,
        };
        let to_account_id = if patch.clear_to_account {
            None
        } else if let Some(reference) = patch.to_account.as_deref() {
            Some(self.resolve_account_ref(reference)?)
        } else {
            existing.to_account_id
        };
        let category_id = if patch.clear_category {
            None
        } else if let Some(reference) = patch.category.as_deref() {
            let expected = expected_category_kind(kind);
            match expected.as_ref() {
                Some(expected_kind) => {
                    Some(self.resolve_category_ref(reference, Some(expected_kind))?)
                }
                None => Some(self.resolve_category_ref(reference, None)?),
            }
        } else {
            existing.category_id
        };

        let start_on = patch
            .start_on
            .clone()
            .unwrap_or_else(|| existing.start_on.clone());
        let end_on = if patch.clear_end_on {
            None
        } else if let Some(value) = patch.end_on.clone() {
            Some(value)
        } else {
            existing.end_on.clone()
        };
        let day_of_month = if patch.clear_day_of_month {
            None
        } else {
            patch.day_of_month.or(existing.day_of_month)
        };
        let weekday = if patch.clear_weekday {
            None
        } else {
            patch.weekday.or(existing.weekday)
        };
        let interval = patch.interval.unwrap_or(existing.interval);
        let minimum_next_due_on = self.compute_rule_next_due_after_edit(
            patch.id,
            &start_on,
            cadence,
            interval,
            day_of_month,
            weekday,
        )?;
        let schedule_changed = cadence != existing.cadence
            || interval != existing.interval
            || day_of_month != existing.day_of_month
            || weekday != existing.weekday
            || start_on != existing.start_on;
        let next_due_on = if patch.clear_next_due_on {
            minimum_next_due_on.clone()
        } else if let Some(value) = patch.next_due_on.as_deref() {
            self.resolve_recurring_next_due_on(
                parse_date(&start_on)?,
                cadence,
                interval,
                day_of_month,
                weekday,
                Some(value),
                parse_date(&minimum_next_due_on)?,
            )?
        } else if schedule_changed {
            minimum_next_due_on.clone()
        } else {
            existing.next_due_on.clone()
        };

        let resolved = ResolvedRecurringRule {
            name: patch.name.clone().unwrap_or(existing.name),
            kind,
            amount_cents: patch.amount_cents.unwrap_or(existing.amount_cents),
            account_id,
            to_account_id,
            category_id,
            payee: resolve_optional_patch(existing.payee, &patch.payee, patch.clear_payee),
            note: resolve_optional_patch(existing.note, &patch.note, patch.clear_note),
            cadence,
            interval,
            day_of_month,
            weekday,
            start_on,
            end_on,
            next_due_on,
        };
        self.validate_resolved_recurring_rule(&resolved)?;

        self.conn.execute(
            "DELETE FROM recurring_occurrences WHERE rule_id = ?1 AND status = 'pending'",
            params![patch.id],
        )?;
        self.conn.execute(
            "UPDATE recurring_rules
             SET name = ?1,
                 kind = ?2,
                 amount_cents = ?3,
                 account_id = ?4,
                 to_account_id = ?5,
                 category_id = ?6,
                 payee = ?7,
                 note = ?8,
                 cadence = ?9,
                 interval = ?10,
                 day_of_month = ?11,
                 weekday = ?12,
                 start_on = ?13,
                 end_on = ?14,
                 next_due_on = ?15,
                 updated_at = ?16
             WHERE id = ?17",
            params![
                resolved.name,
                resolved.kind.as_db_str(),
                resolved.amount_cents,
                resolved.account_id,
                resolved.to_account_id,
                resolved.category_id,
                normalize_optional_text(&resolved.payee),
                normalize_optional_text(&resolved.note),
                resolved.cadence.as_db_str(),
                resolved.interval,
                resolved.day_of_month.map(|value| value as i64),
                resolved.weekday.map(|value| value.as_db_str().to_string()),
                resolved.start_on,
                resolved.end_on,
                resolved.next_due_on,
                now_timestamp(),
                patch.id,
            ],
        )?;
        Ok(())
    }

    pub fn list_recurring_rules(&self) -> Result<Vec<RecurringRuleRecord>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                r.id,
                r.name,
                r.kind,
                r.amount_cents,
                source.id,
                source.name,
                target.id,
                target.name,
                category.id,
                category.name,
                r.payee,
                r.note,
                r.cadence,
                r.interval,
                r.day_of_month,
                r.weekday,
                r.start_on,
                r.end_on,
                COALESCE(
                    (
                        SELECT MIN(o.due_on)
                        FROM recurring_occurrences o
                        WHERE o.rule_id = r.id
                          AND o.status = 'pending'
                    ),
                    r.next_due_on
                ) AS display_next_due_on,
                r.paused,
                r.created_at,
                r.updated_at
             FROM recurring_rules r
             JOIN accounts source ON source.id = r.account_id
             LEFT JOIN accounts target ON target.id = r.to_account_id
             LEFT JOIN categories category ON category.id = r.category_id
             ORDER BY r.name COLLATE NOCASE",
        )?;

        let rows = statement.query_map([], |row| {
            let kind: String = row.get(2)?;
            let cadence: String = row.get(12)?;
            let weekday: Option<String> = row.get(15)?;
            Ok(RecurringRuleRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                amount_cents: row.get(3)?,
                account_id: row.get(4)?,
                account_name: row.get(5)?,
                to_account_id: row.get(6)?,
                to_account_name: row.get(7)?,
                category_id: row.get(8)?,
                category_name: row.get(9)?,
                payee: row.get(10)?,
                note: row.get(11)?,
                cadence: RecurringCadence::from_db(&cadence).map_err(map_db_error)?,
                interval: row.get(13)?,
                day_of_month: row.get::<_, Option<i64>>(14)?.map(|value| value as u32),
                weekday: match weekday {
                    Some(value) => Some(Weekday::from_db(&value).map_err(map_db_error)?),
                    None => None,
                },
                start_on: row.get(16)?,
                end_on: row.get(17)?,
                next_due_on: row.get(18)?,
                paused: row.get::<_, i64>(19)? == 1,
                created_at: row.get(20)?,
                updated_at: row.get(21)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
    pub fn pause_recurring_rule(&self, id: i64) -> Result<(), AppError> {
        self.update_recurring_pause_state(id, true)
    }

    pub fn resume_recurring_rule(&self, id: i64) -> Result<(), AppError> {
        self.update_recurring_pause_state(id, false)
    }

    pub fn delete_recurring_rule(&self, id: i64) -> Result<(), AppError> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM recurring_rules WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(AppError::NotFound(format!(
                "recurring rule `{id}` was not found"
            )));
        }

        self.conn.execute(
            "UPDATE transactions SET recurring_rule_id = NULL, updated_at = ?1 WHERE recurring_rule_id = ?2",
            params![now_timestamp(), id],
        )?;
        self.conn.execute(
            "DELETE FROM recurring_occurrences WHERE rule_id = ?1",
            params![id],
        )?;
        self.conn
            .execute("DELETE FROM recurring_rules WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn run_due_recurring(&self, through: &str) -> Result<usize, AppError> {
        self.sync_due_occurrences(through)?;
        let due = self.list_due_occurrences(through)?;
        let mut posted = 0_usize;
        for occurrence in due {
            if occurrence.status != OccurrenceStatus::Pending {
                continue;
            }
            let rule = self.load_recurring_rule(occurrence.rule_id)?;
            let transaction = ResolvedTransaction {
                txn_date: occurrence.due_on.clone(),
                kind: rule.kind,
                amount_cents: rule.amount_cents,
                account_id: rule.account_id,
                to_account_id: rule.to_account_id,
                category_id: rule.category_id,
                payee: rule.payee.clone(),
                note: rule.note.clone(),
                recurring_rule_id: Some(rule.id),
            };
            self.validate_resolved_transaction(&transaction)?;
            let transaction_id = self.insert_transaction(&transaction)?;
            self.conn.execute(
                "UPDATE recurring_occurrences SET transaction_id = ?1, status = 'posted' WHERE id = ?2",
                params![transaction_id, occurrence.id],
            )?;
            posted += 1;
        }
        Ok(posted)
    }

    pub fn list_due_occurrences(
        &self,
        through: &str,
    ) -> Result<Vec<RecurringOccurrenceRecord>, AppError> {
        self.sync_due_occurrences(through)?;
        let mut statement = self.conn.prepare(
            "SELECT
                o.id,
                o.rule_id,
                r.name,
                o.due_on,
                o.transaction_id,
                o.status,
                r.kind,
                r.amount_cents,
                a.name,
                o.created_at
             FROM recurring_occurrences o
             JOIN recurring_rules r ON r.id = o.rule_id
             JOIN accounts a ON a.id = r.account_id
             WHERE o.status = 'pending' AND o.due_on <= ?1
             ORDER BY o.due_on ASC, o.id ASC",
        )?;

        let rows = statement.query_map(params![through], |row| {
            let status: String = row.get(5)?;
            let kind: String = row.get(6)?;
            Ok(RecurringOccurrenceRecord {
                id: row.get(0)?,
                rule_id: row.get(1)?,
                rule_name: row.get(2)?,
                due_on: row.get(3)?,
                transaction_id: row.get(4)?,
                status: OccurrenceStatus::from_db(&status).map_err(map_db_error)?,
                kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                amount_cents: row.get(7)?,
                account_name: row.get(8)?,
                created_at: row.get(9)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn unreconciled_account_count(&self) -> Result<i64, AppError> {
        self.conn
            .query_row(
                "WITH account_refs AS (
                    SELECT account_id AS account_id
                    FROM transactions
                    WHERE deleted_at IS NULL AND reconciliation_id IS NULL
                    UNION
                    SELECT to_account_id AS account_id
                    FROM transactions
                    WHERE deleted_at IS NULL AND reconciliation_id IS NULL AND to_account_id IS NOT NULL
                 )
                 SELECT COUNT(DISTINCT account_id) FROM account_refs",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn set_budget(
        &self,
        month: &str,
        category_ref: &str,
        amount_cents: i64,
        account_ref: Option<&str>,
        scenario_ref: Option<&str>,
    ) -> Result<i64, AppError> {
        if amount_cents <= 0 {
            return Err(AppError::Validation(
                "budget amount must be positive".to_string(),
            ));
        }

        let month = normalize_month_key(month)?;
        let category_id = self.resolve_category_ref(category_ref, Some(&CategoryKind::Expense))?;
        let scenario_id = self.resolve_optional_scenario_ref(scenario_ref)?;
        let account_id = match account_ref.map(str::trim).filter(|value| !value.is_empty()) {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };
        let timestamp = now_timestamp();
        match scenario_id {
            Some(scenario_id) => {
                let existing: Option<(i64, Option<i64>)> = self
                    .conn
                    .query_row(
                        "SELECT id, account_id
                         FROM scenario_budget_overrides
                         WHERE scenario_id = ?1
                           AND month = ?2
                           AND category_id = ?3",
                        params![scenario_id, &month, category_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let baseline_account_id: Option<i64> = self
                    .conn
                    .query_row(
                        "SELECT account_id
                         FROM budgets
                         WHERE month = ?1
                           AND category_id = ?2",
                        params![&month, category_id],
                        |row| row.get(0),
                    )
                    .optional()?
                    .flatten();

                match existing {
                    Some((id, current_account_id)) => {
                        self.conn.execute(
                            "UPDATE scenario_budget_overrides
                             SET amount_cents = ?1,
                                 account_id = ?2,
                                 updated_at = ?3
                             WHERE id = ?4",
                            params![
                                amount_cents,
                                account_id.or(current_account_id),
                                timestamp,
                                id
                            ],
                        )?;
                        Ok(id)
                    }
                    None => {
                        self.conn.execute(
                            "INSERT INTO scenario_budget_overrides (
                                 scenario_id,
                                 month,
                                 category_id,
                                 account_id,
                                 amount_cents,
                                 created_at,
                                 updated_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                            params![
                                scenario_id,
                                month,
                                category_id,
                                account_id.or(baseline_account_id),
                                amount_cents,
                                timestamp,
                                timestamp,
                            ],
                        )?;
                        Ok(self.conn.last_insert_rowid())
                    }
                }
            }
            None => {
                let existing: Option<(i64, Option<i64>)> = self
                    .conn
                    .query_row(
                        "SELECT id, account_id FROM budgets WHERE month = ?1 AND category_id = ?2",
                        params![&month, category_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;

                match existing {
                    Some((id, current_account_id)) => {
                        self.conn.execute(
                            "UPDATE budgets
                             SET amount_cents = ?1,
                                 account_id = ?2,
                                 updated_at = ?3
                             WHERE id = ?4",
                            params![
                                amount_cents,
                                account_id.or(current_account_id),
                                timestamp,
                                id
                            ],
                        )?;
                        Ok(id)
                    }
                    None => {
                        self.conn.execute(
                            "INSERT INTO budgets (
                                 month,
                                 category_id,
                                 account_id,
                                 amount_cents,
                                 created_at,
                                 updated_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![
                                month,
                                category_id,
                                account_id,
                                amount_cents,
                                timestamp,
                                timestamp,
                            ],
                        )?;
                        Ok(self.conn.last_insert_rowid())
                    }
                }
            }
        }
    }

    pub fn delete_budget(
        &self,
        month: &str,
        category_ref: &str,
        scenario_ref: Option<&str>,
    ) -> Result<(), AppError> {
        let month = normalize_month_key(month)?;
        let category_id = self.resolve_category_ref(category_ref, Some(&CategoryKind::Expense))?;
        let scenario_id = self.resolve_optional_scenario_ref(scenario_ref)?;
        let changed = match scenario_id {
            Some(scenario_id) => self.conn.execute(
                "DELETE FROM scenario_budget_overrides
                 WHERE scenario_id = ?1
                   AND month = ?2
                   AND category_id = ?3",
                params![scenario_id, &month, category_id],
            )?,
            None => self.conn.execute(
                "DELETE FROM budgets
                 WHERE month = ?1
                   AND category_id = ?2",
                params![&month, category_id],
            )?,
        };

        if changed == 0 {
            let message = match scenario_ref
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                Some(scenario) => format!(
                    "scenario budget override for `{}` in {} ({}) was not found",
                    category_ref.trim(),
                    month,
                    scenario
                ),
                None => format!(
                    "budget for `{}` in {} was not found",
                    category_ref.trim(),
                    month
                ),
            };
            return Err(AppError::NotFound(message));
        }

        Ok(())
    }

    pub fn list_budgets(
        &self,
        month: Option<&str>,
        scenario_ref: Option<&str>,
    ) -> Result<Vec<BudgetRecord>, AppError> {
        let normalized_month = match month {
            Some(value) => Some(normalize_month_key(value)?),
            None => None,
        };
        let scenario_id = self.resolve_optional_scenario_ref(scenario_ref)?;
        let scenario_name = match scenario_id {
            Some(id) => Some(self.scenario_name(id)?),
            None => None,
        };
        let mut rows_by_key: BTreeMap<(String, i64), BudgetRecord> = BTreeMap::new();
        let mut statement = self.conn.prepare(
            "SELECT
                b.id,
                b.month,
                b.category_id,
                c.name,
                b.created_at,
                b.updated_at,
                b.account_id,
                a.name,
                b.amount_cents,
                0
             FROM budgets b
             JOIN categories c ON c.id = b.category_id
             LEFT JOIN accounts a ON a.id = b.account_id
             WHERE (?1 IS NULL OR b.month = ?1)
             ORDER BY b.month DESC, c.name COLLATE NOCASE",
        )?;
        let rows = statement.query_map(params![normalized_month.as_deref()], |row| {
            Ok(BudgetRecord {
                id: row.get(0)?,
                month: row.get(1)?,
                category_id: row.get(2)?,
                category_name: row.get(3)?,
                scenario_id,
                scenario_name: scenario_name.clone(),
                is_override: row.get::<_, i64>(9)? == 1,
                account_id: row.get(6)?,
                account_name: row.get(7)?,
                amount_cents: row.get(8)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?;
        for row in rows {
            let row = row?;
            rows_by_key.insert((row.month.clone(), row.category_id), row);
        }

        if let Some(scenario_id) = scenario_id {
            let mut override_statement = self.conn.prepare(
                "SELECT
                    o.id,
                    o.month,
                    o.category_id,
                    c.name,
                    o.created_at,
                    o.updated_at,
                    o.account_id,
                    a.name,
                    o.amount_cents,
                    1
                 FROM scenario_budget_overrides o
                 JOIN categories c ON c.id = o.category_id
                 LEFT JOIN accounts a ON a.id = o.account_id
                 WHERE o.scenario_id = ?1
                   AND (?2 IS NULL OR o.month = ?2)
                 ORDER BY o.month DESC, c.name COLLATE NOCASE",
            )?;
            let override_rows = override_statement.query_map(
                params![scenario_id, normalized_month.as_deref()],
                |row| {
                    Ok(BudgetRecord {
                        id: row.get(0)?,
                        month: row.get(1)?,
                        category_id: row.get(2)?,
                        category_name: row.get(3)?,
                        scenario_id: Some(scenario_id),
                        scenario_name: scenario_name.clone(),
                        is_override: row.get::<_, i64>(9)? == 1,
                        account_id: row.get(6)?,
                        account_name: row.get(7)?,
                        amount_cents: row.get(8)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )?;
            for row in override_rows {
                let row = row?;
                rows_by_key.insert((row.month.clone(), row.category_id), row);
            }
        }

        let mut rows: Vec<_> = rows_by_key.into_values().collect();
        rows.sort_by(|left, right| {
            right.month.cmp(&left.month).then(
                left.category_name
                    .to_lowercase()
                    .cmp(&right.category_name.to_lowercase()),
            )
        });
        Ok(rows)
    }

    pub fn budget_status(
        &self,
        month: &str,
        scenario_ref: Option<&str>,
    ) -> Result<Vec<BudgetStatusRecord>, AppError> {
        let month = normalize_month_key(month)?;
        let scenario_id = self.resolve_optional_scenario_ref(scenario_ref)?;
        let scenario_name = match scenario_id {
            Some(id) => Some(self.scenario_name(id)?),
            None => None,
        };
        let (from, to) = month_bounds(&month)?;
        let mut rows_by_category: BTreeMap<i64, BudgetStatusRecord> = BTreeMap::new();

        let mut budget_statement = self.conn.prepare(
            "SELECT
                b.category_id,
                c.name,
                b.account_id,
                a.name,
                b.amount_cents
             FROM budgets b
             JOIN categories c ON c.id = b.category_id
             LEFT JOIN accounts a ON a.id = b.account_id
             WHERE b.month = ?1
             ORDER BY c.name COLLATE NOCASE",
        )?;
        let budget_rows = budget_statement.query_map(params![&month], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in budget_rows {
            let (category_id, category_name, account_id, account_name, budget_cents) = row?;
            rows_by_category.insert(
                category_id,
                BudgetStatusRecord {
                    month: month.clone(),
                    category_id,
                    category_name,
                    scenario_id,
                    scenario_name: scenario_name.clone(),
                    is_override: false,
                    account_id,
                    account_name,
                    budget_cents,
                    spent_cents: 0,
                    remaining_cents: budget_cents,
                    over_budget: false,
                },
            );
        }

        if let Some(scenario_id) = scenario_id {
            let mut override_statement = self.conn.prepare(
                "SELECT
                    o.category_id,
                    c.name,
                    o.account_id,
                    a.name,
                    o.amount_cents
                 FROM scenario_budget_overrides o
                 JOIN categories c ON c.id = o.category_id
                 LEFT JOIN accounts a ON a.id = o.account_id
                 WHERE o.scenario_id = ?1
                   AND o.month = ?2
                 ORDER BY c.name COLLATE NOCASE",
            )?;
            let override_rows =
                override_statement.query_map(params![scenario_id, &month], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                })?;
            for row in override_rows {
                let (category_id, category_name, account_id, account_name, budget_cents) = row?;
                rows_by_category.insert(
                    category_id,
                    BudgetStatusRecord {
                        month: month.clone(),
                        category_id,
                        category_name,
                        scenario_id: Some(scenario_id),
                        scenario_name: scenario_name.clone(),
                        is_override: true,
                        account_id,
                        account_name,
                        budget_cents,
                        spent_cents: 0,
                        remaining_cents: budget_cents,
                        over_budget: false,
                    },
                );
            }
        }

        let mut spend_statement = self.conn.prepare(
            "SELECT
                t.category_id,
                t.account_id,
                c.name,
                SUM(t.amount_cents)
             FROM transactions t
             JOIN categories c ON c.id = t.category_id
             WHERE t.deleted_at IS NULL
               AND t.kind = 'expense'
               AND t.txn_date >= ?1
               AND t.txn_date <= ?2
               AND t.category_id IS NOT NULL
             GROUP BY t.category_id, t.account_id, c.name
             ORDER BY c.name COLLATE NOCASE",
        )?;
        let spend_rows = spend_statement.query_map(params![from, to], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in spend_rows {
            let (category_id, spend_account_id, category_name, spent_cents) = row?;
            let entry = rows_by_category
                .entry(category_id)
                .or_insert_with(|| BudgetStatusRecord {
                    month: month.clone(),
                    category_id,
                    category_name,
                    scenario_id,
                    scenario_name: scenario_name.clone(),
                    is_override: false,
                    account_id: None,
                    account_name: None,
                    budget_cents: 0,
                    spent_cents: 0,
                    remaining_cents: 0,
                    over_budget: false,
                });
            if let Some(mapped_account_id) = entry.account_id {
                if mapped_account_id != spend_account_id {
                    continue;
                }
            }
            entry.spent_cents += spent_cents;
            entry.remaining_cents = entry.budget_cents - entry.spent_cents;
            entry.over_budget = entry.budget_cents > 0 && entry.spent_cents > entry.budget_cents;
        }

        let mut rows: Vec<_> = rows_by_category.into_values().collect();
        rows.sort_by(|left, right| {
            left.category_name
                .to_lowercase()
                .cmp(&right.category_name.to_lowercase())
        });
        Ok(rows)
    }

    pub fn add_planning_item(&self, item: &NewPlanningItem) -> Result<i64, AppError> {
        let resolved = self.resolve_planning_item_input(item)?;
        self.insert_planning_item(&resolved)
    }

    pub fn edit_planning_item(&self, patch: &UpdatePlanningItem) -> Result<(), AppError> {
        let existing = self.load_planning_item(patch.id)?;
        if existing.archived {
            return Err(AppError::Validation(
                "archived planning items cannot be edited".to_string(),
            ));
        }

        let kind = patch.kind.unwrap_or(existing.kind);
        let account_id = match patch.account.as_deref() {
            Some(reference) => self.resolve_account_ref(reference)?,
            None => existing.account_id,
        };
        let to_account_id = if patch.clear_to_account {
            None
        } else if let Some(reference) = patch.to_account.as_deref() {
            Some(self.resolve_account_ref(reference)?)
        } else {
            existing.to_account_id
        };
        let category_id = if patch.clear_category {
            None
        } else if let Some(reference) = patch.category.as_deref() {
            match expected_category_kind(kind).as_ref() {
                Some(expected_kind) => {
                    Some(self.resolve_category_ref(reference, Some(expected_kind))?)
                }
                None => Some(self.resolve_category_ref(reference, None)?),
            }
        } else {
            existing.category_id
        };
        let scenario_id = if patch.clear_scenario {
            None
        } else if let Some(reference) = patch.scenario.as_deref() {
            Some(self.resolve_scenario_ref(reference)?)
        } else {
            existing.scenario_id
        };
        let resolved = ResolvedPlanningItem {
            scenario_id,
            title: patch.title.clone().unwrap_or(existing.title),
            kind,
            amount_cents: patch.amount_cents.unwrap_or(existing.amount_cents),
            account_id,
            to_account_id,
            category_id,
            due_on: patch.due_on.clone().unwrap_or(existing.due_on),
            payee: resolve_optional_patch(existing.payee, &patch.payee, patch.clear_payee),
            note: resolve_optional_patch(existing.note, &patch.note, patch.clear_note),
        };
        self.validate_resolved_planning_item(&resolved)?;

        let changed = self.conn.execute(
            "UPDATE planning_items
             SET scenario_id = ?1,
                 title = ?2,
                 kind = ?3,
                 amount_cents = ?4,
                 account_id = ?5,
                 to_account_id = ?6,
                 category_id = ?7,
                 due_on = ?8,
                 payee = ?9,
                 note = ?10,
                 updated_at = ?11
             WHERE id = ?12",
            params![
                resolved.scenario_id,
                resolved.title,
                resolved.kind.as_db_str(),
                resolved.amount_cents,
                resolved.account_id,
                resolved.to_account_id,
                resolved.category_id,
                resolved.due_on,
                normalize_optional_text(&resolved.payee),
                normalize_optional_text(&resolved.note),
                now_timestamp(),
                patch.id,
            ],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!(
                "planning item `{}` was not found",
                patch.id
            )));
        }
        Ok(())
    }

    pub fn list_planning_items(
        &self,
        scenario_ref: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<PlanningItemRecord>, AppError> {
        let scenario_id = match scenario_ref
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(reference) => Some(self.resolve_scenario_ref(reference)?),
            None => None,
        };
        let include_baseline_for_scenario = if scenario_id.is_some() { 1_i64 } else { 0_i64 };
        let from = from.map(normalize_date_value).transpose()?;
        let to = to.map(normalize_date_value).transpose()?;
        let mut statement = self.conn.prepare(
            "SELECT
                p.id,
                p.scenario_id,
                s.name,
                p.title,
                p.kind,
                p.amount_cents,
                source.id,
                source.name,
                target.id,
                target.name,
                category.id,
                category.name,
                p.due_on,
                p.payee,
                p.note,
                p.linked_transaction_id,
                p.archived,
                p.created_at,
                p.updated_at
             FROM planning_items p
             JOIN accounts source ON source.id = p.account_id
             LEFT JOIN accounts target ON target.id = p.to_account_id
             LEFT JOIN categories category ON category.id = p.category_id
             LEFT JOIN planning_scenarios s ON s.id = p.scenario_id
             WHERE p.archived = 0
               AND (p.scenario_id IS NULL OR s.archived = 0)
               AND (?1 = 0 OR p.scenario_id IS NULL OR p.scenario_id = ?2)
               AND (?3 IS NULL OR p.due_on >= ?3)
               AND (?4 IS NULL OR p.due_on <= ?4)
             ORDER BY p.due_on ASC, p.id ASC",
        )?;
        let rows = statement.query_map(
            params![
                include_baseline_for_scenario,
                scenario_id,
                from.as_deref(),
                to.as_deref(),
            ],
            |row| {
                let kind: String = row.get(4)?;
                Ok(PlanningItemRecord {
                    id: row.get(0)?,
                    scenario_id: row.get(1)?,
                    scenario_name: row.get(2)?,
                    title: row.get(3)?,
                    kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                    amount_cents: row.get(5)?,
                    account_id: row.get(6)?,
                    account_name: row.get(7)?,
                    to_account_id: row.get(8)?,
                    to_account_name: row.get(9)?,
                    category_id: row.get(10)?,
                    category_name: row.get(11)?,
                    due_on: row.get(12)?,
                    payee: row.get(13)?,
                    note: row.get(14)?,
                    linked_transaction_id: row.get(15)?,
                    archived: row.get::<_, i64>(16)? == 1,
                    created_at: row.get(17)?,
                    updated_at: row.get(18)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn delete_planning_item(&self, id: i64) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE planning_items SET archived = 1, updated_at = ?1 WHERE id = ?2 AND archived = 0",
            params![now_timestamp(), id],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!(
                "planning item `{id}` was not found"
            )));
        }
        Ok(())
    }

    pub fn post_planning_item(&self, id: i64) -> Result<i64, AppError> {
        let item = self.load_planning_item(id)?;
        if item.archived {
            return Err(AppError::Validation(
                "archived planning items cannot be posted".to_string(),
            ));
        }
        if item.linked_transaction_id.is_some() {
            return Err(AppError::Validation(
                "planning item is already linked to a real transaction".to_string(),
            ));
        }
        if matches!(
            item.kind,
            TransactionKind::Income | TransactionKind::Expense
        ) && item.category_id.is_none()
        {
            return Err(AppError::Validation(
                "income and expense planning items need a category before they can be posted"
                    .to_string(),
            ));
        }
        let transaction = ResolvedTransaction {
            txn_date: item.due_on.clone(),
            kind: item.kind,
            amount_cents: item.amount_cents,
            account_id: item.account_id,
            to_account_id: item.to_account_id,
            category_id: item.category_id,
            payee: item.payee.clone(),
            note: item.note.clone(),
            recurring_rule_id: None,
        };
        self.validate_resolved_transaction(&transaction)?;
        let transaction_id = self.insert_transaction(&transaction)?;
        self.conn.execute(
            "UPDATE planning_items
             SET linked_transaction_id = ?1,
                 updated_at = ?2
             WHERE id = ?3",
            params![transaction_id, now_timestamp(), id],
        )?;
        Ok(transaction_id)
    }

    pub fn add_planning_scenario(&self, scenario: &NewPlanningScenario) -> Result<i64, AppError> {
        let name = scenario.name.trim();
        if name.is_empty() {
            return Err(AppError::Validation(
                "scenario name cannot be empty".to_string(),
            ));
        }
        let timestamp = now_timestamp();
        match self.conn.execute(
            "INSERT INTO planning_scenarios (name, note, archived, created_at, updated_at)
             VALUES (?1, ?2, 0, ?3, ?3)",
            params![name, normalize_optional_text(&scenario.note), timestamp],
        ) {
            Ok(_) => Ok(self.conn.last_insert_rowid()),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("scenario", name))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn list_planning_scenarios(&self) -> Result<Vec<PlanningScenarioRecord>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT id, name, note, archived, created_at, updated_at
             FROM planning_scenarios
             WHERE archived = 0
             ORDER BY name COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(PlanningScenarioRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                note: row.get(2)?,
                archived: row.get::<_, i64>(3)? == 1,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn edit_planning_scenario(&self, patch: &UpdatePlanningScenario) -> Result<(), AppError> {
        let existing = self
            .conn
            .query_row(
                "SELECT name, note FROM planning_scenarios WHERE id = ?1 AND archived = 0",
                params![patch.id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("scenario `{}` was not found", patch.id)))?;
        let name = patch
            .name
            .as_deref()
            .unwrap_or(&existing.0)
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(AppError::Validation(
                "scenario name cannot be empty".to_string(),
            ));
        }
        let note = resolve_optional_patch(existing.1, &patch.note, patch.clear_note);
        let changed = self.conn.execute(
            "UPDATE planning_scenarios
             SET name = ?1,
                 note = ?2,
                 updated_at = ?3
             WHERE id = ?4",
            params![
                name,
                normalize_optional_text(&note),
                now_timestamp(),
                patch.id
            ],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!(
                "scenario `{}` was not found",
                patch.id
            )));
        }
        Ok(())
    }

    pub fn delete_planning_scenario(&self, id: i64) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE planning_scenarios
             SET archived = 1,
                 updated_at = ?1
             WHERE id = ?2 AND archived = 0",
            params![now_timestamp(), id],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!("scenario `{id}` was not found")));
        }
        Ok(())
    }

    pub fn add_planning_goal(&self, goal: &NewPlanningGoal) -> Result<i64, AppError> {
        let resolved = self.resolve_planning_goal_input(goal)?;
        self.insert_planning_goal(&resolved)
    }

    pub fn list_planning_goals(&self) -> Result<Vec<PlanningGoalRecord>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                g.id,
                g.name,
                g.kind,
                a.id,
                a.name,
                g.target_amount_cents,
                g.minimum_balance_cents,
                g.due_on,
                g.archived,
                g.created_at,
                g.updated_at
             FROM planning_goals g
             JOIN accounts a ON a.id = g.account_id
             WHERE g.archived = 0
             ORDER BY g.name COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            let kind: String = row.get(2)?;
            Ok(PlanningGoalRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                kind: PlanningGoalKind::from_db(&kind).map_err(map_db_error)?,
                account_id: row.get(3)?,
                account_name: row.get(4)?,
                target_amount_cents: row.get(5)?,
                minimum_balance_cents: row.get(6)?,
                due_on: row.get(7)?,
                archived: row.get::<_, i64>(8)? == 1,
                created_at: row.get(9)?,
                updated_at: row.get(10)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn edit_planning_goal(&self, patch: &UpdatePlanningGoal) -> Result<(), AppError> {
        let existing = self.load_planning_goal(patch.id)?;
        if existing.archived {
            return Err(AppError::Validation(
                "archived goals cannot be edited".to_string(),
            ));
        }
        let kind = patch.kind.unwrap_or(existing.kind);
        let account_id = match patch.account.as_deref() {
            Some(reference) => self.resolve_account_ref(reference)?,
            None => existing.account_id,
        };
        let target_amount_cents = if patch.clear_target_amount {
            None
        } else {
            patch.target_amount_cents.or(existing.target_amount_cents)
        };
        let minimum_balance_cents = if patch.clear_minimum_balance {
            None
        } else {
            patch
                .minimum_balance_cents
                .or(existing.minimum_balance_cents)
        };
        let due_on = if patch.clear_due_on {
            None
        } else {
            patch.due_on.clone().or(existing.due_on)
        };
        let resolved = StoredPlanningGoal {
            name: patch.name.clone().unwrap_or(existing.name),
            kind,
            account_id,
            target_amount_cents,
            minimum_balance_cents,
            due_on,
            archived: false,
            created_at: existing.created_at,
            updated_at: existing.updated_at,
        };
        self.validate_planning_goal(&resolved)?;

        let changed = self.conn.execute(
            "UPDATE planning_goals
             SET name = ?1,
                 kind = ?2,
                 account_id = ?3,
                 target_amount_cents = ?4,
                 minimum_balance_cents = ?5,
                 due_on = ?6,
                 updated_at = ?7
             WHERE id = ?8",
            params![
                resolved.name,
                resolved.kind.as_db_str(),
                resolved.account_id,
                resolved.target_amount_cents,
                resolved.minimum_balance_cents,
                resolved.due_on,
                now_timestamp(),
                patch.id,
            ],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!(
                "goal `{}` was not found",
                patch.id
            )));
        }
        Ok(())
    }

    pub fn delete_planning_goal(&self, id: i64) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE planning_goals
             SET archived = 1,
                 updated_at = ?1
             WHERE id = ?2 AND archived = 0",
            params![now_timestamp(), id],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!("goal `{id}` was not found")));
        }
        Ok(())
    }

    pub fn forecast(
        &self,
        scenario_ref: Option<&str>,
        account_ref: Option<&str>,
        days: usize,
    ) -> Result<ForecastSnapshot, AppError> {
        let scenario_id = self.resolve_optional_scenario_ref(scenario_ref)?;
        let account_id = match account_ref.map(str::trim).filter(|value| !value.is_empty()) {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };
        let days = days.clamp(1, 365);
        let today = Local::now().date_naive();
        let daily_end = today + Duration::days((days - 1) as i64);
        let month_start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
            .expect("current month should always be valid");
        let final_month_start = add_months_with_day(month_start, 11, 1)?;
        let monthly_end = add_months_with_day(final_month_start, 1, 1)?
            .pred_opt()
            .expect("month boundary should have a previous day");
        let overall_end = if daily_end > monthly_end {
            daily_end
        } else {
            monthly_end
        };

        let planning_items =
            self.list_planning_items_for_forecast(scenario_id, today, overall_end)?;
        let recurring_occurrences = self.list_forecast_recurring_occurrences(today, overall_end)?;
        let (budget_rows, mut warnings) =
            self.load_budget_forecast_rows(scenario_id, today, overall_end)?;
        let bill_calendar = self.build_bill_calendar(
            &planning_items,
            &recurring_occurrences,
            account_id,
            today,
            daily_end,
        );
        let goals = self.list_planning_goals()?;
        let tracked_goal_accounts: HashSet<i64> =
            goals.iter().map(|goal| goal.account_id).collect();

        let balances = self.balances(None)?;
        let mut account_balances = balances
            .into_iter()
            .map(|balance| (balance.account_id, balance.current_balance_cents))
            .collect::<HashMap<_, _>>();
        let initial_balances = account_balances.clone();
        let mut balance_history: HashMap<i64, Vec<(NaiveDate, i64)>> = tracked_goal_accounts
            .iter()
            .map(|account_id| (*account_id, Vec::new()))
            .collect();

        let mut events_by_date: BTreeMap<NaiveDate, Vec<ForecastEvent>> = BTreeMap::new();
        for item in &planning_items {
            events_by_date
                .entry(parse_date(&item.due_on)?)
                .or_default()
                .push(build_forecast_transaction_event(
                    item.kind,
                    item.amount_cents,
                    item.account_id,
                    item.to_account_id,
                    account_id,
                ));
        }
        for occurrence in &recurring_occurrences {
            events_by_date
                .entry(parse_date(&occurrence.due_on)?)
                .or_default()
                .push(build_forecast_transaction_event(
                    occurrence.kind,
                    occurrence.amount_cents,
                    occurrence.account_id,
                    occurrence.to_account_id,
                    account_id,
                ));
        }
        self.schedule_budget_forecast_events(
            &budget_rows,
            today,
            overall_end,
            account_id,
            &mut warnings,
            &mut events_by_date,
        )?;

        let mut monthly = initialize_forecast_months(month_start)?;
        let mut daily = Vec::with_capacity(days);
        let mut first_negative_date = None;
        let mut goal_breach_dates: HashMap<i64, String> = HashMap::new();

        let mut cursor = today;
        while cursor <= overall_end {
            let opening_balance = forecast_scope_balance(&account_balances, account_id);
            let mut inflow_cents = 0_i64;
            let mut outflow_cents = 0_i64;
            if let Some(events) = events_by_date.get(&cursor) {
                for event in events {
                    inflow_cents += event.inflow_cents;
                    outflow_cents += event.outflow_cents;
                    for (event_account_id, delta_cents) in &event.per_account_delta {
                        *account_balances.entry(*event_account_id).or_insert(0) += *delta_cents;
                    }
                }
            }
            let closing_balance = forecast_scope_balance(&account_balances, account_id);
            let month_key = cursor.format("%Y-%m").to_string();
            if let Some(point) = monthly.iter_mut().find(|point| point.month == month_key) {
                point.inflow_cents += inflow_cents;
                point.outflow_cents += outflow_cents;
                point.net_cents = point.inflow_cents - point.outflow_cents;
                point.ending_balance_cents = closing_balance;
            }

            for tracked_account_id in &tracked_goal_accounts {
                let balance = *account_balances.get(tracked_account_id).unwrap_or(&0);
                balance_history
                    .entry(*tracked_account_id)
                    .or_default()
                    .push((cursor, balance));
            }

            let mut point_alerts = Vec::new();
            if closing_balance < 0 && first_negative_date.is_none() {
                let date = cursor.format("%Y-%m-%d").to_string();
                first_negative_date = Some(date.clone());
                point_alerts.push(format!("Projected balance turns negative on {date}."));
            }
            for goal in &goals {
                if goal.kind != PlanningGoalKind::BalanceTarget {
                    continue;
                }
                let Some(minimum_balance_cents) = goal.minimum_balance_cents else {
                    continue;
                };
                let goal_balance = *account_balances.get(&goal.account_id).unwrap_or(&0);
                if goal_balance < minimum_balance_cents && !goal_breach_dates.contains_key(&goal.id)
                {
                    let date = cursor.format("%Y-%m-%d").to_string();
                    goal_breach_dates.insert(goal.id, date.clone());
                    point_alerts.push(format!(
                        "{} falls below its balance target on {}.",
                        goal.name, date
                    ));
                }
            }

            if cursor <= daily_end {
                daily.push(ForecastDailyPoint {
                    date: cursor.format("%Y-%m-%d").to_string(),
                    opening_balance_cents: opening_balance,
                    inflow_cents,
                    outflow_cents,
                    net_cents: inflow_cents - outflow_cents,
                    closing_balance_cents: closing_balance,
                    alerts: point_alerts,
                });
            }

            cursor += Duration::days(1);
        }

        let mut alerts = Vec::new();
        if let Some(date) = first_negative_date {
            alerts.push(format!("Projected balance turns negative on {date}."));
        }
        if let Some((goal_id, date)) = goal_breach_dates
            .iter()
            .min_by(|left, right| left.1.cmp(right.1))
        {
            if let Some(goal) = goals.iter().find(|goal| goal.id == *goal_id) {
                alerts.push(format!(
                    "{} falls below its minimum balance on {}.",
                    goal.name, date
                ));
            }
        }

        let goal_status = build_goal_status_records(
            &goals,
            &initial_balances,
            &balance_history,
            overall_end,
            &goal_breach_dates,
        )?;

        Ok(ForecastSnapshot {
            scenario: self.forecast_scenario_selection(scenario_id)?,
            as_of: today.format("%Y-%m-%d").to_string(),
            account: self.forecast_account_selection(account_id)?,
            warnings,
            alerts,
            daily,
            monthly,
            goal_status,
            bill_calendar,
        })
    }

    fn resolve_planning_item_input(
        &self,
        item: &NewPlanningItem,
    ) -> Result<ResolvedPlanningItem, AppError> {
        let scenario_id = self.resolve_optional_scenario_ref(item.scenario.as_deref())?;
        let account_id = self.resolve_account_ref(&item.account)?;
        let to_account_id = match item.to_account.as_deref() {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };
        let category_id = match item.category.as_deref() {
            Some(reference) => match expected_category_kind(item.kind).as_ref() {
                Some(expected_kind) => {
                    Some(self.resolve_category_ref(reference, Some(expected_kind))?)
                }
                None => Some(self.resolve_category_ref(reference, None)?),
            },
            None => None,
        };
        let resolved = ResolvedPlanningItem {
            scenario_id,
            title: item.title.trim().to_string(),
            kind: item.kind,
            amount_cents: item.amount_cents,
            account_id,
            to_account_id,
            category_id,
            due_on: item.due_on.clone(),
            payee: item.payee.clone(),
            note: item.note.clone(),
        };
        self.validate_resolved_planning_item(&resolved)?;
        Ok(resolved)
    }

    fn validate_resolved_planning_item(&self, item: &ResolvedPlanningItem) -> Result<(), AppError> {
        if item.title.trim().is_empty() {
            return Err(AppError::Validation(
                "planning item title cannot be empty".to_string(),
            ));
        }
        parse_date(&item.due_on)?;
        if item.amount_cents <= 0 {
            return Err(AppError::Validation(
                "planning item amount must be positive".to_string(),
            ));
        }
        let account_exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM accounts WHERE id = ?1 AND archived = 0",
            params![item.account_id],
            |row| row.get(0),
        )?;
        if account_exists == 0 {
            return Err(AppError::NotFound(format!(
                "account `{}` was not found",
                item.account_id
            )));
        }
        if let Some(target_id) = item.to_account_id {
            let target_exists: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM accounts WHERE id = ?1 AND archived = 0",
                params![target_id],
                |row| row.get(0),
            )?;
            if target_exists == 0 {
                return Err(AppError::NotFound(format!(
                    "account `{target_id}` was not found"
                )));
            }
        }
        if let Some(category_id) = item.category_id {
            let category_exists: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM categories WHERE id = ?1 AND archived = 0",
                params![category_id],
                |row| row.get(0),
            )?;
            if category_exists == 0 {
                return Err(AppError::NotFound(format!(
                    "category `{category_id}` was not found"
                )));
            }
        }
        if let Some(scenario_id) = item.scenario_id {
            let scenario_exists: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM planning_scenarios WHERE id = ?1 AND archived = 0",
                params![scenario_id],
                |row| row.get(0),
            )?;
            if scenario_exists == 0 {
                return Err(AppError::NotFound(format!(
                    "scenario `{scenario_id}` was not found"
                )));
            }
        }

        match item.kind {
            TransactionKind::Income | TransactionKind::Expense => {
                if item.to_account_id.is_some() {
                    return Err(AppError::Validation(
                        "income and expense planning items cannot target a second account"
                            .to_string(),
                    ));
                }
            }
            TransactionKind::Transfer => {
                if item.to_account_id.is_none() {
                    return Err(AppError::Validation(
                        "transfer planning items require --to-account".to_string(),
                    ));
                }
                if item.category_id.is_some() {
                    return Err(AppError::Validation(
                        "transfer planning items cannot use a category".to_string(),
                    ));
                }
                if item.to_account_id == Some(item.account_id) {
                    return Err(AppError::Validation(
                        "transfer planning items must use different source and destination accounts"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn insert_planning_item(&self, item: &ResolvedPlanningItem) -> Result<i64, AppError> {
        let timestamp = now_timestamp();
        self.conn.execute(
            "INSERT INTO planning_items (
                 scenario_id,
                 title,
                 kind,
                 amount_cents,
                 account_id,
                 to_account_id,
                 category_id,
                 due_on,
                 payee,
                 note,
                 linked_transaction_id,
                 archived,
                 created_at,
                 updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, 0, ?11, ?11)",
            params![
                item.scenario_id,
                item.title,
                item.kind.as_db_str(),
                item.amount_cents,
                item.account_id,
                item.to_account_id,
                item.category_id,
                item.due_on,
                normalize_optional_text(&item.payee),
                normalize_optional_text(&item.note),
                timestamp,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn load_planning_item(&self, id: i64) -> Result<StoredPlanningItem, AppError> {
        self.conn
            .query_row(
                "SELECT
                    id,
                    scenario_id,
                    title,
                    kind,
                    amount_cents,
                    account_id,
                    to_account_id,
                    category_id,
                    due_on,
                    payee,
                    note,
                    linked_transaction_id,
                    archived,
                    created_at,
                    updated_at
                 FROM planning_items
                 WHERE id = ?1",
                params![id],
                |row| {
                    let kind: String = row.get(3)?;
                    Ok(StoredPlanningItem {
                        scenario_id: row.get(1)?,
                        title: row.get(2)?,
                        kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                        amount_cents: row.get(4)?,
                        account_id: row.get(5)?,
                        to_account_id: row.get(6)?,
                        category_id: row.get(7)?,
                        due_on: row.get(8)?,
                        payee: row.get(9)?,
                        note: row.get(10)?,
                        linked_transaction_id: row.get(11)?,
                        archived: row.get::<_, i64>(12)? == 1,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("planning item `{id}` was not found")))
    }

    fn resolve_optional_scenario_ref(
        &self,
        reference: Option<&str>,
    ) -> Result<Option<i64>, AppError> {
        match reference.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => Ok(Some(self.resolve_scenario_ref(value)?)),
            None => Ok(None),
        }
    }

    fn resolve_scenario_ref(&self, reference: &str) -> Result<i64, AppError> {
        let trimmed = reference.trim();
        let id_lookup = trimmed.parse::<i64>().ok();
        let scenario_id = self
            .conn
            .query_row(
                "SELECT id
                 FROM planning_scenarios
                 WHERE archived = 0
                   AND (id = ?1 OR name = ?2 COLLATE NOCASE)",
                params![id_lookup, trimmed],
                |row| row.get(0),
            )
            .optional()?;
        scenario_id.ok_or_else(|| AppError::invalid_ref("scenario", trimmed))
    }

    fn resolve_planning_goal_input(
        &self,
        goal: &NewPlanningGoal,
    ) -> Result<StoredPlanningGoal, AppError> {
        let resolved = StoredPlanningGoal {
            name: goal.name.trim().to_string(),
            kind: goal.kind,
            account_id: self.resolve_account_ref(&goal.account)?,
            target_amount_cents: goal.target_amount_cents,
            minimum_balance_cents: goal.minimum_balance_cents,
            due_on: goal.due_on.clone(),
            archived: false,
            created_at: String::new(),
            updated_at: String::new(),
        };
        self.validate_planning_goal(&resolved)?;
        Ok(resolved)
    }

    fn validate_planning_goal(&self, goal: &StoredPlanningGoal) -> Result<(), AppError> {
        if goal.name.trim().is_empty() {
            return Err(AppError::Validation(
                "goal name cannot be empty".to_string(),
            ));
        }
        let account_exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM accounts WHERE id = ?1 AND archived = 0",
            params![goal.account_id],
            |row| row.get(0),
        )?;
        if account_exists == 0 {
            return Err(AppError::NotFound(format!(
                "account `{}` was not found",
                goal.account_id
            )));
        }
        match goal.kind {
            PlanningGoalKind::SinkingFund => {
                if goal.minimum_balance_cents.is_some() {
                    return Err(AppError::Validation(
                        "sinking fund goals cannot define a minimum balance".to_string(),
                    ));
                }
                let target_amount_cents = goal.target_amount_cents.ok_or_else(|| {
                    AppError::Validation("sinking fund goals require --target-amount".to_string())
                })?;
                if target_amount_cents <= 0 {
                    return Err(AppError::Validation(
                        "target amount must be positive".to_string(),
                    ));
                }
                let due_on = goal.due_on.as_deref().ok_or_else(|| {
                    AppError::Validation("sinking fund goals require --due-on".to_string())
                })?;
                parse_date(due_on)?;
            }
            PlanningGoalKind::BalanceTarget => {
                if goal.target_amount_cents.is_some() || goal.due_on.is_some() {
                    return Err(AppError::Validation(
                        "balance target goals only accept --minimum-balance".to_string(),
                    ));
                }
                let minimum_balance_cents = goal.minimum_balance_cents.ok_or_else(|| {
                    AppError::Validation(
                        "balance target goals require --minimum-balance".to_string(),
                    )
                })?;
                if minimum_balance_cents <= 0 {
                    return Err(AppError::Validation(
                        "minimum balance must be positive".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn insert_planning_goal(&self, goal: &StoredPlanningGoal) -> Result<i64, AppError> {
        let timestamp = now_timestamp();
        match self.conn.execute(
            "INSERT INTO planning_goals (
                 name,
                 kind,
                 account_id,
                 target_amount_cents,
                 minimum_balance_cents,
                 due_on,
                 archived,
                 created_at,
                 updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7)",
            params![
                goal.name,
                goal.kind.as_db_str(),
                goal.account_id,
                goal.target_amount_cents,
                goal.minimum_balance_cents,
                goal.due_on,
                timestamp,
            ],
        ) {
            Ok(_) => Ok(self.conn.last_insert_rowid()),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("goal", &goal.name))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn load_planning_goal(&self, id: i64) -> Result<StoredPlanningGoal, AppError> {
        self.conn
            .query_row(
                "SELECT
                    id,
                    name,
                    kind,
                    account_id,
                    target_amount_cents,
                    minimum_balance_cents,
                    due_on,
                    archived,
                    created_at,
                    updated_at
                 FROM planning_goals
                 WHERE id = ?1",
                params![id],
                |row| {
                    let kind: String = row.get(2)?;
                    Ok(StoredPlanningGoal {
                        name: row.get(1)?,
                        kind: PlanningGoalKind::from_db(&kind).map_err(map_db_error)?,
                        account_id: row.get(3)?,
                        target_amount_cents: row.get(4)?,
                        minimum_balance_cents: row.get(5)?,
                        due_on: row.get(6)?,
                        archived: row.get::<_, i64>(7)? == 1,
                        created_at: row.get(8)?,
                        updated_at: row.get(9)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("goal `{id}` was not found")))
    }

    fn forecast_scenario_selection(
        &self,
        scenario_id: Option<i64>,
    ) -> Result<ForecastSelection, AppError> {
        match scenario_id {
            Some(id) => Ok(ForecastSelection {
                id: Some(id),
                name: Some(self.scenario_name(id)?),
            }),
            None => Ok(ForecastSelection {
                id: None,
                name: Some("baseline".to_string()),
            }),
        }
    }

    fn forecast_account_selection(
        &self,
        account_id: Option<i64>,
    ) -> Result<ForecastSelection, AppError> {
        match account_id {
            Some(id) => Ok(ForecastSelection {
                id: Some(id),
                name: Some(self.account_name(id)?),
            }),
            None => Ok(ForecastSelection {
                id: None,
                name: None,
            }),
        }
    }

    fn scenario_name(&self, scenario_id: i64) -> Result<String, AppError> {
        self.conn
            .query_row(
                "SELECT name FROM planning_scenarios WHERE id = ?1 AND archived = 0",
                params![scenario_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("scenario `{scenario_id}` was not found")))
    }

    fn list_planning_items_for_forecast(
        &self,
        scenario_id: Option<i64>,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<PlanningItemRecord>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                p.id,
                p.scenario_id,
                s.name,
                p.title,
                p.kind,
                p.amount_cents,
                source.id,
                source.name,
                target.id,
                target.name,
                category.id,
                category.name,
                p.due_on,
                p.payee,
                p.note,
                p.linked_transaction_id,
                p.archived,
                p.created_at,
                p.updated_at
             FROM planning_items p
             JOIN accounts source ON source.id = p.account_id
             LEFT JOIN accounts target ON target.id = p.to_account_id
             LEFT JOIN categories category ON category.id = p.category_id
             LEFT JOIN planning_scenarios s ON s.id = p.scenario_id
             WHERE p.archived = 0
               AND (p.scenario_id IS NULL OR s.archived = 0)
               AND (
                    (?1 IS NULL AND p.scenario_id IS NULL)
                    OR
                    (?1 IS NOT NULL AND (p.scenario_id IS NULL OR p.scenario_id = ?1))
               )
               AND p.due_on >= ?2
               AND p.due_on <= ?3
             ORDER BY p.due_on ASC, p.id ASC",
        )?;
        let rows = statement.query_map(
            params![
                scenario_id,
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ],
            |row| {
                let kind: String = row.get(4)?;
                Ok(PlanningItemRecord {
                    id: row.get(0)?,
                    scenario_id: row.get(1)?,
                    scenario_name: row.get(2)?,
                    title: row.get(3)?,
                    kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                    amount_cents: row.get(5)?,
                    account_id: row.get(6)?,
                    account_name: row.get(7)?,
                    to_account_id: row.get(8)?,
                    to_account_name: row.get(9)?,
                    category_id: row.get(10)?,
                    category_name: row.get(11)?,
                    due_on: row.get(12)?,
                    payee: row.get(13)?,
                    note: row.get(14)?,
                    linked_transaction_id: row.get(15)?,
                    archived: row.get::<_, i64>(16)? == 1,
                    created_at: row.get(17)?,
                    updated_at: row.get(18)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn list_forecast_recurring_occurrences(
        &self,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<RecurringForecastOccurrence>, AppError> {
        let mut pending_statement = self.conn.prepare(
            "SELECT
                r.id,
                o.due_on,
                r.name,
                r.kind,
                r.amount_cents,
                source.id,
                source.name,
                r.to_account_id,
                category.name
             FROM recurring_occurrences o
             JOIN recurring_rules r ON r.id = o.rule_id
             JOIN accounts source ON source.id = r.account_id
             LEFT JOIN categories category ON category.id = r.category_id
             WHERE o.status = 'pending'
               AND o.due_on >= ?1
               AND o.due_on <= ?2
             ORDER BY o.due_on ASC, o.id ASC",
        )?;
        let rows = pending_statement.query_map(
            params![
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            ],
            |row| {
                let kind: String = row.get(3)?;
                Ok(RecurringForecastOccurrence {
                    rule_id: row.get(0)?,
                    due_on: row.get(1)?,
                    rule_name: row.get(2)?,
                    kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                    amount_cents: row.get(4)?,
                    account_id: row.get(5)?,
                    account_name: row.get(6)?,
                    to_account_id: row.get(7)?,
                    category_name: row.get(8)?,
                })
            },
        )?;
        let mut occurrences = rows.collect::<Result<Vec<_>, _>>()?;
        let materialized_keys = occurrences
            .iter()
            .map(|occurrence| (occurrence.rule_id, occurrence.due_on.clone()))
            .collect::<HashSet<_>>();

        let mut rule_statement = self.conn.prepare(
            "SELECT
                r.id,
                r.name,
                r.kind,
                r.amount_cents,
                source.id,
                source.name,
                r.to_account_id,
                category.name,
                r.cadence,
                r.interval,
                r.day_of_month,
                r.weekday,
                r.end_on,
                r.next_due_on
             FROM recurring_rules r
             JOIN accounts source ON source.id = r.account_id
             LEFT JOIN categories category ON category.id = r.category_id
             WHERE r.paused = 0
             ORDER BY r.id ASC",
        )?;
        let rules = rule_statement.query_map([], |row| {
            let kind: String = row.get(2)?;
            let cadence: String = row.get(8)?;
            let weekday: Option<String> = row.get(11)?;
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                TransactionKind::from_db(&kind).map_err(map_db_error)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                RecurringCadence::from_db(&cadence).map_err(map_db_error)?,
                row.get::<_, i64>(9)?,
                row.get::<_, Option<i64>>(10)?.map(|value| value as u32),
                match weekday {
                    Some(value) => Some(Weekday::from_db(&value).map_err(map_db_error)?),
                    None => None,
                },
                row.get::<_, Option<String>>(12)?,
                row.get::<_, String>(13)?,
            ))
        })?;

        for rule in rules {
            let (
                rule_id,
                rule_name,
                kind,
                amount_cents,
                account_id,
                account_name,
                to_account_id,
                category_name,
                cadence,
                interval,
                day_of_month,
                weekday,
                end_on,
                next_due_on,
            ) = rule?;

            let mut due_on = parse_date(&next_due_on)?;
            let limit = end_on.as_deref().map(parse_date).transpose()?;

            while due_on < start {
                if let Some(limit) = limit {
                    if due_on > limit {
                        break;
                    }
                }
                due_on = advance_recurrence(due_on, cadence, interval, day_of_month, weekday)?;
            }

            while due_on <= end {
                if let Some(limit) = limit {
                    if due_on > limit {
                        break;
                    }
                }
                let due_on_text = due_on.format("%Y-%m-%d").to_string();
                if !materialized_keys.contains(&(rule_id, due_on_text.clone())) {
                    occurrences.push(RecurringForecastOccurrence {
                        rule_id,
                        due_on: due_on_text,
                        rule_name: rule_name.clone(),
                        kind,
                        amount_cents,
                        account_id,
                        account_name: account_name.clone(),
                        to_account_id,
                        category_name: category_name.clone(),
                    });
                }
                due_on = advance_recurrence(due_on, cadence, interval, day_of_month, weekday)?;
            }
        }

        occurrences.sort_by(|left, right| {
            left.due_on
                .cmp(&right.due_on)
                .then(left.rule_id.cmp(&right.rule_id))
        });
        Ok(occurrences)
    }

    fn load_budget_forecast_rows(
        &self,
        scenario_id: Option<i64>,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<(Vec<BudgetForecastRow>, Vec<String>), AppError> {
        let start_month = start.format("%Y-%m").to_string();
        let end_month = end.format("%Y-%m").to_string();
        let mut rows_by_key: BTreeMap<(String, i64), BudgetForecastRow> = BTreeMap::new();

        let mut baseline_statement = self.conn.prepare(
            "SELECT
                b.month,
                b.category_id,
                c.name,
                b.account_id,
                a.name,
                b.amount_cents
             FROM budgets b
             JOIN categories c ON c.id = b.category_id
             LEFT JOIN accounts a ON a.id = b.account_id
             WHERE b.month >= ?1
               AND b.month <= ?2
             ORDER BY b.month ASC, c.name COLLATE NOCASE",
        )?;
        let baseline_rows =
            baseline_statement.query_map(params![&start_month, &end_month], |row| {
                Ok(BudgetForecastRow {
                    month: row.get(0)?,
                    category_id: row.get(1)?,
                    category_name: row.get(2)?,
                    account_id: row.get(3)?,
                    amount_cents: row.get(5)?,
                })
            })?;
        for row in baseline_rows {
            let row = row?;
            rows_by_key.insert((row.month.clone(), row.category_id), row);
        }

        if let Some(scenario_id) = scenario_id {
            let mut override_statement = self.conn.prepare(
                "SELECT
                    o.month,
                    o.category_id,
                    c.name,
                    o.account_id,
                    a.name,
                    o.amount_cents
                 FROM scenario_budget_overrides o
                 JOIN categories c ON c.id = o.category_id
                 LEFT JOIN accounts a ON a.id = o.account_id
                 WHERE o.scenario_id = ?1
                   AND o.month >= ?2
                   AND o.month <= ?3
                 ORDER BY o.month ASC, c.name COLLATE NOCASE",
            )?;
            let override_rows = override_statement.query_map(
                params![scenario_id, &start_month, &end_month],
                |row| {
                    Ok(BudgetForecastRow {
                        month: row.get(0)?,
                        category_id: row.get(1)?,
                        category_name: row.get(2)?,
                        account_id: row.get(3)?,
                        amount_cents: row.get(5)?,
                    })
                },
            )?;
            for row in override_rows {
                let row = row?;
                rows_by_key.insert((row.month.clone(), row.category_id), row);
            }
        }

        Ok((rows_by_key.into_values().collect(), Vec::new()))
    }

    fn build_bill_calendar(
        &self,
        planning_items: &[PlanningItemRecord],
        recurring_occurrences: &[RecurringForecastOccurrence],
        account_id: Option<i64>,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Vec<BillCalendarItem> {
        let start = start.format("%Y-%m-%d").to_string();
        let end = end.format("%Y-%m-%d").to_string();
        let mut items = Vec::new();
        for item in planning_items {
            if item.kind != TransactionKind::Expense {
                continue;
            }
            if item.due_on < start || item.due_on > end {
                continue;
            }
            if account_id.is_some() && item.account_id != account_id.unwrap() {
                continue;
            }
            items.push(BillCalendarItem {
                date: item.due_on.clone(),
                title: item.title.clone(),
                source: if item.scenario_id.is_some() {
                    "planned_scenario".to_string()
                } else {
                    "planned".to_string()
                },
                kind: item.kind,
                amount_cents: item.amount_cents,
                account_id: item.account_id,
                account_name: item.account_name.clone(),
                category_name: item.category_name.clone(),
                scenario_name: item.scenario_name.clone(),
                linked_transaction_id: item.linked_transaction_id,
            });
        }
        for occurrence in recurring_occurrences {
            if occurrence.kind != TransactionKind::Expense {
                continue;
            }
            if occurrence.due_on < start || occurrence.due_on > end {
                continue;
            }
            if account_id.is_some() && occurrence.account_id != account_id.unwrap() {
                continue;
            }
            items.push(BillCalendarItem {
                date: occurrence.due_on.clone(),
                title: occurrence.rule_name.clone(),
                source: "recurring".to_string(),
                kind: occurrence.kind,
                amount_cents: occurrence.amount_cents,
                account_id: occurrence.account_id,
                account_name: occurrence.account_name.clone(),
                category_name: occurrence.category_name.clone(),
                scenario_name: None,
                linked_transaction_id: None,
            });
        }
        items.sort_by(|left, right| {
            left.date
                .cmp(&right.date)
                .then(left.title.cmp(&right.title))
        });
        items
    }

    fn schedule_budget_forecast_events(
        &self,
        budget_rows: &[BudgetForecastRow],
        today: NaiveDate,
        end: NaiveDate,
        selected_account_id: Option<i64>,
        warnings: &mut Vec<String>,
        events_by_date: &mut BTreeMap<NaiveDate, Vec<ForecastEvent>>,
    ) -> Result<(), AppError> {
        let current_month = today.format("%Y-%m").to_string();
        for row in budget_rows {
            let Some(account_id) = row.account_id else {
                warnings.push(format!(
                    "Budget {} in {} has no account mapping.",
                    row.category_name, row.month
                ));
                continue;
            };
            let (month_start_raw, month_end_raw) = month_bounds(&row.month)?;
            let month_start = parse_date(&month_start_raw)?;
            let month_end = parse_date(&month_end_raw)?;
            let distribution_start = if row.month == current_month {
                today
            } else {
                month_start
            };
            let distribution_end = if month_end < end { month_end } else { end };
            if distribution_start > distribution_end {
                continue;
            }
            let mut amount_cents = row.amount_cents;
            if row.month == current_month {
                let spent_cents =
                    self.actual_budget_spend_so_far(&row.month, row.category_id, Some(account_id))?;
                amount_cents = (row.amount_cents - spent_cents).max(0);
            }
            if amount_cents == 0 {
                continue;
            }
            let day_count = (distribution_end - distribution_start).num_days() + 1;
            let base_amount = amount_cents / day_count;
            let remainder = amount_cents % day_count;
            for step in 0..day_count {
                let amount_for_day = base_amount + if step < remainder { 1 } else { 0 };
                if amount_for_day == 0 {
                    continue;
                }
                let date = distribution_start + Duration::days(step);
                events_by_date
                    .entry(date)
                    .or_default()
                    .push(build_budget_forecast_event(
                        account_id,
                        amount_for_day,
                        selected_account_id,
                    ));
            }
        }
        Ok(())
    }

    fn actual_budget_spend_so_far(
        &self,
        month: &str,
        category_id: i64,
        account_id: Option<i64>,
    ) -> Result<i64, AppError> {
        let (from, _) = month_bounds(month)?;
        let today = Local::now().date_naive().format("%Y-%m-%d").to_string();
        self.conn
            .query_row(
                "SELECT COALESCE(SUM(amount_cents), 0)
                 FROM transactions
                 WHERE deleted_at IS NULL
                   AND kind = 'expense'
                   AND category_id = ?1
                   AND txn_date >= ?2
                   AND txn_date <= ?3
                   AND (?4 IS NULL OR account_id = ?4)",
                params![category_id, from, today, account_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn import_csv_transactions(
        &self,
        plan: &CsvImportPlan,
    ) -> Result<CsvImportResult, AppError> {
        let account_id = self.resolve_account_ref(&plan.account)?;
        let account_name = self.account_name(account_id)?;
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(plan.delimiter)
            .from_path(&plan.path)?;
        let headers = reader.headers()?.clone();
        let date_index = find_csv_column(&headers, &plan.date_column)?;
        let amount_index = find_csv_column(&headers, &plan.amount_column)?;
        let description_index = find_csv_column(&headers, &plan.description_column)?;
        let category_index = find_optional_csv_column(&headers, plan.category_column.as_deref())?;
        let payee_index = find_optional_csv_column(&headers, plan.payee_column.as_deref())?;
        let note_index = find_optional_csv_column(&headers, plan.note_column.as_deref())?;
        let type_index = find_optional_csv_column(&headers, plan.type_column.as_deref())?;

        let mut preview = Vec::new();
        let mut imported_count = 0_usize;
        let mut duplicate_count = 0_usize;

        for (record_index, record) in reader.records().enumerate() {
            let record = record?;
            let line_number = record_index + 2;
            let txn_date = parse_import_date(
                required_csv_value(&record, date_index, line_number, &plan.date_column)?,
                &plan.date_format,
            )?;
            let signed_amount = crate::amount::parse_signed_amount_to_cents(required_csv_value(
                &record,
                amount_index,
                line_number,
                &plan.amount_column,
            )?)?;
            let kind = if let Some(index) = type_index {
                let raw_kind = optional_csv_value(&record, index).unwrap_or("").trim();
                if raw_kind.is_empty() {
                    plan.default_kind.unwrap_or_else(|| {
                        if signed_amount < 0 {
                            TransactionKind::Expense
                        } else {
                            TransactionKind::Income
                        }
                    })
                } else {
                    parse_import_kind(raw_kind)?
                }
            } else {
                plan.default_kind.unwrap_or_else(|| {
                    if signed_amount < 0 {
                        TransactionKind::Expense
                    } else {
                        TransactionKind::Income
                    }
                })
            };
            if kind == TransactionKind::Transfer {
                return Err(AppError::Validation(format!(
                    "CSV import does not support transfer rows (line {line_number})"
                )));
            }

            let amount_cents = signed_amount.abs();
            if amount_cents == 0 {
                return Err(AppError::Validation(format!(
                    "amount must be non-zero on CSV line {line_number}"
                )));
            }

            let description = required_csv_value(
                &record,
                description_index,
                line_number,
                &plan.description_column,
            )?
            .trim()
            .to_string();
            let payee = payee_index
                .and_then(|index| optional_csv_value(&record, index))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| {
                    if description.is_empty() {
                        None
                    } else {
                        Some(description.clone())
                    }
                });
            let note = note_index
                .and_then(|index| optional_csv_value(&record, index))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);

            let category_ref =
                match category_index.and_then(|index| optional_csv_value(&record, index)) {
                    Some(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
                    _ => plan.category.clone(),
                };
            let expected_kind = expected_category_kind(kind)
                .expect("transfer rows are already rejected during CSV import");
            let category_id = match category_ref.as_deref() {
                Some(reference) => {
                    Some(self.resolve_category_ref(reference, Some(&expected_kind))?)
                }
                None => {
                    return Err(AppError::Validation(format!(
                        "CSV line {line_number} is missing a category"
                    )))
                }
            };
            let category_name = match category_id {
                Some(id) => Some(self.category_name(id)?),
                None => None,
            };

            let resolved = ResolvedTransaction {
                txn_date: txn_date.clone(),
                kind,
                amount_cents,
                account_id,
                to_account_id: None,
                category_id,
                payee: payee.clone(),
                note: note.clone(),
                recurring_rule_id: None,
            };
            self.validate_resolved_transaction(&resolved)?;

            let duplicate = if plan.allow_duplicates {
                false
            } else {
                self.import_duplicate_exists(&resolved)?
            };
            if duplicate {
                duplicate_count += 1;
            } else {
                imported_count += 1;
                if !plan.dry_run {
                    self.insert_transaction(&resolved)?;
                }
            }

            preview.push(ImportedTransactionRow {
                line_number,
                txn_date,
                kind,
                amount_cents,
                account_name: account_name.clone(),
                category_name,
                payee,
                note,
                duplicate,
            });
        }

        Ok(CsvImportResult {
            dry_run: plan.dry_run,
            imported_count,
            duplicate_count,
            preview,
        })
    }

    fn import_duplicate_exists(&self, transaction: &ResolvedTransaction) -> Result<bool, AppError> {
        let payee = normalize_optional_text(&transaction.payee);
        let note = normalize_optional_text(&transaction.note);
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM transactions
             WHERE deleted_at IS NULL
               AND txn_date = ?1
               AND kind = ?2
               AND amount_cents = ?3
               AND account_id = ?4
               AND ((to_account_id IS NULL AND ?5 IS NULL) OR to_account_id = ?5)
               AND ((category_id IS NULL AND ?6 IS NULL) OR category_id = ?6)
               AND ((payee IS NULL AND ?7 IS NULL) OR payee = ?7)
               AND ((note IS NULL AND ?8 IS NULL) OR note = ?8)",
            params![
                transaction.txn_date,
                transaction.kind.as_db_str(),
                transaction.amount_cents,
                transaction.account_id,
                transaction.to_account_id,
                transaction.category_id,
                payee,
                note,
            ],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn account_name(&self, id: i64) -> Result<String, AppError> {
        self.conn
            .query_row(
                "SELECT name FROM accounts WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("account `{id}` was not found")))
    }

    fn category_name(&self, id: i64) -> Result<String, AppError> {
        self.conn
            .query_row(
                "SELECT name FROM categories WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("category `{id}` was not found")))
    }

    fn load_account(&self, id: i64) -> Result<Account, AppError> {
        self.conn
            .query_row(
                "SELECT id, name, kind, opening_balance_cents, opened_on, archived
                 FROM accounts
                 WHERE id = ?1",
                params![id],
                |row| {
                    let kind: String = row.get(2)?;
                    Ok(Account {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        kind: AccountKind::from_db(&kind).map_err(map_db_error)?,
                        opening_balance_cents: row.get(3)?,
                        opened_on: row.get(4)?,
                        archived: row.get::<_, i64>(5)? == 1,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("account `{id}` was not found")))
    }

    fn load_category(&self, id: i64) -> Result<(String, CategoryKind), AppError> {
        self.conn
            .query_row(
                "SELECT name, kind
                 FROM categories
                 WHERE id = ?1",
                params![id],
                |row| {
                    let kind: String = row.get(1)?;
                    Ok((
                        row.get::<_, String>(0)?,
                        CategoryKind::from_db(&kind).map_err(map_db_error)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("category `{id}` was not found")))
    }

    fn account_archive_blocker(&self, account_id: i64) -> Result<Option<&'static str>, AppError> {
        let transaction_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM transactions
             WHERE account_id = ?1
                OR to_account_id = ?1",
            params![account_id],
            |row| row.get(0),
        )?;
        if transaction_count > 0 {
            return Ok(Some("transactions"));
        }

        let planning_item_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM planning_items
             WHERE archived = 0
               AND (account_id = ?1 OR to_account_id = ?1)",
            params![account_id],
            |row| row.get(0),
        )?;
        if planning_item_count > 0 {
            return Ok(Some("planning items"));
        }

        let goal_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM planning_goals
             WHERE archived = 0
               AND account_id = ?1",
            params![account_id],
            |row| row.get(0),
        )?;
        if goal_count > 0 {
            return Ok(Some("goals"));
        }

        let recurring_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM recurring_rules
             WHERE account_id = ?1
                OR to_account_id = ?1",
            params![account_id],
            |row| row.get(0),
        )?;
        if recurring_count > 0 {
            return Ok(Some("recurring rules"));
        }

        let budget_count: i64 = self.conn.query_row(
            "SELECT
                (SELECT COUNT(*) FROM budgets WHERE account_id = ?1)
              + (SELECT COUNT(*) FROM scenario_budget_overrides WHERE account_id = ?1)",
            params![account_id],
            |row| row.get(0),
        )?;
        if budget_count > 0 {
            return Ok(Some("budgets"));
        }

        let reconciliation_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM reconciliations
             WHERE account_id = ?1",
            params![account_id],
            |row| row.get(0),
        )?;
        if reconciliation_count > 0 {
            return Ok(Some("reconciliations"));
        }

        Ok(None)
    }

    fn category_has_dependencies(&self, category_id: i64) -> Result<bool, AppError> {
        let transaction_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM transactions WHERE category_id = ?1",
            params![category_id],
            |row| row.get(0),
        )?;
        if transaction_count > 0 {
            return Ok(true);
        }

        let budget_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM budgets WHERE category_id = ?1",
            params![category_id],
            |row| row.get(0),
        )?;
        if budget_count > 0 {
            return Ok(true);
        }

        let recurring_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM recurring_rules WHERE category_id = ?1",
            params![category_id],
            |row| row.get(0),
        )?;
        Ok(recurring_count > 0)
    }

    fn is_initialized(&self) -> Result<bool, AppError> {
        let metadata_exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'metadata'",
            [],
            |row| row.get(0),
        )?;

        if metadata_exists == 0 {
            return Ok(false);
        }

        let metadata_rows = self
            .conn
            .query_row("SELECT COUNT(*) FROM metadata", [], |row| row.get(0))
            .unwrap_or(0_i64);

        Ok(metadata_rows > 0)
    }

    fn repair_schema_if_needed(&mut self) -> Result<(), AppError> {
        match self.conn.execute_batch(
            "ALTER TABLE budgets ADD COLUMN account_id INTEGER REFERENCES accounts(id);",
        ) {
            Ok(_) => {}
            Err(error) => {
                let message = error.to_string();
                if !message.contains("duplicate column name")
                    && !message.contains("duplicate column name: account_id")
                {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    fn migrate_if_needed(&mut self) -> Result<(), AppError> {
        let version = self.schema_version()?;
        if version >= CURRENT_SCHEMA_VERSION {
            return Ok(());
        }

        self.backup_before_migration(version, CURRENT_SCHEMA_VERSION)?;
        if version < 2 {
            self.migrate_v1_to_v2()?;
        }
        if self.schema_version()? < 3 {
            self.migrate_v2_to_v3()?;
        }
        if self.schema_version()? < 4 {
            self.migrate_v3_to_v4()?;
        }
        if self.schema_version()? < 5 {
            self.migrate_v4_to_v5()?;
        }
        if self.schema_version()? < 6 {
            self.migrate_v5_to_v6()?;
        }
        if self.schema_version()? < 7 {
            self.migrate_v6_to_v7()?;
        }
        if self.schema_version()? < 8 {
            self.migrate_v7_to_v8()?;
        }
        Ok(())
    }

    fn schema_version(&self) -> Result<i64, AppError> {
        self.conn
            .query_row(
                "SELECT schema_version FROM metadata WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn backup_before_migration(&self, from_version: i64, to_version: i64) -> Result<(), AppError> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| AppError::path_message("invalid database path", self.path.clone()))?;
        let stem = self
            .path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("tracker");
        let extension = self
            .path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("db");
        let timestamp = Local::now().format("%Y%m%dT%H%M%S");
        let backup_name =
            format!("{stem}.pre-v{from_version}-to-v{to_version}.{timestamp}.{extension}");
        fs::copy(&self.path, parent.join(backup_name))?;
        Ok(())
    }

    fn migrate_v1_to_v2(&mut self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS reconciliations (
                 id INTEGER PRIMARY KEY,
                 account_id INTEGER NOT NULL REFERENCES accounts(id),
                 statement_ending_on TEXT NOT NULL,
                 statement_balance_cents INTEGER NOT NULL,
                 cleared_balance_cents INTEGER NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE transactions_v2 (
                 id INTEGER PRIMARY KEY,
                 txn_date TEXT NOT NULL,
                 kind TEXT NOT NULL CHECK (kind IN ('income', 'expense', 'transfer')),
                 amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
                 account_id INTEGER NOT NULL REFERENCES accounts(id),
                 to_account_id INTEGER REFERENCES accounts(id),
                 category_id INTEGER REFERENCES categories(id),
                 payee TEXT,
                 note TEXT,
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 deleted_at TEXT,
                 recurring_rule_id INTEGER REFERENCES recurring_rules(id),
                 reconciliation_id INTEGER REFERENCES reconciliations(id),
                 CHECK (
                     (kind IN ('income', 'expense') AND to_account_id IS NULL AND category_id IS NOT NULL)
                     OR
                     (kind = 'transfer' AND to_account_id IS NOT NULL AND category_id IS NULL AND to_account_id != account_id)
                 )
             );
             INSERT INTO transactions_v2 (
                 id, txn_date, kind, amount_cents, account_id, to_account_id, category_id,
                 payee, note, created_at, updated_at, deleted_at, recurring_rule_id, reconciliation_id
             )
             SELECT
                 id, txn_date, kind, amount_cents, account_id, to_account_id, category_id,
                 payee, note, created_at, created_at, NULL, NULL, NULL
             FROM transactions;
             DROP TABLE transactions;
             ALTER TABLE transactions_v2 RENAME TO transactions;
             UPDATE metadata SET schema_version = 2 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn migrate_v2_to_v3(&mut self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS recurring_rules (
                 id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL COLLATE NOCASE UNIQUE,
                 kind TEXT NOT NULL CHECK (kind IN ('income', 'expense', 'transfer')),
                 amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
                 account_id INTEGER NOT NULL REFERENCES accounts(id),
                 to_account_id INTEGER REFERENCES accounts(id),
                 category_id INTEGER REFERENCES categories(id),
                 payee TEXT,
                 note TEXT,
                 cadence TEXT NOT NULL CHECK (cadence IN ('weekly', 'monthly')),
                 interval INTEGER NOT NULL DEFAULT 1 CHECK (interval > 0),
                 day_of_month INTEGER,
                 weekday TEXT,
                 start_on TEXT NOT NULL,
                 end_on TEXT,
                 next_due_on TEXT NOT NULL,
                 paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 CHECK (
                     (kind IN ('income', 'expense') AND to_account_id IS NULL AND category_id IS NOT NULL)
                     OR
                     (kind = 'transfer' AND to_account_id IS NOT NULL AND category_id IS NULL AND to_account_id != account_id)
                 ),
                 CHECK (
                     (cadence = 'weekly' AND weekday IS NOT NULL AND day_of_month IS NULL)
                     OR
                     (cadence = 'monthly' AND day_of_month BETWEEN 1 AND 28 AND weekday IS NULL)
                 )
             );
             CREATE TABLE IF NOT EXISTS recurring_occurrences (
                 id INTEGER PRIMARY KEY,
                 rule_id INTEGER NOT NULL REFERENCES recurring_rules(id) ON DELETE CASCADE,
                 due_on TEXT NOT NULL,
                 transaction_id INTEGER REFERENCES transactions(id) ON DELETE SET NULL,
                 status TEXT NOT NULL CHECK (status IN ('pending', 'posted', 'skipped')),
                 created_at TEXT NOT NULL,
                 UNIQUE(rule_id, due_on)
             );
             UPDATE metadata SET schema_version = 3 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn migrate_v3_to_v4(&mut self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS budgets (
                 id INTEGER PRIMARY KEY,
                 month TEXT NOT NULL,
                 category_id INTEGER NOT NULL REFERENCES categories(id),
                 amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 UNIQUE(month, category_id)
             );
             UPDATE metadata SET schema_version = 4 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn migrate_v4_to_v5(&mut self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "BEGIN;
             UPDATE metadata SET schema_version = 5 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn migrate_v5_to_v6(&mut self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "BEGIN;
             UPDATE metadata SET schema_version = 6 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn migrate_v6_to_v7(&mut self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "BEGIN;
             UPDATE metadata SET schema_version = 7 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn migrate_v7_to_v8(&mut self) -> Result<(), AppError> {
        let has_budget_account = {
            let mut statement = self.conn.prepare("PRAGMA table_info(budgets)")?;
            let has_column = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .any(|column| column.eq_ignore_ascii_case("account_id"));
            has_column
        };
        self.conn.execute_batch("BEGIN;")?;
        if !has_budget_account {
            self.conn.execute_batch(
                "ALTER TABLE budgets ADD COLUMN account_id INTEGER REFERENCES accounts(id);",
            )?;
        }
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS planning_scenarios (
                 id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL COLLATE NOCASE UNIQUE,
                 note TEXT,
                 archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS planning_items (
                 id INTEGER PRIMARY KEY,
                 scenario_id INTEGER REFERENCES planning_scenarios(id),
                 title TEXT NOT NULL,
                 kind TEXT NOT NULL CHECK (kind IN ('income', 'expense', 'transfer')),
                 amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
                 account_id INTEGER NOT NULL REFERENCES accounts(id),
                 to_account_id INTEGER REFERENCES accounts(id),
                 category_id INTEGER REFERENCES categories(id),
                 due_on TEXT NOT NULL,
                 payee TEXT,
                 note TEXT,
                 linked_transaction_id INTEGER REFERENCES transactions(id),
                 archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 CHECK (
                     (kind IN ('income', 'expense') AND to_account_id IS NULL)
                     OR
                     (kind = 'transfer' AND to_account_id IS NOT NULL AND to_account_id != account_id)
                 )
             );
             CREATE TABLE IF NOT EXISTS scenario_budget_overrides (
                 id INTEGER PRIMARY KEY,
                 scenario_id INTEGER NOT NULL REFERENCES planning_scenarios(id) ON DELETE CASCADE,
                 month TEXT NOT NULL,
                 category_id INTEGER NOT NULL REFERENCES categories(id),
                 amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
                 account_id INTEGER REFERENCES accounts(id),
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 UNIQUE(scenario_id, month, category_id)
             );
             CREATE TABLE IF NOT EXISTS planning_goals (
                 id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL COLLATE NOCASE UNIQUE,
                 kind TEXT NOT NULL CHECK (kind IN ('sinking_fund', 'balance_target')),
                 account_id INTEGER NOT NULL REFERENCES accounts(id),
                 target_amount_cents INTEGER,
                 minimum_balance_cents INTEGER,
                 due_on TEXT,
                 archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 CHECK (
                     (kind = 'sinking_fund' AND target_amount_cents IS NOT NULL AND due_on IS NOT NULL AND minimum_balance_cents IS NULL)
                     OR
                     (kind = 'balance_target' AND minimum_balance_cents IS NOT NULL AND target_amount_cents IS NULL AND due_on IS NULL)
                 )
             );
             UPDATE metadata SET schema_version = 8 WHERE id = 1;
             COMMIT;",
        )?;
        self.ensure_indexes()?;
        Ok(())
    }

    fn ensure_indexes(&self) -> Result<(), AppError> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(txn_date);
             CREATE INDEX IF NOT EXISTS idx_transactions_account_date ON transactions(account_id, txn_date);
             CREATE INDEX IF NOT EXISTS idx_transactions_to_account_date ON transactions(to_account_id, txn_date);
             CREATE INDEX IF NOT EXISTS idx_transactions_category_date ON transactions(category_id, txn_date);
             CREATE INDEX IF NOT EXISTS idx_transactions_reconciliation ON transactions(reconciliation_id);
             CREATE INDEX IF NOT EXISTS idx_transactions_recurring_rule ON transactions(recurring_rule_id);
             CREATE INDEX IF NOT EXISTS idx_reconciliations_account_date ON reconciliations(account_id, statement_ending_on);
             CREATE INDEX IF NOT EXISTS idx_recurring_rules_next_due ON recurring_rules(next_due_on);
             CREATE INDEX IF NOT EXISTS idx_recurring_occurrences_due_status ON recurring_occurrences(due_on, status);
             CREATE INDEX IF NOT EXISTS idx_budgets_month ON budgets(month);
             CREATE INDEX IF NOT EXISTS idx_budgets_account ON budgets(account_id);
             CREATE INDEX IF NOT EXISTS idx_planning_items_due ON planning_items(due_on);
             CREATE INDEX IF NOT EXISTS idx_planning_items_scenario_due ON planning_items(scenario_id, due_on);
             CREATE INDEX IF NOT EXISTS idx_planning_goals_account ON planning_goals(account_id);
             CREATE INDEX IF NOT EXISTS idx_planning_scenarios_name ON planning_scenarios(name);
             CREATE INDEX IF NOT EXISTS idx_scenario_budget_overrides_lookup ON scenario_budget_overrides(scenario_id, month, category_id);",
        )?;
        Ok(())
    }
    fn resolve_transaction_input(
        &self,
        transaction: &NewTransaction,
    ) -> Result<ResolvedTransaction, AppError> {
        let account_id = self.resolve_account_ref(&transaction.account)?;
        let to_account_id = match transaction.to_account.as_deref() {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };
        let category_id = match transaction.category.as_deref() {
            Some(reference) => match expected_category_kind(transaction.kind).as_ref() {
                Some(expected_kind) => {
                    Some(self.resolve_category_ref(reference, Some(expected_kind))?)
                }
                None => Some(self.resolve_category_ref(reference, None)?),
            },
            None => None,
        };

        let resolved = ResolvedTransaction {
            txn_date: transaction.txn_date.clone(),
            kind: transaction.kind,
            amount_cents: transaction.amount_cents,
            account_id,
            to_account_id,
            category_id,
            payee: normalize_optional_text(&transaction.payee),
            note: normalize_optional_text(&transaction.note),
            recurring_rule_id: transaction.recurring_rule_id,
        };
        self.validate_resolved_transaction(&resolved)?;
        Ok(resolved)
    }

    fn insert_transaction(&self, transaction: &ResolvedTransaction) -> Result<i64, AppError> {
        let timestamp = now_timestamp();
        self.conn.execute(
            "INSERT INTO transactions (
                txn_date,
                kind,
                amount_cents,
                account_id,
                to_account_id,
                category_id,
                payee,
                note,
                created_at,
                updated_at,
                deleted_at,
                recurring_rule_id,
                reconciliation_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11, NULL)",
            params![
                transaction.txn_date,
                transaction.kind.as_db_str(),
                transaction.amount_cents,
                transaction.account_id,
                transaction.to_account_id,
                transaction.category_id,
                normalize_optional_text(&transaction.payee),
                normalize_optional_text(&transaction.note),
                timestamp,
                timestamp,
                transaction.recurring_rule_id,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn load_transaction(&self, id: i64) -> Result<StoredTransaction, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                id,
                txn_date,
                kind,
                amount_cents,
                account_id,
                to_account_id,
                category_id,
                payee,
                note,
                created_at,
                updated_at,
                deleted_at,
                reconciliation_id,
                recurring_rule_id
             FROM transactions
             WHERE id = ?1",
        )?;

        statement
            .query_row(params![id], |row| {
                let kind: String = row.get(2)?;
                Ok(StoredTransaction {
                    id: row.get(0)?,
                    txn_date: row.get(1)?,
                    kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                    amount_cents: row.get(3)?,
                    account_id: row.get(4)?,
                    to_account_id: row.get(5)?,
                    category_id: row.get(6)?,
                    payee: row.get(7)?,
                    note: row.get(8)?,
                    created_at: row.get(9)?,
                    updated_at: row.get(10)?,
                    deleted_at: row.get(11)?,
                    reconciliation_id: row.get(12)?,
                    recurring_rule_id: row.get(13)?,
                })
            })
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("transaction `{id}` was not found")))
    }

    fn resolve_account_ref(&self, reference: &str) -> Result<i64, AppError> {
        let trimmed = reference.trim();
        let id_lookup = trimmed.parse::<i64>().ok();
        let id = self
            .conn
            .query_row(
                "SELECT id
                 FROM accounts
                 WHERE archived = 0
                   AND (id = ?1 OR name = ?2 COLLATE NOCASE)",
                params![id_lookup, trimmed],
                |row| row.get(0),
            )
            .optional()?;
        id.ok_or_else(|| AppError::invalid_ref("account", trimmed))
    }

    fn resolve_category_ref(
        &self,
        reference: &str,
        expected_kind: Option<&CategoryKind>,
    ) -> Result<i64, AppError> {
        let trimmed = reference.trim();
        let id_lookup = trimmed.parse::<i64>().ok();
        let row = self
            .conn
            .query_row(
                "SELECT id, kind
                 FROM categories
                 WHERE archived = 0
                   AND (id = ?1 OR name = ?2 COLLATE NOCASE)",
                params![id_lookup, trimmed],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let (category_id, actual_kind) =
            row.ok_or_else(|| AppError::invalid_ref("category", trimmed))?;
        if let Some(expected) = expected_kind {
            validate_category_kind(expected, &actual_kind)?;
        }
        Ok(category_id)
    }

    fn validate_resolved_transaction(
        &self,
        transaction: &ResolvedTransaction,
    ) -> Result<(), AppError> {
        validate_transaction_input(
            transaction.kind,
            transaction.to_account_id.is_some(),
            transaction.category_id.is_some(),
            transaction.amount_cents,
        )?;

        let account_exists: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM accounts WHERE id = ?1 AND archived = 0",
            params![transaction.account_id],
            |row| row.get(0),
        )?;
        if account_exists == 0 {
            return Err(AppError::NotFound(format!(
                "account `{}` was not found",
                transaction.account_id
            )));
        }

        if let Some(target_id) = transaction.to_account_id {
            let target_exists: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM accounts WHERE id = ?1 AND archived = 0",
                params![target_id],
                |row| row.get(0),
            )?;
            if target_exists == 0 {
                return Err(AppError::NotFound(format!(
                    "account `{target_id}` was not found"
                )));
            }
            if target_id == transaction.account_id {
                return Err(AppError::Validation(
                    "transfer destination account must be different from the source account"
                        .to_string(),
                ));
            }
        }

        if let Some(category_id) = transaction.category_id {
            let actual_kind: String = self
                .conn
                .query_row(
                    "SELECT kind FROM categories WHERE id = ?1 AND archived = 0",
                    params![category_id],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    AppError::NotFound(format!("category `{category_id}` was not found"))
                })?;
            if let Some(expected_kind) = expected_category_kind(transaction.kind) {
                validate_category_kind(&expected_kind, &actual_kind)?;
            }
        }

        Ok(())
    }

    fn summary_all_accounts(&self, from: &str, to: &str) -> Result<SummaryRecord, AppError> {
        let (transaction_count, income_cents, expense_cents, transfer_in_cents, transfer_out_cents):
            (i64, i64, i64, i64, i64) = self.conn.query_row(
            "SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN kind = 'income' THEN amount_cents ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN kind = 'expense' THEN amount_cents ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN kind = 'transfer' THEN amount_cents ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN kind = 'transfer' THEN amount_cents ELSE 0 END), 0)
             FROM transactions
             WHERE deleted_at IS NULL
               AND txn_date >= ?1
               AND txn_date <= ?2",
            params![from, to],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;

        Ok(SummaryRecord {
            from: from.to_string(),
            to: to.to_string(),
            account_id: None,
            account_name: None,
            transaction_count,
            income_cents,
            expense_cents,
            net_cents: income_cents - expense_cents + transfer_in_cents - transfer_out_cents,
            transfer_in_cents,
            transfer_out_cents,
        })
    }

    fn summary_for_account(
        &self,
        from: &str,
        to: &str,
        account_ref: &str,
    ) -> Result<SummaryRecord, AppError> {
        let account_id = self.resolve_account_ref(account_ref)?;
        let account_name = self.account_name(account_id)?;
        let (transaction_count, income_cents, expense_cents, transfer_in_cents, transfer_out_cents):
            (i64, i64, i64, i64, i64) = self.conn.query_row(
            "SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN kind = 'income' AND account_id = ?1 THEN amount_cents ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN kind = 'expense' AND account_id = ?1 THEN amount_cents ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN kind = 'transfer' AND to_account_id = ?1 THEN amount_cents ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN kind = 'transfer' AND account_id = ?1 THEN amount_cents ELSE 0 END), 0)
             FROM transactions
             WHERE deleted_at IS NULL
               AND txn_date >= ?2
               AND txn_date <= ?3
               AND (account_id = ?1 OR to_account_id = ?1)",
            params![account_id, from, to],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;

        Ok(SummaryRecord {
            from: from.to_string(),
            to: to.to_string(),
            account_id: Some(account_id),
            account_name: Some(account_name),
            transaction_count,
            income_cents,
            expense_cents,
            net_cents: income_cents - expense_cents + transfer_in_cents - transfer_out_cents,
            transfer_in_cents,
            transfer_out_cents,
        })
    }

    fn list_eligible_reconciliation_transactions_by_id(
        &self,
        account_id: i64,
        statement_ending_on: &str,
    ) -> Result<Vec<TransactionRecord>, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                t.id,
                t.txn_date,
                t.kind,
                t.amount_cents,
                source.id,
                source.name,
                target.id,
                target.name,
                category.id,
                category.name,
                t.payee,
                t.note,
                t.created_at,
                t.updated_at,
                t.deleted_at,
                t.reconciliation_id,
                t.recurring_rule_id
             FROM transactions t
             JOIN accounts source ON source.id = t.account_id
             LEFT JOIN accounts target ON target.id = t.to_account_id
             LEFT JOIN categories category ON category.id = t.category_id
             WHERE t.deleted_at IS NULL
               AND t.reconciliation_id IS NULL
               AND t.txn_date <= ?1
               AND (t.account_id = ?2 OR t.to_account_id = ?2)
             ORDER BY t.txn_date ASC, t.id ASC",
        )?;

        let rows = statement.query_map(params![statement_ending_on, account_id], |row| {
            map_transaction_row(row)
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn lookup_account_opening_balance(&self, account_id: i64) -> Result<i64, AppError> {
        self.conn
            .query_row(
                "SELECT opening_balance_cents FROM accounts WHERE id = ?1",
                params![account_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("account `{account_id}` was not found")))
    }
    fn resolve_recurring_rule_input(
        &self,
        rule: &NewRecurringRule,
    ) -> Result<ResolvedRecurringRule, AppError> {
        let account_id = self.resolve_account_ref(&rule.account)?;
        let to_account_id = match rule.to_account.as_deref() {
            Some(reference) => Some(self.resolve_account_ref(reference)?),
            None => None,
        };
        let category_id = match rule.category.as_deref() {
            Some(reference) => match expected_category_kind(rule.kind).as_ref() {
                Some(expected_kind) => {
                    Some(self.resolve_category_ref(reference, Some(expected_kind))?)
                }
                None => Some(self.resolve_category_ref(reference, None)?),
            },
            None => None,
        };

        let start_on = parse_date(&rule.start_on)?;
        let next_due_on = self.resolve_recurring_next_due_on(
            start_on,
            rule.cadence,
            rule.interval,
            rule.day_of_month,
            rule.weekday,
            rule.next_due_on.as_deref(),
            start_on,
        )?;

        let resolved = ResolvedRecurringRule {
            name: normalize_name("recurring rule", &rule.name)?,
            kind: rule.kind,
            amount_cents: rule.amount_cents,
            account_id,
            to_account_id,
            category_id,
            payee: normalize_optional_text(&rule.payee),
            note: normalize_optional_text(&rule.note),
            cadence: rule.cadence,
            interval: rule.interval,
            day_of_month: rule.day_of_month,
            weekday: rule.weekday,
            start_on: rule.start_on.clone(),
            end_on: rule.end_on.clone(),
            next_due_on,
        };
        self.validate_resolved_recurring_rule(&resolved)?;
        Ok(resolved)
    }

    fn validate_resolved_recurring_rule(
        &self,
        rule: &ResolvedRecurringRule,
    ) -> Result<(), AppError> {
        self.validate_resolved_transaction(&ResolvedTransaction {
            txn_date: rule.start_on.clone(),
            kind: rule.kind,
            amount_cents: rule.amount_cents,
            account_id: rule.account_id,
            to_account_id: rule.to_account_id,
            category_id: rule.category_id,
            payee: rule.payee.clone(),
            note: rule.note.clone(),
            recurring_rule_id: None,
        })?;

        if rule.interval <= 0 {
            return Err(AppError::Validation(
                "recurring interval must be positive".to_string(),
            ));
        }

        match rule.cadence {
            RecurringCadence::Weekly => {
                if rule.weekday.is_none() || rule.day_of_month.is_some() {
                    return Err(AppError::Validation(
                        "weekly recurring rules require --weekday and forbid --day-of-month"
                            .to_string(),
                    ));
                }
            }
            RecurringCadence::Monthly => {
                match rule.day_of_month {
                    Some(day) if (1..=28).contains(&day) && rule.weekday.is_none() => {}
                    _ => {
                        return Err(AppError::Validation(
                            "monthly recurring rules require --day-of-month between 1 and 28 and forbid --weekday"
                                .to_string(),
                        ))
                    }
                }
            }
        }

        let start_on = parse_date(&rule.start_on)?;
        let next_due_on = parse_date(&rule.next_due_on)?;
        if let Some(end_on) = rule.end_on.as_deref() {
            let end = parse_date(end_on)?;
            if end < start_on {
                return Err(AppError::Validation(
                    "recurring rule end date cannot be earlier than the start date".to_string(),
                ));
            }
            if next_due_on > end {
                return Err(AppError::Validation(
                    "next due date cannot be later than the recurring end date".to_string(),
                ));
            }
        }
        if next_due_on < start_on {
            return Err(AppError::Validation(
                "next due date cannot be earlier than the recurring start date".to_string(),
            ));
        }
        Ok(())
    }

    fn insert_recurring_rule(&self, rule: &ResolvedRecurringRule) -> Result<i64, AppError> {
        let timestamp = now_timestamp();
        match self.conn.execute(
            "INSERT INTO recurring_rules (
                name,
                kind,
                amount_cents,
                account_id,
                to_account_id,
                category_id,
                payee,
                note,
                cadence,
                interval,
                day_of_month,
                weekday,
                start_on,
                end_on,
                next_due_on,
                paused,
                created_at,
                updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, 0, ?16, ?17)",
            params![
                rule.name,
                rule.kind.as_db_str(),
                rule.amount_cents,
                rule.account_id,
                rule.to_account_id,
                rule.category_id,
                normalize_optional_text(&rule.payee),
                normalize_optional_text(&rule.note),
                rule.cadence.as_db_str(),
                rule.interval,
                rule.day_of_month.map(|value| value as i64),
                rule.weekday.map(|value| value.as_db_str().to_string()),
                rule.start_on,
                rule.end_on,
                rule.next_due_on,
                timestamp,
                timestamp,
            ],
        ) {
            Ok(_) => Ok(self.conn.last_insert_rowid()),
            Err(error) if is_unique_constraint(&error) => {
                Err(AppError::duplicate("recurring rule", &rule.name))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn load_recurring_rule(&self, id: i64) -> Result<StoredRecurringRule, AppError> {
        let mut statement = self.conn.prepare(
            "SELECT
                id,
                name,
                kind,
                amount_cents,
                account_id,
                to_account_id,
                category_id,
                payee,
                note,
                cadence,
                interval,
                day_of_month,
                weekday,
                start_on,
                end_on,
                next_due_on,
                paused,
                created_at,
                updated_at
             FROM recurring_rules
             WHERE id = ?1",
        )?;
        statement
            .query_row(params![id], |row| {
                let kind: String = row.get(2)?;
                let cadence: String = row.get(9)?;
                let weekday: Option<String> = row.get(12)?;
                Ok(StoredRecurringRule {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
                    amount_cents: row.get(3)?,
                    account_id: row.get(4)?,
                    to_account_id: row.get(5)?,
                    category_id: row.get(6)?,
                    payee: row.get(7)?,
                    note: row.get(8)?,
                    cadence: RecurringCadence::from_db(&cadence).map_err(map_db_error)?,
                    interval: row.get(10)?,
                    day_of_month: row.get::<_, Option<i64>>(11)?.map(|value| value as u32),
                    weekday: match weekday {
                        Some(value) => Some(Weekday::from_db(&value).map_err(map_db_error)?),
                        None => None,
                    },
                    start_on: row.get(13)?,
                    end_on: row.get(14)?,
                    next_due_on: row.get(15)?,
                    paused: row.get::<_, i64>(16)? == 1,
                    created_at: row.get(17)?,
                    updated_at: row.get(18)?,
                })
            })
            .optional()?
            .ok_or_else(|| AppError::NotFound(format!("recurring rule `{id}` was not found")))
    }

    fn compute_rule_next_due_after_edit(
        &self,
        rule_id: i64,
        start_on: &str,
        cadence: RecurringCadence,
        interval: i64,
        day_of_month: Option<u32>,
        weekday: Option<Weekday>,
    ) -> Result<String, AppError> {
        let start_date = parse_date(start_on)?;
        let last_posted_due: Option<String> = self.conn.query_row(
            "SELECT MAX(due_on) FROM recurring_occurrences WHERE rule_id = ?1 AND status = 'posted'",
            params![rule_id],
            |row| row.get(0),
        )?;
        let anchor = match last_posted_due {
            Some(value) => parse_date(&value)? + Duration::days(1),
            None => start_date,
        };
        compute_next_due_on_or_after(start_date, cadence, interval, day_of_month, weekday, anchor)
            .map(|date| date.format("%Y-%m-%d").to_string())
    }

    fn resolve_recurring_next_due_on(
        &self,
        start_on: NaiveDate,
        cadence: RecurringCadence,
        interval: i64,
        day_of_month: Option<u32>,
        weekday: Option<Weekday>,
        requested_next_due_on: Option<&str>,
        anchor: NaiveDate,
    ) -> Result<String, AppError> {
        let minimum_due_on = compute_next_due_on_or_after(
            start_on,
            cadence,
            interval,
            day_of_month,
            weekday,
            anchor,
        )?;
        let Some(raw_requested) = requested_next_due_on else {
            return Ok(minimum_due_on.format("%Y-%m-%d").to_string());
        };
        let requested_due_on = parse_date(raw_requested)?;
        if requested_due_on < minimum_due_on {
            return Err(AppError::Validation(format!(
                "next due date cannot be earlier than {}",
                minimum_due_on.format("%Y-%m-%d")
            )));
        }
        let aligned_due_on = compute_next_due_on_or_after(
            start_on,
            cadence,
            interval,
            day_of_month,
            weekday,
            requested_due_on,
        )?;
        if aligned_due_on != requested_due_on {
            return Err(AppError::Validation(
                "next due date must match the recurring schedule".to_string(),
            ));
        }
        Ok(requested_due_on.format("%Y-%m-%d").to_string())
    }

    fn update_recurring_pause_state(&self, id: i64, paused: bool) -> Result<(), AppError> {
        let changed = self.conn.execute(
            "UPDATE recurring_rules SET paused = ?1, updated_at = ?2 WHERE id = ?3",
            params![if paused { 1_i64 } else { 0_i64 }, now_timestamp(), id],
        )?;
        if changed == 0 {
            return Err(AppError::NotFound(format!(
                "recurring rule `{id}` was not found"
            )));
        }
        Ok(())
    }

    fn sync_due_occurrences(&self, through: &str) -> Result<(), AppError> {
        let through_date = parse_date(through)?;
        let mut statement = self
            .conn
            .prepare("SELECT id FROM recurring_rules WHERE paused = 0 ORDER BY id ASC")?;
        let ids = statement
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<Result<Vec<_>, _>>()?;

        for id in ids {
            let rule = self.load_recurring_rule(id)?;
            let mut due_on = parse_date(&rule.next_due_on)?;
            let end_on = rule.end_on.as_deref().map(parse_date).transpose()?;
            let mut changed = false;
            while due_on <= through_date {
                if let Some(limit) = end_on {
                    if due_on > limit {
                        break;
                    }
                }
                self.conn.execute(
                    "INSERT OR IGNORE INTO recurring_occurrences (rule_id, due_on, transaction_id, status, created_at)
                     VALUES (?1, ?2, NULL, 'pending', ?3)",
                    params![rule.id, due_on.format("%Y-%m-%d").to_string(), now_timestamp()],
                )?;
                due_on = advance_recurrence(
                    due_on,
                    rule.cadence,
                    rule.interval,
                    rule.day_of_month,
                    rule.weekday,
                )?;
                changed = true;
            }

            if changed {
                self.conn.execute(
                    "UPDATE recurring_rules SET next_due_on = ?1, updated_at = ?2 WHERE id = ?3",
                    params![
                        due_on.format("%Y-%m-%d").to_string(),
                        now_timestamp(),
                        rule.id
                    ],
                )?;
            }
        }

        Ok(())
    }
}
pub fn resolve_db_path(explicit: Option<PathBuf>) -> Result<PathBuf, AppError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(path) = env::var_os("HELIUS_DB_PATH") {
        return Ok(PathBuf::from(path));
    }
    default_db_path()
}

pub fn db_requires_init(path: &Path) -> Result<bool, AppError> {
    if !path.exists() {
        return Ok(true);
    }

    let db = Db::open(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    Ok(!db.is_initialized()?)
}

fn default_db_path() -> Result<PathBuf, AppError> {
    if let Some(local_app_data) = env::var_os("LOCALAPPDATA") {
        return Ok(PathBuf::from(local_app_data)
            .join("Helius")
            .join("tracker.db"));
    }

    let dirs = ProjectDirs::from("", "", "Helius").ok_or_else(|| {
        AppError::Config("unable to determine a default application data directory".to_string())
    })?;
    Ok(dirs.data_local_dir().join("tracker.db"))
}

const FULL_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    currency TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('cash', 'checking', 'savings', 'credit')),
    opening_balance_cents INTEGER NOT NULL DEFAULT 0,
    opened_on TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1))
);

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('income', 'expense')),
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1))
);

CREATE TABLE IF NOT EXISTS reconciliations (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    statement_ending_on TEXT NOT NULL,
    statement_balance_cents INTEGER NOT NULL,
    cleared_balance_cents INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recurring_rules (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('income', 'expense', 'transfer')),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    to_account_id INTEGER REFERENCES accounts(id),
    category_id INTEGER REFERENCES categories(id),
    payee TEXT,
    note TEXT,
    cadence TEXT NOT NULL CHECK (cadence IN ('weekly', 'monthly')),
    interval INTEGER NOT NULL DEFAULT 1 CHECK (interval > 0),
    day_of_month INTEGER,
    weekday TEXT,
    start_on TEXT NOT NULL,
    end_on TEXT,
    next_due_on TEXT NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (kind IN ('income', 'expense') AND to_account_id IS NULL AND category_id IS NOT NULL)
        OR
        (kind = 'transfer' AND to_account_id IS NOT NULL AND category_id IS NULL AND to_account_id != account_id)
    ),
    CHECK (
        (cadence = 'weekly' AND weekday IS NOT NULL AND day_of_month IS NULL)
        OR
        (cadence = 'monthly' AND day_of_month BETWEEN 1 AND 28 AND weekday IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY,
    txn_date TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('income', 'expense', 'transfer')),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    to_account_id INTEGER REFERENCES accounts(id),
    category_id INTEGER REFERENCES categories(id),
    payee TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT,
    recurring_rule_id INTEGER REFERENCES recurring_rules(id),
    reconciliation_id INTEGER REFERENCES reconciliations(id),
    CHECK (
        (kind IN ('income', 'expense') AND to_account_id IS NULL AND category_id IS NOT NULL)
        OR
        (kind = 'transfer' AND to_account_id IS NOT NULL AND category_id IS NULL AND to_account_id != account_id)
    )
);

CREATE TABLE IF NOT EXISTS recurring_occurrences (
    id INTEGER PRIMARY KEY,
    rule_id INTEGER NOT NULL REFERENCES recurring_rules(id) ON DELETE CASCADE,
    due_on TEXT NOT NULL,
    transaction_id INTEGER REFERENCES transactions(id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'posted', 'skipped')),
    created_at TEXT NOT NULL,
    UNIQUE(rule_id, due_on)
);

CREATE TABLE IF NOT EXISTS budgets (
    id INTEGER PRIMARY KEY,
    month TEXT NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    account_id INTEGER REFERENCES accounts(id),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(month, category_id)
);

CREATE TABLE IF NOT EXISTS planning_scenarios (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
    note TEXT,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS planning_items (
    id INTEGER PRIMARY KEY,
    scenario_id INTEGER REFERENCES planning_scenarios(id),
    title TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('income', 'expense', 'transfer')),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    to_account_id INTEGER REFERENCES accounts(id),
    category_id INTEGER REFERENCES categories(id),
    due_on TEXT NOT NULL,
    payee TEXT,
    note TEXT,
    linked_transaction_id INTEGER REFERENCES transactions(id),
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (kind IN ('income', 'expense') AND to_account_id IS NULL)
        OR
        (kind = 'transfer' AND to_account_id IS NOT NULL AND to_account_id != account_id)
    )
);

CREATE TABLE IF NOT EXISTS scenario_budget_overrides (
    id INTEGER PRIMARY KEY,
    scenario_id INTEGER NOT NULL REFERENCES planning_scenarios(id) ON DELETE CASCADE,
    month TEXT NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    account_id INTEGER REFERENCES accounts(id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(scenario_id, month, category_id)
);

CREATE TABLE IF NOT EXISTS planning_goals (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('sinking_fund', 'balance_target')),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    target_amount_cents INTEGER,
    minimum_balance_cents INTEGER,
    due_on TEXT,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (kind = 'sinking_fund' AND target_amount_cents IS NOT NULL AND due_on IS NOT NULL AND minimum_balance_cents IS NULL)
        OR
        (kind = 'balance_target' AND minimum_balance_cents IS NOT NULL AND target_amount_cents IS NULL AND due_on IS NULL)
    )
);
"#;
fn map_transaction_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TransactionRecord> {
    let kind: String = row.get(2)?;
    Ok(TransactionRecord {
        id: row.get(0)?,
        txn_date: row.get(1)?,
        kind: TransactionKind::from_db(&kind).map_err(map_db_error)?,
        amount_cents: row.get(3)?,
        account_id: row.get(4)?,
        account_name: row.get(5)?,
        to_account_id: row.get(6)?,
        to_account_name: row.get(7)?,
        category_id: row.get(8)?,
        category_name: row.get(9)?,
        payee: row.get(10)?,
        note: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
        deleted_at: row.get(14)?,
        reconciliation_id: row.get(15)?,
        recurring_rule_id: row.get(16)?,
    })
}

fn normalize_month_key(raw: &str) -> Result<String, AppError> {
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
    if parts.next().is_some() || NaiveDate::from_ymd_opt(year, month, 1).is_none() {
        return Err(AppError::Validation(
            "month must use a real calendar month in YYYY-MM format".to_string(),
        ));
    }
    Ok(format!("{year:04}-{month:02}"))
}

fn month_bounds(month: &str) -> Result<(String, String), AppError> {
    let month = normalize_month_key(month)?;
    let mut parts = month.split('-');
    let year = parts.next().unwrap().parse::<i32>().unwrap();
    let month_num = parts.next().unwrap().parse::<u32>().unwrap();
    let first = NaiveDate::from_ymd_opt(year, month_num, 1).ok_or_else(|| {
        AppError::Validation("month must use a real calendar month in YYYY-MM format".to_string())
    })?;
    let next_month = if month_num == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month_num + 1, 1)
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

fn find_csv_column(headers: &csv::StringRecord, name: &str) -> Result<usize, AppError> {
    let target = name.trim().to_ascii_lowercase();
    headers
        .iter()
        .position(|header| header.trim().to_ascii_lowercase() == target)
        .ok_or_else(|| AppError::Validation(format!("CSV column `{name}` was not found")))
}

fn find_optional_csv_column(
    headers: &csv::StringRecord,
    name: Option<&str>,
) -> Result<Option<usize>, AppError> {
    match name {
        Some(value) => find_csv_column(headers, value).map(Some),
        None => Ok(None),
    }
}

fn required_csv_value<'a>(
    record: &'a csv::StringRecord,
    index: usize,
    line_number: usize,
    column_name: &str,
) -> Result<&'a str, AppError> {
    let value = record.get(index).ok_or_else(|| {
        AppError::Validation(format!(
            "CSV line {line_number} is missing the `{column_name}` column"
        ))
    })?;
    if value.trim().is_empty() {
        return Err(AppError::Validation(format!(
            "CSV line {line_number} has an empty `{column_name}` value"
        )));
    }
    Ok(value)
}

fn optional_csv_value<'a>(record: &'a csv::StringRecord, index: usize) -> Option<&'a str> {
    record.get(index).filter(|value| !value.trim().is_empty())
}

fn parse_import_date(value: &str, date_format: &str) -> Result<String, AppError> {
    Ok(NaiveDate::parse_from_str(value.trim(), date_format)?
        .format("%Y-%m-%d")
        .to_string())
}

fn parse_import_kind(value: &str) -> Result<TransactionKind, AppError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "income" | "credit" | "deposit" | "inflow" => Ok(TransactionKind::Income),
        "expense" | "debit" | "withdrawal" | "outflow" => Ok(TransactionKind::Expense),
        "transfer" => Ok(TransactionKind::Transfer),
        other => Err(AppError::Validation(format!(
            "unsupported import transaction type `{other}`"
        ))),
    }
}
fn normalize_currency_code(currency: &str) -> Result<String, AppError> {
    let normalized = currency.trim().to_ascii_uppercase();
    let is_valid = normalized.len() == 3 && normalized.chars().all(|ch| ch.is_ascii_uppercase());
    if !is_valid {
        return Err(AppError::Validation(
            "currency must be a 3-letter uppercase code such as USD".to_string(),
        ));
    }
    Ok(normalized)
}

fn normalize_name(entity: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(format!(
            "{entity} name cannot be empty"
        )));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_text(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn resolve_optional_patch(
    existing: Option<String>,
    replacement: &Option<String>,
    clear: bool,
) -> Option<String> {
    if clear {
        None
    } else {
        normalize_optional_text(replacement).or(existing)
    }
}

fn normalize_date_value(raw: &str) -> Result<String, AppError> {
    Ok(parse_date(raw.trim())?.format("%Y-%m-%d").to_string())
}

fn build_forecast_transaction_event(
    kind: TransactionKind,
    amount_cents: i64,
    account_id: i64,
    to_account_id: Option<i64>,
    selected_account_id: Option<i64>,
) -> ForecastEvent {
    match kind {
        TransactionKind::Income => ForecastEvent {
            inflow_cents: if selected_account_id.is_none()
                || selected_account_id == Some(account_id)
            {
                amount_cents
            } else {
                0
            },
            outflow_cents: 0,
            per_account_delta: vec![(account_id, amount_cents)],
        },
        TransactionKind::Expense => ForecastEvent {
            inflow_cents: 0,
            outflow_cents: if selected_account_id.is_none()
                || selected_account_id == Some(account_id)
            {
                amount_cents
            } else {
                0
            },
            per_account_delta: vec![(account_id, -amount_cents)],
        },
        TransactionKind::Transfer => {
            let target_id =
                to_account_id.expect("transfer forecast events require a target account");
            let (inflow_cents, outflow_cents) = match selected_account_id {
                Some(selected_id) if selected_id == account_id => (0, amount_cents),
                Some(selected_id) if selected_id == target_id => (amount_cents, 0),
                Some(_) => (0, 0),
                None => (0, 0),
            };
            ForecastEvent {
                inflow_cents,
                outflow_cents,
                per_account_delta: vec![(account_id, -amount_cents), (target_id, amount_cents)],
            }
        }
    }
}

fn build_budget_forecast_event(
    account_id: i64,
    amount_cents: i64,
    selected_account_id: Option<i64>,
) -> ForecastEvent {
    ForecastEvent {
        inflow_cents: 0,
        outflow_cents: if selected_account_id.is_none() || selected_account_id == Some(account_id) {
            amount_cents
        } else {
            0
        },
        per_account_delta: vec![(account_id, -amount_cents)],
    }
}

fn initialize_forecast_months(
    start_month: NaiveDate,
) -> Result<Vec<ForecastMonthlyPoint>, AppError> {
    let mut points = Vec::with_capacity(12);
    for step in 0..12 {
        let month = add_months_with_day(start_month, step, 1)?;
        points.push(ForecastMonthlyPoint {
            month: month.format("%Y-%m").to_string(),
            inflow_cents: 0,
            outflow_cents: 0,
            net_cents: 0,
            ending_balance_cents: 0,
        });
    }
    Ok(points)
}

fn forecast_scope_balance(
    account_balances: &HashMap<i64, i64>,
    selected_account_id: Option<i64>,
) -> i64 {
    match selected_account_id {
        Some(account_id) => *account_balances.get(&account_id).unwrap_or(&0),
        None => account_balances.values().sum(),
    }
}

fn build_goal_status_records(
    goals: &[PlanningGoalRecord],
    initial_balances: &HashMap<i64, i64>,
    balance_history: &HashMap<i64, Vec<(NaiveDate, i64)>>,
    forecast_end: NaiveDate,
    goal_breach_dates: &HashMap<i64, String>,
) -> Result<Vec<GoalStatusRecord>, AppError> {
    let today = Local::now().date_naive();
    let mut rows = Vec::with_capacity(goals.len());
    for goal in goals {
        let current_balance_cents = *initial_balances.get(&goal.account_id).unwrap_or(&0);
        let history = balance_history.get(&goal.account_id);
        let projected_balance_cents = match (goal.kind, goal.due_on.as_deref()) {
            (PlanningGoalKind::SinkingFund, Some(due_on)) => {
                let due_on = parse_date(due_on)?;
                balance_on_or_before(history, due_on)
            }
            _ => balance_on_or_before(history, forecast_end),
        };
        let (remaining_cents, suggested_monthly_contribution_cents, on_track, breach_date) =
            match goal.kind {
                PlanningGoalKind::SinkingFund => {
                    let target_amount_cents = goal.target_amount_cents.unwrap_or_default();
                    let remaining_cents = (target_amount_cents - current_balance_cents).max(0);
                    let due_on = goal
                        .due_on
                        .as_deref()
                        .map(parse_date)
                        .transpose()?
                        .unwrap_or(forecast_end);
                    let months_remaining = months_until_calendar_month(today, due_on).max(1);
                    let suggested =
                        divide_round_up(remaining_cents.max(0), months_remaining as i64);
                    let on_track = projected_balance_cents >= target_amount_cents;
                    let breach_date = if on_track { None } else { goal.due_on.clone() };
                    (remaining_cents, suggested, on_track, breach_date)
                }
                PlanningGoalKind::BalanceTarget => {
                    let minimum_balance_cents = goal.minimum_balance_cents.unwrap_or_default();
                    let remaining_cents = (minimum_balance_cents - current_balance_cents).max(0);
                    let breach_date = goal_breach_dates.get(&goal.id).cloned();
                    (remaining_cents, 0, breach_date.is_none(), breach_date)
                }
            };

        rows.push(GoalStatusRecord {
            id: goal.id,
            name: goal.name.clone(),
            kind: goal.kind,
            account_id: goal.account_id,
            account_name: goal.account_name.clone(),
            target_amount_cents: goal.target_amount_cents,
            minimum_balance_cents: goal.minimum_balance_cents,
            due_on: goal.due_on.clone(),
            current_balance_cents,
            projected_balance_cents,
            remaining_cents,
            suggested_monthly_contribution_cents,
            on_track,
            breach_date,
        });
    }
    Ok(rows)
}

fn balance_on_or_before(history: Option<&Vec<(NaiveDate, i64)>>, date: NaiveDate) -> i64 {
    let Some(history) = history else {
        return 0;
    };
    history
        .iter()
        .take_while(|(entry_date, _)| *entry_date <= date)
        .last()
        .map(|(_, balance)| *balance)
        .or_else(|| history.last().map(|(_, balance)| *balance))
        .unwrap_or(0)
}

fn months_until_calendar_month(start: NaiveDate, end: NaiveDate) -> i32 {
    let start_index = start.year() * 12 + start.month0() as i32;
    let end_index = end.year() * 12 + end.month0() as i32;
    (end_index - start_index + 1).max(0)
}

fn divide_round_up(value: i64, divisor: i64) -> i64 {
    if value <= 0 {
        0
    } else {
        (value + divisor - 1) / divisor
    }
}

fn validate_transaction_input(
    kind: TransactionKind,
    has_to_account: bool,
    has_category: bool,
    amount_cents: i64,
) -> Result<(), AppError> {
    if amount_cents <= 0 {
        return Err(AppError::Validation("amount must be positive".to_string()));
    }

    match kind {
        TransactionKind::Income | TransactionKind::Expense => {
            if !has_category {
                return Err(AppError::Validation(
                    "income and expense transactions require a category".to_string(),
                ));
            }
            if has_to_account {
                return Err(AppError::Validation(
                    "income and expense transactions cannot include `--to-account`".to_string(),
                ));
            }
        }
        TransactionKind::Transfer => {
            if !has_to_account {
                return Err(AppError::Validation(
                    "transfer transactions require `--to-account`".to_string(),
                ));
            }
            if has_category {
                return Err(AppError::Validation(
                    "transfer transactions cannot include `--category`".to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn expected_category_kind(kind: TransactionKind) -> Option<CategoryKind> {
    match kind {
        TransactionKind::Income => Some(CategoryKind::Income),
        TransactionKind::Expense => Some(CategoryKind::Expense),
        TransactionKind::Transfer => None,
    }
}

fn validate_category_kind(expected: &CategoryKind, actual_db_value: &str) -> Result<(), AppError> {
    let actual = CategoryKind::from_db(actual_db_value)?;
    if &actual != expected {
        return Err(AppError::Validation(format!(
            "category kind mismatch: expected {}, found {}",
            expected.as_db_str(),
            actual.as_db_str()
        )));
    }

    Ok(())
}
fn transaction_effect_for_account(
    account_id: i64,
    kind: TransactionKind,
    amount_cents: i64,
    source_account_id: i64,
    target_account_id: Option<i64>,
) -> i64 {
    match kind {
        TransactionKind::Income if source_account_id == account_id => amount_cents,
        TransactionKind::Expense if source_account_id == account_id => -amount_cents,
        TransactionKind::Transfer if source_account_id == account_id => -amount_cents,
        TransactionKind::Transfer if target_account_id == Some(account_id) => amount_cents,
        _ => 0,
    }
}

fn compute_next_due_on_or_after(
    start_on: NaiveDate,
    cadence: RecurringCadence,
    interval: i64,
    day_of_month: Option<u32>,
    weekday: Option<Weekday>,
    anchor: NaiveDate,
) -> Result<NaiveDate, AppError> {
    let mut due = first_due_on(start_on, cadence, interval, day_of_month, weekday)?;
    while due < anchor {
        due = advance_recurrence(due, cadence, interval, day_of_month, weekday)?;
    }
    Ok(due)
}

fn first_due_on(
    start_on: NaiveDate,
    cadence: RecurringCadence,
    interval: i64,
    day_of_month: Option<u32>,
    weekday: Option<Weekday>,
) -> Result<NaiveDate, AppError> {
    let _ = interval;
    match cadence {
        RecurringCadence::Weekly => {
            let target = weekday.ok_or_else(|| {
                AppError::Validation("weekly recurring rules require a weekday".to_string())
            })?;
            let mut due = start_on;
            while weekday_from_chrono(due.weekday()) != target {
                due += Duration::days(1);
            }
            Ok(due)
        }
        RecurringCadence::Monthly => {
            let day = day_of_month.ok_or_else(|| {
                AppError::Validation("monthly recurring rules require a day of month".to_string())
            })?;
            let candidate = NaiveDate::from_ymd_opt(start_on.year(), start_on.month(), day)
                .ok_or_else(|| {
                    AppError::Validation(
                        "monthly recurring rules require a day of month between 1 and 28"
                            .to_string(),
                    )
                })?;
            if candidate >= start_on {
                Ok(candidate)
            } else {
                advance_recurrence(candidate, cadence, interval, Some(day), None)
            }
        }
    }
}

fn advance_recurrence(
    previous_due: NaiveDate,
    cadence: RecurringCadence,
    interval: i64,
    day_of_month: Option<u32>,
    _weekday: Option<Weekday>,
) -> Result<NaiveDate, AppError> {
    match cadence {
        RecurringCadence::Weekly => Ok(previous_due + Duration::days(7 * interval)),
        RecurringCadence::Monthly => {
            let day = day_of_month.ok_or_else(|| {
                AppError::Validation("monthly recurring rules require a day of month".to_string())
            })?;
            add_months_with_day(previous_due, interval as i32, day)
        }
    }
}

fn add_months_with_day(
    date: NaiveDate,
    months_to_add: i32,
    day: u32,
) -> Result<NaiveDate, AppError> {
    let month_index = date.year() * 12 + date.month0() as i32 + months_to_add;
    let year = month_index.div_euclid(12);
    let month0 = month_index.rem_euclid(12) as u32;
    NaiveDate::from_ymd_opt(year, month0 + 1, day)
        .ok_or_else(|| AppError::Validation("invalid monthly recurring schedule".to_string()))
}

fn weekday_from_chrono(value: ChronoWeekday) -> Weekday {
    match value {
        ChronoWeekday::Mon => Weekday::Mon,
        ChronoWeekday::Tue => Weekday::Tue,
        ChronoWeekday::Wed => Weekday::Wed,
        ChronoWeekday::Thu => Weekday::Thu,
        ChronoWeekday::Fri => Weekday::Fri,
        ChronoWeekday::Sat => Weekday::Sat,
        ChronoWeekday::Sun => Weekday::Sun,
    }
}

fn parse_date(value: &str) -> Result<NaiveDate, AppError> {
    Ok(NaiveDate::parse_from_str(value, "%Y-%m-%d")?)
}

fn is_unique_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(inner, _)
            if inner.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                || inner.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
    )
}

fn now_timestamp() -> String {
    Local::now().to_rfc3339()
}

fn map_db_error(error: AppError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}
