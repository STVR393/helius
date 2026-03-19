# Helius: Local-First Personal Finance Tracker

Helius is a local-first personal finance tracker built in Rust with a full-screen terminal UI and direct CLI commands. It runs as a single executable, stores data in SQLite, and focuses on practical money tracking, budgeting, recurring bills, reconciliation, and cash-flow forecasting without any cloud dependency.

<img width="1897" height="931" alt="image" src="https://github.com/user-attachments/assets/2252b757-5244-41d5-97ce-af2d2785d08c" />

## Why Helius

- Local-first: your data stays in one SQLite file on your machine
- Fast startup: native Rust binary with no daemon and no background service
- Practical workflow: accounts, transactions, budgets, recurring bills, reconciliation, and planning in one place
- Scriptable: direct CLI commands plus JSON output for automation and export

## What It Does

- Full-screen terminal UI with a compact Bloomberg style (I love their terminal :p) theme
- Accounts, categories, income, expense, and transfer transactions
- Recurring rules, reconciliation, budgets, and cash-flow planning
- JSON output for scripting and CSV export for reporting
- Local SQLite storage with schema migrations and automatic DB creation via `init`

## Design Goals

- Fast startup
- Low memory use
- Single-user, local-only workflow
- No daemon, no background service, no async runtime

## Installation

### Option 1: Download a release binary

For most Windows users, the simplest path is:

1. Open the [GitHub Releases](https://github.com/STVR393/helius-personal-finance-tracker/releases) page.
2. Download the latest Windows package, for example `helius-v0.1.3-windows-x86_64.zip`.
3. Extract `helius.exe` into a folder you keep for apps, for example `C:\Tools\Helius\`.
4. Launch it by double-clicking `helius.exe`, or from a terminal with:

```powershell
.\helius.exe
```

On first launch, if no database exists yet, Helius prompts for a 3-letter currency
code and initializes `%LOCALAPPDATA%\Helius\tracker.db` before opening the TUI.

If you want to run it from anywhere, add that folder to your `PATH`.

Release packages are built for local, single-user use and do not require any
extra runtime or database server.

### Option 2: Build from source

Requirements:

- Rust stable toolchain
- Windows is the primary supported target today

Clone the repository, then build:

```powershell
cargo build --release
```

The compiled binary is written to:

```text
target\release\helius.exe
```

### Option 3: Install from a local checkout

If you want a `helius` command in your Cargo bin directory:

```powershell
cargo install --path .
```

## Verify The Download

After you download a release build, check that it starts and prints help:

```powershell
.\helius.exe --help
```

If you prefer to keep your data in a custom location, launch with:

```powershell
.\helius.exe --db C:\path\to\tracker.db
```

## Development

Build and test:

```powershell
cargo test
cargo build --release
```

## Run

Start the terminal UI:

```powershell
helius
```

Use the guided shell:

```powershell
helius shell
```

Run a direct command:

```powershell
helius init --currency USD
helius balance
helius tx list --limit 20
```

If you are running the binary directly without adding it to `PATH`, use:

```powershell
.\helius.exe init --currency USD
.\helius.exe balance
```

## First-Time Setup

If you launch `helius.exe` with no existing database, Helius now prompts for a
currency code and initializes the default database automatically. You can still
set it up manually:

```powershell
helius init --currency USD
helius account add Checking --type checking --opening-balance 1000.00
helius category add Salary --kind income
helius category add Groceries --kind expense
helius tx add --type income --amount 2500.00 --date 2026-03-01 --account Checking --category Salary --payee Employer
helius tx add --type expense --amount 42.50 --date 2026-03-02 --account Checking --category Groceries --payee Market
```

## TUI Controls

- `Tab` / `Shift+Tab`: switch top-level panels
- `j` / `k` or arrows: move selection
- `n`: create a new item in the active panel
- `e`: edit the selected item
- `d`: archive, delete, reset, or restore depending on panel context
- `Enter`: open, activate, or post the selected entry
- `?`: toggle help
- `q`: quit

Forms:

- `Tab` / `Shift+Tab`: move between fields
- `Enter`, `Ctrl+S`, or `F2`: save
- `Esc`: cancel

## Main Commands

Accounts and categories:

```powershell
helius account add "Cash" --type cash
helius account list
helius category add "Housing" --kind expense
helius category list
```

Transactions:

```powershell
helius tx add --type expense --amount 290.00 --date 2026-03-06 --account Cash --category Housing --payee Rent
helius tx list --limit 25
helius tx edit 12 --note "corrected note"
helius tx delete 12
helius tx restore 12
```

Budgets and summaries:

```powershell
helius budget set Groceries --month 2026-03 --amount 300.00 --account Checking
helius budget status 2026-03
helius summary month 2026-03
helius summary range --from 2026-03-01 --to 2026-03-31
```

Recurring rules:

```powershell
helius recurring add "Monthly Rent" --type expense --amount 290.00 --account Cash --category Housing --cadence monthly --day-of-month 6 --start-on 2026-03-17
helius recurring list
helius recurring run
```

Planning:

```powershell
helius forecast show
helius forecast bills
helius scenario add "Recovery Plan"
helius goal add "Cash Floor" --kind balance-target --account Checking --minimum-balance 100.00
```

## Storage

Default database path on Windows:

```text
%LOCALAPPDATA%\Helius\tracker.db
```

Overrides:

- `--db <path>`
- `HELIUS_DB_PATH`

## License

Copyright 2026 Kosta. This project is released under the GNU Affero General Public License v3.0.
See [LICENSE](LICENSE).

## Release Notes

This repository is set up for binary distribution, not for publishing a library crate. Local build artifacts, demo databases, and machine-specific launchers should stay out of version control.
