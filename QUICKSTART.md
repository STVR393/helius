# Quick Start

## 1. Build It

```powershell
cargo build --release
```

## 2. Initialize a Database

If you run `target\release\helius.exe` with no existing database, it will prompt
for a currency code and initialize the default database automatically. You can
also initialize it explicitly:

```powershell
target\release\helius.exe init --currency USD
```

## 3. Add Basic Data

```powershell
target\release\helius.exe account add Checking --type checking --opening-balance 1000.00
target\release\helius.exe category add Salary --kind income
target\release\helius.exe category add Groceries --kind expense
```

## 4. Enter Transactions

```powershell
target\release\helius.exe tx add --type income --amount 2500.00 --date 2026-03-01 --account Checking --category Salary --payee Employer
target\release\helius.exe tx add --type expense --amount 68.40 --date 2026-03-02 --account Checking --category Groceries --payee Supermarket
```

## 5. Open the TUI

```powershell
target\release\helius.exe
```

## Useful Commands

```powershell
target\release\helius.exe balance
target\release\helius.exe tx list --limit 20
target\release\helius.exe summary month
target\release\helius.exe recurring list
target\release\helius.exe forecast show
```

## TUI Hotkeys

- `Tab` / `Shift+Tab`: switch panels
- `j` / `k` or arrows: move selection
- `n`: create
- `e`: edit
- `d`: archive, delete, reset, or restore
- `?`: help
- `q`: quit

Forms:

- `Tab` / `Shift+Tab`: next or previous field
- `Enter`, `Ctrl+S`, or `F2`: save
- `Esc`: cancel

## Database Path

Default Windows location:

```text
%LOCALAPPDATA%\Helius\tracker.db
```

Override it with:

```powershell
target\release\helius.exe --db .\tracker.db balance
```
