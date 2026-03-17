use std::io;
use std::path::PathBuf;

use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::Terminal;

use crate::error::AppError;
use crate::model::SummaryRecord;
use crate::today_iso;

#[path = "ui/app.rs"]
mod app;
#[path = "ui/render.rs"]
mod render;

use app::App;

pub fn run_tui(db_path: PathBuf) -> Result<(), AppError> {
    let mut stdout = io::stdout();
    enable_raw_mode()?;
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let result = {
        let mut app = App::new(db_path)?;
        app.run(&mut terminal)
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

pub(super) fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

pub(super) fn clamp_index(index: usize, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        index.min(len - 1)
    }
}

pub(super) fn shift_index(index: &mut usize, len: usize, delta: isize) {
    if len == 0 {
        *index = 0;
        return;
    }
    let next = (*index as isize + delta).clamp(0, (len - 1) as isize);
    *index = next as usize;
}

pub(super) fn empty_summary() -> SummaryRecord {
    SummaryRecord {
        from: today_iso(),
        to: today_iso(),
        account_id: None,
        account_name: None,
        transaction_count: 0,
        income_cents: 0,
        expense_cents: 0,
        net_cents: 0,
        transfer_in_cents: 0,
        transfer_out_cents: 0,
    }
}

pub(super) fn clean_output(bytes: &[u8]) -> String {
    strip_ansi_codes(&String::from_utf8_lossy(bytes))
        .replace('\r', "")
        .trim_end()
        .to_string()
}

fn strip_ansi_codes(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && matches!(chars.peek(), Some('[')) {
            chars.next();
            while let Some(next) = chars.next() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
            continue;
        }
        output.push(ch);
    }

    output
}

pub(super) fn command_template(keyword: &str) -> Option<String> {
    let today = today_iso();
    match keyword.trim() {
        "income" => Some(format!(
            "tx add --type income --amount 0.00 --date {today} --account \"ACCOUNT\" --category \"CATEGORY\""
        )),
        "expense" => Some(format!(
            "tx add --type expense --amount 0.00 --date {today} --account \"ACCOUNT\" --category \"CATEGORY\""
        )),
        "transfer" => Some(format!(
            "tx add --type transfer --amount 0.00 --date {today} --account \"FROM_ACCOUNT\" --to-account \"TO_ACCOUNT\""
        )),
        "account" => Some(format!(
            "account add \"New Account\" --type checking --opening-balance 0.00 --opened-on {today}"
        )),
        "category" => Some("category add \"New Category\" --kind expense".to_string()),
        "balance" => Some("balance".to_string()),
        "transactions" => Some("tx list --limit 10".to_string()),
        "summary" => Some("summary month".to_string()),
        "recurring" => Some(format!(
            "recurring add \"Monthly Rent\" --type expense --amount 0.00 --account \"ACCOUNT\" --category \"CATEGORY\" --cadence monthly --interval 1 --day-of-month 1 --start-on {today} --next-due-on {today}"
        )),
        "budget" => Some(format!(
            "budget set \"CATEGORY\" --month {} --amount 0.00",
            &today[..7]
        )),
        "reconcile" => Some("reconcile list --account \"ACCOUNT\"".to_string()),
        "watchlist" => Some("watchlist add AAPL --label \"Apple\"".to_string()),
        "quote" => Some("market quote AAPL".to_string()),
        "news" => Some("market news AAPL --limit 5".to_string()),
        "poly" => Some("poly market will-bitcoin-be-above-150k-on-december-31-2026".to_string()),
        "polysearch" => Some("poly search fed cut --limit 8".to_string()),
        "polymovers" => Some("poly movers --limit 8".to_string()),
        "polywatch" => Some("poly watch add will-bitcoin-be-above-150k-on-december-31-2026 --label \"BTC 150k\"".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{command_template, strip_ansi_codes};

    #[test]
    fn strips_ansi_escape_sequences_from_captured_output() {
        assert_eq!(
            strip_ansi_codes("\u{1b}[31mhello\u{1b}[0m world"),
            "hello world"
        );
    }

    #[test]
    fn provides_templates_for_shortcut_keywords() {
        let income = command_template("income").unwrap();
        assert!(income.contains("tx add --type income"));

        let account = command_template("account").unwrap();
        assert!(account.contains("account add"));

        let quote = command_template("quote").unwrap();
        assert!(quote.contains("market quote"));

        assert!(command_template("unknown").is_none());
    }
}
