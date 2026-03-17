use std::env;
use std::io::{self, IsTerminal};

use ratatui::style::{Color, Modifier, Style};

#[derive(Copy, Clone, Debug)]
pub enum Tone {
    Primary,
    Header,
    Info,
    Muted,
    Positive,
    Negative,
    Warning,
    Selected,
}

pub fn ansi_enabled() -> bool {
    env::var_os("NO_COLOR").is_none() && io::stdout().is_terminal()
}

pub fn paint(text: &str, tone: Tone) -> String {
    if !ansi_enabled() {
        return text.to_string();
    }

    let (fg, bg, bold) = ansi_spec(tone);
    match (fg, bg, bold) {
        (Some((fr, fg, fb)), Some((br, bg, bb)), true) => {
            format!("\x1b[1;38;2;{fr};{fg};{fb};48;2;{br};{bg};{bb}m{text}\x1b[0m")
        }
        (Some((fr, fg, fb)), Some((br, bg, bb)), false) => {
            format!("\x1b[38;2;{fr};{fg};{fb};48;2;{br};{bg};{bb}m{text}\x1b[0m")
        }
        (Some((r, g, b)), None, true) => format!("\x1b[1;38;2;{r};{g};{b}m{text}\x1b[0m"),
        (Some((r, g, b)), None, false) => format!("\x1b[38;2;{r};{g};{b}m{text}\x1b[0m"),
        _ => text.to_string(),
    }
}

pub fn nav_style(selected: bool) -> Style {
    if selected {
        Style::default()
            .fg(Color::Rgb(0, 0, 0))
            .bg(Color::Rgb(255, 122, 0))
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Rgb(255, 176, 0))
    }
}

pub fn tone_style(tone: Tone) -> Style {
    match tone {
        Tone::Primary => Style::default().fg(Color::Rgb(255, 176, 0)),
        Tone::Header => Style::default()
            .fg(Color::Rgb(255, 122, 0))
            .add_modifier(Modifier::BOLD),
        Tone::Info => Style::default().fg(Color::Rgb(95, 215, 255)),
        Tone::Muted => Style::default().fg(Color::Rgb(138, 138, 138)),
        Tone::Positive => Style::default().fg(Color::Rgb(0, 210, 106)),
        Tone::Negative => Style::default().fg(Color::Rgb(255, 92, 92)),
        Tone::Warning => Style::default().fg(Color::Rgb(255, 210, 77)),
        Tone::Selected => Style::default()
            .fg(Color::Rgb(0, 0, 0))
            .bg(Color::Rgb(255, 122, 0))
            .add_modifier(Modifier::BOLD),
    }
}

pub fn block_style() -> Style {
    Style::default().bg(Color::Rgb(17, 17, 17))
}

pub fn background() -> Color {
    Color::Rgb(0, 0, 0)
}

pub fn border_color(active: bool) -> Color {
    if active {
        Color::Rgb(255, 122, 0)
    } else {
        Color::Rgb(255, 176, 0)
    }
}

fn ansi_spec(tone: Tone) -> (Option<(u8, u8, u8)>, Option<(u8, u8, u8)>, bool) {
    match tone {
        Tone::Primary => (Some((255, 176, 0)), None, false),
        Tone::Header => (Some((255, 122, 0)), None, true),
        Tone::Info => (Some((95, 215, 255)), None, false),
        Tone::Muted => (Some((138, 138, 138)), None, false),
        Tone::Positive => (Some((0, 210, 106)), None, false),
        Tone::Negative => (Some((255, 92, 92)), None, false),
        Tone::Warning => (Some((255, 210, 77)), None, false),
        Tone::Selected => (Some((0, 0, 0)), Some((255, 122, 0)), true),
    }
}
