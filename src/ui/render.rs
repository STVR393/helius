use chrono::{Datelike, Duration, NaiveDate};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Block, Borders, Chart, Clear, Dataset, GraphType, List, ListItem, Paragraph, Wrap,
};
use ratatui::Frame;

use crate::amount::format_cents;
use crate::market::compose_market_brief;
use crate::model::TransactionKind;
use crate::poly::compose_poly_brief;
use crate::theme::{self, tone_style, Tone};

use super::app::{App, PlanningSubview, View};
use super::centered_rect;

impl App {
    pub(super) fn render(&self, frame: &mut Frame<'_>) {
        let area = frame.area();
        frame.render_widget(
            Block::default().style(Style::default().bg(theme::background())),
            area,
        );

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(2),
            ])
            .split(area);

        self.render_status_bar(frame, layout[0]);
        self.render_body(frame, layout[1]);
        self.render_command_bar(frame, layout[2]);

        if self.show_help {
            self.render_help(frame, centered_rect(74, 64, area));
        }
        if self.input_mode {
            self.render_input_overlay(frame, centered_rect(82, 72, area));
        }
        if self.form.is_some() {
            self.render_form_overlay(frame, centered_rect(74, 72, area));
        }
        if self.import_review.is_some() {
            self.render_import_review_overlay(frame, centered_rect(88, 80, area));
        }
        if self.poly_search.is_some() {
            self.render_poly_search_overlay(frame, centered_rect(88, 78, area));
        }
        if self.reconcile_flow.is_some() {
            self.render_reconcile_overlay(frame, centered_rect(86, 78, area));
        }
    }

    fn render_status_bar(&self, frame: &mut Frame<'_>, area: Rect) {
        let over_budget = self.budgets.iter().filter(|row| row.over_budget).count();
        let line = Line::from(vec![
            Span::styled("HELIUS", tone_style(Tone::Header)),
            Span::raw("  "),
            Span::styled(
                format!("DB {}", self.db_path.display()),
                tone_style(Tone::Muted),
            ),
            Span::raw("  "),
            Span::styled(
                format!("MONTH {}", &self.summary.from[..7]),
                tone_style(Tone::Info),
            ),
            Span::raw("  "),
            Span::styled(format!("CUR {}", self.currency), tone_style(Tone::Info)),
            Span::raw("  "),
            Span::styled(
                format!("DUE {}", self.due_occurrences.len()),
                tone_style(Tone::Warning),
            ),
            Span::raw("  "),
            Span::styled(
                format!("BUDGET ALERTS {}", over_budget),
                tone_style(if over_budget > 0 {
                    Tone::Negative
                } else {
                    Tone::Positive
                }),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "UNRECONCILED {}",
                    self.db.unreconciled_account_count().unwrap_or_default()
                ),
                tone_style(Tone::Primary),
            ),
        ]);
        let paragraph = Paragraph::new(line)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme::border_color(true)))
                    .style(theme::block_style()),
            )
            .alignment(Alignment::Left);
        frame.render_widget(paragraph, area);
    }

    fn render_body(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(22), Constraint::Min(40)])
            .split(area);

        self.render_nav(frame, layout[0]);
        self.render_view(frame, layout[1]);
    }

    fn render_nav(&self, frame: &mut Frame<'_>, area: Rect) {
        let items: Vec<ListItem<'static>> = View::all()
            .iter()
            .enumerate()
            .map(|(index, view)| {
                let style = theme::nav_style(index == self.current_view);
                ListItem::new(Line::from(Span::styled(view.label(), style)))
            })
            .collect();
        let nav = List::new(items).block(
            Block::default()
                .title(Span::styled(" PANELS ", tone_style(Tone::Header)))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(theme::border_color(true)))
                .style(theme::block_style()),
        );
        frame.render_widget(nav, area);
    }

    fn render_view(&self, frame: &mut Frame<'_>, area: Rect) {
        match self.current_view() {
            View::Dashboard => self.render_dashboard(frame, area),
            View::Transactions => self.render_transactions(frame, area),
            View::Accounts => self.render_accounts(frame, area),
            View::Categories => self.render_categories(frame, area),
            View::Summary => self.render_summary(frame, area),
            View::Budgets => self.render_budgets(frame, area),
            View::Planning => self.render_planning(frame, area),
            View::Markets => self.render_markets(frame, area),
            View::Poly => self.render_poly(frame, area),
            View::Recurring => self.render_recurring(frame, area),
            View::Reconcile => self.render_reconcile(frame, area),
        }
    }

    fn render_dashboard(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(8), Constraint::Min(10)])
            .split(area);

        let top = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(34),
                Constraint::Percentage(33),
                Constraint::Percentage(33),
            ])
            .split(layout[0]);

        frame.render_widget(
            self.metric_block(
                "MONTH INCOME",
                format_cents(self.summary.income_cents),
                Tone::Positive,
            ),
            top[0],
        );
        frame.render_widget(
            self.metric_block(
                "MONTH EXPENSE",
                format_cents(-self.summary.expense_cents),
                Tone::Negative,
            ),
            top[1],
        );
        frame.render_widget(
            self.metric_block(
                "MONTH NET",
                format_cents(self.summary.net_cents),
                if self.summary.net_cents >= 0 {
                    Tone::Positive
                } else {
                    Tone::Negative
                },
            ),
            top[2],
        );

        let bottom = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(42),
                Constraint::Percentage(31),
                Constraint::Percentage(27),
            ])
            .split(layout[1]);

        frame.render_widget(
            Paragraph::new(self.cash_flow_chart_lines())
                .block(self.panel_block("6M CASH FLOW", true))
                .wrap(Wrap { trim: false }),
            bottom[0],
        );
        frame.render_widget(
            List::new(self.recent_transaction_items())
                .block(self.panel_block("RECENT TRANSACTIONS", true)),
            bottom[1],
        );
        frame.render_widget(
            Paragraph::new(self.budget_snapshot_lines())
                .block(self.panel_block("BUDGET SNAPSHOT", true))
                .wrap(Wrap { trim: false }),
            bottom[2],
        );
    }

    fn render_transactions(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(5), Constraint::Min(8)])
            .split(area);

        frame.render_widget(
            Paragraph::new(self.transaction_filter_lines())
                .block(self.panel_block("FILTERS", true))
                .wrap(Wrap { trim: false }),
            layout[0],
        );

        let items: Vec<ListItem<'static>> = if self.transactions.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                if self.transaction_filters_active() {
                    "No transactions matched the current filters."
                } else {
                    "No transactions yet. Press N to add one."
                },
                tone_style(Tone::Muted),
            )))]
        } else {
            self.transactions
                .iter()
                .enumerate()
                .map(|(index, transaction)| {
                    let style = if index == self.tx_index {
                        tone_style(Tone::Selected)
                    } else if transaction.deleted_at.is_some() {
                        tone_style(Tone::Muted)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    let label = transaction
                        .payee
                        .as_deref()
                        .or(transaction.category_name.as_deref())
                        .or(transaction.note.as_deref())
                        .unwrap_or("-");
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "#{: <4} {} {: <9} {: >10} {} {}",
                            transaction.id,
                            transaction.txn_date,
                            transaction.kind.as_db_str(),
                            format_cents(transaction.amount_cents),
                            truncate_label(&transaction.account_name, 10),
                            truncate_label(label, 14)
                        ),
                        style,
                    )))
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(self.panel_block("TRANSACTIONS", true)),
            layout[1],
        );
    }

    fn render_accounts(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(52), Constraint::Percentage(48)])
            .split(area);
        let bottom = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
            .split(layout[1]);

        let items: Vec<ListItem<'static>> = if self.accounts.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No accounts yet. Press N to add one.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.accounts
                .iter()
                .enumerate()
                .map(|(index, account)| {
                    let style = if index == self.account_index {
                        tone_style(Tone::Selected)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    let balance = self
                        .balances
                        .iter()
                        .find(|balance| balance.account_id == account.id)
                        .map(|balance| format_cents(balance.current_balance_cents))
                        .unwrap_or_else(|| "0.00".to_string());
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{}  {: <9}  {}",
                            truncate_label(&account.name, 16),
                            account.kind.as_db_str(),
                            balance
                        ),
                        style,
                    )))
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(self.panel_block("ACCOUNTS", true)),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(self.selected_account_lines())
                .block(self.panel_block("ACCOUNT DETAIL", true))
                .wrap(Wrap { trim: false }),
            bottom[0],
        );
        frame.render_widget(
            Paragraph::new(self.balance_trend_lines())
                .block(self.panel_block("TOTAL BALANCE TREND", true))
                .wrap(Wrap { trim: false }),
            bottom[1],
        );
    }
    fn render_categories(&self, frame: &mut Frame<'_>, area: Rect) {
        let items: Vec<ListItem<'static>> = if self.categories.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No categories yet. Press N to add one.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.categories
                .iter()
                .enumerate()
                .map(|(index, category)| {
                    let style = if index == self.category_index {
                        tone_style(Tone::Selected)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "#{: <4} {: <12} {}",
                            category.id,
                            category.kind.as_db_str(),
                            truncate_label(&category.name, 28)
                        ),
                        style,
                    )))
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(self.panel_block("CATEGORIES", true)),
            area,
        );
    }

    fn render_summary(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
            .split(area);

        frame.render_widget(
            Paragraph::new(self.summary_detail_lines())
                .block(self.panel_block("CURRENT MONTH", true))
                .wrap(Wrap { trim: false }),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(self.category_spending_lines())
                .block(self.panel_block("TOP SPENDING CATEGORIES", true))
                .wrap(Wrap { trim: false }),
            layout[1],
        );
    }

    fn render_budgets(&self, frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(
            Paragraph::new(self.budget_detail_lines())
                .block(self.panel_block("BUDGET VS ACTUAL", true))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_planning(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(7),
                Constraint::Min(12),
                Constraint::Length(3),
            ])
            .split(area);
        let top = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
            ])
            .split(layout[0]);

        let projected_30_day_net = self
            .planning_forecast
            .daily
            .iter()
            .take(30)
            .map(|point| point.net_cents)
            .sum::<i64>();
        let lowest_balance = self
            .planning_forecast
            .daily
            .iter()
            .map(|point| point.closing_balance_cents)
            .min()
            .unwrap_or(0);
        let underfunded_goals = self
            .planning_forecast
            .goal_status
            .iter()
            .filter(|goal| !goal.on_track)
            .count();

        frame.render_widget(
            self.metric_block(
                "30D NET",
                format_cents(projected_30_day_net),
                if projected_30_day_net >= 0 {
                    Tone::Positive
                } else {
                    Tone::Negative
                },
            ),
            top[0],
        );
        frame.render_widget(
            self.metric_block(
                "LOWEST 90D",
                format_cents(lowest_balance),
                if lowest_balance >= 0 {
                    Tone::Positive
                } else {
                    Tone::Negative
                },
            ),
            top[1],
        );
        frame.render_widget(
            self.metric_block(
                "DUE BILLS",
                self.planning_forecast.bill_calendar.len().to_string(),
                Tone::Warning,
            ),
            top[2],
        );
        frame.render_widget(
            self.metric_block(
                "WARNINGS",
                self.planning_forecast.warnings.len().to_string(),
                if self.planning_forecast.warnings.is_empty() {
                    Tone::Positive
                } else {
                    Tone::Warning
                },
            ),
            top[3],
        );
        frame.render_widget(
            self.metric_block(
                "GOAL GAPS",
                underfunded_goals.to_string(),
                if underfunded_goals == 0 {
                    Tone::Positive
                } else {
                    Tone::Warning
                },
            ),
            top[4],
        );

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
            .split(layout[1]);
        match self.current_planning_subview() {
            PlanningSubview::Forecast => {
                let forecast_body = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(14), Constraint::Min(10)])
                    .split(body[1]);
                let chart_data = planning_chart_data(
                    &self.planning_actual_weekly,
                    &self.planning_baseline,
                    &self.planning_forecast,
                    self.selected_planning_scenario_id.is_some(),
                );
                frame.render_widget(
                    List::new(self.planning_forecast_items())
                        .block(self.panel_block("90D DAILY", true)),
                    body[0],
                );
                if let Some(chart_data) = chart_data {
                    let mut datasets = vec![
                        Dataset::default()
                            .name("ACTUAL")
                            .marker(symbols::Marker::Dot)
                            .graph_type(GraphType::Line)
                            .style(tone_style(Tone::Positive))
                            .data(&chart_data.actual),
                        Dataset::default()
                            .name(if self.selected_planning_scenario_id.is_some() {
                                "BASELINE"
                            } else {
                                "FORECAST"
                            })
                            .marker(symbols::Marker::Braille)
                            .graph_type(GraphType::Line)
                            .style(tone_style(Tone::Info))
                            .data(&chart_data.baseline),
                    ];
                    if !chart_data.scenario.is_empty() {
                        datasets.push(
                            Dataset::default()
                                .name(
                                    self.planning_forecast
                                        .scenario
                                        .name
                                        .clone()
                                        .unwrap_or_else(|| "SCENARIO".to_string()),
                                )
                                .marker(symbols::Marker::Braille)
                                .graph_type(GraphType::Line)
                                .style(tone_style(Tone::Primary))
                                .data(&chart_data.scenario),
                        );
                    }
                    let chart = Chart::new(datasets)
                        .block(self.panel_block("WEEKLY OPENING BALANCES", true))
                        .x_axis(
                            Axis::default()
                                .style(tone_style(Tone::Muted))
                                .labels(chart_data.x_labels)
                                .bounds(chart_data.x_bounds),
                        )
                        .y_axis(
                            Axis::default()
                                .style(tone_style(Tone::Muted))
                                .labels(chart_data.y_labels)
                                .bounds(chart_data.y_bounds),
                        )
                        .hidden_legend_constraints((
                            Constraint::Percentage(100),
                            Constraint::Length(4),
                        ));
                    frame.render_widget(chart, forecast_body[0]);
                } else {
                    frame.render_widget(
                        Paragraph::new(vec![Line::from(Span::styled(
                            "No forecast chart data yet.",
                            tone_style(Tone::Muted),
                        ))])
                        .block(self.panel_block("WEEKLY OPENING BALANCES", true))
                        .alignment(Alignment::Center)
                        .wrap(Wrap { trim: false }),
                        forecast_body[0],
                    );
                }
                frame.render_widget(
                    Paragraph::new(self.planning_forecast_lines())
                        .block(self.panel_block("FORECAST DETAIL", true))
                        .wrap(Wrap { trim: false }),
                    forecast_body[1],
                );
            }
            PlanningSubview::Calendar => {
                frame.render_widget(
                    List::new(self.planning_item_list_items())
                        .block(self.panel_block("PLANNED ITEMS", true)),
                    body[0],
                );
                frame.render_widget(
                    Paragraph::new(self.planning_calendar_lines())
                        .block(self.panel_block("BILL CALENDAR", true))
                        .wrap(Wrap { trim: false }),
                    body[1],
                );
            }
            PlanningSubview::Goals => {
                frame.render_widget(
                    List::new(self.planning_goal_items()).block(self.panel_block("GOALS", true)),
                    body[0],
                );
                frame.render_widget(
                    Paragraph::new(self.planning_goal_lines())
                        .block(self.panel_block("GOAL DETAIL", true))
                        .wrap(Wrap { trim: false }),
                    body[1],
                );
            }
            PlanningSubview::Scenarios => {
                frame.render_widget(
                    List::new(self.planning_scenario_items())
                        .block(self.panel_block("SCENARIOS", true)),
                    body[0],
                );
                frame.render_widget(
                    Paragraph::new(self.planning_scenario_lines())
                        .block(self.panel_block("SCENARIO DETAIL", true))
                        .wrap(Wrap { trim: false }),
                    body[1],
                );
            }
        }

        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                self.planning_status_text(),
                tone_style(Tone::Primary),
            )))
            .block(self.panel_block("STATUS", true))
            .wrap(Wrap { trim: false }),
            layout[2],
        );
    }

    fn render_markets(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
            .split(area);
        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(9), Constraint::Min(8)])
            .split(layout[1]);

        let items: Vec<ListItem<'static>> = if self.watchlist.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "Watchlist is empty. Press N to add a ticker.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.watchlist
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    let style = if index == self.market_index {
                        tone_style(Tone::Selected)
                    } else if entry
                        .quote
                        .as_ref()
                        .map(|quote| quote.stale)
                        .unwrap_or(true)
                    {
                        tone_style(Tone::Warning)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    let descriptor = entry
                        .label
                        .as_deref()
                        .or_else(|| entry.quote.as_ref().map(|quote| quote.name.as_str()))
                        .unwrap_or("-");
                    let price = entry
                        .quote
                        .as_ref()
                        .map(|quote| format!("{:.2}", quote.last_price))
                        .unwrap_or_else(|| "--".to_string());
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{: <7} {: <16} {}",
                            entry.ticker,
                            truncate_label(descriptor, 16),
                            price
                        ),
                        style,
                    )))
                })
                .collect()
        };

        frame.render_widget(
            List::new(items).block(self.panel_block("WATCHLIST", true)),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(self.market_quote_lines())
                .block(self.panel_block("QUOTE SNAPSHOT", true))
                .wrap(Wrap { trim: false }),
            right[0],
        );
        frame.render_widget(
            Paragraph::new(self.market_brief_lines())
                .block(self.panel_block("BRIEF", true))
                .wrap(Wrap { trim: false }),
            right[1],
        );
    }

    fn render_poly(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
            .split(area);
        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(9),
                Constraint::Min(8),
                Constraint::Length(9),
            ])
            .split(layout[1]);

        let items: Vec<ListItem<'static>> = if self.poly_watchlist.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "Polymarket watchlist is empty. Press N to add a market slug.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.poly_watchlist
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    let style = if index == self.poly_index {
                        tone_style(Tone::Selected)
                    } else if entry
                        .market
                        .as_ref()
                        .map(|market| market.stale)
                        .unwrap_or(true)
                    {
                        tone_style(Tone::Warning)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    let descriptor = entry.label.as_deref().unwrap_or(&entry.slug);
                    let yes = entry
                        .market
                        .as_ref()
                        .and_then(|market| market.yes_price)
                        .map(|value| format!("{:>3.0}%", value * 100.0))
                        .unwrap_or_else(|| " --".to_string());
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{: <16} {: <14} {}",
                            truncate_label(&entry.slug, 16),
                            truncate_label(descriptor, 14),
                            yes
                        ),
                        style,
                    )))
                })
                .collect()
        };

        frame.render_widget(
            List::new(items).block(self.panel_block("POLY WATCH", true)),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(self.poly_market_lines())
                .block(self.panel_block("MARKET SNAPSHOT", true))
                .wrap(Wrap { trim: false }),
            right[0],
        );
        frame.render_widget(
            Paragraph::new(self.poly_brief_lines())
                .block(self.panel_block("POLY BRIEF", true))
                .wrap(Wrap { trim: false }),
            right[1],
        );
        frame.render_widget(
            List::new(self.poly_mover_items()).block(self.panel_block("TOP MOVERS", true)),
            right[2],
        );
    }
    fn render_recurring(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
            .split(area);

        let rule_items: Vec<ListItem<'static>> = if self.recurring_rules.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No recurring rules yet. Press N to add one.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.recurring_rules
                .iter()
                .enumerate()
                .map(|(index, rule)| {
                    let style = if index == self.recurring_index {
                        tone_style(Tone::Selected)
                    } else if rule.paused {
                        tone_style(Tone::Muted)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{} {: <8} {} next {}",
                            truncate_label(&rule.name, 18),
                            rule.kind.as_db_str(),
                            format_cents(rule.amount_cents),
                            rule.next_due_on
                        ),
                        style,
                    )))
                })
                .collect()
        };
        frame.render_widget(
            List::new(rule_items).block(self.panel_block("RECURRING RULES", true)),
            layout[0],
        );

        let due_items: Vec<ListItem<'static>> = if self.due_occurrences.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No due recurring items right now. Press N to add a rule.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.due_occurrences
                .iter()
                .map(|occurrence| {
                    let tone = match occurrence.status {
                        crate::model::OccurrenceStatus::Pending => Tone::Warning,
                        crate::model::OccurrenceStatus::Posted => Tone::Positive,
                        crate::model::OccurrenceStatus::Skipped => Tone::Muted,
                    };
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{} {} {} {}",
                            occurrence.due_on,
                            truncate_label(&occurrence.rule_name, 18),
                            occurrence.kind.as_db_str(),
                            format_cents(occurrence.amount_cents)
                        ),
                        tone_style(tone),
                    )))
                })
                .collect()
        };
        frame.render_widget(
            List::new(due_items).block(self.panel_block("DUE QUEUE", true)),
            layout[1],
        );
    }

    fn render_reconcile(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(area);

        let items: Vec<ListItem<'static>> = if self.reconciliations.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No reconciliations yet. Press R on ACCOUNTS or N here to start one.",
                tone_style(Tone::Muted),
            )))]
        } else {
            self.reconciliations
                .iter()
                .enumerate()
                .map(|(index, reconciliation)| {
                    let style = if index == self.reconciliation_index {
                        tone_style(Tone::Selected)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "#{} {} {} {}",
                            reconciliation.id,
                            truncate_label(&reconciliation.account_name, 16),
                            reconciliation.statement_ending_on,
                            format_cents(reconciliation.statement_balance_cents)
                        ),
                        style,
                    )))
                })
                .collect()
        };
        frame.render_widget(
            List::new(items).block(self.panel_block("RECONCILIATIONS", true)),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(self.selected_reconciliation_lines())
                .block(self.panel_block("DETAIL", true))
                .wrap(Wrap { trim: false }),
            layout[1],
        );
    }

    fn render_command_bar(&self, frame: &mut Frame<'_>, area: Rect) {
        let paragraph = Paragraph::new(Line::from(Span::styled(
            self.command_bar_text(),
            tone_style(Tone::Primary),
        )))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(theme::border_color(false)))
                .style(theme::block_style()),
        );
        frame.render_widget(paragraph, area);
    }

    fn render_help(&self, frame: &mut Frame<'_>, area: Rect) {
        let lines = vec![
            Line::from(Span::styled("HELIUS QUICK HELP", tone_style(Tone::Header))),
            Line::from(""),
            Line::from("Navigation"),
            Line::from("  Tab / Shift+Tab : switch panels"),
            Line::from("  Up/Down or J/K   : move selection"),
            Line::from("  Esc              : close help, write mode, or form"),
            Line::from("  Q                : quit"),
            Line::from(""),
            Line::from("Work directly in the app"),
            Line::from("  N adds from the current panel."),
            Line::from("  E edits the selected account, transaction, category, recurring rule, or budget."),
            Line::from("  I opens CSV import from TRANSACTIONS or ACCOUNTS."),
            Line::from("  F or / opens transaction filters from TRANSACTIONS."),
            Line::from("  C clears transaction filters from TRANSACTIONS."),
            Line::from("  R starts reconciliation from ACCOUNTS or RECONCILE."),
            Line::from("  S still opens raw command mode if you want full CLI control."),
            Line::from(""),
            Line::from("Charts"),
            Line::from("  DASHBOARD : 6-month cash flow bars"),
            Line::from("  SUMMARY   : top category spending bars"),
            Line::from("  BUDGETS   : budget vs actual progress bars"),
            Line::from("  ACCOUNTS  : total balance trend bars"),
            Line::from(
                "  PLANNING  : weekly actual vs projected balances, bills, goals, scenarios",
            ),
            Line::from(""),
            Line::from("Quick actions"),
            Line::from("  G posts due recurring items"),
            Line::from("  P pauses or resumes the selected recurring rule"),
            Line::from("  D deletes/restores the selected row where supported"),
            Line::from("  In ACCOUNTS: E edits the selected account | D archives it if unused | R reconciles it"),
            Line::from("  In CATEGORIES: E edits the selected category | D archives it"),
            Line::from("  In PLANNING: Left/Right subview | N add | E edit | D archive | Enter select/post"),
            Line::from(
                "  In reconciliation review: Space toggles, A selects all, C clears, Ctrl+S or F2 saves",
            ),
            Line::from("  ? or H toggles this help"),
        ];
        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(lines)
                .block(self.panel_block("HELP", true))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_input_overlay(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(8),
                Constraint::Length(5),
            ])
            .split(area);

        let log_lines: Vec<Line<'static>> = if self.command_log.is_empty() {
            vec![Line::from(Span::styled(
                "Write mode is empty. Type `help` or a command below.",
                tone_style(Tone::Muted),
            ))]
        } else {
            self.command_log
                .iter()
                .map(|entry| {
                    let tone = if entry.starts_with('>') {
                        Tone::Info
                    } else if entry.starts_with("ERROR") {
                        Tone::Negative
                    } else {
                        Tone::Primary
                    };
                    Line::from(Span::styled(entry.clone(), tone_style(tone)))
                })
                .collect()
        };

        let input_value = if self.input_buffer.is_empty() {
            String::from("Type a command here")
        } else {
            self.input_buffer.clone()
        };
        let input_tone = if self.input_buffer.is_empty() {
            Tone::Muted
        } else {
            Tone::Selected
        };
        let prompt_lines = vec![
            Line::from(Span::styled(
                "Type a shortcut word or a full command, then press Enter.",
                tone_style(Tone::Muted),
            )),
            Line::from(Span::styled("CURRENT INPUT", tone_style(Tone::Info))),
            Line::from(vec![
                Span::styled("> ", tone_style(Tone::Warning)),
                Span::styled(format!(" {input_value} | "), tone_style(input_tone)),
            ]),
        ];

        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "WRITE MODE",
                tone_style(Tone::Header),
            )))
            .block(self.panel_block("WRITE MODE", true)),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(log_lines)
                .block(self.panel_block("LOG", true))
                .wrap(Wrap { trim: false }),
            layout[1],
        );
        frame.render_widget(
            Paragraph::new(prompt_lines)
                .block(self.panel_block("INPUT", true))
                .wrap(Wrap { trim: false }),
            layout[2],
        );
    }
    fn render_import_review_overlay(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(review) = self.import_review.as_ref() else {
            return;
        };

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(8),
                Constraint::Length(3),
            ])
            .split(area);

        let summary_lines = vec![
            Line::from(Span::styled("CSV IMPORT PREVIEW", tone_style(Tone::Header))),
            Line::from(Span::styled(
                format!(
                    "FILE {} | ACCOUNT {}",
                    review.plan.path.display(),
                    review.plan.account
                ),
                tone_style(Tone::Primary),
            )),
            Line::from(vec![
                Span::styled("READY ", tone_style(Tone::Muted)),
                Span::styled(
                    review.preview.imported_count.to_string(),
                    tone_style(Tone::Positive),
                ),
                Span::raw("  "),
                Span::styled("DUPLICATES ", tone_style(Tone::Muted)),
                Span::styled(
                    review.preview.duplicate_count.to_string(),
                    tone_style(if review.preview.duplicate_count > 0 {
                        Tone::Warning
                    } else {
                        Tone::Positive
                    }),
                ),
                Span::raw("  "),
                Span::styled("ALLOW DUPES ", tone_style(Tone::Muted)),
                Span::styled(
                    if review.plan.allow_duplicates {
                        "yes"
                    } else {
                        "no"
                    },
                    tone_style(Tone::Info),
                ),
            ]),
            Line::from(Span::styled(
                format!(
                    "DATE {} | AMOUNT {} | DESC {} | CATEGORY {}",
                    review.plan.date_column,
                    review.plan.amount_column,
                    review.plan.description_column,
                    review
                        .plan
                        .category_column
                        .clone()
                        .or_else(|| review.plan.category.clone())
                        .unwrap_or_else(|| "-".to_string())
                ),
                tone_style(Tone::Muted),
            )),
        ];

        let items: Vec<ListItem<'static>> = if review.preview.preview.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No rows were found in the CSV preview.",
                tone_style(Tone::Muted),
            )))]
        } else {
            review
                .preview
                .preview
                .iter()
                .enumerate()
                .map(|(index, row)| {
                    let style = if index == review.active {
                        tone_style(Tone::Selected)
                    } else if row.duplicate {
                        tone_style(Tone::Warning)
                    } else {
                        match row.kind {
                            TransactionKind::Income => tone_style(Tone::Positive),
                            TransactionKind::Expense => tone_style(Tone::Negative),
                            TransactionKind::Transfer => tone_style(Tone::Info),
                        }
                    };
                    let status = if row.duplicate { "dup" } else { "new" };
                    let detail = row
                        .payee
                        .as_deref()
                        .or(row.category_name.as_deref())
                        .unwrap_or("-");
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{: <3} {} {: <8} {: >10} {: <4} {}",
                            row.line_number,
                            row.txn_date,
                            row.kind.as_db_str(),
                            format_cents(match row.kind {
                                TransactionKind::Expense => -row.amount_cents,
                                _ => row.amount_cents,
                            }),
                            status,
                            truncate_label(detail, 22)
                        ),
                        style,
                    )))
                })
                .collect()
        };

        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(summary_lines)
                .block(self.panel_block("IMPORT PREVIEW", true))
                .wrap(Wrap { trim: false }),
            layout[0],
        );
        frame.render_widget(
            List::new(items).block(self.panel_block("ROWS", true)),
            layout[1],
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Up/Down: browse preview | Ctrl+S/F2: confirm import | Esc: cancel",
                    tone_style(Tone::Primary),
                )),
                Line::from(Span::styled(
                    "This preview was a dry run. Confirming will write only the non-duplicate rows unless ALLOW DUPES is yes.",
                    tone_style(Tone::Muted),
                )),
            ])
            .block(self.panel_block("ACTIONS", true))
            .wrap(Wrap { trim: false }),
            layout[2],
        );
    }
    fn render_poly_search_overlay(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(search) = self.poly_search.as_ref() else {
            return;
        };

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(8),
                Constraint::Length(3),
            ])
            .split(area);

        let summary_lines = vec![
            Line::from(Span::styled("POLY SEARCH", tone_style(Tone::Header))),
            Line::from(Span::styled(
                format!("QUERY {} | LIMIT {}", search.query, search.limit),
                tone_style(Tone::Primary),
            )),
            Line::from(Span::styled(
                format!("RESULTS {}", search.results.len()),
                tone_style(Tone::Muted),
            )),
        ];

        let items: Vec<ListItem<'static>> = if search.results.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No Polymarket results found.",
                tone_style(Tone::Muted),
            )))]
        } else {
            search
                .results
                .iter()
                .enumerate()
                .map(|(index, result)| {
                    let style = if index == search.active {
                        tone_style(Tone::Selected)
                    } else if result.closed {
                        tone_style(Tone::Muted)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    let yes = result
                        .yes_price
                        .map(|value| format!("{:>3.0}%", value * 100.0))
                        .unwrap_or_else(|| " --".to_string());
                    ListItem::new(Line::from(Span::styled(
                        format!(
                            "{: <22} {: <38} {}",
                            truncate_label(&result.slug, 22),
                            truncate_label(&result.question, 38),
                            yes
                        ),
                        style,
                    )))
                })
                .collect()
        };

        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(summary_lines)
                .block(self.panel_block("SEARCH", true))
                .wrap(Wrap { trim: false }),
            layout[0],
        );
        frame.render_widget(
            List::new(items).block(self.panel_block("RESULTS", true)),
            layout[1],
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Up/Down: select | Enter/A: add to watchlist | Ctrl+S: re-run | Esc: close",
                    tone_style(Tone::Primary),
                )),
                Line::from(Span::styled(
                    "Use N or / in POLY to open search, then Ctrl+S to fetch results.",
                    tone_style(Tone::Muted),
                )),
            ])
            .block(self.panel_block("ACTIONS", true))
            .wrap(Wrap { trim: false }),
            layout[2],
        );
    }
    fn render_reconcile_overlay(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(flow) = self.reconcile_flow.as_ref() else {
            return;
        };

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(area);

        let selected_balance = flow.selected_balance_cents();
        let difference = flow.difference_cents();
        let difference_tone = if difference == 0 {
            Tone::Positive
        } else {
            Tone::Warning
        };

        let summary_lines = vec![
            Line::from(Span::styled(
                "RECONCILE SELECTION",
                tone_style(Tone::Header),
            )),
            Line::from(Span::styled(
                format!(
                    "ACCOUNT {} | THROUGH {} | OPENING {}",
                    flow.account_name,
                    flow.statement_ending_on,
                    format_cents(flow.opening_balance_cents)
                ),
                tone_style(Tone::Primary),
            )),
            Line::from(vec![
                Span::styled("STATEMENT ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(flow.statement_balance_cents),
                    tone_style(Tone::Info),
                ),
                Span::raw("  "),
                Span::styled("SELECTED ", tone_style(Tone::Muted)),
                Span::styled(format_cents(selected_balance), tone_style(Tone::Info)),
                Span::raw("  "),
                Span::styled("DIFF ", tone_style(Tone::Muted)),
                Span::styled(format_cents(difference), tone_style(difference_tone)),
            ]),
            Line::from(Span::styled(
                format!(
                    "{} transactions selected out of {} eligible.",
                    flow.selected_count(),
                    flow.eligible_transactions.len()
                ),
                tone_style(Tone::Muted),
            )),
        ];

        let items: Vec<ListItem<'static>> = flow
            .eligible_transactions
            .iter()
            .enumerate()
            .map(|(index, transaction)| {
                let selected = flow.selected_ids.contains(&transaction.id);
                let marker = if selected { "[*]" } else { "[ ]" };
                let style = if index == flow.active {
                    tone_style(Tone::Selected)
                } else if selected {
                    tone_style(Tone::Positive)
                } else {
                    tone_style(Tone::Primary)
                };
                let delta = match transaction.kind {
                    TransactionKind::Income if transaction.account_id == flow.account_id => {
                        transaction.amount_cents
                    }
                    TransactionKind::Expense if transaction.account_id == flow.account_id => {
                        -transaction.amount_cents
                    }
                    TransactionKind::Transfer if transaction.account_id == flow.account_id => {
                        -transaction.amount_cents
                    }
                    TransactionKind::Transfer
                        if transaction.to_account_id == Some(flow.account_id) =>
                    {
                        transaction.amount_cents
                    }
                    _ => 0,
                };
                let effect = match delta {
                    value if value > 0 => format!("+{}", format_cents(value)),
                    value if value < 0 => format!("-{}", format_cents(value.abs())),
                    _ => "0.00".to_string(),
                };
                let label = transaction
                    .payee
                    .as_deref()
                    .or(transaction.category_name.as_deref())
                    .or(transaction.to_account_name.as_deref())
                    .unwrap_or("-");
                ListItem::new(Line::from(Span::styled(
                    format!(
                        "{} #{} {} {: <8} {: >10} {}",
                        marker,
                        transaction.id,
                        transaction.txn_date,
                        transaction.kind.as_db_str(),
                        effect,
                        truncate_label(label, 22)
                    ),
                    style,
                )))
            })
            .collect();

        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(summary_lines)
                .block(self.panel_block("RECONCILIATION REVIEW", true))
                .wrap(Wrap { trim: false }),
            layout[0],
        );
        frame.render_widget(
            List::new(items).block(self.panel_block("ELIGIBLE TRANSACTIONS", true)),
            layout[1],
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Space/Enter: toggle row | A: select all | C: clear | Ctrl+S/F2: save | Esc: cancel",
                    tone_style(Tone::Primary),
                )),
                Line::from(Span::styled(
                    "The reconciliation only saves when the selected cleared balance matches the statement balance.",
                    tone_style(Tone::Muted),
                )),
            ])
            .block(self.panel_block("ACTIONS", true))
            .wrap(Wrap { trim: false }),
            layout[2],
        );
    }
    fn render_form_overlay(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(form) = self.form.as_ref() else {
            return;
        };

        let lines: Vec<Line<'static>> = form
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let tone = if index == form.active {
                    Tone::Selected
                } else if field.required && field.value.trim().is_empty() {
                    Tone::Warning
                } else {
                    Tone::Primary
                };
                let required = if field.required { "*" } else { " " };
                Line::from(Span::styled(
                    format!(
                        "{} {: <12} {}",
                        required,
                        field.label,
                        if field.value.is_empty() {
                            "_".to_string()
                        } else {
                            field.value.clone()
                        }
                    ),
                    tone_style(tone),
                ))
            })
            .collect();
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(8),
                Constraint::Length(4),
            ])
            .split(area);
        let field_height = layout[1].height.saturating_sub(2) as usize;
        let scroll_y = if field_height == 0 || lines.len() <= field_height {
            0
        } else {
            let mut offset = form.active.saturating_sub(field_height / 2);
            let max_offset = lines.len().saturating_sub(field_height);
            if offset > max_offset {
                offset = max_offset;
            }
            offset
        };

        frame.render_widget(Clear, area);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(form.title, tone_style(Tone::Header))),
                Line::from(Span::styled(form.hint, tone_style(Tone::Muted))),
            ])
            .block(self.panel_block("FORM", true))
            .wrap(Wrap { trim: false }),
            layout[0],
        );
        frame.render_widget(
            Paragraph::new(lines)
                .block(self.panel_block("FIELDS", true))
                .scroll((scroll_y as u16, 0))
                .wrap(Wrap { trim: false }),
            layout[1],
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Type: replace field | Tab: next | Enter/Ctrl+S/F2: save",
                    tone_style(Tone::Primary),
                )),
                Line::from(Span::styled(
                    "Fields marked with * are required.",
                    tone_style(Tone::Muted),
                )),
                Line::from(Span::styled(
                    self.form_error.as_deref().unwrap_or(""),
                    tone_style(Tone::Negative),
                )),
            ])
            .block(self.panel_block("ACTIONS", true))
            .wrap(Wrap { trim: false }),
            layout[2],
        );
    }

    fn transaction_filter_lines(&self) -> Vec<Line<'static>> {
        let limit = self
            .tx_filters
            .limit
            .map(|value| value.to_string())
            .unwrap_or_else(|| "ALL".to_string());
        let deleted_tone = if self.tx_filters.include_deleted {
            Tone::Warning
        } else {
            Tone::Muted
        };

        vec![
            Line::from(vec![
                Span::styled(
                    format!("RESULTS {}", self.transactions.len()),
                    tone_style(Tone::Primary),
                ),
                Span::raw("  "),
                Span::styled(format!("LIMIT {}", limit), tone_style(Tone::Info)),
                Span::raw("  "),
                Span::styled(
                    format!(
                        "DELETED {}",
                        if self.tx_filters.include_deleted {
                            "YES"
                        } else {
                            "NO"
                        }
                    ),
                    tone_style(deleted_tone),
                ),
                Span::raw("  "),
                Span::styled(
                    if self.transaction_filters_active() {
                        "FILTERS ACTIVE"
                    } else {
                        "RECENT MODE"
                    },
                    tone_style(if self.transaction_filters_active() {
                        Tone::Warning
                    } else {
                        Tone::Muted
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("DATE ", tone_style(Tone::Muted)),
                Span::styled(
                    format!(
                        "{} -> {}",
                        filter_value(self.tx_filters.from.as_deref(), 10),
                        filter_value(self.tx_filters.to.as_deref(), 10)
                    ),
                    tone_style(Tone::Primary),
                ),
                Span::raw("  "),
                Span::styled("SEARCH ", tone_style(Tone::Muted)),
                Span::styled(
                    filter_value(self.tx_filters.search.as_deref(), 18),
                    tone_style(if self.tx_filters.search.is_some() {
                        Tone::Info
                    } else {
                        Tone::Muted
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("ACCOUNT ", tone_style(Tone::Muted)),
                Span::styled(
                    filter_value(self.tx_filters.account.as_deref(), 16),
                    tone_style(if self.tx_filters.account.is_some() {
                        Tone::Primary
                    } else {
                        Tone::Muted
                    }),
                ),
                Span::raw("  "),
                Span::styled("CATEGORY ", tone_style(Tone::Muted)),
                Span::styled(
                    filter_value(self.tx_filters.category.as_deref(), 16),
                    tone_style(if self.tx_filters.category.is_some() {
                        Tone::Primary
                    } else {
                        Tone::Muted
                    }),
                ),
            ]),
        ]
    }
    fn recent_transaction_items(&self) -> Vec<ListItem<'static>> {
        if self.recent_transactions.is_empty() {
            return vec![ListItem::new(Line::from(Span::styled(
                "No transactions yet.",
                tone_style(Tone::Muted),
            )))];
        }

        self.recent_transactions
            .iter()
            .take(6)
            .map(|transaction| {
                let tone = if transaction.deleted_at.is_some() {
                    Tone::Muted
                } else {
                    match transaction.kind {
                        TransactionKind::Income => Tone::Positive,
                        TransactionKind::Expense => Tone::Negative,
                        TransactionKind::Transfer => Tone::Info,
                    }
                };
                let detail = transaction
                    .payee
                    .as_deref()
                    .or(transaction.category_name.as_deref())
                    .or(transaction.to_account_name.as_deref())
                    .unwrap_or("-");
                ListItem::new(Line::from(Span::styled(
                    format!(
                        "{} {: <8} {: >10} {}",
                        &transaction.txn_date[5..],
                        transaction.kind.as_db_str(),
                        format_cents(match transaction.kind {
                            TransactionKind::Expense => -transaction.amount_cents,
                            _ => transaction.amount_cents,
                        }),
                        truncate_label(detail, 16)
                    ),
                    tone_style(tone),
                )))
            })
            .collect()
    }

    fn cash_flow_chart_lines(&self) -> Vec<Line<'static>> {
        if self.cash_flow_trend.is_empty() {
            return vec![Line::from(Span::styled(
                "No monthly data yet.",
                tone_style(Tone::Muted),
            ))];
        }

        let max_net = self
            .cash_flow_trend
            .iter()
            .map(|point| point.net_cents.abs())
            .max()
            .unwrap_or(0);

        let mut lines = vec![Line::from(Span::styled(
            "NET FLOW BY MONTH",
            tone_style(Tone::Muted),
        ))];
        for point in &self.cash_flow_trend {
            let filled = scaled_width(point.net_cents.abs(), max_net, 14);
            let tone = if point.net_cents >= 0 {
                Tone::Positive
            } else {
                Tone::Negative
            };
            let sign = if point.net_cents >= 0 { '+' } else { '-' };
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{: <3}", short_month(&point.month)),
                    tone_style(Tone::Primary),
                ),
                Span::raw(" "),
                Span::styled(
                    ascii_bar(filled, 14, if point.net_cents >= 0 { '#' } else { '!' }),
                    tone_style(tone),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{}{}", sign, format_cents(point.net_cents.abs())),
                    tone_style(tone),
                ),
            ]));
        }
        lines
    }

    fn summary_detail_lines(&self) -> Vec<Line<'static>> {
        vec![
            Line::from(vec![
                Span::styled("FROM ", tone_style(Tone::Muted)),
                Span::styled(self.summary.from.clone(), tone_style(Tone::Primary)),
            ]),
            Line::from(vec![
                Span::styled("TO   ", tone_style(Tone::Muted)),
                Span::styled(self.summary.to.clone(), tone_style(Tone::Primary)),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("TXNS       ", tone_style(Tone::Muted)),
                Span::styled(
                    self.summary.transaction_count.to_string(),
                    tone_style(Tone::Primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("INCOME     ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(self.summary.income_cents),
                    tone_style(Tone::Positive),
                ),
            ]),
            Line::from(vec![
                Span::styled("EXPENSE    ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(-self.summary.expense_cents),
                    tone_style(Tone::Negative),
                ),
            ]),
            Line::from(vec![
                Span::styled("NET        ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(self.summary.net_cents),
                    tone_style(if self.summary.net_cents >= 0 {
                        Tone::Positive
                    } else {
                        Tone::Negative
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("TR IN      ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(self.summary.transfer_in_cents),
                    tone_style(Tone::Info),
                ),
            ]),
            Line::from(vec![
                Span::styled("TR OUT     ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(-self.summary.transfer_out_cents),
                    tone_style(Tone::Info),
                ),
            ]),
        ]
    }

    fn budget_snapshot_lines(&self) -> Vec<Line<'static>> {
        if self.budgets.is_empty() {
            let mut lines = vec![Line::from(Span::styled(
                "No budgets yet.",
                tone_style(Tone::Muted),
            ))];
            if let Some(next_due) = self.due_occurrences.first() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "NEXT DUE",
                    tone_style(Tone::Warning),
                )));
                lines.push(Line::from(Span::styled(
                    format!(
                        "{} {} {}",
                        next_due.due_on,
                        truncate_label(&next_due.rule_name, 12),
                        format_cents(next_due.amount_cents)
                    ),
                    tone_style(Tone::Primary),
                )));
            }
            return lines;
        }

        let mut lines: Vec<Line<'static>> = self
            .budgets
            .iter()
            .take(4)
            .map(|row| {
                let tone = if row.over_budget {
                    Tone::Negative
                } else if row.budget_cents == 0 && row.spent_cents > 0 {
                    Tone::Warning
                } else {
                    Tone::Positive
                };
                Line::from(vec![
                    Span::styled(
                        format!("{: <10}", truncate_label(&row.category_name, 10)),
                        tone_style(Tone::Primary),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        budget_progress_bar(row.spent_cents, row.budget_cents, 8),
                        tone_style(tone),
                    ),
                ])
            })
            .collect();

        if let Some(next_due) = self.due_occurrences.first() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!(
                    "NEXT DUE {} {}",
                    &next_due.due_on[5..],
                    truncate_label(&next_due.rule_name, 11)
                ),
                tone_style(Tone::Warning),
            )));
        }

        lines
    }
    fn category_spending_lines(&self) -> Vec<Line<'static>> {
        if self.category_spending.is_empty() {
            return vec![Line::from(Span::styled(
                "No expense activity this month.",
                tone_style(Tone::Muted),
            ))];
        }

        let max_spent = self
            .category_spending
            .iter()
            .map(|point| point.spent_cents)
            .max()
            .unwrap_or(0);

        self.category_spending
            .iter()
            .map(|point| {
                let filled = scaled_width(point.spent_cents, max_spent, 18);
                Line::from(vec![
                    Span::styled(
                        format!("{: <14}", truncate_label(&point.category_name, 14)),
                        tone_style(Tone::Primary),
                    ),
                    Span::styled(ascii_bar(filled, 18, '#'), tone_style(Tone::Negative)),
                    Span::raw(" "),
                    Span::styled(format_cents(point.spent_cents), tone_style(Tone::Negative)),
                ])
            })
            .collect()
    }

    fn balance_trend_lines(&self) -> Vec<Line<'static>> {
        if self.balance_trend.is_empty() {
            return vec![Line::from(Span::styled(
                "No balance history yet.",
                tone_style(Tone::Muted),
            ))];
        }

        let max_balance = self
            .balance_trend
            .iter()
            .map(|point| point.balance_cents.abs())
            .max()
            .unwrap_or(0);

        self.balance_trend
            .iter()
            .map(|point| {
                let filled = scaled_width(point.balance_cents.abs(), max_balance, 18);
                let tone = if point.balance_cents >= 0 {
                    Tone::Positive
                } else {
                    Tone::Negative
                };
                Line::from(vec![
                    Span::styled(
                        format!("{: <3}", short_month(&point.month)),
                        tone_style(Tone::Primary),
                    ),
                    Span::raw(" "),
                    Span::styled(ascii_bar(filled, 18, '#'), tone_style(tone)),
                    Span::raw(" "),
                    Span::styled(format_cents(point.balance_cents), tone_style(tone)),
                ])
            })
            .collect()
    }

    fn selected_account_lines(&self) -> Vec<Line<'static>> {
        let Some(account) = self.accounts.get(self.account_index) else {
            return vec![
                Line::from(Span::styled("No accounts yet.", tone_style(Tone::Muted))),
                Line::from(Span::styled(
                    "Press N to create one.",
                    tone_style(Tone::Muted),
                )),
            ];
        };
        let current_balance_cents = self
            .balances
            .iter()
            .find(|row| row.account_id == account.id)
            .map(|row| row.current_balance_cents)
            .unwrap_or(account.opening_balance_cents);
        vec![
            Line::from(Span::styled(account.name.clone(), tone_style(Tone::Header))),
            Line::from(vec![
                Span::styled("TYPE    ", tone_style(Tone::Muted)),
                Span::styled(account.kind.as_db_str(), tone_style(Tone::Primary)),
            ]),
            Line::from(vec![
                Span::styled("OPENING ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(account.opening_balance_cents),
                    tone_style(Tone::Info),
                ),
            ]),
            Line::from(vec![
                Span::styled("CURRENT ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(current_balance_cents),
                    tone_style(if current_balance_cents >= 0 {
                        Tone::Positive
                    } else {
                        Tone::Negative
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("OPENED  ", tone_style(Tone::Muted)),
                Span::styled(account.opened_on.clone(), tone_style(Tone::Primary)),
            ]),
            Line::from(""),
            Line::from(Span::styled(
                "E edits this account. D archives it if nothing still depends on it.",
                tone_style(Tone::Muted),
            )),
            Line::from(Span::styled(
                "R opens reconciliation for the selected account.",
                tone_style(Tone::Muted),
            )),
        ]
    }

    fn market_quote_lines(&self) -> Vec<Line<'static>> {
        let Some(entry) = self.watchlist.get(self.market_index) else {
            return vec![
                Line::from(Span::styled("No market selected.", tone_style(Tone::Muted))),
                Line::from(Span::styled(
                    "Press N to add a ticker to the watchlist.",
                    tone_style(Tone::Muted),
                )),
            ];
        };

        let mut lines = vec![Line::from(vec![
            Span::styled("TICKER ", tone_style(Tone::Muted)),
            Span::styled(entry.ticker.clone(), tone_style(Tone::Header)),
        ])];

        if let Some(quote) = self.market_quote.as_ref() {
            let direction = quote.change.or(quote.change_percent).unwrap_or(0.0);
            let change_tone = if direction > 0.0 {
                Tone::Positive
            } else if direction < 0.0 {
                Tone::Negative
            } else {
                Tone::Muted
            };
            let listed = match (quote.exchange.as_deref(), quote.currency.as_deref()) {
                (Some(exchange), Some(currency)) => format!("{} / {}", exchange, currency),
                (Some(exchange), None) => exchange.to_string(),
                (None, Some(currency)) => currency.to_string(),
                (None, None) => "-".to_string(),
            };
            let price = match quote.currency.as_deref() {
                Some(currency) => format!("{} {:.2}", currency, quote.last_price),
                None => format!("{:.2}", quote.last_price),
            };
            let move_text = match (quote.change, quote.change_percent) {
                (Some(change), Some(change_percent)) => {
                    format!("{:+.2} ({:+.2}%)", change, change_percent)
                }
                (Some(change), None) => format!("{:+.2}", change),
                (None, Some(change_percent)) => format!("{:+.2}%", change_percent),
                (None, None) => "-".to_string(),
            };
            lines.push(Line::from(vec![
                Span::styled("NAME   ", tone_style(Tone::Muted)),
                Span::styled(quote.name.clone(), tone_style(Tone::Primary)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("LISTED ", tone_style(Tone::Muted)),
                Span::styled(listed, tone_style(Tone::Info)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("PRICE  ", tone_style(Tone::Muted)),
                Span::styled(
                    price,
                    tone_style(if quote.stale {
                        Tone::Warning
                    } else {
                        Tone::Primary
                    }),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("MOVE   ", tone_style(Tone::Muted)),
                Span::styled(move_text, tone_style(change_tone)),
            ]));
            lines.push(Line::from(vec![
                Span::styled("STATE  ", tone_style(Tone::Muted)),
                Span::styled(
                    if quote.stale {
                        "STALE CACHE".to_string()
                    } else {
                        quote
                            .market_state
                            .as_deref()
                            .map(|state| state.to_ascii_uppercase())
                            .unwrap_or_else(|| "-".to_string())
                    },
                    tone_style(if quote.stale {
                        Tone::Warning
                    } else if quote.market_state.is_some() {
                        Tone::Info
                    } else {
                        Tone::Muted
                    }),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("FETCHED", tone_style(Tone::Muted)),
                Span::raw(" "),
                Span::styled(quote.fetched_at.clone(), tone_style(Tone::Muted)),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                "No cached quote is available for this ticker yet.",
                tone_style(Tone::Muted),
            )));
            lines.push(Line::from(Span::styled(
                "Press Enter or R to fetch a fresh snapshot.",
                tone_style(Tone::Warning),
            )));
        }

        lines
    }

    fn market_brief_lines(&self) -> Vec<Line<'static>> {
        let Some(entry) = self.watchlist.get(self.market_index) else {
            return vec![Line::from(Span::styled(
                "Add a ticker to load a market brief.",
                tone_style(Tone::Muted),
            ))];
        };

        let Some(feed) = self.market_news.as_ref() else {
            return vec![
                Line::from(Span::styled(
                    format!("No cached brief input is available for {}.", entry.ticker),
                    tone_style(Tone::Muted),
                )),
                Line::from(Span::styled(
                    "Press Enter or R to fetch the latest brief inputs, or A to refresh the whole watchlist.",
                    tone_style(Tone::Warning),
                )),
            ];
        };

        let brief = compose_market_brief(self.market_quote.as_ref(), feed);
        let mut lines = vec![Line::from(vec![
            Span::styled("BRIEF  ", tone_style(Tone::Muted)),
            Span::styled(
                if brief.stale {
                    format!("stale input from {}", brief.fetched_at)
                } else {
                    format!(
                        "{} article(s) from {} source(s) at {}",
                        brief.headline_count, brief.source_count, brief.fetched_at
                    )
                },
                tone_style(if brief.stale {
                    Tone::Warning
                } else {
                    Tone::Info
                }),
            ),
        ])];

        lines.push(Line::from(vec![
            Span::styled("TONE   ", tone_style(Tone::Muted)),
            Span::styled(
                brief.tone.to_ascii_uppercase(),
                tone_style(match brief.tone.as_str() {
                    "bullish" => Tone::Positive,
                    "bearish" => Tone::Negative,
                    "mixed" => Tone::Warning,
                    _ => Tone::Muted,
                }),
            ),
        ]));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            brief.summary,
            tone_style(Tone::Primary),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "WHY IT MATTERS",
            tone_style(Tone::Header),
        )));
        for item in &brief.why_it_matters {
            lines.push(Line::from(vec![
                Span::styled("- ", tone_style(Tone::Muted)),
                Span::styled(item.clone(), tone_style(Tone::Primary)),
            ]));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "WATCH NEXT",
            tone_style(Tone::Header),
        )));
        for item in &brief.watch_items {
            lines.push(Line::from(vec![
                Span::styled("- ", tone_style(Tone::Muted)),
                Span::styled(item.clone(), tone_style(Tone::Primary)),
            ]));
        }

        lines
    }

    fn budget_detail_lines(&self) -> Vec<Line<'static>> {
        let scenario_name = self
            .selected_planning_scenario()
            .map(|scenario| scenario.name.clone())
            .unwrap_or_else(|| "baseline".to_string());
        if self.budgets.is_empty() {
            return vec![
                Line::from(vec![
                    Span::styled("SCENARIO ", tone_style(Tone::Muted)),
                    Span::styled(scenario_name, tone_style(Tone::Primary)),
                ]),
                Line::from(Span::styled(
                    "No budgets set for this month.",
                    tone_style(Tone::Muted),
                )),
                Line::from(Span::styled(
                    "Press N here to create one, or use S and run `budget set ...`.",
                    tone_style(Tone::Muted),
                )),
            ];
        }

        let mut lines = vec![
            Line::from(vec![
                Span::styled("SCENARIO ", tone_style(Tone::Muted)),
                Span::styled(scenario_name, tone_style(Tone::Primary)),
            ]),
            Line::from(Span::styled(
                "CATEGORY        BAR           SPENT       BUDGET      REMAINING",
                tone_style(Tone::Muted),
            )),
        ];

        for row in &self.budgets {
            let tone = if row.over_budget {
                Tone::Negative
            } else if row.budget_cents == 0 && row.spent_cents > 0 {
                Tone::Warning
            } else {
                Tone::Positive
            };
            let category_label = if row.is_override {
                format!("{} *", row.category_name)
            } else {
                row.category_name.clone()
            };
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{: <15}", truncate_label(&category_label, 15)),
                    tone_style(Tone::Primary),
                ),
                Span::styled(
                    format!(
                        "{: <13}",
                        budget_progress_bar(row.spent_cents, row.budget_cents, 13)
                    ),
                    tone_style(tone),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{: >10}", format_cents(row.spent_cents)),
                    tone_style(Tone::Negative),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{: >10}", format_cents(row.budget_cents)),
                    tone_style(Tone::Primary),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{: >10}", format_cents(row.remaining_cents)),
                    tone_style(if row.remaining_cents >= 0 {
                        Tone::Positive
                    } else {
                        Tone::Negative
                    }),
                ),
            ]));
        }

        if self.budgets.iter().any(|row| row.is_override) {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "* marks a scenario override that differs from the baseline budget.",
                tone_style(Tone::Muted),
            )));
        }

        lines
    }

    fn planning_forecast_items(&self) -> Vec<ListItem<'static>> {
        if self.planning_forecast.daily.is_empty() {
            return vec![ListItem::new(Line::from(Span::styled(
                "No forecast points yet.",
                tone_style(Tone::Muted),
            )))];
        }

        self.planning_forecast
            .daily
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let style = if index == self.planning_day_index {
                    tone_style(Tone::Selected)
                } else if point.closing_balance_cents < 0 {
                    tone_style(Tone::Negative)
                } else {
                    tone_style(Tone::Primary)
                };
                ListItem::new(Line::from(Span::styled(
                    format!(
                        "{} {: >10} net {: >10}",
                        point.date,
                        format_cents(point.closing_balance_cents),
                        format_cents(point.net_cents)
                    ),
                    style,
                )))
            })
            .collect()
    }

    fn planning_forecast_lines(&self) -> Vec<Line<'static>> {
        let lowest_balance = lowest_forecast_balance(&self.planning_forecast);
        let highest_balance = highest_forecast_balance(&self.planning_forecast);
        let next_shortfall = first_negative_forecast_date(&self.planning_forecast)
            .unwrap_or_else(|| "-".to_string());
        let mut lines = vec![
            Line::from(Span::styled(
                format!(
                    "SUBVIEW {} | SCENARIO {}",
                    self.current_planning_subview().label(),
                    self.planning_forecast
                        .scenario
                        .name
                        .as_deref()
                        .unwrap_or("baseline")
                ),
                tone_style(Tone::Muted),
            )),
            Line::from(""),
        ];
        if let Some(point) = self.planning_forecast.daily.get(self.planning_day_index) {
            lines.extend([
                Line::from(vec![
                    Span::styled("DATE   ", tone_style(Tone::Muted)),
                    Span::styled(point.date.clone(), tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("OPEN   ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(point.opening_balance_cents),
                        tone_style(Tone::Primary),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("INFLOW ", tone_style(Tone::Muted)),
                    Span::styled(format_cents(point.inflow_cents), tone_style(Tone::Positive)),
                ]),
                Line::from(vec![
                    Span::styled("OUT    ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(-point.outflow_cents),
                        tone_style(Tone::Negative),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("CLOSE  ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(point.closing_balance_cents),
                        tone_style(if point.closing_balance_cents >= 0 {
                            Tone::Positive
                        } else {
                            Tone::Negative
                        }),
                    ),
                ]),
            ]);
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "PATH SUMMARY",
                tone_style(Tone::Header),
            )));
            lines.extend([
                Line::from(vec![
                    Span::styled("LOW    ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(lowest_balance),
                        tone_style(if lowest_balance >= 0 {
                            Tone::Positive
                        } else {
                            Tone::Negative
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("HIGH   ", tone_style(Tone::Muted)),
                    Span::styled(format_cents(highest_balance), tone_style(Tone::Info)),
                ]),
                Line::from(vec![
                    Span::styled("RISK   ", tone_style(Tone::Muted)),
                    Span::styled(
                        next_shortfall,
                        tone_style(
                            if first_negative_forecast_date(&self.planning_forecast).is_some() {
                                Tone::Warning
                            } else {
                                Tone::Positive
                            },
                        ),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("BILLS  ", tone_style(Tone::Muted)),
                    Span::styled(
                        self.planning_forecast.bill_calendar.len().to_string(),
                        tone_style(Tone::Warning),
                    ),
                ]),
            ]);
            if !point.alerts.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "DAY ALERTS",
                    tone_style(Tone::Header),
                )));
                for alert in &point.alerts {
                    lines.push(Line::from(Span::styled(
                        format!("- {}", alert),
                        tone_style(Tone::Warning),
                    )));
                }
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "12M ROLLUP",
            tone_style(Tone::Header),
        )));
        for month in &self.planning_forecast.monthly {
            lines.push(Line::from(vec![
                Span::styled(format!("{: <7}", month.month), tone_style(Tone::Primary)),
                Span::raw(" "),
                Span::styled(
                    format!("{: >10}", format_cents(month.net_cents)),
                    tone_style(if month.net_cents >= 0 {
                        Tone::Positive
                    } else {
                        Tone::Negative
                    }),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("end {: >10}", format_cents(month.ending_balance_cents)),
                    tone_style(Tone::Info),
                ),
            ]));
        }
        if !self.planning_forecast.warnings.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "WARNINGS",
                tone_style(Tone::Header),
            )));
            for warning in self.planning_forecast.warnings.iter().take(4) {
                lines.push(Line::from(Span::styled(
                    format!("- {}", warning),
                    tone_style(Tone::Warning),
                )));
            }
        }
        lines
    }

    fn planning_item_list_items(&self) -> Vec<ListItem<'static>> {
        if self.planning_items.is_empty() {
            return vec![ListItem::new(Line::from(Span::styled(
                "No planned items. Press N to add one.",
                tone_style(Tone::Muted),
            )))];
        }

        self.planning_items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let style = if index == self.planning_item_index {
                    tone_style(Tone::Selected)
                } else if item.linked_transaction_id.is_some() {
                    tone_style(Tone::Positive)
                } else {
                    tone_style(Tone::Primary)
                };
                ListItem::new(Line::from(Span::styled(
                    format!(
                        "{} {: <18} {: >10}",
                        item.due_on,
                        truncate_label(&item.title, 18),
                        format_cents(item.amount_cents)
                    ),
                    style,
                )))
            })
            .collect()
    }

    fn planning_calendar_lines(&self) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        if let Some(item) = self.planning_items.get(self.planning_item_index) {
            lines.extend([
                Line::from(Span::styled("SELECTED ITEM", tone_style(Tone::Header))),
                Line::from(vec![
                    Span::styled("TITLE  ", tone_style(Tone::Muted)),
                    Span::styled(item.title.clone(), tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("DATE   ", tone_style(Tone::Muted)),
                    Span::styled(item.due_on.clone(), tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("TYPE   ", tone_style(Tone::Muted)),
                    Span::styled(item.kind.as_db_str(), tone_style(Tone::Info)),
                ]),
                Line::from(vec![
                    Span::styled("AMOUNT ", tone_style(Tone::Muted)),
                    Span::styled(format_cents(item.amount_cents), tone_style(Tone::Warning)),
                ]),
                Line::from(vec![
                    Span::styled("STATUS ", tone_style(Tone::Muted)),
                    Span::styled(
                        if item.linked_transaction_id.is_some() {
                            "posted"
                        } else {
                            "planned"
                        },
                        tone_style(if item.linked_transaction_id.is_some() {
                            Tone::Positive
                        } else {
                            Tone::Warning
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("SCEN   ", tone_style(Tone::Muted)),
                    Span::styled(
                        item.scenario_name
                            .clone()
                            .unwrap_or_else(|| "baseline".to_string()),
                        tone_style(Tone::Primary),
                    ),
                ]),
                Line::from(""),
            ]);
        }
        lines.push(Line::from(Span::styled(
            "UPCOMING BILLS",
            tone_style(Tone::Header),
        )));
        if self.planning_forecast.bill_calendar.is_empty() {
            lines.push(Line::from(Span::styled(
                "No upcoming bills in the current horizon.",
                tone_style(Tone::Muted),
            )));
        } else {
            for bill in self.planning_forecast.bill_calendar.iter().take(12) {
                lines.push(Line::from(vec![
                    Span::styled(format!("{: <10}", bill.date), tone_style(Tone::Primary)),
                    Span::styled(
                        format!("{: <18}", truncate_label(&bill.title, 18)),
                        tone_style(Tone::Primary),
                    ),
                    Span::raw(" "),
                    Span::styled(format_cents(bill.amount_cents), tone_style(Tone::Warning)),
                    Span::raw(" "),
                    Span::styled(bill.source.clone(), tone_style(Tone::Muted)),
                ]));
            }
        }
        lines
    }

    fn planning_goal_items(&self) -> Vec<ListItem<'static>> {
        if self.planning_forecast.goal_status.is_empty() {
            return vec![ListItem::new(Line::from(Span::styled(
                "No goals. Press N to add one.",
                tone_style(Tone::Muted),
            )))];
        }
        self.planning_forecast
            .goal_status
            .iter()
            .enumerate()
            .map(|(index, goal)| {
                let style = if index == self.planning_goal_index {
                    tone_style(Tone::Selected)
                } else if goal.on_track {
                    tone_style(Tone::Positive)
                } else {
                    tone_style(Tone::Warning)
                };
                ListItem::new(Line::from(Span::styled(
                    format!(
                        "{: <18} {: <14} {}",
                        truncate_label(&goal.name, 18),
                        goal.kind.as_db_str(),
                        if goal.on_track { "ok" } else { "watch" }
                    ),
                    style,
                )))
            })
            .collect()
    }

    fn planning_goal_lines(&self) -> Vec<Line<'static>> {
        let Some(goal) = self
            .planning_forecast
            .goal_status
            .get(self.planning_goal_index)
        else {
            return vec![Line::from(Span::styled(
                "Select a goal to inspect its projected status.",
                tone_style(Tone::Muted),
            ))];
        };
        vec![
            Line::from(Span::styled(goal.name.clone(), tone_style(Tone::Header))),
            Line::from(vec![
                Span::styled("KIND      ", tone_style(Tone::Muted)),
                Span::styled(goal.kind.as_db_str(), tone_style(Tone::Info)),
            ]),
            Line::from(vec![
                Span::styled("ACCOUNT   ", tone_style(Tone::Muted)),
                Span::styled(goal.account_name.clone(), tone_style(Tone::Primary)),
            ]),
            Line::from(vec![
                Span::styled("CURRENT   ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(goal.current_balance_cents),
                    tone_style(Tone::Primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("PROJECTED ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(goal.projected_balance_cents),
                    tone_style(if goal.projected_balance_cents >= 0 {
                        Tone::Positive
                    } else {
                        Tone::Negative
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("REMAINING ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(goal.remaining_cents),
                    tone_style(Tone::Warning),
                ),
            ]),
            Line::from(vec![
                Span::styled("SUGGESTED ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(goal.suggested_monthly_contribution_cents),
                    tone_style(Tone::Info),
                ),
            ]),
            Line::from(vec![
                Span::styled("STATE     ", tone_style(Tone::Muted)),
                Span::styled(
                    if goal.on_track { "on track" } else { "behind" },
                    tone_style(if goal.on_track {
                        Tone::Positive
                    } else {
                        Tone::Warning
                    }),
                ),
            ]),
            Line::from(vec![
                Span::styled("BREACH    ", tone_style(Tone::Muted)),
                Span::styled(
                    goal.breach_date.clone().unwrap_or_else(|| "-".to_string()),
                    tone_style(Tone::Warning),
                ),
            ]),
        ]
    }

    fn planning_scenario_items(&self) -> Vec<ListItem<'static>> {
        let mut items = vec![ListItem::new(Line::from(Span::styled(
            "BASELINE",
            if self.planning_scenario_index == 0 {
                tone_style(Tone::Selected)
            } else if self.selected_planning_scenario_id.is_none() {
                tone_style(Tone::Positive)
            } else {
                tone_style(Tone::Primary)
            },
        )))];
        items.extend(
            self.planning_scenarios
                .iter()
                .enumerate()
                .map(|(index, scenario)| {
                    let style = if self.planning_scenario_index == index + 1 {
                        tone_style(Tone::Selected)
                    } else if self.selected_planning_scenario_id == Some(scenario.id) {
                        tone_style(Tone::Positive)
                    } else {
                        tone_style(Tone::Primary)
                    };
                    ListItem::new(Line::from(Span::styled(
                        truncate_label(&scenario.name, 26),
                        style,
                    )))
                }),
        );
        if self.planning_scenarios.is_empty() {
            items.push(ListItem::new(Line::from(Span::styled(
                "Press N to create a scenario",
                tone_style(Tone::Muted),
            ))));
        }
        items
    }

    fn planning_scenario_lines(&self) -> Vec<Line<'static>> {
        let baseline_net = self
            .planning_baseline
            .daily
            .iter()
            .take(30)
            .map(|point| point.net_cents)
            .sum::<i64>();
        let baseline_low = lowest_forecast_balance(&self.planning_baseline);
        let baseline_shortfall = first_negative_forecast_date(&self.planning_baseline)
            .unwrap_or_else(|| "-".to_string());
        if let Some(scenario) = self.selected_planning_scenario() {
            let is_active = self.selected_planning_scenario_id == Some(scenario.id);
            if !is_active {
                return vec![
                    Line::from(Span::styled(
                        scenario.name.clone(),
                        tone_style(Tone::Header),
                    )),
                    Line::from(vec![
                        Span::styled("NOTE  ", tone_style(Tone::Muted)),
                        Span::styled(
                            scenario.note.clone().unwrap_or_else(|| "-".to_string()),
                            tone_style(Tone::Primary),
                        ),
                    ]),
                    Line::from(vec![
                        Span::styled("STATE ", tone_style(Tone::Muted)),
                        Span::styled("ready", tone_style(Tone::Info)),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Press Enter to load this scenario into the forecast.",
                        tone_style(Tone::Muted),
                    )),
                    Line::from(Span::styled(
                        "Press E to rename/edit the note, or D to archive it.",
                        tone_style(Tone::Muted),
                    )),
                    Line::from(Span::styled(
                        "Once active, this panel will compare 30D net, lowest balance, and shortfall timing against the baseline.",
                        tone_style(Tone::Muted),
                    )),
                ];
            }
            let active_net = self
                .planning_forecast
                .daily
                .iter()
                .take(30)
                .map(|point| point.net_cents)
                .sum::<i64>();
            let active_low = lowest_forecast_balance(&self.planning_forecast);
            let active_shortfall = first_negative_forecast_date(&self.planning_forecast)
                .unwrap_or_else(|| "-".to_string());
            vec![
                Line::from(Span::styled(
                    scenario.name.clone(),
                    tone_style(Tone::Header),
                )),
                Line::from(vec![
                    Span::styled("NOTE  ", tone_style(Tone::Muted)),
                    Span::styled(
                        scenario.note.clone().unwrap_or_else(|| "-".to_string()),
                        tone_style(Tone::Primary),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("STATE ", tone_style(Tone::Muted)),
                    Span::styled(
                        if is_active { "active" } else { "highlighted" },
                        tone_style(if is_active {
                            Tone::Positive
                        } else {
                            Tone::Info
                        }),
                    ),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("30D NET BASE ", tone_style(Tone::Muted)),
                    Span::styled(format_cents(baseline_net), tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("30D NET SCN  ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(active_net),
                        tone_style(if active_net >= baseline_net {
                            Tone::Positive
                        } else {
                            Tone::Warning
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("LOWEST BASE  ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(baseline_low),
                        tone_style(if baseline_low >= 0 {
                            Tone::Positive
                        } else {
                            Tone::Negative
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("LOWEST SCN   ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(active_low),
                        tone_style(if active_low >= 0 {
                            Tone::Positive
                        } else {
                            Tone::Negative
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("SHORTFALL B  ", tone_style(Tone::Muted)),
                    Span::styled(baseline_shortfall, tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("SHORTFALL S  ", tone_style(Tone::Muted)),
                    Span::styled(active_shortfall, tone_style(Tone::Warning)),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "Press N for scenario items, E to edit this scenario, or D to archive it.",
                    tone_style(Tone::Muted),
                )),
            ]
        } else {
            if self.planning_scenarios.is_empty() {
                return vec![
                    Line::from(Span::styled("BASELINE", tone_style(Tone::Header))),
                    Line::from(vec![
                        Span::styled("30D NET ", tone_style(Tone::Muted)),
                        Span::styled(format_cents(baseline_net), tone_style(Tone::Primary)),
                    ]),
                    Line::from(vec![
                        Span::styled("LOWEST  ", tone_style(Tone::Muted)),
                        Span::styled(
                            format_cents(baseline_low),
                            tone_style(if baseline_low >= 0 {
                                Tone::Positive
                            } else {
                                Tone::Negative
                            }),
                        ),
                    ]),
                    Line::from(vec![
                        Span::styled("RISK    ", tone_style(Tone::Muted)),
                        Span::styled(baseline_shortfall, tone_style(Tone::Primary)),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "No saved scenarios yet.",
                        tone_style(Tone::Muted),
                    )),
                    Line::from(Span::styled(
                        "Press N to create one. Baseline is the built-in default forecast.",
                        tone_style(Tone::Muted),
                    )),
                    Line::from(Span::styled(
                        "Use Left/Right to switch to FORECAST, CALENDAR, or GOALS.",
                        tone_style(Tone::Muted),
                    )),
                ];
            }
            vec![
                Line::from(Span::styled("BASELINE", tone_style(Tone::Header))),
                Line::from(vec![
                    Span::styled("30D NET ", tone_style(Tone::Muted)),
                    Span::styled(format_cents(baseline_net), tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("LOWEST  ", tone_style(Tone::Muted)),
                    Span::styled(
                        format_cents(baseline_low),
                        tone_style(if baseline_low >= 0 {
                            Tone::Positive
                        } else {
                            Tone::Negative
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("RISK    ", tone_style(Tone::Muted)),
                    Span::styled(baseline_shortfall, tone_style(Tone::Primary)),
                ]),
                Line::from(vec![
                    Span::styled("WARNINGS ", tone_style(Tone::Muted)),
                    Span::styled(
                        self.planning_baseline.warnings.len().to_string(),
                        tone_style(if self.planning_baseline.warnings.is_empty() {
                            Tone::Positive
                        } else {
                            Tone::Warning
                        }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("ALERTS   ", tone_style(Tone::Muted)),
                    Span::styled(
                        self.planning_baseline.alerts.len().to_string(),
                        tone_style(if self.planning_baseline.alerts.is_empty() {
                            Tone::Positive
                        } else {
                            Tone::Warning
                        }),
                    ),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "Press Enter on a scenario to compare it, E to edit it, or D to archive it.",
                    tone_style(Tone::Muted),
                )),
            ]
        }
    }

    fn planning_status_text(&self) -> String {
        let scenario = self
            .planning_forecast
            .scenario
            .name
            .as_deref()
            .unwrap_or("baseline");
        let latest_alert = self
            .planning_forecast
            .alerts
            .first()
            .cloned()
            .unwrap_or_else(|| "No active planning alerts.".to_string());
        format!(
            "{} | scenario {} | {}",
            self.current_planning_subview().label(),
            scenario,
            latest_alert
        )
    }

    fn selected_reconciliation_lines(&self) -> Vec<Line<'static>> {
        let Some(reconciliation) = self.reconciliations.get(self.reconciliation_index) else {
            return vec![
                Line::from(Span::styled(
                    "No reconciliations yet.",
                    tone_style(Tone::Muted),
                )),
                Line::from(Span::styled(
                    "Press N or R here to start one, or press R from ACCOUNTS.",
                    tone_style(Tone::Muted),
                )),
            ];
        };

        vec![
            Line::from(vec![
                Span::styled("ACCOUNT ", tone_style(Tone::Muted)),
                Span::styled(
                    reconciliation.account_name.clone(),
                    tone_style(Tone::Primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("ENDING  ", tone_style(Tone::Muted)),
                Span::styled(
                    reconciliation.statement_ending_on.clone(),
                    tone_style(Tone::Primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("STMT    ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(reconciliation.statement_balance_cents),
                    tone_style(Tone::Info),
                ),
            ]),
            Line::from(vec![
                Span::styled("CLEARED ", tone_style(Tone::Muted)),
                Span::styled(
                    format_cents(reconciliation.cleared_balance_cents),
                    tone_style(Tone::Info),
                ),
            ]),
            Line::from(vec![
                Span::styled("TXNS    ", tone_style(Tone::Muted)),
                Span::styled(
                    reconciliation.transaction_count.to_string(),
                    tone_style(Tone::Primary),
                ),
            ]),
            Line::from(vec![
                Span::styled("CREATED ", tone_style(Tone::Muted)),
                Span::styled(reconciliation.created_at.clone(), tone_style(Tone::Muted)),
            ]),
        ]
    }

    fn poly_market_lines(&self) -> Vec<Line<'static>> {
        let Some(entry) = self.poly_watchlist.get(self.poly_index) else {
            return vec![
                Line::from(Span::styled(
                    "No Polymarket market selected.",
                    tone_style(Tone::Muted),
                )),
                Line::from(Span::styled(
                    "Press N to add a Polymarket market slug.",
                    tone_style(Tone::Muted),
                )),
            ];
        };

        let mut lines = vec![Line::from(vec![
            Span::styled("SLUG   ", tone_style(Tone::Muted)),
            Span::styled(entry.slug.clone(), tone_style(Tone::Header)),
        ])];
        if let Some(market) = self.poly_market.as_ref() {
            lines.push(Line::from(vec![
                Span::styled("Q      ", tone_style(Tone::Muted)),
                Span::styled(
                    truncate_label(&market.question, 56),
                    tone_style(Tone::Primary),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("YES    ", tone_style(Tone::Muted)),
                Span::styled(
                    market
                        .yes_price
                        .map(|value| format!("{:.0}%", value * 100.0))
                        .unwrap_or_else(|| "-".to_string()),
                    tone_style(Tone::Positive),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("NO     ", tone_style(Tone::Muted)),
                Span::styled(
                    market
                        .no_price
                        .map(|value| format!("{:.0}%", value * 100.0))
                        .unwrap_or_else(|| "-".to_string()),
                    tone_style(Tone::Negative),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("24H    ", tone_style(Tone::Muted)),
                Span::styled(
                    market
                        .volume_24h
                        .map(|value| format!("${:.0}", value))
                        .unwrap_or_else(|| "-".to_string()),
                    tone_style(Tone::Info),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("LIQ    ", tone_style(Tone::Muted)),
                Span::styled(
                    market
                        .liquidity
                        .map(|value| format!("${:.0}", value))
                        .unwrap_or_else(|| "-".to_string()),
                    tone_style(Tone::Info),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("STATE  ", tone_style(Tone::Muted)),
                Span::styled(
                    if market.stale {
                        "stale".to_string()
                    } else if market.closed {
                        "closed".to_string()
                    } else if market.active {
                        "active".to_string()
                    } else {
                        "inactive".to_string()
                    },
                    tone_style(if market.stale {
                        Tone::Warning
                    } else {
                        Tone::Info
                    }),
                ),
            ]));
            if let Some(end_date) = market.end_date.as_deref() {
                lines.push(Line::from(vec![
                    Span::styled("ENDS   ", tone_style(Tone::Muted)),
                    Span::styled(end_date.to_string(), tone_style(Tone::Primary)),
                ]));
            }
        } else {
            lines.push(Line::from(Span::styled(
                "No cached market snapshot. Press Enter or R to fetch one.",
                tone_style(Tone::Warning),
            )));
        }
        lines
    }

    fn poly_brief_lines(&self) -> Vec<Line<'static>> {
        let Some(entry) = self.poly_watchlist.get(self.poly_index) else {
            return vec![Line::from(Span::styled(
                "Add a Polymarket market to load a brief.",
                tone_style(Tone::Muted),
            ))];
        };
        let Some(market) = self.poly_market.as_ref() else {
            return vec![
                Line::from(Span::styled(
                    format!(
                        "No cached Polymarket market is available for {}.",
                        entry.slug
                    ),
                    tone_style(Tone::Muted),
                )),
                Line::from(Span::styled(
                    "Press Enter or R to fetch it, or A to refresh the full POLY watchlist.",
                    tone_style(Tone::Warning),
                )),
            ];
        };
        let brief = compose_poly_brief(market);
        let mut lines = vec![Line::from(vec![
            Span::styled("BRIEF  ", tone_style(Tone::Muted)),
            Span::styled(
                if brief.stale {
                    format!("stale input from {}", brief.fetched_at)
                } else {
                    format!("snapshot from {}", brief.fetched_at)
                },
                tone_style(if brief.stale {
                    Tone::Warning
                } else {
                    Tone::Info
                }),
            ),
        ])];
        lines.push(Line::from(vec![
            Span::styled("TONE   ", tone_style(Tone::Muted)),
            Span::styled(brief.tone.to_ascii_uppercase(), tone_style(Tone::Info)),
        ]));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            brief.summary,
            tone_style(Tone::Primary),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "WHY IT MATTERS",
            tone_style(Tone::Header),
        )));
        for item in &brief.why_it_matters {
            lines.push(Line::from(vec![
                Span::styled("- ", tone_style(Tone::Muted)),
                Span::styled(item.clone(), tone_style(Tone::Primary)),
            ]));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "WATCH NEXT",
            tone_style(Tone::Header),
        )));
        for item in &brief.watch_items {
            lines.push(Line::from(vec![
                Span::styled("- ", tone_style(Tone::Muted)),
                Span::styled(item.clone(), tone_style(Tone::Primary)),
            ]));
        }
        lines
    }

    fn poly_mover_items(&self) -> Vec<ListItem<'static>> {
        if self.poly_movers.is_empty() {
            return vec![ListItem::new(Line::from(Span::styled(
                "No cached movers yet. Press M or A in POLY.",
                tone_style(Tone::Muted),
            )))];
        }

        self.poly_movers
            .iter()
            .map(|market| {
                let tone = if market.stale {
                    Tone::Warning
                } else {
                    Tone::Primary
                };
                let yes = market
                    .yes_price
                    .map(|value| format!("{:>3.0}%", value * 100.0))
                    .unwrap_or_else(|| " --".to_string());
                let volume = market
                    .volume_24h
                    .map(|value| format!("${:.0}", value))
                    .unwrap_or_else(|| "-".to_string());
                ListItem::new(Line::from(Span::styled(
                    format!(
                        "{: <30} {: >5} {: >8}",
                        truncate_label(&market.question, 30),
                        yes,
                        volume
                    ),
                    tone_style(tone),
                )))
            })
            .collect()
    }

    fn panel_block(&self, title: &'static str, active: bool) -> Block<'static> {
        Block::default()
            .title(Span::styled(
                format!(" {} ", title),
                tone_style(Tone::Header),
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme::border_color(active)))
            .style(theme::block_style())
    }

    fn metric_block(&self, title: &'static str, value: String, tone: Tone) -> Paragraph<'static> {
        Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(value, tone_style(tone))),
        ])
        .block(self.panel_block(title, true))
        .alignment(Alignment::Center)
    }
}

struct PlanningChartData {
    actual: Vec<(f64, f64)>,
    baseline: Vec<(f64, f64)>,
    scenario: Vec<(f64, f64)>,
    x_bounds: [f64; 2],
    y_bounds: [f64; 2],
    x_labels: Vec<Line<'static>>,
    y_labels: Vec<Line<'static>>,
}

fn planning_chart_data(
    actual_history: &[crate::model::WeeklyBalancePoint],
    baseline: &crate::model::ForecastSnapshot,
    forecast: &crate::model::ForecastSnapshot,
    show_scenario: bool,
) -> Option<PlanningChartData> {
    let actual = planning_actual_points(actual_history);
    let baseline_points = planning_weekly_projection_points(actual_history, baseline);
    let scenario_points = if show_scenario {
        planning_weekly_projection_points(actual_history, forecast)
    } else {
        Vec::new()
    };

    let all_points = actual
        .iter()
        .chain(baseline_points.iter())
        .chain(scenario_points.iter())
        .collect::<Vec<_>>();
    if all_points.is_empty() {
        return None;
    }

    let min_y = all_points
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::INFINITY, f64::min);
    let max_y = all_points
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::NEG_INFINITY, f64::max);
    let y_padding = if (max_y - min_y).abs() < f64::EPSILON {
        max_y.abs().max(100.0) * 0.15 + 100.0
    } else {
        ((max_y - min_y) * 0.12).max(100.0)
    };
    let max_x = all_points
        .iter()
        .map(|(x, _)| *x)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let first_week_start = chart_first_week_start(actual_history, baseline, forecast)?;

    Some(PlanningChartData {
        actual,
        baseline: baseline_points,
        scenario: scenario_points,
        x_bounds: [0.0, max_x],
        y_bounds: [min_y - y_padding, max_y + y_padding],
        x_labels: planning_chart_x_labels(first_week_start, max_x.round() as usize),
        y_labels: planning_chart_y_labels(min_y, max_y),
    })
}

fn planning_actual_points(actual_history: &[crate::model::WeeklyBalancePoint]) -> Vec<(f64, f64)> {
    actual_history
        .iter()
        .enumerate()
        .map(|(index, point)| (index as f64, point.opening_balance_cents as f64))
        .collect()
}

fn planning_weekly_projection_points(
    actual_history: &[crate::model::WeeklyBalancePoint],
    snapshot: &crate::model::ForecastSnapshot,
) -> Vec<(f64, f64)> {
    if snapshot.daily.is_empty() {
        return Vec::new();
    }

    let anchor_index = actual_history.len().saturating_sub(1);
    let anchor_week = actual_history
        .last()
        .and_then(|point| parse_iso_date(&point.week_start))
        .or_else(|| {
            snapshot
                .daily
                .first()
                .and_then(|point| parse_iso_date(&point.date))
                .map(start_of_week)
        });
    let Some(anchor_week) = anchor_week else {
        return Vec::new();
    };

    let mut points = Vec::new();
    let anchor_opening = actual_history
        .last()
        .map(|point| point.opening_balance_cents)
        .unwrap_or_else(|| snapshot.daily[0].opening_balance_cents);
    points.push((anchor_index as f64, anchor_opening as f64));

    let mut last_week = anchor_week;
    for day in &snapshot.daily {
        let Some(date) = parse_iso_date(&day.date) else {
            continue;
        };
        let week_start = start_of_week(date);
        if week_start <= anchor_week || week_start == last_week {
            continue;
        }
        let weeks_out = week_start
            .signed_duration_since(anchor_week)
            .num_days()
            .div_euclid(7);
        points.push((
            (anchor_index as i64 + weeks_out) as f64,
            day.opening_balance_cents as f64,
        ));
        last_week = week_start;
    }

    points
}

fn chart_first_week_start(
    actual_history: &[crate::model::WeeklyBalancePoint],
    baseline: &crate::model::ForecastSnapshot,
    forecast: &crate::model::ForecastSnapshot,
) -> Option<NaiveDate> {
    actual_history
        .first()
        .and_then(|point| parse_iso_date(&point.week_start))
        .or_else(|| {
            baseline
                .daily
                .first()
                .and_then(|point| parse_iso_date(&point.date))
                .map(start_of_week)
        })
        .or_else(|| {
            forecast
                .daily
                .first()
                .and_then(|point| parse_iso_date(&point.date))
                .map(start_of_week)
        })
}

fn planning_chart_x_labels(first_week_start: NaiveDate, max_index: usize) -> Vec<Line<'static>> {
    let mut positions = vec![0_usize, max_index / 3, (max_index * 2) / 3, max_index];
    positions.sort_unstable();
    positions.dedup();
    positions
        .into_iter()
        .map(|index| {
            let date = first_week_start + Duration::weeks(index as i64);
            Line::from(Span::styled(
                format_week_label(date),
                tone_style(Tone::Muted),
            ))
        })
        .collect()
}

fn planning_chart_y_labels(min_y: f64, max_y: f64) -> Vec<Line<'static>> {
    let midpoint = min_y + ((max_y - min_y) / 2.0);
    [min_y, midpoint, max_y]
        .into_iter()
        .map(|value| {
            Line::from(Span::styled(
                format_cents(value.round() as i64),
                tone_style(Tone::Muted),
            ))
        })
        .collect()
}

fn format_week_label(date: NaiveDate) -> String {
    date.format("%d %b").to_string().to_uppercase()
}

fn start_of_week(date: NaiveDate) -> NaiveDate {
    date - Duration::days(date.weekday().num_days_from_monday() as i64)
}

fn parse_iso_date(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn lowest_forecast_balance(snapshot: &crate::model::ForecastSnapshot) -> i64 {
    snapshot
        .daily
        .iter()
        .map(|point| point.closing_balance_cents)
        .min()
        .unwrap_or(0)
}

fn highest_forecast_balance(snapshot: &crate::model::ForecastSnapshot) -> i64 {
    snapshot
        .daily
        .iter()
        .map(|point| point.closing_balance_cents)
        .max()
        .unwrap_or(0)
}

fn first_negative_forecast_date(snapshot: &crate::model::ForecastSnapshot) -> Option<String> {
    snapshot
        .daily
        .iter()
        .find(|point| point.closing_balance_cents < 0)
        .map(|point| point.date.clone())
}

fn scaled_width(value: i64, max_value: i64, width: usize) -> usize {
    if value <= 0 || max_value <= 0 || width == 0 {
        return 0;
    }
    let ratio = value as f64 / max_value as f64;
    let filled = (ratio * width as f64).round() as usize;
    filled.clamp(1, width)
}

fn ascii_bar(filled: usize, width: usize, fill: char) -> String {
    let filled = filled.min(width);
    let mut bar = String::with_capacity(width);
    for _ in 0..filled {
        bar.push(fill);
    }
    for _ in filled..width {
        bar.push('.');
    }
    bar
}

fn budget_progress_bar(spent: i64, budget: i64, width: usize) -> String {
    if budget <= 0 {
        return if spent > 0 {
            ascii_bar(width, width, '!')
        } else {
            ascii_bar(0, width, '#')
        };
    }

    let capped = spent.min(budget);
    let filled = scaled_width(capped, budget, width);
    let fill = if spent > budget { '!' } else { '#' };
    ascii_bar(filled, width, fill)
}

fn short_month(month: &str) -> String {
    let month_num = month
        .split('-')
        .nth(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let names = [
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    ];
    names
        .get(month_num.saturating_sub(1))
        .unwrap_or(&"UNK")
        .to_string()
}

fn filter_value(value: Option<&str>, max: usize) -> String {
    match value {
        Some(text) => truncate_label(text, max),
        None => "ANY".to_string(),
    }
}
fn truncate_label(label: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }

    let trimmed = label.trim();
    let count = trimmed.chars().count();
    if count <= max {
        return trimmed.to_string();
    }

    let mut output = String::with_capacity(max);
    for (index, ch) in trimmed.chars().enumerate() {
        if index + 1 >= max {
            break;
        }
        output.push(ch);
    }
    output.push('~');
    output
}

#[cfg(test)]
mod tests {
    use crate::model::{
        BillCalendarItem, ForecastDailyPoint, ForecastMonthlyPoint, ForecastSelection,
        ForecastSnapshot, GoalStatusRecord, PlanningGoalKind, TransactionKind, WeeklyBalancePoint,
    };

    use super::{
        first_negative_forecast_date, highest_forecast_balance, lowest_forecast_balance,
        planning_chart_data, planning_weekly_projection_points,
    };

    fn sample_snapshot() -> ForecastSnapshot {
        ForecastSnapshot {
            scenario: ForecastSelection {
                id: None,
                name: Some("baseline".to_string()),
            },
            as_of: "2026-03-16".to_string(),
            account: ForecastSelection {
                id: None,
                name: None,
            },
            warnings: Vec::new(),
            alerts: Vec::new(),
            daily: vec![
                ForecastDailyPoint {
                    date: "2026-03-16".to_string(),
                    opening_balance_cents: 1000,
                    inflow_cents: 0,
                    outflow_cents: 200,
                    net_cents: -200,
                    closing_balance_cents: 800,
                    alerts: Vec::new(),
                },
                ForecastDailyPoint {
                    date: "2026-03-17".to_string(),
                    opening_balance_cents: 800,
                    inflow_cents: 0,
                    outflow_cents: 1000,
                    net_cents: -1000,
                    closing_balance_cents: -200,
                    alerts: vec!["warning".to_string()],
                },
            ],
            monthly: vec![ForecastMonthlyPoint {
                month: "2026-03".to_string(),
                inflow_cents: 0,
                outflow_cents: 1200,
                net_cents: -1200,
                ending_balance_cents: -200,
            }],
            goal_status: vec![GoalStatusRecord {
                id: 1,
                name: "Buffer".to_string(),
                kind: PlanningGoalKind::BalanceTarget,
                account_id: 1,
                account_name: "Checking".to_string(),
                target_amount_cents: None,
                minimum_balance_cents: Some(0),
                due_on: None,
                current_balance_cents: 800,
                projected_balance_cents: -200,
                remaining_cents: 0,
                suggested_monthly_contribution_cents: 0,
                on_track: false,
                breach_date: Some("2026-03-17".to_string()),
            }],
            bill_calendar: vec![BillCalendarItem {
                date: "2026-03-20".to_string(),
                title: "Rent".to_string(),
                source: "planned".to_string(),
                kind: TransactionKind::Expense,
                amount_cents: 50000,
                account_id: 1,
                account_name: "Checking".to_string(),
                category_name: None,
                scenario_name: None,
                linked_transaction_id: None,
            }],
        }
    }

    #[test]
    fn weekly_projection_points_extend_from_actual_history() {
        let actual = vec![
            WeeklyBalancePoint {
                week_start: "2026-03-02".to_string(),
                opening_balance_cents: 1_000,
            },
            WeeklyBalancePoint {
                week_start: "2026-03-09".to_string(),
                opening_balance_cents: 1_500,
            },
        ];
        let snapshot = ForecastSnapshot {
            scenario: ForecastSelection {
                id: None,
                name: Some("baseline".to_string()),
            },
            as_of: "2026-03-16".to_string(),
            account: ForecastSelection {
                id: None,
                name: None,
            },
            warnings: Vec::new(),
            alerts: Vec::new(),
            daily: vec![
                ForecastDailyPoint {
                    date: "2026-03-16".to_string(),
                    opening_balance_cents: 1_200,
                    inflow_cents: 0,
                    outflow_cents: 0,
                    net_cents: 0,
                    closing_balance_cents: 1_200,
                    alerts: Vec::new(),
                },
                ForecastDailyPoint {
                    date: "2026-03-23".to_string(),
                    opening_balance_cents: 1_700,
                    inflow_cents: 0,
                    outflow_cents: 0,
                    net_cents: 0,
                    closing_balance_cents: 1_700,
                    alerts: Vec::new(),
                },
            ],
            monthly: Vec::new(),
            goal_status: Vec::new(),
            bill_calendar: Vec::new(),
        };

        let points = planning_weekly_projection_points(&actual, &snapshot);
        assert_eq!(points, vec![(1.0, 1_500.0), (2.0, 1_200.0), (3.0, 1_700.0)]);
    }

    #[test]
    fn planning_chart_data_builds_axis_ranges() {
        let actual = vec![WeeklyBalancePoint {
            week_start: "2026-03-02".to_string(),
            opening_balance_cents: 1_000,
        }];
        let baseline = sample_snapshot();
        let chart = planning_chart_data(&actual, &baseline, &baseline, false)
            .expect("chart data should exist");

        assert_eq!(chart.actual.first(), Some(&(0.0, 1_000.0)));
        assert!(!chart.baseline.is_empty());
        assert!(chart.x_bounds[1] >= 1.0);
        assert!(chart.y_bounds[0] < 1_000.0);
        assert!(chart.y_bounds[1] > 800.0);
        assert!(chart.x_labels.len() >= 2);
        assert_eq!(chart.y_labels.len(), 3);
    }

    #[test]
    fn forecast_balance_helpers_find_extremes_and_shortfall() {
        let snapshot = sample_snapshot();
        assert_eq!(lowest_forecast_balance(&snapshot), -200);
        assert_eq!(highest_forecast_balance(&snapshot), 800);
        assert_eq!(
            first_negative_forecast_date(&snapshot).as_deref(),
            Some("2026-03-17")
        );
    }
}
