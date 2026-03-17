use std::cmp::Reverse;
use std::env;

use chrono::{DateTime, Datelike, Duration, Local, Weekday as ChronoWeekday};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::Db;
use crate::error::AppError;
use crate::model::{
    MarketBrief, MarketNewsFeed, MarketNewsItem, MarketQuote, MarketRefreshKind,
    MarketRefreshSummary, MarketSettings, WatchlistEntry,
};

const DEFAULT_VALYU_BASE_URL: &str = "https://api.valyu.ai/v1";
const DEFAULT_TAVILY_BASE_URL: &str = "https://api.tavily.com";
const DEFAULT_NEWS_LIMIT: usize = 6;
const QUOTE_TTL_MINUTES: i64 = 5;
const NEWS_TTL_MINUTES: i64 = 15;
const ESTIMATED_VALYU_QUOTE_COST_USD: f64 = 0.08;
const ESTIMATED_TAVILY_NEWS_COST_USD: f64 = 0.016;

pub trait QuoteProvider {
    fn fetch_quote(&self, ticker: &str) -> Result<MarketQuote, AppError>;
}

pub trait NewsProvider {
    fn fetch_news(&self, ticker: &str, limit: usize) -> Result<MarketNewsFeed, AppError>;
}

pub struct MarketService<Q: QuoteProvider, N: NewsProvider> {
    quote_provider: Q,
    news_provider: N,
}

impl<Q: QuoteProvider, N: NewsProvider> MarketService<Q, N> {
    pub fn new(quote_provider: Q, news_provider: N) -> Self {
        Self {
            quote_provider,
            news_provider,
        }
    }

    pub fn add_watchlist(
        &self,
        db: &Db,
        ticker: &str,
        label: Option<&str>,
    ) -> Result<i64, AppError> {
        db.add_watchlist_item(&normalize_ticker(ticker)?, label)
    }

    pub fn list_watchlist(&self, db: &Db) -> Result<Vec<WatchlistEntry>, AppError> {
        db.list_watchlist_items()?
            .into_iter()
            .map(|item| {
                Ok(WatchlistEntry {
                    id: item.id,
                    ticker: item.ticker.clone(),
                    label: item.label,
                    added_at: item.added_at,
                    quote: db.cached_quote(&item.ticker)?,
                })
            })
            .collect()
    }

    pub fn remove_watchlist(&self, db: &Db, ticker: &str) -> Result<(), AppError> {
        db.remove_watchlist_item(&normalize_ticker(ticker)?)
    }

    pub fn market_settings(&self, db: &Db) -> Result<MarketSettings, AppError> {
        db.market_settings()
    }

    pub fn update_market_settings(
        &self,
        db: &Db,
        quote_refresh_hours: Option<i64>,
        news_refresh_hours: Option<i64>,
        auto_refresh_quotes: Option<bool>,
        auto_refresh_news: Option<bool>,
        weekday_only: Option<bool>,
        max_quote_cost_usd: Option<f64>,
    ) -> Result<MarketSettings, AppError> {
        db.update_market_settings(
            quote_refresh_hours,
            news_refresh_hours,
            auto_refresh_quotes,
            auto_refresh_news,
            weekday_only,
            max_quote_cost_usd,
        )
    }

    pub fn quote(&self, db: &Db, ticker: &str, cached_only: bool) -> Result<MarketQuote, AppError> {
        let ticker = normalize_ticker(ticker)?;
        let cached = db.cached_quote(&ticker)?;
        if let Some(quote) = cached.clone() {
            if !quote.stale || cached_only {
                return Ok(quote);
            }
        }
        if cached_only {
            return Err(AppError::NotFound(format!(
                "no cached quote was found for `{ticker}`"
            )));
        }

        self.fetch_quote_live(db, &ticker).or_else(|error| {
            if let Some(mut stale) = cached {
                stale.stale = true;
                Ok(stale)
            } else {
                Err(error)
            }
        })
    }

    pub fn news(
        &self,
        db: &Db,
        ticker: Option<&str>,
        limit: usize,
        cached_only: bool,
    ) -> Result<MarketNewsFeed, AppError> {
        let ticker = self.resolve_news_ticker(db, ticker)?;
        let limit = limit.max(1);
        let cached = db.cached_news(&ticker)?;
        if let Some(mut feed) = cached.clone() {
            if !feed.stale || cached_only {
                feed.items.truncate(limit);
                return Ok(feed);
            }
        }
        if cached_only {
            return Err(AppError::NotFound(format!(
                "no cached news was found for `{ticker}`"
            )));
        }

        self.fetch_news_live(db, &ticker, limit).or_else(|error| {
            if let Some(mut stale) = cached {
                stale.stale = true;
                stale.items.truncate(limit);
                Ok(stale)
            } else {
                Err(error)
            }
        })
    }

    pub fn brief(
        &self,
        db: &Db,
        ticker: Option<&str>,
        limit: usize,
        cached_only: bool,
    ) -> Result<MarketBrief, AppError> {
        let ticker = self.resolve_news_ticker(db, ticker)?;
        let feed = self.news(db, Some(&ticker), limit, cached_only)?;
        let quote = if cached_only {
            db.cached_quote(&ticker)?
        } else {
            match self.quote(db, &ticker, false) {
                Ok(quote) => Some(quote),
                Err(_) => db.cached_quote(&ticker)?,
            }
        };

        Ok(compose_market_brief(quote.as_ref(), &feed))
    }

    pub fn refresh_watchlist(
        &self,
        db: &Db,
        ticker: Option<&str>,
        refresh_all: bool,
        kind: MarketRefreshKind,
    ) -> Result<MarketRefreshSummary, AppError> {
        let tickers = self.resolve_refresh_tickers(db, ticker, refresh_all, true)?;
        let mut summary = new_refresh_summary(kind, tickers.len());

        for ticker in &tickers {
            if kind.includes_quotes() {
                self.fetch_quote_live(db, ticker)?;
                summary.quote_refreshed += 1;
                summary.estimated_cost_usd += ESTIMATED_VALYU_QUOTE_COST_USD;
            }
            if kind.includes_news() {
                self.fetch_news_live(db, ticker, DEFAULT_NEWS_LIMIT)?;
                summary.news_refreshed += 1;
                summary.estimated_cost_usd += ESTIMATED_TAVILY_NEWS_COST_USD;
            }
        }

        Ok(summary)
    }

    pub fn auto_refresh_watchlist(
        &self,
        db: &Db,
        kind: MarketRefreshKind,
    ) -> Result<MarketRefreshSummary, AppError> {
        let settings = db.market_settings()?;
        let tickers = self.resolve_refresh_tickers(db, None, true, false)?;
        let mut summary = new_refresh_summary(kind, tickers.len());

        if tickers.is_empty() {
            return Ok(summary);
        }
        if settings.weekday_only && is_weekend_today() {
            summary.weekend_blocked = true;
            return Ok(summary);
        }

        for ticker in &tickers {
            if kind.includes_quotes() {
                if !settings.auto_refresh_quotes {
                    summary.quote_skipped_disabled += 1;
                } else if !self.quote_refresh_due(db, ticker, settings.quote_refresh_hours)? {
                    summary.quote_skipped_not_due += 1;
                } else if ESTIMATED_VALYU_QUOTE_COST_USD > settings.max_quote_cost_usd {
                    db.record_market_refresh_run(
                        ticker,
                        MarketRefreshKind::Quotes,
                        "valyu",
                        "skipped_cost_cap",
                        0.0,
                        Some("estimated quote cost exceeds max quote cost cap"),
                        &now_timestamp(),
                    )?;
                    summary.quote_skipped_cost_cap += 1;
                } else if self.fetch_quote_live(db, ticker).is_ok() {
                    summary.quote_refreshed += 1;
                    summary.estimated_cost_usd += ESTIMATED_VALYU_QUOTE_COST_USD;
                } else {
                    summary.failure_count += 1;
                }
            }

            if kind.includes_news() {
                if !settings.auto_refresh_news {
                    summary.news_skipped_disabled += 1;
                } else if !self.news_refresh_due(db, ticker, settings.news_refresh_hours)? {
                    summary.news_skipped_not_due += 1;
                } else if self.fetch_news_live(db, ticker, DEFAULT_NEWS_LIMIT).is_ok() {
                    summary.news_refreshed += 1;
                    summary.estimated_cost_usd += ESTIMATED_TAVILY_NEWS_COST_USD;
                } else {
                    summary.failure_count += 1;
                }
            }
        }

        Ok(summary)
    }

    fn fetch_quote_live(&self, db: &Db, ticker: &str) -> Result<MarketQuote, AppError> {
        let mut quote = self.quote_provider.fetch_quote(ticker)?;
        quote.ticker = ticker.to_string();
        quote.fetched_at = now_timestamp();
        quote.stale = false;
        db.store_quote_cache(&quote, &expiry_timestamp(QUOTE_TTL_MINUTES))?;
        db.record_market_refresh_run(
            ticker,
            MarketRefreshKind::Quotes,
            "valyu",
            "fetched",
            ESTIMATED_VALYU_QUOTE_COST_USD,
            None,
            &quote.fetched_at,
        )?;
        Ok(quote)
    }

    fn fetch_news_live(
        &self,
        db: &Db,
        ticker: &str,
        limit: usize,
    ) -> Result<MarketNewsFeed, AppError> {
        let mut feed = self.news_provider.fetch_news(ticker, limit.max(1))?;
        feed.ticker = ticker.to_string();
        feed.fetched_at = now_timestamp();
        feed.stale = false;
        if feed.items.len() > limit {
            feed.items.truncate(limit);
        }
        db.store_news_cache(&feed, &expiry_timestamp(NEWS_TTL_MINUTES))?;
        db.record_market_refresh_run(
            ticker,
            MarketRefreshKind::News,
            "tavily",
            "fetched",
            ESTIMATED_TAVILY_NEWS_COST_USD,
            None,
            &feed.fetched_at,
        )?;
        Ok(feed)
    }

    fn quote_refresh_due(&self, db: &Db, ticker: &str, hours: i64) -> Result<bool, AppError> {
        match db.cached_quote(ticker)? {
            Some(quote) => refresh_due_from_timestamp(&quote.fetched_at, hours),
            None => Ok(true),
        }
    }

    fn news_refresh_due(&self, db: &Db, ticker: &str, hours: i64) -> Result<bool, AppError> {
        match db.cached_news(ticker)? {
            Some(feed) => refresh_due_from_timestamp(&feed.fetched_at, hours),
            None => Ok(true),
        }
    }

    fn resolve_refresh_tickers(
        &self,
        db: &Db,
        ticker: Option<&str>,
        refresh_all: bool,
        error_on_empty: bool,
    ) -> Result<Vec<String>, AppError> {
        if refresh_all {
            let all = db.list_watchlist_items()?;
            if all.is_empty() {
                if error_on_empty {
                    return Err(AppError::Validation(
                        "watchlist is empty; add a ticker first".to_string(),
                    ));
                }
                return Ok(Vec::new());
            }
            return Ok(all.into_iter().map(|item| item.ticker).collect::<Vec<_>>());
        }

        match ticker {
            Some(value) => Ok(vec![normalize_ticker(value)?]),
            None => Ok(vec![db.single_watchlist_ticker()?]),
        }
    }

    fn resolve_news_ticker(&self, db: &Db, ticker: Option<&str>) -> Result<String, AppError> {
        match ticker {
            Some(value) => normalize_ticker(value),
            None => db.single_watchlist_ticker(),
        }
    }
}

impl MarketService<ValyuClient, TavilyClient> {
    pub fn from_env() -> Self {
        Self::new(ValyuClient::from_env(), TavilyClient::from_env())
    }
}

pub struct ValyuClient {
    api_key: Option<String>,
    base_url: String,
}

impl ValyuClient {
    pub fn from_env() -> Self {
        Self {
            api_key: env::var("HELIUS_VALYU_API_KEY").ok(),
            base_url: env::var("HELIUS_VALYU_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_VALYU_BASE_URL.to_string()),
        }
    }
}

impl QuoteProvider for ValyuClient {
    fn fetch_quote(&self, ticker: &str) -> Result<MarketQuote, AppError> {
        let api_key = self.api_key.as_deref().ok_or_else(|| {
            AppError::Config("missing HELIUS_VALYU_API_KEY environment variable".to_string())
        })?;
        let url = format!("{}/answer", self.base_url.trim_end_matches('/'));
        let body = json!({
            "query": format!(
                "Provide a stock quote snapshot for {ticker}. Return the primary listed equity only. Include name, exchange, currency, change, change_percent, and market_state when available, and omit any field you cannot determine."
            ),
            "included_sources": ["finance"],
            "structured_output": {
                "type": "object",
                "properties": {
                    "ticker": { "type": "string" },
                    "name": { "type": "string" },
                    "exchange": { "type": "string" },
                    "currency": { "type": "string" },
                    "last_price": { "type": "number" },
                    "change": { "type": "number" },
                    "change_percent": { "type": "number" },
                    "market_state": { "type": "string" }
                },
                "required": [
                    "ticker",
                    "last_price"
                ]
            }
        });
        let response = ureq::post(&url)
            .set("Content-Type", "application/json")
            .set("X-API-Key", api_key)
            .send_json(body)
            .map_err(map_ureq_error)?;
        let body = response
            .into_string()
            .map_err(|error| AppError::Http(error.to_string()))?;
        parse_valyu_quote_response(&body, ticker)
    }
}

pub struct TavilyClient {
    api_key: Option<String>,
    base_url: String,
}

impl TavilyClient {
    pub fn from_env() -> Self {
        Self {
            api_key: env::var("HELIUS_TAVILY_API_KEY").ok(),
            base_url: env::var("HELIUS_TAVILY_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_TAVILY_BASE_URL.to_string()),
        }
    }
}

impl NewsProvider for TavilyClient {
    fn fetch_news(&self, ticker: &str, limit: usize) -> Result<MarketNewsFeed, AppError> {
        let api_key = self.api_key.as_deref().ok_or_else(|| {
            AppError::Config("missing HELIUS_TAVILY_API_KEY environment variable".to_string())
        })?;
        let url = format!("{}/search", self.base_url.trim_end_matches('/'));
        let response = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_json(json!({
                "api_key": api_key,
                "query": format!("{ticker} stock market news"),
                "topic": "finance",
                "search_depth": "advanced",
                "max_results": limit.max(1),
                "include_answer": false,
                "include_raw_content": false,
            }))
            .map_err(map_ureq_error)?;
        let body = response
            .into_string()
            .map_err(|error| AppError::Http(error.to_string()))?;
        parse_tavily_news_response(&body, ticker)
    }
}

#[derive(Debug, Deserialize)]
struct TavilyResponse {
    results: Vec<TavilyResult>,
}

#[derive(Debug, Deserialize)]
struct TavilyResult {
    title: String,
    url: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    published_date: Option<String>,
    #[serde(default)]
    published_at: Option<String>,
    #[serde(default)]
    source: Option<String>,
}

fn new_refresh_summary(kind: MarketRefreshKind, tickers_considered: usize) -> MarketRefreshSummary {
    MarketRefreshSummary {
        requested_kind: kind,
        tickers_considered,
        quote_refreshed: 0,
        news_refreshed: 0,
        quote_skipped_not_due: 0,
        news_skipped_not_due: 0,
        quote_skipped_disabled: 0,
        news_skipped_disabled: 0,
        quote_skipped_cost_cap: 0,
        weekend_blocked: false,
        failure_count: 0,
        estimated_cost_usd: 0.0,
        ran_at: now_timestamp(),
    }
}

fn refresh_due_from_timestamp(fetched_at: &str, hours: i64) -> Result<bool, AppError> {
    let fetched_at = DateTime::parse_from_rfc3339(fetched_at)?.with_timezone(&Local);
    Ok(fetched_at + Duration::hours(hours) <= Local::now())
}

fn is_weekend_today() -> bool {
    matches!(
        Local::now().weekday(),
        ChronoWeekday::Sat | ChronoWeekday::Sun
    )
}

pub fn normalize_ticker(raw: &str) -> Result<String, AppError> {
    let normalized = raw.trim().to_ascii_uppercase();
    let is_valid = !normalized.is_empty()
        && normalized.len() <= 15
        && normalized
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '-');
    if !is_valid {
        return Err(AppError::Validation(
            "ticker must use only letters, numbers, '.' or '-'".to_string(),
        ));
    }
    Ok(normalized)
}

fn parse_valyu_quote_response(body: &str, requested_ticker: &str) -> Result<MarketQuote, AppError> {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        if let Some(structured) = value
            .get("structured_output")
            .or_else(|| value.pointer("/data/structured_output"))
        {
            return quote_from_value(structured, requested_ticker);
        }
    }

    let mut collected_json = String::new();
    for line in body.lines() {
        let line = line.trim();
        if !line.starts_with("data:") {
            continue;
        }
        let payload = line.trim_start_matches("data:").trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            if let Some(structured) = value
                .get("structured_output")
                .or_else(|| value.pointer("/data/structured_output"))
            {
                return quote_from_value(structured, requested_ticker);
            }
            if let Some(delta) = value
                .get("content")
                .and_then(Value::as_str)
                .or_else(|| value.get("delta").and_then(Value::as_str))
                .or_else(|| value.pointer("/data/content").and_then(Value::as_str))
                .or_else(|| {
                    value
                        .pointer("/choices/0/delta/content")
                        .and_then(Value::as_str)
                })
                .or_else(|| {
                    value
                        .pointer("/data/choices/0/delta/content")
                        .and_then(Value::as_str)
                })
            {
                collected_json.push_str(delta);
            }
        }
    }

    if !collected_json.trim().is_empty() {
        let parsed = serde_json::from_str::<Value>(&collected_json)
            .map_err(|error| AppError::Http(format!("invalid Valyu structured output: {error}")))?;
        return quote_from_value(&parsed, requested_ticker);
    }

    Err(AppError::Http(
        "Valyu returned an unexpected response format".to_string(),
    ))
}

fn quote_from_value(value: &Value, requested_ticker: &str) -> Result<MarketQuote, AppError> {
    Ok(MarketQuote {
        ticker: value
            .get("ticker")
            .and_then(Value::as_str)
            .unwrap_or(requested_ticker)
            .trim()
            .to_ascii_uppercase(),
        name: optional_string(value, "name").unwrap_or_else(|| requested_ticker.to_string()),
        exchange: optional_string(value, "exchange").map(|value| value.to_ascii_uppercase()),
        currency: optional_string(value, "currency").map(|value| value.to_ascii_uppercase()),
        last_price: required_number(value, "last_price")?,
        change: optional_number(value, "change"),
        change_percent: optional_number(value, "change_percent"),
        market_state: optional_string(value, "market_state"),
        fetched_at: String::new(),
        stale: false,
    })
}

fn parse_tavily_news_response(body: &str, ticker: &str) -> Result<MarketNewsFeed, AppError> {
    let parsed = serde_json::from_str::<TavilyResponse>(body)?;
    Ok(MarketNewsFeed {
        ticker: ticker.to_string(),
        fetched_at: String::new(),
        stale: false,
        items: parsed
            .results
            .into_iter()
            .map(|item| MarketNewsItem {
                title: item.title,
                source: item
                    .source
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| host_from_url(&item.url)),
                url: item.url,
                published_at: item.published_at.or(item.published_date),
                summary: item.content.trim().to_string(),
                ticker: ticker.to_string(),
            })
            .collect(),
    })
}

pub fn compose_market_brief(quote: Option<&MarketQuote>, feed: &MarketNewsFeed) -> MarketBrief {
    let display_name = quote
        .map(|item| item.name.as_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(feed.ticker.as_str());
    let mut notable_items = feed
        .items
        .iter()
        .filter(|item| !is_generic_market_item(item, &feed.ticker))
        .collect::<Vec<_>>();
    notable_items.sort_by_key(|item| {
        Reverse(rank_market_item(
            item,
            &feed.ticker,
            quote.map(|entry| entry.name.as_str()),
        ))
    });
    let lead_context = notable_items
        .iter()
        .find_map(|item| lead_article_context(item));
    let theme_labels = collect_brief_themes(&notable_items);
    let summary = build_brief_summary(
        display_name,
        feed,
        &notable_items,
        theme_labels.as_slice(),
        lead_context.as_deref(),
    );
    let tone = determine_brief_tone(quote, &notable_items, theme_labels.len());
    let why_it_matters = build_why_it_matters(
        quote,
        &notable_items,
        theme_labels.as_slice(),
        lead_context.as_deref(),
    );
    let watch_items = build_watch_items(quote, feed, &notable_items, &theme_labels);
    let source_count = feed
        .items
        .iter()
        .map(|item| item.source.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let fetched_at = if !feed.fetched_at.trim().is_empty() {
        feed.fetched_at.clone()
    } else {
        quote
            .map(|item| item.fetched_at.clone())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(now_timestamp)
    };

    MarketBrief {
        ticker: feed.ticker.clone(),
        summary,
        tone,
        why_it_matters,
        watch_items,
        headline_count: feed.items.len(),
        source_count,
        fetched_at,
        stale: feed.stale || quote.map(|item| item.stale).unwrap_or(false),
    }
}

fn build_brief_summary(
    display_name: &str,
    feed: &MarketNewsFeed,
    notable_items: &[&MarketNewsItem],
    theme_labels: &[&'static str],
    lead_context: Option<&str>,
) -> String {
    if feed.items.is_empty() {
        return format!("No recent articles were cached for {} yet.", display_name);
    }

    if notable_items.is_empty() {
        return format!(
            "Recent coverage for {} is mostly generic stock-overview content, so the fetched articles do not point to one fresh catalyst yet.",
            display_name
        );
    }

    let fallback = format!(
        "The most visible article title is {}.",
        shorten_market_text(&notable_items[0].title, 96)
    );
    let lead_sentence = lead_context
        .map(|context| format!("One article argues {}", ensure_sentence(context)))
        .unwrap_or(fallback);

    match theme_labels {
        [] => format!(
            "Recent coverage for {} is concentrated around one developing angle. {}",
            display_name, lead_sentence
        ),
        [theme] => format!(
            "Recent coverage for {} is centered on {}. {}",
            display_name,
            theme.to_ascii_lowercase(),
            lead_sentence
        ),
        [first, second, ..] => format!(
            "Recent coverage for {} is centered on {} and {}. {}",
            display_name,
            first.to_ascii_lowercase(),
            second.to_ascii_lowercase(),
            lead_sentence
        ),
    }
}

fn build_why_it_matters(
    quote: Option<&MarketQuote>,
    notable_items: &[&MarketNewsItem],
    theme_labels: &[&'static str],
    lead_context: Option<&str>,
) -> Vec<String> {
    let mut lines = Vec::new();

    if let Some(context) = lead_context {
        lines.push(format!(
            "The article context is this: {}",
            ensure_sentence(context)
        ));
    }

    for theme in theme_labels {
        lines.push(match *theme {
            "Earnings" => "Results and guidance can reset revenue, margin, and valuation expectations.".to_string(),
            "Analyst sentiment" => "Analyst calls often shape short-term sentiment, target-price discussion, and revision momentum.".to_string(),
            "Product and platform" => "Product or platform headlines affect demand expectations and the strength of the broader ecosystem story.".to_string(),
            "Regulation and legal" => "Regulatory and legal headlines can pressure multiples because they add uncertainty to the operating outlook.".to_string(),
            "Deals and partnerships" => "Partnership or deal headlines matter when they change the growth narrative or open a new distribution channel.".to_string(),
            "Capital returns" => "Capital-return headlines matter because buybacks and dividends can support sentiment even when operating news is quiet.".to_string(),
            "Supply chain" => "Supply-chain headlines matter when they affect product timing, delivery capacity, or gross margins.".to_string(),
            _ => "The current articles are more informational than decisive, so confirmation from follow-up coverage still matters.".to_string(),
        });
    }

    if let Some(change_percent) = quote.and_then(|item| item.change_percent) {
        if change_percent.abs() >= 2.0 {
            lines.push(format!(
                "The latest cached move is {:+.2}%, which suggests the market is reacting meaningfully rather than treating this as background noise.",
                change_percent
            ));
        }
    }

    if lines.is_empty() {
        if notable_items.is_empty() {
            lines.push("The fetched links look more like stock-overview pages than event-driven reporting, so there is no single market-moving takeaway yet.".to_string());
        } else {
            lines.push("The current mix of stories looks incremental rather than thesis-changing, so follow-up reporting matters more than the first headline.".to_string());
        }
    }

    lines.truncate(3);
    lines
}

fn build_watch_items(
    quote: Option<&MarketQuote>,
    feed: &MarketNewsFeed,
    notable_items: &[&MarketNewsItem],
    theme_labels: &[&'static str],
) -> Vec<String> {
    let mut lines = Vec::new();

    if feed.stale {
        lines.push("Refresh the feed before acting, because the current brief is based on stale cached articles.".to_string());
    }

    if notable_items.is_empty() {
        lines.push("Watch for a concrete catalyst such as earnings, guidance, an analyst revision, or a regulatory update before reading too much into the coverage.".to_string());
    }

    for theme in theme_labels {
        let line = match *theme {
            "Earnings" => Some("Watch management guidance, estimate revisions, and whether follow-up coverage focuses on demand or margins."),
            "Analyst sentiment" => Some("Watch whether upgrades or downgrades broaden across firms instead of staying isolated to one note."),
            "Product and platform" => Some("Watch launch reception, demand signals, and any margin commentary tied to the product cycle."),
            "Regulation and legal" => Some("Watch for filings, official statements, or deadlines that turn headline risk into something measurable."),
            "Deals and partnerships" => Some("Watch whether management quantifies revenue impact, timing, or strategic importance."),
            "Capital returns" => Some("Watch whether capital-return headlines are accompanied by operating updates or remain the main support for sentiment."),
            "Supply chain" => Some("Watch for confirmation on production timing, inventory, and any knock-on effect for margins."),
            _ => None,
        };
        if let Some(line) = line {
            lines.push(line.to_string());
        }
    }

    if let Some(change_percent) = quote.and_then(|item| item.change_percent) {
        if change_percent.abs() >= 3.0 {
            lines.push(format!(
                "Watch for follow-through after the current {:+.2}% move; sharp first reactions can reverse if fresh details do not confirm the narrative.",
                change_percent
            ));
        }
    }

    if lines.is_empty() {
        lines.push("Watch whether multiple outlets keep repeating the same story or begin adding genuinely new facts.".to_string());
    }

    lines.truncate(3);
    lines
}

fn rank_market_item(item: &MarketNewsItem, ticker: &str, company_name: Option<&str>) -> i32 {
    let title = normalize_market_text(&item.title).to_ascii_lowercase();
    let summary = normalize_market_text(&item.summary).to_ascii_lowercase();
    let url = item.url.to_ascii_lowercase();
    let ticker_lower = ticker.to_ascii_lowercase();
    let company_lower = company_name.unwrap_or("").trim().to_ascii_lowercase();
    let combined = format!("{} {}", title, summary);
    let mut score = 0;

    if item.published_at.is_some() {
        score += 12;
    }
    if summary.len() >= 80 {
        score += 10;
    }
    if summary.len() >= 180 {
        score += 8;
    }
    if url.contains("/news/") {
        score += 24;
    }
    if url.contains("/article/") || url.contains("articles") {
        score += 18;
    }
    if contains_any(
        &combined,
        &[
            "earnings",
            "guidance",
            "forecast",
            "upgrade",
            "downgrade",
            "price target",
            "launch",
            "partnership",
            "deal",
            "invest",
            "buyback",
            "dividend",
            "lawsuit",
            "regulator",
            "chip",
            "ai",
            "demand",
            "supply",
            "margin",
            "revenue",
            "profit",
        ],
    ) {
        score += 18;
    }
    if combined.contains(&ticker_lower) {
        score += 8;
    }
    if !company_lower.is_empty() && combined.contains(&company_lower) {
        score += 8;
    }

    if is_broad_market_roundup(&title, &summary, &ticker_lower, &company_lower) {
        score -= 28;
    }
    if is_generic_market_url(&item.url) {
        score -= 35;
    }
    if contains_any(
        &title,
        &[
            "stock price",
            "quote & history",
            "latest stock news",
            "latest stock price",
            "financial analysis",
            "stock price today",
        ],
    ) {
        score -= 40;
    }
    if summary.starts_with("view the latest") {
        score -= 25;
    }
    if summary.contains("historical charts")
        || summary.contains("analyst ratings and financial information")
    {
        score -= 20;
    }

    score
}

fn is_broad_market_roundup(title: &str, summary: &str, ticker: &str, company_name: &str) -> bool {
    let combined = format!("{} {}", title, summary);
    let mentions_ticker =
        combined.contains(ticker) || (!company_name.is_empty() && combined.contains(company_name));
    contains_any(
        &combined,
        &[
            "stock market today",
            "dow, s&p 500, nasdaq",
            "wall street",
            "market today",
            "live coverage",
            "futures",
            "indexes",
        ],
    ) && !mentions_ticker
}
fn is_generic_market_url(url: &str) -> bool {
    let url = url.trim().to_ascii_lowercase();
    url.contains("/quote/") || url.contains("market-data/quotes") || url.contains("/latest-news/")
}
fn lead_article_context(item: &MarketNewsItem) -> Option<String> {
    let title = normalize_market_text(&item.title);
    let summary = normalize_market_text(&item.summary);
    if summary.is_empty() {
        return None;
    }

    split_market_sentences(&summary)
        .into_iter()
        .map(|sentence| sentence.trim().to_string())
        .filter(|sentence| !sentence.is_empty())
        .find(|sentence| !is_generic_context_sentence(sentence, &title))
        .map(|sentence| shorten_market_text(&sentence, 180))
}

fn normalize_market_text(text: &str) -> String {
    text.replace("###", " ")
        .replace('#', " ")
        .replace('\n', " ")
        .replace('\r', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_market_sentences(text: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?') {
            let sentence = current.trim();
            if !sentence.is_empty() {
                sentences.push(sentence.to_string());
            }
            current.clear();
        }
    }

    let trailing = current.trim();
    if !trailing.is_empty() {
        sentences.push(trailing.to_string());
    }

    sentences
}

fn is_generic_context_sentence(sentence: &str, title: &str) -> bool {
    let lowered = sentence.trim().to_ascii_lowercase();
    let normalized_title = title.trim().to_ascii_lowercase();
    let comparable_sentence = lowered
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || ch.is_ascii_whitespace())
        .collect::<String>();
    let comparable_title = normalized_title
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || ch.is_ascii_whitespace())
        .collect::<String>();
    lowered.is_empty()
        || comparable_sentence == comparable_title
        || comparable_title.contains(comparable_sentence.trim())
        || comparable_sentence.contains(comparable_title.trim())
        || lowered.len() < 28
        || lowered.starts_with("view the latest")
        || lowered.starts_with("find the latest")
        || lowered.starts_with("news headlines")
        || lowered.contains("stock price, news, quote & history")
        || lowered.contains("stock price, news, quote and history")
        || lowered.contains("stock quote, history, news")
        || lowered.contains("latest stock news & headlines")
        || lowered.contains("historical charts")
        || lowered.contains("analyst ratings and financial information")
        || lowered.contains("stock trading and investing")
        || lowered.contains("other vital information")
        || lowered.contains("my portfolio")
        || lowered.contains("watch now")
}

fn is_generic_market_item(item: &MarketNewsItem, ticker: &str) -> bool {
    is_generic_market_url(&item.url)
        || is_generic_market_page(&item.title, ticker)
        || is_generic_context_sentence(
            &normalize_market_text(&item.summary),
            &normalize_market_text(&item.title),
        )
}

fn ensure_sentence(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.ends_with('.') || trimmed.ends_with('!') || trimmed.ends_with('?') {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
    }
}
fn determine_brief_tone(
    quote: Option<&MarketQuote>,
    notable_items: &[&MarketNewsItem],
    theme_count: usize,
) -> String {
    let mut score = 0;
    let mut positive = false;
    let mut negative = false;

    for item in notable_items.iter().take(4) {
        let text = format!("{} {}", item.title, item.summary).to_ascii_lowercase();
        if contains_any(
            &text,
            &[
                "beat",
                "upgrade",
                "buyback",
                "partnership",
                "launch",
                "growth",
                "record",
                "surge",
                "strong",
            ],
        ) {
            score += 1;
            positive = true;
        }
        if contains_any(
            &text,
            &[
                "miss",
                "downgrade",
                "lawsuit",
                "probe",
                "antitrust",
                "delay",
                "weak",
                "drop",
                "risk",
                "cut",
                "recall",
            ],
        ) {
            score -= 1;
            negative = true;
        }
    }

    if let Some(change_percent) = quote.and_then(|item| item.change_percent) {
        if change_percent >= 1.5 {
            score += 1;
            positive = true;
        } else if change_percent <= -1.5 {
            score -= 1;
            negative = true;
        }
    }

    if positive && negative {
        "mixed".to_string()
    } else if score >= 2 {
        "bullish".to_string()
    } else if score <= -2 {
        "bearish".to_string()
    } else if theme_count > 1 {
        "mixed".to_string()
    } else {
        "neutral".to_string()
    }
}

fn collect_brief_themes(items: &[&MarketNewsItem]) -> Vec<&'static str> {
    let mut themes = Vec::new();
    for item in items.iter().take(5) {
        let text = format!("{} {}", item.title, item.summary).to_ascii_lowercase();
        let theme = if contains_any(&text, &["earnings", "revenue", "guidance", "eps", "profit"]) {
            Some("Earnings")
        } else if contains_any(
            &text,
            &["analyst", "upgrade", "downgrade", "price target", "rating"],
        ) {
            Some("Analyst sentiment")
        } else if contains_any(
            &text,
            &[
                "launch", "iphone", "chip", "product", "service", "platform", "ai", "device",
            ],
        ) {
            Some("Product and platform")
        } else if contains_any(
            &text,
            &[
                "lawsuit",
                "regulator",
                "regulatory",
                "antitrust",
                "probe",
                "sec",
                "court",
            ],
        ) {
            Some("Regulation and legal")
        } else if contains_any(
            &text,
            &[
                "partnership",
                "partner",
                "deal",
                "acquisition",
                "merger",
                "investment",
            ],
        ) {
            Some("Deals and partnerships")
        } else if contains_any(
            &text,
            &["buyback", "dividend", "capital return", "repurchase"],
        ) {
            Some("Capital returns")
        } else if contains_any(
            &text,
            &[
                "supply chain",
                "supplier",
                "manufacturing",
                "factory",
                "shipment",
                "inventory",
            ],
        ) {
            Some("Supply chain")
        } else {
            None
        };

        if let Some(theme) = theme {
            if !themes.contains(&theme) {
                themes.push(theme);
            }
        }
    }
    themes
}

fn is_generic_market_page(title: &str, ticker: &str) -> bool {
    let text = title.trim().to_ascii_lowercase();
    let ticker = ticker.to_ascii_lowercase();
    text.contains("latest stock news")
        || text.contains("latest stock news & headlines")
        || text.contains("stock price today")
        || text.contains("stock price, news, quote & history")
        || text.contains("stock price, news, quote and history")
        || text.contains("news, quote & history")
        || text.contains("news, quote and history")
        || text.contains("latest stock price")
        || text.contains("financial analysis")
        || text.contains("quote/")
        || text == format!("{} stock", ticker)
        || (text.contains(&ticker) && text.contains("yahoo finance") && text.contains("headlines"))
}

fn shorten_market_text(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        compact
    } else {
        let shortened = compact
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        format!("{}...", shortened.trim_end())
    }
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}
fn required_number(value: &Value, key: &str) -> Result<f64, AppError> {
    optional_number(value, key)
        .ok_or_else(|| AppError::Http(format!("Valyu response missing `{key}`")))
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
}

fn optional_number(value: &Value, key: &str) -> Option<f64> {
    match value.get(key) {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(text)) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn host_from_url(url: &str) -> String {
    let without_scheme = url
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    without_scheme
        .split('/')
        .next()
        .unwrap_or("unknown")
        .to_string()
}

fn map_ureq_error(error: ureq::Error) -> AppError {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            AppError::Http(format!("provider request failed with HTTP {code}: {body}"))
        }
        ureq::Error::Transport(error) => AppError::Http(error.to_string()),
    }
}

fn now_timestamp() -> String {
    Local::now().to_rfc3339()
}

fn expiry_timestamp(minutes: i64) -> String {
    (Local::now() + Duration::minutes(minutes)).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{
        compose_market_brief, lead_article_context, normalize_ticker, parse_tavily_news_response,
        parse_valyu_quote_response, rank_market_item, MarketService, NewsProvider, QuoteProvider,
    };
    use crate::db::Db;
    use crate::model::{MarketNewsFeed, MarketNewsItem, MarketQuote};

    #[derive(Clone)]
    struct MockQuoteProvider {
        quote: Option<MarketQuote>,
        error: Option<String>,
    }

    impl QuoteProvider for MockQuoteProvider {
        fn fetch_quote(&self, _ticker: &str) -> Result<MarketQuote, crate::error::AppError> {
            match (&self.quote, &self.error) {
                (Some(quote), _) => Ok(quote.clone()),
                (_, Some(message)) => Err(crate::error::AppError::Http(message.clone())),
                _ => unreachable!(),
            }
        }
    }

    #[derive(Clone)]
    struct MockNewsProvider {
        feed: Option<MarketNewsFeed>,
        error: Option<String>,
    }

    impl NewsProvider for MockNewsProvider {
        fn fetch_news(
            &self,
            _ticker: &str,
            _limit: usize,
        ) -> Result<MarketNewsFeed, crate::error::AppError> {
            match (&self.feed, &self.error) {
                (Some(feed), _) => Ok(feed.clone()),
                (_, Some(message)) => Err(crate::error::AppError::Http(message.clone())),
                _ => unreachable!(),
            }
        }
    }

    fn temp_db() -> (TempDir, Db) {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("tracker.db");
        let db = Db::open_for_init(&path).unwrap();
        db.init("USD").unwrap();
        (temp, db)
    }

    fn sample_quote() -> MarketQuote {
        MarketQuote {
            ticker: "AAPL".to_string(),
            name: "Apple Inc.".to_string(),
            exchange: Some("NASDAQ".to_string()),
            currency: Some("USD".to_string()),
            last_price: 210.25,
            change: Some(1.75),
            change_percent: Some(0.84),
            market_state: Some("regular".to_string()),
            fetched_at: String::new(),
            stale: false,
        }
    }

    fn sample_news() -> MarketNewsFeed {
        MarketNewsFeed {
            ticker: "AAPL".to_string(),
            fetched_at: String::new(),
            stale: false,
            items: vec![MarketNewsItem {
                title: "Apple launches new hardware".to_string(),
                source: "example.com".to_string(),
                url: "https://example.com/apple".to_string(),
                published_at: Some("2026-03-13T10:00:00Z".to_string()),
                summary: "Summary".to_string(),
                ticker: "AAPL".to_string(),
            }],
        }
    }

    #[test]
    fn normalizes_ticker_symbols() {
        assert_eq!(normalize_ticker(" msft ").unwrap(), "MSFT");
        assert!(normalize_ticker("bad ticker!").is_err());
    }

    #[test]
    fn parses_valyu_json_quote_payload() {
        let quote = parse_valyu_quote_response(
            r#"{"structured_output":{"ticker":"AAPL","name":"Apple Inc.","exchange":"NASDAQ","currency":"USD","last_price":210.25,"change":1.75,"change_percent":0.84,"market_state":"regular"}}"#,
            "AAPL",
        )
        .unwrap();
        assert_eq!(quote.ticker, "AAPL");
        assert_eq!(quote.name, "Apple Inc.");
        assert_eq!(quote.exchange.as_deref(), Some("NASDAQ"));
        assert_eq!(quote.currency.as_deref(), Some("USD"));
    }
    #[test]
    fn parses_valyu_sse_quote_payload_with_null_optional_fields() {
        let quote = parse_valyu_quote_response(
            "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"{\\\"ticker\\\":\\\"AAPL\\\",\\\"name\\\":\\\"Apple Inc.\\\",\\\"exchange\\\":\\\"NASDAQ\\\",\\\"currency\\\":\\\"USD\\\",\\\"last_price\\\":250.11501,\\\"change\\\":null,\\\"change_percent\\\":null,\\\"market_state\\\":null}\"},\"index\":0,\"finish_reason\":\"STOP\"}]}",
            "AAPL",
        )
        .unwrap();
        assert_eq!(quote.ticker, "AAPL");
        assert_eq!(quote.last_price, 250.11501);
        assert_eq!(quote.change, None);
        assert_eq!(quote.change_percent, None);
        assert_eq!(quote.market_state, None);
    }

    #[test]
    fn parses_valyu_quote_payload_without_exchange() {
        let quote = parse_valyu_quote_response(
            r#"{"structured_output":{"ticker":"AAPL","name":"Apple Inc.","currency":"USD","last_price":210.25,"change":1.75,"change_percent":0.84,"market_state":"regular"}}"#,
            "AAPL",
        )
        .unwrap();
        assert_eq!(quote.exchange, None);
    }
    #[test]
    fn parses_valyu_quote_payload_without_currency() {
        let quote = parse_valyu_quote_response(
            r#"{"structured_output":{"ticker":"AAPL","name":"Apple Inc.","exchange":"NASDAQ","last_price":210.25,"change":1.75,"change_percent":0.84,"market_state":"regular"}}"#,
            "AAPL",
        )
        .unwrap();
        assert_eq!(quote.currency, None);
    }
    #[test]
    fn parses_tavily_news_payload() {
        let feed = parse_tavily_news_response(
            r#"{"results":[{"title":"Apple expands services","url":"https://example.com/apple","content":"Apple summary","published_at":"2026-03-13T10:00:00Z","source":"Example News"}]}"#,
            "AAPL",
        )
        .unwrap();
        assert_eq!(feed.ticker, "AAPL");
        assert_eq!(feed.items.len(), 1);
        assert_eq!(feed.items[0].title, "Apple expands services");
        assert_eq!(feed.items[0].source, "Example News");
    }

    #[test]
    fn lead_article_context_prefers_summary_text_over_title() {
        let item = MarketNewsItem {
            title: "Here's the great Nvidia stock mystery - Yahoo Finance".to_string(),
            source: "finance.yahoo.com".to_string(),
            url: "https://example.com/nvda".to_string(),
            published_at: None,
            summary: "# Here's the great Nvidia stock mystery. Nothing could get mighty Nvidia's stock rocking into the weekend, even after a strong quarter and upbeat guidance. Investors are still looking for the next upside catalyst.".to_string(),
            ticker: "NVDA".to_string(),
        };

        let context = lead_article_context(&item).unwrap();
        assert!(
            context.contains("Nothing could get mighty Nvidia's stock rocking into the weekend")
        );
        assert!(!context.contains("Here's the great Nvidia stock mystery"));
    }

    #[test]
    fn article_page_ranks_above_quote_page() {
        let article = MarketNewsItem {
            title: "Here's the great Nvidia stock mystery - Yahoo Finance".to_string(),
            source: "finance.yahoo.com".to_string(),
            url: "https://finance.yahoo.com/news/heres-the-great-nvidia-stock-mystery-133025474.html".to_string(),
            published_at: Some("2026-03-14T10:00:00Z".to_string()),
            summary: "Nothing could get mighty Nvidia's stock rocking into the weekend, two days after its much-awaited earnings.".to_string(),
            ticker: "NVDA".to_string(),
        };
        let quote_page = MarketNewsItem {
            title: "NVIDIA Corporation (NVDA) Stock Price, News, Quote & History".to_string(),
            source: "finance.yahoo.com".to_string(),
            url: "https://finance.yahoo.com/quote/NVDA/".to_string(),
            published_at: None,
            summary: "News headlines Nvidia continues to strengthen its position in the AI market with key partnerships and product announcements".to_string(),
            ticker: "NVDA".to_string(),
        };

        let article_score = rank_market_item(&article, "NVDA", Some("NVIDIA Corporation"));
        let quote_score = rank_market_item(&quote_page, "NVDA", Some("NVIDIA Corporation"));
        assert!(article_score > quote_score);
    }
    #[test]
    fn compose_market_brief_uses_article_context_in_summary() {
        let quote = sample_quote();
        let feed = MarketNewsFeed {
            ticker: "NVDA".to_string(),
            fetched_at: "2026-03-14T10:00:00Z".to_string(),
            stale: false,
            items: vec![MarketNewsItem {
                title: "Here's the great Nvidia stock mystery - Yahoo Finance".to_string(),
                source: "finance.yahoo.com".to_string(),
                url: "https://example.com/nvda".to_string(),
                published_at: None,
                summary: "# Here's the great Nvidia stock mystery. Nothing could get mighty Nvidia's stock rocking into the weekend, even after a strong quarter and upbeat guidance.".to_string(),
                ticker: "NVDA".to_string(),
            }],
        };

        let brief = compose_market_brief(Some(&quote), &feed);
        assert!(brief
            .summary
            .contains("Nothing could get mighty Nvidia's stock rocking into the weekend"));
        assert!(!brief.summary.contains("The lead story is"));
        assert!(brief.why_it_matters[0].contains("The article context is this"));
    }
    #[test]
    fn compose_market_brief_ignores_generic_quote_pages() {
        let quote = MarketQuote {
            ticker: "NVDA".to_string(),
            name: "NVIDIA".to_string(),
            exchange: Some("NASDAQ".to_string()),
            currency: Some("USD".to_string()),
            last_price: 120.0,
            change: None,
            change_percent: None,
            market_state: Some("regular".to_string()),
            fetched_at: "2026-03-14T10:00:00Z".to_string(),
            stale: false,
        };
        let feed = MarketNewsFeed {
            ticker: "NVDA".to_string(),
            fetched_at: "2026-03-14T10:00:00Z".to_string(),
            stale: false,
            items: vec![
                MarketNewsItem {
                    title: "NVIDIA Corporation (NVDA) stock price, news, quote and history ..."
                        .to_string(),
                    source: "nz.finance.yahoo.com".to_string(),
                    url: "https://nz.finance.yahoo.com/quote/NVDA/latest-news/".to_string(),
                    published_at: None,
                    summary: "Find the latest NVIDIA Corporation (NVDA) stock quote, history, news and other vital information to help you with your stock trading and investing.".to_string(),
                    ticker: "NVDA".to_string(),
                },
                MarketNewsItem {
                    title: "Here's the great Nvidia stock mystery - Yahoo Finance".to_string(),
                    source: "finance.yahoo.com".to_string(),
                    url: "https://finance.yahoo.com/news/heres-the-great-nvidia-stock-mystery-133025474.html".to_string(),
                    published_at: None,
                    summary: "# Here's the great Nvidia stock mystery. Nothing could get mighty Nvidia's (NVDA) stock rocking into the weekend, two days after its much-awaited earnings.".to_string(),
                    ticker: "NVDA".to_string(),
                },
            ],
        };
        let brief = compose_market_brief(Some(&quote), &feed);
        assert!(brief
            .summary
            .contains("Nothing could get mighty Nvidia's (NVDA) stock"));
        assert!(!brief.summary.contains("Find the latest NVIDIA Corporation"));
    }
    #[test]
    fn falls_back_to_stale_cached_quote() {
        let (_temp, db) = temp_db();
        db.store_quote_cache(&sample_quote(), "2999-01-01T00:00:00+00:00")
            .unwrap();
        let service = MarketService::new(
            MockQuoteProvider {
                quote: None,
                error: Some("offline".to_string()),
            },
            MockNewsProvider {
                feed: Some(sample_news()),
                error: None,
            },
        );

        let mut quote = db.cached_quote("AAPL").unwrap().unwrap();
        quote.stale = true;
        db.store_quote_cache(&quote, "2000-01-01T00:00:00+00:00")
            .unwrap();

        let result = service.quote(&db, "AAPL", false).unwrap();
        assert!(result.stale);
        assert_eq!(result.ticker, "AAPL");
    }

    #[test]
    fn uses_cached_only_news_when_requested() {
        let (_temp, db) = temp_db();
        db.add_watchlist_item("AAPL", Some("Apple")).unwrap();
        db.store_news_cache(&sample_news(), "2999-01-01T00:00:00+00:00")
            .unwrap();
        let service = MarketService::new(
            MockQuoteProvider {
                quote: Some(sample_quote()),
                error: None,
            },
            MockNewsProvider {
                feed: None,
                error: Some("should not call network".to_string()),
            },
        );

        let feed = service.news(&db, Some("AAPL"), 5, true).unwrap();
        assert_eq!(feed.items.len(), 1);
        assert_eq!(feed.items[0].ticker, "AAPL");
    }
}
