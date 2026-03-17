use std::env;

use chrono::Local;
use serde_json::Value;

use crate::db::Db;
use crate::error::AppError;
use crate::model::{PolyBrief, PolyMarket, PolyRefreshSummary, PolySearchResult, PolyWatchEntry};

const DEFAULT_POLYMARKET_BASE_URL: &str = "https://gamma-api.polymarket.com";
const POLY_MARKET_TTL_MINUTES: i64 = 15;
const POLY_MOVERS_TTL_MINUTES: i64 = 30;
const DEFAULT_MOVERS_LIMIT: usize = 8;

pub struct PolyService {
    base_url: String,
}

impl PolyService {
    pub fn from_env() -> Self {
        Self {
            base_url: env::var("HELIUS_POLYMARKET_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_POLYMARKET_BASE_URL.to_string()),
        }
    }

    pub fn add_watchlist(&self, db: &Db, slug: &str, label: Option<&str>) -> Result<i64, AppError> {
        db.add_poly_watch_item(&normalize_poly_slug(slug)?, label)
    }

    pub fn list_watchlist(&self, db: &Db) -> Result<Vec<PolyWatchEntry>, AppError> {
        db.list_poly_watch_items()?
            .into_iter()
            .map(|item| {
                Ok(PolyWatchEntry {
                    id: item.id,
                    slug: item.slug.clone(),
                    label: item.label,
                    added_at: item.added_at,
                    market: db.cached_poly_market(&item.slug)?,
                })
            })
            .collect()
    }

    pub fn remove_watchlist(&self, db: &Db, slug: &str) -> Result<(), AppError> {
        db.remove_poly_watch_item(&normalize_poly_slug(slug)?)
    }

    pub fn market(&self, db: &Db, slug: &str, cached_only: bool) -> Result<PolyMarket, AppError> {
        let slug = normalize_poly_slug(slug)?;
        let cached = db.cached_poly_market(&slug)?;
        if let Some(market) = cached.clone() {
            if !market.stale || cached_only {
                return Ok(market);
            }
        }
        if cached_only {
            return Err(AppError::NotFound(format!(
                "no cached Polymarket market was found for `{slug}`"
            )));
        }

        self.fetch_market_live(db, &slug).or_else(|error| {
            if let Some(mut stale) = cached {
                stale.stale = true;
                Ok(stale)
            } else {
                Err(error)
            }
        })
    }

    pub fn brief(
        &self,
        db: &Db,
        slug: Option<&str>,
        cached_only: bool,
    ) -> Result<PolyBrief, AppError> {
        let slug = match slug {
            Some(value) => normalize_poly_slug(value)?,
            None => db.single_poly_watch_slug()?,
        };
        let market = self.market(db, &slug, cached_only)?;
        Ok(compose_poly_brief(&market))
    }

    pub fn movers(
        &self,
        db: &Db,
        limit: usize,
        cached_only: bool,
    ) -> Result<Vec<PolyMarket>, AppError> {
        let limit = limit.max(1);
        let cached = db.cached_poly_movers()?;
        if let Some(mut markets) = cached.clone() {
            if !markets.iter().any(|market| market.stale) || cached_only {
                markets.truncate(limit);
                return Ok(markets);
            }
        }
        if cached_only {
            return Err(AppError::NotFound(
                "no cached Polymarket movers were found".to_string(),
            ));
        }

        self.fetch_movers_live(db, limit).or_else(|error| {
            if let Some(mut stale) = cached {
                for market in &mut stale {
                    market.stale = true;
                }
                stale.truncate(limit);
                Ok(stale)
            } else {
                Err(error)
            }
        })
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<PolySearchResult>, AppError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Err(AppError::Validation(
                "search query cannot be empty".to_string(),
            ));
        }
        let url = format!(
            "{}/public-search?q={}&limit={}",
            self.base_url.trim_end_matches('/'),
            url_encode_component(trimmed),
            limit.max(1)
        );
        let response = ureq::get(&url).call().map_err(map_ureq_error)?;
        let body = response
            .into_string()
            .map_err(|error| AppError::Http(error.to_string()))?;
        parse_poly_search_response(&body, limit)
    }

    pub fn refresh_watchlist(
        &self,
        db: &Db,
        slug: Option<&str>,
        refresh_all: bool,
    ) -> Result<PolyRefreshSummary, AppError> {
        let slugs = self.resolve_refresh_slugs(db, slug, refresh_all, true)?;
        let mut summary = PolyRefreshSummary {
            slugs_considered: slugs.len(),
            markets_refreshed: 0,
            movers_refreshed: false,
            failure_count: 0,
            ran_at: now_timestamp(),
        };

        for slug in &slugs {
            match self.fetch_market_live(db, slug) {
                Ok(_) => summary.markets_refreshed += 1,
                Err(_) => summary.failure_count += 1,
            }
        }

        if refresh_all {
            if self.fetch_movers_live(db, DEFAULT_MOVERS_LIMIT).is_ok() {
                summary.movers_refreshed = true;
            }
        }

        Ok(summary)
    }

    fn fetch_market_live(&self, db: &Db, slug: &str) -> Result<PolyMarket, AppError> {
        let url = format!(
            "{}/markets?slug={}",
            self.base_url.trim_end_matches('/'),
            url_encode_component(slug)
        );
        let response = ureq::get(&url).call().map_err(map_ureq_error)?;
        let body = response
            .into_string()
            .map_err(|error| AppError::Http(error.to_string()))?;
        let mut market = parse_poly_market_response(&body, Some(slug))?;
        market.slug = slug.to_string();
        market.fetched_at = now_timestamp();
        market.stale = false;
        db.store_poly_market(&market, &expiry_timestamp(POLY_MARKET_TTL_MINUTES))?;
        Ok(market)
    }

    fn fetch_movers_live(&self, db: &Db, limit: usize) -> Result<Vec<PolyMarket>, AppError> {
        let url = format!(
            "{}/markets?closed=false&active=true&order=volume24hr&ascending=false&limit={}",
            self.base_url.trim_end_matches('/'),
            limit.max(1)
        );
        let response = ureq::get(&url).call().map_err(map_ureq_error)?;
        let body = response
            .into_string()
            .map_err(|error| AppError::Http(error.to_string()))?;
        let mut movers = parse_poly_movers_response(&body, limit)?;
        let fetched_at = now_timestamp();
        for market in &mut movers {
            market.fetched_at = fetched_at.clone();
            market.stale = false;
        }
        db.store_poly_movers(&movers, &expiry_timestamp(POLY_MOVERS_TTL_MINUTES))?;
        Ok(movers)
    }

    fn resolve_refresh_slugs(
        &self,
        db: &Db,
        slug: Option<&str>,
        refresh_all: bool,
        error_on_empty: bool,
    ) -> Result<Vec<String>, AppError> {
        if refresh_all {
            let all = db.list_poly_watch_items()?;
            if all.is_empty() {
                if error_on_empty {
                    return Err(AppError::Validation(
                        "Polymarket watchlist is empty; add a market first".to_string(),
                    ));
                }
                return Ok(Vec::new());
            }
            return Ok(all.into_iter().map(|item| item.slug).collect());
        }

        match slug {
            Some(value) => Ok(vec![normalize_poly_slug(value)?]),
            None => Ok(vec![db.single_poly_watch_slug()?]),
        }
    }
}

pub fn compose_poly_brief(market: &PolyMarket) -> PolyBrief {
    let yes_price = market.yes_price.unwrap_or(0.5);
    let no_price = market.no_price.unwrap_or((1.0 - yes_price).clamp(0.0, 1.0));
    let tone = if yes_price >= 0.67 {
        "favored_yes"
    } else if yes_price <= 0.33 {
        "favored_no"
    } else {
        "close"
    };
    let summary = format!(
        "Polymarket is pricing {} at {:.0}% YES and {:.0}% NO.",
        market.question,
        yes_price * 100.0,
        no_price * 100.0
    );

    let mut why_it_matters = Vec::new();
    if let Some(volume_24h) = market.volume_24h {
        why_it_matters.push(format!(
            "24h volume is about ${:.0}, which is a quick proxy for how actively this market is repricing.",
            volume_24h
        ));
    }
    if let Some(liquidity) = market.liquidity {
        why_it_matters.push(format!(
            "Displayed liquidity is about ${:.0}; lower liquidity usually means noisier odds and wider execution risk.",
            liquidity
        ));
    }
    if let Some(end_date) = market.end_date.as_deref() {
        why_it_matters.push(format!(
            "The market is scheduled to resolve around {}, so information risk usually rises as that date approaches.",
            end_date
        ));
    }
    if why_it_matters.is_empty() {
        why_it_matters.push(
            "Prediction-market pricing is useful as a live probability signal, but it can move sharply on thin information.".to_string(),
        );
    }

    let mut watch_items = Vec::new();
    if (yes_price - 0.5).abs() <= 0.08 {
        watch_items.push(
            "This is a close market; small headline flow can move the implied odds quickly."
                .to_string(),
        );
    } else if yes_price >= 0.8 || yes_price <= 0.2 {
        watch_items.push(
            "The market is already leaning hard one way; the next material update matters more than minor chatter.".to_string(),
        );
    }
    if matches!(market.liquidity, Some(value) if value < 25_000.0) {
        watch_items.push(
            "Liquidity looks relatively light, so treat sharp moves carefully because they may not reflect broad conviction.".to_string(),
        );
    }
    if market.stale {
        watch_items.push(
            "Refresh before acting; the current brief is built from stale cached market data."
                .to_string(),
        );
    }
    if watch_items.is_empty() {
        watch_items.push(
            "Watch whether odds, volume, and liquidity shift together; that is usually a better signal than odds alone.".to_string(),
        );
    }

    PolyBrief {
        slug: market.slug.clone(),
        summary,
        tone: tone.to_string(),
        why_it_matters,
        watch_items,
        fetched_at: market.fetched_at.clone(),
        stale: market.stale,
    }
}

pub fn normalize_poly_slug(raw: &str) -> Result<String, AppError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "Polymarket slug cannot be empty".to_string(),
        ));
    }

    let slug = if let Some((_, tail)) = trimmed.split_once("/event/") {
        tail.split(['?', '#', '/']).next().unwrap_or(tail)
    } else {
        trimmed
    };
    let normalized = slug.trim().trim_matches('/').to_ascii_lowercase();
    let is_valid = !normalized.is_empty()
        && normalized.len() <= 140
        && normalized
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-');
    if !is_valid {
        return Err(AppError::Validation(
            "Polymarket slug must use only letters, numbers, and '-'".to_string(),
        ));
    }
    Ok(normalized)
}

fn parse_poly_market_response(
    body: &str,
    requested_slug: Option<&str>,
) -> Result<PolyMarket, AppError> {
    let value = serde_json::from_str::<Value>(body)?;
    let candidates = extract_market_candidates(&value);
    let slug_match = requested_slug.and_then(|slug| {
        candidates.iter().copied().find(|candidate| {
            candidate
                .get("slug")
                .and_then(Value::as_str)
                .map(|value| value.eq_ignore_ascii_case(slug))
                .unwrap_or(false)
        })
    });
    let selected = slug_match
        .or_else(|| candidates.first().copied())
        .ok_or_else(|| {
            AppError::NotFound(
                "Polymarket market response did not include any market records".to_string(),
            )
        })?;
    build_poly_market(selected, requested_slug, None)
        .ok_or_else(|| AppError::Http("failed to parse Polymarket market response".to_string()))
}

fn parse_poly_movers_response(body: &str, limit: usize) -> Result<Vec<PolyMarket>, AppError> {
    let value = serde_json::from_str::<Value>(body)?;
    let mut markets = extract_market_candidates(&value)
        .into_iter()
        .filter_map(|candidate| build_poly_market(candidate, None, None))
        .collect::<Vec<_>>();
    markets.truncate(limit.max(1));
    Ok(markets)
}

fn parse_poly_search_response(body: &str, limit: usize) -> Result<Vec<PolySearchResult>, AppError> {
    let value = serde_json::from_str::<Value>(body)?;
    let mut results = Vec::new();

    if let Some(events) = value.get("events").and_then(Value::as_array) {
        for event in events {
            let event_title = event
                .get("title")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if let Some(markets) = event.get("markets").and_then(Value::as_array) {
                for market in markets {
                    if let Some(item) = build_poly_market(market, None, event_title.as_deref()) {
                        results.push(PolySearchResult {
                            slug: item.slug,
                            question: item.question,
                            event_title: event_title.clone().or(item.event_title),
                            end_date: item.end_date,
                            active: item.active,
                            closed: item.closed,
                            yes_price: item.yes_price,
                            no_price: item.no_price,
                        });
                    }
                }
            }
        }
    }

    if results.is_empty() {
        for market in extract_market_candidates(&value) {
            if let Some(item) = build_poly_market(market, None, None) {
                results.push(PolySearchResult {
                    slug: item.slug,
                    question: item.question,
                    event_title: item.event_title,
                    end_date: item.end_date,
                    active: item.active,
                    closed: item.closed,
                    yes_price: item.yes_price,
                    no_price: item.no_price,
                });
            }
        }
    }

    results.truncate(limit.max(1));
    Ok(results)
}

fn extract_market_candidates<'a>(value: &'a Value) -> Vec<&'a Value> {
    if let Some(array) = value.as_array() {
        return array.iter().collect();
    }
    if let Some(array) = value.get("data").and_then(Value::as_array) {
        return array.iter().collect();
    }
    if let Some(array) = value.get("markets").and_then(Value::as_array) {
        return array.iter().collect();
    }
    Vec::new()
}

fn build_poly_market(
    value: &Value,
    requested_slug: Option<&str>,
    event_title: Option<&str>,
) -> Option<PolyMarket> {
    let slug = value
        .get("slug")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| requested_slug.map(ToOwned::to_owned))?;
    let question = value
        .get("question")
        .or_else(|| value.get("title"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let subtitle = value
        .get("description")
        .or_else(|| value.get("subtitle"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let end_date = value
        .get("endDate")
        .or_else(|| value.get("end_date"))
        .or_else(|| value.get("endDateIso"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let active = value.get("active").and_then(as_bool_like).unwrap_or(true);
    let closed = value.get("closed").and_then(as_bool_like).unwrap_or(false);
    let liquidity = value
        .get("liquidityNum")
        .or_else(|| value.get("liquidity"))
        .and_then(as_f64_like);
    let volume_24h = value
        .get("volume24hr")
        .or_else(|| value.get("volume24Hr"))
        .or_else(|| value.get("volume24h"))
        .and_then(as_f64_like);
    let volume_total = value
        .get("volumeNum")
        .or_else(|| value.get("volume"))
        .and_then(as_f64_like);
    let outcomes = extract_string_array(value.get("outcomes"));
    let outcome_prices = extract_f64_array(value.get("outcomePrices"));
    let yes_price = outcome_price_for(&outcomes, &outcome_prices, "yes");
    let no_price = outcome_price_for(&outcomes, &outcome_prices, "no");

    Some(PolyMarket {
        slug,
        question,
        subtitle,
        event_title: event_title.map(ToOwned::to_owned).or_else(|| {
            value
                .get("eventTitle")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        }),
        end_date,
        active,
        closed,
        liquidity,
        volume_24h,
        volume_total,
        yes_price,
        no_price,
        fetched_at: now_timestamp(),
        stale: false,
    })
}

fn extract_string_array(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        Some(Value::String(raw)) => serde_json::from_str::<Vec<String>>(raw).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn extract_f64_array(value: Option<&Value>) -> Vec<f64> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(as_f64_like).collect(),
        Some(Value::String(raw)) => serde_json::from_str::<Vec<Value>>(raw)
            .unwrap_or_default()
            .iter()
            .filter_map(as_f64_like)
            .collect(),
        _ => Vec::new(),
    }
}

fn outcome_price_for(outcomes: &[String], prices: &[f64], needle: &str) -> Option<f64> {
    outcomes
        .iter()
        .position(|outcome| outcome.eq_ignore_ascii_case(needle))
        .and_then(|index| prices.get(index).copied())
}

fn as_bool_like(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) => value.as_i64().map(|number| number != 0),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn as_f64_like(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn now_timestamp() -> String {
    Local::now().to_rfc3339()
}

fn expiry_timestamp(minutes: i64) -> String {
    (Local::now() + chrono::Duration::minutes(minutes)).to_rfc3339()
}

fn map_ureq_error(error: ureq::Error) -> AppError {
    match error {
        ureq::Error::Status(code, response) => AppError::Http(format!(
            "Polymarket request failed with status {code}: {}",
            response.status_text()
        )),
        ureq::Error::Transport(transport) => AppError::Http(transport.to_string()),
    }
}

fn url_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push_str("%20"),
            _ => encoded.push_str(&format!("%{:02X}", byte)),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{compose_poly_brief, normalize_poly_slug, parse_poly_market_response};

    #[test]
    fn normalizes_polymarket_slug_from_url() {
        let slug =
            normalize_poly_slug("https://polymarket.com/event/will-fed-cut-rates-in-june?tid=123")
                .unwrap();
        assert_eq!(slug, "will-fed-cut-rates-in-june");
    }

    #[test]
    fn parses_market_payload_with_string_arrays() {
        let body = r#"[
            {
                "slug": "will-fed-cut-rates-in-june",
                "question": "Will the Fed cut rates in June?",
                "description": "CME-style macro event market",
                "active": true,
                "closed": false,
                "liquidityNum": 120000,
                "volume24hr": 54000,
                "volumeNum": 900000,
                "outcomes": "[\"Yes\",\"No\"]",
                "outcomePrices": "[\"0.41\",\"0.59\"]"
            }
        ]"#;
        let market = parse_poly_market_response(body, Some("will-fed-cut-rates-in-june")).unwrap();
        assert_eq!(market.slug, "will-fed-cut-rates-in-june");
        assert_eq!(market.yes_price, Some(0.41));
        assert_eq!(market.no_price, Some(0.59));
        assert_eq!(market.volume_24h, Some(54000.0));
    }

    #[test]
    fn compose_poly_brief_marks_close_market() {
        let market = parse_poly_market_response(
            r#"[{"slug":"test-market","question":"Will X happen?","active":true,"closed":false,"outcomes":"[\"Yes\",\"No\"]","outcomePrices":"[\"0.52\",\"0.48\"]"}]"#,
            Some("test-market"),
        )
        .unwrap();
        let brief = compose_poly_brief(&market);
        assert_eq!(brief.tone, "close");
        assert!(brief.summary.contains("52% YES"));
    }
}
