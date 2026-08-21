//! News and social sentiment ingestion for the market scanner.
//!
//! This module deliberately keeps ingestion and scoring deterministic.  A text
//! model may classify an event before it reaches this module, but source
//! credibility, de-duplication, time decay and bot discounts are applied here
//! so that external text cannot directly place an order.

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::{cmp::Ordering, env};

use reqwest::blocking::Client;

const DEFAULT_HALF_LIFE_SECS: i64 = 6 * 60 * 60;
const DEFAULT_MAX_AGE_SECS: i64 = 3 * 24 * 60 * 60;
const DEFAULT_DEDUPE_WINDOW_SECS: i64 = 5 * 60;

/// A normalized engagement snapshot attached to a news or social item.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EngagementMetrics {
    #[serde(default)]
    pub likes: u64,
    #[serde(default)]
    pub reposts: u64,
    #[serde(default)]
    pub replies: u64,
    #[serde(default)]
    pub quotes: u64,
    #[serde(default)]
    pub views: u64,
    #[serde(default)]
    pub followers: u64,
}

impl EngagementMetrics {
    pub fn is_empty(&self) -> bool {
        self.likes == 0
            && self.reposts == 0
            && self.replies == 0
            && self.quotes == 0
            && self.views == 0
            && self.followers == 0
    }

    fn quality_factor(&self) -> f64 {
        if self.is_empty() {
            return 1.0;
        }

        // Log scaling limits the influence of viral posts and prevents raw
        // view counts from dominating price and source evidence.
        let interactions = self
            .likes
            .saturating_add(self.reposts.saturating_mul(2))
            .saturating_add(self.replies)
            .saturating_add(self.quotes.saturating_mul(2));
        let interaction_score = (interactions as f64).ln_1p() / 12.0;
        let view_score = (self.views as f64).ln_1p() / 40.0;
        (0.9 + interaction_score + view_score).clamp(0.75, 1.35)
    }
}

/// A source profile used to weight events from news providers and X accounts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewsProvider {
    pub id: String,
    pub name: String,
    /// Values such as `exchange`, `official`, `major_media`, `media`, `kol`,
    /// `user` and `unknown` are understood by the default credibility table.
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub base_url: String,
    /// A configured value in [0, 1].  Zero means use the kind/name default.
    #[serde(default)]
    pub credibility: f64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub rate_limit_per_minute: u32,
}

impl Default for NewsProvider {
    fn default() -> Self {
        Self {
            id: "unknown".to_string(),
            name: "Unknown source".to_string(),
            kind: "unknown".to_string(),
            base_url: String::new(),
            credibility: 0.0,
            enabled: true,
            rate_limit_per_minute: 0,
        }
    }
}

impl NewsProvider {
    pub fn new(id: impl Into<String>, name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind: kind.into(),
            ..Self::default()
        }
    }

    pub fn with_credibility(mut self, credibility: f64) -> Self {
        self.credibility = credibility.clamp(0.0, 1.0);
        self
    }

    pub fn effective_credibility(&self) -> f64 {
        if self.credibility.is_finite() && self.credibility > 0.0 {
            return self.credibility.clamp(0.0, 1.0);
        }
        source_credibility(&self.kind, &self.name)
    }
}

/// One normalized item from a news API, project feed, or X/Twitter API.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SentimentEvent {
    /// Provider-native ID.  If missing, a deterministic fingerprint is used.
    #[serde(default)]
    pub id: String,
    /// A Binance symbol or base ticker supplied by the provider.
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub source_kind: String,
    /// Optional per-provider override in [0, 1].  Zero uses the source-kind
    /// table, which keeps hand-created events concise.
    #[serde(default)]
    pub source_credibility: f64,
    /// Examples: `promotion`, `listing`, `hack`, `regulation`, `macro`.
    #[serde(default)]
    pub event_type: String,
    /// Unix timestamp in seconds (UTC).
    #[serde(default)]
    pub published_at: i64,
    /// Polarity in [-1, 1].
    #[serde(default)]
    pub sentiment: f64,
    /// Classifier certainty in [0, 1].
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    #[serde(default)]
    pub engagement: EngagementMetrics,
    /// Probability that the item is generated or coordinated spam in [0, 1].
    #[serde(default)]
    pub bot_probability: f64,
    #[serde(default)]
    pub language: String,
}

impl SentimentEvent {
    pub fn normalized(&self) -> Self {
        let mut event = self.clone();
        event.id = event.id.trim().to_string();
        event.symbol = normalize_symbol(&event.symbol);
        event.symbols = event
            .symbols
            .iter()
            .map(|symbol| normalize_symbol(symbol))
            .filter(|symbol| !symbol.is_empty())
            .collect();
        event.title = normalize_text(&event.title);
        event.text = normalize_text(&event.text);
        event.url = normalize_url(&event.url);
        event.author = event.author.trim().to_string();
        event.provider = event.provider.trim().to_string();
        event.source_kind = event.source_kind.trim().to_ascii_lowercase();
        event.event_type = event.event_type.trim().to_ascii_lowercase();
        event.source_credibility = finite_clamp(event.source_credibility, 0.0, 1.0, 0.0);
        event.sentiment = normalize_polarity(event.sentiment);
        event.confidence = normalize_probability(event.confidence, 0.5);
        event.bot_probability = normalize_probability(event.bot_probability, 0.0);
        event
    }

    pub fn fingerprint(&self) -> String {
        event_fingerprint(self)
    }

    pub fn storage_id(&self) -> String {
        let normalized = self.normalized();
        if normalized.id.is_empty() {
            normalized.fingerprint()
        } else {
            format!("id:{}", normalized.id.to_ascii_lowercase())
        }
    }

    pub fn mentions_symbol(&self, symbol: &str, aliases: &[String]) -> bool {
        matches_ticker(self, symbol, aliases)
    }
}

/// Tunables for event weighting and aggregation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SentimentConfig {
    #[serde(default = "default_half_life")]
    pub half_life_secs: i64,
    #[serde(default = "default_max_age")]
    pub max_age_secs: i64,
    #[serde(default)]
    pub min_event_confidence: f64,
    #[serde(default = "default_dedupe_window")]
    pub dedupe_window_secs: i64,
}

impl Default for SentimentConfig {
    fn default() -> Self {
        Self {
            half_life_secs: DEFAULT_HALF_LIFE_SECS,
            max_age_secs: DEFAULT_MAX_AGE_SECS,
            min_event_confidence: 0.0,
            dedupe_window_secs: DEFAULT_DEDUPE_WINDOW_SECS,
        }
    }
}

/// Aggregated directional evidence for one ticker at a point in time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SentimentAggregate {
    pub symbol: String,
    /// Weighted polarity in [-1, 1].
    pub score: f64,
    /// Evidence quality in [0, 1], independent of direction.
    pub quality: f64,
    /// `bullish`, `bearish`, `neutral`, or `unconfirmed`.
    pub label: String,
    pub event_count: usize,
    pub weighted_event_count: f64,
    pub positive_count: usize,
    pub negative_count: usize,
    pub neutral_count: usize,
    pub unique_sources: usize,
    pub latest_event_at: i64,
    pub as_of: i64,
}

impl SentimentAggregate {
    pub fn is_actionable(&self, min_quality: f64, min_abs_score: f64) -> bool {
        self.quality >= min_quality && self.score.abs() >= min_abs_score
    }
}

/// Deterministic defaults exposed for configuration editors.
pub fn default_config() -> SentimentConfig {
    SentimentConfig::default()
}

fn default_true() -> bool {
    true
}

fn default_confidence() -> f64 {
    0.5
}

fn default_half_life() -> i64 {
    DEFAULT_HALF_LIFE_SECS
}

fn default_max_age() -> i64 {
    DEFAULT_MAX_AGE_SECS
}

fn default_dedupe_window() -> i64 {
    DEFAULT_DEDUPE_WINDOW_SECS
}

fn finite_clamp(value: f64, min: f64, max: f64, fallback: f64) -> f64 {
    if value.is_finite() {
        value.clamp(min, max)
    } else {
        fallback
    }
}

pub fn normalize_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn normalize_url(value: &str) -> String {
    let mut url = value.trim().to_ascii_lowercase();
    if let Some(stripped) = url.strip_prefix("https://") {
        url = stripped.to_string();
    } else if let Some(stripped) = url.strip_prefix("http://") {
        url = stripped.to_string();
    }
    if let Some((base, _)) = url.split_once('?') {
        url = base.to_string();
    }
    while url.ends_with('/') {
        url.pop();
    }
    url
}

/// Normalize a Binance contract or ticker without removing the quote asset.
pub fn normalize_symbol(value: &str) -> String {
    value
        .trim()
        .trim_start_matches('$')
        .trim_start_matches('#')
        .to_ascii_uppercase()
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
        .replace(":", "")
        .replace("PERP", "")
}

/// Return the base ticker used for matching `BTCUSDT`, `BTC/USDT:USDT` and
/// `$BTC` as the same asset.
pub fn normalize_ticker(value: &str) -> String {
    let symbol = normalize_symbol(value);
    for quote in ["USDT", "USDC", "BUSD", "USD"] {
        if symbol.len() > quote.len() && symbol.ends_with(quote) {
            return symbol[..symbol.len() - quote.len()].to_string();
        }
    }
    symbol
}

fn normalize_polarity(value: f64) -> f64 {
    if !value.is_finite() {
        return 0.0;
    }
    let value = if value.abs() > 1.0 && value.abs() <= 100.0 {
        value / 100.0
    } else {
        value
    };
    value.clamp(-1.0, 1.0)
}

fn normalize_probability(value: f64, fallback: f64) -> f64 {
    if !value.is_finite() {
        return fallback;
    }
    let value = if value.abs() > 1.0 && value.abs() <= 100.0 {
        value / 100.0
    } else {
        value
    };
    value.clamp(0.0, 1.0)
}

fn canonical_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn stable_hash(value: &str) -> u64 {
    // FNV-1a is deliberately used instead of DefaultHasher so fingerprints
    // remain stable across process restarts and SQLite sessions.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}

pub fn event_fingerprint(event: &SentimentEvent) -> String {
    let normalized = event.normalized();
    if !normalized.url.is_empty() {
        return format!("url:{:016x}", stable_hash(&normalized.url));
    }
    let body = canonical_text(&format!("{} {}", normalized.title, normalized.text));
    if body.is_empty() {
        return format!(
            "empty:{}:{}",
            normalized.provider.to_ascii_lowercase(),
            normalized.published_at
        );
    }
    format!("text:{:016x}", stable_hash(&body))
}

/// Normalize and remove provider duplicates.  IDs and canonical URLs take
/// precedence; otherwise identical text is bucketed into a five-minute window.
pub fn deduplicate_events(events: &[SentimentEvent]) -> Vec<SentimentEvent> {
    deduplicate_events_with_config(events, &SentimentConfig::default())
}

pub fn dedupe_events(events: &[SentimentEvent]) -> Vec<SentimentEvent> {
    deduplicate_events(events)
}

pub fn deduplicate_events_with_config(
    events: &[SentimentEvent],
    config: &SentimentConfig,
) -> Vec<SentimentEvent> {
    let window = config.dedupe_window_secs.max(1);
    let mut selected: HashMap<String, SentimentEvent> = HashMap::new();
    for event in events {
        let normalized = event.normalized();
        let key = if !normalized.id.is_empty() {
            format!("id:{}", normalized.id.to_ascii_lowercase())
        } else if !normalized.url.is_empty() {
            format!("url:{}", normalized.url)
        } else {
            let bucket = if normalized.published_at > 0 {
                normalized.published_at.div_euclid(window)
            } else {
                0
            };
            format!(
                "text:{:016x}:{}",
                stable_hash(&canonical_text(&format!(
                    "{} {}",
                    normalized.title, normalized.text
                ))),
                bucket
            )
        };
        match selected.get(&key) {
            Some(previous) if event_preference(&normalized) <= event_preference(previous) => {}
            _ => {
                selected.insert(key, normalized);
            }
        }
    }

    let mut result: Vec<_> = selected.into_values().collect();
    result.sort_by(|left, right| {
        right
            .published_at
            .cmp(&left.published_at)
            .then_with(|| left.storage_id().cmp(&right.storage_id()))
    });
    result
}

fn event_preference(event: &SentimentEvent) -> (u64, u64, i64) {
    let source = event_source_credibility(event);
    let engagement = event
        .engagement
        .likes
        .saturating_add(event.engagement.reposts.saturating_mul(2))
        .saturating_add(event.engagement.replies)
        .saturating_add(event.engagement.quotes);
    (
        (source * 1_000_000.0) as u64,
        engagement,
        event.published_at,
    )
}

pub fn source_credibility(kind: &str, name: &str) -> f64 {
    let kind = kind.trim().to_ascii_lowercase();
    let name = name.trim().to_ascii_lowercase();
    if kind == "exchange" || name.contains("binance") || name.contains("bybit") {
        0.98
    } else if kind == "official" || kind == "project" || kind == "official_account" {
        0.92
    } else if kind == "major_media"
        || name.contains("reuters")
        || name.contains("bloomberg")
        || name.contains("coindesk")
        || name.contains("cointelegraph")
    {
        0.86
    } else if kind == "media" || kind == "news" {
        0.76
    } else if kind == "kol" || kind == "influencer" {
        0.55
    } else if kind == "user" || kind == "social" {
        0.38
    } else {
        0.42
    }
}

pub fn provider_credibility(provider: &NewsProvider) -> f64 {
    provider.effective_credibility()
}

fn event_source_credibility(event: &SentimentEvent) -> f64 {
    if event.source_credibility.is_finite() && event.source_credibility > 0.0 {
        event.source_credibility.clamp(0.0, 1.0)
    } else {
        source_credibility(&event.source_kind, &event.provider)
    }
}

/// Return the source-independent time multiplier.  Future timestamps are not
/// rewarded, while an invalid timestamp receives zero weight.
pub fn time_decay(published_at: i64, now: i64, half_life_secs: i64) -> f64 {
    if published_at <= 0 || half_life_secs <= 0 {
        return 0.0;
    }
    let age = now.saturating_sub(published_at).max(0) as f64;
    2_f64.powf(-age / half_life_secs as f64).clamp(0.0, 1.0)
}

pub fn engagement_adjustment(event: &SentimentEvent) -> f64 {
    let quality = event.engagement.quality_factor();
    let bot_discount = (1.0 - event.bot_probability.clamp(0.0, 1.0) * 0.85).clamp(0.1, 1.0);
    (quality * bot_discount).clamp(0.1, 1.35)
}

fn event_weight(event: &SentimentEvent, now: i64, config: &SentimentConfig) -> f64 {
    if event.published_at <= 0
        || (config.max_age_secs > 0 && now.saturating_sub(event.published_at) > config.max_age_secs)
        || event.confidence < config.min_event_confidence
    {
        return 0.0;
    }
    event_source_credibility(event)
        * event.confidence.clamp(0.0, 1.0)
        * time_decay(event.published_at, now, config.half_life_secs)
        * engagement_adjustment(event)
}

fn contains_token(text: &str, alias: &str) -> bool {
    let text = text.to_ascii_uppercase();
    let alias = alias.trim().to_ascii_uppercase();
    if alias.is_empty() {
        return false;
    }
    let mut offset = 0;
    while let Some(relative) = text[offset..].find(&alias) {
        let start = offset + relative;
        let end = start + alias.len();
        let before = text[..start].chars().next_back();
        let after = text[end..].chars().next();
        let boundary_before = before.map(|c| !c.is_ascii_alphanumeric()).unwrap_or(true);
        let boundary_after = after.map(|c| !c.is_ascii_alphanumeric()).unwrap_or(true);
        if boundary_before && boundary_after {
            return true;
        }
        offset = end;
        if offset >= text.len() {
            break;
        }
    }
    false
}

/// Match an event to a base ticker or Binance contract, with optional aliases.
pub fn matches_ticker(event: &SentimentEvent, symbol: &str, aliases: &[String]) -> bool {
    let normalized = event.normalized();
    let target = normalize_ticker(symbol);
    if target.is_empty() {
        return false;
    }
    let mut candidates = vec![target.clone(), normalize_symbol(symbol)];
    candidates.extend(aliases.iter().flat_map(|alias| {
        let raw = normalize_symbol(alias);
        [raw.clone(), normalize_ticker(&raw)]
    }));
    candidates.sort();
    candidates.dedup();

    let mut explicit = std::iter::once(normalized.symbol.as_str())
        .chain(normalized.symbols.iter().map(String::as_str));
    if explicit.any(|value| {
        let ticker = normalize_ticker(value);
        ticker == target
            || candidates
                .iter()
                .any(|candidate| normalize_ticker(candidate) == ticker)
    }) {
        return true;
    }
    let haystack = format!("{} {}", normalized.title, normalized.text);
    candidates
        .iter()
        .any(|candidate| contains_token(&haystack, candidate))
}

pub fn event_mentions_symbol(event: &SentimentEvent, symbol: &str, aliases: &[String]) -> bool {
    matches_ticker(event, symbol, aliases)
}

pub fn extract_tickers(text: &str) -> Vec<String> {
    let mut tickers = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    for (index, marker) in chars.iter().enumerate() {
        if *marker != '$' && *marker != '#' {
            continue;
        }
        let mut end = index + 1;
        while end < chars.len() && chars[end].is_ascii_alphanumeric() && end - index <= 15 {
            end += 1;
        }
        if end.saturating_sub(index + 1) >= 2 {
            let raw: String = chars[index + 1..end].iter().collect();
            if raw
                .chars()
                .next()
                .is_some_and(|character| character.is_ascii_alphabetic())
            {
                tickers.push(normalize_ticker(&raw));
            }
        }
    }
    tickers.sort();
    tickers.dedup();
    tickers
}

/// Aggregate events that explicitly mention `symbol` or one of its aliases.
pub fn aggregate_sentiment(
    symbol: &str,
    events: &[SentimentEvent],
    aliases: &[String],
    now: i64,
    config: &SentimentConfig,
) -> SentimentAggregate {
    let symbol = normalize_ticker(symbol);
    let unique = deduplicate_events_with_config(events, config);
    let mut weighted_sum = 0.0;
    let mut weight_total = 0.0;
    let mut positive_count = 0;
    let mut negative_count = 0;
    let mut neutral_count = 0;
    let mut sources = HashSet::new();
    let mut latest_event_at = 0;
    let mut event_count = 0;

    for event in unique {
        if !matches_ticker(&event, &symbol, aliases) {
            continue;
        }
        let weight = event_weight(&event, now, config);
        if weight <= 0.0 {
            continue;
        }
        event_count += 1;
        weight_total += weight;
        weighted_sum += event.sentiment * weight;
        latest_event_at = latest_event_at.max(event.published_at);
        sources.insert(if event.provider.is_empty() {
            event.author.to_ascii_lowercase()
        } else {
            event.provider.to_ascii_lowercase()
        });
        match event.sentiment.partial_cmp(&0.1).unwrap_or(Ordering::Equal) {
            Ordering::Greater => positive_count += 1,
            Ordering::Less if event.sentiment < -0.1 => negative_count += 1,
            _ => neutral_count += 1,
        }
    }

    let score = if weight_total > 0.0 {
        (weighted_sum / weight_total).clamp(-1.0, 1.0)
    } else {
        0.0
    };
    let source_diversity = (sources.len() as f64 / 4.0).clamp(0.0, 1.0);
    let evidence_depth = (1.0 - (-weight_total / 2.5).exp()).clamp(0.0, 1.0);
    let directional_consistency = if weight_total > 0.0 {
        (weighted_sum.abs() / weight_total).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let quality = (evidence_depth
        * (0.55 + source_diversity * 0.45)
        * (0.65 + directional_consistency * 0.35))
        .clamp(0.0, 1.0);
    let label = if event_count == 0 || quality < 0.2 {
        "unconfirmed"
    } else if score >= 0.2 {
        "bullish"
    } else if score <= -0.2 {
        "bearish"
    } else {
        "neutral"
    };

    SentimentAggregate {
        symbol,
        score,
        quality,
        label: label.to_string(),
        event_count,
        weighted_event_count: weight_total,
        positive_count,
        negative_count,
        neutral_count,
        unique_sources: sources.len(),
        latest_event_at,
        as_of: now,
    }
}

pub fn aggregate_events(
    symbol: &str,
    events: &[SentimentEvent],
    aliases: &[String],
    now: i64,
    config: &SentimentConfig,
) -> SentimentAggregate {
    aggregate_sentiment(symbol, events, aliases, now, config)
}

pub fn aggregate_for_symbol(
    symbol: &str,
    events: &[SentimentEvent],
    aliases: &[String],
    now: i64,
) -> SentimentAggregate {
    aggregate_sentiment(symbol, events, aliases, now, &SentimentConfig::default())
}

fn value_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.trim().to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn first_string(object: &Map<String, Value>, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value_string(object.get(*key)))
        .unwrap_or_default()
}

fn first_number(object: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        let value = object.get(*key)?;
        match value {
            Value::Number(number) => number.as_f64(),
            Value::String(value) => value.trim().parse::<f64>().ok(),
            _ => None,
        }
    })
}

fn parse_timestamp(value: Option<&Value>) -> i64 {
    let Some(value) = value else { return 0 };
    if let Some(number) = value.as_i64() {
        return normalize_timestamp(number);
    }
    let Some(raw) = value_string(Some(value)) else {
        return 0;
    };
    if let Ok(number) = raw.parse::<i64>() {
        return normalize_timestamp(number);
    }
    if let Ok(parsed) = DateTime::parse_from_rfc3339(&raw) {
        return parsed.timestamp();
    }
    if let Ok(parsed) = DateTime::parse_from_rfc2822(&raw) {
        return parsed.timestamp();
    }
    for format in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(&raw, format) {
            return Utc.from_utc_datetime(&parsed).timestamp();
        }
        if let Ok(parsed) = NaiveDate::parse_from_str(&raw, format) {
            return parsed
                .and_hms_opt(0, 0, 0)
                .map(|date| Utc.from_utc_datetime(&date).timestamp())
                .unwrap_or(0);
        }
    }
    0
}

fn normalize_timestamp(timestamp: i64) -> i64 {
    if timestamp.abs() >= 10_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

fn parse_u64(value: Option<&Value>) -> u64 {
    let Some(value) = value else { return 0 };
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_f64().map(|v| v.max(0.0) as u64))
            .unwrap_or(0),
        Value::String(value) => value
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| v.max(0.0) as u64)
            .unwrap_or(0),
        _ => 0,
    }
}

fn parse_symbols(value: Option<&Value>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    let mut symbols = match value {
        Value::Array(values) => values
            .iter()
            .filter_map(|item| value_string(Some(item)))
            .collect(),
        Value::String(value) => value
            .split(|character: char| character == ',' || character.is_whitespace())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    };
    symbols = symbols
        .into_iter()
        .map(|symbol| normalize_symbol(&symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect();
    symbols.sort();
    symbols.dedup();
    symbols
}

fn parse_sentiment(object: &Map<String, Value>) -> f64 {
    let mut value = first_number(
        object,
        &["sentiment", "polarity", "sentiment_score", "score"],
    );
    if value.is_none() {
        if let Some(Value::Object(sentiment)) = object.get("sentiment") {
            value = first_number(sentiment, &["score", "polarity", "value"]);
        }
    }
    if let Some(value) = value {
        return normalize_polarity(value);
    }
    let label = first_string(object, &["sentiment_label", "label"]).to_ascii_lowercase();
    if ["bullish", "positive", "pos", "good"]
        .iter()
        .any(|item| label.contains(item))
    {
        0.5
    } else if ["bearish", "negative", "neg", "bad"]
        .iter()
        .any(|item| label.contains(item))
    {
        -0.5
    } else {
        0.0
    }
}

fn parse_engagement(object: &Map<String, Value>) -> EngagementMetrics {
    let metrics = object
        .get("engagement")
        .or_else(|| object.get("metrics"))
        .or_else(|| object.get("public_metrics"))
        .and_then(Value::as_object);
    let get = |keys: &[&str]| -> u64 {
        keys.iter()
            .find_map(|key| {
                metrics
                    .and_then(|item| item.get(*key))
                    .or_else(|| object.get(*key))
            })
            .map(|value| parse_u64(Some(value)))
            .unwrap_or(0)
    };
    EngagementMetrics {
        likes: get(&["likes", "like_count", "favorite_count"]),
        reposts: get(&["reposts", "retweets", "retweet_count", "shares"]),
        replies: get(&["replies", "reply_count", "comments"]),
        quotes: get(&["quotes", "quote_count"]),
        views: get(&["views", "view_count", "impressions"]),
        followers: get(&["followers", "followers_count"]),
    }
}

fn parse_event_value(value: &Value, provider: &NewsProvider) -> Result<SentimentEvent> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("sentiment event must be a JSON object"))?;
    let mut event = SentimentEvent {
        id: first_string(object, &["id", "guid", "uuid", "tweet_id", "article_id"]),
        symbol: first_string(object, &["symbol", "ticker", "coin"]),
        symbols: parse_symbols(object.get("symbols").or_else(|| object.get("tickers"))),
        title: first_string(object, &["title", "headline", "name"]),
        text: first_string(object, &["text", "content", "body", "description"]),
        url: first_string(object, &["url", "link", "permalink"]),
        author: first_string(
            object,
            &["author", "username", "user", "handle", "screen_name"],
        ),
        provider: first_string(object, &["provider", "source", "source_name"]),
        source_kind: first_string(object, &["source_kind", "source_type", "kind"]),
        source_credibility: first_number(
            object,
            &["source_credibility", "sourceCredibility", "credibility"],
        )
        .unwrap_or_else(|| provider.effective_credibility()),
        event_type: first_string(object, &["event_type", "category", "type"]),
        published_at: parse_timestamp(
            [
                "published_at",
                "publishedAt",
                "created_at",
                "createdAt",
                "timestamp",
                "time",
                "date",
            ]
            .iter()
            .find_map(|key| object.get(*key)),
        ),
        sentiment: parse_sentiment(object),
        confidence: first_number(object, &["confidence", "certainty"]).unwrap_or(0.5),
        engagement: parse_engagement(object),
        bot_probability: first_number(
            object,
            &["bot_probability", "botProbability", "spam_probability"],
        )
        .unwrap_or(0.0),
        language: first_string(object, &["language", "lang"]),
    };
    if event.provider.is_empty() {
        event.provider = if provider.id.is_empty() {
            provider.name.clone()
        } else {
            provider.id.clone()
        };
    }
    if event.source_kind.is_empty() {
        event.source_kind = provider.kind.clone();
    }
    event = event.normalized();
    if event.id.is_empty() {
        event.id = event.fingerprint();
    }
    Ok(event)
}

fn looks_like_event(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        [
            "title",
            "headline",
            "text",
            "content",
            "body",
            "url",
            "link",
            "published_at",
            "created_at",
        ]
        .iter()
        .any(|key| object.contains_key(*key))
    })
}

fn collect_records(value: &Value, records: &mut Vec<Value>) {
    match value {
        Value::Array(values) => {
            for value in values {
                if looks_like_event(value) {
                    records.push(value.clone());
                } else {
                    collect_records(value, records);
                }
            }
        }
        Value::Object(object) => {
            let mut found_container = false;
            for key in ["events", "data", "articles", "results", "statuses", "items"] {
                if let Some(value) = object.get(key) {
                    found_container = true;
                    collect_records(value, records);
                }
            }
            if !found_container && looks_like_event(value) {
                records.push(value.clone());
            }
        }
        _ => {}
    }
}

/// Parse an API response that is either an array of events or a wrapper with
/// `data`, `events`, `articles`, `results`, `statuses` or `items`.
pub fn parse_events_json(body: &str, provider: &NewsProvider) -> Result<Vec<SentimentEvent>> {
    let value: Value = serde_json::from_str(body).context("invalid sentiment API JSON")?;
    let mut records = Vec::new();
    collect_records(&value, &mut records);
    if records.is_empty() && !value.is_null() && !value.as_array().is_some_and(Vec::is_empty) {
        return Err(anyhow!("sentiment API response contains no event records"));
    }
    records
        .iter()
        .map(|record| parse_event_value(record, provider))
        .collect()
}

pub fn parse_api_response(body: &str, provider: &NewsProvider) -> Result<Vec<SentimentEvent>> {
    parse_events_json(body, provider)
}

/// Optional remote sources. Empty credentials are treated as a disabled source;
/// the scanner remains usable with public market data and reports that sentiment
/// evidence is unavailable rather than inventing a score.
#[derive(Debug, Clone, Default)]
pub struct SentimentFetchConfig {
    pub x_bearer_token: String,
    pub x_search_url: String,
    pub news_json_url: String,
    pub news_api_key: String,
    pub rss_url: String,
}

impl SentimentFetchConfig {
    pub fn from_env() -> Self {
        Self {
            x_bearer_token: env::var("GQT_X_BEARER_TOKEN").unwrap_or_default(),
            x_search_url: env::var("GQT_X_SEARCH_URL").unwrap_or_else(|_| {
                "https://api.twitter.com/2/tweets/search/recent?query=crypto%20lang%3Aen%20-is%3Aretweet&tweet.fields=created_at,public_metrics&max_results=100".into()
            }),
            news_json_url: env::var("GQT_NEWS_JSON_URL").unwrap_or_default(),
            news_api_key: env::var("GQT_NEWS_API_KEY").unwrap_or_default(),
            rss_url: env::var("GQT_NEWS_RSS_URL").unwrap_or_else(|_| {
                "https://www.coindesk.com/arc/outboundfeeds/rss/,https://cointelegraph.com/rss".into()
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        (!self.x_bearer_token.trim().is_empty() && !self.x_search_url.trim().is_empty())
            || !self.news_json_url.trim().is_empty()
            || !self.rss_url.trim().is_empty()
    }
}

/// Fetch configured X/news/RSS sources. X and news APIs are intentionally
/// configured by URL so providers can enforce their own terms and rate limits.
pub fn fetch_remote_events(
    client: &Client,
    config: &SentimentFetchConfig,
) -> Result<Vec<SentimentEvent>> {
    let mut events = Vec::new();
    if !config.x_bearer_token.trim().is_empty() && !config.x_search_url.trim().is_empty() {
        let response = client
            .get(&config.x_search_url)
            .bearer_auth(config.x_bearer_token.trim())
            .header("Accept", "application/json")
            .send()
            .context("X API 请求失败")?
            .error_for_status()
            .context("X API 返回错误状态")?;
        let body = response.text().context("无法读取 X API 响应")?;
        let provider = NewsProvider::new("x", "X/Twitter", "social");
        let mut parsed = parse_api_response(&body, &provider)?;
        for event in &mut parsed {
            if event.symbols.is_empty() {
                event.symbols = extract_tickers(&format!("{} {}", event.title, event.text));
            }
            apply_lexical_classification(event);
        }
        events.extend(parsed);
    }
    if !config.news_json_url.trim().is_empty() {
        let mut request = client
            .get(&config.news_json_url)
            .header("Accept", "application/json");
        if !config.news_api_key.trim().is_empty() {
            request = request.header("X-API-Key", config.news_api_key.trim());
        }
        let body = request
            .send()
            .context("新闻 API 请求失败")?
            .error_for_status()
            .context("新闻 API 返回错误状态")?
            .text()
            .context("无法读取新闻 API 响应")?;
        let provider = NewsProvider::new("news", "授权新闻源", "news");
        let mut parsed = parse_api_response(&body, &provider)?;
        for event in &mut parsed {
            if event.symbols.is_empty() {
                event.symbols = extract_tickers(&format!("{} {}", event.title, event.text));
            }
            apply_lexical_classification(event);
        }
        events.extend(parsed);
    }
    let mut rss_errors = Vec::new();
    for rss_url in config
        .rss_url
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
    {
        let result = client
            .get(rss_url)
            .header("Accept", "application/rss+xml, application/xml, text/xml")
            .send()
            .and_then(|response| response.error_for_status())
            .and_then(|response| response.text());
        match result {
            Ok(body) => {
                let provider = if rss_url.contains("cointelegraph") {
                    "cointelegraph"
                } else if rss_url.contains("coindesk") {
                    "coindesk"
                } else {
                    "rss"
                };
                let mut parsed = parse_rss_events(&body);
                for event in &mut parsed {
                    event.provider = provider.into();
                }
                events.extend(parsed);
            }
            Err(error) => rss_errors.push(format!("{rss_url}: {error}")),
        }
    }
    if events.is_empty() && !rss_errors.is_empty() {
        return Err(anyhow!("RSS 新闻请求失败: {}", rss_errors.join("; ")));
    }
    Ok(deduplicate_events(&events))
}

fn apply_lexical_classification(event: &mut SentimentEvent) {
    let text = format!("{} {}", event.title, event.text).to_ascii_lowercase();
    let positive = [
        "partnership",
        "launch",
        "listing",
        "approval",
        "approved",
        "upgrade",
        "adoption",
        "bullish",
        "buyback",
        "etf inflow",
        "integration",
        "skyrocket",
        "surge",
        "surging",
        "inflow",
        "tops",
        "上线",
        "合作",
        "升级",
        "通过",
    ];
    let negative = [
        "hack",
        "exploit",
        "stolen",
        "lawsuit",
        "delist",
        "delisting",
        "scam",
        "fraud",
        "halt",
        "outage",
        "bankrupt",
        "bearish",
        "dump",
        "plunge",
        "plunges",
        "漏洞",
        "被盗",
        "下架",
        "诈骗",
    ];
    let positives = positive.iter().filter(|word| text.contains(**word)).count() as f64;
    let negatives = negative.iter().filter(|word| text.contains(**word)).count() as f64;
    if positives + negatives > 0.0 {
        event.sentiment = ((positives - negatives) / (positives + negatives)).clamp(-1.0, 1.0);
        event.confidence = (0.45 + (positives + negatives) * 0.08).clamp(0.45, 0.82);
    }
    if event.event_type.is_empty() {
        event.event_type = if negatives > 0.0 {
            "risk"
        } else if positives > 0.0 {
            "promotion"
        } else {
            "commentary"
        }
        .into();
    }
}

fn parse_rss_events(body: &str) -> Vec<SentimentEvent> {
    let mut events = Vec::new();
    for item in body.split("<item").skip(1) {
        let title = xml_tag(item, "title");
        let description = xml_tag(item, "description");
        let url = xml_tag(item, "link");
        let published = xml_tag(item, "pubDate");
        if title.is_empty() && description.is_empty() {
            continue;
        }
        let mut event = SentimentEvent {
            id: url.clone(),
            title,
            text: description,
            url,
            provider: "rss".into(),
            source_kind: "news".into(),
            published_at: parse_timestamp(Some(&Value::String(published))),
            ..Default::default()
        };
        event.symbols = extract_tickers(&format!("{} {}", event.title, event.text));
        apply_lexical_classification(&mut event);
        events.push(event.normalized());
    }
    events
}

fn xml_tag(value: &str, tag: &str) -> String {
    let open = format!("<{tag}");
    let Some(tag_start) = value.find(&open) else {
        return String::new();
    };
    let Some(open_end_offset) = value[tag_start..].find('>') else {
        return String::new();
    };
    let content_start = tag_start + open_end_offset + 1;
    let close = format!("</{tag}>");
    let Some(end) = value[content_start..].find(&close) else {
        return String::new();
    };
    value[content_start..content_start + end]
        .replace("<![CDATA[", "")
        .replace("]]>", "")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .trim()
        .to_string()
}

/// Small SQLite repository for restart-safe ingestion and later paper-trading
/// evaluation.  Payloads remain JSON so adding fields is backwards compatible.
pub struct SentimentStore {
    connection: Connection,
}

impl SentimentStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("cannot create sentiment data directory")?;
        }
        let connection = Connection::open(path).context("cannot open sentiment database")?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS sentiment_events (
                 event_id TEXT PRIMARY KEY,
                 fingerprint TEXT NOT NULL,
                 symbol TEXT NOT NULL DEFAULT '',
                 published_at INTEGER NOT NULL,
                 provider TEXT NOT NULL DEFAULT '',
                 payload_json TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_sentiment_symbol_time
             ON sentiment_events(symbol, published_at DESC);
             CREATE INDEX IF NOT EXISTS idx_sentiment_provider_time
             ON sentiment_events(provider, published_at DESC);",
        )?;
        Ok(Self { connection })
    }

    pub fn upsert_event(&self, event: &SentimentEvent) -> Result<bool> {
        let event = event.normalized();
        let payload = serde_json::to_string(&event).context("cannot encode sentiment event")?;
        let changed = self.connection.execute(
            "INSERT INTO sentiment_events
               (event_id, fingerprint, symbol, published_at, provider, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(event_id) DO UPDATE SET
               fingerprint = excluded.fingerprint,
               symbol = excluded.symbol,
               published_at = excluded.published_at,
               provider = excluded.provider,
               payload_json = excluded.payload_json",
            params![
                event.storage_id(),
                event.fingerprint(),
                event.symbol,
                event.published_at,
                event.provider,
                payload,
                Utc::now().timestamp(),
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn upsert_events(&self, events: &[SentimentEvent]) -> Result<usize> {
        events.iter().try_fold(0, |count, event| {
            Ok(count + usize::from(self.upsert_event(event)?))
        })
    }

    pub fn load_events(
        &self,
        symbol: Option<&str>,
        since: i64,
        limit: usize,
    ) -> Result<Vec<SentimentEvent>> {
        let limit = limit.clamp(1, 100_000) as i64;
        let mut events = Vec::new();
        if let Some(symbol) = symbol {
            let mut statement = self.connection.prepare(
                "SELECT payload_json FROM sentiment_events
                 WHERE published_at >= ?1 AND (symbol = '' OR symbol = ?2)
                 ORDER BY published_at DESC LIMIT ?3",
            )?;
            let rows = statement
                .query_map(params![since, normalize_symbol(symbol), limit], |row| {
                    row.get::<_, String>(0)
                })?;
            for row in rows {
                events.push(serde_json::from_str(&row?).context("invalid stored sentiment event")?);
            }
        } else {
            let mut statement = self.connection.prepare(
                "SELECT payload_json FROM sentiment_events
                 WHERE published_at >= ?1 ORDER BY published_at DESC LIMIT ?2",
            )?;
            let rows = statement.query_map(params![since, limit], |row| row.get::<_, String>(0))?;
            for row in rows {
                events.push(serde_json::from_str(&row?).context("invalid stored sentiment event")?);
            }
        }
        Ok(events)
    }

    pub fn aggregate(
        &self,
        symbol: &str,
        aliases: &[String],
        now: i64,
        config: &SentimentConfig,
    ) -> Result<SentimentAggregate> {
        let since = if config.max_age_secs > 0 {
            now.saturating_sub(config.max_age_secs)
        } else {
            0
        };
        let events = self.load_events(None, since, 100_000)?;
        Ok(aggregate_sentiment(symbol, &events, aliases, now, config))
    }

    pub fn prune_before(&self, timestamp: i64) -> Result<usize> {
        Ok(self.connection.execute(
            "DELETE FROM sentiment_events WHERE published_at < ?1",
            params![timestamp],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn event(
        id: &str,
        symbol: &str,
        text: &str,
        sentiment: f64,
        published_at: i64,
    ) -> SentimentEvent {
        SentimentEvent {
            id: id.to_string(),
            symbol: symbol.to_string(),
            text: text.to_string(),
            provider: "binance".to_string(),
            source_kind: "exchange".to_string(),
            sentiment,
            confidence: 1.0,
            published_at,
            ..Default::default()
        }
    }

    #[test]
    fn normalization_and_dedupe_prefer_credible_source() {
        let first = event("same", "BTC/USDT", "  BTC   listing  ", 0.2, 100);
        let mut second = first.clone();
        second.provider = "unknown-user".into();
        second.source_kind = "user".into();
        second.engagement.likes = 1_000_000;
        let result = deduplicate_events(&[second, first.clone()]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].provider, "binance");
        assert_eq!(result[0].symbol, "BTCUSDT");
        assert_eq!(normalize_text("a   b\n c"), "a b c");
    }

    #[test]
    fn ticker_matching_uses_boundaries_and_aliases() {
        let item = event(
            "1",
            "",
            "The $BTC move is stronger than the solution",
            0.4,
            100,
        );
        assert!(matches_ticker(&item, "BTCUSDT", &[]));
        assert!(!matches_ticker(&item, "SOLUSDT", &[]));
        let item = event("2", "", "The project announced a partnership", 0.4, 100);
        assert!(matches_ticker(&item, "ETHUSDT", &["project".to_string()]));
    }

    #[test]
    fn decay_and_bot_discount_reduce_weight() {
        let now = 100 * 60 * 60;
        let fresh = event("fresh", "BTC", "fresh", 1.0, now);
        let old = event("old", "BTC", "old", 1.0, now - DEFAULT_HALF_LIFE_SECS);
        assert!((time_decay(fresh.published_at, now, DEFAULT_HALF_LIFE_SECS) - 1.0).abs() < 1e-9);
        assert!((time_decay(old.published_at, now, DEFAULT_HALF_LIFE_SECS) - 0.5).abs() < 1e-9);
        let mut spam = fresh.clone();
        spam.bot_probability = 1.0;
        assert!(engagement_adjustment(&spam) < engagement_adjustment(&fresh));
    }

    #[test]
    fn aggregate_reports_direction_and_quality() {
        let now = 10_000;
        let events = vec![
            event("1", "BTC", "$BTC adoption news", 0.8, now - 30),
            event("2", "BTC", "BTC exchange announcement", 0.6, now - 60),
        ];
        let aggregate =
            aggregate_sentiment("BTCUSDT", &events, &[], now, &SentimentConfig::default());
        assert_eq!(aggregate.symbol, "BTC");
        assert_eq!(aggregate.event_count, 2);
        assert_eq!(aggregate.label, "bullish");
        assert!(aggregate.score > 0.5);
        assert!(aggregate.quality > 0.2);
    }

    #[test]
    fn parse_wrapped_api_response_and_timestamp() {
        let provider = NewsProvider::new("x", "X API", "social");
        let body = r#"{"data":[{"id":"42","text":"$ETH upgrade","created_at":"2025-01-02T03:04:05Z","sentiment":80,"confidence":0.9,"public_metrics":{"like_count":12,"retweet_count":3}}]}"#;
        let events = parse_events_json(body, &provider).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "42");
        assert_eq!(events[0].published_at, 1_735_787_045);
        assert_eq!(events[0].engagement.likes, 12);
        assert!((events[0].sentiment - 0.8).abs() < 1e-9);
        assert_eq!(events[0].provider, "x");
    }

    #[test]
    fn rss_tags_are_extracted_after_item_prefix() {
        let body = "<item><guid>rss-1</guid><title>BTC listing</title><link>https://example.test/a</link><description><![CDATA[$BTC is listed]]></description><pubDate>Thu, 02 Jan 2025 03:04:05 GMT</pubDate></item>";
        let events = parse_rss_events(body);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].title, "BTC listing");
        assert_eq!(events[0].url, "example.test/a");
        assert_eq!(events[0].published_at, 1_735_787_045);
        assert_eq!(events[0].symbols, vec!["BTC"]);
    }

    #[test]
    fn sqlite_round_trip() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("gqt-sentiment-{suffix}.db"));
        let store = SentimentStore::open(&path).unwrap();
        let item = event("db-1", "BTC", "stored", 0.3, 100);
        assert!(store.upsert_event(&item).unwrap());
        let loaded = store.load_events(None, 0, 10).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "db-1");
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
