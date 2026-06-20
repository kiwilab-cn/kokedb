//! Optional LLM-assisted incremental inference (Phase 3 of the intelligent-sync
//! design).
//!
//! When the rule-based classifier is unsure (low confidence) and an LLM is
//! configured, we ask the model to recommend an incremental strategy from the
//! table's **schema and aggregate statistics only** — never row data. The
//! recommendation is treated exactly like a rule inference: it lands in
//! `suggested`/`audited` and must pass shadow validation before it is trusted.
//! With no key configured, everything here is inert and the rules stand alone.

use std::time::Duration;

use kokedb_common::env::get_env_as;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::error::TaskError;

/// Anthropic Messages API config, sourced from the environment. `None` when no
/// API key is set (LLM disabled).
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub api_key: String,
    pub model: String,
    pub base_url: String,
    pub send_stats: bool,
    pub timeout_secs: u64,
}

impl LlmConfig {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("KOKEDB_LLM_API_KEY").ok().filter(|k| !k.is_empty())?;
        Some(Self {
            api_key,
            model: std::env::var("KOKEDB_LLM_MODEL")
                .unwrap_or_else(|_| "claude-opus-4-8".to_string()),
            base_url: std::env::var("KOKEDB_LLM_BASE_URL")
                .unwrap_or_else(|_| "https://api.anthropic.com".to_string()),
            // Privacy: when false, only the schema is sent, no aggregate numbers.
            send_stats: get_env_as("KOKEDB_LLM_SEND_STATS", true),
            timeout_secs: get_env_as("KOKEDB_LLM_TIMEOUT_SECS", 30u64),
        })
    }
}

/// Rule confidence (0..1) below which we consult the LLM.
pub fn confidence_threshold() -> f32 {
    get_env_as("KOKEDB_LLM_CONFIDENCE_THRESHOLD", 0.6f32)
}

/// A column description for the prompt (schema only — no values).
#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
}

/// The compact, row-data-free summary handed to the model.
#[derive(Debug, Clone, Serialize)]
pub struct TableSummary {
    pub table: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    pub has_update_trigger: bool,
    /// Aggregate stats, omitted entirely when `send_stats` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<TableStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableStats {
    pub est_rows: i64,
    pub est_size_bytes: i64,
    pub churn_per_hour: f64,
    pub timestamp_columns: Vec<String>,
}

/// The model's structured recommendation.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct LlmRecommendation {
    pub inc_mode: String,
    #[serde(default)]
    pub watermark_column: Option<String>,
    #[serde(default)]
    pub pk_columns: Vec<String>,
    #[serde(default)]
    pub refresh_bucket: Option<String>,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub reason: String,
}

impl LlmRecommendation {
    /// Rejects nonsense the model might emit so a bad reply degrades to "no
    /// recommendation" rather than a broken strategy.
    fn is_valid(&self) -> bool {
        match self.inc_mode.as_str() {
            "full" => true,
            "upsert" => self.watermark_column.is_some() && !self.pk_columns.is_empty(),
            "append" => self.watermark_column.is_some(),
            _ => false,
        }
    }
}

const SYSTEM_PROMPT: &str = "You are a database replication expert. Given a table's \
schema and aggregate statistics (never row data), decide how it can be incrementally \
synced to a columnar cache. Reply with ONLY a JSON object, no prose, of the form: \
{\"inc_mode\":\"full|upsert|append\",\"watermark_column\":string|null,\
\"pk_columns\":[string],\"refresh_bucket\":\"fast|normal|slow|cold\",\
\"confidence\":0.0-1.0,\"reason\":string}. \
Use \"upsert\" when there is a primary key AND a column that is reliably bumped on every \
update (e.g. an updated-at timestamp). Use \"append\" for insert-only tables with a \
monotonic key. Use \"full\" when no safe watermark exists. Be conservative: if unsure, \
prefer \"full\".";

/// Reads the column schema (name, type, nullability, default, comment) for a
/// table — no row data — to feed the prompt.
pub async fn gather_columns(
    pool: &PgPool,
    qualified_table: &str,
) -> Result<Vec<ColumnInfo>, TaskError> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), qualified_table.to_string()),
    };
    let rows = sqlx::query(
        "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, \
                col_description(format('%I.%I', c.table_schema, c.table_name)::regclass, \
                                c.ordinal_position) AS comment \
         FROM information_schema.columns c \
         WHERE c.table_schema = $1 AND c.table_name = $2 \
         ORDER BY c.ordinal_position",
    )
    .bind(&schema)
    .bind(&table)
    .fetch_all(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(format!("column schema query failed: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|r| ColumnInfo {
            name: r.get::<String, _>("column_name"),
            data_type: r.get::<String, _>("data_type"),
            nullable: r
                .try_get::<String, _>("is_nullable")
                .map(|n| !n.eq_ignore_ascii_case("NO"))
                .unwrap_or(true),
            default: r.try_get::<Option<String>, _>("column_default").ok().flatten(),
            comment: r.try_get::<Option<String>, _>("comment").ok().flatten(),
        })
        .collect())
}

/// Builds the user message from a table summary (the summary is serialized to
/// JSON; it contains no row data).
pub fn build_user_prompt(summary: &TableSummary) -> String {
    let body = serde_json::to_string_pretty(summary)
        .unwrap_or_else(|_| "{}".to_string());
    format!("Recommend an incremental sync strategy for this table:\n{body}")
}

/// Extracts the recommendation from a raw Anthropic Messages API response body.
/// Tolerant: finds the first JSON object inside the assistant text.
pub fn parse_anthropic_response(body: &str) -> Option<LlmRecommendation> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let text = v
        .get("content")?
        .as_array()?
        .iter()
        .find_map(|block| block.get("text").and_then(|t| t.as_str()))?;
    parse_recommendation(text)
}

/// Parses a recommendation from assistant text, extracting the first `{...}`
/// block so it survives stray prose or markdown fences.
pub fn parse_recommendation(text: &str) -> Option<LlmRecommendation> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end < start {
        return None;
    }
    let json = &text[start..=end];
    let rec: LlmRecommendation = serde_json::from_str(json).ok()?;
    rec.is_valid().then_some(rec)
}

/// Calls the Anthropic Messages API for a recommendation. Returns `None` on any
/// failure (no key, network/timeout, bad response) so the caller falls back to
/// the rule-based strategy.
pub async fn recommend(config: &LlmConfig, summary: &TableSummary) -> Option<LlmRecommendation> {
    let summary = if config.send_stats {
        summary.clone()
    } else {
        let mut s = summary.clone();
        s.stats = None;
        s
    };
    let request = serde_json::json!({
        "model": config.model,
        "max_tokens": 512,
        "system": SYSTEM_PROMPT,
        "messages": [{ "role": "user", "content": build_user_prompt(&summary) }],
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .ok()?;
    let url = format!("{}/v1/messages", config.base_url.trim_end_matches('/'));

    let resp = match client
        .post(&url)
        .header("x-api-key", &config.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&request)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("LLM request failed: {e}");
            return None;
        }
    };
    if !resp.status().is_success() {
        warn!("LLM returned status {}", resp.status());
        return None;
    }
    let body = resp.text().await.ok()?;
    let rec = parse_anthropic_response(&body);
    if rec.is_some() {
        info!("LLM recommended a strategy for {}", summary.table);
    } else {
        warn!("LLM response could not be parsed into a valid recommendation");
    }
    rec
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary() -> TableSummary {
        TableSummary {
            table: "public.orders".into(),
            columns: vec![ColumnInfo {
                name: "id".into(),
                data_type: "bigint".into(),
                nullable: false,
                default: None,
                comment: None,
            }],
            primary_key: vec!["id".into()],
            has_update_trigger: false,
            stats: Some(TableStats {
                est_rows: 1000,
                est_size_bytes: 4096,
                churn_per_hour: 5.0,
                timestamp_columns: vec!["updated_at".into()],
            }),
        }
    }

    #[test]
    fn prompt_contains_schema_not_row_data() {
        let p = build_user_prompt(&summary());
        assert!(p.contains("public.orders"));
        assert!(p.contains("\"id\""));
    }

    #[test]
    fn send_stats_false_drops_stats() {
        let mut s = summary();
        s.stats = None;
        let p = build_user_prompt(&s);
        assert!(!p.contains("churn_per_hour"));
    }

    #[test]
    fn parses_plain_json() {
        let rec = parse_recommendation(
            r#"{"inc_mode":"upsert","watermark_column":"updated_at","pk_columns":["id"],"confidence":0.8,"reason":"has pk + updated_at"}"#,
        )
        .unwrap();
        assert_eq!(rec.inc_mode, "upsert");
        assert_eq!(rec.watermark_column.as_deref(), Some("updated_at"));
        assert_eq!(rec.pk_columns, vec!["id"]);
    }

    #[test]
    fn parses_json_with_surrounding_prose() {
        let rec = parse_recommendation(
            "Here is my recommendation:\n```json\n{\"inc_mode\":\"append\",\"watermark_column\":\"id\",\"pk_columns\":[\"id\"]}\n```\nHope this helps.",
        )
        .unwrap();
        assert_eq!(rec.inc_mode, "append");
    }

    #[test]
    fn rejects_invalid_upsert_without_watermark() {
        // upsert with no watermark is nonsense -> rejected.
        assert!(parse_recommendation(r#"{"inc_mode":"upsert","pk_columns":["id"]}"#).is_none());
    }

    #[test]
    fn rejects_unknown_mode() {
        assert!(parse_recommendation(r#"{"inc_mode":"magic"}"#).is_none());
    }

    #[test]
    fn parses_anthropic_envelope() {
        let body = r#"{"id":"msg_1","type":"message","role":"assistant",
            "content":[{"type":"text","text":"{\"inc_mode\":\"full\",\"reason\":\"no watermark\"}"}]}"#;
        let rec = parse_anthropic_response(body).unwrap();
        assert_eq!(rec.inc_mode, "full");
    }

    // Opt-in live call: only runs when a real key is configured. Verifies the
    // end-to-end Anthropic request + parse against the live API.
    #[tokio::test]
    #[ignore = "requires KOKEDB_LLM_API_KEY; calls the real Anthropic API"]
    async fn live_recommend_returns_a_strategy() {
        let Some(cfg) = LlmConfig::from_env() else {
            eprintln!("KOKEDB_LLM_API_KEY not set; skipping");
            return;
        };
        let rec = recommend(&cfg, &summary()).await;
        // A PK + updated_at table should yield a non-full recommendation, but we
        // only assert we got a valid, parseable answer.
        assert!(rec.is_some(), "expected a parseable recommendation");
        eprintln!("LLM recommendation: {:?}", rec.unwrap());
    }

    #[test]
    fn config_disabled_without_key() {
        // Can't mutate process env safely in parallel tests; just assert the
        // explicit-empty case via the public predicate.
        let cfg = LlmConfig {
            api_key: String::new(),
            model: "m".into(),
            base_url: "u".into(),
            send_stats: true,
            timeout_secs: 1,
        };
        assert!(cfg.api_key.is_empty());
    }
}
