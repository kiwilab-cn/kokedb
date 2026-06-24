//! PostgreSQL wire handlers bridging to kokedb's protocol-agnostic query path.
//!
//! Both the simple and the extended (Parse/Bind/Execute) query protocols are
//! supported. The extended protocol is implemented by textually substituting
//! bound `$N` parameters into the SQL — mirroring the MySQL server's prepared
//! path — then running it through the same plan/result-cache pipeline. Result
//! metadata for `Describe` is obtained by planning the statement (without
//! collecting any rows) and reading the output schema.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures::{stream, Sink, TryStreamExt};
use kokedb_common::hash::get_plan_hash;
use kokedb_common::spec::Plan;
use kokedb_query::binder::{parser, query};
use kokedb_query::context::{create_session_context, SharedServices};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::encode::{encode_batch, fields_for_schema, fields_with_format};

/// Bundles the handlers for one server. Cloned per connection (cheap: it just
/// holds shared service handles).
#[derive(Clone)]
pub struct KokedbHandlers {
    handler: Arc<KokedbQueryHandler>,
}

impl KokedbHandlers {
    pub fn new(shared: SharedServices) -> Self {
        Self {
            handler: Arc::new(KokedbQueryHandler {
                shared,
                parser: Arc::new(NoopQueryParser::new()),
            }),
        }
    }
}

impl PgWireServerHandlers for KokedbHandlers {
    // Startup/copy/error/cancel default to NoopHandler (startup noop = no auth,
    // like the MySQL server with no password configured).
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }
}

pub struct KokedbQueryHandler {
    shared: SharedServices,
    parser: Arc<NoopQueryParser>,
}

fn pg_err(msg: impl std::fmt::Display) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        msg.to_string(),
    )))
}

/// Extracts a `/*+ max_staleness(N) */` hint, mirroring the MySQL server so the
/// freshness contract works for PostgreSQL clients too.
fn parse_staleness_hint(sql: &str) -> Option<u64> {
    let lower = sql.to_ascii_lowercase();
    let key = "max_staleness(";
    let start = lower.find(key)? + key.len();
    let rel_end = sql[start..].find(')')?;
    sql[start..start + rel_end].trim().parse::<u64>().ok()
}

/// Escapes a string for use as a single-quoted SQL literal.
fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Replaces `$1`, `$2`, … placeholders in `sql` with the literals produced by
/// `lit(idx)` (1-based as written, 0-based to the callback). Placeholders inside
/// single-quoted strings or double-quoted identifiers are left untouched.
fn substitute_placeholders(
    sql: &str,
    mut lit: impl FnMut(usize) -> PgWireResult<String>,
) -> PgWireResult<String> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '\'' if !in_dquote => {
                in_squote = !in_squote;
                out.push(c);
                i += 1;
            }
            '"' if !in_squote => {
                in_dquote = !in_dquote;
                out.push(c);
                i += 1;
            }
            '$' if !in_squote && !in_dquote && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                let n: usize = sql[i + 1..j].parse().map_err(pg_err)?;
                if n == 0 {
                    return Err(pg_err("invalid parameter $0"));
                }
                out.push_str(&lit(n - 1)?);
                i = j;
            }
            _ => {
                // Push the whole UTF-8 char, not just one byte.
                let ch = sql[i..].chars().next().unwrap_or(c);
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    Ok(out)
}

/// Highest `$N` placeholder index that appears in `sql` outside string literals
/// and quoted identifiers (i.e. the number of parameters). Returns 0 if none.
fn placeholder_count(sql: &str) -> usize {
    let mut max = 0usize;
    let _ = substitute_placeholders(sql, |idx| {
        max = max.max(idx + 1);
        Ok(String::new())
    });
    max
}

/// SQL literal for one bound parameter, decoding from its wire type.
fn param_literal(portal: &Portal<String>, idx: usize) -> PgWireResult<String> {
    // A NULL parameter has no bytes.
    if portal.parameters.get(idx).map(|p| p.is_none()).unwrap_or(true) {
        return Ok("NULL".to_string());
    }
    let ty = portal
        .statement
        .parameter_types
        .get(idx)
        .and_then(|t| t.clone())
        .unwrap_or(Type::UNKNOWN);

    let opt = |v: Option<String>| v.unwrap_or_else(|| "NULL".to_string());
    if ty == Type::BOOL {
        Ok(opt(portal.parameter::<bool>(idx, &ty)?.map(|b| b.to_string())))
    } else if ty == Type::INT2 {
        Ok(opt(portal.parameter::<i16>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::INT4 {
        Ok(opt(portal.parameter::<i32>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::INT8 {
        Ok(opt(portal.parameter::<i64>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::FLOAT4 {
        Ok(opt(portal.parameter::<f32>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::FLOAT8 {
        Ok(opt(portal.parameter::<f64>(idx, &ty)?.map(|v| v.to_string())))
    } else {
        // Text, varchar, unknown, and anything else: decode as a string and
        // emit a quoted literal. Works for text-format params from any type.
        Ok(opt(portal
            .parameter::<String>(idx, &ty)?
            .map(|s| quote_literal(&s))))
    }
}

/// SQL `CAST(NULL AS <type>)` literal for a parameter at describe time, when no
/// value has been bound yet. The cast preserves the result schema's types.
fn null_literal_for_type(ty: &Type) -> String {
    let sql_ty = if *ty == Type::BOOL {
        "boolean"
    } else if *ty == Type::INT2 {
        "smallint"
    } else if *ty == Type::INT4 {
        "integer"
    } else if *ty == Type::INT8 {
        "bigint"
    } else if *ty == Type::FLOAT4 {
        "real"
    } else if *ty == Type::FLOAT8 {
        "double precision"
    } else if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::UNKNOWN {
        "text"
    } else {
        return "NULL".to_string();
    };
    format!("CAST(NULL AS {sql_ty})")
}

impl KokedbQueryHandler {
    /// Parses + hashes the SQL, going through the shared plan cache.
    fn prepare(&self, sql: &str) -> PgWireResult<(Plan, u128)> {
        let plan_cache = self.shared.plan_cache();
        if let Some(hit) = plan_cache.get(sql) {
            return Ok((hit.0.clone(), hit.1));
        }
        let plan = parser(sql).map_err(pg_err)?;
        let key = get_plan_hash(&plan).map_err(pg_err)?;
        plan_cache.insert(sql, Arc::new((plan.clone(), key)));
        Ok((plan, key))
    }

    /// Runs one statement through the kokedb query path and returns the result
    /// batches plus the output schema.
    async fn execute(&self, sql: &str) -> PgWireResult<(Vec<RecordBatch>, SchemaRef)> {
        let max_staleness = parse_staleness_hint(sql).filter(|&s| s > 0);
        let (plan, cache_key) = self.prepare(sql)?;

        let cacheable = matches!(plan, Plan::Query(_)) && max_staleness.is_none();
        let cache = self.shared.result_cache();

        if cacheable && cache.contains(cache_key).await {
            if let Ok(stream) = cache.get(cache_key).await {
                let schema = stream.schema();
                let batches = stream.try_collect::<Vec<_>>().await.map_err(pg_err)?;
                return Ok((batches, schema));
            }
        }

        let ctx = Arc::new(create_session_context(&self.shared).map_err(pg_err)?);
        let stream = query(ctx, &plan, cache_key, max_staleness)
            .await
            .map_err(pg_err)?;
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.map_err(pg_err)?;

        if cacheable && !batches.is_empty() {
            let _ = cache.insert(cache_key, &batches).await;
        }
        Ok((batches, schema))
    }

    /// Plans the statement and returns its output schema without executing it.
    /// Non-query statements have no row description, so an empty schema is
    /// returned for them.
    async fn output_schema(&self, sql: &str) -> PgWireResult<SchemaRef> {
        let (plan, cache_key) = self.prepare(sql)?;
        if !matches!(plan, Plan::Query(_)) {
            return Ok(Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        }
        let ctx = Arc::new(create_session_context(&self.shared).map_err(pg_err)?);
        let stream = query(ctx, &plan, cache_key, None).await.map_err(pg_err)?;
        Ok(stream.schema())
    }

    /// Builds a `Response::Query` or `Response::Execution` from result batches.
    fn respond(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        format: &Format,
    ) -> PgWireResult<Response> {
        if schema.fields().is_empty() {
            return Ok(Response::Execution(Tag::new("OK")));
        }
        let fields = fields_with_format(&schema, format);
        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(encode_batch(batch, &fields)?);
        }
        let row_stream = stream::iter(rows.into_iter().map(Ok));
        Ok(Response::Query(QueryResponse::new(fields, row_stream)))
    }
}

#[async_trait]
impl SimpleQueryHandler for KokedbQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (batches, schema) = self.execute(query).await?;
        Ok(vec![Self::respond(batches, schema, &Format::UnifiedText)?])
    }
}

#[async_trait]
impl ExtendedQueryHandler for KokedbQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_placeholders(&portal.statement.statement, |idx| {
            param_literal(portal, idx)
        })?;
        let (batches, schema) = self.execute(&sql).await?;
        Self::respond(batches, schema, &portal.result_column_format)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Report one type per `$N` placeholder so the client binds the right
        // number of parameters. We can't infer types from kokedb's custom plan
        // IR, so we honour the client-supplied OID (JDBC, asyncpg, libpq and
        // `prepare_typed` all send them) and fall back to TEXT otherwise.
        let n = placeholder_count(&target.statement).max(target.parameter_types.len());
        let param_types: Vec<Type> = (0..n)
            .map(|i| {
                target
                    .parameter_types
                    .get(i)
                    .and_then(|t| t.clone())
                    .unwrap_or(Type::TEXT)
            })
            .collect();
        // Substitute placeholders with typed NULLs so the statement plans and we
        // can read the result schema without binding real values.
        let sql = substitute_placeholders(&target.statement, |idx| {
            Ok(null_literal_for_type(
                param_types.get(idx).unwrap_or(&Type::UNKNOWN),
            ))
        })?;
        let schema = self.output_schema(&sql).await?;
        let fields = fields_for_schema(&schema);
        Ok(DescribeStatementResponse::new(
            param_types,
            (*fields).clone(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_placeholders(&portal.statement.statement, |idx| {
            param_literal(portal, idx)
        })?;
        let schema = self.output_schema(&sql).await?;
        let fields = fields_with_format(&schema, &portal.result_column_format);
        Ok(DescribePortalResponse::new((*fields).clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_numbered_placeholders() {
        let out =
            substitute_placeholders("SELECT * FROM t WHERE a = $1 AND b = $2", |i| Ok(format!("P{i}")))
                .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = P0 AND b = P1");
    }

    #[test]
    fn leaves_placeholders_inside_quotes() {
        let out = substitute_placeholders("SELECT '$1', \"$2\", $1", |i| Ok(format!("X{i}"))).unwrap();
        assert_eq!(out, "SELECT '$1', \"$2\", X0");
    }

    #[test]
    fn counts_placeholders() {
        assert_eq!(placeholder_count("SELECT $1, $2 WHERE x = $1"), 2);
        assert_eq!(placeholder_count("SELECT '$3', $1"), 1);
        assert_eq!(placeholder_count("SELECT 1"), 0);
    }

    #[test]
    fn rejects_zero_placeholder() {
        assert!(substitute_placeholders("SELECT $0", |i| Ok(format!("{i}"))).is_err());
    }

    #[test]
    fn quote_literal_escapes_quotes() {
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }

    #[test]
    fn parses_staleness_hint_value() {
        assert_eq!(parse_staleness_hint("SELECT /*+ max_staleness(30) */ 1"), Some(30));
        assert_eq!(parse_staleness_hint("SELECT 1"), None);
    }

    #[test]
    fn null_cast_for_known_types() {
        assert_eq!(null_literal_for_type(&Type::INT4), "CAST(NULL AS integer)");
        assert_eq!(null_literal_for_type(&Type::TEXT), "CAST(NULL AS text)");
        assert_eq!(null_literal_for_type(&Type::TIMESTAMP), "NULL");
    }
}
