//! PostgreSQL wire handlers that bridge to kokedb's protocol-agnostic query
//! path. Simple query protocol only (text format); the extended/prepared
//! protocol is a follow-up.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use futures::{stream, Sink, TryStreamExt};
use kokedb_common::hash::get_plan_hash;
use kokedb_common::spec::Plan;
use kokedb_query::binder::{parser, query};
use kokedb_query::context::{create_session_context, SharedServices};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::encode::{encode_batch, fields_for_schema};

/// Bundles the handlers for one server. Cloned per connection (cheap: it just
/// holds shared service handles).
#[derive(Clone)]
pub struct KokedbHandlers {
    handler: Arc<KokedbQueryHandler>,
}

impl KokedbHandlers {
    pub fn new(shared: SharedServices) -> Self {
        Self {
            handler: Arc::new(KokedbQueryHandler { shared }),
        }
    }
}

impl PgWireServerHandlers for KokedbHandlers {
    // Extended/startup/copy/error/cancel default to NoopHandler (startup noop =
    // no auth, like the MySQL server with no password configured).
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }
}

pub struct KokedbQueryHandler {
    shared: SharedServices,
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

impl KokedbQueryHandler {
    /// Runs one statement through the kokedb query path and returns the result
    /// batches plus whether the plan is result-cacheable.
    async fn execute(&self, sql: &str) -> PgWireResult<(Vec<RecordBatch>, datafusion::arrow::datatypes::SchemaRef)> {
        let max_staleness = parse_staleness_hint(sql).filter(|&s| s > 0);

        // Plan cache: parse + hash are pure functions of the SQL text.
        let plan_cache = self.shared.plan_cache();
        let (plan, cache_key) = if let Some(hit) = plan_cache.get(sql) {
            (hit.0.clone(), hit.1)
        } else {
            let plan = parser(sql).map_err(pg_err)?;
            let key = get_plan_hash(&plan).map_err(pg_err)?;
            plan_cache.insert(sql, Arc::new((plan.clone(), key)));
            (plan, key)
        };

        let cacheable = matches!(plan, Plan::Query(_)) && max_staleness.is_none();
        let cache = self.shared.result_cache();

        // Serve from the shared result cache when possible.
        if cacheable {
            if cache.contains(cache_key).await {
                if let Ok(stream) = cache.get(cache_key).await {
                    let schema = stream.schema();
                    let batches = stream.try_collect::<Vec<_>>().await.map_err(pg_err)?;
                    return Ok((batches, schema));
                }
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

        // A schemaless result is a command (e.g. CREATE CATALOG); report a tag.
        if schema.fields().is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        let fields = fields_for_schema(&schema);
        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(encode_batch(batch, &fields)?);
        }
        let row_stream = stream::iter(rows.into_iter().map(Ok));
        Ok(vec![Response::Query(QueryResponse::new(fields, row_stream))])
    }
}
