//! End-to-end test of the PostgreSQL wire server: a real libpq client
//! (tokio-postgres) connects to the in-process server and runs DDL + queries
//! through the same engine as the MySQL server.
//!
//!   KOKEDB_TEST_DSN=postgresql://postgres:123456@127.0.0.1:25433/postgres
//!   PG_META_DSN=postgresql://postgres:123456@127.0.0.1:25433/kokedb
//!   cargo test -p kokedb-pg-svc --test e2e_pg_wire -- --ignored --test-threads=1

use std::time::Duration;

use kokedb_cache::result_cache::ResultCache;
use kokedb_pg_svc::handler::KokedbHandlers;
use kokedb_query::context::init_shared_services;
use pgwire::tokio::process_socket;
use sqlx::postgres::PgPool;
use sqlx::Row as _;
use tokio::net::TcpListener;
use tokio_postgres::types::Type;
use tokio_postgres::SimpleQueryMessage;

fn source_dsn() -> String {
    std::env::var("KOKEDB_TEST_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25433/postgres".into())
}
fn meta_dsn() -> String {
    std::env::var("PG_META_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25433/kokedb".into())
}

async fn clean_meta(pool: &PgPool, catalog: &str) {
    for sql in [
        "DELETE FROM system.catalog WHERE name = $1",
        "DELETE FROM system.table_sync_policy WHERE catalog = $1",
        "DELETE FROM system.table_sync_state WHERE catalog = $1",
        "DELETE FROM system.table_arrow_schema WHERE catalog_name = $1",
        "DELETE FROM system.cache_job WHERE catalog = $1",
        "DELETE FROM system.database_policy WHERE catalog = $1",
    ] {
        let _ = sqlx::query(sql).bind(catalog).execute(pool).await;
    }
}

async fn wait_until_cached(meta: &PgPool, catalog: &str, table: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let row = sqlx::query(
            "SELECT local_path FROM system.table_arrow_schema \
             WHERE catalog_name = $1 AND table_name = $2",
        )
        .bind(catalog)
        .bind(table)
        .fetch_optional(meta)
        .await
        .ok()
        .flatten();
        if let Some(r) = row {
            if r.try_get::<Option<String>, _>("local_path")
                .ok()
                .flatten()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
            {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

/// Extracts a single-column value from the first row carrying `col` in a
/// simple-query result. Rows from other statements (multi-statement queries)
/// that lack the column are skipped rather than panicking.
fn first_value(msgs: &[SimpleQueryMessage], col: &str) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.try_get(col).ok().flatten().map(|s| s.to_string()),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn pg_wire_create_catalog_and_query() {
    let catalog = "pgwire_shop";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS pgw_items",
        "CREATE TABLE pgw_items (id int PRIMARY KEY, name text)",
        "INSERT INTO pgw_items SELECT g, 'n' || g FROM generate_series(1, 50) g",
    ] {
        sqlx::query(stmt).execute(&source).await.unwrap();
    }
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, catalog).await;

    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();
    let handlers = KokedbHandlers::new(shared);

    // Start the wire server on an ephemeral port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let h = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(sock, None, h).await;
            });
        }
    });

    // Connect a real libpq client and drive DDL + queries over the wire.
    let conn_str = format!("host=127.0.0.1 port={} user=postgres dbname=kokedb", addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect to kokedb pg wire");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .simple_query(&format!(
            "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
             table_set=\"public.pgw_items\")",
            source_dsn()
        ))
        .await
        .expect("CREATE CATALOG over pg wire");

    assert!(
        wait_until_cached(&meta, catalog, "pgw_items", Duration::from_secs(60)).await,
        "pgw_items was not cached in time"
    );

    let total = client
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.pgw_items"))
        .await
        .unwrap();
    assert_eq!(first_value(&total, "c").as_deref(), Some("50"), "row count over pg wire");

    let filtered = client
        .simple_query(&format!(
            "SELECT count(*) AS c FROM {catalog}.public.pgw_items WHERE id <= 10"
        ))
        .await
        .unwrap();
    assert_eq!(first_value(&filtered, "c").as_deref(), Some("10"), "filtered query over pg wire");

    // A value query exercises text encoding of a non-aggregate column.
    let named = client
        .simple_query(&format!(
            "SELECT name AS n FROM {catalog}.public.pgw_items WHERE id = 7"
        ))
        .await
        .unwrap();
    assert_eq!(first_value(&named, "n").as_deref(), Some("n7"), "text column over pg wire");

    // Multi-statement simple query: both statements answered, in order.
    let multi = client
        .simple_query(&format!(
            "SELECT count(*) AS c FROM {catalog}.public.pgw_items WHERE id <= 5; \
             SELECT name AS n FROM {catalog}.public.pgw_items WHERE id = 9;"
        ))
        .await
        .expect("multi-statement simple query");
    assert_eq!(first_value(&multi, "c").as_deref(), Some("5"), "first statement result");
    assert_eq!(first_value(&multi, "n").as_deref(), Some("n9"), "second statement result");

    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS pgw_items").execute(&source).await;
}

/// Drives the extended/prepared protocol (Parse/Bind/Execute with bound `$N`
/// parameters and binary result formats) end to end via tokio-postgres'
/// `prepare`/`query`, which always uses the extended protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn pg_wire_extended_prepared_query() {
    let catalog = "pgwire_ext";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS pgw_ext",
        "CREATE TABLE pgw_ext (id int PRIMARY KEY, name text, price float8)",
        "INSERT INTO pgw_ext SELECT g, 'n' || g, g * 1.5 FROM generate_series(1, 20) g",
    ] {
        sqlx::query(stmt).execute(&source).await.unwrap();
    }
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, catalog).await;

    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();
    let handlers = KokedbHandlers::new(shared);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let h = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(sock, None, h).await;
            });
        }
    });

    let conn_str = format!("host=127.0.0.1 port={} user=postgres dbname=kokedb", addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect to kokedb pg wire");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .simple_query(&format!(
            "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
             table_set=\"public.pgw_ext\")",
            source_dsn()
        ))
        .await
        .expect("CREATE CATALOG over pg wire");
    assert!(
        wait_until_cached(&meta, catalog, "pgw_ext", Duration::from_secs(60)).await,
        "pgw_ext was not cached in time"
    );

    // Integer parameter, binary result decode of text + float8 columns. We use
    // `prepare_typed` so the client sends parameter OIDs in Parse (as JDBC,
    // asyncpg and libpq do); kokedb's parser can't infer placeholder types.
    let stmt = client
        .prepare_typed(
            &format!("SELECT name, price FROM {catalog}.public.pgw_ext WHERE id = $1"),
            &[Type::INT4],
        )
        .await
        .expect("prepare with int param");
    let row = client.query_one(&stmt, &[&7i32]).await.expect("execute int param");
    assert_eq!(row.get::<_, String>("name"), "n7");
    assert!((row.get::<_, f64>("price") - 10.5).abs() < 1e-9, "float8 binary decode");

    // Integer parameter feeding an aggregate (int8 result, binary decode).
    let stmt = client
        .prepare_typed(
            &format!("SELECT count(*) AS c FROM {catalog}.public.pgw_ext WHERE id <= $1"),
            &[Type::INT4],
        )
        .await
        .expect("prepare aggregate");
    let row = client.query_one(&stmt, &[&10i32]).await.expect("execute aggregate");
    assert_eq!(row.get::<_, i64>("c"), 10, "int8 binary decode of count");

    // Text parameter substitution (quoted literal).
    let stmt = client
        .prepare_typed(
            &format!("SELECT id FROM {catalog}.public.pgw_ext WHERE name = $1"),
            &[Type::TEXT],
        )
        .await
        .expect("prepare text param");
    let row = client.query_one(&stmt, &[&"n3"]).await.expect("execute text param");
    assert_eq!(row.get::<_, i32>("id"), 3, "text param + int4 binary decode");

    // Two parameters in one statement.
    let stmt = client
        .prepare_typed(
            &format!(
                "SELECT count(*) AS c FROM {catalog}.public.pgw_ext WHERE id >= $1 AND id <= $2"
            ),
            &[Type::INT4, Type::INT4],
        )
        .await
        .expect("prepare two params");
    let row = client.query_one(&stmt, &[&5i32, &10i32]).await.expect("execute two params");
    assert_eq!(row.get::<_, i64>("c"), 6, "two-parameter range");

    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS pgw_ext").execute(&source).await;
}

/// Exercises authentication (cleartext password against `system.app_user`) and
/// catalog-scoped multi-tenancy: a superuser creates two catalogs, a restricted
/// tenant can query its own catalog but is denied the other, and a wrong
/// password is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn pg_wire_auth_and_multitenancy() {
    let cat_a = "mt_cat_a";
    let cat_b = "mt_cat_b";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS mt_a",
        "DROP TABLE IF EXISTS mt_b",
        "CREATE TABLE mt_a (id int PRIMARY KEY)",
        "CREATE TABLE mt_b (id int PRIMARY KEY)",
        "INSERT INTO mt_a SELECT generate_series(1, 5)",
        "INSERT INTO mt_b SELECT generate_series(1, 9)",
    ] {
        sqlx::query(stmt).execute(&source).await.unwrap();
    }
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, cat_a).await;
    clean_meta(&meta, cat_b).await;

    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();

    // Seed accounts: a superuser and a tenant scoped to cat_a only.
    let m = shared.meta();
    let _ = sqlx::query("DELETE FROM system.app_user WHERE username IN ('mt_admin','mt_tenant')")
        .execute(&meta)
        .await;
    m.upsert_app_user("mt_admin", &kokedb_common::auth::password_digest("adminpw"), true, "*")
        .await
        .unwrap();
    m.upsert_app_user(
        "mt_tenant",
        &kokedb_common::auth::password_digest("tenantpw"),
        false,
        cat_a,
    )
    .await
    .unwrap();

    // Auth-enabled server.
    let handlers = KokedbHandlers::new(shared);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let h = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(sock, None, h).await;
            });
        }
    });
    let port = addr.port();

    // Wrong password is rejected.
    let bad = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=mt_admin password=wrong dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await;
    assert!(bad.is_err(), "wrong password should be rejected");

    // Superuser connects and creates both catalogs.
    let (admin, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=mt_admin password=adminpw dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("admin auth");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    for (cat, tbl) in [(cat_a, "mt_a"), (cat_b, "mt_b")] {
        admin
            .simple_query(&format!(
                "CREATE CATALOG {cat} USING '{}' WITH properties(cache_policy=\"select\", \
                 table_set=\"public.{tbl}\")",
                source_dsn()
            ))
            .await
            .expect("admin CREATE CATALOG");
    }
    assert!(wait_until_cached(&meta, cat_a, "mt_a", Duration::from_secs(60)).await);
    assert!(wait_until_cached(&meta, cat_b, "mt_b", Duration::from_secs(60)).await);

    // Tenant scoped to cat_a: allowed on cat_a, denied on cat_b.
    let (tenant, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=mt_tenant password=tenantpw dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("tenant auth");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let allowed = tenant
        .simple_query(&format!("SELECT count(*) AS c FROM {cat_a}.public.mt_a"))
        .await
        .expect("tenant query on own catalog");
    assert_eq!(first_value(&allowed, "c").as_deref(), Some("5"), "tenant sees its catalog");

    let denied = tenant
        .simple_query(&format!("SELECT count(*) AS c FROM {cat_b}.public.mt_b"))
        .await;
    assert!(denied.is_err(), "tenant must be denied access to other catalog");

    // Cleanup: remove users so other suites (and serve()'s auth gate) see an
    // empty app_user table again.
    let _ = sqlx::query("DELETE FROM system.app_user WHERE username IN ('mt_admin','mt_tenant')")
        .execute(&meta)
        .await;
    clean_meta(&meta, cat_a).await;
    clean_meta(&meta, cat_b).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS mt_a").execute(&source).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS mt_b").execute(&source).await;
}

/// Drives user management over SQL: CREATE USER / SHOW USERS / GRANT CATALOG /
/// REVOKE CATALOG / DROP USER, and verifies authentication flips on *live*
/// (dynamic auth: creating the first user enables auth for new connections,
/// no restart) and that restricted users cannot manage users.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn pg_wire_user_management_sql() {
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    let _ = sqlx::query("DELETE FROM system.app_user WHERE username IN ('sql_admin','sql_tenant')")
        .execute(&meta)
        .await;

    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();
    let handlers = KokedbHandlers::new(shared);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let h = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(sock, None, h).await;
            });
        }
    });
    let port = addr.port();

    // No users yet -> open access; this bootstrap session is unrestricted and
    // may manage users.
    let (boot, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("open-access bootstrap connection");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    boot.simple_query(
        "CREATE USER sql_admin WITH properties(password=\"adm1n\", superuser=\"true\")",
    )
    .await
    .expect("CREATE USER superuser");
    boot.simple_query(
        "CREATE USER sql_tenant WITH properties(password=\"t3nant\", catalogs=\"shop\")",
    )
    .await
    .expect("CREATE USER tenant");

    // Duplicate create must fail.
    assert!(
        boot.simple_query("CREATE USER sql_admin WITH properties(password=\"x\")")
            .await
            .is_err(),
        "duplicate CREATE USER should error"
    );

    // SHOW USERS lists both, without password digests.
    let users = boot.simple_query("SHOW USERS LIKE 'sql_*'").await.expect("SHOW USERS");
    let names: Vec<String> = users
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                r.try_get("username").ok().flatten().map(|s| s.to_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["sql_admin", "sql_tenant"], "SHOW USERS rows");

    // GRANT/REVOKE mutate the allow-list.
    boot.simple_query("GRANT CATALOG warehouse TO sql_tenant")
        .await
        .expect("GRANT CATALOG");
    let users = boot.simple_query("SHOW USERS LIKE 'sql_tenant'").await.unwrap();
    assert_eq!(
        first_value(&users, "allowed_catalogs").as_deref(),
        Some("shop,warehouse"),
        "grant adds to allow-list"
    );
    boot.simple_query("REVOKE CATALOG shop FROM sql_tenant")
        .await
        .expect("REVOKE CATALOG");
    let users = boot.simple_query("SHOW USERS LIKE 'sql_tenant'").await.unwrap();
    assert_eq!(
        first_value(&users, "allowed_catalogs").as_deref(),
        Some("warehouse"),
        "revoke removes from allow-list"
    );

    // Dynamic auth: users now exist, so a NEW connection must authenticate.
    let unauthed = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=nobody dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await;
    assert!(unauthed.is_err(), "auth must be live once users exist");

    let (tenant, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=sql_tenant password=t3nant dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("tenant authenticates with CREATE USER password");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // A restricted user cannot manage users.
    assert!(
        tenant
            .simple_query("CREATE USER sneaky WITH properties(password=\"x\")")
            .await
            .is_err(),
        "restricted user must not manage users"
    );
    assert!(
        tenant.simple_query("SHOW USERS").await.is_err(),
        "restricted user must not list users"
    );

    // DROP USER via the (still-unrestricted) bootstrap session; IF EXISTS is
    // idempotent.
    boot.simple_query("DROP USER sql_tenant").await.expect("DROP USER");
    boot.simple_query("DROP USER IF EXISTS sql_tenant")
        .await
        .expect("DROP USER IF EXISTS is idempotent");
    assert!(
        boot.simple_query("DROP USER sql_tenant").await.is_err(),
        "dropping a missing user without IF EXISTS errors"
    );
    boot.simple_query("DROP USER sql_admin").await.expect("DROP USER admin");

    // Password must never be persisted into SQL history.
    let leaked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM system.sql_stats WHERE sql_text ILIKE '%CREATE USER%'",
    )
    .fetch_one(&meta)
    .await
    .unwrap_or(0);
    assert_eq!(leaked, 0, "CREATE USER statements must not be stored in sql history");

    let _ = sqlx::query("DELETE FROM system.app_user WHERE username IN ('sql_admin','sql_tenant')")
        .execute(&meta)
        .await;
}

/// Database/table-level authorization: a user granted a single TABLE can read
/// exactly that table (the catalog and database stay resolvable), a DATABASE
/// grant covers all tables in the schema, and SHOW TABLES only lists granted
/// tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn pg_wire_table_level_authorization() {
    let catalog = "authz_cat";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS az_ok",
        "DROP TABLE IF EXISTS az_no",
        "CREATE TABLE az_ok (id int PRIMARY KEY)",
        "CREATE TABLE az_no (id int PRIMARY KEY)",
        "INSERT INTO az_ok SELECT generate_series(1, 4)",
        "INSERT INTO az_no SELECT generate_series(1, 7)",
    ] {
        sqlx::query(stmt).execute(&source).await.unwrap();
    }
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, catalog).await;
    let _ = sqlx::query(
        "DELETE FROM system.app_user WHERE username IN ('az_admin','az_table_user','az_db_user')",
    )
    .execute(&meta)
    .await;

    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();
    let handlers = KokedbHandlers::new(shared);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let h = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(sock, None, h).await;
            });
        }
    });
    let port = addr.port();

    // Bootstrap (open access): create the catalog and the users.
    let (boot, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("bootstrap connection");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    boot.simple_query(&format!(
        "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
         table_set=\"public.az_ok,public.az_no\")",
        source_dsn()
    ))
    .await
    .expect("CREATE CATALOG");
    assert!(wait_until_cached(&meta, catalog, "az_ok", Duration::from_secs(60)).await);
    assert!(wait_until_cached(&meta, catalog, "az_no", Duration::from_secs(60)).await);

    boot.simple_query(
        "CREATE USER az_admin WITH properties(password=\"adm\", superuser=\"true\")",
    )
    .await
    .expect("create admin");
    boot.simple_query("CREATE USER az_table_user WITH properties(password=\"pw1\")")
        .await
        .expect("create table-scoped user");
    boot.simple_query("CREATE USER az_db_user WITH properties(password=\"pw2\")")
        .await
        .expect("create db-scoped user");
    boot.simple_query(&format!("GRANT TABLE {catalog}.public.az_ok TO az_table_user"))
        .await
        .expect("GRANT TABLE");
    boot.simple_query(&format!("GRANT DATABASE {catalog}.public TO az_db_user"))
        .await
        .expect("GRANT DATABASE");

    // Table-scoped user: granted table readable, sibling denied, catalog visible.
    let (t_user, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=az_table_user password=pw1 dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("table-scoped user authenticates");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let ok = t_user
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.az_ok"))
        .await
        .expect("granted table readable");
    assert_eq!(first_value(&ok, "c").as_deref(), Some("4"));
    let denied = t_user
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.az_no"))
        .await;
    let denied_msg = format!("{denied:?}");
    assert!(denied.is_err(), "sibling table must be denied");
    assert!(
        denied_msg.contains("permission denied"),
        "denial should be explicit, got: {denied_msg}"
    );
    // SHOW TABLES only lists granted tables.
    let listed = t_user
        .simple_query(&format!("SHOW TABLES IN {catalog}.public"))
        .await
        .expect("SHOW TABLES for table-scoped user");
    let table_names: Vec<String> = listed
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                r.try_get("name").ok().flatten().map(|s| s.to_string())
            }
            _ => None,
        })
        .collect();
    assert!(
        table_names.contains(&"az_ok".to_string())
            && !table_names.contains(&"az_no".to_string()),
        "SHOW TABLES must list only granted tables, got {table_names:?}"
    );

    // Database-scoped user: every table in the schema readable.
    let (d_user, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=az_db_user password=pw2 dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("db-scoped user authenticates");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let ok = d_user
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.az_no"))
        .await
        .expect("db grant covers all tables in the schema");
    assert_eq!(first_value(&ok, "c").as_deref(), Some("7"));

    // Revoke the table grant; a fresh connection no longer reads it.
    let (admin, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=az_admin password=adm dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("admin authenticates");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    admin
        .simple_query(&format!("REVOKE TABLE {catalog}.public.az_ok FROM az_table_user"))
        .await
        .expect("REVOKE TABLE");
    let (t_user2, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=az_table_user password=pw1 dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("reconnect after revoke");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    assert!(
        t_user2
            .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.az_ok"))
            .await
            .is_err(),
        "revoked table must be denied on a fresh connection"
    );

    // Cleanup.
    let _ = sqlx::query(
        "DELETE FROM system.app_user WHERE username IN ('az_admin','az_table_user','az_db_user')",
    )
    .execute(&meta)
    .await;
    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS az_ok").execute(&source).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS az_no").execute(&source).await;
}

/// Row-level security: a `CREATE ROW POLICY` predicate is AND-ed into every
/// read the target user performs on the table; other users are unaffected;
/// invalid predicates are rejected at CREATE; dropping the policy restores
/// full reads; restricted users cannot manage policies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn pg_wire_row_level_security() {
    let catalog = "rls_cat";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS rls_orders",
        "CREATE TABLE rls_orders (id int PRIMARY KEY, region text)",
        "INSERT INTO rls_orders VALUES (1,'cn'),(2,'cn'),(3,'cn'),(4,'us'),(5,'us')",
    ] {
        sqlx::query(stmt).execute(&source).await.unwrap();
    }
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DELETE FROM system.app_user WHERE username IN ('rls_admin','rls_user')")
        .execute(&meta)
        .await;
    let _ = sqlx::query("DELETE FROM system.row_policy WHERE username IN ('rls_admin','rls_user')")
        .execute(&meta)
        .await;

    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();
    let handlers = KokedbHandlers::new(shared);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let h = handlers.clone();
            tokio::spawn(async move {
                let _ = process_socket(sock, None, h).await;
            });
        }
    });
    let port = addr.port();

    // Bootstrap: catalog + users + policy.
    let (boot, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("bootstrap connection");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    boot.simple_query(&format!(
        "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
         table_set=\"public.rls_orders\")",
        source_dsn()
    ))
    .await
    .expect("CREATE CATALOG");
    assert!(wait_until_cached(&meta, catalog, "rls_orders", Duration::from_secs(60)).await);

    boot.simple_query(
        "CREATE USER rls_admin WITH properties(password=\"adm\", superuser=\"true\")",
    )
    .await
    .expect("create admin");
    boot.simple_query(&format!(
        "CREATE USER rls_user WITH properties(password=\"pw\", catalogs=\"{catalog}\")"
    ))
    .await
    .expect("create rls user");

    // An invalid predicate is rejected at CREATE time.
    assert!(
        boot.simple_query(&format!(
            "CREATE ROW POLICY ON {catalog}.public.rls_orders FOR rls_user USING \"((not valid\""
        ))
        .await
        .is_err(),
        "invalid predicate must be rejected at CREATE"
    );

    boot.simple_query(&format!(
        "CREATE ROW POLICY ON {catalog}.public.rls_orders FOR rls_user USING \"region = 'cn'\""
    ))
    .await
    .expect("CREATE ROW POLICY");

    // SHOW ROW POLICIES lists it.
    let policies = boot.simple_query("SHOW ROW POLICIES").await.expect("SHOW ROW POLICIES");
    assert_eq!(
        first_value(&policies, "username").as_deref(),
        Some("rls_user"),
        "policy listed"
    );
    assert_eq!(
        first_value(&policies, "filter").as_deref(),
        Some("region = 'cn'"),
        "filter text listed"
    );

    // The filtered user sees only its rows — in every shape of query.
    let (user, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=rls_user password=pw dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("rls user authenticates");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let count = user
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.rls_orders"))
        .await
        .expect("filtered count");
    assert_eq!(first_value(&count, "c").as_deref(), Some("3"), "policy filters rows");
    let us = user
        .simple_query(&format!(
            "SELECT count(*) AS c FROM {catalog}.public.rls_orders WHERE region = 'us'"
        ))
        .await
        .expect("user predicate ANDs with policy");
    assert_eq!(
        first_value(&us, "c").as_deref(),
        Some("0"),
        "policy must AND with the user's own WHERE clause"
    );

    // A restricted user cannot manage row policies.
    assert!(
        user.simple_query(&format!(
            "CREATE ROW POLICY ON {catalog}.public.rls_orders FOR rls_admin USING \"1 = 1\""
        ))
        .await
        .is_err(),
        "restricted users must not manage row policies"
    );

    // The admin (unrestricted) sees everything.
    let (admin, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=rls_admin password=adm dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("admin authenticates");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let all = admin
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.rls_orders"))
        .await
        .expect("admin reads unfiltered");
    assert_eq!(first_value(&all, "c").as_deref(), Some("5"), "admin unaffected by policy");

    // Dropping the policy restores full reads on the next connection.
    admin
        .simple_query(&format!(
            "DROP ROW POLICY ON {catalog}.public.rls_orders FOR rls_user"
        ))
        .await
        .expect("DROP ROW POLICY");
    let (user2, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=rls_user password=pw dbname=kokedb"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("rls user reconnects");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let full = user2
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.rls_orders"))
        .await
        .expect("unfiltered after drop");
    assert_eq!(first_value(&full, "c").as_deref(), Some("5"), "policy removed");

    // Cleanup.
    let _ = sqlx::query("DELETE FROM system.app_user WHERE username IN ('rls_admin','rls_user')")
        .execute(&meta)
        .await;
    let _ = sqlx::query("DELETE FROM system.row_policy WHERE username IN ('rls_admin','rls_user')")
        .execute(&meta)
        .await;
    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS rls_orders").execute(&source).await;
}
