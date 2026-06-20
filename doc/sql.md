# SQL Syntax


## DDL

- CREATE CATALOG

Supports creating catalogs from PostgreSQL databases. MySQL and Oracle support coming soon.

1. **default**  
```sql
create catalog demo using postgresql://postgres:123456@127.00.1:25432/postgres;
```
Cache policy is smart by default if not set.

2. **smart**  
```sql
create catalog demo using postgresql://postgres:123456@192.168.0.227:25432/postgres with properties(cache_policy="smart");
```
Cache hot tables which are queried frequently **in the past 7 days**. If there are none, cache the top 10 tables.

3. **all**  
```sql
create catalog demo using postgresql://postgres:123456@192.168.0.227:25432/postgres with properties(cache_policy="all");
```
Cache all tables from the remote catalog.

4. **topk**  
```sql
create catalog demo using postgresql://postgres:123456@192.168.0.227:25432/postgres with properties(cache_policy="topk", k="10");
```
Cache the top N tables in the remote catalog, where "top" is determined by table size and row count.

5. **select**  
```sql
create catalog demo using postgresql://postgres:123456@192.168.0.227:25432/postgres with properties(cache_policy="select", table_set="test.newtable,public.newtable");
```
Cache the tables which are in the **table_set**.

- SHOW CATALOGS

- SHOW DATABASES

- SHOW TABLES

- SHOW COLUMNS

- SHOW TABLE METADATA

Shows the inferred sync decision and the observed signals that drove it for a
cached table (`catalog.database.table`, with the catalog/database defaulting to
the session). Includes the refresh cadence bucket/interval, the incremental mode
+ lifecycle status + tier + source (rule/llm), the detected watermark/primary
key, validation counters, and the size/churn/access signals.

```sql
SHOW TABLE METADATA FROM demo.public.orders;
```

- SHOW CACHE JOBS

Lists recent table-sync runs (one row per sync) with status, timing, duration,
and any error — for observability and debugging. Optionally scoped to a single
table or a whole catalog; newest first.

```sql
SHOW CACHE JOBS;                                  -- all recent runs
SHOW CACHE JOBS FROM CATALOG demo;                -- one catalog
SHOW CACHE JOBS FROM TABLE demo.public.orders;    -- one table
```

- ALTER TABLE ... SET CACHE POLICY

Manually override a cached table's incremental sync strategy. A user override is
trusted (activated without shadow validation) and sticky — periodic
re-evaluation will not overwrite it. Supported options: `inc_mode`
(`full` | `upsert` | `append`), `watermark_column`, `pk_columns`. `upsert`
requires both a watermark column and primary key; `append` requires a watermark;
`full` disables incremental sync (always full refresh).

```sql
ALTER TABLE demo.public.orders SET CACHE POLICY WITH (
  inc_mode = 'upsert', watermark_column = 'updated_at', pk_columns = 'id'
);

-- Disable incremental for a table:
ALTER TABLE demo.public.events SET CACHE POLICY WITH (inc_mode = 'full');
```


## DML

- REFRESH CACHE FROM TABLE

Manually trigger a sync of a single cached table from its remote source. The
table is resolved as `catalog.database.table` (a 2- or 1-part name falls back to
the session's default catalog/database). The sync is enqueued asynchronously and
the command returns immediately with the queued task id; whether the run is full
or incremental is decided by the runner from the table's detected metadata and
its persisted sync state.

```sql
REFRESH CACHE FROM TABLE demo.public.orders;
```
```
+--------------------+--------------------------------------+--------+
| table              | task_id                              | status |
+--------------------+--------------------------------------+--------+
| demo.public.orders | 3f2a...-...                          | QUEUED |
+--------------------+--------------------------------------+--------+
```



## Select

Description
Spark supports a SELECT statement and conforms to the ANSI SQL standard. Queries are used to retrieve result sets from one or more tables. The following section describes the overall query syntax and the sub-sections cover different constructs of a query along with examples.

### Syntax
```sql
[ WITH with_query [ , ... ] ]
select_statement [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select_statement, ... ]
    [ ORDER BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ] } ]
    [ SORT BY { expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ] } ]
    [ CLUSTER BY { expression [ , ... ] } ]
    [ DISTRIBUTE BY { expression [, ... ] } ]
    [ WINDOW { named_window [ , WINDOW named_window, ... ] } ]
    [ LIMIT { ALL | expression } ]  
```

While select_statement is defined as

```sql
SELECT [ hints , ... ] [ ALL | DISTINCT ] { [ [ named_expression | regex_column_names | star ] [ , ... ] | TRANSFORM (...) ] }
    FROM { from_item [ , ... ] }
    [ PIVOT clause ]
    [ UNPIVOT clause ]
    [ LATERAL VIEW clause ] [ ... ] 
    [ WHERE boolean_expression ]
    [ GROUP BY expression [ , ... ] ]
    [ HAVING boolean_expression ]  
```

### Parameters

- with_query

Specifies the common table expressions (CTEs) before the main query block. These table expressions are allowed to be referenced later in the FROM clause. This is useful to abstract out repeated subquery blocks in the FROM clause and improves readability of the query.

- hints

Hints can be specified to help spark optimizer make better planning decisions. Currently spark supports hints that influence selection of join strategies and repartitioning of the data.

- ALL

Select all matching rows from the relation and is enabled by default.

- DISTINCT

Select all matching rows from the relation after removing duplicates in results.

- named_expression

An expression with an assigned name. In general, it denotes a column expression.

**Syntax:** expression [[AS] alias]

- star

The * (star) clause is used to select all or most columns from one or all relations in a FROM clause.

- from_item

Specifies a source of input for the query. It can be one of the following:

  - Table relation
  - Join relation
  - Pivot relation
  - Unpivot relation
  - Table-value function
  - Inline table
  - [ LATERAL ] ( Subquery )
  - File

- PIVOT

The PIVOT clause is used for data perspective; We can get the aggregated values based on specific column value.

- UNPIVOT

The UNPIVOT clause transforms columns into rows. It is the reverse of PIVOT, except for aggregation of values.

- LATERAL VIEW

The LATERAL VIEW clause is used in conjunction with generator functions such as EXPLODE, which will generate a virtual table containing one or more rows. LATERAL VIEW will apply the rows to each original output row.

- WHERE

Filters the result of the FROM clause based on the supplied predicates.

- GROUP BY

Specifies the expressions that are used to group the rows. This is used in conjunction with aggregate functions (MIN, MAX, COUNT, SUM, AVG, etc.) to group rows based on the grouping expressions and aggregate values in each group. When a FILTER clause is attached to an aggregate function, only the matching rows are passed to that function.

- HAVING

Specifies the predicates by which the rows produced by GROUP BY are filtered. The HAVING clause is used to filter rows after the grouping is performed. If HAVING is specified without GROUP BY, it indicates a GROUP BY without grouping expressions (global aggregate).

- ORDER BY

Specifies an ordering of the rows of the complete result set of the query. The output rows are ordered across the partitions. This parameter is mutually exclusive with SORT BY, CLUSTER BY and DISTRIBUTE BY and can not be specified together.

- SORT BY

Specifies an ordering by which the rows are ordered within each partition. This parameter is mutually exclusive with ORDER BY and CLUSTER BY and can not be specified together.

- CLUSTER BY

Specifies a set of expressions that is used to repartition and sort the rows. Using this clause has the same effect of using DISTRIBUTE BY and SORT BY together.

- DISTRIBUTE BY

Specifies a set of expressions by which the result rows are repartitioned. This parameter is mutually exclusive with ORDER BY and CLUSTER BY and can not be specified together.

- LIMIT

Specifies the maximum number of rows that can be returned by a statement or subquery. This clause is mostly used in the conjunction with ORDER BY to produce a deterministic result.

- boolean_expression

Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).

- expression

Specifies a combination of one or more values, operators, and SQL functions that evaluates to a value.

- named_window

Specifies aliases for one or more source window specifications. The source window specifications can be referenced in the widow definitions in the query.

- regex_column_names

When spark.sql.parser.quotedRegexColumnNames is true, quoted identifiers (using backticks) in SELECT statement are interpreted as regular expressions and SELECT statement can take regex-based column specification. For example, below SQL will only take column c:
```sql
   SELECT `(a|b)?+.+` FROM (
     SELECT 1 as a, 2 as b, 3 as c
   )
```

- TRANSFORM

Specifies a hive-style transform query specification to transform the input by forking and running user-specified command or script.


## UDF/UDTF/UDAF
