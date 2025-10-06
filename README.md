# KokeDB

**"Though tiny as it can be, the moss blooms with pride like the tree peony."**

[![GitHub License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/kiwilab-cn/kokedb/blob/main/LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/kiwilab-cn/kokedb?style=social)](https://github.com/kiwilab-cn/kokedb/stargazers)

KokeDB is an open-source project designed to accelerate query computations on large datasets in OLTP databases. By integrating with your OLTP database, KokeDB leverages columnar storage and vectorized execution engines to speed up queries. It also supports automated materialized views, caching frequent query results and automatically updating them when source table data changes.

## Features

- **Accelerated Query Computation**: Utilizes columnar storage and vectorized execution for faster OLTP database queries on large data volumes.
- **Automated Materialized Views**: Intelligently caches query results for frequent queries and automates updates based on source data changes.
- **Database Integration**: Seamlessly connects to OLTP databases and handles data caching with incremental updates.

## Goals and Roadmap

KokeDB aims to evolve into a comprehensive query acceleration tool with the following long-term features:

1. **Support for Common Databases**: Integration with databases like Oracle, MySQL, PostgreSQL, and more.
2. **Client Query Protocols**: Support for protocols such as MySQL, PostgreSQL, Arrow Flight SQL, etc.
3. **Underlying Column Formats**: Compatibility with data lake formats like Lance and DeltaLake.
4. **Streaming Tables**: Support for streaming data tables.
5. **Distributed Computing and Object Storage**: Enable distributed execution and integration with object storage systems.
6. **Data Services and BI Integration**: Provide data serving capabilities and seamless integration with Business Intelligence tools.

## Initial MVP

The Minimum Viable Product (MVP) focuses on core functionality to get started quickly:

1. **PostgreSQL Integration**: Support for connecting to PostgreSQL databases.
2. **MySQL Query Protocol**: Provide MySQL-compatible access for querying.
3. **DataFusion Execution Engine**: Use DataFusion for single-node query execution.
4. **SQL Parsing**: Leverage Sail-SQL-Parser for parsing SQL queries.
5. **Intelligent Materialized Views**: Automatic incremental updates from remote source tables and query result refreshes.

## How It Works

KokeDB operates on the following core principles:

1. **Database Integration and Data Sync**:
   - Upon integration, automatically identifies primary keys for tables (initially via metadata queries) and lastUpdateTime fields (based on a dictionary).
   - Uses these fields for local caching and incremental updates.
   - For unidentified fields, falls back to scheduled full updates.

2. **Query Caching**:
   - After a user executes a query, stores the SQL and results in a two-level cache (memory + disk).

3. **Cache Hit Optimization**:
   - On subsequent identical queries, directly returns results from the cache.

4. **Background Updates**:
   - Background scheduled tasks monitor remote table changes and update corresponding query results automatically.

## SQL Parsing Functionality

The SQL parsing functionality in this project is based on code from the [Sail project](https://github.com/lakehq/sail.git), specifically the `sail-sql-macro`, `sail-sql-parser`, and `sail-sql-analyzer` modules, which are licensed under the Apache License 2.0. These components have been adapted and integrated into this project to support SQL statement parsing and analysis. All original copyright notices and license terms from the Sail project have been retained as required by the Apache License 2.0.

For more details on the Sail project, please refer to its [GitHub repository](https://github.com/lakehq/sail.git).

## Installation

(Coming soon: Detailed installation instructions. For now, clone the repository and follow the setup in the docs folder.)

```bash
git clone https://github.com/kiwilab-cn/kokedb.git
cd kokedb
cargo run
```

## Usage

(Example usage snippets. Basic steps:)

1. Start postgresql meta server.  
```sehll
sudo docker run -d --name test-postgres -e POSTGRES_PASSWORD=123456 -e PGDATA=/var/lib/postgresql/data/pgdata  -v /opt/postgresql/data:/var/lib/postgresql/data -p 0.0.0.0:25432:5432 postgres:17.6
```

2. Start the KokeDB server.  
```shell
cargo run
```
3. Connect via MySQL client and create remote table.  
```shell
mysql -h 127.0.0.1 -P 3306 -u root
```
4. Execute sql queries.  

Example query:  
```sql
create catalog demo using postgresql://postgres:123456@192.168.0.227:25432/postgres;
SELECT * FROM demo.public.table WHERE condition;
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get involved, report issues, or submit pull requests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

*Note: This is an early-stage project. Feedback and contributions are highly appreciated!*
