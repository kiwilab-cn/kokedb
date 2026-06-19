use either::Either;
use kokedb_sql_macro::TreeParser;

use crate::{
    ast::{
        identifier::Ident,
        literal::NumberLiteral,
        operator::{Ampersand, At, Colon, Minus, Period, QuestionMark, SchemaSeparator, Slash},
        statement::PropertyKeyValue,
    },
    common::Sequence,
};

#[derive(Debug, Clone, TreeParser)]
pub struct DatabaseJdbcDsn {
    pub schema: Ident,
    pub schema_separator: SchemaSeparator,
    pub credentials: Credentials,
    pub cred_host_separator: At,
    pub server: Server,
    pub server_db_separator: Slash,
    pub database: Ident,
    pub params: Option<UrlParamesList>,
}

#[derive(Debug, Clone, TreeParser)]
pub struct UrlParamesList {
    pub separator: QuestionMark,
    pub params: Sequence<PropertyKeyValue, Ampersand>,
}

#[derive(Debug, Clone, TreeParser)]
pub struct Credentials {
    pub username: Ident,
    pub user_pass_separator: Colon,
    pub password: Option<Ident>,
}

#[derive(Debug, Clone, TreeParser)]
pub struct Server {
    /// Dot-separated host labels. Accepts IPv4 (numeric octets), bare hostnames
    /// / docker service names (e.g. `postgres`, `localhost`), and dotted DNS
    /// names with hyphens (e.g. `my-db.rds.amazonaws.com`).
    pub host: Sequence<HostLabel, Period>,
    pub host_port_separator: Colon,
    pub port: Option<NumberLiteral>,
}

/// One dot-separated label of a host, itself a sequence of hyphen-joined parts
/// (each part an identifier or a number, since the lexer splits on `-` and `.`).
#[derive(Debug, Clone, TreeParser)]
pub struct HostLabel {
    pub head: Either<Ident, NumberLiteral>,
    pub tail: Vec<HostLabelPart>,
}

#[derive(Debug, Clone, TreeParser)]
pub struct HostLabelPart {
    pub separator: Minus,
    pub part: Either<Ident, NumberLiteral>,
}
