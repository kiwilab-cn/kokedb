use kokedb_sql_macro::TreeParser;

use crate::{
    ast::{
        identifier::Ident,
        literal::NumberLiteral,
        operator::{Ampersand, At, Colon, Period, QuestionMark, SchemaSeparator, Slash},
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
    pub host: Sequence<NumberLiteral, Period>,
    pub host_port_separator: Colon,
    pub port: Option<NumberLiteral>,
}
