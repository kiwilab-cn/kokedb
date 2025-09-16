use kokedb_sql_parser::{
    ast::{dsn::DatabaseJdbcDsn, statement::PropertyValue},
    string::StringValue,
};

use crate::error::{SqlError, SqlResult};

pub fn from_ast_database_jdbc_dsn(dsn: DatabaseJdbcDsn) -> SqlResult<String> {
    let support_schema = vec![
        "postgresql".to_string(),
        "oracle".to_string(),
        "mysql".to_string(),
    ];
    let schema = dsn.schema.value;
    if !support_schema.contains(&schema) {
        return Err(SqlError::NotSupported(format!(
            "Unsupported schema:{}",
            &schema
        )));
    }

    let passwd = if dsn.credentials.password.is_some() {
        dsn.credentials.password.unwrap().value
    } else {
        "".to_string()
    };

    let port = if dsn.server.port.is_some() {
        dsn.server.port.unwrap().value
    } else {
        "".to_string()
    };

    let host = dsn
        .server
        .host
        .items()
        .map(|x| x.value.clone())
        .collect::<Vec<String>>();

    let mut dsn_str = format!(
        "{}://{}:{}@{}:{}/{}",
        &schema,
        dsn.credentials.username.value,
        passwd,
        host.join("."),
        port,
        dsn.database.value,
    );

    if dsn.params.is_some() {
        let params = dsn.params.unwrap();
        let mut kvs = vec![];
        for param in params.params.items() {
            let key = param.key.clone();
            let value = param.value.clone();

            let key_str = format!("{:?}", key);
            let value_str = match value {
                Some(x) => {
                    let eq = x.0;
                    if eq.is_none() {
                        return Err(SqlError::InvalidArgument(format!(
                            "Failed to check parameter:{:?}",
                            &key_str
                        )));
                    }
                    let v = x.1;

                    match v {
                        PropertyValue::String(string_literal) => match string_literal.value {
                            StringValue::Valid { value, .. } => value,
                            StringValue::Invalid { .. } => "".to_string(),
                        },
                        _ => "".to_string(),
                    }
                }
                None => "".to_string(),
            };
            let kv = format!("{}={}", key_str, value_str);
            kvs.push(kv);
        }
        dsn_str = format!("{}?{}", dsn_str, kvs.join("&"));
    }

    Ok(dsn_str)
}
