use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum CachePolicy {
    TopK { k: u32 },
    All,
    Select { table_set: String },
    Smart,
}

#[derive(Debug)]
pub enum ParseError {
    CachePolicyNotFound,
    InvalidCachePolicy(String),
    KValueNotFound,
    InvalidKValue(String),
    TableSetNotFound,
    InvalidTableSetValue(String),
}

impl CachePolicy {
    pub fn to_string(&self) -> String {
        match self {
            CachePolicy::TopK { k } => format!("topk:k={}", k),
            CachePolicy::All => "all".to_string(),
            CachePolicy::Select { table_set } => format!("select:table_set={}", table_set),
            CachePolicy::Smart => "smart".to_string(),
        }
    }

    pub fn from_string(s: &str) -> Result<Self, ParseError> {
        if s == "all" {
            return Ok(CachePolicy::All);
        } else if s == "smart" || s.is_empty() {
            return Ok(CachePolicy::Smart);
        }

        if let Some(colon_pos) = s.find(':') {
            let (policy_type, params) = s.split_at(colon_pos);
            let params = &params[1..]; // 跳过冒号

            match policy_type {
                "topk" => {
                    if let Some(k_value) = params.strip_prefix("k=") {
                        let k = k_value
                            .parse::<u32>()
                            .map_err(|_| ParseError::InvalidKValue(k_value.to_string()))?;
                        Ok(CachePolicy::TopK { k })
                    } else {
                        Err(ParseError::KValueNotFound)
                    }
                }
                "select" => {
                    if let Some(table_set) = params.strip_prefix("table_set=") {
                        Ok(CachePolicy::Select {
                            table_set: table_set.to_string(),
                        })
                    } else {
                        Err(ParseError::TableSetNotFound)
                    }
                }
                _ => Err(ParseError::InvalidCachePolicy(policy_type.to_string())),
            }
        } else {
            Err(ParseError::InvalidCachePolicy(s.to_string()))
        }
    }
}

pub fn parse_cache_policy(properties: Vec<(String, String)>) -> Result<CachePolicy, ParseError> {
    let props: HashMap<String, String> = properties.into_iter().collect();

    let default = CachePolicy::Smart;
    let cache_policy = props.get("cache_policy");
    if cache_policy.is_none() {
        return Ok(default);
    }

    let cache_policy = cache_policy.unwrap();
    match cache_policy.as_str() {
        "topk" => {
            let k_value = props.get("k").ok_or(ParseError::KValueNotFound)?;

            let k = k_value
                .parse::<u32>()
                .map_err(|_| ParseError::InvalidKValue(k_value.clone()))?;
            if k == 0 {
                return Err(ParseError::InvalidKValue("0 is not allowed.".to_string()));
            }
            Ok(CachePolicy::TopK { k })
        }
        "all" => Ok(CachePolicy::All),
        "smart" => Ok(CachePolicy::Smart),
        "select" => {
            let table_set = props
                .get("table_set")
                .ok_or(ParseError::TableSetNotFound)?
                .clone();
            if table_set.is_empty() {
                return Err(ParseError::InvalidTableSetValue(
                    "empty string is not allowed.".to_string(),
                ));
            }

            Ok(CachePolicy::Select { table_set })
        }
        _ => Err(ParseError::InvalidCachePolicy(cache_policy.clone())),
    }
}
