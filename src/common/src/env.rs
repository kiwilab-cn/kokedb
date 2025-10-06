use std::{env, str::FromStr};

pub fn get_env_as<T>(key: &str, default: T) -> T
where
    T: FromStr,
    T::Err: std::fmt::Debug, // 为了 parse 失败时可以处理错误
{
    env::var(key)
        .ok()
        .and_then(|s| s.parse::<T>().ok())
        .unwrap_or(default)
}
