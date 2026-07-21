//! Column-masking function specs shared by the SQL layer (validation), the
//! session ACL, and the plan resolver (enforcement).
//!
//! A column policy's payload is a comma-separated list of entries:
//!
//! * `col`                — mask with a typed NULL (the default)
//! * `col=null`           — same, explicit
//! * `col=hash`           — SHA-256 hex of the value (deterministic, so the
//!   column stays joinable/groupable without revealing the value)
//! * `col=partial(p,s)`   — keep the first `p` and last `s` characters, mask
//!   the middle with `*` (length-preserving)
//! * `col=redact`         — the fixed string `***`
//!
//! `hash`/`partial`/`redact` apply to string columns; on any other type the
//! read path falls back to NULL masking (fail-safe — a mismatched mask must
//! narrow, never widen, what the user can see).

/// How a masked column's values are transformed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaskFunction {
    /// Typed NULL (works for every data type).
    Null,
    /// SHA-256 hex of the string value.
    Hash,
    /// Keep the first `prefix` and last `suffix` characters; the middle
    /// becomes `*`s (length-preserving, char-safe).
    Partial { prefix: usize, suffix: usize },
    /// The fixed string `***`.
    Redact,
}

/// One masked column with its function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMask {
    pub column: String,
    pub func: MaskFunction,
}

/// Parses one `col[=func]` entry. Errors carry a user-facing reason.
pub fn parse_mask_entry(entry: &str) -> Result<ColumnMask, String> {
    let entry = entry.trim();
    let (column, func) = match entry.split_once('=') {
        None => (entry, MaskFunction::Null),
        Some((col, spec)) => {
            let spec = spec.trim();
            let func = if spec.eq_ignore_ascii_case("null") {
                MaskFunction::Null
            } else if spec.eq_ignore_ascii_case("hash") {
                MaskFunction::Hash
            } else if spec.eq_ignore_ascii_case("redact") {
                MaskFunction::Redact
            } else if let Some(args) = spec
                .strip_prefix("partial(")
                .and_then(|rest| rest.strip_suffix(')'))
            {
                let mut parts = args.split(',').map(str::trim);
                let (Some(p), Some(s), None) = (parts.next(), parts.next(), parts.next()) else {
                    return Err(format!(
                        "partial() takes exactly two arguments (prefix, suffix), got '{spec}'"
                    ));
                };
                let prefix: usize = p
                    .parse()
                    .map_err(|_| format!("partial() prefix must be a number, got '{p}'"))?;
                let suffix: usize = s
                    .parse()
                    .map_err(|_| format!("partial() suffix must be a number, got '{s}'"))?;
                MaskFunction::Partial { prefix, suffix }
            } else {
                return Err(format!(
                    "unknown mask function '{spec}' (expected null, hash, partial(p,s), redact)"
                ));
            };
            (col.trim(), func)
        }
    };
    if column.is_empty() {
        return Err("mask entry is missing a column name".to_string());
    }
    Ok(ColumnMask {
        column: column.to_string(),
        func,
    })
}

/// Parses the full comma-separated policy payload. `partial(p,s)` contains a
/// comma, so entries are split at top-level commas only (outside parentheses).
pub fn parse_mask_list(payload: &str) -> Result<Vec<ColumnMask>, String> {
    let mut masks = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let bytes = payload.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                let entry = &payload[start..i];
                if !entry.trim().is_empty() {
                    masks.push(parse_mask_entry(entry)?);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let tail = &payload[start..];
    if !tail.trim().is_empty() {
        masks.push(parse_mask_entry(tail)?);
    }
    if masks.is_empty() {
        return Err("column policy needs at least one column".to_string());
    }
    Ok(masks)
}

/// Applies a mask function to one string value (used by the runtime UDFs and
/// unit-testable on its own). `Null` is handled by the planner, not here.
pub fn mask_string(value: &str, func: &MaskFunction) -> String {
    match func {
        MaskFunction::Null => String::new(),
        MaskFunction::Redact => "***".to_string(),
        MaskFunction::Hash => {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(value.as_bytes()))
        }
        MaskFunction::Partial { prefix, suffix } => {
            let chars: Vec<char> = value.chars().collect();
            if chars.len() <= prefix + suffix {
                // Too short to reveal anything safely: mask it all.
                return "*".repeat(chars.len().max(3));
            }
            let mut out = String::with_capacity(value.len());
            out.extend(&chars[..*prefix]);
            out.extend(std::iter::repeat('*').take(chars.len() - prefix - suffix));
            out.extend(&chars[chars.len() - suffix..]);
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_and_explicit_entries() {
        assert_eq!(
            parse_mask_entry("ssn").unwrap(),
            ColumnMask { column: "ssn".into(), func: MaskFunction::Null }
        );
        assert_eq!(parse_mask_entry("a=null").unwrap().func, MaskFunction::Null);
        assert_eq!(parse_mask_entry("a=HASH").unwrap().func, MaskFunction::Hash);
        assert_eq!(parse_mask_entry("a=redact").unwrap().func, MaskFunction::Redact);
        assert_eq!(
            parse_mask_entry("a=partial(2, 4)").unwrap().func,
            MaskFunction::Partial { prefix: 2, suffix: 4 }
        );
    }

    #[test]
    fn rejects_malformed_entries() {
        assert!(parse_mask_entry("a=frobnicate").is_err());
        assert!(parse_mask_entry("a=partial(2)").is_err());
        assert!(parse_mask_entry("a=partial(x,y)").is_err());
        assert!(parse_mask_entry("=hash").is_err());
    }

    #[test]
    fn splits_lists_outside_parentheses_only() {
        let masks = parse_mask_list("ssn=partial(0,4), email=hash, note").unwrap();
        assert_eq!(masks.len(), 3);
        assert_eq!(masks[0].func, MaskFunction::Partial { prefix: 0, suffix: 4 });
        assert_eq!(masks[1].func, MaskFunction::Hash);
        assert_eq!(masks[2].func, MaskFunction::Null);
        assert!(parse_mask_list(" , ").is_err());
    }

    #[test]
    fn mask_string_behaviors() {
        assert_eq!(mask_string("secret", &MaskFunction::Redact), "***");
        // Deterministic 64-char hex.
        let h = mask_string("secret", &MaskFunction::Hash);
        assert_eq!(h.len(), 64);
        assert_eq!(h, mask_string("secret", &MaskFunction::Hash));
        // Length-preserving partial, char-safe.
        assert_eq!(
            mask_string("111-22-6789", &MaskFunction::Partial { prefix: 0, suffix: 4 }),
            "*******6789"
        );
        assert_eq!(
            mask_string("数据库安全", &MaskFunction::Partial { prefix: 1, suffix: 1 }),
            "数***全"
        );
        // Too short: fully masked, never revealed.
        assert_eq!(
            mask_string("ab", &MaskFunction::Partial { prefix: 2, suffix: 2 }),
            "***"
        );
    }
}
