use super::{Encoding, MessageOut};

#[derive(Debug, Clone)]
pub enum Filter {
    KeyEquals(String),
    KeyContains(String),
    ValueContains(String),
    ValueEquals { text: String, json: Option<serde_json::Value> },
    KeyNotEquals(String),
    ValueNotEquals { text: String, json: Option<serde_json::Value> },
    JsonPathEquals { path: String, value: String },
    JsonPathNotEquals { path: String, value: String },
    JsonPathContains { path: String, value: String },
    KeyCompare { op: CmpOp, value: Option<f64> },
    ValueCompare { op: CmpOp, value: Option<f64> },
    JsonPathCompare { path: String, op: CmpOp, value: Option<f64> },
    /// Case-insensitive contains on key text OR value text — the bare-text
    /// filter box of the timeline design (`filter=contains&q=...`). The
    /// value side keeps the same decode-error exclusion as `ValueContains`
    /// (a base64-of-raw-bytes blob is not real content); the key side
    /// matches whenever the key decoded at all (a `None` key never matches,
    /// same convention as `KeyEquals`/`KeyContains`). Holds the needle
    /// already lower-cased at parse time (`Filter::parse`), not the raw
    /// query text — `matches` is called once per decoded record in the
    /// merge loop, so lower-casing here once beats re-lowering on every
    /// record.
    Contains(String),
}

/// Numeric comparison operators (owner ruling 2026-08-17). Comparisons use
/// the shared double model; a non-numeric target or query matches nothing.
#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl CmpOp {
    fn parse(op: Option<&str>, kind: &str) -> Result<CmpOp, String> {
        match op {
            Some("gt") => Ok(CmpOp::Gt),
            Some("gte") => Ok(CmpOp::Gte),
            Some("lt") => Ok(CmpOp::Lt),
            Some("lte") => Ok(CmpOp::Lte),
            Some(other) => Err(format!("filter {kind} does not support op '{other}'")),
            None => Err(format!("filter {kind} requires an op parameter")),
        }
    }

    fn holds(self, target: f64, expected: f64) -> bool {
        match self {
            CmpOp::Gt => target > expected,
            CmpOp::Gte => target >= expected,
            CmpOp::Lt => target < expected,
            CmpOp::Lte => target <= expected,
        }
    }
}

/// "A number" is the JSON number grammar — the frontend runs JSON.parse on
/// its side, so using the JSON parser here keeps the mirror exact by
/// construction (`1e3` parses; `""`, `+42`, `0x2` don't).
fn json_number(text: &str) -> Option<f64> {
    serde_json::from_str::<f64>(text).ok()
}

/// The numeric reading of a JSON scalar: a number itself, or a string that
/// parses as one. Bools/null/objects/arrays have none.
fn scalar_number(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => json_number(s),
        _ => None,
    }
}

/// The numeric reading of a whole value text: parsed as JSON it is a
/// number, or a string parsing as one (raw text like `42` IS valid JSON).
fn value_number(text: &str) -> Option<f64> {
    scalar_number(&serde_json::from_str::<serde_json::Value>(text).ok()?)
}

impl Filter {
    pub fn parse(kind: &str, q: &str, path: Option<&str>, op: Option<&str>) -> Result<Filter, String> {
        match kind {
            "key_eq" => Ok(Filter::KeyEquals(q.to_lowercase())),
            "key_contains" => Ok(Filter::KeyContains(q.to_lowercase())),
            "value_contains" => Ok(Filter::ValueContains(q.to_lowercase())),
            // The needle's JSON form is parsed ONCE here, never per record.
            "value_eq" => Ok(Filter::ValueEquals { text: q.to_lowercase(), json: serde_json::from_str(q).ok() }),
            "key_neq" => Ok(Filter::KeyNotEquals(q.to_lowercase())),
            "value_neq" => Ok(Filter::ValueNotEquals { text: q.to_lowercase(), json: serde_json::from_str(q).ok() }),
            "json_neq" => path
                .map(|p| Filter::JsonPathNotEquals { path: p.to_string(), value: q.to_lowercase() })
                .ok_or_else(|| "filter json_neq requires a path parameter".to_string()),
            "contains" => Ok(Filter::Contains(q.to_lowercase())),
            "json_eq" => path
                .map(|p| Filter::JsonPathEquals { path: p.to_string(), value: q.to_lowercase() })
                .ok_or_else(|| "filter json_eq requires a path parameter".to_string()),
            "json_contains" => path
                .map(|p| Filter::JsonPathContains { path: p.to_string(), value: q.to_lowercase() })
                .ok_or_else(|| "filter json_contains requires a path parameter".to_string()),
            "key_cmp" => Ok(Filter::KeyCompare { op: CmpOp::parse(op, kind)?, value: json_number(q) }),
            "value_cmp" => Ok(Filter::ValueCompare { op: CmpOp::parse(op, kind)?, value: json_number(q) }),
            "json_cmp" => {
                let p = path.ok_or_else(|| "filter json_cmp requires a path parameter".to_string())?;
                Ok(Filter::JsonPathCompare { path: p.to_string(), op: CmpOp::parse(op, kind)?, value: json_number(q) })
            }
            other => Err(format!("unknown filter kind '{other}'")),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Segment {
    Name(String),
    // Unquoted `[]`: quantifies over the array — ANY element may satisfy
    // the rest of the path (owner ruling 2026-08-17).
    AnyElement,
}

// Path tokenizer, identical on both sides (frontend `splitPath`): unquoted
// `.` separates segments; a double-quoted run is part of its segment with
// the quotes stripped, so `.`/`:`/`=`/`[]` inside quotes are literal. An
// unclosed quote runs to the end. An unquoted `[]` pair emits an
// any-element marker. Quoting affects tokenization only.
fn split_path(path: &str) -> Vec<Segment> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut after_marker = false;
    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' {
            in_quotes = !in_quotes;
        } else if c == '.' && !in_quotes {
            // The separator right after a `[]` marker introduces the next
            // segment — it must not mint an empty Name (which never
            // resolves); a genuinely empty segment (`a..b`) still does.
            if !(after_marker && current.is_empty()) {
                segments.push(Segment::Name(std::mem::take(&mut current)));
            }
            after_marker = false;
        } else if c == '[' && !in_quotes && chars.get(i + 1) == Some(&']') {
            if !current.is_empty() {
                segments.push(Segment::Name(std::mem::take(&mut current)));
            }
            segments.push(Segment::AnyElement);
            after_marker = true;
            i += 1;
        } else {
            current.push(c);
            after_marker = false;
        }
        i += 1;
    }
    if !(after_marker && current.is_empty()) {
        segments.push(Segment::Name(current));
    }
    segments
}

/// Every JSON node the path can reach: exactly one without `[]` markers,
/// one per matching element combination with them.
fn path_candidates<'a>(root: &'a serde_json::Value, path: &str) -> Vec<&'a serde_json::Value> {
    fn walk<'a>(node: &'a serde_json::Value, segs: &[Segment], out: &mut Vec<&'a serde_json::Value>) {
        let Some((first, rest)) = segs.split_first() else {
            out.push(node);
            return;
        };
        match first {
            Segment::Name(n) => match node {
                serde_json::Value::Object(map) => {
                    if let Some(v) = map.get(n) {
                        walk(v, rest, out);
                    }
                }
                serde_json::Value::Array(items) => {
                    if let Some(v) = n.parse::<usize>().ok().and_then(|i| items.get(i)) {
                        walk(v, rest, out);
                    }
                }
                _ => {}
            },
            Segment::AnyElement => {
                if let serde_json::Value::Array(items) = node {
                    for v in items {
                        walk(v, rest, out);
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &split_path(path), &mut out);
    out
}

// Case-insensitive structural equality: string keys and string values
// compare lower-cased; numbers compare as doubles (the client's JSON.parse
// has no other number model — serde's variant-exact `1.0 != 1` would match
// different rows in the scanned window vs the live tail); bools/null exact.
fn json_eq_ci(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (a, b) {
        (Value::String(x), Value::String(y)) => x.to_lowercase() == y.to_lowercase(),
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(xa, yb)| json_eq_ci(xa, yb))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, xv)| {
                    y.iter()
                        .find(|(yk, _)| yk.to_lowercase() == k.to_lowercase())
                        .is_some_and(|(_, yv)| json_eq_ci(xv, yv))
                })
        }
        _ => a == b,
    }
}

/// True when some node the path reaches in a readable JSON value (exactly
/// one node without `[]` markers, any element combination with them)
/// satisfies `pred`.
fn any_json_path_match(msg: &MessageOut, path: &str, pred: impl Fn(&serde_json::Value) -> bool) -> bool {
    msg.value.as_ref().is_some_and(|v| {
        v.encoding != Encoding::DecodeError
            && serde_json::from_str::<serde_json::Value>(&v.text)
                .ok()
                .is_some_and(|root| path_candidates(&root, path).into_iter().any(&pred))
    })
}

/// The `=`/`!=` equality reading of a readable value text: JSON semantic
/// equality when both sides parse as JSON, else lower-cased text equality.
fn value_equals(text: &str, json: &Option<serde_json::Value>, value_text: &str) -> bool {
    match (json, serde_json::from_str::<serde_json::Value>(value_text).ok()) {
        (Some(expected), Some(actual)) => json_eq_ci(&actual, expected),
        _ => value_text.to_lowercase() == *text,
    }
}

fn scalar_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.to_lowercase()),
        // Through f64 deliberately: the client stringifies via a JS double
        // (`String(JSON.parse(...))`), so `100.0` must read "100" and >2^53
        // integers collapse identically on both sides. Both shortest-
        // round-trip formatters (ryu here, V8 there) print the same digits
        // for the same double.
        serde_json::Value::Number(n) => n.as_f64().map(|f| f.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Null => Some("null".to_string()),
        _ => None,
    }
}

pub fn matches(filter: &Filter, msg: &MessageOut) -> bool {
    match filter {
        Filter::KeyEquals(q) => msg.key.as_ref().is_some_and(|k| k.text.to_lowercase() == *q),
        Filter::KeyContains(q) => msg.key.as_ref().is_some_and(|k| k.text.to_lowercase().contains(q.as_str())),
        // A value that failed to decode has no real content to search — its
        // `text` is just the base64 of the raw bytes, so matching against it
        // would produce false hits with no relationship to actual content.
        Filter::ValueContains(q) => msg.value.as_ref().is_some_and(|v| {
            v.encoding != Encoding::DecodeError && v.text.to_lowercase().contains(q.as_str())
        }),
        Filter::ValueEquals { text, json } => msg.value.as_ref().is_some_and(|v| {
            v.encoding != Encoding::DecodeError && value_equals(text, json, &v.text)
        }),
        Filter::KeyNotEquals(q) => msg.key.as_ref().is_some_and(|k| k.text.to_lowercase() != *q),
        Filter::ValueNotEquals { text, json } => msg.value.as_ref().is_some_and(|v| {
            v.encoding != Encoding::DecodeError && !value_equals(text, json, &v.text)
        }),
        Filter::JsonPathEquals { path, value } => {
            any_json_path_match(msg, path, |c| scalar_text(c).is_some_and(|found| found == *value))
        }
        Filter::JsonPathNotEquals { path, value } => {
            any_json_path_match(msg, path, |c| scalar_text(c).is_some_and(|found| found != *value))
        }
        Filter::JsonPathContains { path, value } => {
            any_json_path_match(msg, path, |c| scalar_text(c).is_some_and(|found| found.contains(value.as_str())))
        }
        Filter::KeyCompare { op, value } => value.is_some_and(|expected| {
            msg.key
                .as_ref()
                .and_then(|k| json_number(&k.text))
                .is_some_and(|target| op.holds(target, expected))
        }),
        Filter::ValueCompare { op, value } => value.is_some_and(|expected| {
            msg.value
                .as_ref()
                .filter(|v| v.encoding != Encoding::DecodeError)
                .and_then(|v| value_number(&v.text))
                .is_some_and(|target| op.holds(target, expected))
        }),
        Filter::JsonPathCompare { path, op, value } => value.is_some_and(|expected| {
            any_json_path_match(msg, path, |c| {
                scalar_number(c).is_some_and(|target| op.holds(target, expected))
            })
        }),
        Filter::Contains(q) => {
            let key_hit = msg.key.as_ref().is_some_and(|k| k.text.to_lowercase().contains(q.as_str()));
            let value_hit = msg.value.as_ref().is_some_and(|v| {
                v.encoding != Encoding::DecodeError && v.text.to_lowercase().contains(q.as_str())
            });
            key_hit || value_hit
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{DecodedPayload, Encoding, MessageOut};

    fn msg(key: Option<&str>, value: &str, encoding: Encoding) -> MessageOut {
        MessageOut {
            partition: 0, offset: 0, timestamp_ms: Some(1),
            key: key.map(|k| DecodedPayload { encoding: Encoding::Utf8, text: k.into(), schema_id: None, error: None }),
            value: Some(DecodedPayload { encoding, text: value.into(), schema_id: None, error: None }),
            headers: vec![],
        }
    }

    #[test]
    fn key_filters() {
        let m = msg(Some("order-42"), "x", Encoding::Utf8);
        assert!(matches(&Filter::KeyEquals("order-42".into()), &m));
        assert!(!matches(&Filter::KeyEquals("order-4".into()), &m));
        assert!(matches(&Filter::KeyContains("er-4".into()), &m));
        assert!(!matches(&Filter::KeyContains("nope".into()), &m));
        assert!(!matches(&Filter::KeyEquals("x".into()), &msg(None, "x", Encoding::Utf8))); // null key never key-matches
    }

    #[test]
    fn value_contains() {
        let m = msg(None, "hello kafka world", Encoding::Utf8);
        assert!(matches(&Filter::ValueContains("kafka".into()), &m));
        assert!(!matches(&Filter::ValueContains("rabbit".into()), &m));
    }

    /// A value that failed to decode is stored as the raw
    /// payload's base64 `text` under `Encoding::DecodeError`. Matching
    /// content filters against that base64 blob produces false matches with
    /// no relationship to the actual (undecodable) content — per plan
    /// semantics, a decode-failed value matches NO content filter.
    #[test]
    fn value_contains_never_matches_decode_error() {
        let m = msg(None, "a-base64-blob-that-happens-to-contain-kafka", Encoding::DecodeError);
        assert!(!matches(&Filter::ValueContains("kafka".into()), &m));
    }

    #[test]
    fn json_path_never_matches_decode_error() {
        // Base64 text that also happens to be valid JSON shaped like the path expects.
        let m = msg(None, r#"{"a":1}"#, Encoding::DecodeError);
        let f = Filter::JsonPathEquals { path: "a".into(), value: "1".into() };
        assert!(!matches(&f, &m));
    }

    #[test]
    fn json_path_descends_objects_and_arrays() {
        let m = msg(None, r#"{"payload":{"user":{"id":42},"tags":["a","b"]}}"#, Encoding::Json);
        let f = Filter::JsonPathEquals { path: "payload.user.id".into(), value: "42".into() };
        assert!(matches(&f, &m));
        let f = Filter::JsonPathEquals { path: "payload.tags.1".into(), value: "b".into() };
        assert!(matches(&f, &m));
        let f = Filter::JsonPathEquals { path: "payload.user.id".into(), value: "43".into() };
        assert!(!matches(&f, &m));
        let f = Filter::JsonPathEquals { path: "payload.missing".into(), value: "42".into() };
        assert!(!matches(&f, &m));
    }

    #[test]
    fn json_path_on_non_json_value_never_matches() {
        let m = msg(None, "plain text", Encoding::Utf8);
        assert!(!matches(&Filter::JsonPathEquals { path: "a".into(), value: "1".into() }, &m));
    }

    #[test]
    fn parse_validates() {
        assert!(matches!(Filter::parse("key_eq", "k", None, None), Ok(Filter::KeyEquals(_))));
        assert!(matches!(Filter::parse("json_eq", "42", Some("a.b"), None), Ok(Filter::JsonPathEquals { .. })));
        assert!(matches!(Filter::parse("contains", "x", None, None), Ok(Filter::Contains(_))));
        assert!(Filter::parse("json_eq", "42", None, None).is_err());
        assert!(Filter::parse("sideways", "x", None, None).is_err());
    }

    #[test]
    fn key_filters_are_case_insensitive() {
        let m = msg(Some("Order-42"), "x", Encoding::Utf8);
        assert!(matches(&Filter::parse("key_eq", "ORDER-42", None, None).unwrap(), &m));
        assert!(matches(&Filter::parse("key_contains", "oRdEr", None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("key_eq", "order-4", None, None).unwrap(), &m));
    }

    #[test]
    fn value_contains_is_case_insensitive() {
        let m = msg(None, "Hello KAFKA World", Encoding::Utf8);
        assert!(matches(&Filter::parse("value_contains", "kafka", None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_contains", "rabbit", None, None).unwrap(), &m));
    }

    #[test]
    fn json_eq_is_case_insensitive_on_strings_exact_on_numbers() {
        let m = msg(None, r#"{"user":{"name":"Alice","id":42}}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_eq", "ALICE", Some("user.name"), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "42", Some("user.id"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_eq", "43", Some("user.id"), None).unwrap(), &m));
    }

    #[test]
    fn value_eq_plain_text_is_case_insensitive_equality() {
        let m = msg(None, "Hello World", Encoding::Utf8);
        assert!(matches(&Filter::parse("value_eq", "hello world", None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_eq", "hello", None, None).unwrap(), &m));
    }

    #[test]
    fn value_eq_json_is_semantic_whitespace_and_key_order_immune() {
        let m = msg(None, r#"{"a": 1, "b": {"c": "X"}}"#, Encoding::Json);
        assert!(matches(&Filter::parse("value_eq", r#"{"b":{"c":"x"},"a":1}"#, None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_eq", r#"{"b":{"c":"x"},"a":2}"#, None, None).unwrap(), &m));
    }

    #[test]
    fn value_eq_json_needle_against_non_json_value_falls_back_to_text() {
        let m = msg(None, "not json", Encoding::Utf8);
        assert!(!matches(&Filter::parse("value_eq", r#"{"a":1}"#, None, None).unwrap(), &m));
        let m2 = msg(None, r#"{"a":1}"#, Encoding::Utf8);
        assert!(matches(&Filter::parse("value_eq", r#"{"a": 1}"#, None, None).unwrap(), &m2));
    }

    /// serde's own `Number` equality is variant-exact (`1.0 != 1`), but the
    /// client's JSON.parse collapses every number to a double — the server
    /// must share that model or the same filter matches different rows in
    /// the scanned window vs the live tail.
    #[test]
    fn value_eq_json_numbers_compare_as_doubles_like_the_client() {
        let m = msg(None, r#"{"qty":1}"#, Encoding::Json);
        assert!(matches(&Filter::parse("value_eq", r#"{"qty":1.0}"#, None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_eq", r#"{"qty":2}"#, None, None).unwrap(), &m));
    }

    /// Same double model for path scalars: `100.0` stringifies as "100"
    /// (shortest round-trip, like JS `String(100.0)`), and integers past
    /// 2^53 collapse to the double both sides share — a documented
    /// limitation, not a divergence.
    #[test]
    fn json_path_scalars_stringify_like_the_client_double_model() {
        let m = msg(None, r#"{"amount":100.0,"id":12345678901234567890}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_eq", "100", Some("amount"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_eq", "100.0", Some("amount"), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "12345678901234567000", Some("id"), None).unwrap(), &m));
    }

    #[test]
    fn value_eq_never_matches_decode_error() {
        let m = msg(None, "blob", Encoding::DecodeError);
        assert!(!matches(&Filter::parse("value_eq", "blob", None, None).unwrap(), &m));
    }

    #[test]
    fn json_contains_matches_scalar_substring_case_insensitively() {
        let m = msg(None, r#"{"user":{"name":"Alice Smith","id":42}}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_contains", "SMITH", Some("user.name"), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_contains", "4", Some("user.id"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_contains", "bob", Some("user.name"), None).unwrap(), &m));
    }

    #[test]
    fn json_contains_empty_needle_means_field_exists_as_scalar() {
        let m = msg(None, r#"{"a":{"b":1},"c":"x"}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_contains", "", Some("c"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_contains", "", Some("a"), None).unwrap(), &m)); // object, not scalar
        assert!(!matches(&Filter::parse("json_contains", "", Some("missing"), None).unwrap(), &m));
    }

    #[test]
    fn json_contains_requires_path_and_never_matches_decode_error() {
        assert!(Filter::parse("json_contains", "x", None, None).is_err());
        let m = msg(None, r#"{"a":"x"}"#, Encoding::DecodeError);
        assert!(!matches(&Filter::parse("json_contains", "x", Some("a"), None).unwrap(), &m));
    }

    /// Owner ruling 2026-08-17: a double-quoted run inside a path is part
    /// of its segment with quotes stripped, so field names containing
    /// `.`/`:`/`=` are addressable — `path."to.value"` names `to.value`.
    #[test]
    fn quoted_path_segments_address_fields_with_special_characters() {
        let m = msg(None, r#"{"a.b":1,"x":{"b=c":"hit","d:e":[7]}}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_eq", "1", Some(r#""a.b""#), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "hit", Some(r#"x."b=c""#), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "7", Some(r#"x."d:e".0"#), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_eq", "1", Some(r#""a.c""#), None).unwrap(), &m));
    }

    /// An unclosed quote runs to the end of the path; quoting is
    /// tokenization-only, so a quoted numeric still indexes an array.
    #[test]
    fn quoted_path_edges() {
        let m = msg(None, r#"{"a.b":1,"list":[5]}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_eq", "1", Some(r#""a.b"#), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "5", Some(r#"list."0""#), None).unwrap(), &m));
    }

    fn cmp(kind: &str, q: &str, path: Option<&str>, op: &str) -> Filter {
        Filter::parse(kind, q, path, Some(op)).unwrap()
    }

    /// Owner ruling 2026-08-17: `>`/`>=`/`<`/`<=` compare numbers on all
    /// three targets; a non-numeric target OR query matches nothing.
    #[test]
    fn numeric_comparisons_on_field_scalars() {
        let m = msg(None, r#"{"amount":100.5,"qty":"42","name":"abc"}"#, Encoding::Json);
        assert!(matches(&cmp("json_cmp", "100", Some("amount"), "gt"), &m));
        assert!(!matches(&cmp("json_cmp", "100.5", Some("amount"), "gt"), &m));
        assert!(matches(&cmp("json_cmp", "100.5", Some("amount"), "gte"), &m));
        assert!(matches(&cmp("json_cmp", "101", Some("amount"), "lt"), &m));
        assert!(matches(&cmp("json_cmp", "1e2", Some("amount"), "gt"), &m));
        // A string field parsable as a number counts as that number.
        assert!(matches(&cmp("json_cmp", "41", Some("qty"), "gt"), &m));
        assert!(matches(&cmp("json_cmp", "42", Some("qty"), "lte"), &m));
        // Non-numeric target or query: honest empty, never an error.
        assert!(!matches(&cmp("json_cmp", "1", Some("name"), "gt"), &m));
        assert!(!matches(&cmp("json_cmp", "abc", Some("amount"), "gt"), &m));
        assert!(!matches(&cmp("json_cmp", "", Some("amount"), "gt"), &m));
    }

    #[test]
    fn numeric_comparisons_on_key_and_whole_value() {
        let m = msg(Some("42"), "7", Encoding::Utf8);
        assert!(matches(&cmp("key_cmp", "41", None, "gt"), &m));
        assert!(!matches(&cmp("key_cmp", "42", None, "lt"), &m));
        assert!(matches(&cmp("value_cmp", "7", None, "gte"), &m));
        assert!(!matches(&cmp("value_cmp", "7", None, "lt"), &m));
        // A JSON-string value parsable as a number counts.
        let quoted = msg(None, r#""42""#, Encoding::Json);
        assert!(matches(&cmp("value_cmp", "41", None, "gt"), &quoted));
        // Non-numeric key/value: empty.
        let text = msg(Some("order-42"), "not a number", Encoding::Utf8);
        assert!(!matches(&cmp("key_cmp", "1", None, "gt"), &text));
        assert!(!matches(&cmp("value_cmp", "1", None, "gt"), &text));
    }

    #[test]
    fn comparisons_require_op_and_reject_unknown_op_and_skip_decode_errors() {
        assert!(Filter::parse("json_cmp", "1", Some("a"), None).is_err());
        assert!(Filter::parse("key_cmp", "1", None, Some("sideways")).is_err());
        let m = msg(None, "42", Encoding::DecodeError);
        assert!(!matches(&cmp("value_cmp", "1", None, "gt"), &m));
    }

    /// Owner ruling 2026-08-17: `!=` is the exact negation of `=` — but
    /// only where the target is readable.
    #[test]
    fn not_equals_on_all_targets_case_insensitively() {
        let m = msg(Some("Order-42"), r#"{"status":"Open","qty":2}"#, Encoding::Json);
        assert!(matches(&Filter::parse("key_neq", "order-43", None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("key_neq", "ORDER-42", None, None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_neq", "closed", Some("status"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_neq", "OPEN", Some("status"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_neq", r#"{"qty":2,"status":"open"}"#, None, None).unwrap(), &m));
        assert!(matches(&Filter::parse("value_neq", r#"{"qty":3,"status":"open"}"#, None, None).unwrap(), &m));
    }

    /// A null key, decode-error value, or missing/non-scalar field never
    /// `!=`-matches: we never assert content we couldn't read is different.
    #[test]
    fn not_equals_never_asserts_about_unreadable_content() {
        let m = msg(None, "x", Encoding::DecodeError);
        assert!(!matches(&Filter::parse("key_neq", "a", None, None).unwrap(), &m));
        assert!(!matches(&Filter::parse("value_neq", "a", None, None).unwrap(), &m));
        let j = msg(None, r#"{"a":{"b":1}}"#, Encoding::Json);
        assert!(!matches(&Filter::parse("json_neq", "x", Some("missing"), None).unwrap(), &j));
        assert!(!matches(&Filter::parse("json_neq", "x", Some("a"), None).unwrap(), &j));
    }

    /// Owner ruling 2026-08-17: an unquoted `[]` at the end of a segment
    /// quantifies over that array — the expression matches if ANY element
    /// satisfies the leaf operator.
    #[test]
    fn any_element_marker_matches_when_some_element_satisfies_the_leaf() {
        let m = msg(None, r#"{"items":[{"sku":"aaa","qty":1},{"sku":"BBB","qty":7}],"nums":[1,2,3]}"#, Encoding::Json);
        assert!(matches(&Filter::parse("json_eq", "bbb", Some("items[].sku"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_eq", "ccc", Some("items[].sku"), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_contains", "b", Some("items[].sku"), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_cmp", "5", Some("items[].qty"), Some("gt")).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_cmp", "7", Some("items[].qty"), Some("gt")).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "2", Some("nums[]"), None).unwrap(), &m));
        // `!=` under `[]` is existential: SOME element differs.
        assert!(matches(&Filter::parse("json_neq", "aaa", Some("items[].sku"), None).unwrap(), &m));
    }

    #[test]
    fn any_element_marker_edges() {
        let m = msg(
            None,
            r#"{"empty":[],"notarray":5,"nested":[[1,2],[3]],"same":[5,5],"items[]":1}"#,
            Encoding::Json,
        );
        assert!(!matches(&Filter::parse("json_eq", "1", Some("empty[]"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_eq", "5", Some("notarray[]"), None).unwrap(), &m));
        assert!(matches(&Filter::parse("json_eq", "3", Some("nested[][]"), None).unwrap(), &m));
        assert!(!matches(&Filter::parse("json_neq", "5", Some("same[]"), None).unwrap(), &m));
        // A quoted "items[]" is a literal field name, not a marker.
        assert!(matches(&Filter::parse("json_eq", "1", Some(r#""items[]""#), None).unwrap(), &m));
        // Numeric indexing still works alongside the marker.
        assert!(matches(&Filter::parse("json_eq", "2", Some("nested.0.1"), None).unwrap(), &m));
    }

    #[test]
    fn contains_hits_on_key() {
        let m = msg(Some("order-42"), "irrelevant value", Encoding::Utf8);
        assert!(matches(&Filter::Contains("order".into()), &m));
        assert!(!matches(&Filter::Contains("nope".into()), &m));
    }

    #[test]
    fn contains_hits_on_value() {
        let m = msg(None, "hello kafka world", Encoding::Utf8);
        assert!(matches(&Filter::Contains("kafka".into()), &m));
        assert!(!matches(&Filter::Contains("rabbit".into()), &m));
    }

    #[test]
    fn contains_is_case_insensitive() {
        // Built via `Filter::parse`, like every real call site — `Contains`
        // holds its needle already lower-cased (see its own doc comment), so
        // a query built any other way isn't guaranteed to be case-insensitive.
        let m = msg(Some("Order-42"), "Hello KAFKA World", Encoding::Utf8);
        assert!(matches(&Filter::parse("contains", "ORDER", None, None).unwrap(), &m));
        assert!(matches(&Filter::parse("contains", "kafka", None, None).unwrap(), &m));
    }

    #[test]
    fn contains_never_matches_decode_error_value() {
        let m = msg(None, "a-base64-blob-that-happens-to-contain-special", Encoding::DecodeError);
        assert!(!matches(&Filter::Contains("special".into()), &m));
    }
}
