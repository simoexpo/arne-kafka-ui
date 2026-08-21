use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {0}: {1}")]
    Io(String, std::io::Error),
    #[error("config parse error: {0}")]
    Parse(String),
    #[error("environment variable '{0}' referenced in config is not set")]
    MissingEnvVar(String),
    #[error("unclosed ${{...}} in config")]
    UnclosedInterpolation,
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub clusters: Vec<ClusterConfig>,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap: String,
    /// How often librdkafka should report what it has sent this cluster, in
    /// milliseconds. `0` — the default — leaves the report off entirely: the
    /// callback is never armed, so nothing is parsed and nothing is stored.
    /// Diagnostic only; it costs no broker traffic either way.
    #[serde(default)]
    pub broker_call_stats_ms: u64,
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub schema_registry: Option<SchemaRegistryConfig>,
}

#[derive(Deserialize, Clone, PartialEq)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
    #[serde(default = "default_true")]
    pub tls: bool,
}

/// Hand-written so a stray `tracing::debug!(?cfg)` (or any other Debug
/// rendering) can never leak the plaintext broker password into logs —
/// mirrors `ClusterHandle`'s own hand-written `Debug` for the same reason.
impl std::fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("tls", &self.tls)
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct SchemaRegistryConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Limits {
    #[serde(default = "default_sampler_interval")]
    pub sampler_interval_secs: u64,
    /// Per-request cap on records scanned by one timeline page (`run_page`),
    /// filtered or not — see the messages-timeline design's "Empty-page
    /// contract": this bounds both a filtered scan hunting for matches and
    /// an unfiltered page traversing a hole-dominated region, so neither can
    /// turn into an unbounded scan within a single request.
    #[serde(default = "default_timeline_scan_budget")]
    pub timeline_scan_budget: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            sampler_interval_secs: default_sampler_interval(),
            timeline_scan_budget: default_timeline_scan_budget(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_port() -> u16 {
    8080
}
fn default_sampler_interval() -> u64 {
    10
}
fn default_timeline_scan_budget() -> u64 {
    250_000
}

fn interpolate(raw: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find('}').ok_or(ConfigError::UnclosedInterpolation)?;
        let var = &after[..end];
        let val = lookup(var).ok_or_else(|| ConfigError::MissingEnvVar(var.to_string()))?;
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Interpolates `${VAR}` env references in raw YAML text, WITHOUT touching
/// YAML comments — so a commented-out line referencing an unset env var
/// (e.g. `# password: "${KAFKA_PASSWORD}"`) never fails config load, and the
/// spec's canonical unquoted-flow-scalar syntax (`password: ${VAR}` inside a
/// `{ ... }` flow mapping) still works, since we interpolate before parsing.
///
/// Processes the text line by line. Within each line, a `#` starts a YAML
/// comment when it is NOT inside a quoted string and is either at the start
/// of the line or preceded by whitespace (matching YAML's own comment rule).
/// Everything before that `#` is interpolated via `interpolate()`; the `#`
/// and the rest of the line are appended verbatim.
///
/// Known limitation: block scalars (`|` / `>`) whose content contains an
/// unquoted ` #` will not have text after that `#` interpolated, since this
/// function has no concept of block-scalar context — it only tracks quotes.
/// Acceptable for v1: no config field uses block scalars.
fn interpolate_outside_comments(
    raw: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(raw.len());
    let mut lines = raw.split('\n').peekable();
    while let Some(line) = lines.next() {
        let comment_start = find_comment_start(line);
        let (code, comment) = line.split_at(comment_start);
        out.push_str(&interpolate(code, lookup)?);
        out.push_str(comment);
        if lines.peek().is_some() {
            out.push('\n');
        }
    }
    Ok(out)
}

/// Finds the byte offset of the `#` that starts a YAML comment in `line`, or
/// `line.len()` if there is none.
///
/// This is a pragmatic approximation of YAML tokenization, sufficient for
/// config files — not a full YAML lexer. Rules:
/// - A `#` starts a comment when it is outside any quoted string and is
///   either at the start of the line or preceded by whitespace (matching
///   YAML's own comment rule).
/// - A `'` or `"` OPENS a quoted string only when it can start a scalar.
///   `:` and `-` are only structural (mapping/sequence) indicators when
///   FOLLOWED by whitespace, so a quote may open only when: it is at the
///   start of the line (ignoring leading whitespace); OR it immediately (no
///   space) follows `[`, `{`, or `,` (flow collections don't require a space
///   after these); OR it immediately follows whitespace AND the nearest
///   non-space character before that whitespace is `:`, `-`, `,`, `[`, `{`
///   (indicator-then-space-then-quote), or there is none. A quote glued
///   directly to a preceding `-` or `:` (no space) is plain-scalar content
///   (e.g. `db-'s-host`, `x:'s-host`) and never opens a string; nor does one
///   glued to any other non-indicator character (e.g. the apostrophe in
///   `alice's`).
/// - Once inside a single-quoted string, `''` is an escaped literal quote
///   (the pair is skipped); a lone `'` closes the string.
/// - Once inside a double-quoted string, `\"` escapes (the pair is
///   skipped); an unescaped `"` closes the string.
///
/// Known limitation: block scalars (`|` / `>`) whose content contains an
/// unquoted ` #` will not have text after that `#` interpolated, since this
/// function has no concept of block-scalar context. Acceptable for v1: no
/// config field uses block scalars.
fn find_comment_start(line: &str) -> usize {
    fn can_open_quote(prev_was_space: bool, last_non_ws: Option<char>) -> bool {
        match last_non_ws {
            None => true, // (a) line start, ignoring leading whitespace
            Some(c) if !prev_was_space => matches!(c, '[' | '{' | ','), // (b) glued: only flow separators are structural without a space
            Some(c) => matches!(c, ':' | '-' | ',' | '[' | '{'), // (c) indicator-then-space-then-quote
        }
    }

    let mut in_single = false;
    let mut in_double = false;
    let mut prev_was_space = true; // start-of-line counts as preceded by whitespace
    let mut last_non_ws: Option<char> = None; // nearest non-whitespace char seen, for quote-open rule
    let mut chars = line.char_indices().peekable();
    while let Some((i, ch)) = chars.next() {
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next(); // escaped '' — stays inside the string
                } else {
                    in_single = false;
                }
            }
        } else if in_double {
            if ch == '\\' {
                if chars.peek().is_some() {
                    chars.next(); // escaped char — skip it
                }
            } else if ch == '"' {
                in_double = false;
            }
        } else {
            match ch {
                '\'' if can_open_quote(prev_was_space, last_non_ws) => in_single = true,
                '"' if can_open_quote(prev_was_space, last_non_ws) => in_double = true,
                '#' if prev_was_space => return i,
                _ => {}
            }
        }
        prev_was_space = ch.is_whitespace();
        if !ch.is_whitespace() {
            last_non_ws = Some(ch);
        }
    }
    line.len()
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.clusters.is_empty() {
            return Err(ConfigError::Invalid(
                "clusters: at least one cluster is required".into(),
            ));
        }
        let mut seen = HashSet::new();
        for (i, c) in self.clusters.iter().enumerate() {
            let name_ok = !c.name.is_empty()
                && c.name
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || "._-".contains(ch));
            if !name_ok {
                return Err(ConfigError::Invalid(format!(
                    "clusters[{i}].name: must be non-empty and contain only [a-zA-Z0-9._-], got '{}'",
                    c.name
                )));
            }
            if !seen.insert(c.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "clusters[{i}].name: duplicate name '{}'",
                    c.name
                )));
            }
            if c.bootstrap.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "clusters[{i}].bootstrap: must not be empty"
                )));
            }
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(path.display().to_string(), e))?;
        let interpolated = interpolate_outside_comments(&raw, &|v| std::env::var(v).ok())?;
        let cfg: Config =
            serde_yaml::from_str(&interpolated).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = r#"
clusters:
  - name: prod
    bootstrap: broker1:9092,broker2:9092
    sasl: { mechanism: SCRAM-SHA-512, username: app, password: "${TEST_KAFKA_PW}" }
    schema_registry: { url: http://sr:8081 }
  - name: local
    bootstrap: localhost:9092
server: { port: 9000 }
limits: { sampler_interval_secs: 5 }
"#;

    #[test]
    fn sasl_config_debug_redacts_the_password() {
        let sasl = SaslConfig {
            mechanism: SaslMechanism::Plain,
            username: "app".into(),
            password: "s3cret".into(),
            tls: true,
        };
        let debug = format!("{sasl:?}");
        assert!(
            !debug.contains("s3cret"),
            "password must never appear in Debug output: {debug}"
        );
        assert!(debug.contains("<redacted>"), "got: {debug}");
    }

    fn parse(yaml: &str) -> Result<Config, ConfigError> {
        let interpolated = interpolate_outside_comments(yaml, &|v| {
            (v == "TEST_KAFKA_PW").then(|| "s3cret".to_string())
        })?;
        let cfg: Config =
            serde_yaml::from_str(&interpolated).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn parses_full_config_with_env_interpolation() {
        let cfg = parse(GOOD).unwrap();
        assert_eq!(cfg.clusters.len(), 2);
        assert_eq!(cfg.clusters[0].name, "prod");
        let sasl = cfg.clusters[0].sasl.as_ref().unwrap();
        assert_eq!(sasl.password, "s3cret");
        assert!(sasl.tls);
        assert!(matches!(sasl.mechanism, SaslMechanism::ScramSha512));
        assert_eq!(cfg.clusters[1].sasl, None);
        assert_eq!(cfg.server.port, 9000);
    }

    #[test]
    fn defaults_apply_when_sections_missing() {
        let cfg = parse("clusters:\n  - name: a\n    bootstrap: x:9092\n").unwrap();
        assert_eq!(cfg.server.port, 8080);
        assert_eq!(cfg.limits.sampler_interval_secs, 10);
        assert_eq!(cfg.limits.timeline_scan_budget, 250_000);
    }

    #[test]
    fn missing_env_var_is_a_precise_error() {
        let err = parse("clusters:\n  - name: a\n    bootstrap: \"${NOPE}\"\n").unwrap_err();
        assert!(err.to_string().contains("NOPE"), "got: {err}");
    }

    #[test]
    fn unknown_sasl_mechanism_fails_parse() {
        let yaml = "clusters:\n  - name: a\n    bootstrap: x\n    sasl: { mechanism: MAGIC, username: u, password: p }\n";
        let err = parse(yaml).unwrap_err();
        assert!(
            err.to_string().contains("MAGIC") || err.to_string().contains("unknown variant"),
            "got: {err}"
        );
    }

    #[test]
    fn empty_clusters_rejected() {
        let err = parse("clusters: []\n").unwrap_err();
        assert!(err.to_string().contains("at least one"), "got: {err}");
    }

    #[test]
    fn duplicate_names_rejected() {
        let err = parse("clusters:\n  - {name: a, bootstrap: x}\n  - {name: a, bootstrap: y}\n")
            .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got: {err}");
    }

    #[test]
    fn invalid_name_rejected() {
        let err = parse("clusters:\n  - {name: \"a b\", bootstrap: x}\n").unwrap_err();
        assert!(err.to_string().contains("clusters[0].name"), "got: {err}");
    }

    #[test]
    fn interpolate_handles_multiple_vars_and_unclosed() {
        let ok = interpolate("a=${X} b=${Y}", &|v| Some(format!("<{v}>"))).unwrap();
        assert_eq!(ok, "a=<X> b=<Y>");
        assert!(interpolate("bad ${X", &|_| Some("v".into())).is_err());
    }

    #[test]
    fn commented_out_interpolation_is_ignored() {
        let yaml = "clusters:\n  - name: local\n    bootstrap: localhost:9092\n    # sasl: { mechanism: SCRAM-SHA-512, username: app, password: \"${NOPE}\" }\n";
        // lookup knows nothing: if the comment were interpolated, this would error.
        let cfg = parse(yaml).unwrap();
        assert_eq!(cfg.clusters[0].name, "local");
        assert_eq!(cfg.clusters[0].sasl, None);
    }

    #[test]
    fn active_values_still_interpolate() {
        let cfg = parse(GOOD).unwrap();
        let sasl = cfg.clusters[0].sasl.as_ref().unwrap();
        assert_eq!(sasl.password, "s3cret");
    }

    #[test]
    fn unquoted_var_in_flow_mapping_interpolates() {
        // Canonical syntax: unquoted ${VAR} inside
        // a flow mapping. This must remain valid, since `{`/`}` are YAML flow
        // indicators and become invalid syntax if left unquoted-but-unsubstituted.
        let yaml = "clusters:\n  - name: prod\n    bootstrap: broker1:9092\n    sasl: { mechanism: SCRAM-SHA-512, username: app, password: ${TEST_KAFKA_PW} }\n";
        let cfg = parse(yaml).unwrap();
        let sasl = cfg.clusters[0].sasl.as_ref().unwrap();
        assert_eq!(sasl.password, "s3cret");
    }

    #[test]
    fn hash_inside_quoted_value_still_interpolates() {
        let yaml = "clusters:\n  - name: a\n    bootstrap: x\n    sasl: { mechanism: PLAIN, username: u, password: \"ab#${TEST_KAFKA_PW}\" }\n";
        let cfg = parse(yaml).unwrap();
        let sasl = cfg.clusters[0].sasl.as_ref().unwrap();
        assert_eq!(sasl.password, "ab#s3cret");
    }

    #[test]
    fn trailing_comment_after_value_ignored() {
        let yaml = "clusters:\n  - name: a\n    bootstrap: x:9092  # uses ${NOPE}\n";
        // lookup knows nothing (not even NOPE): if the trailing comment were
        // interpolated, this would error.
        let cfg = parse(yaml).unwrap();
        assert_eq!(cfg.clusters[0].bootstrap, "x:9092");
    }

    #[test]
    fn apostrophe_in_plain_scalar_does_not_break_comment_detection() {
        let yaml = "clusters:\n  - name: a\n    bootstrap: alice's-host:9092  # uses ${NOPE}\n";
        // A bare apostrophe inside an unquoted plain scalar is valid YAML and must
        // not be mistaken for the start of a single-quoted string — otherwise the
        // real trailing comment gets treated as in-quote code and interpolated.
        // lookup knows nothing (not even NOPE): if that happened, this would error.
        let cfg = parse(yaml).unwrap();
        assert_eq!(cfg.clusters[0].bootstrap, "alice's-host:9092");
    }

    #[test]
    fn hyphen_glued_quote_is_scalar_content() {
        // `-` is only a structural (sequence/flow) indicator when followed by
        // whitespace. Glued directly to a quote (no space), it's plain-scalar
        // text, so the quote must NOT open a string here.
        let yaml = "clusters:\n  - name: a\n    bootstrap: db-'s-host:9092 # uses ${NOPE}\n";
        let cfg = parse(yaml).unwrap();
        assert_eq!(cfg.clusters[0].bootstrap, "db-'s-host:9092");
    }

    #[test]
    fn colon_glued_quote_is_scalar_content() {
        // Same rule for `:` — only structural when followed by whitespace.
        let yaml = "clusters:\n  - name: a\n    bootstrap: x:'s-host:9092 # uses ${NOPE}\n";
        let cfg = parse(yaml).unwrap();
        assert_eq!(cfg.clusters[0].bootstrap, "x:'s-host:9092");
    }

    #[test]
    fn quotes_after_flow_separators_still_open() {
        // Positive guard: flow collections don't require a space after `[`,
        // `{`, or `,`, so a quote glued directly to one of those must still
        // open a string, and `${VAR}` inside it must still interpolate.
        let yaml = "sasl: { mechanism: 'PLAIN', username: 'u' }\ntags: ['${TEST_KAFKA_PW}','b']\n";
        let out = interpolate_outside_comments(yaml, &|v| {
            (v == "TEST_KAFKA_PW").then(|| "s3cret".to_string())
        })
        .unwrap();
        assert!(out.contains("['s3cret','b']"), "got: {out}");
    }
}
