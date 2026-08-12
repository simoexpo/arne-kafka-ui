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
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub schema_registry: Option<SchemaRegistryConfig>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
    #[serde(default = "default_true")]
    pub tls: bool,
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
    fn default() -> Self { Self { port: default_port() } }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Limits {
    #[serde(default = "default_max_matches")]
    pub max_search_matches: u32,
    #[serde(default = "default_sampler_interval")]
    pub sampler_interval_secs: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_search_matches: default_max_matches(), sampler_interval_secs: default_sampler_interval() }
    }
}

fn default_true() -> bool { true }
fn default_port() -> u16 { 8080 }
fn default_max_matches() -> u32 { 500 }
fn default_sampler_interval() -> u64 { 10 }

pub fn interpolate(raw: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<String, ConfigError> {
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
/// - A `'` or `"` OPENS a quoted string only when it can start a scalar:
///   the previous non-whitespace character on the line is one of `:` `,`
///   `[` `{` `-`, or there is none (line start). Otherwise the quote
///   character is plain-scalar content (e.g. the apostrophe in `alice's`)
///   and is ignored — it never toggles quote state.
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
    fn can_open_quote(last_non_ws: Option<char>) -> bool {
        match last_non_ws {
            None => true,
            Some(c) => matches!(c, ':' | ',' | '[' | '{' | '-'),
        }
    }

    let mut in_single = false;
    let mut in_double = false;
    let mut prev_was_space = true; // start-of-line counts as preceded by whitespace
    let mut last_non_ws: Option<char> = None; // last non-whitespace char seen, for quote-open rule
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
                '\'' if can_open_quote(last_non_ws) => in_single = true,
                '"' if can_open_quote(last_non_ws) => in_double = true,
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
            return Err(ConfigError::Invalid("clusters: at least one cluster is required".into()));
        }
        let mut seen = HashSet::new();
        for (i, c) in self.clusters.iter().enumerate() {
            let name_ok = !c.name.is_empty()
                && c.name.chars().all(|ch| ch.is_ascii_alphanumeric() || "._-".contains(ch));
            if !name_ok {
                return Err(ConfigError::Invalid(format!(
                    "clusters[{i}].name: must be non-empty and contain only [a-zA-Z0-9._-], got '{}'", c.name
                )));
            }
            if !seen.insert(c.name.clone()) {
                return Err(ConfigError::Invalid(format!("clusters[{i}].name: duplicate name '{}'", c.name)));
            }
            if c.bootstrap.trim().is_empty() {
                return Err(ConfigError::Invalid(format!("clusters[{i}].bootstrap: must not be empty")));
            }
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(path.display().to_string(), e))?;
        let interpolated = interpolate_outside_comments(&raw, &|v| std::env::var(v).ok())?;
        let cfg: Config = serde_yaml::from_str(&interpolated).map_err(|e| ConfigError::Parse(e.to_string()))?;
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
limits: { max_search_matches: 100, sampler_interval_secs: 5 }
"#;

    fn parse(yaml: &str) -> Result<Config, ConfigError> {
        let interpolated = interpolate_outside_comments(yaml, &|v| {
            (v == "TEST_KAFKA_PW").then(|| "s3cret".to_string())
        })?;
        let cfg: Config = serde_yaml::from_str(&interpolated)
            .map_err(|e| ConfigError::Parse(e.to_string()))?;
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
        assert_eq!(cfg.limits.max_search_matches, 100);
    }

    #[test]
    fn defaults_apply_when_sections_missing() {
        let cfg = parse("clusters:\n  - name: a\n    bootstrap: x:9092\n").unwrap();
        assert_eq!(cfg.server.port, 8080);
        assert_eq!(cfg.limits.max_search_matches, 500);
        assert_eq!(cfg.limits.sampler_interval_secs, 10);
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
        assert!(err.to_string().contains("MAGIC") || err.to_string().contains("unknown variant"), "got: {err}");
    }

    #[test]
    fn empty_clusters_rejected() {
        let err = parse("clusters: []\n").unwrap_err();
        assert!(err.to_string().contains("at least one"), "got: {err}");
    }

    #[test]
    fn duplicate_names_rejected() {
        let err = parse("clusters:\n  - {name: a, bootstrap: x}\n  - {name: a, bootstrap: y}\n").unwrap_err();
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
        // Spec's canonical syntax (design spec line ~166): unquoted ${VAR} inside
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
}
