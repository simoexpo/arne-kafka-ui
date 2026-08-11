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
        let interpolated = interpolate(&raw, &|v| std::env::var(v).ok())?;
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
        let interpolated = interpolate(yaml, &|v| {
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
}
