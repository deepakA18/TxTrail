use crate::model::Commitment;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub cluster: Cluster,
    pub rpc_url: String,
    pub route_id: String,
    pub observer_id: String,
    pub output_dir: String,
    pub submission: SubmissionConfig,
    pub observation: ObservationConfig,
    pub limits: LimitsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionConfig {
    pub skip_preflight: bool,
    pub preflight_commitment: Commitment,
    pub rpc_max_retries: u64,
    pub client_resubmissions: u64,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationConfig {
    pub target_commitment: Commitment,
    pub poll_interval_ms: u64,
    pub height_poll_interval_ms: u64,
    pub deadline_ms: u64,
    pub websocket_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_in_flight_transactions: u64,
    pub max_http_requests_per_second: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "lowercase")]
pub enum Cluster {
    Local,
    Devnet,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.observation.poll_interval_ms == 0 {
            return Err("observation.poll_interval_ms must be greater than zero".into());
        }

        if self.submission.request_timeout_ms == 0 {
            return Err("submission.request_timeout_ms must be greater than zero".into());
        }

        if self.observation.height_poll_interval_ms == 0 {
            return Err("observation.height_poll_interval_ms must be greater than zero".into());
        }

        if self.observation.deadline_ms == 0 {
            return Err("observation.deadline_ms must be greater than zero".into());
        }

        if self.limits.max_in_flight_transactions == 0 {
            return Err("limits.max_in_flight_transactions must be greater than zero".into());
        }

        if self.limits.max_http_requests_per_second == 0 {
            return Err("limits.max_http_requests_per_second must be greater than zero".into());
        }

        if self.rpc_url.trim().is_empty() {
            return Err("rpc_url must not be empty".into());
        }

        if self.route_id.trim().is_empty() {
            return Err("route id must not be empty".into());
        }

        if self.observer_id.trim().is_empty() {
            return Err("observation id must not be empty".into());
        }

        if self.output_dir.trim().is_empty() {
            return Err("output dir must not be empty".into());
        }

        if self.submission.client_resubmissions != 0 {
            return Err("client_resubmissions must be 0 in v1".into());
        }
        if self.observation.target_commitment != Commitment::Finalized {
            return Err("v1 requires observation.target_commitment = finalized".into());
        }
        if self.observation.websocket_enabled {
            return Err("WebSocket observation is not implemented; use polling".into());
        }
        if self.limits.max_in_flight_transactions != 1 {
            return Err("v1 supports one transaction at a time".into());
        }
        let url = reqwest::Url::parse(&self.rpc_url).map_err(|_| "Invalid RPC URL")?;
        if !["http", "https"].contains(&url.scheme())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("RPC URL must be HTTP(S) without credentials, query, or fragment".into());
        }
        let host = url.host_str().unwrap_or("").trim_matches(['[', ']']);
        let loopback = host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        match self.cluster {
            Cluster::Local if !loopback => {
                return Err("local profile requires a loopback endpoint".into());
            }
            Cluster::Devnet if url.as_str() != "https://api.devnet.solana.com/" => {
                return Err("devnet profile requires https://api.devnet.solana.com".into());
            }
            _ => {}
        }
        if !self
            .route_id
            .chars()
            .chain(self.observer_id.chars())
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        {
            return Err("route/observer aliases must use letters, numbers, '-' or '_'".into());
        }
        Ok(())
    }

    pub fn load_config(path: &std::path::Path) -> Result<Config, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|err| format!("Cannot read {}: {err}", path.display()))?;

        let config = toml::from_str::<Config>(&contents)
            .map_err(|err| format!("Failed to parse TOML config {}: {err}", path.display()))?;

        config.validate()?;
        Ok(config)
    }
}
