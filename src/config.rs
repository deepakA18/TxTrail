use serde::Deserialize;

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct SubmissionConfig {
    pub skip_preflight: bool,
    pub preflight_commitment: Commitment,
    pub rpc_max_retries: u64,
    pub client_resubmissions: u64,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct ObservationConfig {
    pub target_commitment: Commitment,
    pub poll_interval_ms: u64,
    pub height_poll_interval_ms: u64,
    pub deadline_ms: u64,
    pub websocket_enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct LimitsConfig {
    pub max_in_flight_transactions: u64,
    pub max_http_requests_per_second: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cluster {
    Local,
    Devnet,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
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
