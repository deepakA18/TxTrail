use crate::{
    config::{Cluster, Config},
    model::{Commitment, Event, TransportFailureKind},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

pub const DEVNET_GENESIS: &str = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG";
const MAINNET_GENESIS: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
const TESTNET_GENESIS: &str = "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY";
#[derive(Debug, Clone)]
pub struct RpcError {
    pub category: String,
    pub rpc: Option<Value>,
    pub transport: Option<TransportFailureKind>,
}
impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.category)
    }
}
impl std::error::Error for RpcError {}
impl RpcError {
    fn protocol(message: &str) -> Self {
        Self {
            category: message.into(),
            rpc: None,
            transport: None,
        }
    }
}
#[derive(Clone)]
pub struct Rpc {
    client: reqwest::Client,
    url: String,
    next: Arc<tokio::sync::Mutex<Instant>>,
    interval: Duration,
    ids: Arc<AtomicU64>,
    diagnostics: Arc<Mutex<Vec<Event>>>,
}
impl Rpc {
    pub fn new(config: &Config) -> Result<Self, String> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(Duration::from_millis(config.submission.request_timeout_ms))
            .build()
            .map_err(|_| "Cannot initialize HTTP client")?;
        Ok(Self {
            client,
            url: config.rpc_url.clone(),
            next: Arc::new(tokio::sync::Mutex::new(Instant::now())),
            interval: Duration::from_secs_f64(
                1.0 / config.limits.max_http_requests_per_second as f64,
            ),
            ids: Arc::new(AtomicU64::new(1)),
            diagnostics: Arc::new(Mutex::new(vec![])),
        })
    }
    pub fn drain_diagnostics(&self) -> Vec<Event> {
        std::mem::take(&mut *self.diagnostics.lock().unwrap())
    }
    async fn throttle(&self) {
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.interval;
    }
    async fn defer(&self, duration: Duration) {
        let mut next = self.next.lock().await;
        *next = (*next).max(Instant::now() + duration);
    }
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let attempts = if method == "sendTransaction" { 1 } else { 3 };
        for attempt in 0..attempts {
            self.throttle().await;
            let id = self.ids.fetch_add(1, Ordering::Relaxed);
            let response = self
                .client
                .post(&self.url)
                .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .send()
                .await;
            let mut retry_after = None;
            let result = match response {
                Err(e) => Err(RpcError {
                    category: if e.is_timeout() {
                        "transport_timeout"
                    } else if e.is_connect() {
                        "transport_connect"
                    } else {
                        "transport_read"
                    }
                    .into(),
                    rpc: None,
                    transport: Some(if e.is_timeout() {
                        TransportFailureKind::Timeout
                    } else if e.is_connect() {
                        TransportFailureKind::Connect
                    } else {
                        TransportFailureKind::Read
                    }),
                }),
                Ok(mut response) => {
                    let status = response.status();
                    if status.as_u16() == 429 {
                        retry_after = response
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|v| v.to_str().ok())
                            .and_then(retry_delay);
                    }
                    if !status.is_success() {
                        Err(RpcError::protocol(&format!("http_{}", status.as_u16())))
                    } else {
                        let mut body = Vec::new();
                        let mut failure = None;
                        loop {
                            match response.chunk().await {
                                Ok(Some(chunk)) if body.len() + chunk.len() <= 4 * 1024 * 1024 => {
                                    body.extend_from_slice(&chunk)
                                }
                                Ok(Some(_)) => {
                                    failure = Some(RpcError::protocol("response_too_large"));
                                    break;
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    failure = Some(RpcError {
                                        category: "transport_read".into(),
                                        rpc: None,
                                        transport: Some(if e.is_timeout() {
                                            TransportFailureKind::Timeout
                                        } else {
                                            TransportFailureKind::Read
                                        }),
                                    });
                                    break;
                                }
                            }
                        }
                        if let Some(e) = failure {
                            Err(e)
                        } else {
                            decode(&body, id)
                        }
                    }
                }
            };
            let category = result
                .as_ref()
                .map(|_| "ok".to_string())
                .unwrap_or_else(|e| e.category.clone());
            let retryable = result.as_ref().err().is_some_and(|e| {
                e.transport.is_some()
                    || e.category == "http_429"
                    || e.category == "http_500"
                    || e.category == "http_503"
            });
            let backoff = retry_after.unwrap_or(Duration::from_millis(
                (250_u64 << attempt) + (id.wrapping_mul(37) % 97),
            ));
            self.diagnostics.lock().unwrap().push(Event::RpcDiagnostic {
                method: method.into(),
                request_id: id,
                category,
                retry_after_ms: if retryable {
                    Some(backoff.as_millis().min(u64::MAX as u128) as u64)
                } else {
                    None
                },
            });
            if retryable {
                self.defer(backoff).await;
            }
            if !retryable || attempt + 1 == attempts {
                return result;
            }
        }
        unreachable!()
    }
    pub async fn verify_network(&self, config: &Config) -> Result<String, RpcError> {
        let identity = self
            .call("getGenesisHash", json!([]))
            .await?
            .as_str()
            .ok_or_else(|| RpcError::protocol("invalid_genesis_hash"))?
            .to_owned();
        match config.cluster {
            Cluster::Devnet if identity != DEVNET_GENESIS => {
                return Err(RpcError::protocol("devnet_genesis_mismatch"));
            }
            Cluster::Local
                if [MAINNET_GENESIS, TESTNET_GENESIS, DEVNET_GENESIS]
                    .contains(&identity.as_str()) =>
            {
                return Err(RpcError::protocol("local_profile_points_to_public_cluster"));
            }
            _ => {}
        }
        Ok(identity)
    }
    pub async fn latest(&self, commitment: &Commitment) -> Result<(String, u64, u64), RpcError> {
        let v = self
            .call("getLatestBlockhash", json!([{"commitment":commitment}]))
            .await?;
        Ok((
            v["value"]["blockhash"]
                .as_str()
                .ok_or_else(|| RpcError::protocol("missing_blockhash"))?
                .into(),
            number(&v["value"]["lastValidBlockHeight"])?,
            number(&v["context"]["slot"])?,
        ))
    }
    pub async fn height(&self) -> Result<u64, RpcError> {
        number(
            &self
                .call("getBlockHeight", json!([{"commitment":"finalized"}]))
                .await?,
        )
    }
    pub async fn status(
        &self,
        signature: &str,
        history: bool,
    ) -> Result<Option<(u64, Commitment, Option<Value>)>, RpcError> {
        let v = self
            .call(
                "getSignatureStatuses",
                json!([[signature],{"searchTransactionHistory":history}]),
            )
            .await?;
        let statuses = v["value"]
            .as_array()
            .filter(|v| v.len() == 1)
            .ok_or_else(|| RpcError::protocol("invalid_status_array"))?;
        let status = &statuses[0];
        if status.is_null() {
            return Ok(None);
        }
        let commitment: Commitment =
            serde_json::from_value(status["confirmationStatus"].clone())
                .map_err(|_| RpcError::protocol("missing_or_unknown_commitment"))?;
        let err = status
            .get("err")
            .ok_or_else(|| RpcError::protocol("missing_execution_error"))?;
        Ok(Some((
            number(&status["slot"])?,
            commitment,
            if err.is_null() {
                None
            } else {
                Some(sanitize(err))
            },
        )))
    }
    pub async fn transaction(&self, signature: &str) -> Result<Value, RpcError> {
        self.call("getTransaction",json!([signature,{"commitment":"finalized","encoding":"json","maxSupportedTransactionVersion":0}])).await
    }
}
fn number(v: &Value) -> Result<u64, RpcError> {
    v.as_u64()
        .ok_or_else(|| RpcError::protocol("invalid_numeric_response"))
}
fn decode(bytes: &[u8], id: u64) -> Result<Value, RpcError> {
    let v: Value =
        serde_json::from_slice(bytes).map_err(|_| RpcError::protocol("malformed_json"))?;
    if v["jsonrpc"] != "2.0" || v["id"] != id {
        return Err(RpcError::protocol("jsonrpc_response_identity_mismatch"));
    }
    if let Some(error) = v.get("error") {
        if v.get("result").is_some() || error["code"].as_i64().is_none() {
            return Err(RpcError::protocol("invalid_jsonrpc_error"));
        }
        return Err(RpcError {
            category: "rpc_error".into(),
            rpc: Some(sanitize(error)),
            transport: None,
        });
    }
    v.get("result")
        .cloned()
        .ok_or_else(|| RpcError::protocol("missing_jsonrpc_result"))
}
fn retry_delay(s: &str) -> Option<Duration> {
    s.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
        httpdate::parse_http_date(s)
            .ok()
            .map(|time| time.duration_since(SystemTime::now()).unwrap_or_default())
    })
}
/// Keep error structure and known symbolic values, redact free-form strings that
/// could contain authenticated endpoints or key material supplied by a server.
pub fn sanitize(v: &Value) -> Value {
    match v {
        Value::String(s)
            if s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') =>
        {
            v.clone()
        }
        Value::String(_) => json!("[redacted text]"),
        Value::Array(a) => Value::Array(a.iter().map(sanitize).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        if ["token", "secret", "apiKey", "url", "keypair"].contains(&k.as_str()) {
                            json!("[redacted]")
                        } else {
                            sanitize(v)
                        },
                    )
                })
                .collect(),
        ),
        _ => v.clone(),
    }
}
