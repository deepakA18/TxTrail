use crate::{
    config::{Cluster, Config},
    model::*,
    rpc::Rpc,
    store::{self, Recorder, Writer},
    transaction::{self, Workload},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
#[derive(Clone)]
struct Capture {
    recorder: Recorder,
    run: String,
    session: String,
    transaction: String,
    signature: Option<String>,
    start: Instant,
    ids: Arc<AtomicU64>,
}
impl Capture {
    fn envelope(&self, event: Event, attempt: Option<&str>, source: &str) -> EventEnvelope {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        EventEnvelope {
            schema_version: 1,
            run_id: self.run.clone(),
            session_id: self.session.clone(),
            sequence: 0,
            event_id: format!("{}-{id}", self.session),
            transaction_id: self.transaction.clone(),
            signature: self.signature.clone(),
            attempt_id: attempt.map(str::to_owned),
            source_id: source.into(),
            observed_at_utc: chrono::Utc::now().to_rfc3339(),
            elapsed_ns: self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            event,
        }
    }
    async fn emit(&self, event: Event, attempt: Option<&str>, source: &str) -> Result<(), String> {
        let envelope = self.envelope(event, attempt, source);
        self.recorder.record(envelope).await?;
        Ok(())
    }
    async fn diagnostics(&self, rpc: &Rpc, source: &str) -> Result<(), String> {
        for e in rpc.drain_diagnostics() {
            self.emit(e, None, source).await?;
        }
        Ok(())
    }
}
fn unique() -> String {
    format!(
        "{}-{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%S%f"),
        std::process::id()
    )
}
async fn metadata(c: &Capture, config: &Config, network: String) -> Result<(), String> {
    let hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(config).map_err(|e| e.to_string())?)
    );
    c.emit(Event::RunStarted{tool_version:env!("CARGO_PKG_VERSION").into(),configuration_hash:hash,network_identity:network,environment:json!({"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"route_id":config.route_id,"observer_id":config.observer_id,"poll_interval_ms":config.observation.poll_interval_ms,"height_poll_interval_ms":config.observation.height_poll_interval_ms,"deadline_ms":config.observation.deadline_ms,"target_commitment":config.observation.target_commitment,"rpc_max_retries":config.submission.rpc_max_retries,"format":"legacy","observer_startup":"pending signature registered before send; polling and submission share limiter"})},None,&config.route_id).await
}
async fn bounded<T>(
    ms: u64,
    f: impl std::future::Future<Output = Result<T, crate::rpc::RpcError>>,
) -> Result<T, String> {
    tokio::time::timeout(Duration::from_millis(ms), f)
        .await
        .map_err(|_| "RPC operation budget exceeded".to_string())?
        .map_err(|e| e.to_string())
}
pub async fn send(config: &Config, keypair: &Path, workload: Workload) -> Result<PathBuf, String> {
    config.validate()?;
    if !matches!(workload, Workload::Transfer) && !matches!(config.cluster, Cluster::Local) {
        return Err("Failure workloads are local-only".into());
    }
    let rpc = Rpc::new(config)?;
    let network = bounded(30_000, rpc.verify_network(config)).await?;
    let prepared = tokio::time::timeout(
        Duration::from_secs(60),
        transaction::prepare(&rpc, config, keypair, workload),
    )
    .await
    .map_err(|_| "Transaction preparation budget exceeded")??;
    let run = unique();
    let dir = PathBuf::from(&config.output_dir).join(&run);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let writer = Writer::open(&dir.join("events.jsonl"), false, 64)?;
    let capture = Capture {
        recorder: writer.recorder.clone(),
        run,
        session: unique(),
        transaction: prepared.signature.clone(),
        signature: Some(prepared.signature.clone()),
        start: Instant::now(),
        ids: Arc::new(AtomicU64::new(1)),
    };
    eprintln!("Recording run: {}", dir.display());
    let result = async {
        metadata(&capture, config, network).await?;
        capture
            .emit(
                Event::TransactionPrepared {
                    signature: prepared.signature.clone(),
                    wire_hash: prepared.wire_hash.clone(),
                    blockhash: prepared.blockhash.clone(),
                    last_valid_block_height: prepared.last_valid_block_height,
                    blockhash_context_slot: prepared.context_slot,
                    workload_parameters: prepared.workload.clone(),
                },
                None,
                &config.route_id,
            )
            .await?;
        capture.diagnostics(&rpc, &config.route_id).await?;
        let skip = match workload {
            Workload::ExecutionError => true,
            Workload::PreflightError => false,
            Workload::Transfer => config.submission.skip_preflight,
        };
        let submit = async {
            capture
                .emit(
                    Event::SendStarted {
                        route: config.route_id.clone(),
                        preflight_commitment: config.submission.preflight_commitment.clone(),
                        skip_preflight: skip,
                        rpc_max_retries: config.submission.rpc_max_retries,
                    },
                    Some("attempt-1"),
                    &config.route_id,
                )
                .await?;
            // No automatic resend, even if HTTP times out after server acceptance.
            let response=rpc.call("sendTransaction",json!([prepared.wire,{"encoding":"base64","skipPreflight":skip,"preflightCommitment":config.submission.preflight_commitment,"maxRetries":config.submission.rpc_max_retries,"minContextSlot":prepared.context_slot}])).await;
            match response {
                Ok(value) if value.as_str() == Some(&prepared.signature) => {
                    capture
                        .emit(
                            Event::RpcAcknowledged {
                                returned_signature: prepared.signature.clone(),
                            },
                            Some("attempt-1"),
                            &config.route_id,
                        )
                        .await?
                }
                Ok(_) => {
                    capture
                        .emit(
                            Event::TransportFailed {
                                kind: TransportFailureKind::Read,
                            },
                            Some("attempt-1"),
                            &config.route_id,
                        )
                        .await?;
                    capture
                        .emit(
                            Event::RpcDiagnostic {
                                method: "sendTransaction".into(),
                                request_id: 0,
                                category: "mismatched_returned_signature".into(),
                                retry_after_ms: None,
                            },
                            None,
                            &config.route_id,
                        )
                        .await?;
                }
                Err(error) => {
                    if let Some(raw) = error.rpc {
                        let stage = if raw["code"] == -32002 {
                            RejectionStage::Preflight
                        } else {
                            RejectionStage::Unknown
                        };
                        capture
                            .emit(
                                Event::RpcRejected { stage, error: raw },
                                Some("attempt-1"),
                                &config.route_id,
                            )
                            .await?;
                    } else {
                        capture
                            .emit(
                                Event::TransportFailed {
                                    kind: error.transport.unwrap_or(TransportFailureKind::Read),
                                },
                                Some("attempt-1"),
                                &config.route_id,
                            )
                            .await?;
                    }
                }
            }
            capture.diagnostics(&rpc, &config.route_id).await
        };
        let observe = observe(
            &capture,
            &rpc,
            config,
            Some(prepared.last_valid_block_height),
        );
        tokio::try_join!(submit, observe)?;
        Ok::<(), String>(())
    };
    let (reason, mut flags) = tokio::select! {
        result=result=>match result{Ok(())=>("observation_finished",vec![]),Err(e)=>("run_error",vec![e])},
        _=tokio::signal::ctrl_c()=>("interrupted",vec!["User interrupted; submission may have been delivered".into()]),
    };
    if capture.recorder.saturated() {
        flags.push("event queue saturated; latency precision limited".into());
    }
    let terminal = capture
        .emit(
            Event::RunFinished {
                reason: reason.into(),
                incomplete_data: flags.clone(),
            },
            None,
            &config.route_id,
        )
        .await;
    drop(capture);
    let flushed = writer.finish().await;
    terminal?;
    flushed?;
    if reason == "run_error" {
        return Err(format!(
            "Run saved at {}: {}",
            dir.display(),
            flags.join("; ")
        ));
    }
    Ok(dir)
}
async fn record_status(
    c: &Capture,
    rpc: &Rpc,
    config: &Config,
    history: bool,
) -> Result<bool, String> {
    let signature = c.signature.as_deref().ok_or("Missing signature")?;
    match rpc.status(signature, history).await {
        Ok(Some((slot, commitment, execution_error))) => {
            let finalized = commitment == Commitment::Finalized;
            c.emit(
                Event::SignatureObserved {
                    slot,
                    commitment,
                    execution_error,
                },
                None,
                &config.observer_id,
            )
            .await?;
            c.diagnostics(rpc, &config.observer_id).await?;
            Ok(finalized)
        }
        Ok(None) => {
            c.diagnostics(rpc, &config.observer_id).await?;
            Ok(false)
        }
        Err(e) => {
            c.emit(
                Event::ObserverUnavailable {
                    cause: e.category,
                    retry_after_ms: Some(config.observation.poll_interval_ms),
                },
                None,
                &config.observer_id,
            )
            .await?;
            c.diagnostics(rpc, &config.observer_id).await?;
            Ok(false)
        }
    }
}
async fn enrich(c: &Capture, rpc: &Rpc, config: &Config) -> Result<(), String> {
    let signature = c.signature.as_deref().ok_or("Missing signature")?;
    match rpc.transaction(signature).await {
        Ok(v) if !v.is_null() => {
            if let (Some(slot), Some(fee), Some(err)) = (
                v["slot"].as_u64(),
                v["meta"]["fee"].as_u64(),
                v["meta"].get("err"),
            ) {
                if v["transaction"]["signatures"][0].as_str() != Some(signature) {
                    return c
                        .emit(
                            Event::ObserverUnavailable {
                                cause: "transaction_details_signature_mismatch".into(),
                                retry_after_ms: None,
                            },
                            None,
                            &config.observer_id,
                        )
                        .await;
                }
                let error = if err.is_null() {
                    None
                } else {
                    Some(crate::rpc::sanitize(err))
                };
                // getTransaction was explicitly requested at finalized commitment.
                c.emit(
                    Event::SignatureObserved {
                        slot,
                        commitment: Commitment::Finalized,
                        execution_error: error.clone(),
                    },
                    None,
                    &config.observer_id,
                )
                .await?;
                c.emit(
                    Event::TransactionDetailsObserved {
                        slot,
                        blockhash: v["transaction"]["message"]["recentBlockhash"]
                            .as_str()
                            .map(str::to_owned),
                        fee,
                        execution_error: error,
                        compute_units: v["meta"]["computeUnitsConsumed"].as_u64(),
                    },
                    None,
                    &config.observer_id,
                )
                .await?;
            } else {
                c.emit(
                    Event::ObserverUnavailable {
                        cause: "invalid_transaction_details".into(),
                        retry_after_ms: None,
                    },
                    None,
                    &config.observer_id,
                )
                .await?;
            }
        }
        Ok(_) => {
            c.emit(
                Event::ObserverUnavailable {
                    cause: "transaction_history_unavailable".into(),
                    retry_after_ms: None,
                },
                None,
                &config.observer_id,
            )
            .await?
        }
        Err(e) => {
            c.emit(
                Event::ObserverUnavailable {
                    cause: format!("transaction_details_{}", e.category),
                    retry_after_ms: None,
                },
                None,
                &config.observer_id,
            )
            .await?
        }
    }
    c.diagnostics(rpc, &config.observer_id).await
}
async fn observe(
    c: &Capture,
    rpc: &Rpc,
    config: &Config,
    boundary: Option<u64>,
) -> Result<(), String> {
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(config.observation.deadline_ms);
    let mut height_due = tokio::time::Instant::now();
    let mut finalized = false;
    let polling = async {
        loop {
            if record_status(c, rpc, config, false).await? {
                finalized = true;
                break;
            }
            if tokio::time::Instant::now() >= height_due {
                match rpc.height().await {
                    Ok(height) => {
                        c.emit(
                            Event::BlockHeightObserved {
                                height,
                                commitment: Commitment::Finalized,
                            },
                            None,
                            &config.observer_id,
                        )
                        .await?;
                        if boundary.is_some_and(|b| height > b) {
                            c.emit(
                                Event::ValidityWindowPassed {
                                    height,
                                    commitment: Commitment::Finalized,
                                },
                                None,
                                &config.observer_id,
                            )
                            .await?;
                            break;
                        }
                    }
                    Err(e) => {
                        c.emit(
                            Event::ObserverUnavailable {
                                cause: e.category,
                                retry_after_ms: Some(config.observation.height_poll_interval_ms),
                            },
                            None,
                            &config.observer_id,
                        )
                        .await?
                    }
                }
                c.diagnostics(rpc, &config.observer_id).await?;
                height_due = tokio::time::Instant::now()
                    + Duration::from_millis(config.observation.height_poll_interval_ms);
            }
            tokio::time::sleep(Duration::from_millis(config.observation.poll_interval_ms)).await;
        }
        Ok::<(), String>(())
    };
    match tokio::time::timeout_at(deadline, polling).await {
        Ok(result) => result?,
        Err(_) => {
            c.emit(
                Event::ObservationDeadlineReached {
                    deadline_ms: config.observation.deadline_ms,
                },
                None,
                &config.observer_id,
            )
            .await?
        }
    }
    // Bounded final lookups preserve uncertainty when history is missing.
    let reconciliation = async {
        if !finalized {
            record_status(c, rpc, config, true).await?;
        }
        enrich(c, rpc, config).await
    };
    match tokio::time::timeout(Duration::from_secs(20), reconciliation).await {
        Ok(result) => result?,
        Err(_) => {
            c.emit(
                Event::ObserverUnavailable {
                    cause: "history_lookup_budget_exceeded".into(),
                    retry_after_ms: None,
                },
                None,
                &config.observer_id,
            )
            .await?
        }
    }

    Ok(())
}
pub async fn reconcile(config: &Config, path: &Path) -> Result<PathBuf, String> {
    config.validate()?;
    let input = store::read(path)?;
    if !input.warnings.is_empty() {
        return Err("Repair truncated log before reconciliation".into());
    }
    let states = crate::reducer::replay(&input.events)?;
    let rpc = Rpc::new(config)?;
    let network = bounded(30_000, rpc.verify_network(config)).await?;
    let known = input
        .events
        .iter()
        .find_map(|e| {
            if let Event::RunStarted {
                network_identity, ..
            } = &e.event
            {
                Some(network_identity)
            } else {
                None
            }
        })
        .ok_or("Run lacks network identity; refusing live reconciliation")?;
    if *known != network {
        return Err("Reconciliation network differs from recorded run".into());
    }
    let run = input.events.first().ok_or("Empty run")?.run_id.clone();
    let file = store::event_path(path);
    let writer = Writer::open(&file, true, 64)?;
    let base = Capture {
        recorder: writer.recorder.clone(),
        run,
        session: unique(),
        transaction: "run".into(),
        signature: None,
        start: Instant::now(),
        ids: Arc::new(AtomicU64::new(1)),
    };
    let work = async {
        metadata(&base, config, network).await?;
        for state in states.values() {
            if matches!(
                state.execution,
                ExecutionState::FinalizedSuccess | ExecutionState::FinalizedError
            ) {
                continue;
            }
            let signature = state
                .signature
                .clone()
                .or_else(|| state.evidence.iter().find_map(|e| e.signature.clone()))
                .ok_or("Missing signature in unresolved transaction")?;
            let c = Capture {
                transaction: state.transaction_id.clone(),
                signature: Some(signature),
                ..base.clone()
            };
            observe(&c, &rpc, config, state.last_valid_block_height).await?;
        }
        Ok::<(), String>(())
    };
    let result = tokio::select! {r=work=>r,_=tokio::signal::ctrl_c()=>Err("Reconciliation interrupted".into())};
    let mut flags = result
        .as_ref()
        .err()
        .cloned()
        .into_iter()
        .collect::<Vec<_>>();
    if base.recorder.saturated() {
        flags.push("event queue saturated".into());
    }
    let terminal = base
        .emit(
            Event::RunFinished {
                reason: "reconciliation_finished".into(),
                incomplete_data: flags,
            },
            None,
            &config.observer_id,
        )
        .await;
    drop(base);
    let flushed = writer.finish().await;
    terminal?;
    flushed?;
    result?;
    Ok(file)
}
