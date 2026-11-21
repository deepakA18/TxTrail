use crate::{model::*, reducer::replay, store::ReadResult};
use serde::Serialize;
#[derive(Debug, Serialize)]
pub struct Row {
    pub transaction_id: String,
    pub signature: Option<String>,
    pub outcome: Outcome,
    pub attempts: usize,
    pub acknowledgement_ms: Vec<f64>,
    pub first_execution_ms: Option<f64>,
    pub finalized_ms: Option<f64>,
    pub explanation: String,
    pub timeline: Vec<EventEnvelope>,
}
#[derive(Debug, Serialize)]
pub struct Quantiles {
    pub samples: usize,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub run_id: Option<String>,
    pub submitted: usize,
    pub finalized_success: usize,
    pub finalized_error: usize,
    pub rejected: usize,
    pub unresolved: usize,
    pub success_fraction: Option<f64>,
    pub unresolved_fraction: Option<f64>,
    pub finalized_latency: Quantiles,
    pub capture_complete: bool,
    pub warnings: Vec<String>,
    pub metadata: Vec<EventEnvelope>,
    pub transactions: Vec<Row>,
}
pub fn quantiles(mut values: Vec<f64>) -> Quantiles {
    values.sort_by(f64::total_cmp);
    let q = |p: f64| {
        if values.is_empty() {
            None
        } else {
            Some(values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1)])
        }
    };
    Quantiles {
        samples: values.len(),
        p50_ms: q(0.5),
        p95_ms: q(0.95),
        p99_ms: q(0.99),
    }
}
fn duration(start: &EventEnvelope, end: &EventEnvelope) -> Option<f64> {
    if start.session_id != end.session_id {
        return None;
    }
    end.elapsed_ns
        .checked_sub(start.elapsed_ns)
        .map(|n| n as f64 / 1_000_000.0)
}
pub fn build(input: &ReadResult) -> Result<Report, String> {
    let states = replay(&input.events)?;
    let metadata: Vec<_> = input
        .events
        .iter()
        .filter(|e| {
            matches!(
                e.event,
                Event::RunStarted { .. } | Event::RunFinished { .. }
            )
        })
        .cloned()
        .collect();
    let mut warnings = input.warnings.clone();
    let mut starts = std::collections::BTreeSet::new();
    let mut ends = std::collections::BTreeSet::new();
    for e in &metadata {
        match &e.event {
            Event::RunStarted { .. } => {
                starts.insert(&e.session_id);
            }
            Event::RunFinished {
                incomplete_data, ..
            } => {
                ends.insert(&e.session_id);
                warnings.extend(incomplete_data.clone());
            }
            _ => {}
        }
    }
    let capture_complete = !starts.is_empty() && starts.is_subset(&ends) && warnings.is_empty();
    if !capture_complete {
        warnings.push("Capture incomplete or no clean session finish recorded.".into());
    }
    if input
        .events
        .iter()
        .any(|e| matches!(e.event, Event::ObserverUnavailable { .. }))
    {
        warnings.push("Observer outages limit first-seen measurements.".into());
    }
    let saturated = warnings.iter().any(|w| w.contains("saturat"));
    let mut transactions = vec![];
    for state in states.values() {
        let evidence = &state.evidence;
        let start = evidence
            .iter()
            .find(|e| matches!(e.event, Event::SendStarted { .. }));
        let latency = |end: Option<&EventEnvelope>| {
            if saturated {
                None
            } else {
                start.zip(end).and_then(|(s, e)| duration(s, e))
            }
        };
        let execution = evidence
            .iter()
            .find(|e| matches!(e.event, Event::SignatureObserved { .. }));
        let finalized = evidence.iter().find(|e| {
            matches!(
                e.event,
                Event::SignatureObserved {
                    commitment: Commitment::Finalized,
                    ..
                }
            )
        });
        let acknowledgement_ms = if saturated {
            vec![]
        } else {
            evidence
                .iter()
                .filter(|e| matches!(e.event, Event::RpcAcknowledged { .. }))
                .filter_map(|ack| {
                    evidence
                        .iter()
                        .find(|e| {
                            matches!(e.event, Event::SendStarted { .. })
                                && e.attempt_id == ack.attempt_id
                        })
                        .and_then(|s| duration(s, ack))
                })
                .collect()
        };
        let outcome = state.outcome();
        let explanation=match outcome {
            Outcome::FinalizedSuccess=>"Finalized signature evidence establishes successful execution; the delivering route remains unknown.",
            Outcome::FinalizedExecutionError=>"The transaction landed in a finalized bank but failed execution; this is not a preflight error.",
            Outcome::SubmissionRejectedNoChainEvidence=>"Recorded attempts were rejected; no on-chain execution evidence was observed.",
            Outcome::UnresolvedAtDeadline=>"The observation deadline ended without finalization. This does not prove expiry or a dropped transaction.",
            Outcome::UnobservedAfterValidityWindow=>"Finalized height passed the validity boundary without observed execution. Missing history leaves delivery unresolved.",
            Outcome::ConflictingEvidence=>"Finalized observations disagree. Investigation is required; no result silently wins.",
            Outcome::Pending=>"Available evidence does not establish a final outcome.",
        }.into();
        transactions.push(Row {
            transaction_id: state.transaction_id.clone(),
            signature: state
                .signature
                .clone()
                .or_else(|| evidence.iter().find_map(|e| e.signature.clone())),
            outcome,
            attempts: state.attempts.len(),
            acknowledgement_ms,
            first_execution_ms: latency(execution),
            finalized_ms: latency(finalized),
            explanation,
            timeline: evidence.clone(),
        });
    }
    let submitted = transactions.iter().filter(|t| t.attempts > 0).count();
    let count = |outcome| {
        transactions
            .iter()
            .filter(|t| t.attempts > 0 && t.outcome == outcome)
            .count()
    };
    let finalized_success = count(Outcome::FinalizedSuccess);
    let finalized_error = count(Outcome::FinalizedExecutionError);
    let rejected = count(Outcome::SubmissionRejectedNoChainEvidence);
    let unresolved = submitted - finalized_success - finalized_error - rejected;
    let fraction = |n| {
        if submitted == 0 {
            None
        } else {
            Some(n as f64 / submitted as f64)
        }
    };
    let finalized_latency = quantiles(transactions.iter().filter_map(|t| t.finalized_ms).collect());
    warnings.push("Timing is client observation latency, including polling and request scheduling, not true execution time. Submission and observer may share an endpoint.".into());
    if finalized_latency.samples < 100 {
        warnings.push("Small sample: p95/p99 do not support tail-latency conclusions.".into());
    }
    Ok(Report {
        run_id: input.events.first().map(|e| e.run_id.clone()),
        submitted,
        finalized_success,
        finalized_error,
        rejected,
        unresolved,
        success_fraction: fraction(finalized_success),
        unresolved_fraction: fraction(unresolved),
        finalized_latency,
        capture_complete,
        warnings,
        metadata,
        transactions,
    })
}
fn safe(s: &str) -> String {
    s.replace('|', "\\|").replace(['\r', '\n'], " ")
}
fn label(o: &Outcome) -> String {
    serde_json::to_value(o).unwrap().as_str().unwrap().into()
}
fn ms(v: Option<f64>) -> String {
    v.map(|n| format!("{n:.3} ms"))
        .unwrap_or_else(|| "unknown".into())
}
pub fn markdown(r: &Report, signature: Option<&str>) -> Result<String, String> {
    let mut out = format!(
        "# TxTrail report\n\nRun: `{}`\n\n{} unique transactions; {} submitted; {} finalized successes; {} finalized execution errors; {} rejected; {} unresolved.\n\nCapture complete: {}.\n",
        safe(r.run_id.as_deref().unwrap_or("unknown")),
        r.transactions.len(),
        r.submitted,
        r.finalized_success,
        r.finalized_error,
        r.rejected,
        r.unresolved,
        r.capture_complete
    );
    for w in &r.warnings {
        out.push_str(&format!("\n- {}", safe(w)));
    }
    out.push_str("\n\n## Run metadata\n\n");
    for e in &r.metadata {
        out.push_str(&format!(
            "- {}\n",
            safe(&serde_json::to_string(&e.event).unwrap())
        ));
    }
    let selected: Vec<_> = r
        .transactions
        .iter()
        .filter(|t| signature.is_none_or(|s| t.signature.as_deref() == Some(s)))
        .collect();
    if signature.is_some() && selected.is_empty() {
        return Err("Signature not found".into());
    }
    for t in selected {
        out.push_str(&format!("\n## {}\n\nSignature: `{}`\n\n**{}** — {}\n\nAttempts: {}. First execution observation: {}; finalized observation: {}. Ack samples (ms): {:?}.\n\n| Seq | Session | Elapsed ns | Source | Evidence |\n|---|---|---:|---|---|\n",safe(&t.transaction_id),safe(t.signature.as_deref().unwrap_or("unknown")),label(&t.outcome),t.explanation,t.attempts,ms(t.first_execution_ms),ms(t.finalized_ms),t.acknowledgement_ms));
        for e in &t.timeline {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                e.sequence,
                safe(&e.session_id),
                e.elapsed_ns,
                safe(&e.source_id),
                safe(&serde_json::to_string(&e.event).unwrap())
            ));
        }
    }
    let q = &r.finalized_latency;
    out.push_str(&format!(
        "\nFinalized observation latency: {} samples; nearest-rank p50 {}; p95 {}; p99 {}.\n",
        q.samples,
        ms(q.p50_ms),
        ms(q.p95_ms),
        ms(q.p99_ms)
    ));
    Ok(out)
}
fn cell(s: &str) -> String {
    let s = if s.starts_with(['=', '+', '-', '@', '\t', '\r']) {
        format!("'{s}")
    } else {
        s.into()
    };
    format!("\"{}\"", s.replace('"', "\"\""))
}
pub fn csv(r: &Report) -> String {
    let mut out =
        "transaction_id,signature,outcome,attempts,first_execution_ms,finalized_ms\n".to_owned();
    for t in &r.transactions {
        out.push_str(&format!(
            "{},{},{},{},{},{}\n",
            cell(&t.transaction_id),
            cell(t.signature.as_deref().unwrap_or("")),
            label(&t.outcome),
            t.attempts,
            t.first_execution_ms
                .map(|v| v.to_string())
                .unwrap_or_default(),
            t.finalized_ms.map(|v| v.to_string()).unwrap_or_default()
        ));
    }
    out
}
