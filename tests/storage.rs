use tempfile::tempdir;
use txtrail::{model::*, store::*};
const FIXTURE: &str = include_str!("fixtures/success.jsonl");
#[test]
fn final_line_recovery_and_middle_corruption() {
    let text = format!("{FIXTURE}{{\"schema_version\":");
    let recovered = parse_jsonl(&text).unwrap();
    assert_eq!(recovered.events.len(), 4);
    assert_eq!(recovered.warnings.len(), 1);
    assert!(
        parse_jsonl(&format!("{{\"schema_version\":\n{FIXTURE}"))
            .unwrap_err()
            .contains("Line 1")
    );
    assert!(parse_jsonl(&format!("{FIXTURE}not-json")).is_err());
    assert_eq!(parse_jsonl(FIXTURE.trim_end()).unwrap().events.len(), 4);
    assert!(parse_jsonl(&FIXTURE.replace("\"schema_version\":1", "\"schema_version\":2")).is_err());
}
#[test]
fn utf8_truncation_recovers_only_final_record() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    let mut bytes = FIXTURE.as_bytes().to_vec();
    bytes.extend_from_slice(b"{\"text\":\"");
    bytes.push(0xe2);
    std::fs::write(&path, bytes).unwrap();
    let r = read(&path).unwrap();
    assert_eq!(r.events.len(), 4);
    assert_eq!(r.warnings.len(), 1);
}
#[tokio::test]
async fn writer_flushes_orders_locks_and_resumes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    let writer = Writer::open(&path, false, 1).unwrap();
    assert!(Writer::open(&path, true, 1).is_err());
    let input = parse_jsonl(FIXTURE).unwrap();
    for e in input.events {
        writer.recorder.record(e).await.unwrap();
    }
    writer.finish().await.unwrap();
    assert_eq!(read(&path).unwrap().events.len(), 4);
    let writer = Writer::open(&path, true, 1).unwrap();
    let mut e = example_finalized_success_event();
    e.run_id = "fixture-run".into();
    e.transaction_id = "fixture-tx".into();
    e.signature = Some("fixture-signature".into());
    assert_eq!(writer.recorder.record(e).await.unwrap().sequence, 5);
    writer.finish().await.unwrap();
    assert_eq!(read(&path).unwrap().events.len(), 5);
}
#[tokio::test]
async fn append_refuses_truncated_logs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, format!("{FIXTURE}{{")).unwrap();
    assert!(Writer::open(&path, true, 1).is_err());
}
#[test]
fn report_metrics_have_hand_calculated_values() {
    let r = txtrail::report::build(&parse_jsonl(FIXTURE).unwrap()).unwrap();
    assert_eq!(r.submitted, 1);
    assert_eq!(r.success_fraction, Some(1.0));
    assert_eq!(r.transactions[0].acknowledgement_ms, vec![1.0]);
    assert_eq!(r.transactions[0].finalized_ms, Some(2.0));
    let q = txtrail::report::quantiles(vec![1.0, 2.0, 3.0, 4.0]);
    assert_eq!(q.p50_ms, Some(2.0));
    assert_eq!(q.p95_ms, Some(4.0));
    let r = txtrail::report::build(
        &parse_jsonl(include_str!("fixtures/unresolved-deadline.jsonl")).unwrap(),
    )
    .unwrap();
    assert_eq!(r.unresolved_fraction, Some(1.0));
    assert_eq!(r.finalized_latency.samples, 0);
}

#[test]
fn mixed_outcomes_keep_unknowns_in_denominator_and_deduplicate_ack_samples() {
    let mut input = parse_jsonl(FIXTURE).unwrap();
    let mut ack = input.events[2].clone();
    ack.event_id = "second-ack-same-attempt".into();
    ack.sequence = 5;
    ack.elapsed_ns += 1000000;
    input.events.push(ack);
    let mut unresolved = parse_jsonl(include_str!("fixtures/unresolved-deadline.jsonl")).unwrap();
    for (i, e) in unresolved.events.iter_mut().enumerate() {
        e.run_id = "fixture-run".into();
        e.transaction_id = "second-transaction".into();
        e.signature = Some("second-signature".into());
        e.event_id = format!("second-{i}");
        e.sequence = (6 + i) as u64;
        if let Event::TransactionPrepared { signature, .. } = &mut e.event {
            *signature = "second-signature".into();
        }
        if let Event::RpcAcknowledged { returned_signature } = &mut e.event {
            *returned_signature = "second-signature".into();
        }
    }
    input.events.extend(unresolved.events);
    let r = txtrail::report::build(&input).unwrap();
    assert_eq!(r.submitted, 2);
    assert_eq!(r.success_fraction, Some(0.5));
    assert_eq!(r.unresolved_fraction, Some(0.5));
    assert_eq!(r.finalized_transactions, 1);
    assert_eq!(r.non_finalized_transactions, 1);
    assert_eq!(r.transactions[0].acknowledgement_ms, vec![1.0]);
    assert_eq!(r.finalized_latency.samples, 1);
    input.warnings.push("event queue saturated".into());
    let limited = txtrail::report::build(&input).unwrap();
    assert!(!limited.capture_complete);
    assert_eq!(limited.finalized_latency.samples, 0);
    assert!(
        limited
            .transactions
            .iter()
            .all(|t| t.acknowledgement_ms.is_empty())
    );
}
