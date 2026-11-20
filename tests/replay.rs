use txtrail::model::*;
use txtrail::reducer::{apply_event, replay};

fn events(text: &str) -> Vec<EventEnvelope> {
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn success() -> Vec<EventEnvelope> {
    events(include_str!("fixtures/success.jsonl"))
}

#[test]
fn deterministic_fixtures_have_expected_outcomes() {
    let cases = [
        (
            include_str!("fixtures/success.jsonl"),
            Outcome::FinalizedSuccess,
        ),
        (
            include_str!("fixtures/execution-error.jsonl"),
            Outcome::FinalizedExecutionError,
        ),
        (
            include_str!("fixtures/preflight-rejection.jsonl"),
            Outcome::SubmissionRejectedNoChainEvidence,
        ),
        (
            include_str!("fixtures/ack-timeout-then-finalized.jsonl"),
            Outcome::FinalizedSuccess,
        ),
        (
            include_str!("fixtures/unresolved-deadline.jsonl"),
            Outcome::UnresolvedAtDeadline,
        ),
        (
            include_str!("fixtures/validity-passed.jsonl"),
            Outcome::UnobservedAfterValidityWindow,
        ),
        (
            include_str!("fixtures/conflicting-finalized.jsonl"),
            Outcome::ConflictingEvidence,
        ),
        (
            include_str!("fixtures/tentative-then-finalized.jsonl"),
            Outcome::FinalizedSuccess,
        ),
    ];
    for (text, expected) in cases {
        let input = events(text);
        let first = replay(&input).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first["fixture-tx"].outcome(), expected);
        assert_eq!(first["fixture-tx"].evidence, input);
        let mut shuffled = input.clone();
        shuffled.reverse();
        assert_eq!(first, replay(&shuffled).unwrap());
        for event in input {
            let encoded = serde_json::to_string(&event).unwrap();
            assert_eq!(
                event,
                serde_json::from_str::<EventEnvelope>(&encoded).unwrap()
            );
        }
    }
}

#[test]
fn repeated_observation_preserves_first_seen_and_unique_transaction_count() {
    let mut input = success();
    let original = replay(&input).unwrap();
    let mut repeated = input.last().unwrap().clone();
    repeated.event_id = "new-observation".into();
    repeated.sequence += 1;
    repeated.elapsed_ns += 100;
    repeated.source_id = "second-observer".into();
    input.push(repeated.clone());
    input.push(repeated);
    let result = replay(&input).unwrap();
    assert_eq!(result.len(), 1);
    let state = &result["fixture-tx"];
    assert_eq!(state.evidence.len(), 5);
    assert_eq!(
        state.first_execution_observation,
        original["fixture-tx"].first_execution_observation
    );
    assert_eq!(
        state.first_finalized_observation,
        original["fixture-tx"].first_finalized_observation
    );
}

#[test]
fn restart_reconciliation_does_not_invent_cross_session_latency() {
    let mut input = events(include_str!("fixtures/unresolved-deadline.jsonl"));
    let mut finalized = success().pop().unwrap();
    finalized.event_id = "reconciled".into();
    finalized.sequence = 6;
    finalized.session_id = "restart-session".into();
    finalized.elapsed_ns = 10;
    input.push(finalized);
    let result = replay(&input).unwrap();
    let state = &result["fixture-tx"];
    assert_eq!(state.outcome(), Outcome::FinalizedSuccess);
    let start = ObservationTime {
        session_id: "fixture-session".into(),
        elapsed_ns: 2_000_000,
    };
    assert_eq!(
        state
            .first_finalized_observation
            .as_ref()
            .unwrap()
            .duration_since(&start),
        None
    );
    assert_eq!(
        start.duration_since(&ObservationTime {
            session_id: start.session_id.clone(),
            elapsed_ns: 1_000_000
        }),
        Some(1_000_000)
    );
}

#[test]
fn replay_rejects_duplicate_identity_corruption_and_mixed_runs() {
    let input = success();
    let mut reused_id = input.clone();
    let mut changed = input[0].clone();
    changed.elapsed_ns += 1;
    reused_id.push(changed);
    assert!(replay(&reused_id).is_err());
    let mut reused_sequence = input.clone();
    reused_sequence[1].sequence = 1;
    assert!(replay(&reused_sequence).is_err());
    let mut mixed = input.clone();
    mixed[1].run_id = "another-run".into();
    assert!(replay(&mixed).is_err());
    let mut unknown_schema = input;
    unknown_schema[0].schema_version = 2;
    assert!(replay(&unknown_schema).is_err());
}

#[test]
fn partial_logs_cannot_mix_signatures() {
    let mut input = success();
    let mut state = TransactionState::new("fixture-tx".into());
    apply_event(&mut state, &input[3]).unwrap();
    let before = state.clone();
    input[3].event_id = "different-signature".into();
    input[3].signature = Some("another-signature".into());
    assert!(apply_event(&mut state, &input[3]).is_err());
    assert_eq!(state, before);
}

#[test]
fn transport_failure_cannot_erase_acknowledgement() {
    let mut input = success();
    let mut failure = input[2].clone();
    failure.event_id = "late-timeout".into();
    failure.sequence = 5;
    failure.event = Event::TransportFailed {
        kind: TransportFailureKind::Timeout,
    };
    input.push(failure);
    let states = replay(&input).unwrap();
    assert_eq!(
        states["fixture-tx"].attempts["attempt-1"],
        SubmissionState::Acknowledged
    );
}

#[test]
fn receipt_and_details_are_retained_without_inventing_commitment() {
    let mut state = TransactionState::new("fixture-tx".into());
    let mut event = success()[0].clone();
    event.event = Event::ReceivedNotification;
    apply_event(&mut state, &event).unwrap();
    event.event_id = "details".into();
    event.event = Event::TransactionDetailsObserved {
        slot: 42,
        blockhash: None,
        fee: 5000,
        execution_error: None,
        compute_units: None,
    };
    apply_event(&mut state, &event).unwrap();
    assert_eq!(state.execution, ExecutionState::Unobserved);
    assert_eq!(state.evidence.len(), 2);
}

#[test]
fn explicit_validity_passage_requires_finalized_height_evidence() {
    let prepared = success()[0].clone();
    let mut state = TransactionState::new(prepared.transaction_id.clone());
    apply_event(&mut state, &prepared).unwrap();
    for (height, commitment) in [(100, Commitment::Finalized), (101, Commitment::Confirmed)] {
        let mut event = prepared.clone();
        event.event_id = "invalid-expiry".into();
        event.event = Event::ValidityWindowPassed { height, commitment };
        let before = state.clone();
        assert!(apply_event(&mut state, &event).is_err());
        assert_eq!(state, before);
    }
}

#[test]
fn finalization_before_ack_is_valid() {
    let mut input = success();
    input[2].sequence = 5;
    input[2].elapsed_ns = 5_000_000;
    let state = replay(&input).unwrap().remove("fixture-tx").unwrap();
    assert_eq!(state.outcome(), Outcome::FinalizedSuccess);
    assert_eq!(state.attempts["attempt-1"], SubmissionState::Acknowledged);
}

#[test]
fn same_signature_cannot_inflate_transaction_count() {
    let mut input = success();
    let mut duplicate_transaction = input[3].clone();
    duplicate_transaction.event_id = "another-logical-id".into();
    duplicate_transaction.transaction_id = "other-tx".into();
    duplicate_transaction.sequence = 5;
    input.push(duplicate_transaction);
    assert!(replay(&input).is_err());
}

#[test]
fn observer_outage_is_retained_through_recovery() {
    let mut input = success();
    let mut outage = input[1].clone();
    outage.event_id = "outage".into();
    outage.sequence = 3;
    outage.event = Event::ObserverUnavailable {
        cause: "synthetic disconnect".into(),
        retry_after_ms: Some(1000),
    };
    input[2].sequence = 4;
    input[3].sequence = 5;
    input.push(outage);
    let state = replay(&input).unwrap().remove("fixture-tx").unwrap();
    assert_eq!(state.observation, ObservationState::Complete);
    assert!(
        state
            .evidence
            .iter()
            .any(|e| matches!(e.event, Event::ObserverUnavailable { .. }))
    );
}

#[test]
fn finalized_error_details_must_agree() {
    let mut input = events(include_str!("fixtures/execution-error.jsonl"));
    let mut other = input.last().unwrap().clone();
    other.event_id = "different-error".into();
    other.sequence += 1;
    if let Event::SignatureObserved {
        execution_error, ..
    } = &mut other.event
    {
        *execution_error =
            Some(serde_json::json!({"InstructionError": [0, "InvalidInstructionData"]}));
    }
    input.push(other);
    assert_eq!(
        replay(&input).unwrap()["fixture-tx"].outcome(),
        Outcome::ConflictingEvidence
    );
}

#[test]
fn conflicting_terminal_attempt_response_is_atomic() {
    let input = success();
    let mut state = replay(&input).unwrap().remove("fixture-tx").unwrap();
    let before = state.clone();
    let mut rejection = input[2].clone();
    rejection.event_id = "contradictory-response".into();
    rejection.event = Event::RpcRejected {
        stage: RejectionStage::Unknown,
        error: serde_json::json!({"code": -1}),
    };
    assert!(apply_event(&mut state, &rejection).is_err());
    assert_eq!(state, before);
}
