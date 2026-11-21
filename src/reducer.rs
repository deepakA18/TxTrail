use crate::model::{
    Commitment, Event, EventEnvelope, ExecutionState, ObservationState, SubmissionState,
    TransactionState, ValidityState,
};

pub fn apply_event(state: &mut TransactionState, envelope: &EventEnvelope) -> Result<(), String> {
    if envelope.schema_version != 1 {
        return Err("Unsupported event schema version".into());
    }
    if envelope.event_id.trim().is_empty()
        || envelope.run_id.trim().is_empty()
        || envelope.session_id.trim().is_empty()
    {
        return Err("Event, run, and session IDs must be nonempty".into());
    }
    if let Some(first) = state.evidence.first()
        && first.run_id != envelope.run_id
    {
        return Err("Run ID mismatch".into());
    }
    if state.transaction_id != envelope.transaction_id {
        return Err(format!(
            "transaction_id mismatch: expected '{}', got '{}'",
            state.transaction_id, envelope.transaction_id
        ));
    }
    if let Some(previous) = state
        .evidence
        .iter()
        .find(|previous| previous.event_id == envelope.event_id)
    {
        if previous == envelope {
            return Ok(());
        }

        return Err("Event ID reused with different contents".into());
    }
    if let (Some(expected), Some(actual)) = (&state.signature, &envelope.signature)
        && expected != actual
    {
        return Err("Signature differs from prepared transaction".into());
    }
    if let Some(actual) = &envelope.signature
        && state
            .evidence
            .iter()
            .filter_map(|e| e.signature.as_ref())
            .any(|s| s != actual)
    {
        return Err("Signature conflicts with prior evidence".into());
    }
    // Conflicting terminal responses for one attempt must not silently overwrite each other.
    let terminal = match &envelope.event {
        Event::RpcAcknowledged { .. } => Some(SubmissionState::Acknowledged),
        Event::RpcRejected { .. } => Some(SubmissionState::Rejected),
        _ => None,
    };
    if let (Some(id), Some(incoming)) = (&envelope.attempt_id, terminal)
        && let Some(existing) = state.attempts.get(id)
        && matches!(
            existing,
            SubmissionState::Acknowledged | SubmissionState::Rejected
        )
        && *existing != incoming
    {
        return Err("Conflicting terminal submission responses".into());
    }
    match &envelope.event {
        Event::RunStarted { .. }
        | Event::RunFinished { .. }
        | Event::RpcDiagnostic { .. }
        | Event::ReceivedNotification
        | Event::TransactionDetailsObserved { .. } => {}
        Event::SendStarted { .. } => {
            let id = envelope
                .attempt_id
                .as_ref()
                .ok_or("SendStarted requires attempt_id")?;
            envelope
                .signature
                .as_ref()
                .ok_or("SendStarted requires signature")?;
            state
                .attempts
                .entry(id.clone())
                .or_insert(SubmissionState::Uncertain);
        }
        Event::ObserverUnavailable { .. } => {
            if state.observation != ObservationState::Complete
                && state.observation != ObservationState::DeadlineReached
            {
                state.observation = ObservationState::Interrupted;
            }
        }
        Event::ValidityWindowPassed { height, commitment } => {
            let boundary = state
                .last_valid_block_height
                .ok_or("Validity passage requires preparation")?;
            if *commitment != Commitment::Finalized || *height <= boundary {
                return Err("Insufficient evidence for validity passage".into());
            }
            state.validity = ValidityState::ValidityWindowPassed;
        }
        Event::TransportFailed { .. } => {
            let attempt_id = envelope
                .attempt_id
                .as_ref()
                .ok_or("TransportFailed event requires an attempt_id")?;

            state
                .attempts
                .entry(attempt_id.clone())
                .and_modify(|s| {
                    if *s == SubmissionState::NotStarted {
                        *s = SubmissionState::Uncertain;
                    }
                })
                .or_insert(SubmissionState::Uncertain);
        }
        Event::ObservationDeadlineReached { .. } => {
            if state.observation != ObservationState::Complete {
                state.observation = ObservationState::DeadlineReached;
            }
        }
        Event::SignatureObserved {
            slot,
            commitment,
            execution_error,
            ..
        } => {
            let incoming = classify_execution(commitment, execution_error);
            let already_finalized = matches!(
                state.execution,
                ExecutionState::FinalizedSuccess | ExecutionState::FinalizedError
            );

            envelope
                .signature
                .as_ref()
                .ok_or("SignatureObserved requires a signature")?;

            let conflicts_with_previous = commitment == &Commitment::Finalized
                && state.evidence.iter().any(|previous| match &previous.event {
                    Event::SignatureObserved {
                        slot: previous_slot,
                        commitment: Commitment::Finalized,
                        execution_error: previous_error,
                    } => previous_slot != slot || previous_error != execution_error,
                    _ => false,
                });

            if state.execution != ExecutionState::ConflictingEvidence {
                if conflicts_with_previous {
                    state.execution = ExecutionState::ConflictingEvidence;
                } else if !already_finalized {
                    state.execution = incoming;
                }
            }
            if commitment == &Commitment::Finalized {
                state.observation = ObservationState::Complete;
            }
        }
        Event::RpcAcknowledged { returned_signature } => {
            let attempt_id = envelope
                .attempt_id
                .as_ref()
                .ok_or("RpcAcknowledgement failed requires an attempt_id")?;

            let signature = envelope
                .signature
                .as_ref()
                .ok_or("RpcAcknowledgement failed requires a signature")?;

            if returned_signature != signature {
                return Err("Mismatch Signature!".into());
            }

            state
                .attempts
                .insert(attempt_id.clone(), SubmissionState::Acknowledged);
        }
        Event::RpcRejected { .. } => {
            let attempt_id = envelope
                .attempt_id
                .as_ref()
                .ok_or("RpcRejected failed requires an attempt_id")?;

            state
                .attempts
                .insert(attempt_id.clone(), SubmissionState::Rejected);
        }
        Event::TransactionPrepared {
            signature,
            last_valid_block_height,
            ..
        } => {
            if Some(signature) != envelope.signature.as_ref() {
                return Err("Mismatch signature!".into());
            }
            if let Some(existing) = &state.signature
                && existing != signature
            {
                return Err("Conflicting prepared signature".into());
            }

            if let Some(existing_height) = state.last_valid_block_height
                && existing_height != *last_valid_block_height
            {
                return Err("Prepared validity boundary mismatch!".into());
            }
            state.signature = Some(signature.clone());
            state.last_valid_block_height = Some(*last_valid_block_height);
        }
        Event::BlockHeightObserved { height, commitment } => {
            if commitment == &Commitment::Finalized
                && state.validity != ValidityState::ValidityWindowPassed
                && let Some(boundary) = state.last_valid_block_height
            {
                state.validity = if *height > boundary {
                    ValidityState::ValidityWindowPassed
                } else {
                    ValidityState::ValidAtLastCheck
                };
            }
        }
    }

    if let Event::SignatureObserved { commitment, .. } = &envelope.event {
        let time = crate::model::ObservationTime {
            session_id: envelope.session_id.clone(),
            elapsed_ns: envelope.elapsed_ns,
        };
        state
            .first_execution_observation
            .get_or_insert(time.clone());
        if *commitment == Commitment::Finalized {
            state.first_finalized_observation.get_or_insert(time);
        }
    }
    state.evidence.push(envelope.clone());

    Ok(())
}

fn classify_execution(
    commitment: &Commitment,
    execution_error: &Option<serde_json::Value>,
) -> ExecutionState {
    match (commitment, execution_error.is_some()) {
        (Commitment::Finalized, false) => ExecutionState::FinalizedSuccess,
        (Commitment::Finalized, true) => ExecutionState::FinalizedError,
        (Commitment::Processed | Commitment::Confirmed, false) => ExecutionState::TentativeSuccess,
        (Commitment::Processed | Commitment::Confirmed, true) => ExecutionState::TentativeError,
    }
}

/// Replay already-decoded events in writer sequence order without I/O or clocks.
/// Duplicate event IDs are validated by the same reducer as live evidence.
pub fn replay(
    events: &[EventEnvelope],
) -> Result<std::collections::BTreeMap<String, TransactionState>, String> {
    let mut ordered: Vec<_> = events.iter().collect();
    ordered.sort_by_key(|e| e.sequence);
    let mut ids = std::collections::BTreeMap::new();
    let mut sequences = std::collections::BTreeMap::new();
    let mut states = std::collections::BTreeMap::new();
    let mut signatures = std::collections::BTreeMap::new();
    let run = events.first().map(|e| &e.run_id);
    for event in ordered {
        if event.schema_version != 1
            || event.event_id.trim().is_empty()
            || event.run_id.trim().is_empty()
            || event.session_id.trim().is_empty()
        {
            return Err("Invalid event envelope".into());
        }
        if Some(&event.run_id) != run {
            return Err("Mixed runs in replay".into());
        }
        if let Some(old) = ids.insert(&event.event_id, event) {
            if old != event {
                return Err("Event ID reused with different contents".into());
            }
            continue;
        }
        if sequences.insert(event.sequence, &event.event_id).is_some() {
            return Err("Writer sequence reused by distinct events".into());
        }
        if matches!(
            event.event,
            Event::RunStarted { .. } | Event::RunFinished { .. }
        ) {
            if event.schema_version != 1 {
                return Err("Unsupported event schema version".into());
            }
            continue;
        }
        if let Some(signature) = &event.signature
            && let Some(transaction) = signatures.insert(signature, &event.transaction_id)
            && transaction != &event.transaction_id
        {
            return Err("One signature assigned to multiple transaction IDs".into());
        }
        let state = states
            .entry(event.transaction_id.clone())
            .or_insert_with(|| TransactionState::new(event.transaction_id.clone()));
        apply_event(state, event)?;
    }
    Ok(states)
}
#[cfg(test)]
mod tests {
    use std::{assert_eq, vec};

    use super::*;
    use crate::model::{
        ExecutionState, ObservationState, RejectionStage, ValidityState,
        example_finalized_success_event, example_timeout_event,
    };

    #[test]
    fn timeout_preserves_execution_uncertainty() {
        let event = example_timeout_event();
        let mut state = TransactionState::new(event.transaction_id.clone());

        apply_event(&mut state, &event).expect("timeout event should be accepted");

        let attempt_id = event.attempt_id.as_ref().unwrap();
        assert_eq!(
            state.attempts.get(attempt_id),
            Some(&SubmissionState::Uncertain)
        );

        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.validity, ValidityState::Unknown);
        assert_eq!(state.observation, ObservationState::Active);
        assert_eq!(state.evidence, vec![event]);
    }

    #[test]
    fn wrong_transaction() {
        let event = example_timeout_event();
        let mut state = TransactionState::new("0asfbedjfds".into());
        let before = state.clone();
        assert!(apply_event(&mut state, &event).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn missing_attempt() {
        let mut event = example_timeout_event();
        event.attempt_id = None;

        let mut state = TransactionState::new(event.transaction_id.clone());
        let before = state.clone();

        let result = apply_event(&mut state, &event);

        assert_eq!(
            result,
            Err("TransportFailed event requires an attempt_id".into())
        );
        assert_eq!(state, before);
    }

    #[test]
    fn deadline_does_not_imply_expiry() {
        let mut event = example_timeout_event();
        event.attempt_id = None;
        event.event = Event::ObservationDeadlineReached {
            deadline_ms: 120_000,
        };

        let mut state = TransactionState::new(event.transaction_id.clone());
        apply_event(&mut state, &event).expect("deadline event should be accepted");
        assert_eq!(state.observation, ObservationState::DeadlineReached);
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.validity, ValidityState::Unknown);
        assert!(state.attempts.is_empty());
        assert_eq!(state.evidence, vec![event]);
    }

    #[test]
    fn duplicate_timeout_does_not_change_state() {
        let event = example_timeout_event();
        let mut state = TransactionState::new(event.transaction_id.clone());
        apply_event(&mut state, &event).expect("timeout event should be accepted");
        let before = state.clone();
        apply_event(&mut state, &event).expect("timeout event should be accepted");
        assert_eq!(state, before);
    }

    #[test]
    fn finalized_observation_classifies_success_and_error() {
        assert_eq!(
            classify_execution(&Commitment::Finalized, &None),
            ExecutionState::FinalizedSuccess
        );

        let error = Some(serde_json::json!({
            "InstructionError": [0, "InvalidArgument"]
        }));

        assert_eq!(
            classify_execution(&Commitment::Finalized, &error),
            ExecutionState::FinalizedError
        );
    }

    #[test]
    fn processed_observation_classifies_success_and_error() {
        assert_eq!(
            classify_execution(&Commitment::Processed, &None),
            ExecutionState::TentativeSuccess
        );

        let error = Some(serde_json::json!({
            "InstructionError": [0, "InvalidArgument"]
        }));

        assert_eq!(
            classify_execution(&Commitment::Processed, &error),
            ExecutionState::TentativeError
        );
    }

    #[test]
    fn confirmed_observation_classifies_success_and_error() {
        assert_eq!(
            classify_execution(&Commitment::Confirmed, &None),
            ExecutionState::TentativeSuccess
        );

        let error = Some(serde_json::json!({
            "InstructionError": [0, "InvalidArgument"]
        }));

        assert_eq!(
            classify_execution(&Commitment::Confirmed, &error),
            ExecutionState::TentativeError
        );
    }

    #[test]
    fn timeout_then_finalized_success() {
        let timeout = example_timeout_event();
        let finalized = example_finalized_success_event();
        let mut state: TransactionState = TransactionState::new(timeout.transaction_id.clone());

        apply_event(&mut state, &timeout).expect("timeout should be accepted");
        apply_event(&mut state, &finalized).expect("finalization should be accepted");

        let attempt_id = timeout.attempt_id.as_ref().unwrap();

        assert_eq!(
            state.attempts.get(attempt_id),
            Some(&SubmissionState::Uncertain)
        );
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.execution, ExecutionState::FinalizedSuccess);
        assert_eq!(state.validity, ValidityState::Unknown);
        assert_eq!(state.evidence, vec![timeout, finalized]);
    }

    #[test]
    fn tentative_error_does_not_replace_finalized_success() {
        let finalized = example_finalized_success_event();

        let mut tentative = finalized.clone();
        tentative.event_id = "later-confirmed-error".into();
        tentative.sequence += 1;
        tentative.elapsed_ns += 1;
        tentative.event = Event::SignatureObserved {
            slot: 43,
            commitment: Commitment::Confirmed,
            execution_error: Some(serde_json::json!({
                "InstructionError": [0, "InvalidArgument"]
            })),
        };

        let mut state = TransactionState::new(finalized.transaction_id.clone());

        apply_event(&mut state, &finalized).expect("finalization should be accepted");
        apply_event(&mut state, &tentative).expect("tentative evidence should be accepted");

        assert_eq!(state.execution, ExecutionState::FinalizedSuccess);
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.evidence, vec![finalized, tentative]);
    }

    #[test]
    fn deadline_does_not_replace_complete_observation() {
        let finalized = example_finalized_success_event();

        let mut deadline = finalized.clone();
        deadline.event_id = "later-deadline".into();
        deadline.sequence += 1;
        deadline.elapsed_ns += 1;
        deadline.event = Event::ObservationDeadlineReached {
            deadline_ms: 120_000,
        };

        let mut state: TransactionState = TransactionState::new(finalized.transaction_id.clone());

        apply_event(&mut state, &finalized).expect("finalization should be accepted");
        apply_event(&mut state, &deadline).expect("deadline should be accepted");

        assert_eq!(state.execution, ExecutionState::FinalizedSuccess);
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.evidence, vec![finalized, deadline]);
    }

    #[test]
    fn finalized_execution_error_is_preserved() {
        let mut failed = example_finalized_success_event();
        failed.event = Event::SignatureObserved {
            slot: 42,
            commitment: Commitment::Finalized,
            execution_error: Some(serde_json::json!({
                "InstructionError": [0, "InvalidArgument"]
            })),
        };

        let mut state: TransactionState = TransactionState::new(failed.transaction_id.clone());
        apply_event(&mut state, &failed).expect("failed should be accepted");

        assert_eq!(state.execution, ExecutionState::FinalizedError);
        assert_eq!(state.observation, ObservationState::Complete);
        assert!(state.attempts.is_empty());
        assert_eq!(state.evidence, vec![failed]);
    }

    #[test]
    fn contradictory_finalized_results_are_flagged() {
        let success = example_finalized_success_event();
        let mut failed = success.clone();
        failed.event_id = "dfdffdfe".into();
        failed.sequence = 30;
        failed.elapsed_ns = 30;
        failed.event = Event::SignatureObserved {
            slot: 42,
            commitment: Commitment::Finalized,
            execution_error: Some(serde_json::json!({
                "InstructionError": [0, "InvalidArgument"]
            })),
        };

        let mut state: TransactionState = TransactionState::new(failed.transaction_id.clone());

        apply_event(&mut state, &success).expect("success should be accepted");
        apply_event(&mut state, &failed).expect("failed should be accepted");

        assert_eq!(state.execution, ExecutionState::ConflictingEvidence);
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.evidence, vec![success, failed]);
    }

    #[test]
    fn later_success_does_not_erase_conflict() {
        let success = example_finalized_success_event();
        let mut failed = success.clone();
        failed.event_id = "dfdffdfe".into();
        failed.sequence = 30;
        failed.elapsed_ns = 30;
        failed.event = Event::SignatureObserved {
            slot: 42,
            commitment: Commitment::Finalized,
            execution_error: Some(serde_json::json!({
                "InstructionError": [0, "InvalidArgument"]
            })),
        };
        let mut later_success = success.clone();
        later_success.event_id = "success-after-conflict".into();
        later_success.sequence = failed.sequence + 1;
        later_success.elapsed_ns = failed.elapsed_ns + 1;

        let mut state: TransactionState = TransactionState::new(failed.transaction_id.clone());

        apply_event(&mut state, &success).expect("success should be accepted");
        apply_event(&mut state, &failed).expect("failure evidence should be accepted");
        apply_event(&mut state, &later_success).expect("later evidence should be accepted");

        assert_eq!(state.execution, ExecutionState::ConflictingEvidence);
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.evidence, vec![success, failed, later_success]);
    }

    #[test]
    fn different_finalized_slots_are_flagged() {
        let first = example_finalized_success_event();
        let mut second = first.clone();
        second.event_id = "hirgfngfg".into();
        second.sequence = 35;
        second.elapsed_ns = 34;
        second.event = Event::SignatureObserved {
            slot: 43, // The first fixture uses slot 42.
            commitment: Commitment::Finalized,
            execution_error: None,
        };

        let mut state: TransactionState = TransactionState::new(second.transaction_id.clone());

        apply_event(&mut state, &first).expect("first should be accepted");
        apply_event(&mut state, &second).expect("second should be accepted");

        assert_eq!(state.execution, ExecutionState::ConflictingEvidence);
        assert_eq!(state.evidence, vec![first, second]);
    }

    #[test]
    fn acknowledgement_does_not_imply_execution() {
        let mut event = example_timeout_event();
        event.event = Event::RpcAcknowledged {
            returned_signature: event.signature.clone().unwrap(),
        };
        let mut state: TransactionState = TransactionState::new(event.transaction_id.clone());
        apply_event(&mut state, &event).expect("event should be accepted");

        let attempt_id = event.attempt_id.as_ref().unwrap();
        assert_eq!(
            state.attempts.get(attempt_id),
            Some(&SubmissionState::Acknowledged)
        );
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.observation, ObservationState::Active);
        assert_eq!(state.validity, ValidityState::Unknown);
        assert_eq!(state.evidence, vec![event]);
    }

    #[test]
    fn acknowledgement_does_not_imply_execution_attempt() {
        let mut event = example_timeout_event();
        event.event = Event::RpcAcknowledged {
            returned_signature: event.signature.clone().unwrap(),
        };
        event.attempt_id = None;

        let mut state: TransactionState = TransactionState::new(event.transaction_id.clone());
        let before = state.clone();

        assert!(apply_event(&mut state, &event).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn acknowledgement_without_signature_is_rejected() {
        let mut event = example_timeout_event();
        event.event = Event::RpcAcknowledged {
            returned_signature: event.signature.clone().unwrap(),
        };
        event.signature = None;

        let mut state = TransactionState::new(event.transaction_id.clone());
        let before = state.clone();

        assert!(apply_event(&mut state, &event).is_err());
        assert_eq!(state, before);
    }
    #[test]
    fn acknowledgement_does_not_imply_execution_differs() {
        let mut event = example_timeout_event();

        event.event = Event::RpcAcknowledged {
            returned_signature: "different_signature".into(),
        };
        let mut state: TransactionState = TransactionState::new(event.transaction_id.clone());
        let before = state.clone();

        assert!(apply_event(&mut state, &event).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn preflight_rejection_does_not_imply_execution_failure() {
        let mut event = example_timeout_event();

        event.event = Event::RpcRejected {
            stage: RejectionStage::Preflight,
            error: serde_json::json!({
                "code": -32002,
                "message": "Transaction simulation failed",
                "data": {
                    "err": "AccountNotFound"
                }
            }),
        };

        let mut state: TransactionState = TransactionState::new(event.transaction_id.clone());

        apply_event(&mut state, &event).expect("rejection evidence should be accepted");

        let attempt_id = event.attempt_id.as_ref().unwrap();

        assert_eq!(
            state.attempts.get(attempt_id),
            Some(&SubmissionState::Rejected)
        );
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.observation, ObservationState::Active);
        assert_eq!(state.validity, ValidityState::Unknown);
        assert_eq!(state.evidence, vec![event]);
    }

    #[test]
    fn preflight_rejection_then_finalized_success() {
        let mut rejected = example_timeout_event();

        rejected.event = Event::RpcRejected {
            stage: RejectionStage::Preflight,
            error: serde_json::json!({
                "code": -32002,
                "message": "Transaction simulation failed",
                "data": {
                    "err": "AccountNotFound"
                }
            }),
        };
        let mut state: TransactionState = TransactionState::new(rejected.transaction_id.clone());

        let finalized = example_finalized_success_event();
        apply_event(&mut state, &rejected).expect("rejection evidence should be accepted");

        apply_event(&mut state, &finalized).expect("finalized evidence should be accepted");
        let attempt_id = rejected.attempt_id.as_ref().unwrap();

        assert_eq!(
            state.attempts.get(attempt_id),
            Some(&SubmissionState::Rejected)
        );
        assert_eq!(state.execution, ExecutionState::FinalizedSuccess);
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.evidence, vec![rejected, finalized]);
    }

    #[test]
    fn finalized_height_at_boundary_is_valid() {
        let mut prepared = example_timeout_event();
        prepared.attempt_id = None;
        prepared.event = Event::TransactionPrepared {
            signature: prepared.signature.clone().unwrap(),
            wire_hash: "example-wire-hash".into(),
            blockhash: "example-blockhash".into(),
            last_valid_block_height: 100,
            blockhash_context_slot: 42,
            workload_parameters: serde_json::json!({}),
        };

        let mut height_event = prepared.clone();
        height_event.event_id = "height-observation-1".into();
        height_event.sequence += 1;
        height_event.elapsed_ns += 1;
        height_event.event = Event::BlockHeightObserved {
            height: 100,
            commitment: Commitment::Finalized,
        };

        let mut state = TransactionState::new(prepared.transaction_id.clone());

        apply_event(&mut state, &prepared).expect("preparation should be accepted");

        apply_event(&mut state, &height_event).expect("height observation should be accepted");

        assert_eq!(state.validity, ValidityState::ValidAtLastCheck);
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.last_valid_block_height, Some(100));
        assert_eq!(state.evidence, vec![prepared, height_event]);
    }

    // Synthetic events keep these tests independent of RPCs and wall clocks.
    fn prepared_event() -> EventEnvelope {
        let mut event = example_timeout_event();
        event.event_id = "prepared-1".into();
        event.attempt_id = None;
        event.event = Event::TransactionPrepared {
            signature: event.signature.clone().unwrap(),
            wire_hash: "fixture-wire-hash".into(),
            blockhash: "fixture-blockhash".into(),
            last_valid_block_height: 100,
            blockhash_context_slot: 42,
            workload_parameters: serde_json::json!({}),
        };
        event
    }

    fn later_height(
        previous: &EventEnvelope,
        height: u64,
        commitment: Commitment,
    ) -> EventEnvelope {
        let mut event = previous.clone();
        event.sequence += 1;
        event.elapsed_ns += 1;
        event.event_id = format!("height-{}", event.sequence);
        event.event = Event::BlockHeightObserved { height, commitment };
        event
    }

    #[test]
    fn preparation_stores_signature_and_boundary() {
        let prepared = prepared_event();
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        apply_event(&mut state, &prepared).unwrap();
        assert_eq!(state.signature, prepared.signature);
        assert_eq!(state.last_valid_block_height, Some(100));
        assert_eq!(state.validity, ValidityState::Unknown);
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.observation, ObservationState::Active);
        assert!(state.attempts.is_empty());
        assert_eq!(state.evidence, vec![prepared]);
    }

    #[test]
    fn preparation_with_mismatched_envelope_signature_is_rejected() {
        let mut prepared = prepared_event();
        prepared.signature = Some("different-signature".into());
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        let before = state.clone();
        assert!(apply_event(&mut state, &prepared).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn observation_with_different_prepared_signature_is_rejected() {
        let prepared = prepared_event();
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        apply_event(&mut state, &prepared).unwrap();
        let before = state.clone();
        let mut observed = example_finalized_success_event();
        observed.signature = Some("different-signature".into());
        assert!(apply_event(&mut state, &observed).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn observation_without_signature_is_rejected() {
        let mut observed = example_finalized_success_event();
        observed.signature = None;
        let mut state = TransactionState::new(observed.transaction_id.clone());
        let before = state.clone();
        assert!(apply_event(&mut state, &observed).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn finalized_height_above_boundary_passes_validity_window() {
        let prepared = prepared_event();
        let height = later_height(&prepared, 101, Commitment::Finalized);
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        apply_event(&mut state, &prepared).unwrap();
        apply_event(&mut state, &height).unwrap();
        assert_eq!(state.validity, ValidityState::ValidityWindowPassed);
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.observation, ObservationState::Active);
        assert_eq!(state.evidence, vec![prepared, height]);
    }

    #[test]
    fn non_finalized_height_cannot_establish_validity() {
        for commitment in [Commitment::Processed, Commitment::Confirmed] {
            let prepared = prepared_event();
            let height = later_height(&prepared, 101, commitment);
            let mut state = TransactionState::new(prepared.transaction_id.clone());
            apply_event(&mut state, &prepared).unwrap();
            apply_event(&mut state, &height).unwrap();
            assert_eq!(state.validity, ValidityState::Unknown);
            assert_eq!(state.execution, ExecutionState::Unobserved);
            assert_eq!(state.evidence, vec![prepared, height]);
        }
    }

    #[test]
    fn height_without_preparation_leaves_validity_unknown() {
        let height = later_height(&example_timeout_event(), 101, Commitment::Finalized);
        let mut state = TransactionState::new(height.transaction_id.clone());
        apply_event(&mut state, &height).unwrap();
        assert_eq!(state.validity, ValidityState::Unknown);
        assert_eq!(state.last_valid_block_height, None);
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.evidence, vec![height]);
    }

    #[test]
    fn lower_height_cannot_reverse_passed_validity_window() {
        let prepared = prepared_event();
        let passed = later_height(&prepared, 101, Commitment::Finalized);
        let older = later_height(&passed, 99, Commitment::Finalized);
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        for event in [&prepared, &passed, &older] {
            apply_event(&mut state, event).unwrap();
        }
        assert_eq!(state.validity, ValidityState::ValidityWindowPassed);
        assert_eq!(state.execution, ExecutionState::Unobserved);
        assert_eq!(state.evidence, vec![prepared, passed, older]);
    }

    #[test]
    fn repeated_preparation_cannot_change_boundary() {
        let prepared = prepared_event();
        let mut changed = prepared.clone();
        changed.event_id = "prepared-changed-boundary".into();
        changed.sequence += 1;
        changed.elapsed_ns += 1;
        if let Event::TransactionPrepared {
            last_valid_block_height,
            ..
        } = &mut changed.event
        {
            *last_valid_block_height = 120;
        }
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        apply_event(&mut state, &prepared).unwrap();
        let before = state.clone();
        assert!(apply_event(&mut state, &changed).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn consistent_repeated_preparation_is_accepted() {
        let prepared = prepared_event();
        let mut repeated = prepared.clone();
        repeated.event_id = "prepared-repeat".into();
        repeated.sequence += 1;
        repeated.elapsed_ns += 1;
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        apply_event(&mut state, &prepared).unwrap();
        apply_event(&mut state, &repeated).unwrap();
        assert_eq!(state.signature, prepared.signature);
        assert_eq!(state.last_valid_block_height, Some(100));
        assert_eq!(state.evidence, vec![prepared, repeated]);
    }

    #[test]
    fn acknowledgement_cannot_override_prepared_identity() {
        let prepared = prepared_event();
        let mut ack = example_timeout_event();
        ack.event_id = "wrong-identity-ack".into();
        ack.sequence = prepared.sequence + 1;
        ack.elapsed_ns = prepared.elapsed_ns + 1;
        ack.signature = Some("different-signature".into());
        ack.event = Event::RpcAcknowledged {
            returned_signature: "different-signature".into(),
        };
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        apply_event(&mut state, &prepared).unwrap();
        let before = state.clone();
        assert!(apply_event(&mut state, &ack).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn validity_passage_preserves_finalized_execution() {
        let prepared = prepared_event();
        let finalized = example_finalized_success_event();
        let height = later_height(&finalized, 101, Commitment::Finalized);
        let mut state = TransactionState::new(prepared.transaction_id.clone());
        for event in [&prepared, &finalized, &height] {
            apply_event(&mut state, event).unwrap();
        }
        assert_eq!(state.validity, ValidityState::ValidityWindowPassed);
        assert_eq!(state.execution, ExecutionState::FinalizedSuccess);
        assert_eq!(state.observation, ObservationState::Complete);
        assert_eq!(state.evidence, vec![prepared, finalized, height]);
    }

    #[test]
    fn reused_id_changed_payload() {
        let event = example_timeout_event();
        let mut state = TransactionState::new(event.transaction_id.clone());
        apply_event(&mut state, &event).unwrap();
        let before = state.clone();
        let mut changed = event.clone();
        changed.event = Event::TransportFailed {
            kind: crate::model::TransportFailureKind::Read,
        };
        assert!(apply_event(&mut state, &changed).is_err());
        assert_eq!(state, before);
    }
}
