use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SubmissionState {
    NotStarted,
    Acknowledged,
    Rejected,
    Uncertain,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum ExecutionState {
    Unobserved,
    TentativeSuccess,
    TentativeError,
    FinalizedSuccess,
    FinalizedError,
    ConflictingEvidence,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum ValidityState {
    Unknown,
    ValidAtLastCheck,
    ValidityWindowPassed,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum ObservationState {
    Active,
    Interrupted,
    DeadlineReached,
    Complete,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
pub enum Event {
    RunStarted {
        tool_version: String,
        configuration_hash: String,
        network_identity: String,
        environment: serde_json::Value,
    },
    SendStarted {
        route: String,
        preflight_commitment: Commitment,
        skip_preflight: bool,
        rpc_max_retries: u64,
    },
    ReceivedNotification,
    RpcDiagnostic {
        method: String,
        request_id: u64,
        category: String,
        retry_after_ms: Option<u64>,
    },
    ObserverUnavailable {
        cause: String,
        retry_after_ms: Option<u64>,
    },
    TransactionDetailsObserved {
        slot: u64,
        blockhash: Option<String>,
        fee: u64,
        execution_error: Option<serde_json::Value>,
        compute_units: Option<u64>,
    },
    ValidityWindowPassed {
        height: u64,
        commitment: Commitment,
    },
    RunFinished {
        reason: String,
        incomplete_data: Vec<String>,
    },
    RpcAcknowledged {
        returned_signature: String,
    },
    TransportFailed {
        kind: TransportFailureKind,
    },
    ObservationDeadlineReached {
        deadline_ms: u64,
    },
    SignatureObserved {
        slot: u64,
        commitment: Commitment,
        execution_error: Option<serde_json::Value>,
    },
    RpcRejected {
        stage: RejectionStage,
        error: serde_json::Value,
    },
    TransactionPrepared {
        signature: String,
        wire_hash: String,
        blockhash: String,
        last_valid_block_height: u64,
        blockhash_context_slot: u64,
        workload_parameters: serde_json::Value,
    },
    BlockHeightObserved {
        height: u64,
        commitment: Commitment,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum TransportFailureKind {
    Timeout,
    Connect,
    Read,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RejectionStage {
    Preflight,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct EventEnvelope {
    pub schema_version: u32,
    pub run_id: String,
    pub session_id: String,
    pub sequence: u64,
    pub event_id: String,
    pub transaction_id: String,
    pub signature: Option<String>,
    pub attempt_id: Option<String>,
    pub source_id: String,
    pub observed_at_utc: String,
    pub elapsed_ns: u64,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TransactionState {
    pub first_execution_observation: Option<ObservationTime>,
    pub first_finalized_observation: Option<ObservationTime>,
    pub transaction_id: String,
    pub signature: Option<String>,
    pub attempts: std::collections::BTreeMap<String, SubmissionState>,
    pub execution: ExecutionState,
    pub validity: ValidityState,
    pub observation: ObservationState,
    pub evidence: Vec<EventEnvelope>,
    pub last_valid_block_height: Option<u64>,
}

pub fn example_timeout_event() -> EventEnvelope {
    EventEnvelope {
        schema_version: 1,
        run_id: "0xfdfe".into(),
        session_id: "0xddfdfd".into(),
        sequence: 24,
        event_id: "0asdfdte".into(),
        transaction_id: "dfdhfuefds".into(),
        signature: Some("ateedfd".into()),
        attempt_id: Some("asdtetdsfda".into()),
        source_id: "dasfeefe".into(),
        observed_at_utc: "2026-11-19T10:00:00Z".into(),
        elapsed_ns: 23,
        event: Event::TransportFailed {
            kind: TransportFailureKind::Timeout,
        },
    }
}

pub fn example_finalized_success_event() -> EventEnvelope {
    EventEnvelope {
        schema_version: 1,
        run_id: "0xfdfe".into(),
        session_id: "0xddfdfd".into(),
        sequence: 26,
        event_id: "0dfdaef".into(),
        transaction_id: "dfdhfuefds".into(),
        signature: Some("ateedfd".into()),
        attempt_id: None,
        source_id: "dasfeefe".into(),
        observed_at_utc: "2026-11-19T10:00:00Z".into(),
        elapsed_ns: 25,
        event: Event::SignatureObserved {
            slot: 42,
            commitment: Commitment::Finalized,
            execution_error: None,
        },
    }
}

impl TransactionState {
    pub fn new(transaction_id: String) -> Self {
        TransactionState {
            first_execution_observation: None,
            first_finalized_observation: None,
            transaction_id,
            signature: None,
            attempts: std::collections::BTreeMap::new(),
            execution: ExecutionState::Unobserved,
            validity: ValidityState::Unknown,
            observation: ObservationState::Active,
            evidence: Vec::new(),
            last_valid_block_height: None,
        }
    }
}

/// Monotonic timestamps are comparable only within the same process session.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ObservationTime {
    pub session_id: String,
    pub elapsed_ns: u64,
}

impl ObservationTime {
    pub fn duration_since(&self, start: &Self) -> Option<u64> {
        if self.session_id != start.session_id {
            return None;
        }
        self.elapsed_ns.checked_sub(start.elapsed_ns)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Pending,
    FinalizedSuccess,
    FinalizedExecutionError,
    SubmissionRejectedNoChainEvidence,
    UnresolvedAtDeadline,
    UnobservedAfterValidityWindow,
    ConflictingEvidence,
}

impl TransactionState {
    pub fn outcome(&self) -> Outcome {
        match self.execution {
            ExecutionState::ConflictingEvidence => return Outcome::ConflictingEvidence,
            ExecutionState::FinalizedSuccess => return Outcome::FinalizedSuccess,
            ExecutionState::FinalizedError => return Outcome::FinalizedExecutionError,
            _ => {}
        }
        if self.validity == ValidityState::ValidityWindowPassed
            && self.execution == ExecutionState::Unobserved
        {
            return Outcome::UnobservedAfterValidityWindow;
        }
        if self.observation == ObservationState::DeadlineReached {
            return Outcome::UnresolvedAtDeadline;
        }
        if self.execution == ExecutionState::Unobserved
            && !self.attempts.is_empty()
            && self
                .attempts
                .values()
                .all(|s| *s == SubmissionState::Rejected)
        {
            return Outcome::SubmissionRejectedNoChainEvidence;
        }
        Outcome::Pending
    }
}
