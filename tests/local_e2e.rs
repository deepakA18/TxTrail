//! Opt-in: uses an already running, funded local validator; never starts or resets it.
use txtrail::{
    config::{Cluster, Config},
    model::{Event, Outcome},
    transaction::Workload,
};

#[tokio::test]
#[ignore = "requires TXTRAIL_LOCAL_CONFIG and TXTRAIL_LOCAL_KEYPAIR"]
async fn local_success_preflight_and_landed_error() {
    let config_path = std::env::var("TXTRAIL_LOCAL_CONFIG").expect("set TXTRAIL_LOCAL_CONFIG");
    let key = std::env::var("TXTRAIL_LOCAL_KEYPAIR").expect("set TXTRAIL_LOCAL_KEYPAIR");
    let mut config = Config::load_config(std::path::Path::new(&config_path)).unwrap();
    assert!(matches!(config.cluster, Cluster::Local));
    let output = tempfile::tempdir().unwrap();
    config.output_dir = output.path().to_string_lossy().into_owned();
    for (workload, expected) in [
        (Workload::Transfer, Outcome::FinalizedSuccess),
        (
            Workload::PreflightError,
            Outcome::SubmissionRejectedNoChainEvidence,
        ),
        (Workload::ExecutionError, Outcome::FinalizedExecutionError),
    ] {
        let path = txtrail::runner::send(&config, std::path::Path::new(&key), workload)
            .await
            .unwrap();
        let input = txtrail::store::read(&path).unwrap();
        let report = txtrail::report::build(&input).unwrap();
        assert!(report.capture_complete);
        assert_eq!(report.transactions[0].outcome, expected);
        if expected == Outcome::FinalizedExecutionError {
            assert!(input.events.iter().any(|e| matches!(
                &e.event,
                Event::TransactionDetailsObserved {
                    execution_error: Some(_),
                    ..
                }
            )));
        }
    }
}
