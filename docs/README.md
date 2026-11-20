# TxTrail

TxTrail is a Rust CLI being built to record and explain the lifecycle of a Solana transaction. Its intended purpose is to distinguish RPC acknowledgement from execution and finality, and preserve uncertainty when evidence is incomplete.

## Current status

Milestones 0 and 1 implement configuration validation and the offline evidence model. The library exposes a pure reducer, deterministic replay of decoded events, and evidence-based outcome labels. Synthetic JSONL fixtures exercise success, execution error, rejection, timeout, unresolved deadlines, validity passage, and conflicts. See [milestone 1 notes](milestone-1.md).

 The `doctor` command reads a TOML file, checks required fields and supported enum values, and validates positive timeouts, intervals, and limits alongside nonempty strings.

It does not yet contact an RPC endpoint, check network health, sign or submit transactions, capture events, or generate transaction reports. A valid configuration does not establish that an endpoint is reachable or belongs to the selected cluster.

## Requirements

- Rust and Cargo. Recorded development tool versions are in [toolchain.txt](toolchain.txt).
- Internet access for the initial dependency download, unless dependencies are already cached.

No wallet, SOL, running validator, paid provider, or subscription is needed for the current command. Solana CLI and local validator versions are recorded for later integration work; they are not required for configuration validation.

## Run

Run all commands below from the repository root, not the `docs` directory:

```sh
cargo run -- doctor --config config/local.toml
```

Expected application output:

```text
Configuration valid; network checks not implemented.
```

Successful validation exits with code `0`. File-reading, parsing, or validation failures print an error to standard error and exit with code `1`.

The application makes no network requests during configuration loading. After dependencies are cached, Cargo can also run offline:

```sh
cargo run --offline -- doctor --config config/local.toml
```

For command help:

```sh
cargo run -- --help
cargo run -- doctor --help
```

## Configuration

The example configuration is [config/local.toml](../config/local.toml). It describes settings for future submission and observation features; those features are not active yet.

| Setting | Intended meaning |
|---|---|
| `cluster` | Network profile: `local` or `devnet`. |
| `rpc_url` | RPC endpoint address. Currently checked only for a nonempty value. |
| `route_id` | Alias identifying the submission route. |
| `observer_id` | Alias identifying the source of observations. |
| `output_dir` | Directory for future run data. |
| `submission.skip_preflight` | Whether submission should skip preflight checks. |
| `submission.preflight_commitment` | Commitment used for preflight checks. |
| `submission.rpc_max_retries` | Retry setting to pass to the RPC server. Zero disables server retries. |
| `submission.client_resubmissions` | Client resubmission setting; zero keeps resubmissions disabled for the initial implementation. |
| `submission.request_timeout_ms` | Timeout for an individual submission request, in milliseconds. |
| `observation.target_commitment` | Commitment the observer should wait for. |
| `observation.poll_interval_ms` | Interval between signature-status polls. |
| `observation.height_poll_interval_ms` | Interval between block-height checks. |
| `observation.deadline_ms` | Observation deadline; reaching it does not prove transaction expiry. |
| `observation.websocket_enabled` | Setting for the later optional WebSocket observer. |
| `limits.max_in_flight_transactions` | Maximum number of transactions tracked concurrently. |
| `limits.max_http_requests_per_second` | Shared application HTTP request-rate limit. |

Supported commitments are `processed`, `confirmed`, and `finalized`. Timeouts, intervals, deadlines, and limits must be greater than zero. Retry counts may be zero. Endpoint, identity, and output-directory strings cannot be empty or whitespace-only.

These settings are application configuration, not promises about provider quotas. Network identity checks and submission restrictions still need to be implemented before live submission.

## Development checks

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Milestone 1 checks run offline after dependencies are installed. Unit tests cover state transitions and invalid evidence; integration tests replay the synthetic fixtures through the same reducer and verify deterministic outcomes. No validator or public endpoint is used.

## Planned scope

Later milestones will add durable JSONL capture, replay/inspect CLI commands, offline reports, mocked RPC tests, local transaction submission, and optional low-volume devnet validation. Mainnet submission and paid infrastructure are outside the first release.

## Local files

`.gitignore` excludes `.local/`, `runs/`, and `target/`. Keep wallet files in `.local/`; never commit private keys or authenticated endpoint URLs. Commit `Cargo.lock` and only reviewed, sanitized example data.

