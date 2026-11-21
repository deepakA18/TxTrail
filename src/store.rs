//! Single-writer, bounded, durable JSONL capture. No network code lives here.
use crate::{model::EventEnvelope, reducer::replay};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct ReadResult {
    pub events: Vec<EventEnvelope>,
    pub warnings: Vec<String>,
}
pub fn parse_jsonl(contents: &str) -> Result<ReadResult, String> {
    let mut result = ReadResult {
        events: vec![],
        warnings: vec![],
    };
    for (index, part) in contents.split_inclusive('\n').enumerate() {
        let line = part.trim_end_matches(['\r', '\n']);
        let number = index + 1;
        let event: EventEnvelope = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(e) if !part.ends_with('\n') && e.is_eof() => {
                result.warnings.push(format!(
                    "Line {number}: truncated final event ignored; valid prefix recovered"
                ));
                break;
            }
            Err(e) => return Err(format!("Line {number}: {e}")),
        };
        if event.schema_version != 1 {
            return Err(format!(
                "Line {number}: unsupported schema version {}",
                event.schema_version
            ));
        }
        if event.event_id.is_empty() || event.run_id.is_empty() || event.session_id.is_empty() {
            return Err(format!("Line {number}: missing identity"));
        }
        if chrono::DateTime::parse_from_rfc3339(&event.observed_at_utc).is_err() {
            return Err(format!("Line {number}: invalid UTC timestamp"));
        }
        result.events.push(event);
    }
    Ok(result)
}
pub fn event_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("events.jsonl")
    } else {
        path.into()
    }
}
pub fn read(path: &Path) -> Result<ReadResult, String> {
    let bytes = std::fs::read(event_path(path))
        .map_err(|e| format!("Cannot read {}: {e}", path.display()))?;
    match std::str::from_utf8(&bytes) {
        Ok(s) => parse_jsonl(s),
        Err(e) if e.error_len().is_none() => {
            let end = bytes[..e.valid_up_to()]
                .iter()
                .rposition(|b| *b == b'\n')
                .map_or(0, |p| p + 1);
            let mut result =
                parse_jsonl(std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?)?;
            result.warnings.push(format!(
                "Line {}: truncated UTF-8 final event ignored",
                result.events.len() + 1
            ));
            Ok(result)
        }
        Err(e) => Err(format!("Invalid UTF-8 log: {e}")),
    }
}
struct Pending {
    event: EventEnvelope,
    ack: oneshot::Sender<Result<EventEnvelope, String>>,
}
#[derive(Clone)]
pub struct Recorder {
    tx: mpsc::Sender<Pending>,
    saturated: Arc<AtomicBool>,
}
pub struct Writer {
    pub recorder: Recorder,
    task: tokio::task::JoinHandle<Result<(), String>>,
}
impl Recorder {
    pub fn saturated(&self) -> bool {
        self.saturated.load(Ordering::Relaxed)
    }
    pub async fn record(&self, event: EventEnvelope) -> Result<EventEnvelope, String> {
        let (ack, receive) = oneshot::channel();
        let p = Pending { event, ack };
        match self.tx.try_send(p) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(p)) => {
                self.saturated.store(true, Ordering::Relaxed);
                self.tx
                    .send(p)
                    .await
                    .map_err(|_| "Storage failed; capture incomplete; stop submissions")?;
            }
            Err(_) => return Err("Storage failed; capture incomplete; stop submissions".into()),
        }
        receive
            .await
            .map_err(|_| "Writer stopped; capture incomplete".to_string())?
    }
}
trait DurableSink: Write + Send + 'static {
    fn durable_flush(&mut self) -> std::io::Result<()>;
}
impl DurableSink for std::fs::File {
    fn durable_flush(&mut self) -> std::io::Result<()> {
        self.sync_all()
    }
}
impl Writer {
    pub fn open(path: &Path, append: bool, capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("Queue capacity must be positive".into());
        }
        let mut file = if append {
            OpenOptions::new().read(true).append(true).open(path)
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
        }
        .map_err(|e| e.to_string())?;
        file.try_lock()
            .map_err(|e| format!("Cannot lock run for writing: {e}"))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| e.to_string())?;
        let previous = parse_jsonl(&contents)?;
        if !previous.warnings.is_empty() {
            return Err("Cannot append to truncated log; preserve it and recover prefix into a new file first".into());
        }
        replay(&previous.events)?;
        let sequence = previous
            .events
            .iter()
            .map(|e| e.sequence)
            .max()
            .unwrap_or(0);
        if !contents.is_empty() && !contents.ends_with('\n') {
            file.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        Ok(Self::with_sink(file, sequence, capacity))
    }
    fn with_sink(mut file: impl DurableSink, sequence: u64, capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<Pending>(capacity);
        let task = tokio::task::spawn_blocking(move || {
            let mut sequence = sequence;
            let mut buffer = Vec::with_capacity(4096);
            while let Some(mut p) = rx.blocking_recv() {
                sequence = sequence.checked_add(1).ok_or("Sequence overflow")?;
                p.event.sequence = sequence;
                buffer.clear();
                let write = (|| -> Result<(), String> {
                    serde_json::to_writer(&mut buffer, &p.event).map_err(|e| e.to_string())?;
                    buffer.push(b'\n');
                    file.write_all(&buffer).map_err(|e| e.to_string())?;
                    file.durable_flush().map_err(|e| e.to_string())?;
                    Ok(())
                })();
                if let Err(e) = write {
                    let message = format!("Storage failed; capture incomplete: {e}");
                    let _ = p.ack.send(Err(message.clone()));
                    return Err(message);
                }
                let _ = p.ack.send(Ok(p.event));
            }
            file.durable_flush().map_err(|e| e.to_string())
        });
        Self {
            recorder: Recorder {
                tx,
                saturated: Arc::new(AtomicBool::new(false)),
            },
            task,
        }
    }
    pub async fn finish(self) -> Result<(), String> {
        drop(self.recorder);
        self.task.await.map_err(|e| e.to_string())?
    }
}
#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::*;

    #[test]
    fn reads_success_fixture() {
        let contents = include_str!("../tests/fixtures/success.jsonl");

        let result = parse_jsonl(contents).expect("valid fixture should parse");

        assert_eq!(result.events.len(), 4);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn malformed_json_reports_line_number() {
        let fixture = include_str!("../tests/fixtures/success.jsonl");
        let first_line = fixture.lines().next().unwrap();

        let contents = format!("{first_line}\nnot valid JSON\n");

        let error = parse_jsonl(&contents).expect_err("malformed JSON should fail");

        assert!(error.starts_with("Line 2:"), "{error}");
    }
}

#[cfg(test)]
mod writer_failure_tests {
    use super::*;
    struct Failing;
    impl Write for Failing {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("injected disk failure"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl DurableSink for Failing {
        fn durable_flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn storage_failure_stops_capture_and_future_submissions() {
        let writer = Writer::with_sink(Failing, 0, 1);
        assert!(
            writer
                .recorder
                .record(crate::model::example_timeout_event())
                .await
                .unwrap_err()
                .contains("capture incomplete")
        );
        assert!(
            writer
                .recorder
                .record(crate::model::example_timeout_event())
                .await
                .is_err()
        );
        assert!(writer.finish().await.is_err());
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::*;
    #[tokio::test]
    async fn full_channel_waits_and_preserves_both_events() {
        let (tx, mut rx) = mpsc::channel::<Pending>(1);
        let recorder = Recorder {
            tx,
            saturated: Arc::new(AtomicBool::new(false)),
        };
        let first = recorder.clone();
        let a =
            tokio::spawn(async move { first.record(crate::model::example_timeout_event()).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while recorder.tx.capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = recorder.clone();
        let b =
            tokio::spawn(async move { second.record(crate::model::example_timeout_event()).await });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !recorder.saturated() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!a.is_finished());
        assert!(!b.is_finished());
        for _ in 0..2 {
            let pending = rx.recv().await.unwrap();
            pending.ack.send(Ok(pending.event)).unwrap();
        }
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
    }
}
