//! Controlled serialization experiment, not a network benchmark.
use std::{hint::black_box, time::Instant};
use txtrail::model::EventEnvelope;
fn trial(events: &[EventEnvelope], reuse: bool, rounds: usize) -> u128 {
    let mut buffer = Vec::with_capacity(1024);
    let start = Instant::now();
    for _ in 0..rounds {
        for event in events {
            if reuse {
                buffer.clear();
                serde_json::to_writer(&mut buffer, black_box(event)).unwrap();
                black_box(&buffer);
            } else {
                black_box(serde_json::to_vec(black_box(event)).unwrap());
            }
        }
    }
    start.elapsed().as_nanos()
}
fn main() {
    let events: Vec<EventEnvelope> = include_str!("../tests/fixtures/success.jsonl")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    for e in &events {
        let mut buf = Vec::new();
        serde_json::to_writer(&mut buf, e).unwrap();
        assert_eq!(buf, serde_json::to_vec(e).unwrap());
    }
    trial(&events, false, 2000);
    trial(&events, true, 2000);
    println!("trial,events,fresh_buffer_ns,reused_buffer_ns");
    for i in 0..7 {
        let (fresh, reused) = if i % 2 == 0 {
            (trial(&events, false, 20000), trial(&events, true, 20000))
        } else {
            let reused = trial(&events, true, 20000);
            (trial(&events, false, 20000), reused)
        };
        println!("{i},{},{fresh},{reused}", events.len() * 20000);
    }
}
