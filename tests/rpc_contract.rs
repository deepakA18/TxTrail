use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};
use txtrail::{config::Config, rpc::Rpc};
fn server(replies: Vec<(u16, &'static str, Value)>) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let task = std::thread::spawn(move || {
        for (status, header, result) in replies {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = vec![];
            let mut buf = [0; 2048];
            let request = loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let len = headers
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + len {
                        break serde_json::from_slice::<Value>(&bytes[end + 4..end + 4 + len])
                            .unwrap();
                    }
                }
            };
            let body = if result == json!("malformed") {
                "not json".into()
            } else if result.get("error").is_some() {
                json!({"jsonrpc":"2.0","id":request["id"],"error":result["error"]}).to_string()
            } else if result == json!("wrong-id") {
                json!({"jsonrpc":"2.0","id":999,"result":null}).to_string()
            } else {
                json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string()
            };
            write!(stream,"HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n{header}\r\n{body}",body.len()).unwrap();
        }
    });
    (format!("http://{addr}"), task)
}
fn config(url: String) -> Config {
    let mut c: Config = toml::from_str(include_str!("../config/local.toml")).unwrap();
    c.rpc_url = url;
    c.limits.max_http_requests_per_second = 1000;
    c
}
#[tokio::test]
async fn handles_429_500_and_null_status() {
    let (url, task) = server(vec![
        (429, "Retry-After: 0\r\n", Value::Null),
        (500, "", Value::Null),
        (200, "", json!({"value":[null]})),
    ]);
    let rpc = Rpc::new(&config(url)).unwrap();
    assert!(rpc.status("synthetic", true).await.unwrap().is_none());
    assert_eq!(rpc.drain_diagnostics().len(), 3);
    task.join().unwrap();
}
#[tokio::test]
async fn validates_jsonrpc_and_does_not_retry_send() {
    for result in [
        json!("malformed"),
        json!("wrong-id"),
        json!({"error":{"code":-32002,"message":"synthetic rejection"}}),
    ] {
        let (url, task) = server(vec![(200, "", result)]);
        let rpc = Rpc::new(&config(url)).unwrap();
        assert!(rpc.call("sendTransaction", json!([])).await.is_err());
        assert_eq!(rpc.drain_diagnostics().len(), 1);
        task.join().unwrap();
    }
}
#[tokio::test]
async fn devnet_identity_mismatch_fails_closed() {
    let (url, task) = server(vec![(200, "", json!("5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"))]);
    let c = config(url);
    let rpc = Rpc::new(&c).unwrap();
    assert!(rpc.verify_network(&c).await.is_err());
    task.join().unwrap();
}
#[test]
fn rejects_unsafe_profiles_and_redacts_errors() {
    let mut c = config("https://api.mainnet-beta.solana.com".into());
    assert!(c.validate().is_err());
    c.rpc_url = "http://localhost:8899?api_key=secret".into();
    assert!(c.validate().is_err());
    let value = txtrail::rpc::sanitize(
        &json!({"message":"https://example.com?api_key=secret","data":{"err":"AccountNotFound"}}),
    );
    assert!(!value.to_string().contains("secret"));
    assert_eq!(value["data"]["err"], "AccountNotFound");
}
