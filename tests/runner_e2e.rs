//! Full runner tests use a deterministic local fake RPC, never a public endpoint.
use serde_json::{json,Value};
use std::{io::{Read,Write},net::TcpListener,sync::{Arc,atomic::{AtomicBool,AtomicUsize,Ordering}},time::Duration};
use txtrail::{config::Config,model::{Outcome,SubmissionState},transaction::Workload};
fn respond(mut stream:std::net::TcpStream, delivered:Arc<AtomicBool>, sends:Arc<AtomicUsize>, mode:&'static str){
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();let mut bytes=vec![];let mut buf=[0;4096];
    let request=loop{let n=stream.read(&mut buf).unwrap();if n==0{return;}bytes.extend_from_slice(&buf[..n]);if let Some(end)=bytes.windows(4).position(|w|w==b"\r\n\r\n"){
        let headers=String::from_utf8_lossy(&bytes[..end]);let len=headers.lines().find_map(|l|l.to_lowercase().strip_prefix("content-length:").and_then(|v|v.trim().parse::<usize>().ok())).unwrap();
        if bytes.len()>=end+4+len{break serde_json::from_slice::<Value>(&bytes[end+4..end+4+len]).unwrap();}
    }};
    let result=match request["method"].as_str().unwrap(){
        "getGenesisHash"=>json!("synthetic-local-genesis"),
        "getMinimumBalanceForRentExemption"=>json!(890880),
        "getBalance"=>json!({"value":2_000_000_000u64}),
        "getLatestBlockhash"=>json!({"context":{"slot":42},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}}),
        "sendTransaction"=>{sends.fetch_add(1,Ordering::SeqCst);delivered.store(true,Ordering::SeqCst);if mode=="timeout"{std::thread::sleep(Duration::from_millis(250));}json!("wrong-signature")},
        "getSignatureStatuses"=>if delivered.load(Ordering::SeqCst){json!({"value":[{"slot":43,"confirmationStatus":"finalized","err":null}]})}else{json!({"value":[null]})},
        "getBlockHeight"=>json!(50),
        "getTransaction"=>Value::Null,
        other=>panic!("unexpected method {other}"),
    };
    let body=json!({"jsonrpc":"2.0","id":request["id"],"result":result}).to_string();let _=write!(stream,"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
}
async fn scenario(mode:&'static str){
    let dir=tempfile::tempdir().unwrap();let key=dir.path().join("wallet.json");
    solana_sdk::signature::write_keypair_file(&solana_sdk::signature::Keypair::new(),&key).unwrap();
    let listener=TcpListener::bind("127.0.0.1:0").unwrap();listener.set_nonblocking(true).unwrap();let addr=listener.local_addr().unwrap();
    let stop=Arc::new(AtomicBool::new(false));let sends=Arc::new(AtomicUsize::new(0));let flag=stop.clone();let count=sends.clone();
    let server=std::thread::spawn(move||{let delivered=Arc::new(AtomicBool::new(false));let mut children=vec![];while !flag.load(Ordering::SeqCst){match listener.accept(){Ok((stream,_))=>{let d=delivered.clone();let s=count.clone();children.push(std::thread::spawn(move||respond(stream,d,s,mode)));},Err(e)if e.kind()==std::io::ErrorKind::WouldBlock=>std::thread::sleep(Duration::from_millis(2)),Err(e)=>panic!("{e}")}}for c in children{c.join().unwrap();}});
    let mut config:Config=toml::from_str(include_str!("../config/local.toml")).unwrap();config.rpc_url=format!("http://{addr}");config.output_dir=dir.path().join("runs").to_string_lossy().into_owned();config.submission.request_timeout_ms=100;config.limits.max_http_requests_per_second=1000;config.observation.poll_interval_ms=10;config.observation.deadline_ms=2000;
    let result=txtrail::runner::send(&config,&key,Workload::Transfer).await;
    stop.store(true,Ordering::SeqCst);server.join().unwrap();let path=result.unwrap();
    let input=txtrail::store::read(&path).unwrap();let states=txtrail::reducer::replay(&input.events).unwrap();assert_eq!(states.len(),1);
    let state=states.values().next().unwrap();assert_eq!(state.outcome(),Outcome::FinalizedSuccess);assert_eq!(state.attempts["attempt-1"],SubmissionState::Uncertain);assert_eq!(sends.load(Ordering::SeqCst),1);
    assert!(input.events.iter().any(|e|matches!(&e.event,txtrail::model::Event::RunFinished{..})));
}
#[tokio::test]async fn acknowledgement_timeout_then_finalization(){scenario("timeout").await;}
#[tokio::test]async fn mismatched_acknowledgement_does_not_prevent_observation(){scenario("mismatch").await;}
