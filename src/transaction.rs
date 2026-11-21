use crate::{config::Config, rpc::Rpc};
use base64::Engine;
use serde_json::json;
use sha2::{Digest, Sha256};
use solana_sdk::{
    hash::Hash,
    signature::{Keypair, Signer, read_keypair_file},
    system_instruction,
    transaction::Transaction,
};
use std::{path::Path, str::FromStr};
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Workload {
    Transfer,
    PreflightError,
    ExecutionError,
}
pub struct Prepared {
    pub signature: String,
    pub wire: String,
    pub wire_hash: String,
    pub blockhash: String,
    pub last_valid_block_height: u64,
    pub context_slot: u64,
    pub workload: serde_json::Value,
}
pub async fn prepare(
    rpc: &Rpc,
    config: &Config,
    keypair: &Path,
    workload: Workload,
) -> Result<Prepared, String> {
    let payer = read_keypair_file(keypair)
        .map_err(|_| "Cannot read keypair file (expected a Solana JSON keypair)")?;
    let recipient = Keypair::new().pubkey();
    let rent = rpc
        .call("getMinimumBalanceForRentExemption", json!([0]))
        .await
        .map_err(|e| e.to_string())?
        .as_u64()
        .ok_or("Invalid rent response")?;
    if rent > 10_000_000 {
        return Err("Rent requirement exceeds v1 transfer safety budget".into());
    }
    let amount = rent.max(1_000_000);
    let balance = rpc
        .call(
            "getBalance",
            json!([payer.pubkey().to_string(),{"commitment":"finalized"}]),
        )
        .await
        .map_err(|e| e.to_string())?["value"]
        .as_u64()
        .ok_or("Invalid balance response")?;
    if balance < amount + 10_000 {
        return Err(
            "Insufficient local/devnet funds; use the local faucet or free devnet faucet".into(),
        );
    }
    let (blockhash, height, slot) = rpc
        .latest(&config.submission.preflight_commitment)
        .await
        .map_err(|e| e.to_string())?;
    let mut instruction = system_instruction::transfer(&payer.pubkey(), &recipient, amount);
    // Valid transaction/signatures, invalid System Program instruction data. With
    // preflight enabled this is rejected; disabled it can land and fail execution.
    if !matches!(workload, Workload::Transfer) {
        instruction.data = vec![255];
    }
    let tx = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&payer.pubkey()),
        &[&payer],
        Hash::from_str(&blockhash).map_err(|_| "Invalid blockhash")?,
    );
    let bytes = bincode::serialize(&tx).map_err(|e| e.to_string())?;
    let signature = tx.signatures[0].to_string();
    Ok(Prepared {
        signature,
        wire: base64::engine::general_purpose::STANDARD.encode(&bytes),
        wire_hash: format!("{:x}", Sha256::digest(&bytes)),
        blockhash,
        last_valid_block_height: height,
        context_slot: slot,
        workload: json!({"kind":format!("{workload:?}"),"lamports":amount,"recipient":recipient.to_string(),"payer":payer.pubkey().to_string(),"format":"legacy","supported_transaction_version":0,"min_context_slot":slot}),
    })
}
