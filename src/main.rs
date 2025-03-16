use anyhow::Result;
use dirs::home_dir;
use futures::StreamExt;
use serde::Deserialize;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::*;

#[derive(Debug, Deserialize)]
struct Config {
    rpc_url: String,
    grpc_url: String,
    grpc_auth_token: String,
    sender_keypair_path: String,
    recipient_address: String,
    amount_sol: f64,
}

const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";

const GRPC_CONNECT_TIMEOUT_SECS: u64 = 30;
const GRPC_REQUEST_TIMEOUT_SECS: u64 = 30;
const GRPC_MESSAGE_SIZE_LIMIT: usize = 20 * 1024 * 1024; // 20MB

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

const CONFIG_FILE: &str = "config.yaml";

async fn create_block_subscription() -> HashMap<String, SubscribeRequestFilterBlocks> {
    let mut blocks = HashMap::new();
    blocks.insert(
        "blocks".to_owned(),
        SubscribeRequestFilterBlocks {
            account_include: vec![SOLEND_PROGRAM_ID.to_string()],
            include_transactions: None,
            include_accounts: None,
            include_entries: None,
        },
    );
    blocks
}

async fn create_and_send_transaction(
    rpc_client: &RpcClient,
    sender: &Keypair,
    recipient: &Pubkey,
    amount_sol: f64,
) -> Result<()> {
    let recent_blockhash = rpc_client.get_latest_blockhash().await?;
    let lamports = (amount_sol * LAMPORTS_PER_SOL) as u64;

    let instruction = system_instruction::transfer(&sender.pubkey(), recipient, lamports);

    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&sender.pubkey()),
        &[sender],
        recent_blockhash,
    );

    match rpc_client
        .send_transaction_with_config(
            &transaction,
            RpcSendTransactionConfig {
                skip_preflight: true,
                ..RpcSendTransactionConfig::default()
            },
        )
        .await
    {
        Ok(signature) => println!("Transaction sent: {}", signature),
        Err(e) => println!("Error sending transaction: {}", e),
    }
    Ok(())
}

async fn setup_grpc_client(config: &Config) -> Result<GeyserGrpcClient<impl Interceptor>> {
    GeyserGrpcClient::build_from_shared(config.grpc_url.clone())?
        .x_token(Some(config.grpc_auth_token.clone()))?
        .connect_timeout(Duration::from_secs(GRPC_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(GRPC_REQUEST_TIMEOUT_SECS))
        .max_decoding_message_size(GRPC_MESSAGE_SIZE_LIMIT)
        .max_encoding_message_size(GRPC_MESSAGE_SIZE_LIMIT)
        .connect()
        .await
        .map_err(Into::into)
}

async fn handle_block_update(
    block: yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
    rpc_client: &RpcClient,
    sender: &Keypair,
    recipient: &Pubkey,
    amount_sol: f64,
) -> Result<()> {
    if let UpdateOneof::Block(block_data) = block {
        println!("New block: {}", block_data.slot);
        create_and_send_transaction(rpc_client, sender, recipient, amount_sol).await
    } else {
        Ok(())
    }
}

async fn subscribe_and_process(
    client: &mut GeyserGrpcClient<impl Interceptor>,
    rpc_client: RpcClient,
    sender: &Keypair,
    recipient: &Pubkey,
    amount_sol: f64,
) -> Result<()> {
    let blocks = create_block_subscription().await;

    let request = SubscribeRequest {
        accounts: HashMap::default(),
        slots: HashMap::default(),
        transactions: HashMap::default(),
        blocks,
        blocks_meta: HashMap::default(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        entry: HashMap::default(),
        transactions_status: HashMap::default(),
    };

    let (_subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;
    println!("Starting to monitor blocks with Solend transactions...");

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if let Some(update) = msg.update_oneof {
                    handle_block_update(update, &rpc_client, sender, recipient, amount_sol).await?;
                }
            }
            Err(e) => {
                println!("Error receiving message: {:?}", e);
                break;
            }
        }
    }
    Ok(())
}

fn read_keypair(path: &str) -> Result<Keypair> {
    let expanded_path = if path.starts_with("~/") {
        let mut path_buf = PathBuf::from(home_dir().expect("Could not find home directory"));
        path_buf.push(&path[2..]);
        path_buf
    } else {
        PathBuf::from(path)
    };

    let keypair_bytes = fs::read_to_string(expanded_path)?;
    let keypair: Vec<u8> = serde_json::from_str(&keypair_bytes)?;
    Ok(Keypair::from_bytes(&keypair)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config: Config = serde_yaml::from_str(&fs::read_to_string(CONFIG_FILE)?)?;
    let rpc_client =
        RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::confirmed());
    let sender_keypair = read_keypair(&config.sender_keypair_path)?;
    let recipient = Pubkey::from_str(&config.recipient_address)?;

    let mut client = setup_grpc_client(&config).await?;

    subscribe_and_process(
        &mut client,
        rpc_client,
        &sender_keypair,
        &recipient,
        config.amount_sol,
    )
    .await
}
