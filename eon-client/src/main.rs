#![allow(dead_code, unused)]
mod app;
mod net;

use std::{error::Error, fs::File, io::Write, net::Ipv4Addr, path::{Path, PathBuf}, str::FromStr, time::Duration};

use app::repl::Sequence;
use clap::Parser;
use futures::{prelude::*, StreamExt};
use libp2p::{core::Multiaddr, multiaddr::Protocol, identity::{Keypair, self}};
use tokio::task::spawn;
use tracing_subscriber::EnvFilter;
use tracing_appender::{non_blocking, non_blocking::WorkerGuard};
use tracing::{Level, event, info};
use anyhow::Result;

use crate::{net::network, app::cli::AppCli};

use serde::{Serialize, Deserialize};
use serde_with::{base64::Base64, serde_as};

fn init_tracing(output: impl Write + Send + 'static, level: Level) -> Result<WorkerGuard> {
    let (non_blocking, guard) = non_blocking(output);

    let env_filter = EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(env_filter)
        .init();

    Ok(guard)
}

fn init_file_tracing(name: &str, level: Level) -> Result<WorkerGuard> {
    let file = File::create(format!("{name}.log"))?;
    init_tracing(file, level)
}

fn init_stdout_tracing(level: Level) -> Result<WorkerGuard> {
    init_tracing(std::io::stdout(), level)
}

fn get_keypair(secret_key_seed: Option<u8>) -> Keypair {
    match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        }
        None => identity::Keypair::generate_ed25519(),
    }
}

fn get_commands(path: &Path) -> Result<Sequence, Box<dyn Error + Send + Sync>> { 
    let script = std::fs::read_to_string(path)?;
    Ok(Sequence::from_str(&script)?)
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let opt = Opt::parse();

    let keypair = get_keypair(opt.secret_key_seed);
    let peer_id = keypair.public().to_peer_id();

    let log_level = opt.log_level.unwrap_or(Level::INFO);
    let _guard = if opt.stdout {
        init_stdout_tracing(log_level)?
    }
    else {
        init_file_tracing(&peer_id.to_base58(), log_level)?
    };

    info!("My id: {peer_id}");

    let network_client =
        network::new(keypair, opt.bootstrap_mode).await?;

    if !opt.bootstrap_mode {
        network_client.on_new_listen_addr().await?;

        let bootstrap_addr = opt.bootstrap_addr
            .unwrap_or(Ipv4Addr::new(127, 0, 0, 1));

        while let Err(_) = network_client.bootstrap(bootstrap_addr).await {
            event!(Level::ERROR, "Bootstrap fail!");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        
        event!(Level::INFO, "Bootstrap complete");
    }

    let mut app = AppCli::new(network_client);

    if let Some(commands) = opt.script {
        app.execute(commands).await;
    }
    else if let Some(script_path) = opt.script_path {
        let commands = get_commands(&script_path)?;
        app.execute(commands).await;
    }
    else {
        app.run().await?;
    }

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "libp2p file sharing example")]
struct Opt {
    #[clap(long)]
    log_level: Option<Level>,
    
    /// Fixed value to generate deterministic peer ID.
    #[clap(long)]
    secret_key_seed: Option<u8>,

    #[clap(long, action)]
    bootstrap_mode: bool,

    #[clap(long)]
    bootstrap_addr: Option<Ipv4Addr>,

    #[clap(long)]
    stdout: bool,

    #[clap(long)]
    script: Option<Sequence>,

    #[clap(long)]
    script_path: Option<PathBuf>,
}
