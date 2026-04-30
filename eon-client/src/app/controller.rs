use crate::{
    app::{repl::*, state::AppStateHandle},
    net::network::{Client, KadPeerData, KadRequest, KadResponse},
};
use anyhow::Result;
use base64::prelude::*;
use futures::{prelude::*, stream::FuturesUnordered, StreamExt};
use libp2p::{
    core::Multiaddr,
    identity::{self, Keypair, PeerId},
    multiaddr::Protocol, request_response::ResponseChannel,
};
use objects::{prelude::*, system};
use serde::Deserialize;
use std::{
    collections::HashSet,
    error::Error,
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex}, time::Duration,
};
use tokio::{sync::{mpsc, oneshot}, task::spawn};
use tracing::{event, info, debug, Level};

pub enum AppStatus {
    Running,
    Done,
}

struct AppController {
    state: AppStateHandle,
    network_client: Client,
    rx: mpsc::Receiver<(Command, oneshot::Sender<()>)>,
}

impl AppController {
    async fn run(&mut self) {
        loop {
            tokio::select! {
                //Some(event) = self.network_events.next() => self.handle_event(event).await.unwrap(),
                Ok(_) = self.network_client.on_identify_received() => { },
                Ok(_) = self.network_client.on_new_listen_addr() => { },
                Ok((rpc, channel)) = self.network_client.on_object_request() => {
                    event!(Level::INFO, "Responding to request.");

                    if let Some(objects) = self.state.rpc(rpc).await {
                        self.network_client.respond_rpc(objects, channel).await;
                    }
                }
                Ok((peer, request, channel)) = self.network_client.on_fastkad_request() => {
                    self.handle_fastkad_request(peer, request, channel).await;
                }
                Ok(serialized) = self.network_client.on_store_request() => {
                    event!(Level::INFO, "Got store request.");
                    self.handle_store_request(serialized).await;
                }
                Some((cmd, sender)) = self.rx.recv() => { self.handle_command(cmd, sender).await.unwrap(); }
            }
        }
    }

    async fn handle_fastkad_request(&self, peer: PeerId, request: KadRequest, channel: ResponseChannel<KadResponse>) {
        let closer_peers = self.network_client.find_closest_local_peers(request.id, peer).await;
        let provider_peers = self.network_client.find_providers(request.id).await;
        let shortcut_peers = self.state.get_providers(request.id).await;

        let response = KadResponse {
            closer_peers: HashSet::from_iter(closer_peers.into_iter()),
            provider_peers: HashSet::from_iter(provider_peers.into_iter()),
            shortcut_peers: HashSet::from_iter(shortcut_peers.into_iter())
        };

        self.network_client.respond_fastkad_rpc(response, channel).await;
    }

    async fn handle_store_request(&mut self, data: Vec<u8>) {
        let obj = system::deserialize::<SignedObject>(&data);
        let id = obj.get_object_id();
        self.state.add(obj).await;
        self.network_client.start_providing(id).await;
    }

    async fn get_first_fastkad_response(
        &self,
        obj: ObjectId,
        peers: HashSet<KadPeerData>,
    ) -> Option<(PeerId, KadResponse)> {
        let mut response_futures = peers.into_iter().map(|peer| {
            async move {
                let response = self
                    .network_client
                    .send_fastkad_rpc(peer.clone(), KadRequest { id: obj })
                    .await?;

                Result::<(PeerId, KadResponse), Box<dyn Error + Send + Sync>>::Ok((peer.id, response))
            }
            .boxed()
        }).peekable();
        
        if response_futures.peek().is_some() {
            futures::future::select_ok(response_futures)
                .await
                .ok()
                .map(|res| res.0)
        }
        else {
            None
        }
    }

    async fn drive_query_for_single_peer(
        &self,
        obj: ObjectId,
        peer: KadPeerData,
        visited_peers: Arc<Mutex<HashSet<PeerId>>>,
        parallel_factor: usize
    ) -> Option<HashSet<KadPeerData>> {
        let mut peers_to_ask = HashSet::from([peer]);

        let mut i = 0;
        while let Some((peer_id, out)) = self.get_first_fastkad_response(obj, peers_to_ask).await {
            i = i + 1;
            
            peers_to_ask = {
                let mut visited = visited_peers.lock().unwrap();
                let result = out.closer_peers
                    .into_iter()
                    .filter(|x| visited.contains(&x.id))
                    .take(parallel_factor)
                    .collect();

                visited.insert(peer_id);

                result
            };

            if !out.shortcut_peers.is_empty() {
                info!("Found shortcut after asking {i} times");
                return Some(out.shortcut_peers);
            }
            if !out.provider_peers.is_empty() {
                info!("Found provider after asking {i} times");
                return Some(out.provider_peers);
            }
        }

        None
    }

    async fn get_providers(
        &mut self,
        obj_id: ObjectId,
    ) -> Result<HashSet<KadPeerData>, Box<dyn Error + Send + Sync>> {
        const PARALLEL_FACTOR: usize = 3;

        let my_id = self.network_client.get_peer_id();
        let visited_peers = Arc::new(Mutex::new(HashSet::new()));
        let out = self
            .network_client
            .find_closest_local_peers(obj_id, my_id)
            .await
            .into_iter()
            .take(PARALLEL_FACTOR)
            .map(|peer| {
                self.drive_query_for_single_peer(obj_id, peer, visited_peers.clone(), PARALLEL_FACTOR)
            })
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
            .ok_or("None of the queries finished")?
            .ok_or("No providers found")?;

        self.state.store_providers(obj_id, Vec::from_iter(out.clone().into_iter()));

        Ok(out)
    }

    async fn handle_command(
        &mut self,
        cmd: Command,
        sender: oneshot::Sender<()>
    ) -> Result<AppStatus, Box<dyn Error + Send + Sync>> {
        let event: Option<AppStatus> = match cmd {
            Command::Provide { path } => {
                let file = BinaryFile::new(&path);
                let serialized = file
                    .make_typed()
                    .sign(self.network_client.get_keys())
                    .unwrap();
                let obj_id = serialized.get_object_id();

                info!("Providing: {}", BASE64_STANDARD.encode(&obj_id));

                self.state.add(serialized).await;
                self.network_client.start_providing(obj_id).await;

                None
            }
            Command::Publish { path } => {
                let file = BinaryFile::new(&path);
                let obj = file
                    .make_typed()
                    .sign(self.network_client.get_keys())
                    .unwrap();

                let id = obj.get_object_id();
                let data = obj.serialize();

                self.network_client.publish(id, data).await;

                info!("Published: {}", BASE64_STANDARD.encode(&id));

                None
            }
            Command::Get { id } => {
                let id: ObjectId = id.into();
                let providers = self.get_providers(id.clone()).await?;
                if providers.is_empty() {
                    return Err(format!("Could not find provider for file {id:?}.").into());
                }

                let requests = providers.into_iter().map(|p| {
                    event!(Level::INFO, "Found provider: {p:?}");
                    let mut network_client = self.network_client.clone();

                    let rpc = GetObject::new(id).make_typed();
                    async move { network_client.send_rpc(p, rpc).await }.boxed()
                });

                let results = futures::future::select_ok(requests)
                    .await
                    .map_err(|_| "None of the providers returned file.")?
                    .0;

                let file = results
                    .into_iter()
                    .filter(|file| file.get_object_id() == id)
                    .next()
                    .unwrap();
                let file = system::deserialize::<BinaryFile>(&file.get_data());

                info!("Got file: {}", file.filename);

                None
            }
            Command::Wait { time } => {
                tokio::time::sleep(*time).await;
                None
            }
            Command::WaitRandom { time } => {
                let time = rand::random_range(Duration::ZERO..*time);
                tokio::time::sleep(time).await;
                None
            }
            Command::Quit => Some(AppStatus::Done),
        };

        let _ = sender.send(());
        Ok(event.unwrap_or(AppStatus::Running))
    }
}

pub struct AppControllerHandle {
    tx: mpsc::Sender<(Command, oneshot::Sender<()>)>,
}

impl AppControllerHandle {
    pub fn new(network_client: Client) -> Self {
        let (tx, rx) = mpsc::channel(32);

        let mut mgr = AppController {
            state: AppStateHandle::new(),
            network_client,
            rx,
        };

        spawn(async move {
            mgr.run().await;
        });

        Self { tx }
    }

    pub async fn send(&mut self, cmd: Command) {
        // TODO - refactor this!
        // This can't be handled here in the long term
        if let Command::Wait { time } = cmd {
            tokio::time::sleep(*time).await;
        }
        else {
            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send((cmd, tx)).await;
            let _ = rx.await;
        }
    }
}
