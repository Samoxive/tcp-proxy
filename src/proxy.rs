use crate::config::App;
use crate::Shutdown;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use tracing::{error, info};

const PROXY_BUFFER_SIZE: usize = 4096;
const TCP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UpstreamTarget {
    pub host: String,
    pub port: u16,
}

impl UpstreamTarget {
    fn new(host: &str, port: u16) -> Self {
        UpstreamTarget {
            host: host.into(),
            port,
        }
    }
}

pub struct Proxy {
    pub ports: Vec<u16>,
    pub upstream_targets: Vec<UpstreamTarget>,
}

#[derive(Debug)]
pub enum ProxyConstructorError {
    EmptyTarget,
    EmptyPort,
    InvalidPort(ParseIntError),
}

#[derive(Debug)]
pub enum ProxyRunError {
    Create(io::Error),
    Bind(io::Error),
    Listen(io::Error),
}

impl Proxy {
    pub fn from_app(app: App) -> Result<Proxy, ProxyConstructorError> {
        let mut targets = vec![];

        for target in &app.targets {
            let mut components = target.split(':').take(2);

            let host = components
                .next()
                .ok_or(ProxyConstructorError::EmptyTarget)?;

            let port = components
                .next()
                .ok_or(ProxyConstructorError::EmptyPort)?
                .parse::<u16>()
                .map_err(ProxyConstructorError::InvalidPort)?;

            targets.push(UpstreamTarget::new(host, port))
        }

        Ok(Proxy {
            ports: app.ports,
            upstream_targets: targets,
        })
    }

    // first, we must attempt listening local ports to sort out permission
    // port in use errors, then we must resolve host names we are given.
    // if they all fail to resolve we can return an error because they likely
    // won't work. if *some* of them fail we can start proxying with the upstreams
    // we can reach. we need to attempt connecting to them again though since
    // dns can update or services can start running later.
    pub async fn run(&self, shutdown: Shutdown) -> Result<(), ProxyRunError> {
        let mut listeners: Vec<TcpListener> = vec![];

        for port in self.ports.iter() {
            let socket = match TcpSocket::new_v4() {
                Ok(socket) => socket,
                Err(err) => {
                    error!("failed to create a new bind socket");
                    return Err(ProxyRunError::Create(err));
                }
            };

            let socket_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), *port);

            if let Err(err) = socket.bind(socket_address) {
                error!("failed to bind socket to address: {}", socket_address);
                return Err(ProxyRunError::Bind(err));
            };

            let listener = match socket.listen(32) {
                Ok(listener) => listener,
                Err(err) => {
                    error!("failed to listen to socket");
                    return Err(ProxyRunError::Listen(err));
                }
            };

            listeners.push(listener);
        }

        let (sender, receiver) = mpsc::channel(32);
        for listener in listeners {
            tokio::spawn(accept_connections(
                listener,
                sender.clone(),
                shutdown.clone(),
            ));
        }

        let mut fan_out = UpstreamFanOutWorker::new(&self.upstream_targets, receiver, shutdown);

        tokio::spawn(async move {
            fan_out.run().await;
        });

        Ok(())
    }
}

pub async fn accept_connections(
    listener: TcpListener,
    sender: Sender<TcpStream>,
    shutdown: Shutdown,
) -> Result<(), ()> {
    let mut shutdown_receiver = shutdown.subscribe();
    loop {
        info!(
            "listening to new connections to {:?}",
            listener.local_addr()
        );
        let socket: io::Result<(TcpStream, SocketAddr)> = tokio::select! {
            socket = listener.accept() => { socket }
            _ = shutdown_receiver.recv() => {
                return Ok(());
            }
        };

        match socket {
            Ok((socket, addr)) => {
                info!("accepted connection from {}", addr);
                match sender.send(socket).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("failed to send new connection to relay worker, no longer accepting connections for {:?} {}", listener.local_addr(), err);
                        return Err(());
                    }
                }
            }
            Err(err) => {
                error!(
                    "failed to accept new connection for {:?} {}",
                    listener.local_addr(),
                    err
                );
                return Err(());
            }
        }
    }
}

#[derive(Clone)]
pub struct AliveConnectionManager {
    connections: Arc<Mutex<HashMap<UpstreamTarget, u16>>>, // realistically we can't have more than u16 connections
}

impl AliveConnectionManager {
    fn new(upstream_targets: &[UpstreamTarget]) -> Self {
        let connections = Arc::new(Mutex::new(
            upstream_targets
                .iter()
                .map(|target| (target.clone(), 0))
                .collect::<HashMap<UpstreamTarget, u16>>(),
        ));

        Self { connections }
    }

    async fn get_targets(&self) -> Vec<(UpstreamTarget, u16)> {
        let mut connections = self
            .connections
            .lock()
            .await
            .iter()
            .map(|(target, port)| (target.clone(), *port))
            .collect::<Vec<(UpstreamTarget, u16)>>();

        connections.sort_by_key(|(_target, count)| *count);

        connections
    }

    async fn submit_connection(&self, target: UpstreamTarget) {
        let mut connections = self.connections.lock().await;

        connections.entry(target).and_modify(|count| *count += 1);
    }

    async fn drop_connection(&self, target: UpstreamTarget) {
        let mut connections = self.connections.lock().await;

        connections.entry(target).and_modify(|count| *count -= 1);
    }
}

pub struct UpstreamFanOutWorker {
    alive_connections: AliveConnectionManager,
    // upstream_ips: HashMap<String, Ipv4Addr>, we can use this when we resolve hostnames ourselves
    connection_receiver: Receiver<TcpStream>,
    shutdown: Shutdown,
}

impl UpstreamFanOutWorker {
    pub fn new(
        upstream_targets: &[UpstreamTarget],
        connection_receiver: Receiver<TcpStream>,
        shutdown: Shutdown,
    ) -> Self {
        UpstreamFanOutWorker {
            alive_connections: AliveConnectionManager::new(upstream_targets),
            connection_receiver,
            shutdown,
        }
    }

    pub async fn run(&mut self) {
        let mut shutdown_receiver = self.shutdown.subscribe();
        loop {
            let connection = tokio::select! {
                connection = self.connection_receiver.recv() => { connection }
                _ = shutdown_receiver.recv() => {
                    return;
                }
            };

            let new_connection = if let Some(connection) = connection {
                connection
            } else {
                error!("failed to receive new connection from socket listeners, no longer relaying new connections.");
                return;
            };

            // sort upstream targets by least connections alive
            // we *could* use round robin to distribute connections but that could be unfair
            // the best way could be distributing connections based on packets/bytes sent/received
            // but this will do for now.
            let targets = self.alive_connections.get_targets().await;

            let mut target_and_upstream_connection = None;
            for (target, _count) in targets {
                // we can use the &str parameter directly if we keep target string as host:port
                // this will use system's DNS resolver so it may cache the ip. we might want to
                // have our own resolver handle this so that we can pick up target server configuration
                // changes or load balancing based on DNS
                match TcpStream::connect((target.host.as_str(), target.port)).await {
                    Ok(connection) => {
                        // increment the connection count
                        target_and_upstream_connection = Some((target, connection));
                        break;
                    }
                    Err(err) => {
                        error!("failed to initiate connection with upstream target {:?} for new relay connection, trying others... {}", target, err);
                    }
                }
            }

            if let Some((target, upstream_connection)) = target_and_upstream_connection {
                // start proxying connection
                // pass connection manager so that it can be decremented when connection dies
                let connection_manager = self.alive_connections.clone();
                let shutdown = self.shutdown.clone();
                tokio::spawn(async move {
                    relay(
                        new_connection,
                        upstream_connection,
                        target,
                        shutdown,
                        connection_manager,
                    )
                    .await;
                });
            } else {
                // couldn't start any upstream connection, kill new connection
                drop(new_connection);
            }
        }
    }
}

pub async fn relay(
    connection: TcpStream,
    upstream_connection: TcpStream,
    target: UpstreamTarget,
    shutdown: Shutdown,
    connection_manager: AliveConnectionManager,
) {
    async fn relay_one_direction(
        mut read_stream: OwnedReadHalf,
        mut write_stream: OwnedWriteHalf,
        shutdown: Shutdown,
    ) {
        info!(
            "relaying {:?}->{:?}->{:?}",
            read_stream.peer_addr(),
            read_stream.local_addr(),
            write_stream.peer_addr()
        );
        let mut shutdown_receiver = shutdown.subscribe();
        let mut buffer = vec![0; PROXY_BUFFER_SIZE];

        loop {
            buffer.as_mut_slice().fill(0);

            let read_count = tokio::select! {
                timeout_result = timeout(TCP_TIMEOUT, read_stream.read(buffer.as_mut_slice())) => {
                    match timeout_result {
                        Ok(count_result) => {
                            match count_result {
                                Ok(count) => count,
                                Err(err) => {
                                    error!("can't read from tcp stream for {:?}->{:?}->{:?} relay {}", read_stream.peer_addr(), read_stream.local_addr(), write_stream.peer_addr(), err);
                                    return;
                                }
                            }
                        }
                        Err(_) => {
                            error!("reading from tcp stream for {:?}->{:?}->{:?} relay timed out", read_stream.peer_addr(), read_stream.local_addr(), write_stream.peer_addr());
                            return;
                        }
                    }
                }
                _ = shutdown_receiver.recv() => {
                    return;
                }
            };

            if read_count == 0 {
                info!(
                    "reached EOF while reading from tcp stream for {:?}->{:?}->{:?} relay",
                    read_stream.peer_addr(),
                    read_stream.local_addr(),
                    write_stream.peer_addr()
                );
                return;
            }

            let mut write_cursor = 0;
            while write_cursor < read_count {
                let write_count = tokio::select! {
                    timeout_result = timeout(TCP_TIMEOUT, write_stream.write(&buffer[write_cursor..read_count])) => {
                        match timeout_result {
                            Ok(count_result) => {
                                match count_result {
                                    Ok(count) => count,
                                    Err(err) => {
                                        error!("can't write to tcp stream for {:?}->{:?}->{:?} relay {}", read_stream.peer_addr(), read_stream.local_addr(), write_stream.peer_addr(), err);
                                        return;
                                    }
                                }
                            }
                            Err(_) => {
                                error!("writing to tcp stream for {:?}->{:?}->{:?} relay timed out", read_stream.peer_addr(), read_stream.local_addr(), write_stream.peer_addr());
                                return;
                            }
                        }
                    }
                    _ = shutdown_receiver.recv() => {
                        return;
                    }
                };

                if write_count == 0 {
                    info!(
                        "reached EOF while writing to tcp stream for {:?}->{:?}->{:?} relay",
                        read_stream.peer_addr(),
                        read_stream.local_addr(),
                        write_stream.peer_addr()
                    );
                    return;
                }

                write_cursor += write_count;
            }
        }
    }

    connection_manager.submit_connection(target.clone()).await;
    let (read_connection, write_connection) = connection.into_split();
    let (read_upstream_connection, write_upstream_connection) = upstream_connection.into_split();

    let upstream_task = tokio::spawn(relay_one_direction(
        read_connection,
        write_upstream_connection,
        shutdown.clone(),
    ));
    let downstream_task = tokio::spawn(relay_one_direction(
        read_upstream_connection,
        write_connection,
        shutdown.clone(),
    ));

    let _ = upstream_task.await;
    let _ = downstream_task.await;
    connection_manager.drop_connection(target).await;
}
