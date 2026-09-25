//! A TCP proxy to the local server that can go silent, like a host that
//! lost power or a PostgreSQL whose processes are frozen: it stops passing
//! bytes both ways and never resets the connections. What the door sent is
//! accepted by the kernel and never answered.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub(super) struct Proxy {
    pub(super) port: u16,
    silent: Arc<AtomicBool>,
}

impl Proxy {
    pub(super) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = Arc::new(AtomicBool::new(false));

        let flag = silent.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect("127.0.0.1:5432").await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::spawn(Self::pass(client_read, server_write, flag.clone()));
                tokio::spawn(Self::pass(server_read, client_write, flag.clone()));
            }
        });

        Self { port, silent }
    }

    async fn pass(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        silent: Arc<AtomicBool>,
    ) {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            while silent.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let Ok(n) = from.read(&mut buf).await else {
                return;
            };
            if n == 0 {
                return;
            }
            // Bytes read just before going silent are lost with the host,
            // and the connection stays open: nothing ever resets it.
            if silent.load(Ordering::Relaxed) {
                continue;
            }
            if to.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    }

    pub(super) fn go_silent(&self) {
        self.silent.store(true, Ordering::Relaxed);
    }
}
