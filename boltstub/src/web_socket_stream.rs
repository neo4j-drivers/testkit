use crate::net_actor::Connection;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct WebSocketStream<RW> {
    conn: RW,
}

impl<RW: AsyncRead + AsyncWrite> WebSocketStream<RW> {
    pub fn new(conn: RW) -> Self {
        Self { conn }
    }
}

impl<RW: AsyncRead + AsyncWrite> AsyncRead for WebSocketStream<RW> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        todo!()
    }
}

impl<RW: AsyncRead + AsyncWrite> AsyncWrite for WebSocketStream<RW> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        todo!()
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        todo!()
    }
}

impl<RW: Connection> Connection for WebSocketStream<RW> {
    fn addresses(&self) -> io::Result<(SocketAddr, SocketAddr)> {
        self.conn.addresses()
    }
}
