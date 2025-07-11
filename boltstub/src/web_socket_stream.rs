use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use std::{io, iter, mem};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use pin_project::pin_project;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::net_actor::Connection;

const MAGIC_WS_STRING: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const PONG: &[u8] = b"\x0A\x00";

#[pin_project]
pub struct WebSocketStream<RW> {
    #[pin]
    conn: RW,
    read_state: Option<WsRead>,
    read_buf: VecDeque<u8>,
    write_buf: Vec<u8>,
    flush_state: Option<WsWrite>,
}

impl<RW: AsyncRead + AsyncWrite> WebSocketStream<RW> {
    pub async fn new(mut conn: RW) -> io::Result<Self> {
        Self::handshake(&mut conn).await?;
        Ok(Self {
            conn,
            read_state: None,
            read_buf: VecDeque::with_capacity(8 * 1024),
            write_buf: Vec::with_capacity(8 * 1024),
            flush_state: None,
        })
    }

    async fn handshake(conn: &mut RW) -> io::Result<()> {
        let mut conn: Pin<&mut RW> = pin!(conn);
        let mut buf = Vec::with_capacity(1024);
        while &buf[buf.len().saturating_sub(4)..] != b"\r\n\r\n" {
            let mut read_buf = [0u8; 1];
            conn.as_mut().read_exact(&mut read_buf).await?;
            buf.push(read_buf[0]);
        }
        let request = String::from_utf8(buf).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid UTF-8 in websocket handshake request",
            )
        })?;
        let mut ws_upgrade = false;
        let mut ws_key = None;
        for line in request.split("\r\n") {
            let Some((h_key, h_value)) = Self::extract_header(line) else {
                continue;
            };
            if h_key.eq_ignore_ascii_case("upgrade") && h_value.eq_ignore_ascii_case("websocket") {
                ws_upgrade = true;
            } else if h_key.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = Some(h_value)
            }
        }
        if !ws_upgrade {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing `Upgrade: websocket` header",
            ));
        }
        let Some(ws_key) = ws_key else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Missing `Sec-WebSocket-Key` header",
            ));
        };
        let ws_key = {
            let mut hasher = Sha1::new();
            hasher.update(format!("{ws_key}{MAGIC_WS_STRING}").as_bytes());
            BASE64_STANDARD.encode(hasher.finalize())
        };
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {ws_key}\r\n\
             \r\n"
        );
        conn.write_all(response.as_bytes()).await?;
        conn.flush().await
    }

    fn extract_header(line: &str) -> Option<(&str, &str)> {
        let line = line.trim();
        let mut parts = line.splitn(3, ':');
        let key = parts.next()?.trim();
        let value = parts.next()?.trim();
        if parts.next().is_some() {
            return None;
        }
        Some((key, value))
    }
}

impl<RW: AsyncRead + AsyncWrite> AsyncRead for WebSocketStream<RW> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        if !this.read_buf.is_empty() {
            let (s1, s2) = this.read_buf.as_slices();
            let len1 = s1.len().min(buf.remaining());
            let len2 = s2.len().min(buf.remaining().saturating_sub(len1));
            buf.put_slice(&s1[..len1]);
            buf.put_slice(&s2[..len2]);
            this.read_buf.drain(..len1 + len2);
            return Poll::Ready(Ok(()));
        }
        let read_state = this.read_state.get_or_insert_with(WsRead::new);
        match read_state.poll(cx, this.conn) {
            Poll::Ready(Ok(data)) => {
                if data.is_empty() {
                    this.read_state.take();
                    return Poll::Ready(Ok(()));
                }
                this.read_buf.extend(data);
                this.read_state.take();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<RW: AsyncRead + AsyncWrite> AsyncWrite for WebSocketStream<RW> {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        if this.flush_state.is_some() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Cannot write while flushing",
            )));
        }
        this.write_buf.extend(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        let flush_state = this
            .flush_state
            .get_or_insert_with(|| WsWrite::new(mem::take(this.write_buf)));
        match flush_state.poll(cx, this.conn) {
            Poll::Ready(Ok(())) => {
                this.flush_state.take();
                Poll::Ready(Ok(()))
            }
            res => res,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().conn.poll_shutdown(cx)
    }
}

impl<RW: Connection> Connection for WebSocketStream<RW> {
    fn addresses(&self) -> io::Result<(SocketAddr, SocketAddr)> {
        self.conn.addresses()
    }
}

enum WsRead {
    ReadingFrameStart {
        buf: [u8; 2],
        pos: usize,
        payload: Vec<u8>,
        payload_pos: usize,
    },
    ReadingShortFrameLen {
        buf: [u8; 2],
        pos: usize,
        fin: bool,
        opcode: u8,
        masked: bool,
        payload: Vec<u8>,
        payload_pos: usize,
    },
    ReadingLongFrameLen {
        buf: [u8; 8],
        pos: usize,
        fin: bool,
        opcode: u8,
        masked: bool,
        payload: Vec<u8>,
        payload_pos: usize,
    },
    ReadingMask {
        buf: [u8; 4],
        pos: usize,
        payload_len: usize,
        fin: bool,
        opcode: u8,
        payload: Vec<u8>,
        payload_pos: usize,
    },
    ReadingPayload {
        mask: Option<[u8; 4]>,
        payload: Vec<u8>,
        payload_pos: usize,
        payload_start: usize,
        fin: bool,
        opcode: u8,
    },
    AnswerPing {
        pos: usize,
        payload: Vec<u8>,
        payload_pos: usize,
    },
    Done(Vec<u8>),
}

impl WsRead {
    fn new() -> Self {
        WsRead::ReadingFrameStart {
            buf: [0; 2],
            pos: 0,
            payload: Vec::new(),
            payload_pos: 0,
        }
    }

    fn poll<'a>(
        &'a mut self,
        cx: &mut Context<'_>,
        mut rw: Pin<&mut (impl AsyncRead + AsyncWrite)>,
    ) -> Poll<io::Result<&'a [u8]>> {
        loop {
            match self {
                WsRead::ReadingFrameStart {
                    buf,
                    pos,
                    payload,
                    payload_pos,
                } => match read_into_buffer(cx, rw.as_mut(), buf, pos) {
                    ReadResult::Return(poll) => return poll.map_ok(|()| [].as_slice()),
                    ReadResult::Done => {
                        let fin = buf[0] & 0b1000_0000 != 0;
                        let rsv1 = buf[0] & 0b0100_0000 != 0;
                        let rsv2 = buf[0] & 0b0010_0000 != 0;
                        let rsv3 = buf[0] & 0b0001_0000 != 0;
                        if rsv1 || rsv2 || rsv3 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Reserved bits are set",
                            )));
                        }
                        let opcode = buf[0] & 0b0000_1111;
                        let payload_len_marker = buf[1] & 0b0111_1111;
                        let masked = buf[1] & 0b1000_0000 != 0;
                        match payload_len_marker {
                            126 => {
                                *self = WsRead::ReadingShortFrameLen {
                                    buf: [0; 2],
                                    pos: 0,
                                    fin,
                                    opcode,
                                    masked,
                                    payload: mem::take(payload),
                                    payload_pos: *payload_pos,
                                };
                            }
                            127 => {
                                *self = WsRead::ReadingLongFrameLen {
                                    buf: [0; 8],
                                    pos: 0,
                                    fin,
                                    opcode,
                                    masked,
                                    payload: mem::take(payload),
                                    payload_pos: *payload_pos,
                                };
                            }
                            len => match masked {
                                true => {
                                    *self = WsRead::ReadingMask {
                                        buf: [0; 4],
                                        pos: 0,
                                        payload_len: len.into(),
                                        fin,
                                        opcode,
                                        payload: mem::take(payload),
                                        payload_pos: *payload_pos,
                                    };
                                }
                                false => {
                                    let mut payload = mem::take(payload);
                                    payload.extend(iter::repeat_n(0, len.into()));
                                    *self = WsRead::ReadingPayload {
                                        mask: None,
                                        payload,
                                        payload_pos: *payload_pos,
                                        payload_start: *payload_pos,
                                        fin,
                                        opcode,
                                    };
                                }
                            },
                        };
                    }
                },
                WsRead::ReadingShortFrameLen {
                    buf,
                    pos,
                    fin,
                    opcode,
                    masked,
                    payload,
                    payload_pos,
                } => match read_into_buffer(cx, rw.as_mut(), buf, pos) {
                    ReadResult::Return(poll) => return poll.map_ok(|()| [].as_slice()),
                    ReadResult::Done => match masked {
                        true => {
                            *self = WsRead::ReadingMask {
                                buf: [0; 4],
                                pos: 0,
                                payload_len: u16::from_be_bytes(*buf).into(),
                                fin: *fin,
                                opcode: *opcode,
                                payload: mem::take(payload),
                                payload_pos: *payload_pos,
                            };
                        }
                        false => {
                            let mut payload = mem::take(payload);
                            payload.extend(iter::repeat_n(0, u16::from_be_bytes(*buf).into()));
                            *self = WsRead::ReadingPayload {
                                mask: None,
                                payload,
                                payload_pos: *payload_pos,
                                payload_start: *payload_pos,
                                fin: *fin,
                                opcode: *opcode,
                            };
                        }
                    },
                },
                WsRead::ReadingLongFrameLen {
                    buf,
                    pos,
                    fin,
                    opcode,
                    masked,
                    payload,
                    payload_pos,
                } => match read_into_buffer(cx, rw.as_mut(), buf, pos) {
                    ReadResult::Return(poll) => return poll.map_ok(|()| [].as_slice()),
                    ReadResult::Done => match masked {
                        true => {
                            *self = WsRead::ReadingMask {
                                buf: [0; 4],
                                pos: 0,
                                payload_len: usize::from_be_bytes(*buf),
                                fin: *fin,
                                opcode: *opcode,
                                payload: mem::take(payload),
                                payload_pos: *payload_pos,
                            };
                        }
                        false => {
                            let mut payload = mem::take(payload);
                            payload.extend(iter::repeat_n(0, usize::from_be_bytes(*buf)));
                            *self = WsRead::ReadingPayload {
                                mask: None,
                                payload,
                                payload_pos: *payload_pos,
                                payload_start: *payload_pos,
                                fin: *fin,
                                opcode: *opcode,
                            };
                        }
                    },
                },
                WsRead::ReadingMask {
                    buf,
                    payload_len,
                    pos,
                    fin,
                    opcode,
                    payload,
                    payload_pos,
                } => match read_into_buffer(cx, rw.as_mut(), buf, pos) {
                    ReadResult::Return(poll) => return poll.map_ok(|()| [].as_slice()),
                    ReadResult::Done => {
                        let mut payload = mem::take(payload);
                        payload.extend(iter::repeat_n(0, *payload_len));
                        *self = WsRead::ReadingPayload {
                            mask: Some(*buf),
                            payload,
                            payload_pos: *payload_pos,
                            payload_start: *payload_pos,
                            fin: *fin,
                            opcode: *opcode,
                        };
                    }
                },
                WsRead::ReadingPayload {
                    mask,
                    payload,
                    payload_pos,
                    payload_start,
                    fin,
                    opcode,
                } => match read_into_buffer(cx, rw.as_mut(), payload, payload_pos) {
                    ReadResult::Return(poll) => return poll.map_ok(|()| [].as_slice()),
                    ReadResult::Done => {
                        if let Some(mask) = mask.take() {
                            payload[*payload_start..].iter_mut().enumerate().for_each(
                                |(i, byte)| {
                                    *byte ^= mask[i % 4];
                                },
                            );
                        }
                        if *opcode & 0b0000_1000 != 0 {
                            // PING
                            *self = WsRead::AnswerPing {
                                pos: 0,
                                payload: mem::take(payload),
                                payload_pos: *payload_pos,
                            };
                            continue;
                        }
                        match *fin {
                            true => {
                                *self = WsRead::Done(mem::take(payload));
                                continue;
                            }
                            false => {
                                // also read continuation frame(s)
                                *self = WsRead::ReadingFrameStart {
                                    buf: [0; 2],
                                    pos: 0,
                                    payload: mem::take(payload),
                                    payload_pos: *payload_pos,
                                };
                            }
                        }
                    }
                },
                WsRead::AnswerPing {
                    pos,
                    payload,
                    payload_pos,
                } => {
                    match write_buffer(cx, rw.as_mut(), PONG, pos) {
                        WriteResult::Return(poll) => return poll.map(Err),
                        WriteResult::Done => {
                            // After sending PONG, read the next frame
                            *self = WsRead::ReadingFrameStart {
                                buf: [0; 2],
                                pos: 0,
                                payload: mem::take(payload),
                                payload_pos: *payload_pos,
                            };
                        }
                    }
                }
                WsRead::Done(data) => {
                    return Poll::Ready(Ok(data));
                }
            }
        }
    }
}

enum WsWrite {
    WritingHeader {
        header: Vec<u8>,
        payload: Vec<u8>,
        pos: usize,
    },
    WritingPayload {
        payload: Vec<u8>,
        pos: usize,
    },
    Flushing,
}

impl WsWrite {
    fn new(payload: Vec<u8>) -> Self {
        WsWrite::WritingHeader {
            header: Self::ws_frame_header(&payload),
            payload,
            pos: 0,
        }
    }

    fn ws_frame_header(payload: &[u8]) -> Vec<u8> {
        let payload_len = payload.len();
        let mut frame_header = Vec::with_capacity(9);
        frame_header.push(0b1000_0010);
        if payload_len < 126 {
            frame_header.push(payload_len as u8);
        } else if payload_len < 0x10000 {
            frame_header.push(126);
            frame_header.extend_from_slice(&(payload_len as u16).to_be_bytes());
        } else {
            frame_header.push(127);
            frame_header.extend_from_slice(&(payload_len as u64).to_be_bytes());
        }
        frame_header
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        mut rw: Pin<&mut impl AsyncWrite>,
    ) -> Poll<io::Result<()>> {
        loop {
            match self {
                WsWrite::WritingHeader {
                    header,
                    payload,
                    pos,
                } => match write_buffer(cx, rw.as_mut(), header, pos) {
                    WriteResult::Return(poll) => return poll.map(Err),
                    WriteResult::Done => {
                        *self = WsWrite::WritingPayload {
                            payload: mem::take(payload),
                            pos: 0,
                        };
                    }
                },
                WsWrite::WritingPayload { payload, pos } => {
                    match write_buffer(cx, rw.as_mut(), payload, pos) {
                        WriteResult::Return(poll) => return poll.map(Err),
                        WriteResult::Done => {
                            *self = WsWrite::Flushing;
                        }
                    }
                }
                WsWrite::Flushing => return rw.as_mut().poll_flush(cx),
            }
        }
    }
}

enum ReadResult {
    Return(Poll<io::Result<()>>),
    Done,
}

fn read_into_buffer(
    cx: &mut Context<'_>,
    rw: Pin<&mut impl AsyncRead>,
    buf: &mut [u8],
    pos: &mut usize,
) -> ReadResult {
    if *pos >= buf.len() {
        return ReadResult::Done;
    }
    let mut read_buf = ReadBuf::new(&mut buf[*pos..]);
    match rw.poll_read(cx, &mut read_buf) {
        Poll::Ready(Ok(())) => {
            if read_buf.filled().is_empty() {
                return ReadResult::Return(Poll::Ready(Ok(())));
            }
            *pos += read_buf.filled().len();
        }
        Poll::Ready(Err(err)) => return ReadResult::Return(Poll::Ready(Err(err))),
        Poll::Pending => return ReadResult::Return(Poll::Pending),
    }
    if *pos < buf.len() {
        cx.waker().wake_by_ref();
        return ReadResult::Return(Poll::Pending);
    }
    ReadResult::Done
}

enum WriteResult {
    Return(Poll<io::Error>),
    Done,
}

fn write_buffer(
    cx: &mut Context<'_>,
    mut rw: Pin<&mut impl AsyncWrite>,
    buf: &[u8],
    pos: &mut usize,
) -> WriteResult {
    fn flush(cx: &mut Context<'_>, rw: Pin<&mut impl AsyncWrite>) -> WriteResult {
        match rw.poll_flush(cx) {
            Poll::Ready(Ok(())) => WriteResult::Done,
            Poll::Ready(Err(err)) => WriteResult::Return(Poll::Ready(err)),
            Poll::Pending => WriteResult::Return(Poll::Pending),
        }
    }

    if *pos >= buf.len() {
        return flush(cx, rw.as_mut());
    }
    match rw.as_mut().poll_write(cx, &buf[*pos..]) {
        Poll::Ready(Ok(written)) => {
            *pos += written;
        }
        Poll::Ready(Err(err)) => return WriteResult::Return(Poll::Ready(err)),
        Poll::Pending => return WriteResult::Return(Poll::Pending),
    }
    if *pos >= buf.len() {
        flush(cx, rw.as_mut())
    } else {
        cx.waker().wake_by_ref();
        WriteResult::Return(Poll::Pending)
    }
}
