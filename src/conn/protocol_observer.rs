// Licensed under the Apache License, Version 2.0 or the MIT license, at your option.

//! Optional synchronous observation of MySQL logical packets and compressed transport frames.

use std::{
    fmt,
    io::{self, Read, Write},
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::io::Stream;

/// Process-local identity of an observed physical connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolConnectionId(u64);

/// Events produced while a connection exchanges MySQL protocol packets.
#[derive(Debug)]
#[non_exhaustive]
pub enum ProtocolEvent<'a> {
    /// A logical packet was sent. The size excludes transport headers.
    PacketSent { payload_len: usize },
    /// A logical packet was read after TLS decryption and decompression.
    ///
    /// The payload can contain authentication or application data.
    PacketReceived(&'a [u8]),
    /// The first byte of a command, recorded before the driver attempts to send it.
    CommandStarted(u8),
    /// The connection has negotiated protocol compression.
    CompressionEnabled,
    /// The compressed frame header was read. The size includes the seven-byte header.
    CompressedFrameStarted { wire_len: usize },
}

/// Receives synchronous, per-connection protocol events.
pub trait ProtocolObserver: Send + Sync + 'static {
    /// Observes an event from the given physical connection.
    fn observe(&self, connection: ProtocolConnectionId, event: ProtocolEvent<'_>);
}

#[derive(Clone)]
pub(crate) struct ObserverHandle(pub(crate) Arc<dyn ProtocolObserver>);

impl fmt::Debug for ObserverHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProtocolObserver")
    }
}

impl PartialEq for ObserverHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ObserverHandle {}

const COMPRESSED_HEADER_LEN: usize = 7;
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct ObservedStream {
    pub(crate) inner: Stream,
    observer: Option<(ProtocolConnectionId, ObserverHandle)>,
    compressed: bool,
    header: [u8; COMPRESSED_HEADER_LEN],
    header_offset: usize,
    payload_left: usize,
}

impl ObservedStream {
    pub(crate) fn new(inner: Stream, observer: Option<&ObserverHandle>) -> Self {
        let observer = observer.map(|observer| {
            let id = ProtocolConnectionId(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed));
            (id, observer.clone())
        });
        Self {
            inner,
            observer,
            compressed: false,
            header: [0; COMPRESSED_HEADER_LEN],
            header_offset: COMPRESSED_HEADER_LEN,
            payload_left: 0,
        }
    }

    pub(crate) fn observer_id(&self) -> Option<ProtocolConnectionId> {
        self.observer.as_ref().map(|(id, _)| *id)
    }

    pub(crate) fn observe(&self, event: ProtocolEvent<'_>) {
        if let Some((id, observer)) = &self.observer {
            observer.0.observe(*id, event);
        }
    }

    pub(crate) fn enable_compression(&mut self) {
        self.compressed = true;
        self.observe(ProtocolEvent::CompressionEnabled);
    }
}

impl Deref for ObservedStream {
    type Target = Stream;

    fn deref(&self) -> &Stream {
        &self.inner
    }
}

impl Read for ObservedStream {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.observer.is_none() || !self.compressed || output.is_empty() {
            return self.inner.read(output);
        }

        // Deliver at most one frame to the decoder at a time so an observer sees
        // the wire size of a frame when the decoder actually needs that frame.
        if self.header_offset == COMPRESSED_HEADER_LEN && self.payload_left == 0 {
            let bytes_read = self.inner.read(&mut self.header)?;
            if bytes_read == 0 {
                return Ok(0);
            }
            self.inner.read_exact(&mut self.header[bytes_read..])?;
            self.header_offset = 0;
            self.payload_left = usize::from(self.header[0])
                | usize::from(self.header[1]) << 8
                | usize::from(self.header[2]) << 16;
            self.observe(ProtocolEvent::CompressedFrameStarted {
                wire_len: COMPRESSED_HEADER_LEN + self.payload_left,
            });
        }

        if self.header_offset < COMPRESSED_HEADER_LEN {
            let available = &self.header[self.header_offset..];
            let len = available.len().min(output.len());
            output[..len].copy_from_slice(&available[..len]);
            self.header_offset += len;
            return Ok(len);
        }

        let len = self.payload_left.min(output.len());
        let bytes_read = self.inner.read(&mut output[..len])?;
        self.payload_left -= bytes_read;
        Ok(bytes_read)
    }
}

impl Write for ObservedStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
