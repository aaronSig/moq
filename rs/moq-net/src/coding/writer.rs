use std::fmt::Debug;

use crate::{Error, coding::*, ietf};

/// A wrapper around a [web_transport_trait::SendStream] that will reset on Drop.
pub struct Writer<S: web_transport_trait::SendStream, V> {
	stream: Option<S>,
	buffer: bytes::BytesMut,
	version: V,
}

impl<S: web_transport_trait::SendStream, V> Writer<S, V> {
	/// Create a new writer for the given stream and version.
	pub fn new(stream: S, version: V) -> Self {
		Self {
			stream: Some(stream),
			buffer: Default::default(),
			version,
		}
	}

	/// Encode the given message to the stream.
	pub async fn encode<T: Encode<V> + Debug>(&mut self, msg: &T) -> Result<(), Error>
	where
		V: Clone,
	{
		self.buffer.clear();
		msg.encode(&mut self.buffer, self.version.clone())?;

		while !self.buffer.is_empty() {
			self.stream
				.as_mut()
				.unwrap()
				.write_buf(&mut self.buffer)
				.await
				.map_err(Error::from_transport)?;
		}

		Ok(())
	}

	pub(crate) async fn write<Buf: bytes::Buf + Send>(&mut self, buf: &mut Buf) -> Result<usize, Error> {
		self.stream
			.as_mut()
			.unwrap()
			.write_buf(buf)
			.await
			.map_err(Error::from_transport)
	}

	/// Write the entire `Buf` to the stream.
	///
	/// NOTE: This can avoid performing a copy when using `Bytes`.
	pub async fn write_all<Buf: bytes::Buf + Send>(&mut self, buf: &mut Buf) -> Result<(), Error> {
		while buf.has_remaining() {
			self.write(buf).await?;
		}
		Ok(())
	}

	/// Write the entire [`bytes::Bytes`] chunk to the stream.
	pub async fn write_chunk(&mut self, chunk: bytes::Bytes) -> Result<(), Error> {
		self.stream
			.as_mut()
			.unwrap()
			.write_chunk(chunk)
			.await
			.map_err(Error::from_transport)
	}

	/// Mark the stream as finished.
	pub fn finish(&mut self) -> Result<(), Error> {
		self.stream.as_mut().unwrap().finish().map_err(Error::from_transport)
	}

	/// Abort the stream with the given error.
	///
	/// Consumes the writer: a later write won't compile, and the [`Drop`] fallback can't
	/// reset a second time and overwrite the reason with a plain [`Error::Cancel`].
	pub fn abort(mut self, err: &Error) {
		if let Some(mut stream) = self.stream.take() {
			stream.reset(err.to_code());
		}
	}

	/// Finish the stream and wait for the peer to acknowledge everything written.
	///
	/// [`Self::finish`] alone is not enough to deliver a final message. A stream that has sent
	/// its FIN is still retransmitting unacknowledged data, and a RESET_STREAM from that state
	/// discards it, so the [`Drop`] fallback below can throw away bytes the peer never read.
	/// Keep the Drop fallback armed while awaiting acknowledgement: cancelling an
	/// obsolete subscription must still reset its queued reliable bytes. Only a
	/// successful acknowledgement disarms it, so normal completion never resets.
	pub async fn close(mut self) -> Result<(), Error> {
		let Some(stream) = self.stream.as_mut() else {
			return Ok(());
		};

		stream.finish().map_err(Error::from_transport)?;
		stream.closed().await.map_err(Error::from_transport)?;
		self.stream.take();

		Ok(())
	}

	/// Wait for the stream to be closed, or the [Self::finish] to be acknowledged by the peer.
	pub async fn closed(&mut self) -> Result<(), Error> {
		self.stream
			.as_mut()
			.unwrap()
			.closed()
			.await
			.map_err(Error::from_transport)?;
		Ok(())
	}

	/// Set the stream's send order: streams with HIGHER values are transmitted first.
	///
	/// This is the transport trait's convention (matching W3C `sendOrder` and quinn's
	/// scheduler) and the model's [`Subscription::priority`](crate::track::Subscription),
	/// where higher values preempt lower ones. The lite priority queue's rank is the
	/// opposite (0 = most urgent); rank holders convert via `PriorityHandle::send_order`.
	pub fn set_priority(&mut self, send_order: u8) {
		self.stream.as_mut().unwrap().set_priority(send_order);
	}

	/// Cast the writer to a different version, used during version negotiation.
	pub fn with_version<O>(mut self, version: O) -> Writer<S, O> {
		Writer {
			// We need to use an Option so Drop doesn't reset the stream.
			stream: self.stream.take(),
			buffer: std::mem::take(&mut self.buffer),
			version,
		}
	}
}

impl<S: web_transport_trait::SendStream> Writer<S, ietf::Version> {
	/// Encode an IETF `Message` to the stream, writing `[type_id][size][body]`.
	pub async fn encode_message<T: ietf::Message>(&mut self, msg: &T) -> Result<(), Error> {
		self.encode(&T::ID).await?;
		self.encode(msg).await
	}
}

impl<S: web_transport_trait::SendStream, V> Drop for Writer<S, V> {
	fn drop(&mut self) {
		if let Some(mut stream) = self.stream.take() {
			// Unlike the Quinn default, we abort the stream on drop.
			stream.reset(Error::Cancel.to_code());
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::lite::test_transport::{Log, SinkSend};

	struct AckGatedSend {
		inner: SinkSend,
		ack: kio::Consumer<bool>,
	}

	impl web_transport_trait::SendStream for AckGatedSend {
		type Error = crate::lite::test_transport::SinkError;
		async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
			web_transport_trait::SendStream::write(&mut self.inner, buf).await
		}
		fn set_priority(&mut self, order: u8) {
			web_transport_trait::SendStream::set_priority(&mut self.inner, order);
		}
		fn finish(&mut self) -> Result<(), Self::Error> {
			web_transport_trait::SendStream::finish(&mut self.inner)
		}
		fn reset(&mut self, code: u32) {
			web_transport_trait::SendStream::reset(&mut self.inner, code);
		}
		async fn closed(&mut self) -> Result<(), Self::Error> {
			self.ack
				.wait(|ack| {
					if **ack {
						std::task::Poll::Ready(())
					} else {
						std::task::Poll::Pending
					}
				})
				.await
				.map_err(|_| crate::lite::test_transport::SinkError)?;
			web_transport_trait::SendStream::closed(&mut self.inner).await
		}
	}

	#[tokio::test]
	async fn cancel_close_before_ack_resets_queued_data() {
		let log = Log::default();
		let ack = kio::Producer::new(false);
		let send = AckGatedSend {
			inner: SinkSend::new(log.clone()),
			ack: ack.consume(),
		};
		let mut writer = Writer::new(send, crate::lite::Version::Lite05);
		writer
			.write_chunk(bytes::Bytes::from_static(b"obsolete video"))
			.await
			.unwrap();
		let mut close = Box::pin(writer.close());
		assert!(futures::poll!(close.as_mut()).is_pending());
		drop(close);
		assert_eq!(
			log.resets(),
			vec![Error::Cancel.to_code()],
			"cancelled FIN wait left obsolete bytes queued"
		);
	}

	#[tokio::test]
	async fn close_after_delayed_ack_never_resets() {
		let log = Log::default();
		let ack = kio::Producer::new(false);
		let send = AckGatedSend {
			inner: SinkSend::new(log.clone()),
			ack: ack.consume(),
		};
		let mut writer = Writer::new(send, crate::lite::Version::Lite05);
		writer
			.write_chunk(bytes::Bytes::from_static(b"complete video"))
			.await
			.unwrap();
		let mut close = Box::pin(writer.close());
		assert!(futures::poll!(close.as_mut()).is_pending());
		*ack.write().ok().unwrap() = true;
		close.await.unwrap();
		assert!(log.resets().is_empty());
		assert_eq!(*log.writes.lock().unwrap(), b"complete video");
	}

	#[test]
	fn set_priority_forwards_send_order() {
		let log = Log::default();
		let mut writer = Writer::new(SinkSend::new(log.clone()), crate::lite::Version::Lite05);

		for send_order in 0u8..=255 {
			writer.set_priority(send_order);
		}

		assert_eq!(log.priorities(), (0u8..=255).collect::<Vec<_>>());
	}
}
