use std::io::{self, BufWriter, Write};

use hubuum_budget::{ByteLimit, LimitedWriter};
use tokio::sync::mpsc;

use crate::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactResult, ArtifactStore, ArtifactWriter,
    VerifiedArtifact, verify,
};

/// Run a synchronous producer on a blocking worker and independently verify its
/// staged bytes. The producer must stream its input: this bounds the transport,
/// not allocations inside the callback. Nothing is published by this operation.
/// Requires a Tokio runtime; callers own admission and concurrency limits.
///
/// The channel holds one block. The producer and consumer each retain bounded
/// buffers, independent of the artifact's length. Sink errors are sticky, even
/// if the callback ignores them. Producer failures, panics, and storage errors
/// abort the attempt after the blocking worker has exited. An abort failure is
/// reported; the durable staging deadline remains the recovery backstop.
///
/// Dropping this future disconnects the producer at its next write. It cannot
/// stop arbitrary CPU work inside the callback, or perform asynchronous cleanup;
/// abandoned attempts remain subject to the store's staging deadline. Owners
/// must await cooperative cancellation before releasing execution reservations.
pub async fn stage_producer<S, F, T>(
    store: &S,
    identity: ArtifactIdentity,
    limits: ArtifactLimits,
    produce: F,
) -> ArtifactResult<(VerifiedArtifact, T)>
where
    S: ArtifactStore + ?Sized,
    F: FnOnce(&mut dyn Write) -> ArtifactResult<T> + Send + 'static,
    T: Send + 'static,
{
    let mut output = ArtifactWriter::new(store, identity.clone(), limits).await?;
    let (sender, mut receiver) = mpsc::channel(1);
    let producer = tokio::task::spawn_blocking(move || {
        let sink = BlockSender {
            sender,
            block_bytes: limits.block_bytes() as usize,
            failed: false,
        };
        let buffered = BufWriter::with_capacity(limits.block_bytes() as usize, sink);
        let mut writer =
            LimitedWriter::new(buffered, ByteLimit::new(limits.artifact_bytes() as usize));
        let result = produce(&mut writer);
        if writer.budget().exceeded() {
            return Err(ArtifactError::Limit("Artifact exceeds byte budget".into()));
        }
        let metadata = result?;
        writer.flush().map_err(producer_io_error)?;
        Ok(metadata)
    });
    let mut transfer = Ok(());
    while let Some(block) = receiver.recv().await {
        if let Err(error) = output.write(&block).await {
            transfer = Err(error);
            break;
        }
    }
    // Release a blocked send before joining, including after a destination error.
    drop(receiver);
    let produced = producer
        .await
        .map_err(|_| ArtifactError::Backend("Artifact producer worker failed".into()))
        .and_then(|result| result);
    let result = async {
        transfer?;
        let metadata = produced?;
        let manifest = output.seal().await?;
        Ok((verify(store, &manifest).await?, metadata))
    }
    .await;
    match result {
        Ok(value) => Ok(value),
        Err(error) => match store.abort(&identity).await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(ArtifactError::Backend(format!(
                "Artifact staging failed ({error}); abort failed ({cleanup}); staging cleanup remains required"
            ))),
        },
    }
}

fn producer_io_error(error: io::Error) -> ArtifactError {
    ArtifactError::Backend(format!("Artifact producer output failed: {error}"))
}

struct BlockSender {
    sender: mpsc::Sender<Vec<u8>>,
    block_bytes: usize,
    failed: bool,
}

impl Write for BlockSender {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.flush()?;
        for block in bytes.chunks(self.block_bytes) {
            if self.sender.blocking_send(block.to_vec()).is_err() {
                self.failed = true;
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Artifact destination stopped receiving",
                ));
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failed || self.sender.is_closed() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Artifact destination stopped receiving",
            ));
        }
        Ok(())
    }
}
