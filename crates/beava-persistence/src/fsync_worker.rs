//! Group-commit fsync worker — satisfies SRV-DUR-01 (1–5ms OR 1MB coalesce)
//! and SRV-DUR-02 (ACK after fsync) by owning a single `WalWriter` and
//! broadcasting a `durable_lsn` watermark every time a batch is fsynced.

use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::error::PersistError;
use crate::rotation;
use crate::writer::WalWriter;
use crate::{Lsn, RecordType, WalRecord};

/// Configuration for `WalSink::spawn`.
#[derive(Debug, Clone)]
pub struct WalSinkConfig {
    pub dir: PathBuf,
    pub initial_start_lsn: Lsn,
    pub initial_registry_version: u32,
    /// Max time to coalesce appends before fsync. Default 2 ms.
    pub fsync_interval_ms: u64,
    /// Max staged bytes (per segment) before forcing an fsync. Default 1 MiB.
    pub fsync_bytes: u64,
    /// Segment size before rotating to a new segment. Default 128 MiB.
    pub segment_bytes: u64,
}

impl Default for WalSinkConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./beava-wal"),
            initial_start_lsn: 1,
            initial_registry_version: 1,
            fsync_interval_ms: 2,
            fsync_bytes: 1 << 20,
            segment_bytes: 128 << 20,
        }
    }
}

struct AppendRequest {
    payload: Vec<u8>,
    done: oneshot::Sender<Result<Lsn, PersistError>>,
}

enum ControlMsg {
    TruncateUpTo {
        covered_lsn: Lsn,
        ack: oneshot::Sender<Result<u32, PersistError>>,
    },
    Shutdown {
        ack: oneshot::Sender<Result<(), PersistError>>,
    },
}

/// Handle to the fsync worker. Cloneable; callers use `append_event` to
/// stage a record and await its durable LSN.
#[derive(Clone)]
pub struct WalSink {
    append_tx: mpsc::Sender<AppendRequest>,
    control_tx: mpsc::Sender<ControlMsg>,
    durable_rx: watch::Receiver<Lsn>,
}

impl WalSink {
    /// Spawn the worker task. Returns the sink handle + the worker JoinHandle
    /// (callers should await the handle after `shutdown()` for clean exit).
    pub fn spawn(cfg: WalSinkConfig) -> Result<(Self, JoinHandle<()>), PersistError> {
        let writer = WalWriter::open(
            &cfg.dir,
            cfg.initial_start_lsn,
            cfg.initial_registry_version,
        )?;

        let (append_tx, append_rx) = mpsc::channel::<AppendRequest>(1024);
        let (control_tx, control_rx) = mpsc::channel::<ControlMsg>(16);
        // durable_lsn starts below the first-to-be-assigned LSN.
        let (durable_tx, durable_rx) =
            watch::channel::<Lsn>(cfg.initial_start_lsn.saturating_sub(1));

        let join = tokio::spawn(worker_loop(cfg, writer, append_rx, control_rx, durable_tx));

        Ok((
            Self {
                append_tx,
                control_tx,
                durable_rx,
            },
            join,
        ))
    }

    /// Enqueue a payload for durable append. Resolves only after the assigned
    /// LSN has been fsynced to disk.
    pub async fn append_event(&self, payload: Vec<u8>) -> Result<Lsn, PersistError> {
        let (tx, rx) = oneshot::channel();
        self.append_tx
            .send(AppendRequest { payload, done: tx })
            .await
            .map_err(|_| {
                PersistError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "WAL sink worker closed",
                ))
            })?;
        rx.await.map_err(|_| {
            PersistError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL sink worker dropped ack channel",
            ))
        })?
    }

    /// Current highest-durable LSN (snapshot of the watch channel).
    pub fn durable_lsn(&self) -> Lsn {
        *self.durable_rx.borrow()
    }

    /// Delete closed segments whose last LSN < `covered_lsn`. Returns the
    /// number of segments removed.
    pub async fn truncate_up_to(&self, covered_lsn: Lsn) -> Result<u32, PersistError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlMsg::TruncateUpTo {
                covered_lsn,
                ack: tx,
            })
            .await
            .map_err(|_| {
                PersistError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "WAL sink worker closed",
                ))
            })?;
        rx.await.map_err(|_| {
            PersistError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL sink worker dropped ack channel",
            ))
        })?
    }

    /// Flush any pending batch, close the current segment, and stop the worker.
    pub async fn shutdown(self) -> Result<(), PersistError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlMsg::Shutdown { ack: tx })
            .await
            .map_err(|_| {
                PersistError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "WAL sink worker already closed",
                ))
            })?;
        rx.await.map_err(|_| {
            PersistError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL sink worker dropped ack channel",
            ))
        })?
    }
}

/// Estimate on-disk size of an encoded record. Must match `record::encode_record`
/// byte count: `4 (length) + 4 (crc) + 8 (lsn) + 1 (type) + payload.len()`.
fn encoded_size(payload_len: usize) -> u64 {
    (4 + 4 + 8 + 1 + payload_len) as u64
}

async fn worker_loop(
    cfg: WalSinkConfig,
    mut writer: WalWriter,
    mut append_rx: mpsc::Receiver<AppendRequest>,
    mut control_rx: mpsc::Receiver<ControlMsg>,
    durable_tx: watch::Sender<Lsn>,
) {
    let mut pending: Vec<AppendRequest> = Vec::new();
    let mut staged_bytes: u64 = 0;
    let mut next_lsn: Lsn = cfg.initial_start_lsn;
    let mut current_start_lsn: Lsn = cfg.initial_start_lsn;
    let fsync_interval = Duration::from_millis(cfg.fsync_interval_ms);

    loop {
        let flush_deadline = if pending.is_empty() {
            None
        } else {
            Some(tokio::time::sleep(fsync_interval))
        };

        let flush_now = tokio::select! {
            biased;

            // Control messages (truncate / shutdown) — handle inline without flushing.
            ctrl = control_rx.recv() => {
                match ctrl {
                    Some(ControlMsg::TruncateUpTo { covered_lsn, ack }) => {
                        let res = rotation::truncate_up_to(&cfg.dir, current_start_lsn, covered_lsn);
                        let _ = ack.send(res);
                        false
                    }
                    Some(ControlMsg::Shutdown { ack }) => {
                        let res = flush_batch(
                            &cfg,
                            &mut writer,
                            &mut pending,
                            &mut staged_bytes,
                            &mut next_lsn,
                            &mut current_start_lsn,
                            &durable_tx,
                        ).await;
                        let close = writer.sync_data();
                        let combined = res.and_then(|_| close.map_err(PersistError::Io));
                        let _ = ack.send(combined);
                        return;
                    }
                    None => {
                        // Control channel dropped — shut down.
                        let _ = flush_batch(
                            &cfg,
                            &mut writer,
                            &mut pending,
                            &mut staged_bytes,
                            &mut next_lsn,
                            &mut current_start_lsn,
                            &durable_tx,
                        ).await;
                        let _ = writer.sync_data();
                        return;
                    }
                }
            }

            // New append request.
            req = append_rx.recv() => {
                match req {
                    Some(req) => {
                        staged_bytes += encoded_size(req.payload.len());
                        pending.push(req);
                        staged_bytes >= cfg.fsync_bytes
                    }
                    None => {
                        // All senders dropped — flush remaining and exit.
                        let _ = flush_batch(
                            &cfg,
                            &mut writer,
                            &mut pending,
                            &mut staged_bytes,
                            &mut next_lsn,
                            &mut current_start_lsn,
                            &durable_tx,
                        ).await;
                        let _ = writer.sync_data();
                        return;
                    }
                }
            }

            // Coalesce timer fired and we have pending work.
            _ = async { flush_deadline.unwrap().await }, if !pending.is_empty() => {
                true
            }
        };

        if flush_now {
            let _ = flush_batch(
                &cfg,
                &mut writer,
                &mut pending,
                &mut staged_bytes,
                &mut next_lsn,
                &mut current_start_lsn,
                &durable_tx,
            )
            .await;
        }
    }
}

async fn flush_batch(
    cfg: &WalSinkConfig,
    writer_ref: &mut WalWriter,
    pending: &mut Vec<AppendRequest>,
    staged_bytes: &mut u64,
    next_lsn: &mut Lsn,
    current_start_lsn: &mut Lsn,
    durable_tx: &watch::Sender<Lsn>,
) -> Result<(), PersistError> {
    if pending.is_empty() {
        return Ok(());
    }

    // Assign LSNs + encode + append.
    let mut dones: Vec<(Lsn, oneshot::Sender<Result<Lsn, PersistError>>)> =
        Vec::with_capacity(pending.len());
    let mut highest = *next_lsn;
    for req in pending.drain(..) {
        let lsn = *next_lsn;
        *next_lsn += 1;
        highest = lsn;
        let record = WalRecord {
            lsn,
            record_type: RecordType::Event,
            payload: req.payload,
        };
        if let Err(e) = writer_ref.append(&record) {
            let _ = req.done.send(Err(e));
            continue;
        }
        dones.push((lsn, req.done));
    }
    *staged_bytes = 0;

    // fsync via spawn_blocking so the tokio runtime stays responsive.
    // Temporarily move the writer into the blocking task by using a
    // raw pointer wrapper — but std::fs::File::sync_data requires &self,
    // so we can just call it through a mutable borrow inside spawn_blocking
    // via a mem::swap. Simpler: call sync_data inline (current_thread runtime).
    //
    // For v0 we invoke sync_data inline — it's a blocking syscall but on
    // the current_thread runtime the worker IS the thread, and the server
    // uses a multi-thread runtime for the HTTP accept side. Good enough
    // for the Phase 6 gate; Phase 13 revisits if P99 fsync shows up as a
    // bottleneck.
    //
    // NB: We attempted spawn_blocking here but it conflicts with
    // current_thread runtime used in tests (no blocking pool). Keeping
    // inline fsync avoids the test-vs-prod divergence.
    let sync_result = writer_ref.sync_data().map_err(PersistError::Io);

    // After fsync, bump watermark + resolve all waiters.
    if sync_result.is_ok() {
        let _ = durable_tx.send(highest);
        for (lsn, tx) in dones.drain(..) {
            let _ = tx.send(Ok(lsn));
        }
    } else {
        let err = || PersistError::Io(std::io::Error::other("fsync"));
        for (_lsn, tx) in dones.drain(..) {
            let _ = tx.send(Err(err()));
        }
        return sync_result;
    }

    // Rotate if we've outgrown the segment.
    if writer_ref.bytes_written() >= cfg.segment_bytes {
        let new_start = *next_lsn;
        rotation::rotate(
            writer_ref,
            &cfg.dir,
            new_start,
            cfg.initial_registry_version,
        )?;
        *current_start_lsn = new_start;
    }

    Ok(())
}
