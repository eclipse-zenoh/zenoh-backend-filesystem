//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use log::{trace, warn};
use rocksdb::{IteratorMode, DB};
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zenoh::buf::{WBuf, ZBuf};
use zenoh::prelude::*;
use zenoh::time::{Timestamp, NTP64};
use zenoh::Result as ZResult;
use zenoh_buffers::reader::HasReader;
use zenoh_collections::{Timed, TimedEvent, Timer};
use zenoh_core::{bail, zerror};

lazy_static::lazy_static! {
    static ref GC_PERIOD: Duration = Duration::new(30, 0);
    static ref MIN_DELAY_BEFORE_REMOVAL: NTP64 = NTP64::from(Duration::new(5, 0));
}

pub(crate) struct DataInfoMgr {
    // Note: rocksdb isn't thread-safe. See https://github.com/rust-rocksdb/rust-rocksdb/issues/404
    db: Arc<Mutex<DB>>,
    // Note: Timer is kept to not be dropped and keep the GC periodic event running
    #[allow(dead_code)]
    timer: Timer,
}

impl DataInfoMgr {
    // Name of the RocksDB directory for the data-info database
    pub(crate) const DB_FILENAME: &'static str = ".zenoh_datainfo";

    pub(crate) async fn new(base_dir: &Path) -> ZResult<Self> {
        let mut backup_file = PathBuf::from(base_dir);
        backup_file.push(DataInfoMgr::DB_FILENAME);

        let db = DB::open_default(&backup_file).map_err(|e| {
            zerror!(
                "Failed to open data-info database from {:?}: {}",
                backup_file,
                e
            )
        })?;
        let db = Arc::new(Mutex::new(db));

        // start periodic GC event
        let timer = Timer::default();
        let gc = TimedEvent::periodic(*GC_PERIOD, GarbageCollectionEvent { db: db.clone() });
        let _ = timer.add_async(gc).await;

        Ok(DataInfoMgr { db, timer })
    }

    pub(crate) async fn close(&self) -> ZResult<()> {
        let db = self.db.lock().await;
        // Flush before to close
        db.flush()
            .and_then(|()| DB::destroy(&rocksdb::Options::default(), db.path()))
            .map_err(|err| zerror!("Failed to close data-info database: {}", err).into())
    }

    pub(crate) async fn put_data_info<P: AsRef<Path>>(
        &self,
        file: P,
        encoding: &Encoding,
        timestamp: &Timestamp,
    ) -> ZResult<()> {
        let key = file.as_ref().to_string_lossy();
        trace!("Put data-info for {}", key);
        let mut value: WBuf = WBuf::new(32, true);
        // note: encode timestamp at first for faster decoding when only this one is required
        let write_ok = value.write_timestamp(timestamp)
            && value.write_zint(u8::from(*encoding.prefix()).into())
            && value.write_string(encoding.suffix());
        if !write_ok {
            bail!("Failed to encode data-info for {:?}", file.as_ref())
        } else {
            self.db
                .lock()
                .await
                .put(key.as_bytes(), value.get_first_slice(..))
                .map_err(|e| {
                    zerror!("Failed to save data-info for {:?}: {}", file.as_ref(), e).into()
                })
        }
    }

    pub(crate) async fn rename_key<P: AsRef<Path>>(&self, from: P, to: P) -> ZResult<()> {
        let from_key = from.as_ref().to_string_lossy();
        let to_key = to.as_ref().to_string_lossy();
        trace!("Changing data-info from {} to {}", from_key, to_key);
        let db_instance = self.db.lock().await;
        let val = db_instance.get_pinned(from_key.as_bytes());
        match val {
            Ok(Some(pin_val)) => {
                db_instance.put(to_key.as_bytes(), pin_val).map_err(|e| {
                    zerror!("Failed to save data-info for {:?}: {}", to.as_ref(), e)
                })?;
                db_instance.delete(from_key.as_bytes()).map_err(|e| {
                    zerror!("Failed to save data-info for {:?}: {}", to.as_ref(), e).into()
                })
            }
            Ok(None) => {
                trace!("data-info for {:?} not found", from.as_ref());
                bail!(
                    "Failed to get data-info for {:?}: data-info not found",
                    from.as_ref()
                )
            }
            Err(e) => bail!("Failed to get data-info for {:?}: {}", from.as_ref(), e),
        }
    }

    pub(crate) async fn get_encoding_and_timestamp<P: AsRef<Path>>(
        &self,
        file: P,
    ) -> ZResult<Option<(Encoding, Timestamp)>> {
        let key = file.as_ref().to_string_lossy();
        trace!("Get data-info for {}", key);
        match self.db.lock().await.get_pinned(key.as_bytes()) {
            Ok(Some(pin_val)) => decode_encoding_timestamp_from_value(pin_val.as_ref()).map(Some),
            Ok(None) => {
                trace!("data-info for {:?} not found", file.as_ref());
                Ok(None)
            }
            Err(e) => bail!("Failed to get data-info for {:?}: {}", file.as_ref(), e),
        }
    }

    pub(crate) async fn get_timestamp<P: AsRef<Path>>(
        &self,
        file: P,
    ) -> ZResult<Option<Timestamp>> {
        let key = file.as_ref().to_string_lossy();
        trace!("Get timestamp for {}", key);
        match self.db.lock().await.get_pinned(key.as_bytes()) {
            Ok(Some(pin_val)) => decode_timestamp_from_value(pin_val.as_ref()).map(Some),
            Ok(None) => {
                trace!("timestamp for {:?} not found", file.as_ref());
                Ok(None)
            }
            Err(e) => bail!("Failed to get data-info for {:?}: {}", file.as_ref(), e),
        }
    }

    pub async fn get_deleted_entries(&self) -> Vec<(String, Timestamp)> {
        let mut result = Vec::new();
        let db = self.db.lock().await;
        for (key, value) in db.iterator(IteratorMode::Start) {
            if let Ok(path) = std::str::from_utf8(&key).map(Path::new) {
                if !path.exists() {
                    match decode_timestamp_from_value(&value) {
                        Ok(timestamp) => {
                            result.push((std::str::from_utf8(&key).unwrap().to_string(), timestamp));
                        }
                        Err(e) => warn!("Failed to decode data-info for file {:?}: {}", path, e),
                    }
                }
            }
        }
        result
    }
}

fn decode_encoding_timestamp_from_value(val: &[u8]) -> ZResult<(Encoding, Timestamp)> {
    let buf = ZBuf::from(val.to_vec());
    let mut buf = buf.reader();
    let timestamp = buf
        .read_timestamp()
        .ok_or_else(|| zerror!("Failed to decode data-info (timestamp)"))?;
    let encoding_prefix = buf
        .read_zint()
        .ok_or_else(|| zerror!("Failed to decode data-info (encoding.prefix)"))?;
    let encoding_suffix = buf
        .read_string()
        .ok_or_else(|| zerror!("Failed to decode data-info (encoding.suffix)"))?;
    let encoding_prefix = encoding_prefix
        .try_into()
        .map_err(|_| zerror!("Unknown encoding {}", encoding_prefix))?;
    let encoding = if encoding_suffix.is_empty() {
        Encoding::Exact(encoding_prefix)
    } else {
        Encoding::WithSuffix(encoding_prefix, encoding_suffix.into())
    };
    Ok((encoding, timestamp))
}

fn decode_timestamp_from_value(val: &[u8]) -> ZResult<Timestamp> {
    let buf = ZBuf::from(val.to_vec());
    let mut buf = buf.reader();
    let timestamp = buf
        .read_timestamp()
        .ok_or_else(|| zerror!("Failed to decode data-info (timestamp)"))?;
    Ok(timestamp)
}

// Periodic event cleaning-up data info for no-longer existing files
struct GarbageCollectionEvent {
    db: Arc<Mutex<DB>>,
}

#[async_trait]
impl Timed for GarbageCollectionEvent {
    async fn run(&mut self) {
        trace!("Start garbage collection of obsolete data-infos");
        let time_limit = NTP64::from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
            - *MIN_DELAY_BEFORE_REMOVAL;
        let db = self.db.lock().await;
        for (key, value) in db.iterator(IteratorMode::Start) {
            if let Ok(path) = std::str::from_utf8(&key).map(Path::new) {
                if !path.exists() {
                    // check if path was marked as deleted for a long time
                    match decode_timestamp_from_value(&value) {
                        Ok(timestamp) => {
                            if timestamp.get_time() < &time_limit {
                                trace!("Cleanup old data-info for {:?}", path);
                                db.delete(&key).unwrap_or_else(|e| {
                                    warn!("Failed to delete data-info for file {:?}: {}", path, e)
                                });
                            }
                        }
                        Err(e) => warn!("Failed to decode data-info for file {:?}: {}", path, e),
                    }
                }
            }
        }
        trace!("End garbage collection of obsolete data-infos");
    }
}
