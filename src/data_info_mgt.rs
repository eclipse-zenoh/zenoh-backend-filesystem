//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use log::{trace, warn};
use rocksdb::{IteratorMode, DB};
use std::convert::TryFrom;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uhlc::NTP64;
use zenoh::net::ZInt;
use zenoh::{Timestamp, ZError, ZErrorKind, ZResult};
use zenoh_util::collections::{Timed, TimedEvent, Timer};
use zenoh_util::{zerror, zerror2};

// maximum size of serialized data-info: encoding (u64) + timestamp (u64 + ID at max size)
const MAX_VAL_LEN: usize = 8 + 8 + uhlc::ID::MAX_SIZE;
// minimum size of serialized data-info: encoding (u64) + timestamp (u64 + ID at 1 byte)
const MIN_VAL_LEN: usize = 8 + 8 + uhlc::ID::MAX_SIZE;

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
            zerror2!(ZErrorKind::Other {
                descr: format!(
                    "Failed to open data-info database from {:?}: {}",
                    backup_file, e
                )
            })
        })?;
        let db = Arc::new(Mutex::new(db));

        // start periodic GC event
        let timer = Timer::new();
        let gc = TimedEvent::periodic(*GC_PERIOD, GarbageCollectionEvent { db: db.clone() });
        let _ = timer.add(gc).await;

        Ok(DataInfoMgr { db, timer })
    }

    pub(crate) async fn close(&self) -> ZResult<()> {
        let db = self.db.lock().await;
        // Flush before to close
        db.flush()
            .and_then(|()| DB::destroy(&rocksdb::Options::default(), db.path()))
            .map_err(|err| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to close data-info database: {}", err)
                })
            })
    }

    pub(crate) async fn put_data_info<P: AsRef<Path>>(
        &self,
        file: P,
        encoding: ZInt,
        timestamp: Timestamp,
    ) -> ZResult<()> {
        let key = file.as_ref().to_string_lossy();
        trace!("Put data-info for {}", key);
        let mut value: Vec<u8> = Vec::with_capacity(MAX_VAL_LEN);
        value
            .write_all(&encoding.to_ne_bytes())
            .and_then(|()| value.write_all(&timestamp.get_time().as_u64().to_ne_bytes()))
            .and_then(|()| value.write_all(timestamp.get_id().as_slice()))
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to encode data-info for {:?}: {}", file.as_ref(), e)
                })
            })?;

        self.db
            .lock()
            .await
            .put(key.as_bytes(), value)
            .map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to save data-info for {:?}: {}", file.as_ref(), e)
                })
            })
    }

    pub(crate) async fn get_encoding_and_timestamp<P: AsRef<Path>>(
        &self,
        file: P,
    ) -> ZResult<Option<(ZInt, Timestamp)>> {
        let key = file.as_ref().to_string_lossy();
        trace!("Get data-info for {}", key);
        match self.db.lock().await.get_pinned(key.as_bytes()) {
            Ok(Some(pin_val)) => decode_encoding_timestamp_from_value(pin_val.as_ref()).map(Some),
            Ok(None) => {
                trace!("data-info for {:?} not found", file.as_ref());
                Ok(None)
            }
            Err(e) => zerror!(ZErrorKind::Other {
                descr: format!("Failed to save data-info for {:?}: {}", file.as_ref(), e)
            }),
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
            Err(e) => zerror!(ZErrorKind::Other {
                descr: format!("Failed to save data-info for {:?}: {}", file.as_ref(), e)
            }),
        }
    }
}

fn decode_encoding_timestamp_from_value(val: &[u8]) -> ZResult<(ZInt, Timestamp)> {
    if val.len() < MIN_VAL_LEN {
        return zerror!(ZErrorKind::Other {
            descr: "Failed decode data-info (buffer too small)".to_string()
        });
    }
    let mut encoding_bytes = [0u8; 8];
    encoding_bytes.clone_from_slice(&val[..8]);
    let encoding = ZInt::from_ne_bytes(encoding_bytes);
    let mut time_bytes = [0u8; 8];
    time_bytes.clone_from_slice(&val[8..16]);
    let time = u64::from_ne_bytes(time_bytes);
    let id = uhlc::ID::try_from(&val[16..]).unwrap();
    let timestamp = Timestamp::new(NTP64(time), id);
    Ok((encoding, timestamp))
}

fn decode_timestamp_from_value(val: &[u8]) -> ZResult<Timestamp> {
    if val.len() < MIN_VAL_LEN {
        return zerror!(ZErrorKind::Other {
            descr: "Failed decode data-info (buffer too small)".to_string()
        });
    }
    let mut time_bytes = [0u8; 8];
    time_bytes.clone_from_slice(&val[8..16]);
    let time = u64::from_ne_bytes(time_bytes);
    let id = uhlc::ID::try_from(&val[16..]).unwrap();
    Ok(Timestamp::new(NTP64(time), id))
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
