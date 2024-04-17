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
use rocksdb::DB;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::trace;
use zenoh::buffers::{reader::HasReader, writer::HasWriter};
use zenoh::prelude::*;
use zenoh::time::{Timestamp, NTP64};
use zenoh::Result as ZResult;
use zenoh_codec::{RCodec, WCodec, Zenoh080};
use zenoh_core::{bail, zerror};

lazy_static::lazy_static! {
    static ref GC_PERIOD: Duration = Duration::new(30, 0);
    static ref MIN_DELAY_BEFORE_REMOVAL: NTP64 = NTP64::from(Duration::new(5, 0));
}

pub(crate) struct DataInfoMgr {
    // Note: rocksdb isn't thread-safe. See https://github.com/rust-rocksdb/rust-rocksdb/issues/404
    db: Arc<Mutex<DB>>,
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

        Ok(DataInfoMgr { db })
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
        const ERR: &str = "Failed to encode data-info for";

        let key = file.as_ref().to_string_lossy();
        trace!("Put data-info for {}", key);
        let mut value = vec![];
        let mut writer = value.writer();
        let codec = Zenoh080::new();
        // note: encode timestamp at first for faster decoding when only this one is required
        codec
            .write(&mut writer, timestamp)
            .map_err(|_| zerror!("{} {:?}", ERR, file.as_ref()))?;
        codec
            .write(&mut writer, encoding)
            .map_err(|_| zerror!("{} {:?}", ERR, file.as_ref()))?;

        self.db
            .lock()
            .await
            .put(key.as_bytes(), value)
            .map_err(|e| zerror!("Failed to save data-info for {:?}: {}", file.as_ref(), e).into())
    }

    pub(crate) async fn del_data_info<P: AsRef<Path>>(&self, file: P) -> ZResult<()> {
        let key = file.as_ref().to_string_lossy();
        trace!("Delete data-info for {}", key);
        let db = self.db.lock().await;
        match db.delete(key.as_bytes()) {
            Ok(()) => Ok(()),
            Err(e) => Err(format!(
                "Failed to delete data-info for file {:?}: {}",
                file.as_ref(),
                e
            )
            .into()),
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
}

fn decode_encoding_timestamp_from_value(val: &[u8]) -> ZResult<(Encoding, Timestamp)> {
    let codec = Zenoh080::new();
    let mut reader = val.reader();
    let timestamp: Timestamp = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    let encoding: Encoding = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (encoding)"))?;
    Ok((encoding, timestamp))
}
