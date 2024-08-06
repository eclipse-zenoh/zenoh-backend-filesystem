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
use std::{
    borrow::Cow,
    convert::TryFrom,
    fmt,
    fs::{metadata, remove_dir, remove_dir_all, remove_file, rename, DirBuilder, File},
    io::prelude::*,
    iter::Iterator,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::{debug, trace, warn};
use walkdir::{IntoIter, WalkDir};
use zenoh::{
    bytes::Encoding,
    internal::{
        bail,
        buffers::{SplitBuffer, ZBuf},
        zerror, Value,
    },
    key_expr::keyexpr,
    time::{Timestamp, TimestampId},
    Result as ZResult,
};

use crate::{blockon_runtime, data_info_mgt::*};

pub const CONFLICT_SUFFIX: &str = ".##z";

pub(crate) enum OnClosure {
    DeleteAll,
    DoNothing,
}

// a structure holding a zenoh path (absolute) and the corresponding file-system path (including the base_dir)
#[derive(Debug)]
pub(crate) struct ZFile<'a> {
    pub(crate) zpath: Cow<'a, str>,
    fspath: PathBuf,
}

impl fmt::Display for ZFile<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.fspath)
    }
}

pub(crate) struct FilesMgr {
    base_dir: PathBuf,
    data_info_mgr: DataInfoMgr,
    follow_links: bool,
    keep_mime: bool,
    dir_builder: DirBuilder,
    on_closure: OnClosure,
}

impl FilesMgr {
    pub(crate) async fn new(
        base_dir: PathBuf,
        follow_links: bool,
        keep_mime: bool,
        on_closure: OnClosure,
    ) -> ZResult<Self> {
        let data_info_mgr = DataInfoMgr::new(base_dir.as_path()).await?;

        let mut dir_builder = DirBuilder::new();
        dir_builder.recursive(true);

        Ok(FilesMgr {
            base_dir,
            data_info_mgr,
            follow_links,
            keep_mime,
            dir_builder,
            on_closure,
        })
    }

    pub(crate) fn base_dir(&self) -> &Path {
        self.base_dir.as_path()
    }

    pub(crate) fn to_zfile<'a>(&self, zpath: &'a str) -> ZFile<'a> {
        ZFile {
            zpath: Cow::from(zpath),
            fspath: self.to_fspath(zpath),
        }
    }

    fn to_fspath(&self, zpath: &str) -> PathBuf {
        let mut fspath = self.base_dir.clone();
        fspath.push(zpath_to_fspath(zpath).as_ref());
        fspath
    }

    // ### Behaviour in case of conflict
    // A possible case of conflict occurs when a PUT operation operates on a prefix of another PUT.
    // This leads to having a directory and file with the same name which is forbidden on file systems.
    // We fix this by appending a suffix `.##z` to the conflicting file. This is dealt with internally and not exposed to the user.

    pub(crate) async fn write_file(
        &self,
        zfile: &ZFile<'_>,
        content: ZBuf,
        encoding: Encoding,
        timestamp: &Timestamp,
    ) -> ZResult<()> {
        let file = &zfile.fspath;

        // Create parent directories if needed
        let parent = file.parent().unwrap();
        let ancestor = parent.ancestors().collect::<Vec<_>>();
        for a in ancestor {
            // if the ancestor is a directory, break, no need to check anything else
            if a.exists() && a.is_dir() {
                break;
            }
            // if the ancestor is a file, rename the file with a conflict suffix and update the info on rocksdb
            if a.exists() && a.is_file() {
                let conflict_file = self.get_conflict_file(a.to_path_buf());
                trace!(
                    "Conflict detected for {:?}. Writing to conflict free file {:?}",
                    a,
                    conflict_file
                );
                rename(a, &conflict_file)
                    .map_err(|e| zerror!("Failed to write in file {:?}: {}", conflict_file, e))?;
                match self.data_info_mgr.rename_key(a, &conflict_file).await {
                    Ok(_) => None,
                    Err(_) => {
                        // fallback: get encoding and timestamp from file's metadata
                        let (a_encoding, a_timestamp) = self.generate_metadata(a, timestamp);
                        self.data_info_mgr
                            .put_data_info(file, a_encoding, &a_timestamp)
                            .await
                            .ok()
                    }
                };
            }
        }

        self.dir_builder
            .create(parent)
            .map_err(|e| zerror!("Failed to create directories for file {:?}: {}", file, e))?;

        // Write file
        trace!("Write in file {:?}", file);
        let file = if file.exists() && file.is_dir() {
            self.get_conflict_file(file.to_path_buf())
        } else {
            file.to_path_buf()
        };
        trace!("Writing in conflict-free file {:?}", file);
        let mut f = File::create(&file)
            .map_err(|e| zerror!("Failed to write in file {:?}: {}", file, e))?;
        for slice in content.slices() {
            f.write_all(slice)
                .map_err(|e| zerror!("Failed to write in file {:?}: {}", file, e))?;
        }

        // save data-info
        self.data_info_mgr
            .put_data_info(file, encoding, timestamp)
            .await
    }

    fn get_conflict_file(&self, file: PathBuf) -> PathBuf {
        match file.to_str() {
            Some(x) => PathBuf::from(get_conflict_resolved_keyexpr(x)),
            None => file.to_path_buf(),
        }
    }

    pub(crate) async fn delete_file(&self, zfile: &ZFile<'_>) -> ZResult<()> {
        let file = &zfile.fspath;

        let file = if file.exists() && file.is_file() {
            file.to_path_buf()
        } else {
            self.get_conflict_file(file.to_path_buf())
        };

        // Delete file
        trace!("Delete file {:?}", file);
        if file.exists() {
            remove_file(&file).map_err(|e| zerror!("Failed to delete file {:?}: {}", file, e))?;
            // try to delete parent directories if empty
            let mut f = file.as_path();
            while let Some(parent) = f.parent() {
                if parent != self.base_dir() && remove_dir(parent).is_ok() {
                    trace!("Removed empty dir: {:?}", parent);
                } else {
                    break;
                }
                f = parent;
            }
        }

        // save timestamp in data-info (encoding is not used)
        self.data_info_mgr.del_data_info(file).await
    }

    // Read a file and return it's content (as Vec<u8>), encoding and timestamp.
    // Encoding and timestamp are retrieved from the data_info_mgr if file was put via zenoh.
    // Otherwise, the encoding is guessed from the file extension, and the timestamp is computed from the file's time.
    pub(crate) async fn read_file(&self, zfile: &ZFile<'_>) -> ZResult<Option<(Value, Timestamp)>> {
        let file = &zfile.fspath;
        match self.perform_read(file).await? {
            Some(x) => Ok(Some(x)),
            None => self.perform_read_from_conflict(file.to_path_buf()).await,
        }
    }

    async fn perform_read_from_conflict(
        &self,
        file: PathBuf,
    ) -> ZResult<Option<(Value, Timestamp)>> {
        let file = self.get_conflict_file(file.to_path_buf());
        self.perform_read(&file.to_path_buf()).await
    }

    async fn perform_read(&self, file: &Path) -> ZResult<Option<(Value, Timestamp)>> {
        // consider file only is it exists, it's a file and in case of "follow_links=true" it doesn't contain symlink
        if file.exists() && file.is_file() && (self.follow_links || !self.contains_symlink(file)) {
            match File::open(file) {
                Ok(mut f) => {
                    // TODO: what if file is too big ??
                    let size = f.metadata().map(|m| m.len()).unwrap_or(256);
                    if size <= usize::MAX as u64 {
                        trace!("Read file {:?}", file);
                        let mut content: Vec<u8> = Vec::with_capacity(size as usize);
                        if let Err(e) = f.read_to_end(&mut content) {
                            bail!(r#"Error reading file {:?}: {}"#, file, e)
                        } else {
                            let (encoding, timestamp) =
                                self.get_encoding_and_timestamp(file).await?;
                            Ok(Some((Value::new(content, encoding), timestamp)))
                        }
                    } else {
                        bail!(r#"Error reading file {:?}: too big to fit in memory"#, file)
                    }
                }
                Err(e) => bail!(r#"Error reading file {:?}: {}"#, file, e),
            }
        } else {
            Ok(None)
        }
    }

    // Search for files matching path_expr.
    pub(crate) fn matching_files<'a>(&self, zpath_expr: &'a keyexpr) -> FilesIterator<'a> {
        // find the longest segment without '*' to search for files only in the corresponding
        let star_idx = zpath_expr.find('*').unwrap();
        let segment = match zpath_expr[..star_idx].rfind('/') {
            Some(i) => &zpath_expr[..i],
            None => "",
        };
        // Directory to search for matching files is base_dir + segment converted as a file-system path
        let search_dir = self.to_fspath(segment);
        let base_dir_len = self.base_dir.as_os_str().len();

        if !self.follow_links && self.contains_symlink(&search_dir) {
            debug!(
                "Don't search for files in {:?} as it's within a symbolic link",
                search_dir
            );
            // return a useless FilesIterator that won't return anything (simpler than to return an Option<FilesIterator>)
            let walkdir = WalkDir::new("");
            FilesIterator {
                walk_iter: walkdir.into_iter(),
                zpath_expr,
                base_dir_len,
            }
        } else {
            debug!(
                "For path_expr={} search matching files in {:?}",
                zpath_expr, search_dir
            );
            let walkdir = WalkDir::new(search_dir).follow_links(self.follow_links);
            FilesIterator {
                walk_iter: walkdir.into_iter(),
                zpath_expr,
                base_dir_len,
            }
        }
    }

    fn generate_metadata(&self, file: &Path, timestamp: &Timestamp) -> (Encoding, Timestamp) {
        let a_encoding = self.guess_encoding(file);
        let a_timestamp = match self.get_timestamp_from_metadata(file) {
            Ok(a_ts) => a_ts,
            Err(_) => *timestamp,
        };
        (a_encoding, a_timestamp)
    }

    async fn get_encoding_and_timestamp(&self, file: &Path) -> ZResult<(Encoding, Timestamp)> {
        // try to get Encoding and Timestamp from data_info_mgr
        match self.data_info_mgr.get_encoding_and_timestamp(&file).await? {
            Some((encoding, timestamp)) => Ok((encoding, timestamp)),
            None => {
                trace!("data-info for {:?} not found; fallback to metadata", file);
                let encoding = self.guess_encoding(file);
                // fallback: get timestamp from file's metadata
                let timestamp = self.get_timestamp_from_metadata(file)?;

                Ok((encoding, timestamp))
            }
        }
    }

    fn guess_encoding(&self, file: &Path) -> Encoding {
        if self.keep_mime {
            // fallback: guess mime type from file extension
            let mime_type = mime_guess::from_path(file).first_or_octet_stream();
            Encoding::from(mime_type.essence_str().to_string())
        } else {
            Encoding::APPLICATION_OCTET_STREAM
        }
    }

    fn get_timestamp_from_metadata<P: AsRef<Path>>(&self, file: P) -> ZResult<Timestamp> {
        let metadata = metadata(&file).map_err(|e| {
            zerror!(
                "Failed to get meta-data for file {:?}: {}",
                file.as_ref(),
                e
            )
        })?;
        let sys_time = metadata
            .modified()
            .or_else(|_| metadata.accessed())
            .or_else(|_| metadata.created())
            .unwrap_or_else(|_| SystemTime::now());
        Ok(Timestamp::new(
            sys_time.duration_since(UNIX_EPOCH).unwrap().into(),
            TimestampId::try_from([1]).unwrap(),
        ))
    }

    // Check if a Path contains a segment which is a symbolic link
    fn contains_symlink<P: AsRef<Path>>(&self, path: P) -> bool {
        if is_symlink(&path) {
            return true;
        }

        let mut current = path.as_ref();
        while let Some(parent) = current.parent() {
            // check only up-to base_dir, and don't mind if it's itself a symbolic link
            if parent == self.base_dir() {
                return false;
            } else if is_symlink(parent) {
                return true;
            }
            current = parent;
        }
        false
    }
}

impl Drop for FilesMgr {
    fn drop(&mut self) {
        debug!("Closing File System Storage on {:?}", self.base_dir);
        match self.on_closure {
            OnClosure::DeleteAll => {
                // Close data_info_mgr at first
                blockon_runtime(async move {
                    self.data_info_mgr
                        .close()
                        .await
                        .unwrap_or_else(|e| warn!("{}", e));
                    remove_dir_all(&self.base_dir).unwrap_or_else(|err| {
                        warn!("Failed to cleanup directory {:?}; {}", self.base_dir, err)
                    });
                });
            }
            OnClosure::DoNothing => {
                debug!(
                    "Close File System Storage, keeping directory {:?} as it is",
                    self.base_dir
                );
            }
        }
    }
}

pub(crate) struct FilesIterator<'a> {
    walk_iter: IntoIter,
    zpath_expr: &'a keyexpr,
    base_dir_len: usize,
}

impl<'a> Iterator for FilesIterator<'a> {
    type Item = ZFile<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.walk_iter.next() {
                Some(Ok(e)) => {
                    if e.file_type().is_dir() {
                        // skip content of DataInfoMgr::DB_FILENAME directory
                        if e.file_name().to_str().unwrap_or_default() == DataInfoMgr::DB_FILENAME {
                            self.walk_iter.skip_current_dir();
                        }
                    } else {
                        let fspath = e.into_path();
                        if let Some(s) = fspath.to_str() {
                            // coarse_zpath is the file's absolute path stripped from base_dir and converted as zenoh path
                            let coarse_zpath = fspath_to_zpath(&s[self.base_dir_len..]);
                            // zpath trims away the CONFLICT_SUFFIX if present
                            let zpath = get_trimmed_keyexpr(&coarse_zpath);
                            let zpath_as_ke = match keyexpr::new(zpath) {
                                Ok(ke) => ke,
                                Err(e) => {
                                    tracing::error!(
                                        "Couldn't convert `{}` into a key expression: {}",
                                        &zpath,
                                        e
                                    );
                                    continue;
                                }
                            };
                            // convert it to zenoh path for matching test with zpath_expr
                            if self.zpath_expr.intersects(zpath_as_ke) {
                                // matching file; return a ZFile
                                let zfile = ZFile {
                                    zpath: zpath.to_string().into(),
                                    fspath: fspath.clone(),
                                };
                                return Some(zfile);
                            }
                        } else {
                            debug!(
                                "Looking for files matching {}: ignore {:?} as non UTF-8 filename",
                                self.zpath_expr, fspath
                            );
                        };
                    }
                    continue;
                }
                None => return None,
                Some(Err(err)) => {
                    // Cannot read file or dir... that might be normal (or not...) ignore it
                    debug!(
                        "Possible issue looking for files matching {} : {}",
                        self.zpath_expr, err
                    );
                    continue;
                }
            };
        }
    }
}

#[cfg(unix)]
#[inline(always)]
pub(crate) fn zpath_to_fspath(zpath: &str) -> Cow<'_, str> {
    Cow::from(zpath)
}

#[cfg(windows)]
pub(crate) fn zpath_to_fspath(zpath: &str) -> Cow<'_, str> {
    const WIN_SEP: &str = r#"\"#;
    Cow::from(zpath.replace('/', WIN_SEP))
}

#[cfg(unix)]
#[inline(always)]
pub(crate) fn fspath_to_zpath(fspath: &str) -> Cow<'_, str> {
    Cow::from(fspath)
}

#[cfg(windows)]
pub(crate) fn fspath_to_zpath(fspath: &str) -> Cow<'_, str> {
    const ZENOH_SEP: &str = "/";
    Cow::from(fspath.replace(std::path::MAIN_SEPARATOR, ZENOH_SEP))
}

fn is_symlink<P: AsRef<Path>>(path: P) -> bool {
    match path.as_ref().symlink_metadata() {
        Ok(metadata) => metadata.file_type().is_symlink(),
        Err(_) => false,
    }
}

pub(crate) fn get_trimmed_keyexpr(keyexpr: &str) -> &str {
    let k = if keyexpr.ends_with(CONFLICT_SUFFIX) {
        keyexpr.strip_suffix(CONFLICT_SUFFIX).unwrap_or(keyexpr)
    } else {
        keyexpr
    };
    k.strip_prefix('/').unwrap_or(k)
}

pub(crate) fn get_conflict_resolved_keyexpr(keyexpr: &str) -> String {
    format!("{keyexpr}{CONFLICT_SUFFIX}")
}
