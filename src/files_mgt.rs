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
use async_std::task;
use log::{debug, trace, warn};
use std::borrow::Cow;
use std::fmt;
use std::fs::{metadata, remove_dir, remove_dir_all, remove_file, DirBuilder, File};
// use std::ffi::OsString;
use std::io::prelude::*;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::{IntoIter, WalkDir};
use zenoh::buf::ZBuf;
use zenoh::net::protocol::core::rname;
use zenoh::prelude::*;
use zenoh::time::{Timestamp, TimestampId};
use zenoh_util::{zerror, zerror2};

use crate::data_info_mgt::*;

pub const CONFLICT_SUFFIX: &str = "__z__";

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
        let mut os_str = self.base_dir().as_os_str().to_os_string();
        os_str.push(zpath_to_fspath(zpath).as_ref());
        PathBuf::from(os_str)
    }

    pub(crate) async fn write_file(
        &self,
        zfile: &ZFile<'_>,
        content: ZBuf,
        encoding: &Encoding,
        timestamp: &Timestamp,
    ) -> ZResult<()> {
        let file = &zfile.fspath;

        // Create parent directories if needed
        let parent = file.parent().unwrap();
        let mut ancestor = parent.ancestors().collect::<Vec<_>>();
        ancestor.reverse();
        for a in ancestor {
            trace!("checking ancestor {:?}", a);
            if a.exists() && a.is_file(){
                trace!("Conflict detected for {:?}", a);
                let conflict_file = self.get_conflict_file(a.to_path_buf());
                trace!("Writing to conflict free file {:?}", conflict_file);
                //TODO: move everything related to this file to the conflict file, including rocksdb entry if present
                let mut cf = File::create(&conflict_file).map_err(|e| {
                    zerror2!(ZErrorKind::Other {
                        descr: format!("Failed to write in file {:?}: {}", conflict_file, e)
                    })
                })?;
                let conflict_value = match self.perform_read(a.to_path_buf()).await? {
                    Some(val) => val.0,
                    None => Vec::new().into(),
                };
                let conflict_content = conflict_value.payload;
                for slice in conflict_content {
                    cf.write_all(slice.as_slice()).map_err(|e| {
                        zerror2!(ZErrorKind::Other {
                            descr: format!("Failed to write in file {:?}: {}", conflict_file, e)
                        })
                    })?;
                }
                let (conflict_encoding, conflict_timestamp) = self.get_encoding_and_timestamp(a.to_path_buf()).await?;
                // save data-info
                self.data_info_mgr
                    .put_data_info(conflict_file, &conflict_encoding, &conflict_timestamp)
                    .await?;
                // cleanup the previous file
                remove_file(a.to_path_buf()).map_err(|e| {
                    zerror2!(ZErrorKind::Other {
                        descr: format!("Failed to delete file {:?}: {}", a, e)
                    })
                })?;
            }
        }

            self.dir_builder.create(parent).map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to create directories for file {:?}: {}", file, e)
                })
            })?;

        // Write file
        trace!("Write in file {:?}", file);
        let file = if file.exists() && file.is_dir() {
            self.get_conflict_file(file.to_path_buf())
        } else{
            file.to_path_buf()
        };
        trace!("Writing in conflict-free file {:?}", file);
        let mut f = File::create(&file).map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!("Failed to write in file {:?}: {}", file, e)
            })
        })?;
        for slice in content {
            f.write_all(slice.as_slice()).map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to write in file {:?}: {}", file, e)
                })
            })?;
        }

        // save data-info
        self.data_info_mgr
            .put_data_info(file, encoding, timestamp)
            .await
    }

    fn get_conflict_file(&self, file: PathBuf) -> PathBuf{
        match file.to_str(){
            Some(x) => 
            PathBuf::from(format!("{}{}", x, CONFLICT_SUFFIX)),
            None => file.to_path_buf(),
        }
    }

    pub(crate) async fn delete_file(
        &self,
        zfile: &ZFile<'_>,
        timestamp: &Timestamp,
    ) -> ZResult<()> {
        let file = &zfile.fspath;

        let file = if file.exists() && file.is_file() {
            file.to_path_buf()
        } else {
            self.get_conflict_file(file.to_path_buf())
        };

        // Delete file
        trace!("Delete file {:?}", file);
        if file.exists() {
            remove_file(file.to_path_buf()).map_err(|e| {
                zerror2!(ZErrorKind::Other {
                    descr: format!("Failed to delete file {:?}: {}", file, e)
                })
            })?;
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
        self.data_info_mgr
            .put_data_info(file, &Encoding::EMPTY, timestamp)
            .await
    }

    // Read a file and return it's content (as Vec<u8>), encoding and timestamp.
    // Encoding and timestamp are retrieved from the data_info_mgr if file was put via zenoh.
    // Otherwise, the encoding is guessed from the file extension, and the timestamp is computed from the file's time.
    pub(crate) async fn read_file(&self, zfile: &ZFile<'_>) -> ZResult<Option<(Value, Timestamp)>> {
        let file = &zfile.fspath;
        //TODO: what if the query comes for the filename with CONFLICT_SUFFIX??
        // let trimmed_file = if file.file_name().unwrap_or(&OsString::new()).to_os_string().to_str().unwrap_or("").ends_with(CONFLICT_SUFFIX) {
        //     Path::new(file.to_str().unwrap_or("").strip_suffix(CONFLICT_SUFFIX).unwrap_or(""))
        // } else {
        //     file
        // };
        // debug!{"The trimmed file name is {:?}", trimmed_file};
        match self.perform_read(file.to_path_buf()).await? {
            Some(x) => Ok(Some(x)),
            None => self.perform_read_from_conflict(file.to_path_buf()).await,
        }
    }

    async fn perform_read_from_conflict(&self, file:PathBuf) -> ZResult<Option<(Value, Timestamp)>> {
        let file = self.get_conflict_file(file.to_path_buf()); 
        self.perform_read(file.to_path_buf()).await
    }

    async fn perform_read(&self, file:PathBuf) -> ZResult<Option<(Value, Timestamp)>> {
        // consider file only is it exists, it's a file and in case of "follow_links=true" it doesn't contain symlink
        if file.exists() && file.is_file() && (self.follow_links || !self.contains_symlink(&file)) {
            match File::open(&file) {
                Ok(mut f) => {
                    // TODO: what if file is too big ??
                    let size = f.metadata().map(|m| m.len()).unwrap_or(256);
                    if size <= usize::MAX as u64 {
                        trace!("Read file {:?}", file);
                        let mut content: Vec<u8> = Vec::with_capacity(size as usize);
                        if let Err(e) = f.read_to_end(&mut content) {
                            zerror!(ZErrorKind::Other {
                                descr: format!(r#"Error reading file {:?}: {}"#, file, e)
                            })
                        } else {
                            let (encoding, timestamp) =
                                self.get_encoding_and_timestamp(file.to_path_buf()).await?;
                            Ok(Some((
                                Value {
                                    payload: content.into(),
                                    encoding,
                                },
                                timestamp,
                            )))
                        }
                    } else {
                        zerror!(ZErrorKind::Other {
                            descr: format!(
                                r#"Error reading file {:?}: too big to fit in memory"#,
                                file
                            )
                        })
                    }
                }
                Err(e) => zerror!(ZErrorKind::Other {
                    descr: format!(r#"Error reading file {:?}: {}"#, file, e)
                }),
            }
        } else {
            Ok(None)
        }
    }

    // Search for files matching path_expr.
    pub(crate) fn matching_files<'a>(&self, zpath_expr: &'a str) -> FilesIterator<'a> {
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

    async fn get_encoding_and_timestamp(
        &self,
        file: PathBuf,
    ) -> ZResult<(Encoding, Timestamp)> {
        // try to get Encoding and Timestamp from data_info_mgr
        match self.data_info_mgr.get_encoding_and_timestamp(&file).await? {
            Some((encoding, timestamp)) => Ok((encoding, timestamp)),
            None => {
                trace!("data-info for {:?} not found; fallback to metadata", file);
                let encoding = if self.keep_mime {
                    // fallback: guess mime type from file extension
                    let mime_type = mime_guess::from_path(&file).first_or_octet_stream();
                    Encoding::from(mime_type.essence_str().to_string())
                } else {
                    Encoding::APP_OCTET_STREAM
                };

                // fallback: get timestamp from file's metadata
                let timestamp = self.get_timestamp_from_metadata(file)?;

                Ok((encoding, timestamp))
            }
        }
    }

    pub(crate) async fn get_timestamp(&self, zfile: &ZFile<'_>) -> ZResult<Option<Timestamp>> {
        let file = &zfile.fspath;
        // try to get Timestamp from data_info_mgr
        match self.data_info_mgr.get_timestamp(&file).await? {
            Some(x) => Ok(Some(x)),
            None => {
                // fallback: get timestamp from file's metadata if it exists
                if file.exists() {
                    let timestamp = self.get_timestamp_from_metadata(file)?;
                    Ok(Some(timestamp))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn get_timestamp_from_metadata<P: AsRef<Path>>(&self, file: P) -> ZResult<Timestamp> {
        let metadata = metadata(&file).map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!(
                    "Failed to get meta-data for file {:?}: {}",
                    file.as_ref(),
                    e
                )
            })
        })?;
        let sys_time = metadata
            .modified()
            .or_else(|_| metadata.accessed())
            .or_else(|_| metadata.created())
            .unwrap_or_else(|_| SystemTime::now());
        Ok(Timestamp::new(
            sys_time.duration_since(UNIX_EPOCH).unwrap().into(),
            TimestampId::new(1, [0u8; TimestampId::MAX_SIZE]),
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
                let _ = task::block_on(async move {
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
    zpath_expr: &'a str,
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
                            // zpath is the file's absolute path stripped from base_dir and converted as zenoh path
                            let coarse_zpath =
                            fspath_to_zpath(&s[self.base_dir_len..]);
                            // note: force owning to not have fspath borrowed
                            let zpath = Cow::from(if coarse_zpath.ends_with(CONFLICT_SUFFIX) {
                                coarse_zpath.strip_suffix(CONFLICT_SUFFIX).unwrap_or(coarse_zpath.as_ref())
                            } else{
                                coarse_zpath.as_ref()
                            }.to_owned());
                            // convert it to zenoh path for matching test with zpath_expr
                            if rname::intersect(&zpath, self.zpath_expr) {
                                // matching file; return a ZFile
                                let zfile = ZFile {
                                    zpath,
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
