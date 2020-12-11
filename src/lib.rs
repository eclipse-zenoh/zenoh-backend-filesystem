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
#![feature(async_closure)]

use async_trait::async_trait;
use log::{debug, warn};
use std::convert::TryFrom;
use std::fs::DirBuilder;
use std::io::prelude::*;
use std::path::PathBuf;
use tempfile::tempfile_in;
use zenoh::net::{DataInfo, Sample};
use zenoh::{Change, ChangeKind, Properties, Selector, Value, ZError, ZErrorKind, ZResult};
use zenoh_backend_traits::*;
use zenoh_util::{zenoh_home, zerror, zerror2};

mod data_info_mgt;
mod files_mgt;
use files_mgt::*;

/// The environement variable used to configure the root of all storages managed by this FileSystemBackend.
pub const SCOPE_ENV_VAR: &str = "ZBACKEND_FS_ROOT";

/// The default root (whithin zenoh's home directory) if the ZBACKEND_FS_ROOT environment variable is not specified.
pub const DEFAULT_ROOT_DIR: &str = "zbackend_fs";

// Properies used by the Backend
//  - None

// Properies used by the Storage
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";
pub const PROP_STORAGE_FOLLOW_LINK: &str = "follow_links";
pub const PROP_STORAGE_KEEP_MIME: &str = "keep_mime_types";

#[no_mangle]
pub fn create_backend(properties: &Properties) -> ZResult<Box<dyn Backend>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();

    let root = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
        PathBuf::from(dir)
    } else {
        let mut dir = PathBuf::from(zenoh_home());
        dir.push(DEFAULT_ROOT_DIR);
        dir
    };
    let mut props = properties.clone();
    props.insert("root".into(), root.to_string_lossy().into());

    let admin_status = zenoh::utils::properties_to_json_value(&props);
    Ok(Box::new(FileSystemBackend { admin_status, root }))
}

pub struct FileSystemBackend {
    admin_status: Value,
    root: PathBuf,
}

#[async_trait]
impl Backend for FileSystemBackend {
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    async fn create_storage(&mut self, props: Properties) -> ZResult<Box<dyn Storage>> {
        let path_expr = props.get(PROP_STORAGE_PATH_EXPR).unwrap();
        let path_prefix = props
            .get(PROP_STORAGE_PATH_PREFIX)
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        r#"Missing required property for File System Storage: "{}""#,
                        PROP_STORAGE_PATH_PREFIX
                    )
                })
            })?
            .clone();
        if !path_expr.starts_with(&path_prefix) {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"The specified "{}={}" is not a prefix of "{}={}""#,
                    PROP_STORAGE_PATH_PREFIX, path_prefix, PROP_STORAGE_PATH_EXPR, path_expr
                )
            });
        }

        let read_only = props.contains_key(PROP_STORAGE_READ_ONLY);
        let follow_links = match props.get(PROP_STORAGE_FOLLOW_LINK) {
            Some(s) => {
                if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("yes") {
                    true
                } else if s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("no") {
                    false
                } else {
                    return zerror!(ZErrorKind::Other {
                        descr: format!(
                            r#"Invalid value for File System Storage property "{}={}""#,
                            PROP_STORAGE_FOLLOW_LINK, s
                        )
                    });
                }
            }
            None => false,
        };
        let keep_mime = match props.get(PROP_STORAGE_KEEP_MIME) {
            Some(s) => {
                if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("yes") {
                    true
                } else if s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("no") {
                    false
                } else {
                    return zerror!(ZErrorKind::Other {
                        descr: format!(
                            r#"Invalid value for File System Storage property "{}={}""#,
                            PROP_STORAGE_KEEP_MIME, s
                        )
                    });
                }
            }
            None => true,
        };

        let on_closure = match props.get(PROP_STORAGE_ON_CLOSURE) {
            Some(s) => {
                if s == "delete_all" {
                    OnClosure::DeleteAll
                } else {
                    return zerror!(ZErrorKind::Other {
                        descr: format!("Unsupported value for 'on_closure' property: {}", s)
                    });
                }
            }
            None => OnClosure::DoNothing,
        };

        let base_dir = props
            .get(PROP_STORAGE_DIR)
            .map(|dir| {
                // prepend base_dir with self.root
                let mut base_dir = self.root.clone();
                for segment in dir.split(std::path::MAIN_SEPARATOR) {
                    if !segment.is_empty() {
                        base_dir.push(segment);
                    }
                }
                base_dir
            })
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        r#"Missing required property for File System Storage: "{}""#,
                        PROP_STORAGE_DIR
                    )
                })
            })?;

        // check if base_dir exists and is readable (and writeable if not "read_only" mode)
        let mut dir_builder = DirBuilder::new();
        dir_builder.recursive(true);
        let base_dir_path = PathBuf::from(&base_dir);
        if !base_dir_path.exists() {
            if let Err(err) = dir_builder.create(&base_dir) {
                return zerror!(ZErrorKind::Other {
                    descr: format!(
                        r#"Cannot create File System Storage on "dir"={:?} : {}"#,
                        base_dir, err
                    )
                });
            }
        } else if !base_dir_path.is_dir() {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"Cannot create File System Storage on "dir"={:?} : this is not a directory"#,
                    base_dir
                )
            });
        } else if let Err(err) = base_dir_path.read_dir() {
            return zerror!(ZErrorKind::Other {
                descr: format!(
                    r#"Cannot create File System Storage on "dir"={:?} : {}"#,
                    base_dir, err
                )
            });
        } else if !read_only {
            // try to write a random file
            let _ = tempfile_in(&base_dir)
                .map(|mut f| writeln!(f, "test"))
                .map_err(|err| {
                    zerror2!(ZErrorKind::Other {
                        descr: format!(
                            r#"Cannot create writeable File System Storage on "dir"={:?} : {}"#,
                            base_dir, err
                        )
                    })
                })?;
        }

        let files_mgr = FilesMgr::new(base_dir, follow_links, keep_mime, on_closure).await?;

        let admin_status = zenoh::utils::properties_to_json_value(&props);
        Ok(Box::new(FileSystemStorage {
            admin_status,
            path_prefix,
            files_mgr,
            read_only,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Box<dyn IncomingDataInterceptor>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Box<dyn OutgoingDataInterceptor>> {
        None
    }
}

struct FileSystemStorage {
    admin_status: Value,
    path_prefix: String,
    files_mgr: FilesMgr,
    read_only: bool,
}

impl FileSystemStorage {
    async fn reply_with_matching_files(&self, query: &Query, path_expr: &str) {
        for zfile in self.files_mgr.matching_files(path_expr) {
            self.reply_with_file(query, &zfile).await;
        }
    }

    async fn reply_with_file(&self, query: &Query, zfile: &ZFile<'_>) {
        match self.files_mgr.read_file(&zfile).await {
            Ok(Some((value, timestamp))) => {
                debug!(
                    "Replying to query on {} with file {}",
                    query.res_name(),
                    zfile,
                );
                // append path_prefix to the zenoh path of this ZFile
                let zpath = concat_str(&self.path_prefix, zfile.zpath.as_ref());
                let (encoding, payload) = value.encode();

                let data_info = DataInfo {
                    source_id: None,
                    source_sn: None,
                    first_router_id: None,
                    first_router_sn: None,
                    timestamp: Some(timestamp),
                    kind: None,
                    encoding: Some(encoding),
                };
                query
                    .reply(Sample {
                        res_name: zpath,
                        payload,
                        data_info: Some(data_info),
                    })
                    .await;
            }
            Ok(None) => (), // file not found, do nothing
            Err(e) => warn!(
                "Replying to query on {} : failed to read file {} : {}",
                query.res_name(),
                zfile,
                e
            ),
        }
    }
}

#[async_trait]
impl Storage for FileSystemStorage {
    async fn get_admin_status(&self) -> Value {
        self.admin_status.clone()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<()> {
        // transform the Sample into a Change to get kind, encoding and timestamp (not decoding => RawValue)
        let change = Change::from_sample(sample, false)?;

        // strip path from "path_prefix" and converted to a ZFile
        let zfile = change
            .path
            .as_str()
            .strip_prefix(&self.path_prefix)
            .map(|p| self.files_mgr.to_zfile(p))
            .ok_or_else(|| {
                zerror2!(ZErrorKind::Other {
                    descr: format!(
                        "Received a Sample not starting with path_prefix '{}'",
                        self.path_prefix
                    )
                })
            })?;

        // get latest timestamp for this file (if referenced in data-info db or if exists on disk)
        // and drop incoming sample if older
        if let Some(old_ts) = self.files_mgr.get_timestamp(&zfile).await? {
            if change.timestamp < old_ts {
                debug!("{} on {} dropped: out-of-date", change.kind, change.path);
                return Ok(());
            }
        }

        // Store or delete the sample depending the ChangeKind
        match change.kind {
            ChangeKind::PUT => {
                if !self.read_only {
                    // check that there is a value for this PUT sample
                    if change.value.is_none() {
                        return zerror!(ZErrorKind::Other {
                            descr: format!(
                                "Received a PUT Sample without value for {}",
                                change.path
                            )
                        });
                    }

                    // get the encoding and buffer from the value (RawValue => direct access to inner RBuf)
                    let (encoding, buf) = change.value.unwrap().encode();

                    // write file
                    self.files_mgr
                        .write_file(&zfile, buf, encoding, change.timestamp)
                        .await
                } else {
                    warn!(
                        "Received PUT for read-only Files System Storage on {:?} - ignored",
                        self.files_mgr.base_dir()
                    );
                    Ok(())
                }
            }
            ChangeKind::DELETE => {
                if !self.read_only {
                    // delete file
                    self.files_mgr.delete_file(&zfile, change.timestamp).await
                } else {
                    warn!(
                        "Received DELETE for read-only Files System Storage on {:?} - ignored",
                        self.files_mgr.base_dir()
                    );
                    Ok(())
                }
            }
            ChangeKind::PATCH => {
                warn!("Received PATCH for {}: not yet supported", change.path);
                Ok(())
            }
        }
    }

    // When receiving a Query (i.e. on GET operations)
    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        // get the query's Selector
        let selector = Selector::try_from(&query)?;

        // get the list of sub-path expressions that will match the same stored keys than
        // the selector, if those keys had the path_prefix.
        let path_exprs = utils::get_sub_path_exprs(selector.path_expr.as_str(), &self.path_prefix);
        debug!(
            "Query on {} with path_prefix={} => sub_path_exprs = {:?}",
            selector.path_expr, self.path_prefix, path_exprs
        );

        for path_expr in path_exprs {
            if path_expr.contains('*') {
                self.reply_with_matching_files(&query, path_expr).await;
            } else {
                // path_expr correspond to 1 single file.
                // Convert it to ZFile and reply it.
                let zfile = self.files_mgr.to_zfile(path_expr);
                self.reply_with_file(&query, &zfile).await;
            }
        }

        Ok(())
    }
}

pub(crate) fn concat_str<S1: AsRef<str>, S2: AsRef<str>>(s1: S1, s2: S2) -> String {
    let mut result = String::with_capacity(s1.as_ref().len() + s2.as_ref().len());
    result.push_str(s1.as_ref());
    result.push_str(s2.as_ref());
    result
}
