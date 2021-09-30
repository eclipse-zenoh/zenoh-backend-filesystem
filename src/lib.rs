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

use async_trait::async_trait;
use log::{debug, warn};
use std::fs::DirBuilder;
use std::io::prelude::*;
use std::path::PathBuf;
use tempfile::tempfile_in;
use zenoh::{prelude::*, time::new_reception_timestamp};
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

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static!(
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
);

#[no_mangle]
pub fn create_backend(_unused: &Properties) -> ZResult<Box<dyn Backend>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();
    debug!("FileSystem backend {}", LONG_VERSION.as_str());

    let root = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
        PathBuf::from(dir)
    } else {
        let mut dir = PathBuf::from(zenoh_home());
        dir.push(DEFAULT_ROOT_DIR);
        dir
    };
    debug!("Using root dir: {}", root.display());

    let mut properties = Properties::default();
    properties.insert("root".into(), root.to_string_lossy().into());
    properties.insert("version".into(), LONG_VERSION.clone());

    let admin_status = zenoh::properties::properties_to_json_value(&properties);
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

        let mut props = props.clone();
        props.insert("dir_full_path".into(), base_dir.to_string_lossy().into());

        let files_mgr = FilesMgr::new(base_dir, follow_links, keep_mime, on_closure).await?;

        let admin_status = zenoh::properties::properties_to_json_value(&props);
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
        match self.files_mgr.read_file(zfile).await {
            Ok(Some((value, timestamp))) => {
                debug!(
                    "Replying to query on {} with file {}",
                    query.selector(),
                    zfile,
                );
                // append path_prefix to the zenoh path of this ZFile
                let zpath = concat_str(&self.path_prefix, zfile.zpath.as_ref());
                query
                    .reply(Sample::new(zpath, value).with_timestamp(timestamp))
                    .await;
            }
            Ok(None) => (), // file not found, do nothing
            Err(e) => warn!(
                "Replying to query on {} : failed to read file {} : {}",
                query.selector(),
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
        // let change = Change::from_sample(sample, false)?;

        // strip path from "path_prefix" and converted to a ZFile
        let zfile = sample
            .res_name
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
        let sample_ts = sample.timestamp.unwrap_or_else(new_reception_timestamp);
        if let Some(old_ts) = self.files_mgr.get_timestamp(&zfile).await? {
            if sample_ts < old_ts {
                debug!(
                    "{} on {} dropped: out-of-date",
                    sample.kind, sample.res_name
                );
                return Ok(());
            }
        }

        // Store or delete the sample depending the ChangeKind
        match sample.kind {
            SampleKind::Put => {
                if !self.read_only {
                    // write file
                    self.files_mgr
                        .write_file(
                            &zfile,
                            sample.value.payload,
                            &sample.value.encoding,
                            &sample_ts,
                        )
                        .await
                } else {
                    warn!(
                        "Received PUT for read-only Files System Storage on {:?} - ignored",
                        self.files_mgr.base_dir()
                    );
                    Ok(())
                }
            }
            SampleKind::Delete => {
                if !self.read_only {
                    // delete file
                    self.files_mgr.delete_file(&zfile, &sample_ts).await
                } else {
                    warn!(
                        "Received DELETE for read-only Files System Storage on {:?} - ignored",
                        self.files_mgr.base_dir()
                    );
                    Ok(())
                }
            }
            SampleKind::Patch => {
                warn!("Received PATCH for {}: not yet supported", sample.res_name);
                Ok(())
            }
        }
    }

    // When receiving a Query (i.e. on GET operations)
    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        // get the query's Selector
        let selector = query.selector();

        // get the list of sub-path expressions that will match the same stored keys than
        // the selector, if those keys had the path_prefix.
        let sub_selectors = utils::get_sub_key_selectors(selector.key_selector, &self.path_prefix);
        debug!(
            "Query on {} with path_prefix={} => sub_path_exprs = {:?}",
            selector.key_selector, self.path_prefix, sub_selectors
        );

        for sub_selector in sub_selectors {
            if sub_selector.contains('*') {
                self.reply_with_matching_files(&query, sub_selector).await;
            } else {
                // path_expr correspond to 1 single file.
                // Convert it to ZFile and reply it.
                let zfile = self.files_mgr.to_zfile(sub_selector);
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
