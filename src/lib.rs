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

use async_trait::async_trait;
use log::{debug, warn};
use zenoh_plugin_trait::{Plugin, plugin_version};
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::prelude::*;
use std::path::PathBuf;
use std::{fs::DirBuilder, sync::Arc};
use tempfile::tempfile_in;
use zenoh::prelude::*;
use zenoh::time::Timestamp;
use zenoh::Result as ZResult;
use zenoh_backend_traits::{
    config::StorageConfig, config::VolumeConfig, Storage, StorageInsertionResult,
    Volume,
};
use zenoh_backend_traits::{Capability, History, Persistence, StoredData, VolumeInstance};
use zenoh_core::{bail, zerror};
use zenoh_util::zenoh_home;

mod data_info_mgt;
mod files_mgt;
use files_mgt::*;

/// The environement variable used to configure the root of all storages managed by this FileSystemBackend.
pub const SCOPE_ENV_VAR: &str = "ZENOH_BACKEND_FS_ROOT";

/// The default root (whithin zenoh's home directory) if the ZENOH_BACKEND_FS_ROOT environment variable is not specified.
pub const DEFAULT_ROOT_DIR: &str = "zenoh_backend_fs";

// Properies used by the Backend
//  - None

// Properies used by the Storage
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";
pub const PROP_STORAGE_FOLLOW_LINK: &str = "follow_links";
pub const PROP_STORAGE_KEEP_MIME: &str = "keep_mime_types";

// Special key for None (when the prefix being stripped exactly matches the key)
pub const NONE_KEY: &str = "@@none_key@@";

pub struct FileSystemBackend {}
zenoh_plugin_trait::declare_plugin!(FileSystemBackend);

impl Plugin for FileSystemBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "filesystem_backend";
    const PLUGIN_VERSION: &'static str = plugin_version!();

    fn start(_name: &str, _config: &Self::StartArgs) -> ZResult<Self::Instance> {
        // For some reasons env_logger is sometime not active in a loaded library.
        // Try to activate it here, ignoring failures.
        let _ = env_logger::try_init();
        debug!("FileSystem backend {}", Self::PLUGIN_VERSION);

        let root_path = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
            PathBuf::from(dir)
        } else {
            let mut dir = PathBuf::from(zenoh_home());
            dir.push(DEFAULT_ROOT_DIR);
            dir
        };
        if let Err(e) = std::fs::create_dir_all(&root_path) {
            bail!(
                r#"Failed to create directory ${{{}}}={}: {}"#,
                SCOPE_ENV_VAR,
                root_path.display(),
                e
            );
        }
        let root = match dunce::canonicalize(&root_path) {
            Ok(dir) => dir,
            Err(e) => bail!(
                r#"Invalid path for ${{{}}}={}: {}"#,
                SCOPE_ENV_VAR,
                root_path.display(),
                e
            ),
        };
        debug!("Using root dir: {}", root.display());

        let mut properties = zenoh::properties::Properties::default();
        properties.insert("root".into(), root.to_string_lossy().into());
        properties.insert("version".into(), Self::PLUGIN_VERSION.into());

        let admin_status = HashMap::from(properties)
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();
        Ok(Box::new(FileSystemVolume { admin_status, root }))
    }
}

pub struct FileSystemVolume {
    admin_status: serde_json::Value,
    root: PathBuf,
}

fn extract_bool(
    from: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    default: bool,
) -> ZResult<bool> {
    match from.get(key) {
        Some(serde_json::Value::Bool(s)) => Ok(*s),
        None => Ok(default),
        _ => bail!(
            r#"Invalid value for File System Storage configuration: `{}` must be a boolean"#,
            key
        ),
    }
}

#[async_trait]
impl Volume for FileSystemVolume {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Durable,
            history: History::Latest,
            read_cost: 0, // for now all reads locally are treared as 0, can optimize later
        }
    }

    async fn create_storage(&self, mut config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("fs backed volumes require volume-specific configuration"),
        };

        let read_only = extract_bool(volume_cfg, PROP_STORAGE_READ_ONLY, false)?;
        let follow_links = extract_bool(volume_cfg, PROP_STORAGE_FOLLOW_LINK, false)?;
        let keep_mime = extract_bool(volume_cfg, PROP_STORAGE_KEEP_MIME, true)?;
        let on_closure = match config.volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "delete_all" => OnClosure::DeleteAll,
            Some(serde_json::Value::String(s)) if s == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            Some(s) => {
                bail!(
                    r#"Unsupported value {:?} for `on_closure` property: must be either "delete_all" or "do_nothing". Default is "do_nothing""#,
                    s
                )
            }
        };

        let base_dir =
            if let Some(serde_json::Value::String(dir)) = config.volume_cfg.get(PROP_STORAGE_DIR) {
                let dir_path = PathBuf::from(dir.as_str());
                if dir_path.is_absolute() {
                    bail!(
                        r#"Invalid property "{}"="{}": the path must be relative"#,
                        PROP_STORAGE_DIR,
                        dir
                    );
                }
                if dir_path
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
                {
                    bail!(
                        r#"Invalid property "{}"="{}": the path must not contain any '..'"#,
                        PROP_STORAGE_DIR,
                        dir
                    );
                }

                // prepend base_dir with self.root
                let mut base_dir = self.root.clone();
                base_dir.push(dir_path);
                base_dir
            } else {
                bail!(
                    r#"Missing required property for File System Storage: "{}""#,
                    PROP_STORAGE_DIR
                )
            };

        // check if base_dir exists and is readable (and writeable if not "read_only" mode)
        let mut dir_builder = DirBuilder::new();
        dir_builder.recursive(true);
        let base_dir_path = PathBuf::from(&base_dir);
        if !base_dir_path.exists() {
            if let Err(err) = dir_builder.create(&base_dir) {
                bail!(
                    r#"Cannot create File System Storage on "dir"={:?} : {}"#,
                    base_dir,
                    err
                )
            }
        } else if !base_dir_path.is_dir() {
            bail!(
                r#"Cannot create File System Storage on "dir"={:?} : this is not a directory"#,
                base_dir
            )
        } else if let Err(err) = base_dir_path.read_dir() {
            bail!(
                r#"Cannot create File System Storage on "dir"={:?} : {}"#,
                base_dir,
                err
            )
        } else if !read_only {
            // try to write a random file
            let _ = tempfile_in(&base_dir)
                .map(|mut f| writeln!(f, "test"))
                .map_err(|err| {
                    zerror!(
                        r#"Cannot create writeable File System Storage on "dir"={:?} : {}"#,
                        base_dir,
                        err
                    )
                })?;
        }

        config
            .volume_cfg
            .as_object_mut()
            .unwrap()
            .insert("dir_full_path".into(), base_dir.to_string_lossy().into());

        log::debug!(
            "Storage on {} will store files in {}",
            config.key_expr,
            base_dir.display()
        );

        let files_mgr = FilesMgr::new(base_dir, follow_links, keep_mime, on_closure).await?;
        Ok(Box::new(FileSystemStorage {
            config,
            files_mgr,
            read_only,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Sync + Send>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Sync + Send>> {
        None
    }
}

struct FileSystemStorage {
    config: StorageConfig,
    files_mgr: FilesMgr,
    read_only: bool,
}

#[async_trait]
impl Storage for FileSystemStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.config.to_json_value()
    }

    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        value: Value,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        if !self.read_only {
            if let Some(k) = key {
                let k = k.as_str();
                let zfile = self.files_mgr.to_zfile(k);
                // write file
                self.files_mgr
                    .write_file(&zfile, value.payload, &value.encoding, &timestamp)
                    .await?;
                Ok(StorageInsertionResult::Inserted)
            } else {
                let zfile = self.files_mgr.to_zfile(NONE_KEY);
                // write file
                self.files_mgr
                    .write_file(&zfile, value.payload, &value.encoding, &timestamp)
                    .await?;
                Ok(StorageInsertionResult::Inserted)
            }
        } else {
            warn!(
                "Received PUT for read-only Files System Storage on {:?} - ignored",
                self.files_mgr.base_dir()
            );
            Err("Received update for read-only File System Storage".into())
        }
    }

    /// Function called for each incoming delete request to this storage.
    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        if !self.read_only {
            if let Some(k) = key {
                let k = k.as_str();
                let zfile = self.files_mgr.to_zfile(k);
                // delete file
                self.files_mgr.delete_file(&zfile).await?;
                Ok(StorageInsertionResult::Deleted)
            } else {
                let zfile = self.files_mgr.to_zfile(NONE_KEY);
                // delete file
                self.files_mgr.delete_file(&zfile).await?;
                Ok(StorageInsertionResult::Deleted)
            }
        } else {
            warn!(
                "Received DELETE for read-only Files System Storage on {:?} - ignored",
                self.files_mgr.base_dir()
            );
            Err("Received update for read-only File System Storage".into())
        }
    }

    /// Function to retrieve the sample associated with a single key.
    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        if key.is_some() {
            let k = key.clone().unwrap();
            let k = k.as_str();
            let zfile = self.files_mgr.to_zfile(k);
            match self.files_mgr.read_file(&zfile).await {
                Ok(Some((value, timestamp))) => Ok(vec![StoredData { value, timestamp }]),
                Ok(None) => Ok(vec![]),
                Err(e) => {
                    Err(format!("Get key {:?} : failed to read file {} : {}", key, zfile, e).into())
                }
            }
        } else {
            let zfile = self.files_mgr.to_zfile(NONE_KEY);
            match self.files_mgr.read_file(&zfile).await {
                Ok(Some((value, timestamp))) => Ok(vec![StoredData { value, timestamp }]),
                Ok(None) => Ok(vec![]),
                Err(e) => {
                    Err(format!("Get key {:?} : failed to read file {} : {}", key, zfile, e).into())
                }
            }
        }
    }

    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let mut result = Vec::new();

        // get all files in the filesystem
        for zfile in self
            .files_mgr
            .matching_files(unsafe { keyexpr::from_str_unchecked("**") })
        {
            let trimmed_zpath = get_trimmed_keyexpr(zfile.zpath.as_ref());
            let trimmed_zfile = self.files_mgr.to_zfile(trimmed_zpath);
            match self.files_mgr.read_file(&trimmed_zfile).await {
                Ok(Some((_, timestamp))) => {
                    // if strip_prefix is set, prefix it back to the zenoh path of this ZFile
                    let zpath = if zfile.zpath.eq(NONE_KEY) {
                        None
                    } else {
                        Some(zfile.zpath.as_ref().try_into().unwrap())
                    };
                    result.push((zpath, timestamp));
                }
                Ok(None) => (), // file not found, do nothing
                Err(e) => warn!(
                    "Getting all entries : failed to read file {} : {}",
                    zfile, e
                ),
            }
        }
        Ok(result)
    }
}
