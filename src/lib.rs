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

use std::{collections::HashMap, fs::DirBuilder, future::Future, io::prelude::*, path::PathBuf};

use async_trait::async_trait;
use tempfile::tempfile_in;
use tracing::{debug, warn};
use zenoh::{
    internal::{bail, zenoh_home, zerror, Value},
    key_expr::{keyexpr, OwnedKeyExpr},
    query::Parameters,
    time::Timestamp,
    try_init_log_from_env, Result as ZResult,
};
use zenoh_backend_traits::{
    config::{StorageConfig, VolumeConfig},
    Capability, History, Persistence, Storage, StorageInsertionResult, StoredData, Volume,
    VolumeInstance,
};
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};

mod data_info_mgt;
mod files_mgt;
use files_mgt::*;

const WORKER_THREAD_NUM: usize = 2;
const MAX_BLOCK_THREAD_NUM: usize = 50;
lazy_static::lazy_static! {
    // The global runtime is used in the dynamic plugins, which we can't get the current runtime
    static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
               .worker_threads(WORKER_THREAD_NUM)
               .max_blocking_threads(MAX_BLOCK_THREAD_NUM)
               .enable_all()
               .build()
               .expect("Unable to create runtime");
}
#[inline(always)]
fn blockon_runtime<F: Future>(task: F) -> F::Output {
    // Check whether able to get the current runtime
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            // Able to get the current runtime (standalone binary), spawn on the current runtime
            tokio::task::block_in_place(|| rt.block_on(task))
        }
        Err(_) => {
            // Unable to get the current runtime (dynamic plugins), spawn on the global runtime
            tokio::task::block_in_place(|| TOKIO_RUNTIME.block_on(task))
        }
    }
}

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
pub const ROOT_KEY: &str = "@root";

pub struct FileSystemBackend {}

#[cfg(feature = "dynamic_plugin")]
zenoh_plugin_trait::declare_plugin!(FileSystemBackend);

impl Plugin for FileSystemBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "filesystem_backend";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(_name: &str, _config: &Self::StartArgs) -> ZResult<Self::Instance> {
        try_init_log_from_env();
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

        let mut parameters = Parameters::default();
        parameters.insert::<String, String>("root".into(), root.to_string_lossy().into());
        parameters.insert::<String, String>("version".into(), Self::PLUGIN_VERSION.into());

        let admin_status = HashMap::from(parameters)
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

        tracing::debug!(
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
                    .write_file(
                        &zfile,
                        value.payload().into(),
                        value.encoding().clone(),
                        &timestamp,
                    )
                    .await?;
                Ok(StorageInsertionResult::Inserted)
            } else {
                let zfile = self.files_mgr.to_zfile(ROOT_KEY);
                // write file
                self.files_mgr
                    .write_file(
                        &zfile,
                        value.payload().into(),
                        value.encoding().clone(),
                        &timestamp,
                    )
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
                let zfile = self.files_mgr.to_zfile(ROOT_KEY);
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
            let zfile = self.files_mgr.to_zfile(ROOT_KEY);
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
        // Add the root entry if it exists.
        // Root key can't be acuired from `matching_files` call
        // because it's name is specially chosen to be not allowed as key value ("@root")
        if let Some((_, timestamp)) = self
            .files_mgr
            .read_file(&self.files_mgr.to_zfile(ROOT_KEY))
            .await?
        {
            result.push((None, timestamp));
        }
        // Get all files in the filesystem.
        // Also skip the root key file which was already added above.
        // This is just for completeness, it's skipped anyway due to it's name starting from '@'
        for zfile in self
            .files_mgr
            .matching_files(unsafe { keyexpr::from_str_unchecked("**") })
            .filter(|zfile| zfile.zpath != ROOT_KEY)
        {
            let trimmed_zpath = get_trimmed_keyexpr(zfile.zpath.as_ref());
            let trimmed_zfile = self.files_mgr.to_zfile(trimmed_zpath);
            match self.files_mgr.read_file(&trimmed_zfile).await {
                Ok(Some((_, timestamp))) => {
                    let zpath = Some(zfile.zpath.as_ref().try_into().unwrap());
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
