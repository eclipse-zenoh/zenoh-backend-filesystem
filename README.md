<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/zenoh-dragon.png" height="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-filesystem/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-filesystem/actions?query=workflow%3A%22CI%22)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/2GJ958VuHs)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Eclipse Zenoh

The Eclipse Zenoh: Zero Overhead Pub/sub, Store/Query and Compute.

Zenoh (pronounce _/zeno/_) unifies data in motion, data at rest and computations. It carefully blends traditional pub/sub with geo-distributed storages, queries and computations, while retaining a level of time and space efficiency that is well beyond any of the mainstream stacks.

Check the website [zenoh.io](http://zenoh.io) and the [roadmap](https://github.com/eclipse-zenoh/roadmap) for more detailed information.

-------------------------------
# File system backend

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](https://zenoh.io/docs/manual/abstractions/#storage) for more details.

This backend relies on the host's file system to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zenoh_backend_fs`**.


:point_right: **Install latest release:** see [below](#How-to-install-it)

:point_right: **Build "main" branch:** see [below](#How-to-build-it)

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router (`zenohd`) installed, and the `zenoh_backend_fs` library file is available in `~/.zenoh/lib`.
 - Declare the `ZENOH_BACKEND_FS_ROOT` environment variable to the directory where you want the files to be stored (or exposed from).
   If you don't declare it, the `~/.zenoh/zenoh_backend_fs` directory will be used.

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API.
### **Setup via a JSON5 configuration file**

  - Create a `zenoh.json5` configuration file containing:
    ```json5
    {
      plugins: {
        // configuration of "storage-manager" plugin:
        storage_manager: {
          volumes: {
            // configuration of a "fs" volume (the "zenoh_backend_fs" backend library will be loaded at startup)
            fs: {},
          },
          storages: {
            // configuration of a "demo" storage using the "fs" volume
            demo: {
              // the key expression this storage will subscribes to
              key_expr: "demo/example/**",
              // this prefix will be stripped from the received key when converting to file path
              // this argument is optional.
              strip_prefix: "demo/example",
              volume: {
                id: "fs",
                // the key/values will be stored as files within this directory (relative to ${ZENOH_BACKEND_FS_ROOT})
                dir: "example"
              }
            }
          }
        },
        // Optionally, add the REST plugin
        rest: { http_port: 8000 }
      }
    }
    ```
  - Run the zenoh router with:  
    `zenohd -c zenoh.json5`

### **Setup at runtime via `curl` commands on the admin space**

  - Run the zenoh router, with write permissions to its admin space:
    `zenohd --adminspace-permissions rw`
  - Add the "fs" backend (the "zenoh_backend_fs" library will be loaded):  
    `curl -X PUT -H 'content-type:application/json' -d '{}' http://localhost:8000/@/router/local/config/plugins/storage_manager/volumes/fs`
  - Add the "demo" storage using the "fs" backend:  
    `curl -X PUT -H 'content-type:application/json' -d '{key_expr:"demo/example/**",strip_prefix:"demo/example", volume: {id: "fs", dir:"example"}}' http://localhost:8000/@/router/local/config/plugins/storage_manager/storages/demo`

### **Tests using the REST API**

Using `curl` to publish and query keys/values, you can:
```bash
# Put values that will be stored under ${ZENOH_BACKEND_FS_ROOT}/example
curl -X PUT -d "TEST-1" http://localhost:8000/demo/example/test-1
curl -X PUT -d "B" http://localhost:8000/demo/example/a/b

# Retrive the values
curl http://localhost:8000/demo/example/**
```

<!-- TODO: after release of eclipse/zenoh:0.6.0 update wrt. conf file and uncomment this:

### **Usage with `eclipse/zenoh` Docker image**
Alternatively, you can test the zenoh router in a Docker container:
 - Download the [docker-compose.yml](https://github.com/eclipse-zenoh/zenoh-backend-filesystem/blob/main/docker-compose.yml) file
 - In the same directory, create the `./zenoh_docker/lib` sub-directories and place the `libzenoh_backend_fs.so` library
   for `x86_64-unknown-linux-musl` target within.
 - Also create a `./zenoh_filesystem/test` directory that will be used for the storage.
 - Start the containers running
   ```bash
   docker-compose up -d
   ```
 - Run the `curl` commands above, and explore the resulting file in `./zenoh_filesystem/test`
-->

-------------------------------
## Configuration
### Extra configuration for filesystem-backed volumes

Volumes using the `fs` backend don't need any extra configuration at the volume level. Any volume can use the `fs` backend by specifying the value `"fs"` for the `backend` configuration key. A volume named `fs` will automatically be backed by the `fs` backend if no other backend is specified.

-------------------------------
### Storage-level configuration for filesystem-backed volumes

Storages relying on a `fs` backed volume must/can specify additional configuration specific to that volume, as shown in the example [above](#setup-via-a-json5-configuration-file):
- `dir` (**required**, string) : The directory that will be used to store data.

- `read_only` (optional, boolean) : the storage will only answer to GET queries. It will not accept any PUT or DELETE message, and won't write any file. `false` by default.

- `on_closure` (optional, string) : the strategy to use when the Storage is removed. There are 2 options:
  - `"do_nothing"`: the storage's directory remains untouched (this is the default behaviour)
  - `"delete_all"`: the storage's directory is deleted with all its content.

- `follow_links` (optional, boolean) : If set to `true` the storage will follow the symbolic links. The default value is `false`.

- `keep_mime_types` (optional, boolean) : When replying to a GET query with a file for which the zenoh encoding is not known, the storage guess its mime-type according to the file extension. If the mime-type doesn't correspond to a supported zenoh encoding, this option will drive the returned value:
   - `true` (default value): a [Custom value](https://docs.rs/zenoh/latest/zenoh/enum.Value.html#variant.Custom)
     is returned with the description set to the mime-type.
   - `false`: a [Raw value](https://docs.rs/zenoh/latest/zenoh/enum.Value.html#variant.Raw) with
     APP_OCTET_STREAM encoding is returned.

-------------------------------
## **Behaviour of the backend**

### Mapping to file system
Each **storage** will map to a directory with path: `${ZENOH_BACKEND_FS_ROOT}/<dir>`, where:
  * `${ZENOH_BACKEND_FS_ROOT}` is an environment variable that could be specified before zenoh router startup.
     If this variable is not specified `${ZENOH_HOME}/zenoh_backend_fs` will be used
     (where the default value of `${ZENOH_HOME}` is `~/.zenoh`).
  * `<dir>` is the `"dir"` property specified at storage creation.
Each zenoh **key/value** put into the storage will map to a file within the storage's directory where:
  * the file path will be `${ZENOH_BACKEND_FS_ROOT}/<dir>/<relative_zenoh_key>`, where `<relative_zenoh_key>`
    will be the zenoh key, stripped from the `"strip_prefix"` property specified at storage creation.
  * the content of the file will be the value written as a RawValue. I.e. the same bytes buffer that has been
    transported by zenoh. For UTF-8 compatible formats (StringUTF8, JSon, Integer, Float...) it means the file
    will be readable as a text format.
  * the encoding and the timestamp of the key/value will be stored in a RocksDB database stored in the storage directory.

### Behaviour on deletion

On deletion of a key, the corresponding file is removed. An entry with deletion timestamp is inserted in the
RocksDB database (to avoid re-insertion of points with an older timestamp in case of un-ordered messages).  
At regular interval, a task cleans-up the RocksDB database from entries with old timestamps that don't have a
corresponding existing file.

### Behaviour on GET

On GET operations, the storage searches for matching and existing files, and return their raw content as a reply.
For each, the encoding and timestamp are retrieved from the RocksDB database. But if no entry is found in the
database for a file (e.g. for files created without zenoh), the encoding is deduced from the file's extension
(using [mime_guess](https://crates.io/crates/mime_guess)), and the timestamp is deduced from the file's
modification time.

-------------------------------
## How to install it

To install the latest release of this backend library, you can do as follows:

### Manual installation (all platforms)

All release packages can be downloaded from:  
 - https://download.eclipse.org/zenoh/zenoh-backend-filesystem/latest/   

Each subdirectory has the name of the Rust target. See the platforms each target corresponds to on https://doc.rust-lang.org/stable/rustc/platform-support.html

Choose your platform and download the `.zip` file.  
Unzip it in the same directory than `zenohd` or to any directory where it can find the backend library (e.g. /usr/lib or ~/.zenoh/lib)

### Linux Debian

Add Eclipse Zenoh private repository to the sources list, and install the `zenoh-backend-filesystem` package:

```bash
echo "deb [trusted=yes] https://download.eclipse.org/zenoh/debian-repo/ /" | sudo tee -a /etc/apt/sources.list.d/zenoh.list > /dev/null
sudo apt update
sudo apt install zenoh-backend-filesystem
```

-------------------------------
## How to build it

> :warning: **WARNING** :warning: : Zenoh and its ecosystem are under active development. When you build from git, make sure you also build from git any other Zenoh repository you plan to use (e.g. binding, plugin, backend, etc.). It may happen that some changes in git are not compatible with the most recent packaged Zenoh release (e.g. deb, docker, pip). We put particular effort in mantaining compatibility between the various git repositories in the Zenoh project. 

At first, install [Clang](https://clang.llvm.org/) and [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). If you already have the Rust toolchain installed, make sure it is up-to-date with:

```bash
$ rustup update
```

> :warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
built with the exact same Rust version than `zenohd`, and using for `zenoh` dependency the same version (or commit number) than 'zenohd'.
Otherwise, incompatibilities in memory mapping of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.  
Example:
```bash
$ zenohd --version
The zenoh router v0.6.0-beta.1 built with rustc 1.64.0 (a55dd71d5 2022-09-19)
```
Here, `zenohd` has been built with the rustc version `1.64.0`.  
Install and use this toolchain with the following command:

```bash
$ rustup default 1.64.0
```

And `zenohd` version corresponds to an un-released commit with id `1f20c86`. Update the `zenoh` dependency in Cargo.lock with this command:
```bash
$ cargo update -p zenoh --precise 1f20c86
```

Then build the backend with:
```bash
$ cargo build --release --all-targets
```
