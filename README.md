<img src="http://zenoh.io/img/zenoh-dragon-small.png" width="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-filesystem/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-filesystem/actions?query=workflow%3A%22CI%22)
[![Gitter](https://badges.gitter.im/atolab/zenoh.svg)](https://gitter.im/atolab/zenoh?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# File system backend for Eclipse zenoh

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on the host's file system to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zbackend_fs`**.

-------------------------------
## **Examples of usage**

Using `curl` on the zenoh router to add backend and storages:
```bash
# Add a backend that will have all its storages storing data in subdirectories of ${ZBACKEND_FS_ROOT} directory.
# ${ZBACKEND_FS_ROOT} is an environment variable that must be set before starting the zenoh router.
# If not set, it defaults to "~/.zenoh/zbackend_fs/"
curl -X PUT -H 'content-type:application/properties' http://localhost:8000/@/router/local/plugin/storages/backend/fs

# Add a storage on /demo/example/** storing data in files under ${ZBACKEND_FS_ROOT}/test/ directory
# We use 'path_prefix=/demo/example' thus a zenoh path "/demo/example/a/b" will be stored as "${ZBACKEND_FS_ROOT}/test/a/b"
curl -X PUT -H 'content-type:application/properties' -d "path_expr=/demo/example/**;path_prefix=/demo/example;dir=test" http://localhost:8000/@/router/local/plugin/storages/backend/fs/storage/example

# Add a storage that will expose the same files than an Apache HTTP server, in read-only mode
# this assumes that ${ZBACKEND_FS_ROOT} is set to the Apache DocumentRoot (e.g. "/usr/web")
curl -X PUT -H 'content-type:application/properties' -d "path_expr=/www.test.org/**;path_prefix=/www.test.org;dir=test.org;read_only" http://localhost:8000/@/router/local/plugin/storages/backend/fs/storage/test.org
```

-------------------------------
## **Properties for Backend creation**

- **`"lib"`** (optional) : the path to the backend library file. If not speficied, the Backend identifier in admin space must be `fs` (i.e. zenoh will automatically search for a library named `zbackend_fs`).

-------------------------------
## **Properties for Storage creation**

- **`"path_expr"`** (**required**) : the Storage's [Path Expression](../abstractions#path-expression)

- **`"path_prefix"`** (optional) : a prefix of the `"path_expr"` that will be stripped from each path to store.  
  _Example: with `"path_expr"="/demo/example/**"` and `"path_prefix"="/demo/example/"` the path `"/demo/example/foo/bar"` will be stored as key: `"foo/bar"`. But replying to a get on `"/demo/**"`, the key `"foo/bar"` will be transformed back to the original path (`"/demo/example/foo/bar"`)._

- **`"dir"`** (**required**) : The directory that will be used to store

- **`"read_only"`** (optional) : the storage will only answer to GET queries. It will not accept any PUT or DELETE message, and won't write any file. Not set by default. *(the value doesn't matter, only the property existence is checked)*

- **`"on_closure"`** (optional) : the strategy to use when the Storage is removed. There are 2 options:
  - *unset*: the storage's directory remains untouched (this is the default behaviour)
  - `"delete_all"`: the storage's directory is deleted with all its content.

- **`"follow_links"`** (optional) : If set to `true` the storage will follow the symbolic links. The default value is `false`.

-------------------------------
## **Behaviour of the backend**

### Mapping to file system
Each **storage** will map to a directory with path: `${ZBACKEND_FS_ROOT}/<dir>`, where:
  * `${ZBACKEND_FS_ROOT}` is an environment variable that could be specified before zenoh router startup.
     If this variable is not specified `${ZENOH_HOME}/zbackend_fs` will be used
     (where the default value of `${ZENOH_HOME}` is `~/.zenoh`).
  * `<dir>` is the `"dir"` propertiy specified at storage creation.
Each **key/value** put into the storage will map to a file within the storage's directory where:
  * the file path will be `${ZBACKEND_FS_ROOT}/<dir>/<relative_zenoh_path>`, where `<relative_zenoh_path>``
    will be the zenoh path used as key, stripped from the `"path_prefix"` property specified at storage creation.
  * the content of the file will be the value written as a RawValue. I.e. the same bytes buffer that has been
    transported by zenoh. For UTF-8 compatible formats (StringUTF8, JSon, Integer, Float...) it means the file
    will be readable as a text format.
 * the encoding and the timestamp of the key/value will be stored in a RocksDB database stored in the storage directory.

### Behaviour on deletion
On deletion of a path, the corresponding file is removed. An entry with deletion timestamp is inserted in the
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
## How to build it

Install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). Currently, zenoh requires a nightly version of Rust, type the following to install it after you have followed the previous instructions:

```bash
$ rustup default nightly
```

And then build the backend with:

```bash
$ cargo build --release --all-targets
```
