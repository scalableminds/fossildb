# Changelog

## Added
 - New API endpoint `GetDatabaseInfo`, returning information about the database and its collections (estimated version counts, sizes, compression, LSM levels, memtables, compaction state, first and last key). It reads only RocksDB metadata, so it stays fast on large databases.
 - New `--blockCacheSizeMB` option (default 512) for a block cache shared by all column families. It does not apply when `--rocksOptionsFile` is set. [#68](https://github.com/scalableminds/fossildb/pull/68)
 - New API endpoints `DeleteAllByPrefix` and `PutMultipleVersions`. [#47](https://github.com/scalableminds/fossildb/pull/47)
 - New API endpoints `GetMultipleKeysByListWithMultipleVersions` and `PutMultipleKeysWithMultipleVersions` for reading and writing multiple keys/versions in one request. [#48](https://github.com/scalableminds/fossildb/pull/48)
 - `ListKeys` now supports optional `prefix` field
 - New API endpoint `GetMultipleKeysByList`. [#52](https://github.com/scalableminds/fossildb/pull/52)

## Changed
- New SST files use lz4 compression, and zstd for the bottommost level, instead of snappy for all levels. Existing snappy files stay readable, but are only converted when a compaction rewrites them, which may never happen for old data. Run `CompactAllData` to convert all files at once. See [Compression](README.md#compression). [#66](https://github.com/scalableminds/fossildb/pull/66)

## Breaking Changes

 - The `GetMultipleKeys` call now takes a `startAfterKey` instead of a `key` for pagination. The returned list will only start *after* this key. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Now needs Java 11+

## Fixes

 - Fixed `CompactAllData` and `ExportDB` not compacting any data. They only compacted the (empty) default column family. Now they compact all column families, including their bottommost level.
 - Fixed native resource leaks: `ExportDB` now closes the exported DB (releasing its lock), and backup engines, RocksDB options and other native handles are released.
 - Fixed keys with non-ASCII characters being returned garbled and not matching `get` lookups or `prefix` filters. Keys are now always encoded as UTF-8.
 - Fixed `GetMultipleKeys` becoming extremely slow (and eventually overflowing the stack) on collections with many keys.
 - `PutMultipleVersions`, `PutMultipleKeysWithMultipleVersions`, `DeleteMultipleVersions` and `DeleteAllByPrefix` are now atomic: if the request fails, nothing is written.
 - Fixed a bug where the pagination for `GetMultipleKeys` could lead to an endless loop if some keys are prefixes of others. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Empty entries are now removed in the response of `GetMultipleKeysByListWithMultipleVersions`. Those could previously occur if no versions matched the requested range. [#51](https://github.com/scalableminds/fossildb/pull/51)
