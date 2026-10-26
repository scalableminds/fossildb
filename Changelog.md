# Changelog

## Added
 - New API endpoints `DeleteAllByPrefix` and `PutMultipleVersions`. [#47](https://github.com/scalableminds/fossildb/pull/47)
 - New API endpoints `GetMultipleKeysByListWithMultipleVersions` and `PutMultipleKeysWithMultipleVersions` for reading and writing multiple keys/versions in one request. [#48](https://github.com/scalableminds/fossildb/pull/48)
 - `ListKeys` now supports optional `prefix` field
 - New API endpoint `GetMultipleKeysByList`. [#52](https://github.com/scalableminds/fossildb/pull/52)

## Breaking Changes

 - The `GetMultipleKeys` call now takes a `startAfterKey` instead of a `key` for pagination. The returned list will only start *after* this key. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Now needs Java 11+
 - A `comparator` set in the RocksDB options file is now ignored, FossilDB always uses RocksDB's default bytewise comparator. A DB created with a different comparator fails to open. [#65](https://github.com/scalableminds/fossildb/pull/65)

## Fixes

 - Fixed reads, deletes and `Put` without version becoming slow next to large deleted ranges. Iterators now stop at the end of the key or prefix being accessed. [#65](https://github.com/scalableminds/fossildb/pull/65)
 - Fixed native resource leaks: `ExportDB` now closes the exported DB (releasing its lock), and backup engines, RocksDB options and other native handles are released.
 - Fixed keys with non-ASCII characters being returned garbled and not matching `get` lookups or `prefix` filters. Keys are now always encoded as UTF-8.
 - Fixed `GetMultipleKeys` becoming extremely slow (and eventually overflowing the stack) on collections with many keys.
 - Fixed a bug where the pagination for `GetMultipleKeys` could lead to an endless loop if some keys are prefixes of others. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Empty entries are now removed in the response of `GetMultipleKeysByListWithMultipleVersions`. Those could previously occur if no versions matched the requested range. [#51](https://github.com/scalableminds/fossildb/pull/51)
