# Changelog

## Added
 - New API endpoints `DeleteAllByPrefix` and `PutMultipleVersions`. [#47](https://github.com/scalableminds/fossildb/pull/47)
 - New API endpoints `GetMultipleKeysByListWithMultipleVersions` and `PutMultipleKeysWithMultipleVersions` for reading and writing multiple keys/versions in one request. [#48](https://github.com/scalableminds/fossildb/pull/48)
 - `ListKeys` now supports optional `prefix` field
 - New API endpoint `GetMultipleKeysByList`. [#52](https://github.com/scalableminds/fossildb/pull/52)

## Breaking Changes

 - The `GetMultipleKeys` call now takes a `startAfterKey` instead of a `key` for pagination. The returned list will only start *after* this key. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Now needs Java 17+ (Scala 3.10). [#71](https://github.com/scalableminds/fossildb/pull/71)
 - Upgraded RocksDB from 9.4 to 11.8. Data and backups written by this version may not be readable by older FossilDB versions, so make a backup before upgrading if you might need to roll back. [#71](https://github.com/scalableminds/fossildb/pull/71)

## Fixes

 - Fixed native resource leaks: `ExportDB` now closes the exported DB (releasing its lock), and backup engines, RocksDB options and other native handles are released.
 - Fixed keys with non-ASCII characters being returned garbled and not matching `get` lookups or `prefix` filters. Keys are now always encoded as UTF-8.
 - Fixed `GetMultipleKeys` becoming extremely slow (and eventually overflowing the stack) on collections with many keys.
 - Fixed a bug where the pagination for `GetMultipleKeys` could lead to an endless loop if some keys are prefixes of others. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Empty entries are now removed in the response of `GetMultipleKeysByListWithMultipleVersions`. Those could previously occur if no versions matched the requested range. [#51](https://github.com/scalableminds/fossildb/pull/51)
