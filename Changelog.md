# Changelog

## Added
 - New API endpoints `DeleteAllByPrefix` and `PutMultipleVersions`. [#47](https://github.com/scalableminds/fossildb/pull/47)
 - New API endpoints `GetMultipleKeysByListWithMultipleVersions` and `PutMultipleKeysWithMultipleVersions` for reading and writing multiple keys/versions in one request. [#48](https://github.com/scalableminds/fossildb/pull/48)
 - `ListKeys` now supports optional `prefix` field
 - New API endpoint `GetMultipleKeysByList`. [#52](https://github.com/scalableminds/fossildb/pull/52)
 - New API endpoint `PutIfVersionMatches`: writes a new version of a key only if the newest stored version is the one the caller expects, and reports the version now in effect. An absent `expectedActualVersion` means the caller expects the key not to exist. This is the primitive a client needs to take a lock or allocate ids without a second coordination service; `applied = false` is another writer having got there first, not an error.

## Breaking Changes

 - The `GetMultipleKeys` call now takes a `startAfterKey` instead of a `key` for pagination. The returned list will only start *after* this key. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Now needs Java 11+

## Fixes

 - Fixed a bug where the pagination for `GetMultipleKeys` could lead to an endless loop if some keys are prefixes of others. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Empty entries are now removed in the response of `GetMultipleKeysByListWithMultipleVersions`. Those could previously occur if no versions matched the requested range. [#51](https://github.com/scalableminds/fossildb/pull/51)
