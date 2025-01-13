# Changelog

## Added
 - New API endpoints `DeleteAllByPrefix` and `PutMultipleVersions`. [#47](https://github.com/scalableminds/fossildb/pull/47)
 - New API endpoints `GetMultipleKeysByListWithMultipleVersions` and `PutMultipleKeysWithMultipleVersions` for reading and writing multiple keys/versions in one request. [#48](https://github.com/scalableminds/fossildb/pull/48)
 - `ListKeys` now supports optional `prefix` field

## Breaking Changes

 - The `GetMultipleKeys` call now takes a `startAfterKey` instead of a `key` for pagination. The returned list will only start *after* this key. [#38](https://github.com/scalableminds/fossildb/pull/38)
 - Now needs Java 11+

## Fixes

 - Fixed a bug where the pagination for `GetMultipleKeys` could lead to an endless loop if some keys are prefixes of others. [#38](https://github.com/scalableminds/fossildb/pull/38)
