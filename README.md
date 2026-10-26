# <img src="logo.svg" alt="FossilDB" width="200">

Versioned Key-Value Store with RocksDB backend and gRPC API

[![CircleCI](https://circleci.com/gh/scalableminds/fossildb.svg?style=svg&circle-token=89b8341a216f2ce9e8f40c913b45196b3694347a)](https://circleci.com/gh/scalableminds/fossildb)

## Installation & Usage
You can download the [executable jar](https://github.com/scalableminds/fossildb/releases/latest),
```
java -jar fossildb.jar -c default
```
or use a [docker image](https://hub.docker.com/r/scalableminds/fossildb/tags) and run
```
docker run scalableminds/fossildb:master fossildb -c default
```

For further options, see `help`:
```
  -p, --port <num>         port to listen on. Default: 7155
  -d, --dataDir <path>     database directory. Default: data
  -b, --backupDir <path>   backup directory. Default: backup
  -c, --columnFamilies <cf1>,<cf2>...
                           column families of the database (created if there is no db yet)
```

## Compression
SST files are compressed with lz4, and with zstd for the bottommost level, which holds most of the data.
This applies to column families that are not configured via `--rocksOptionsFile`.

Older FossilDB versions used snappy for all levels. When opening such a database, the existing snappy files
stay readable and new files use lz4/zstd, so the database contains a mix of both. Old files are only converted
when a compaction rewrites them, which may never happen for data that isn't written anymore.
To convert all files at once, call `CompactAllData`. It rewrites the whole database, so it causes heavy
disk I/O for a while and needs free disk space for the rewritten files. The database stays available meanwhile.

## API
FossilDB can be used via its [gRPC API](src/main/protobuf/fossildbapi.proto).
