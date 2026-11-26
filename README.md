# <img src="logo.svg" alt="FossilDB" width="200">

Versioned Key-Value Store with RocksDB backend and gRPC API

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/scalableminds/fossildb/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/scalableminds/fossildb/tree/master)

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
  -r, --rocksOptionsFile <filepath>
                           rocksdb options file. Default: None
  --blockCacheSizeMB <MB>  size of the block cache shared by all column families, in MB (10^6 bytes). Has no effect if --rocksOptionsFile is set, then the table options from that file apply. Default: 512
  --maxBackgroundJobs <num>
                           maximum number of concurrent background compactions and flushes (rocksdb max_background_jobs). Overrides the value from --rocksOptionsFile. Default: the value from --rocksOptionsFile if set, else 8
  --compactionThreads <num>
                           number of files CompactAllData rewrites in parallel. Limited so that automatic compactions keep at least one compaction slot once they fall behind. Default: 2
```

## RocksDB Options File
With `--rocksOptionsFile`, collections are **not** stored in the column families of the same name.
FossilDB opens the column families listed in the options file first (in file order, usually starting with `default`),
followed by all collections given with `-c`. The n-th collection in `-c` then uses the n-th of these column families.

For example, with an options file that only lists `default` and `-c collectionA,collectionB`,
`collectionA` is stored in the column family `default` and `collectionB` in the column family `collectionA`.

This mapping is kept for compatibility with existing databases. To keep reading the same data:
 - Always start the database with the same options file, or one that lists the same column families in the same order.
 - Do not reorder or remove collections in `-c`.
 - Do not drop `--rocksOptionsFile` for a database that was created with it, or vice versa.
 - Tools like `ldb` show the column family names, not the collection names.

## Compression
SST files are compressed with lz4, and with zstd for the bottommost level, which holds most of the data.
This applies to column families that are not configured via `--rocksOptionsFile`.

Older FossilDB versions used snappy for all levels. When opening such a database, the existing snappy files
stay readable and new files use lz4/zstd, so the database contains a mix of both. Old files are only converted
when a compaction rewrites them, which may never happen for data that isn't written anymore.
To convert all files at once, call `CompactAllData`. The call returns once all files are converted.

`CompactAllData` rewrites each SST file in level 1 and below whose compression differs from the current options,
one file at a time (`--compactionThreads` files in parallel). Each rewrite is a short compaction of a single file
(about 15 s for 1 GB), so the database stays available and writes continue meanwhile.

RocksDB counts each rewrite as a running compaction. While compactions keep up, RocksDB runs only one at a time, so
automatic compactions wait while `CompactAllData` runs, and level 0 grows by a few files. Once level 0 reaches the
speed-up threshold (8 files with the default options), RocksDB runs more compactions in parallel
(`--maxBackgroundJobs`; with the default of 8, up to 6, 2 of them used by `CompactAllData`), and automatic
compactions catch up, well before RocksDB would slow down writes (20 files). Reads may be somewhat slower meanwhile.
On large databases it still runs for hours with heavy disk I/O, and it needs free disk space for the rewritten files.
The progress is logged after each file.

Files that are already converted are skipped, so if the call is interrupted (e.g. by a restart), calling it again
continues where it stopped. It does not merge files or move data to other levels, so it doesn't drop deleted entries
that are in other files than the data they delete, and doesn't free their space. To do that, call `CompactAllData`
with `full = true`. This compacts all data into the bottommost level at once, as `CompactAllData` did before. It
takes compaction slots for hours on large databases, so automatic compactions fall behind and writes may stall
meanwhile. Level-0 files (at most a few per collection) are left as they are,
as they can only be compacted together with most of the level below. Normal compaction rewrites them once more data is
written.

## API
FossilDB can be used via its [gRPC API](src/main/protobuf/fossildbapi.proto).
