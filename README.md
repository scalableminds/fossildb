# FossilDB

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
