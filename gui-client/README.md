# FossilDB Client

Desktop app (macOS, Linux) for browsing and managing FossilDB databases. It is meant to replace the terminal client
in [`../client/interactive`](../client/interactive).

## Running

With [uv](https://docs.astral.sh/uv/):

```
cd gui-client
uv run fossildb-gui                                      # choose a server in the connect window
uv run fossildb-gui localhost:7155                       # connect right away
uv run fossildb-gui localhost:7155 --ssh user@server     # through an SSH tunnel
```

The API code is generated from [`../src/main/protobuf/fossildbapi.proto`](../src/main/protobuf/fossildbapi.proto) on
the first start, and again whenever that file changes.

## Features

- **Overview**: server version, disk usage, write status, and statistics for every collection (estimated number of
  versions, size on disk and uncompressed, compression, LSM levels, pending compaction, first and last key).
  Double-click a collection to open it.
- **Older servers** (from 0.1.37) can't list their collections or report statistics. Enter the collection name
  instead; WEBKNOSSOS collections and recently used ones are suggested.
- **Keys**: loaded page by page while scrolling, filtered by prefix. Version counts are loaded for visible rows.
  Any key can be opened directly by name.
- **Values**, for each version:
  - *Structure*: protobuf wire format decoded without a schema, with other interpretations of each field
    (signed integers, floats, packed numbers).
  - *Typed*: decoded with a message type from `.proto` files (Schemas → Load .proto Folder…). The message type is
    remembered per collection.
  - *Hex*: hex dump of the first 256 KiB.
  - Download as a file.
- **Writes** (off by default, Database → Enable Writes, per window): delete a version, delete all versions of a key,
  delete all keys with a prefix, compact all data. Each asks for confirmation; deleting by prefix shows how many
  keys match and requires typing the prefix.

## SSH tunnels

With *SSH tunnel*, the app runs the system's `ssh` to forward a local port to the FossilDB address as seen from the
SSH host (usually `localhost:7155`). Your `~/.ssh/config` applies. There is no terminal to type a password or key
passphrase into, so authentication must work without one, e.g. with keys loaded in the ssh agent. The tunnel is
closed with the window.

## Development

```
uv run pytest
```

Tests use an in-process fake FossilDB server; Qt runs offscreen.

The app icons in `src/fossildb_gui/resources` are rendered from the repository's `icon.svg`.
After changing it, run `uv run python scripts/render_icon.py`.
