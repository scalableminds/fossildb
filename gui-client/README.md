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

- **Connections**: direct or through an SSH tunnel. Recent connections can be named, e.g. "production"; the name
  is shown in the window title and in confirmations. Choosing a recent connection fills in its values; double-click
  or Enter connects. Right-click it to rename or remove it.
  A dot shows whether a recent direct connection answers (green) or not (gray; the tooltip says why). FossilDB
  servers listening on this computer, e.g. in Docker, are found and listed as well.
- **Overview**: server version, disk usage and write status, and a table of the collections: size on disk (with a
  bar for comparison), estimated number of versions, compression, and warnings for pending compactions or many deletion
  markers. Hover over a collection for all its statistics (uncompressed size, LSM levels, first and last key, …).
  Open a collection with a left click or double-click, depending on the desktop setting; right-click to copy.
- **Sizes** are shown in decimal units (kB, MB, GB) by default; the View menu switches to binary ones (KiB, MiB,
  GiB).
- **Older servers** (from 0.1.37) can't list their collections or report statistics. Enter the collection name
  instead; WEBKNOSSOS collections and recently used ones are suggested.
- **Keys**: loaded page by page while scrolling, filtered by prefix. Version counts are loaded for visible rows.
  Any key can be opened directly by name.
- **Versions**: listed newest first, 1 000 at a time, more while scrolling. "Go to version" opens any version
  directly; if it doesn't exist, the next older one is shown. In the key list, version counts stop at 10 000.
- **Values**, for each version:
  - *Protobuf (raw)*: protobuf wire format decoded without a schema, with other interpretations of each field
    (signed integers, floats, packed numbers).
  - *Protobuf (typed)*: decoded with a message type from `.proto` files (Schemas → Load .proto Folder…). The
    message type is remembered per collection. *Guess* finds the type that knows all fields of the value. The
    *JSON* button copies or saves the value as JSON, with the field names of the schema.
  - *Text* and *JSON*: for values that are text, the first 1 MiB. JSON is shown as a collapsible tree, or
    pretty-printed (Raw).
  - *Hex*: hex dump of the first 256 KiB.
  - Numbers whose field or key name contains "timestamp" are also shown as ISO date (UTC), read as milliseconds
    or seconds, whichever gives a date between 1990 and 2100.
  - The trees (Protobuf and JSON) can be filtered by text, keeping the matches with their parents and contents,
    and expanded or collapsed completely. Hover over a size for its tag, length prefix and value bytes.
  - Tabs that don't apply to a value, e.g. Text for binary data, are disabled.
  - Download as a file.
- **Writes** (off by default, Database → Enable Writes, per window): delete a version, delete all versions of a key,
  delete all keys with a prefix, compact all data, create a backup (replaces the previous one; the server keeps only
  the newest), export the database into a new directory on the server. Each asks for confirmation; deleting by
  prefix shows how many keys match and requires typing the prefix.

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
