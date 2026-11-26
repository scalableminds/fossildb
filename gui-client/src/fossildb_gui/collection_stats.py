"""Texts about the server and its collections, from GetDatabaseInfo: warnings, details and totals."""

import html
import time

from fossildb_gui import fmt

# Deletion markers are worth a warning from this share of all entries on, since they slow down reads until a
# compaction removes them
DELETIONS_WARNING_SHARE = 0.1


def uncompressed_bytes(c) -> int:
    return c.rawKeyBytes + c.rawValueBytes


def ratio(c) -> float | None:
    """Compression ratio: uncompressed size / size on disk."""
    return uncompressed_bytes(c) / c.liveSstFilesBytes if c.liveSstFilesBytes else None


def compression(c) -> str:
    """E.g. "8.1× ZSTD"."""
    types = ", ".join(sorted({name for level in c.levels for name in level.compressions}))
    value = ratio(c)
    return " ".join(part for part in (f"{value:.1f}×" if value else "", types) if part)


def deletions(c) -> int:
    return c.sstDeletions + c.memtableDeletions


def warnings(c) -> list[str]:
    result = []
    if c.estimatedPendingCompactionBytes:
        result.append(f"⏳ {fmt.size(c.estimatedPendingCompactionBytes)} to compact")
    elif c.compactionPending:
        result.append("⏳ compaction pending")
    entries = c.sstEntries + c.memtableEntries
    if entries and deletions(c) >= DELETIONS_WARNING_SHARE * entries:
        result.append(f"⚠ {fmt.compact_count(deletions(c))} deletions")
    return result


def _written(c) -> str:
    if not c.HasField("oldestSstFileCreationTimeSeconds"):
        return ""
    oldest, newest = fmt.date(c.oldestSstFileCreationTimeSeconds), fmt.date(c.newestSstFileCreationTimeSeconds)
    return oldest if oldest == newest else f"{oldest} – {newest}"


def _detail_rows(c) -> list[tuple[str, str]]:
    rows = [
        ("Column family", c.columnFamily if c.columnFamily != c.name else ""),
        ("Versions", f"{fmt.count(c.estimatedVersionCount)} (estimated)"),
        ("On disk", fmt.size(c.liveSstFilesBytes)),
        ("Uncompressed", fmt.size(uncompressed_bytes(c))),
        ("Average value", fmt.size(c.rawValueBytes / c.sstEntries) if c.sstEntries else ""),
        ("Data files", fmt.count(c.sstFileCount)),
        *(
            (f"Level {level.level}", f"{fmt.plural(level.fileCount, 'file')}, {fmt.size(level.sizeBytes)}, "
             f"{', '.join(level.compressions)}")
            for level in c.levels
        ),
        ("Unflushed", f"{fmt.count(c.memtableEntries)} entries in memory"),
        ("Deletions", f"{fmt.count(deletions(c))} not yet compacted away"),
        ("Pending compaction", fmt.size(c.estimatedPendingCompactionBytes)),
        ("Index memory", fmt.size(c.tableReadersMemoryBytes)),
        ("Written", _written(c)),
        ("First key", c.firstKey),
        ("Last key", c.lastKey),
        ("Status", "   ".join(warnings(c))),
    ]  # fmt: skip
    return [(name, value) for name, value in rows if value]


def details_html(c) -> str:
    """All statistics of the collection, as tooltip."""
    cells = "".join(
        f"<tr><td style='padding-right:12px'>{name}</td><td>{html.escape(value)}</td></tr>"
        for name, value in _detail_rows(c)
    )
    return f"<b>{html.escape(c.name)}</b><table>{cells}</table>"


def summary_text(c, server: str) -> str:
    """All statistics of the collection as plain text, e.g. for pasting into a chat."""
    rows = _detail_rows(c)
    width = max(len(name) for name, _ in rows) + 2
    lines = [f"{c.name} on {server}, {time.strftime('%Y-%m-%d %H:%M')}"]
    return "\n".join(lines + [f"  {name + ':':<{width}}{value}" for name, value in rows])


def collections_total(info) -> str:
    """E.g. "10 with ~30.4 M versions"."""
    versions = sum(c.estimatedVersionCount for c in info.collections)
    return f"{len(info.collections)} with ~{fmt.compact_count(versions)} versions"


def size_total(info) -> str:
    """E.g. "10.3 GB on disk, 53.6 GB uncompressed (5.2×)"."""
    on_disk = sum(c.liveSstFilesBytes for c in info.collections)
    uncompressed = sum(uncompressed_bytes(c) for c in info.collections)
    total_ratio = f" ({uncompressed / on_disk:.1f}×)" if on_disk else ""
    return f"{fmt.size(on_disk)} on disk, {fmt.size(uncompressed)} uncompressed{total_ratio}"


def server_status_html(info) -> str:
    """Problems first, then what runs, e.g. "backup in progress"."""
    problems = []
    if info.writesStopped:
        problems.append("<b style='color:#d33'>Writes are stopped</b>")
    if info.delayedWriteRateBytesPerSecond:
        problems.append(f"<b>Writes are slowed down</b> to {fmt.size(info.delayedWriteRateBytesPerSecond)}/s")
    if info.backgroundErrors:
        problems.append(f"<b style='color:#d33'>{fmt.plural(info.backgroundErrors, 'background error')}</b>")
    activity = []
    if info.backupInProgress:
        activity.append("backup in progress")
    if info.compactionInProgress:
        activity.append("CompactAllData in progress")
    if info.runningCompactions:
        activity.append(f"{fmt.plural(info.runningCompactions, 'compaction')} running")
    if info.runningFlushes:
        activity.append(f"{fmt.plural(info.runningFlushes, 'flush', 'flushes')} running")
    return " · ".join(problems + activity) or "OK, idle"
