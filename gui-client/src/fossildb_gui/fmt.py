"""Formatting numbers, sizes and times for display."""

import datetime as dt


def size(num_bytes: int | float) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError


def count(n: int) -> str:
    return f"{n:,}"


def timestamp(seconds: float) -> str:
    return dt.datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M")


def date(seconds: float) -> str:
    return dt.datetime.fromtimestamp(seconds).strftime("%Y-%m-%d")


def duration(seconds: float) -> str:
    seconds = int(seconds)
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes = rest // 60
    if days:
        return f"{days} d {hours} h"
    if hours:
        return f"{hours} h {minutes} min"
    return f"{minutes} min"
