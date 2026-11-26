from fossildb_gui import fmt


def test_sizes():
    assert fmt.size(999) == "999 B"
    assert fmt.size(1_500_000) == "1.5 MB"
    fmt.set_decimal(False)
    assert fmt.size(1536) == "1.5 KiB"


def test_counts():
    assert fmt.count(41_300_000) == "41\u202f300\u202f000"  # narrow no-break spaces
    assert fmt.count(999) == "999"
    assert [fmt.compact_count(n) for n in (870, 3400, 41_300_000)] == ["870", "3.4 k", "41.3 M"]
    assert fmt.plural(1, "byte") == "1 byte" and fmt.plural(2, "flush", "flushes") == "2 flushes"
    assert fmt.one_line("a\nb" + "c" * 300, 5) == "a⏎bcc …"


def test_timestamps():
    assert fmt.iso_timestamp(1732540000123) == "2024-11-25T13:06:40.123Z"  # milliseconds
    assert fmt.iso_timestamp(1732540000) == "2024-11-25T13:06:40Z"  # seconds
    assert fmt.iso_timestamp(0) is None and fmt.iso_timestamp(True) is None and fmt.iso_timestamp(10**20) is None
    assert fmt.with_timestamp("createdTimestamp", 1732540000, "1732540000") == "1732540000 (2024-11-25T13:06:40Z)"
    assert fmt.with_timestamp("count", 1732540000, "1732540000") == "1732540000"


def test_object_id_date():
    assert fmt.object_id_date("5a0627196b0000221d37a590") == "2017-11-10T22:24:25Z"
    assert fmt.object_id_date("5A0627196B0000221D37A590") == "2017-11-10T22:24:25Z"
    assert fmt.object_id_date("5a0627196b0000221d37a59") is None  # too short
    assert fmt.object_id_date("5a0627196b0000221d37a590-x") is None
    assert fmt.object_id_date("00000000-587f-a699-2700-00bc111c538f") is None
    assert fmt.object_id_date("000000000000000000000000") is None  # 1970
