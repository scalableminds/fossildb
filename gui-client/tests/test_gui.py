"""Drives the main window against the fake server, with Qt running offscreen."""

import time

import pytest
from PySide6.QtWidgets import QAbstractItemView, QApplication

from fossildb_gui.connection import connect
from fossildb_gui.key_browser import KeyBrowser
from fossildb_gui.main_window import MainWindow
from fossildb_gui.schema_manager import SchemaManager
from fossildb_gui.settings import ConnectionSpec
from fossildb_gui.tasks import TaskRunner
from test_wire import MESSAGE


def wait_until(condition, timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        QApplication.processEvents()
        time.sleep(0.01)


@pytest.fixture
def window(qt_app, fake_server):
    fake, address = fake_server
    for i in range(1200):  # more than two pages
        fake.put("collectionA", f"key{i:05}", 0, MESSAGE)
    fake.put("collectionA", "key00000", 1, b"\xff\xfe")
    connection = connect(address)
    runner = TaskRunner()
    window = MainWindow(ConnectionSpec(address), connection, connection.database_info(), runner, SchemaManager(runner))
    window.show()
    yield fake, window
    window.close()
    QApplication.processEvents()


def open_collection(window, name="collectionA") -> KeyBrowser:
    window.open_collection(name)
    browser = window._tabs.currentWidget()
    assert isinstance(browser, KeyBrowser)
    wait_until(lambda: len(browser._model.keys) > 0)
    return browser


def test_overview(window):
    fake, window = window
    overview = window._overview
    if fake.legacy:
        assert overview._manual_box.isVisibleTo(window)
        assert not overview._table.isVisibleTo(window)
    else:
        assert overview._model.rowCount() == 2
        assert not overview._manual_box.isVisibleTo(window)


def test_browse_keys_and_values(window):
    _, window = window
    browser = open_collection(window)
    model = browser._model
    assert len(model.keys) == 100 and model.has_more
    model.fetchMore()
    wait_until(lambda: len(model.keys) == 200)

    record = browser.record
    browser._table.selectRow(0)
    wait_until(lambda: record._versions == [1, 0])
    wait_until(lambda: "bytes" in record._value_label.text())
    # Version 1 is not protobuf, so the hex dump is shown
    assert record._tabs.currentIndex() == record._hex_tab
    assert not record._tabs.isTabEnabled(record._text_tab) and not record._tabs.isTabEnabled(record._json_tab)
    assert not record._tabs.isTabEnabled(record._structure_tab) and not record._tabs.isTabEnabled(record._typed_tab)
    assert record._tabs.tabToolTip(record._structure_tab).startswith("Not a protobuf message")
    assert record._hex.toPlainText().startswith("00000000  ff fe")

    record._versions_view.setCurrentIndex(record._versions_model.index(1))
    wait_until(lambda: record._structure_model.rowCount() == 7)
    assert record._tabs.currentIndex() == record._structure_tab
    assert record._tabs.isTabEnabled(record._structure_tab) and record._tabs.isTabEnabled(record._typed_tab)


def test_prefix_filter_and_missing_collection(window):
    _, window = window
    browser = open_collection(window)
    browser._model.reset("key0119")
    wait_until(lambda: browser._model.keys == [f"key0119{i}" for i in range(10)])

    window.open_collection("doesNotExist")
    missing = window._tabs.currentWidget()
    wait_until(lambda: missing._model.error is not None)
    missing._update_status()
    assert "does not exist" in missing._status.text()


def test_deletes_need_enabled_writes(window, monkeypatch):
    fake, window = window
    browser = open_collection(window)
    record = browser.record
    browser._table.selectRow(0)
    wait_until(lambda: record._version is not None)
    assert not record._delete_version_button.isEnabled()

    window._writes_action.setChecked(True)
    assert record._delete_version_button.isEnabled()
    monkeypatch.setattr(window, "confirm_write", lambda *args, **kwargs: True)
    record._delete_key()
    wait_until(lambda: "key00000" not in fake.data["collectionA"])
    wait_until(lambda: browser._model.keys[0] == "key00001")


def test_long_paths_are_elided(qt_app):
    from fossildb_gui.overview import _Value

    label = _Value(elide=True)
    path = "/very/long/path/" * 20 + "data"
    label.setText(path)
    label.show()
    label.resize(200, 20)
    assert label.text() != path and label.text().endswith("data")
    assert label.toolTip() == path
    label.resize(5000, 20)
    assert label.text() == path and label.toolTip() == ""


def test_named_connections(qt_app):
    from fossildb_gui import settings
    from fossildb_gui.connect_dialog import ConnectDialog

    settings._set("recentConnections", None)
    spec = ConnectionSpec("db:7155", "jump")
    settings.add_recent_connection(spec)
    settings.rename_recent_connection(spec, " production ")
    assert settings.recent_connections()[0].title == "production (db:7155 via jump)"
    # Reconnecting without a name, e.g. from the command line, keeps the name
    assert settings.add_recent_connection(ConnectionSpec("db:7155", "jump")).name == "production"

    dialog = ConnectDialog(TaskRunner())
    assert dialog._recent.item(0).text() == "production (db:7155 via jump)"
    assert dialog._spec().name == "production"
    # Typing a different address drops the name that belonged to the recent connection
    dialog._address.setText("other:7155")
    dialog._address.textEdited.emit("other:7155")
    assert dialog._spec() == ConnectionSpec("other:7155", "jump") and dialog._spec().name == ""

    dialog._recent.setCurrentRow(0)
    dialog._remove_selected()
    assert settings.recent_connections() == [] and dialog._recent.count() == 0


def test_next_page_loads_before_the_end(window):
    _, window = window
    browser = open_collection(window)
    model, table = browser._model, browser._table
    assert len(model.keys) == 100
    # Scroll so that the last visible row is 30 rows above the end
    rows_visible = table.viewport().height() // table.rowHeight(0)
    table.scrollTo(model.index(69, 0), QAbstractItemView.ScrollHint.PositionAtBottom)
    assert table.rowAt(table.viewport().height() - 1) < 99 and rows_visible > 0
    wait_until(lambda: len(model.keys) == 200)


def test_measure_total_key_size(window, monkeypatch):
    from fossildb_gui import record_view

    fake, window = window
    for version in range(40):
        fake.put("collectionA", "big", version, bytes(1000 + version))
    # Requests of about 10 values, so that measuring takes several of them
    monkeypatch.setattr(record_view, "MEASURE_TARGET_BYTES", 10_000)
    browser = open_collection(window)
    record = browser.record
    record.show_key("big")
    wait_until(lambda: len(record._versions) == 40 and record._measure_button.isEnabled())
    requests_before = fake.multiple_versions_requests
    record._measure_button.click()
    wait_until(lambda: not record._measuring)
    assert record._sizes == {version: 1000 + version for version in range(40)}
    assert "Total: 39.8 KiB" in record._measure_label.text()
    assert "largest 1.0 KiB (version 39)" in record._measure_label.text()
    assert fake.multiple_versions_requests - requests_before > 2
    assert record._versions_model.stringList()[0] == "39  ·  1.0 KiB"

    # Stopping keeps what was measured so far
    record._measure_button.click()
    assert record._measuring
    record._measure_button.click()
    assert not record._measuring and "Stopped" in record._measure_label.text()


def test_json_and_text_tabs(window):
    fake, window = window
    fake.put("collectionA", "json", 0, b'{"a": [1, 2]}')
    fake.put("collectionA", "json", 1, b"just text")
    browser = open_collection(window)
    record = browser.record
    record.show_key("json")
    wait_until(lambda: "bytes" in record._value_label.text())
    # Version 1 is text, but not JSON
    assert record._tabs.currentIndex() == record._text_tab
    assert record._tabs.isTabEnabled(record._json_tab) and "Not valid JSON" in record._json_note.text()

    record._versions_view.setCurrentIndex(record._versions_model.index(1))
    wait_until(lambda: record._json.toPlainText() != "")
    assert record._tabs.currentIndex() == record._json_tab
    assert record._json.toPlainText() == '{\n  "a": [\n    1,\n    2\n  ]\n}'
    # The tree is expanded, since the document is small
    model, tree = record._json_model, record._json_tree
    assert model.index(0, 1).data() == "[1, 2]" and tree.isExpanded(model.index(0, 0))
    assert record._json_stack.currentWidget() is tree
    # The summary of an expanded array is dimmed, its items are not
    from PySide6.QtGui import QPalette
    from PySide6.QtWidgets import QStyleOptionViewItem

    def text_color(index):
        option = QStyleOptionViewItem()
        tree.itemDelegateForColumn(1).initStyleOption(option, index)
        return option.palette.color(QPalette.ColorRole.Text)

    dimmed = tree.palette().color(QPalette.ColorRole.PlaceholderText)
    assert text_color(model.index(0, 1)) == dimmed
    assert text_color(model.index(0, 1, model.index(0, 0))) != dimmed
    tree.collapse(model.index(0, 0))
    assert text_color(model.index(0, 1)) != dimmed
    record._json_raw_button.click()
    assert record._json_stack.currentWidget() is record._json and not record._json_expand_button.isEnabled()
    assert record._text.toPlainText() == '{"a": [1, 2]}'


def test_object_id_date_in_title(window):
    _, window = window
    record = open_collection(window).record
    record.show_key("5a0627196b0000221d37a590")
    assert record._title.text() == "collectionA / 5a0627196b0000221d37a590  (ObjectId created 2017-11-10T22:24:25Z)"
    record.show_key("key00001")
    assert record._title.text() == "collectionA / key00001"


def test_loading_more_row(window):
    from PySide6.QtCore import Qt

    fake, window = window
    browser = open_collection(window)
    model = browser._model
    fake.list_keys_gate.clear()
    model.fetchMore()
    try:
        assert model.rowCount() == 101
        assert model.index(100, 0).data() == "Loading more…"
        assert model.flags(model.index(100, 0)) == Qt.ItemFlag.NoItemFlags
        assert "loading more" in browser._status.text()
    finally:
        fake.list_keys_gate.set()
    wait_until(lambda: len(model.keys) == 200)
    assert model.rowCount() == 200 and model.index(100, 0).data() == "key00100"


def test_overview_columns(qt_app):
    from fossildb_gui import fmt, overview
    from fossildb_gui.api import pb

    assert [fmt.compact_count(n) for n in (870, 3400, 41_300_000)] == ["870", "3.4 k", "41.3 M"]
    small = pb.CollectionInfoProto(name="small", columnFamily="small", liveSstFilesBytes=10, sstEntries=10, firstKey="a<b")
    big = pb.CollectionInfoProto(
        name="big", columnFamily="big", liveSstFilesBytes=1000, sstEntries=100, sstDeletions=20,
        estimatedPendingCompactionBytes=3 << 30,
    )  # fmt: skip
    assert overview._warnings(small) == []
    assert overview._warnings(big) == ["⏳ 3.0 GiB to compact", "⚠ 20 deletions"]
    assert "a&lt;b" in overview._details(small)
    summary = overview.summary_text(big, "production (db:7155)").splitlines()
    assert summary[0].startswith("big on production (db:7155), ")
    assert "  On disk:            1000 B" in summary
    assert summary[-1] == "  Status:             ⏳ 3.0 GiB to compact   ⚠ 20 deletions"

    widget = overview.OverviewWidget(ConnectionSpec("x:1"))
    widget.set_info(pb.DatabaseInfoProto(collections=[small, big]))
    proxy = widget._table.model()
    # Largest first, with bars relative to it
    assert [proxy.index(row, 0).data() for row in range(2)] == ["big", "small"]
    assert proxy.index(1, overview._SIZE_COLUMN).data(overview._BAR_ROLE) == 0.01
    assert widget._table.isColumnHidden(1)  # column families match the names


@pytest.mark.parametrize("single_click", [True, False])
def test_only_left_click_opens_collections(qt_app, single_click):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest
    from PySide6.QtWidgets import QProxyStyle, QStyle

    from fossildb_gui import overview
    from fossildb_gui.api import pb

    class ClickStyle(QProxyStyle):
        def styleHint(self, hint, option=None, widget=None, data=None):
            if hint == QStyle.StyleHint.SH_ItemView_ActivateItemOnSingleClick:
                return int(single_click)
            return super().styleHint(hint, option, widget, data)

    widget = overview.OverviewWidget(ConnectionSpec("x:1"))
    widget.set_info(pb.DatabaseInfoProto(collections=[pb.CollectionInfoProto(name="a", columnFamily="a")]))
    style = ClickStyle("fusion")
    widget._table.setStyle(style)
    widget.show()
    opened = []
    widget.open_collection.connect(opened.append)
    viewport = widget._table.viewport()
    position = widget._table.visualRect(widget._table.model().index(0, 0)).center()
    click = QTest.mouseClick if single_click else QTest.mouseDClick
    for button in (Qt.MouseButton.RightButton, Qt.MouseButton.MiddleButton):
        click(viewport, button, Qt.KeyboardModifier.NoModifier, position)
    assert opened == []
    click(viewport, Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, position)
    assert opened == ["a"]
    widget.close()


def test_auto_refresh(window):
    fake, window = window
    button = window._overview._auto_refresh
    if fake.legacy:
        assert not button.isEnabled()
        return
    table = window._overview._table
    table.selectRow(1)
    selected = table.model().index(1, 0).data()
    window._overview._auto_refresh_timer.setInterval(50)
    before = fake.info_requests
    button.setChecked(True)
    wait_until(lambda: fake.info_requests >= before + 3)
    # The rows are updated in place, so the selection stays
    assert table.selectionModel().selectedRows()[0].data() == selected
    button.setChecked(False)
    QApplication.processEvents()
    stopped_at = fake.info_requests
    time.sleep(0.2)
    QApplication.processEvents()
    assert fake.info_requests <= stopped_at + 1  # at most one that was already on its way
