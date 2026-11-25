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
    # Expand and collapse all; field 5 is a nested message
    view, model = record._structure_view, record._structure_model
    record._structure_expand_button.click()
    assert view.isExpanded(model.index(4, 0))
    record._structure_collapse_button.click()
    assert not view.isExpanded(model.index(4, 0))
    # Loaded values show their size in the version list
    assert record._versions_model.stringList() == ["1  ·  2 B", f"0  ·  {len(MESSAGE)} B"]
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
    assert "Total: 40.8 kB" in record._measure_label.text()
    assert "largest 1.0 kB (version 39)" in record._measure_label.text()
    assert fake.multiple_versions_requests - requests_before > 2
    assert record._versions_model.stringList()[0] == "39  ·  1.0 kB"

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
    assert overview._warnings(big) == ["⏳ 3.2 GB to compact", "⚠ 20 deletions"]
    assert "a&lt;b" in overview._details(small)
    summary = overview.summary_text(big, "production (db:7155)").splitlines()
    assert summary[0].startswith("big on production (db:7155), ")
    assert "  On disk:            1.0 kB" in summary
    assert summary[-1] == "  Status:             ⏳ 3.2 GB to compact   ⚠ 20 deletions"

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


def test_ctrl_tab_switches_tabs(window):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    _, window = window
    browser = open_collection(window)
    window.open_collection("collectionB")
    tabs = window._tabs
    assert tabs.count() == 3 and tabs.currentIndex() == 2
    window.activateWindow()
    wait_until(lambda: QApplication.activeWindow() is window)
    ctrl = Qt.KeyboardModifier.ControlModifier
    shift = Qt.KeyboardModifier.ShiftModifier
    # Wraps around, and works while widgets that use Tab themselves have the focus
    focused = tabs.currentWidget()._table
    focused.setFocus()
    QTest.keyClick(focused, Qt.Key.Key_Tab, ctrl)
    assert tabs.currentIndex() == 0
    browser.record._json.setFocus()
    QTest.keyClick(window._overview._table, Qt.Key.Key_Tab, ctrl)
    assert tabs.currentIndex() == 1
    browser.record._json.setFocus()
    QTest.keyClick(browser.record._json, Qt.Key.Key_Backtab, ctrl | shift)
    assert tabs.currentIndex() == 0
    QTest.keyClick(window._overview._table, Qt.Key.Key_Backtab, ctrl | shift)
    assert tabs.currentIndex() == 2


def test_window_geometry_is_remembered(qt_app, current_server):
    from fossildb_gui import settings

    _, address = current_server
    runner = TaskRunner()

    def new_window():
        connection = connect(address)
        return MainWindow(ConnectionSpec(address), connection, connection.database_info(), runner, SchemaManager(runner))

    settings._set("windowGeometry", None)
    first = new_window()
    available = first.screen().availableGeometry()
    assert abs(first.width() - available.width() * 0.8) <= 1
    first.show()
    # A second window does not cover the first one
    second = new_window()
    assert second.pos() != first.pos()
    second.close()
    # The offscreen test screen is narrower than the window's minimum width, so only the height can change
    first.resize(first.width(), 500)
    first.close()
    QApplication.processEvents()
    third = new_window()
    assert third.height() == 500
    third.close()


def test_splitter_is_remembered(qt_app):
    from PySide6.QtWidgets import QLabel, QSplitter

    from fossildb_gui import settings

    settings._set("splitters/test", None)

    def splitter():
        widget = QSplitter()
        widget.addWidget(QLabel("a"))
        widget.addWidget(QLabel("b"))
        widget.resize(400, 100)
        widget.setSizes([300, 100])
        settings.remember_splitter(widget, "test")
        return widget

    first = splitter()
    first.show()
    first.setSizes([100, 300])
    first.splitterMoved.emit(100, 1)
    wait_until(lambda: settings._get("splitters/test", ""))
    second = splitter()
    second.show()
    QApplication.processEvents()
    assert second.sizes()[0] < second.sizes()[1]
    # A handle width set in the code wins over the one in the saved state
    third = QSplitter()
    third.addWidget(QLabel("a"))
    third.addWidget(QLabel("b"))
    third.setHandleWidth(9)
    settings.remember_splitter(third, "test")
    assert third.handleWidth() == 9


def test_uuid_keys_fit(window):
    from fossildb_gui import settings

    fake, window = window
    settings._set("splitters/keys", None)
    key = "00000000-5813-8649-5500-00a6004814b0"
    fake.put("collectionA", key, 0, b"x")
    window.resize(1400, 800)  # the offscreen screen would be too narrow
    browser = open_collection(window)
    QApplication.processEvents()
    table = browser._table
    assert table.columnWidth(0) >= table.fontMetrics().horizontalAdvance(key) + 8


def test_enter_connects(qt_app):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    from fossildb_gui import settings
    from fossildb_gui.connect_dialog import ConnectDialog

    settings._set("recentConnections", None)
    settings.add_recent_connection(ConnectionSpec("db:7155"))
    runner = TaskRunner()
    for field in ("_address", "_name", "_ssh_host", "_recent"):
        dialog = ConnectDialog(runner)
        calls = []
        dialog._connect = calls.append
        dialog._tunnel.setChecked(True)
        dialog._ssh_host.setText("jump")
        dialog.show()
        QTest.keyClick(getattr(dialog, field), Qt.Key.Key_Return)
        expected = ConnectionSpec("db:7155") if field == "_recent" else ConnectionSpec("db:7155", "jump")
        assert calls == [expected], field
        dialog.reject()


def test_arrow_keys_move_to_the_list(window):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    fake, window = window
    window.activateWindow()
    wait_until(lambda: QApplication.activeWindow() is window)
    overview = window._overview
    opened = []
    overview.open_collection.connect(opened.append)
    # E.g. after clicking a tab, the tab bar has the focus, which doesn't use the Down key
    tab_bar = window._tabs.tabBar()
    tab_bar.setFocus()
    QTest.keyClick(tab_bar, Qt.Key.Key_Down)
    if fake.legacy:  # no list to move to
        assert QApplication.focusWidget() is tab_bar
        return
    table = overview._table
    assert QApplication.focusWidget() is table and table.selectionModel().selectedRows()[0].row() == 0
    QTest.keyClick(table, Qt.Key.Key_Down)
    QTest.keyClick(table, Qt.Key.Key_Return)
    assert opened == [table.model().index(1, 0).data()]

    # The same in a collection tab, where the first key is selected, and then the next one
    browser = open_collection(window)
    tab_bar.setFocus()
    QTest.keyClick(tab_bar, Qt.Key.Key_Down)
    assert QApplication.focusWidget() is browser._table and browser._table.currentIndex().row() == 0
    tab_bar.setFocus()
    QTest.keyClick(tab_bar, Qt.Key.Key_Down)
    assert browser._table.currentIndex().row() == 1


@pytest.mark.parametrize("single_click", [True, False])
def test_choosing_a_recent_connection(qt_app, single_click):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest
    from PySide6.QtWidgets import QDialogButtonBox, QProxyStyle, QStyle

    from fossildb_gui import settings
    from fossildb_gui.connect_dialog import ConnectDialog

    class ClickStyle(QProxyStyle):
        def styleHint(self, hint, option=None, widget=None, data=None):
            if hint == QStyle.StyleHint.SH_ItemView_ActivateItemOnSingleClick:
                return int(single_click)
            return super().styleHint(hint, option, widget, data)

    settings._set("recentConnections", None)
    for address in ("c:3", "b:2", "a:1"):
        settings.add_recent_connection(ConnectionSpec(address))
    dialog = ConnectDialog(TaskRunner())
    calls = []
    dialog._connect = calls.append
    style = ClickStyle("fusion")
    dialog._recent.setStyle(style)
    dialog.show()
    assert dialog._connect_button.isDefault()
    assert not dialog._buttons.button(QDialogButtonBox.StandardButton.Cancel).isDefault()

    viewport = dialog._recent.viewport()
    second = dialog._recent.visualItemRect(dialog._recent.item(1)).center()
    QTest.mouseClick(viewport, Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, second)
    assert calls == [] and dialog._address.text() == "b:2"
    QTest.keyClick(dialog._recent, Qt.Key.Key_Down)
    assert calls == [] and dialog._address.text() == "c:3"
    QTest.mouseDClick(viewport, Qt.MouseButton.RightButton, Qt.KeyboardModifier.NoModifier, second)
    QTest.mouseDClick(viewport, Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, second)
    assert calls == [ConnectionSpec("b:2")]
    # Enter connects to the current one (QTest's double-click alone doesn't change it, a real one would)
    QTest.keyClick(dialog._recent, Qt.Key.Key_Return)
    assert calls == [ConnectionSpec("b:2"), ConnectionSpec("c:3")]
    dialog.reject()


@pytest.mark.parametrize("field", ["_recent", "_address"])
def test_focus_returns_after_failed_connect(qt_app, field):
    import socket

    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    from fossildb_gui import settings
    from fossildb_gui.connect_dialog import ConnectDialog

    closed = socket.create_server(("127.0.0.1", 0))
    address = f"127.0.0.1:{closed.getsockname()[1]}"
    closed.close()
    settings._set("recentConnections", None)
    settings.add_recent_connection(ConnectionSpec(address))
    dialog = ConnectDialog(TaskRunner())
    dialog.show()
    dialog.activateWindow()
    wait_until(lambda: QApplication.activeWindow() is dialog)
    widget = getattr(dialog, field)
    widget.setFocus()
    QTest.keyClick(widget, Qt.Key.Key_Return)
    wait_until(lambda: "Connection failed" in dialog._status.text())
    assert QApplication.focusWidget() is widget
    dialog.reject()


def test_overview_totals(qt_app):
    from fossildb_gui import overview
    from fossildb_gui.api import pb

    def collection(name, disk, raw, versions):
        return pb.CollectionInfoProto(
            name=name, columnFamily=name, liveSstFilesBytes=disk, rawKeyBytes=raw // 10, rawValueBytes=raw - raw // 10,
            estimatedVersionCount=versions,
        )  # fmt: skip

    widget = overview.OverviewWidget(ConnectionSpec("x:1"))
    widget.set_info(pb.DatabaseInfoProto(collections=[collection("a", 1 << 30, 5 << 30, 30_000_000), collection("b", 0, 0, 400_000)]))
    assert widget._labels["Collections"].text() == "2 with ~30.4 M versions"
    assert widget._labels["Size"].text() == "1.1 GB on disk, 5.4 GB uncompressed (5.0×)"


def test_connect_dialog_checks_and_finds_servers(qt_app, monkeypatch):
    import socket

    from fossildb_gui import discovery, settings
    from fossildb_gui.connect_dialog import ConnectDialog

    from conftest import FakeFossilDB, _serve

    servers = [_serve(FakeFossilDB()) for _ in range(2)]
    (_, known), (_, local) = servers
    closed = socket.create_server(("127.0.0.1", 0))
    unreachable = f"127.0.0.1:{closed.getsockname()[1]}"
    closed.close()
    local_port = int(local.rpartition(":")[2])
    known_port = int(known.rpartition(":")[2])
    # Both servers listen on this computer, but the known one is in the recent connections already
    monkeypatch.setattr(discovery, "listening_local_ports", lambda: {local_port, known_port})
    settings._set("recentConnections", None)
    settings.add_recent_connection(ConnectionSpec(unreachable))
    settings.add_recent_connection(ConnectionSpec(known.replace("127.0.0.1", "localhost")))
    settings.add_recent_connection(ConnectionSpec("db:7155", "jump"))

    dialog = ConnectDialog(TaskRunner())
    items = lambda: [dialog._recent.item(row) for row in range(dialog._recent.count())]  # noqa: E731
    wait_until(lambda: dialog._recent.count() == 5 and all(item.toolTip() for item in items()[1:3]))
    texts = [item.text() for item in items()]
    assert texts == ["db:7155 via jump", known.replace("127.0.0.1", "localhost"), unreachable, "On this computer", f"localhost:{local_port}"]
    assert items()[0].toolTip() == ""  # tunnels are not checked
    assert items()[1].toolTip() == "FossilDB answers"
    assert items()[2].toolTip().startswith("Not reachable: Nothing is listening")
    dialog.reject()
    for server, _ in servers:
        server.stop(None)


def test_size_units(window):
    from fossildb_gui import fmt, main_window, settings

    fake, window = window
    assert fmt.size(1_500_000) == "1.5 MB"
    fake.put("collectionA", "big", 0, bytes(1500))
    browser = open_collection(window)
    record = browser.record
    record.show_key("big")
    wait_until(lambda: record._versions_model.stringList() == ["0  ·  1.5 kB"])
    assert "1.5 kB" in record._value_label.text()

    main_window._set_size_units(False)
    assert settings.decimal_sizes() is False and window._binary_action.isChecked()
    assert record._versions_model.stringList() == ["0  ·  1.5 KiB"]
    wait_until(lambda: "1.5 KiB" in record._value_label.text())
    if not fake.legacy:
        assert window._overview._labels["Size"].text().startswith("2.0 KiB on disk")


def test_arrow_keys_between_keys_and_versions(window):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    _, window = window
    browser = open_collection(window)
    window.activateWindow()
    wait_until(lambda: QApplication.activeWindow() is window)
    table, versions = browser._table, browser.record.versions_view
    table.setFocus()
    table.selectRow(0)
    wait_until(lambda: browser.record._versions == [1, 0])
    QTest.keyClick(table, Qt.Key.Key_Right)
    assert QApplication.focusWidget() is versions and versions.currentIndex().row() == 0
    QTest.keyClick(versions, Qt.Key.Key_Down)
    assert versions.currentIndex().row() == 1
    QTest.keyClick(versions, Qt.Key.Key_Left)
    assert QApplication.focusWidget() is table and table.currentIndex().row() == 0
    # The chosen version stays when going back and forth
    QTest.keyClick(table, Qt.Key.Key_Right)
    assert versions.currentIndex().row() == 1


def test_tab_leaves_the_tables(window):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    fake, window = window
    browser = open_collection(window)
    window.activateWindow()
    wait_until(lambda: QApplication.activeWindow() is window)
    tables = [(browser, browser._table)] + ([] if fake.legacy else [(window._overview, window._overview._table)])
    for tab, table in tables:
        window._tabs.setCurrentWidget(tab)
        table.setFocus()
        table.setCurrentIndex(table.model().index(0, 0))
        QTest.keyClick(table, Qt.Key.Key_Tab)
        assert QApplication.focusWidget() is not table
        table.setFocus()
        QTest.keyClick(table, Qt.Key.Key_Backtab, Qt.KeyboardModifier.ShiftModifier)
        assert QApplication.focusWidget() is not table


def test_arrow_keys_at_the_ends_of_lists(window, capfd):
    """Keys that a list doesn't use, e.g. Up in its first row, must not be sent back to it."""
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    fake, window = window
    window.activateWindow()
    wait_until(lambda: QApplication.activeWindow() is window)
    browser = open_collection(window)
    views = [(browser, browser._table)] + ([] if fake.legacy else [(window._overview, window._overview._table)])
    for tab, view in views:
        window._tabs.setCurrentWidget(tab)
        view.setFocus()
        view.setCurrentIndex(view.model().index(0, 0))
        for key in (Qt.Key.Key_Up, Qt.Key.Key_PageUp):
            QTest.keyClick(view, key)
        assert view.currentIndex().row() == 0
    # PySide only logs errors in event handlers, so the test looks for them in the output
    assert "RecursionError" not in capfd.readouterr().err


def test_tree_model_column_tooltips(qt_app):
    from PySide6.QtCore import Qt

    from fossildb_gui.tree import Node, TreeModel, root_node

    model = TreeModel(["a", "b"])
    model.set_root(root_node([Node(("x", "y"), tooltip="row", column_tooltips={1: "cell"})]))
    assert model.index(0, 0).data(Qt.ItemDataRole.ToolTipRole) == "row"
    assert model.index(0, 1).data(Qt.ItemDataRole.ToolTipRole) == "cell"
