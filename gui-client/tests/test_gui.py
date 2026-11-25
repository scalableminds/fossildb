"""Drives the main window against the fake server, with Qt running offscreen."""

import time

import pytest
from PySide6.QtWidgets import QApplication

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
    assert len(model.keys) == 500 and model.has_more
    model.fetchMore()
    wait_until(lambda: len(model.keys) == 1000)

    record = browser.record
    browser._table.selectRow(0)
    wait_until(lambda: record._versions == [1, 0])
    wait_until(lambda: "bytes" in record._value_label.text())
    # Version 1 is not protobuf, so the hex dump is shown
    assert record._tabs.currentIndex() == record._hex_tab
    assert record._hex.toPlainText().startswith("00000000  ff fe")

    record._versions_view.setCurrentIndex(record._versions_model.index(1))
    wait_until(lambda: record._structure_model.rowCount() == 7)
    assert record._tabs.currentIndex() == record._structure_tab


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
