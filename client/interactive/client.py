import argparse

from db_connection import (connect, getKey, getMultipleKeys, listKeys,
                           listVersions)
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import Button, DataTable, Footer, Header, Input, RichLog

# import logging

# logFile = "sample.log"
# logging.basicConfig(
#     filename=logFile,
#     filemode="a",
#     level=logging.DEBUG,
#     format="%(asctime)s - %(levelname)s: %(message)s",
# )

KEY_LIST_LIMIT = 20


class ListKeysWidget(Widget):
    def __init__(self, stub, **kwargs):
        super().__init__(**kwargs)
        self.stub = stub

    def compose(self) -> ComposeResult:
        with Horizontal(id="horizontal-list"):
            yield DataTable(id="list-keys-table")
            info_widget = KeyInfoWidget(id="key-info")
            info_widget.key = "No key selected"
            info_widget.stub = self.stub
            yield info_widget

    @on(DataTable.CellHighlighted)
    def on_data_table_row_highlighted(self, event):
        self.query_one(KeyInfoWidget).update_key(event.value)
        self.refresh()


class KeyInfoWidget(Widget):
    key = ""

    def update_key(self, key):
        self.key = key

        self.query_one("#write-button").styles.visibility = "visible"

        log = self.query_one("#key-info-label")

        log.clear()
        log.write(Text("Key:", style="bold magenta"))
        log.write(key)

        try:
            versions = listVersions(stub, self.collection, key)
            log.write(Text("Versions:", style="bold magenta"))
            log.write(",".join(map(str, versions)))
        except Exception as e:
            log.write("Could not load versions: " + str(e))

    def write_key(self):
        try:
            value = getKey(stub, self.collection, self.key)
            with open("out.bin", "wb") as f:
                f.write(value)
            self.query_one("#key-info-label").write("Wrote data to out.bin")
        except Exception as e:
            self.query_one("#key-info-label").write("Could not write key: " + str(e))

    def on_button_pressed(self, event):
        if event.button.id == "write-button":
            self.write_key()

    def compose(self) -> ComposeResult:
        with Vertical():
            yield RichLog(id="key-info-label", wrap=True)
            writeButton = Button(label="Write data to out.bin", id="write-button")
            writeButton.styles.visibility = "hidden"
            yield writeButton


class FossilDBClient(App):

    """A Textual app to manage FossilDB databases."""

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit the client"),
        ("r", "refresh", "Refresh the data"),
        Binding(
            "pagedown,j",
            "show_next",
            f"Show next {KEY_LIST_LIMIT} keys",
            priority=True,
            show=True,
        ),
        Binding(
            "pageup,k",
            "show_prev",
            f"Show previous {KEY_LIST_LIMIT} keys",
            priority=True,
            show=True,
        ),
        Binding("down", "next_key", "Select the next key", priority=True, show=False),
        Binding("up", "prev_key", "Select the previous key", priority=True, show=False),
    ]

    after_key = ""
    prefix = ""
    collection = "volumeData"
    CSS_PATH = "client.tcss"

    last_keys = [""]

    def __init__(self, stub, collection):
        super().__init__()
        self.stub = stub
        self.collection = collection

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Input(
            placeholder="Select collection:", id="collection", value=self.collection
        )
        yield Input(
            placeholder="Find keys with prefix: (leave empty to list all keys)",
            id="prefix",
        )
        yield ListKeysWidget(id="list-keys", stub=self.stub)

        yield Footer()

    @on(Input.Submitted)
    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "collection":
            self.collection = event.input.value
        if event.input.id == "prefix":
            self.prefix = event.input.value
        self.refresh_data()

    def refresh_data(self) -> None:
        """Refresh the data in the table."""
        table = self.query_one(DataTable)
        self.query_one(KeyInfoWidget).collection = self.collection
        table.clear(columns=True)
        table.add_column("key")
        try:
            if self.prefix != "":
                keys = getMultipleKeys(
                    self.stub,
                    self.collection,
                    self.prefix,
                    self.after_key,
                    KEY_LIST_LIMIT,
                )
            else:
                keys = listKeys(
                    self.stub, self.collection, self.after_key, KEY_LIST_LIMIT
                )
            for i, key in enumerate(keys):
                label = Text(str(i), style="#B0FC38 italic")
                table.add_row(key, label=label)
            self.last_keys.append(keys[-1])
            table.focus()
        except Exception as e:
            table.add_row("Could not load keys: " + str(e))

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark

    def action_quit(self) -> None:
        """An action to quit the app."""
        self.exit()

    def action_refresh(self) -> None:
        """An action to refresh the data."""
        self.refresh_data()

    def action_show_next(self) -> None:
        """An action to show the next KEY_LIST_LIMIT keys."""
        self.after_key = self.last_keys[-1]
        self.refresh_data()

    def action_show_prev(self) -> None:
        """An action to show the previous KEY_LIST_LIMIT keys."""
        if len(self.last_keys) > 2:
            self.last_keys.pop()
            self.last_keys.pop()
            self.after_key = self.last_keys[-1]
        self.refresh_data()

    def action_next_key(self) -> None:
        """An action to select the next key."""
        table = self.query_one(DataTable)
        current_row = table.cursor_coordinate.row
        if current_row < len(table.rows) - 1:
            table.cursor_coordinate = (current_row + 1, table.cursor_coordinate.column)
        else:
            self.action_show_next()

    def action_prev_key(self) -> None:
        """An action to select the previous key."""
        table = self.query_one(DataTable)
        current_row = table.cursor_coordinate.row
        if current_row > 0:
            table.cursor_coordinate = (current_row - 1, table.cursor_coordinate.column)
        else:
            self.action_show_prev()
            table.cursor_coordinate = (
                len(table.rows) - 1,
                table.cursor_coordinate.column,
            )


def init_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="fossildb port", default="7155")
    parser.add_argument("-i", "--ip", help="fossildb ip", default="localhost")
    parser.add_argument("-c", "--collection", help="collection to use", default="")
    return parser


if __name__ == "__main__":
    parser = init_argument_parser()
    args = parser.parse_args()
    stub = connect(args.ip, args.port)
    app = FossilDBClient(stub, args.collection)
    app.run()
