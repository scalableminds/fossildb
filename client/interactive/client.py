from textual import on
from textual.app import App, ComposeResult
from textual.widgets import (
    Button,
    Header,
    Footer,
    Input,
    DataTable,
    RichLog,
)
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from rich.text import Text
from db_connection import connect, listKeys, getKey, listVersions

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
        self.query_one("#delete-button").styles.visibility = "visible"

        log = self.query_one("#key-info-label")

        log.clear()
        log.write(Text("Key:", style="bold magenta"))
        log.write(key)

        try:
            versions = listVersions(stub, self.collection, key)
            log.write(Text("Versions:", style="bold magenta"))
            ",".join(map(str, versions))
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
        if event.button.id == "delete-button":
            self.delete_key()

    def compose(self) -> ComposeResult:
        with Vertical():
            yield RichLog(id="key-info-label", wrap=True)
            writeButton = Button(label="Write data to out.bin", id="write-button")
            writeButton.styles.visibility = "hidden"
            yield writeButton
            deleteButton = Button(label="Delete key", id="delete-button")
            deleteButton.styles.visibility = "hidden"
            yield deleteButton


class FossilDBClient(App):
    """A Textual app to manage FossilDB databases."""

    BINDINGS = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit the client"),
        ("r", "refresh", "Refresh the data"),
        ("k", "show_next", f"Show next {KEY_LIST_LIMIT} keys"),
    ]

    after_key = ""
    collection = "volumeData"
    CSS_PATH = "client.tcss"

    def __init__(self, stub):
        super().__init__()
        self.stub = stub

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Input(placeholder="Select collection:", id="collection")
        yield Input(placeholder="Start after key:", id="after_key")
        yield ListKeysWidget(id="list-keys", stub=self.stub)

        yield Footer()

    @on(Input.Submitted)
    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "collection":
            self.collection = event.input.value
        if event.input.id == "after_key":
            self.after_key = event.input.value
        self.refresh_data()

    def refresh_data(self) -> None:
        """Refresh the data in the table."""
        table = self.query_one(DataTable)
        self.query_one(KeyInfoWidget).collection = self.collection
        table.clear(columns=True)
        table.add_column("key")
        try:
            keys = listKeys(self.stub, self.collection, self.after_key, KEY_LIST_LIMIT)
            for i, key in enumerate(keys):
                label = Text(str(i), style="#B0FC38 italic")
                table.add_row(key, label=label)
            self.last_key = keys[-1]
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
        self.after_key = self.last_key
        self.refresh_data()


if __name__ == "__main__":
    stub = connect()
    app = FossilDBClient(stub)
    app.run()
