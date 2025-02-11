import argparse
import logging

from record_explorer import RecordExplorer
from db_connection import connect, getMultipleKeys, listKeys, listVersions
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.suggester import SuggestFromList
from textual.widgets import TabbedContent
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Static,
    TabPane,
)

logging.basicConfig(level=logging.DEBUG)


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
        pass


class FileNameHint(Widget):
    filename = reactive("none")

    def render(self) -> str:
        return f"Press the button above to download the value for the selected key to {self.filename}"


class KeyInfoWidget(Widget):
    key = ""
    key_save_filename = ""
    versions = []

    def sanitize_filename(self, name):
        import re

        s = str(name).strip().replace(" ", "_")
        return re.sub(r"(?u)[^-\w.]", "_", s)

    def update_key(self, key):
        self.key = key

        key_info_label = self.query_one("#key-info-label")

        key_info_text = Text("Key:", style="bold magenta")
        key_info_text.append(Text(key, style="bold white"))

        if key == "More keys on the next page...":
            return

        try:
            self.versions = listVersions(stub, self.collection, key)
            self.versions.sort()
            key_info_text.append(Text("\nVersions: "))
            key_info_text.append(
                Text(",".join(map(str, self.versions)), style="bold white")
            )
            self.sanitized_key_name = self.sanitize_filename(
                f"{self.collection}_{key}_{self.versions[-1]}"
            )
        except Exception as e:
            key_info_text.append(Text("\nCould not load versions: " + str(e)))
        key_info_label.update(key_info_text)

    def explore_key(self):
        # Add new Explorer tab
        tabbed_content = self.app.query_one(TabbedContent)
        tab_id = f"record_explorer_tab_{self.sanitized_key_name}"

        if not tabbed_content.query(f"#{tab_id}"):
            tabbed_content.add_pane(
                TabPane(
                    "Record Explorer " + self.key,
                    RecordExplorer(
                        stub=self.stub,
                        key=self.key,
                        collection=self.collection,
                        id=f"record_explorer_{self.sanitized_key_name}",
                    ),
                    id=tab_id,
                )
            )
        # Set the active tab
        tabbed_content.active = tab_id

    def on_button_pressed(self, event):
        event.stop()
        if event.button.id == "explore-button":
            self.explore_key()

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(id="key-info-label")

            exploreButton = Button(label="Explore record", id="explore-button")
            yield exploreButton


class FossilDBClient(App):
    """A Textual app to manage FossilDB databases."""

    BINDINGS = [
        ("q", "quit", "Quit the client"),
        ("r", "refresh", "Refresh the data"),
        Binding(
            "pagedown",
            "show_next",
            f"Show next page of keys",
            priority=True,
            show=True,
        ),
        Binding(
            "j",
            "show_next",
            f"Show next page of keys",
            show=True,
        ),
        Binding(
            "pageup",
            "show_prev",
            f"Show previous page of keys",
            priority=True,
            show=True,
        ),
        Binding(
            "k",
            "show_prev",
            f"Show previous page of keys",
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

    knownCollections = [
        "skeletons",
        "volumes",
        "volumeData",
        "volumeSegmentIndex",
        "editableMappingsInfo",
        "editableMappingsAgglomerateToGraph",
        "editableMappingsSegmentToAgglomerate",
        "annotations",
        "annotationUpdates",
    ]

    def __init__(self, stub, collection, count):
        super().__init__()
        self.stub = stub
        self.collection = collection
        self.key_list_limit = int(count)

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with TabbedContent(id="main-tabs", initial="main-tab"):
            with TabPane(id="main-tab", title="FossilDB Explorer"):
                with Vertical():
                    yield Input(
                        placeholder="Select collection:",
                        id="collection",
                        value=self.collection,
                        suggester=SuggestFromList(self.knownCollections),
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
                    self.key_list_limit + 1,  # +1 to check if there are more keys
                )
            else:
                keys = listKeys(
                    self.stub, self.collection, self.after_key, self.key_list_limit + 1
                )
            overlength = False
            if len(keys) > self.key_list_limit:
                keys = keys[:-1]
                overlength = True

            for i, key in enumerate(keys):
                label = Text(str(i), style="#B0FC38 italic")
                table.add_row(key, label=label)
            if overlength:
                table.add_row(
                    "More keys on the next page...",
                    label=Text("...", style="#B0FC38 italic"),
                )
            self.last_keys.append(keys[-1])
            table.focus()
        except Exception as e:
            table.add_row("Could not load keys: " + str(e))

    def action_quit(self) -> None:
        """An action to quit the app."""
        self.exit()

    def action_refresh(self) -> None:
        """An action to refresh the data."""
        self.refresh_data()

    def action_show_next(self) -> None:
        """An action to show the next key_list_limit keys."""
        self.after_key = self.last_keys[-1]
        self.refresh_data()

    def action_show_prev(self) -> None:
        """An action to show the previous key_list_limit keys."""
        if len(self.last_keys) > 2:
            self.last_keys.pop()
            self.last_keys.pop()
            self.after_key = self.last_keys[-1]
        self.refresh_data()

    def action_next_key(self) -> None:
        """An action to select the next key."""
        table = self.query_one(DataTable)
        current_row = table.cursor_coordinate.row
        if current_row < self.key_list_limit - 1:
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
            if self.after_key != "":
                self.action_show_prev()
                table.cursor_coordinate = (
                    len(table.rows) - 2,  # -1 for last row, -1 for the More keys row
                    table.cursor_coordinate.column,
                )


def init_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "host",
        help="fossildb host and ip, e.g. localhost:7155",
        default="localhost:7155",
        nargs="?",
    )
    parser.add_argument("-c", "--collection", help="collection to use", default="")
    parser.add_argument("-n", "--count", help="number of keys to list", default=40)
    return parser


if __name__ == "__main__":
    parser = init_argument_parser()
    args = parser.parse_args()
    stub = connect(args.host)
    app = FossilDBClient(stub, args.collection, args.count)
    app.run()
