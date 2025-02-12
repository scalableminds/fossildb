import argparse
import logging

from db_connection import connect, getMultipleKeys, listKeys, listVersions
from record_explorer import RecordExplorer
from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.suggester import SuggestFromList
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Static,
    TabbedContent,
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
    def on_data_table_row_highlighted(self, event: DataTable.CellHighlighted) -> None:
        selected = event.coordinate
        key_coordinate = (selected[0], 0)
        key = self.query_one(DataTable).get_cell_at(key_coordinate)
        # Last row always contains meta information
        if key_coordinate[0] != len(self.query_one(DataTable).rows) - 1:
            self.query_one(KeyInfoWidget).update_key(key)
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

    def update_key(self, key: str) -> None:
        self.key = key

        key_info_label = self.query_one("#key-info-label")

        if key == "":
            key_info_label.update("No key selected")
            self.query_one("#explore-button").disabled = True
            return

        key_info_text = Text("Key: ", style="bold magenta")
        key_info_text.append(Text(key, style="bold white"))
        self.query_one("#explore-button").disabled = False

        if key == "More keys on the next page...":
            return

        try:
            self.versions = listVersions(stub, self.collection, key)
            self.versions.sort()
            num_versions = (
                f"{len(self.versions)} versions"
                if len(self.versions) > 1
                else "1 version"
            )
            key_info_text.append(Text(f"\n{num_versions}: "))
            key_info_text.append(
                Text(",".join(map(str, self.versions)), style="bold white")
            )
            self.sanitized_key_name = RecordExplorer.sanitize_filename(
                f"{self.collection}_{key}_{self.versions[-1]}"
            )
        except Exception as e:
            key_info_text.append(Text("\nCould not load versions: " + str(e)))
        key_info_label.update(key_info_text)

    def explore_key(self) -> None:
        # Add new Explorer tab
        tabbed_content = self.app.query_one(TabbedContent)
        tab_id = f"record_explorer_tab_{self.sanitized_key_name}"

        record_explorer = RecordExplorer(
            stub=self.stub,
            key=self.key,
            collection=self.collection,
            id=f"record_explorer_{self.sanitized_key_name}",
        )

        if not tabbed_content.query(f"#{tab_id}"):
            tabbed_content.add_pane(
                TabPane(
                    "Record Explorer " + self.key,
                    record_explorer,
                    id=tab_id,
                )
            )
        # Set the active tab
        tabbed_content.active = tab_id

    def on_button_pressed(self, event: Button.Pressed) -> None:
        event.stop()
        if event.button.id == "explore-button":
            self.explore_key()

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(id="key-info-label")

            exploreButton = Button(label="Explore record (e)", id="explore-button")
            yield exploreButton


class RecordBrowser(Static):

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
        Binding("c", "go_to_collection_selection", "Select collection", show=True),
        Binding("e", "explore_key", "Explore the selected key", show=True),
    ]

    more_keys_available = False
    prefix = ""
    collection = "volumeData"

    # found_keys stores all found keys of the current collection / prefix
    found_keys = []

    # the offset of the current key list
    query_offset = 0

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

    def __init__(self, stub, collection: str, key_list_limit: int, **kwargs):
        super().__init__(**kwargs)
        self.stub = stub
        self.collection = collection
        self.key_list_limit = key_list_limit

    def compose(self) -> ComposeResult:
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

    def reset_local_keys(self) -> None:
        self.query_one(KeyInfoWidget).update_key("")
        self.query_offset = 0
        self.found_keys = []

    @on(Input.Submitted)
    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "collection":
            self.collection = event.input.value
        if event.input.id == "prefix":
            self.prefix = event.input.value
        self.reset_local_keys()
        self.refresh_data()

    @work
    async def load_version_number(self, key: str, key_index: int) -> None:
        table = self.query_one(DataTable)
        try:
            versions = listVersions(self.stub, self.collection, key)
            numVersions = len(versions)
            table.update_cell_at((key_index, 1), str(numVersions))
        except Exception as e:
            table.update_cell_at((key_index, 1), "Could not load versions: " + str(e))

    def fetch_keys(self, query_after_key: str, limit: int) -> list:
        if self.prefix != "":
            found_keys = getMultipleKeys(
                self.stub,
                self.collection,
                self.prefix,
                query_after_key,
                limit + 1,  # +1 to check if there are more keys
            )
        else:
            found_keys = listKeys(
                self.stub, self.collection, query_after_key, limit + 1
            )
        return found_keys

    def refresh_data(self) -> None:
        """Refresh the data in the table."""
        table = self.query_one(DataTable)
        self.query_one(KeyInfoWidget).collection = self.collection
        table.clear(columns=True)
        table.add_column("key")
        table.add_column("#versions")

        # Query offset is the index of the key that will be the first key in the new list
        if self.query_offset != 0:
            query_after_key = self.found_keys[self.query_offset - 1]
        else:
            query_after_key = ""

        try:
            result_keys = self.fetch_keys(query_after_key, self.key_list_limit)
            self.more_keys_available = False
            if len(result_keys) > self.key_list_limit:
                self.more_keys_available = True
                result_keys.pop()

            if self.query_offset == 0:
                # First query, replace the list
                self.found_keys = result_keys
            elif self.query_offset < len(self.found_keys):
                # Querying keys that we already know, update the list
                for i in range(len(result_keys)):
                    self.found_keys[self.query_offset + i] = result_keys[i]
            else:
                self.found_keys.extend(result_keys)

            if self.more_keys_available:
                self.estimate_key_count()

            for i, key in enumerate(result_keys):
                label = Text(str(i + self.query_offset), style="#B0FC38 italic")
                table.add_row(key, "", label=label)
                # Asynchronously fetch the number of versions for each key
                self.load_version_number(key, i)
            if self.more_keys_available:
                table.add_row(
                    f"Found more than {self.key_list_limit} keys, more on the next page...",
                    "",
                    label=Text("...", style="#B0FC38 italic"),
                )
            else:
                self.more_keys_row_key = None
                table.add_row(
                    f"Found {len(self.found_keys)} keys",
                    "",
                    label=Text("EOF", style="#B0FC38 italic"),
                )
            table.focus()
        except Exception as e:
            if "No store for column family" in str(e):
                table.add_row("Collection not found: " + self.collection)
            else:
                table.add_row("Could not load keys: " + str(e))

    @work(exclusive=True)
    async def estimate_key_count(self) -> None:
        """Estimate the number of keys in the collection."""

        def update_count(count):
            if self.more_keys_available:
                # This note is only shown if there are more keys available
                table = self.query_one(DataTable)
                more_keys_coords = (self.key_list_limit, 0)
                table.update_cell_at(
                    more_keys_coords, f"Found {count} keys, more on the next page..."
                )

        # For huge collections, don't request more than 1e6 times
        TOTAL_REQUEST_COUNT = 1e6
        KEY_LIST_LIMIT = 100
        request_count = 0
        count = len(self.found_keys)
        last_key = self.found_keys[-1]
        while request_count < TOTAL_REQUEST_COUNT:
            keys = self.fetch_keys(last_key, KEY_LIST_LIMIT)
            count += len(keys)
            if len(keys) < KEY_LIST_LIMIT:
                break
            last_key = keys[-1]
            request_count += 1
            update_count(count)
        update_count(count)

    def action_quit(self) -> None:
        """An action to quit the app."""
        self.app.exit()

    def action_refresh(self) -> None:
        """An action to refresh the data."""
        self.refresh_data()

    def action_show_next(self) -> None:
        """An action to show the next key_list_limit keys."""
        if self.more_keys_available:
            self.query_offset += self.key_list_limit
            self.refresh_data()

    def action_show_prev(self) -> None:
        """An action to show the previous key_list_limit keys."""
        if self.query_offset > 0:
            self.query_offset -= self.key_list_limit
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
            if self.query_offset > 0:
                self.action_show_prev()
                table.cursor_coordinate = (
                    len(table.rows) - 2,  # -1 for last row, -1 for the More keys row
                    table.cursor_coordinate.column,
                )

    def action_go_to_collection_selection(self) -> None:
        """An action to select the collection."""
        self.query_one("#collection").focus()

    def action_explore_key(self) -> None:
        """An action to explore the selected key."""
        self.query_one(KeyInfoWidget).explore_key()


class FossilDBClient(App):
    """A Textual app to manage FossilDB databases."""

    CSS_PATH = "client.tcss"

    title = "FossilDB Client"

    def __init__(self, stub, collection, count):
        super().__init__()
        self.stub = stub
        self.collection = collection
        self.key_list_limit = int(count)

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        with TabbedContent(id="main-tabs", initial="main-tab"):
            with TabPane(id="main-tab", title="FossilDB Browser"):
                yield RecordBrowser(
                    id="record-browser",
                    stub=self.stub,
                    collection=self.collection,
                    key_list_limit=self.key_list_limit,
                )
        yield Footer()


def init_argument_parser() -> argparse.ArgumentParser:
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
