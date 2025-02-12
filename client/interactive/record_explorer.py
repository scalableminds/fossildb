import re

from db_connection import deleteVersion, getKey, listVersions
from protobuf_decoder.protobuf_decoder import Parser
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import (Button, Collapsible, Label, OptionList, Rule,
                             Static, TabbedContent)
from textual.widgets.option_list import Option


class ProtobufDecoder:
    def __init__(self):
        self.parser = Parser()

    def decode(self, data: bytes):
        hex = data.hex()
        return self.parser.parse(hex)


class RecordExplorer(Static):
    def __init__(self, stub, key: str, collection: str, **kwargs):
        super().__init__(**kwargs)
        self.stub = stub
        self.decoder = ProtobufDecoder()
        self.key = key
        self.collection = collection

        try:
            self.versions = listVersions(stub, self.collection, key)
            self.versions.sort()
            self.selected_version = self.versions[-1]
        except Exception as e:
            print("Could not load versions: " + str(e))

    selected_version = reactive(0)

    BINDINGS = {
        Binding("d", "download_data", "Download the selected version", show=True),
        Binding("del", "delete_data", "Delete the selected version", show=True),
        Binding("k", "next_version", "Next version", show=True),
        Binding("j", "previous_version", "Previous version", show=True),
        Binding("x", "close_tab", "Close tab", show=True),
    }

    cached_data = {}

    def get_data(self):
        if self.selected_version in self.cached_data:
            return self.cached_data[self.selected_version]
        self.cached_data[self.selected_version] = getKey(
            self.stub, self.collection, self.key, self.selected_version
        )
        return self.cached_data[self.selected_version]

    def sanitize_filename(name):
        s = str(name).strip().replace(" ", "_")
        return re.sub(r"(?u)[^-\w.]", "_", s)

    def get_filename(self):
        sanitized_key = RecordExplorer.sanitize_filename(
            f"{self.collection}_{self.key}_{self.selected_version}"
        )
        return f"{sanitized_key}.bin"

    def action_download_data(self):
        data = self.get_data()
        with open(self.get_filename(), "wb") as f:
            f.write(data)
        self.app.push_screen(DownloadNotification(filename=self.get_filename()))

    def action_delete_data(self):
        async def delete_callback(result: bool):
            if result:
                deleteVersion(
                    self.stub, self.collection, self.key, self.selected_version
                )
                self.versions = listVersions(self.stub, self.collection, self.key)
                self.versions.sort()
                if len(self.versions) == 0:
                    self.app.pop_screen()
                else:
                    self.selected_version = self.versions[-1]
                    await self.recompose()

        self.app.push_screen(
            DeleteModal(
                stub=self.stub,
                collection=self.collection,
                key=self.key,
                version=self.selected_version,
            ),
            delete_callback,
        )

    def display_record(self):
        data = self.get_data()
        parsed = self.decoder.decode(data).to_dict()
        return Vertical(*self.render_wire(parsed), id="record_explorer_display")

    def decode_varint(self, value: int):
        def interpret_as_twos_complement(val: int, bits: int) -> int:
            """Interprets an unsigned integer as a two's complement signed integer with the given bit width."""
            if val >= (1 << (bits - 1)):
                return val - (1 << bits)
            return val

        def interpret_as_signed_type(val: int) -> int:
            """Custom logic to interpret as a signed type (similar to sint in proto)."""
            if val % 2 == 0:
                return val // 2
            else:
                return -(val // 2) - 1

        result = []
        uint_val = value
        result.append({"type": "uint", "value": str(uint_val)})

        for bits in [8, 16, 32, 64]:
            int_val = interpret_as_twos_complement(uint_val, bits)
            if int_val != uint_val:
                result.append({"type": f"int{bits}", "value": str(int_val)})

        signed_int_val = interpret_as_signed_type(uint_val)
        if signed_int_val != uint_val:
            result.append({"type": "sint", "value": str(signed_int_val)})

        return result

    def render_wire(self, spec: dict):
        for field in spec["results"]:
            field_number = field["field"]
            field_type = field["wire_type"]
            field_data = field["data"]
            if field_type == "length_delimited":
                yield Collapsible(
                    *self.render_wire(field_data),
                    title=f"Field {field_number} (Type protobuf)",
                    classes="protobuf",
                )
            elif field_type == "varint":
                values = self.decode_varint(field_data)
                text = "\n".join([f"({val['type']}) {val['value']}" for val in values])
                yield Collapsible(
                    Static(text),
                    title=f"Field {field_number} (Type {field_type})",
                    classes="varint",
                )
            elif field_type == "fixed64":
                double = field_data["value"]
                integer = field_data["signed_int"]
                yield Collapsible(
                    Static(f"(double) {double}\n(int) {integer}"),
                    title=f"Field {field_number} (Type {field_type})",
                    classes="fixed64",
                )
            elif field_type == "string":
                yield Collapsible(
                    Label(f"{field_data}"),
                    title=f"Field {field_number} (Type {field_type})",
                    classes="string",
                )
            else:
                yield Collapsible(
                    Label(f"{field_data}"),
                    title=f"Field {field_number} (Type {field_type})",
                )

    def render_version_buttons(self):
        with Horizontal():
            for version in self.versions:
                yield Button(f"Select version {version}", id=f"version_{version}")

    def render_info_panel(self):
        info_text = Text("Exploring record/wire for ")
        info_text.append(self.collection, style="bold magenta")
        info_text.append(":")
        info_text.append(self.key, style="bold magenta")
        info_text.append(".\nCurrently viewing version ")
        info_text.append(str(self.selected_version), style="bold blue")

        return Vertical(
            Static(info_text),
            Rule(),
            Button("Download selected version", id="download_button"),
            Button("Delete selected version", id="delete_button"),
            Rule(),
            *self.render_version_buttons(),
            id="record_explorer_info_panel",
        )

    def compose(self):
        with Horizontal():
            yield self.display_record()
            yield self.render_info_panel()

    async def on_button_pressed(self, event):
        if event.button.id.startswith("version_"):
            version = int(event.button.id.split("_")[1])
            self.selected_version = version
            await self.recompose()
        if event.button.id == "download_button":
            self.action_download_data()
        if event.button.id == "delete_button":
            self.action_delete_data()

    async def action_previous_version(self):
        current_index = list(self.versions).index(self.selected_version)
        if current_index == 0:
            return
        self.selected_version = self.versions[current_index - 1]
        await self.recompose()

    async def action_next_version(self):
        current_index = list(self.versions).index(self.selected_version)
        if current_index == len(self.versions) - 1:
            return
        self.selected_version = self.versions[current_index + 1]
        await self.recompose()

    def action_close_tab(self):
        tabbed_content = self.app.query_one(TabbedContent)
        tabbed_content.active = "main-tab"
        tabbed_content.remove_pane(self.parent.id)


class DownloadNotification(Screen):
    """Screen with a note on where the downloaded file is stored."""

    CSS_PATH = "modal.tcss"

    def __init__(self, name=None, id=None, classes=None, filename=None, **kwargs):
        super().__init__(name, id, classes)
        self.filename = filename

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static(f"The version has been stored in {self.filename}", id="note"),
            Button("Okay", variant="primary", id="okay"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "okay":
            self.app.pop_screen()


class DeleteModal(Screen):
    """Screen with a note on where the downloaded file is stored."""

    CSS_PATH = "modal.tcss"

    def __init__(
        self,
        name=None,
        id=None,
        classes=None,
        collection=None,
        key=None,
        version=None,
        **kwargs,
    ):
        super().__init__(name, id, classes)
        self.collection = collection
        self.key = key
        self.version = version

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(
                f"Are you sure you want to delete version {self.version} of {self.collection}:{self.key}?",
                id="note",
            ),
            Button("Yes", variant="error", id="yes"),
            Button("No", variant="primary", id="no"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "yes":
            self.dismiss(True)
        if event.button.id == "no":
            self.dismiss(False)
