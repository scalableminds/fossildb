from protobuf_decoder.protobuf_decoder import Parser
from textual.app import ComposeResult
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Label, Static, Collapsible, Rule, Button, OptionList
from textual.widgets.option_list import Option
from textual.containers import Horizontal, Vertical, Grid
import json

from db_connection import deleteVersion, getKey, listVersions

class ProtobufDecoder():
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

    cached_data = {}
    def get_data(self):
        if self.selected_version in self.cached_data:
            return self.cached_data[self.selected_version]
        self.cached_data[self.selected_version] = getKey(self.stub, self.collection, self.key, self.selected_version)
        return self.cached_data[self.selected_version]
    
    def get_filename(self):
        return f"{self.collection}_{self.key}_{self.selected_version}.bin"
    
    def download_data(self):
        data = self.get_data()
        with open(self.get_filename(), "wb") as f:
            f.write(data)
        self.app.push_screen(DownloadNotification(filename=self.get_filename()))

    def delete_data(self):
        async def delete_callback(result: bool):
            if result:
                deleteVersion(self.stub, self.collection, self.key, self.selected_version)
                self.versions = listVersions(self.stub, self.collection, self.key)
                self.versions.sort()
                if len(self.versions) == 0:
                    self.app.pop_screen()
                else:
                    self.selected_version = self.versions[-1]
                    await self.recompose()

        self.app.push_screen(DeleteModal(stub=self.stub, collection=self.collection, key=self.key, version=self.selected_version), delete_callback)

    def display_record(self):
        data = self.get_data()
        parsed = self.decoder.decode(data).to_dict()
        # TODO: There is no scrollbar for some reason
        return Vertical(*self.render_wire(parsed))
    

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
                    *self.render_wire(field_data), title=f"Field {field_number} (Type protobuf)")
            elif field_type == "varint":
                values = self.decode_varint(field_data)
                text = ", ".join([f"({val['type']}) {val['value']}" for val in values])
                yield Collapsible(Static(text), title=f"Field {field_number} (Type {field_type})")
            elif field_type == "fixed64":
                double = field_data["value"]
                integer = field_data["signed_int"]
                yield Collapsible(Static(f"(double) {double}, (int) {integer}"), title=f"Field {field_number} (Type {field_type})")
            else:
                yield Collapsible(Label(f"{field_data}"), title=f"Field {field_number} (Type {field_type})")

    def render_version_buttons(self):
        with Horizontal():
            for version in self.versions:
                yield Button(f"Select version {version}", id=f"version_{version}")

    def render_info_panel(self):
        return Vertical(
            Static(f"Exploring record/wire for {self.collection}:{self.key}. Currently viewing version {self.selected_version}"),
            Rule(),
            Button("Download selected version", id="download_button"),
            Button("Delete selected version", id="delete_button"),
            Rule(),
            *self.render_version_buttons(),
            id = "record_explorer_info_panel"
        )

    def compose(self):
        # TODO: There is a third element in the middle for some reason
        with Horizontal():
            yield self.display_record()
            yield self.render_info_panel()

    async def on_button_pressed(self, event):
        if event.button.id.startswith("version_"):
            version = int(event.button.id.split("_")[1])
            self.selected_version = version
            await self.recompose()
        if event.button.id == "download_button":
            self.download_data()
        if event.button.id == "delete_button":
            self.delete_data()


class DownloadNotification(Screen):
    """Screen with a note on where the downloaded file is stored."""

    CSS_PATH = "modal.tcss"

    def __init__(self, name = None, id = None, classes = None, filename = None, **kwargs):
        super().__init__(name, id, classes)
        self.filename = filename

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(f"The version has been stored in {self.filename}", id="note"),
            Button("Okay", variant="primary", id="okay"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "okay":
            self.app.pop_screen()

class DeleteModal(Screen):
    """Screen with a note on where the downloaded file is stored."""

    CSS_PATH = "modal.tcss"

    def __init__(self, name = None, id = None, classes = None,collection = None, key = None, version = None, **kwargs):
        super().__init__(name, id, classes)
        self.collection = collection
        self.key = key
        self.version = version

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label(f"Are you sure you want to delete version {self.version} of {self.collection}:{self.key}?", id="note"),
            Button("Yes", variant="error", id="yes"),
            Button("No", variant="primary", id="no"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "yes":
            self.dismiss(True)
        if event.button.id == "no":
            self.dismiss(False)

if __name__ == "__main__":
    print("Running decoder demo!")
    decoder = ProtobufDecoder()
    with open("data.bin", "rb") as f:
        data = f.read()
        print(json.dumps(decoder.decode(data).to_dict()))