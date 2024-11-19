import os
import json
import logging
from datetime import datetime, timezone
from dspace_rest_client.models import Bitstream  # noqa

_this_dir = os.path.dirname(os.path.abspath(__file__))
_logger = logging.getLogger("cache")


def ts() -> str:
    return str(datetime.now(timezone.utc))


class cache:

    file_name = "metadata.json"

    def __init__(self, dir: str, load: bool):
        self._dir = dir
        self._load = load
        self._file = os.path.join(self._dir, cache.file_name)
        self._data = {}
        self._info = {}
        self._dirty = False
        if self._load:
            if os.path.exists(self._file) is False:
                _logger.error(f"File {self._file} does not exist.")
                raise FileNotFoundError(f"File {self._file} does not exist.")
            self.deserialize()

    def __len__(self):
        return len(self._data)

    @property
    def data(self):
        return self._data

    def add_info(self, key: str, val):
        self._info[key] = val

    def validate_save(self):
        """
            Check if we can safely store new data.
        """
        os.makedirs(self._dir, exist_ok=True)
        if os.path.exists(self._file):
            _logger.critical(f"File {self._file} already exists.")
            raise FileExistsError(f"File {self._file} already exists.")

    def iter_items(self):
        for k, v in self._data.items():
            yield v["item"], v["bitstreams"]

    def add(self, item, bitstreams):
        self._dirty = True
        uuid = item['uuid']
        if uuid in self._data:
            _logger.error(f"Item {uuid} already exists.")
            return

        bitstreams = bitstreams or []
        self._data[uuid] = {
            "item": item,
            "bitstreams": [b.as_dict() for b in bitstreams],
        }

    def deserialize(self):
        try:
            import orjson
            has_orjson = True
        except Exception:
            has_orjson = False

        _logger.info(f"Loading {self._file}, has_orjson:{has_orjson}")
        with open(self._file, "r") as f:
            if has_orjson:
                d = orjson.loads(f.read())  # noqa
            else:
                d = json.load(f)
        _logger.info(
            f"Loaded {self._file} from {d['timestamp']}, {len(d['data'])} items, [{d['info']}] info.")
        self._data = d["data"]
        for k, v in self._data.items():
            for i, b in enumerate(v["bitstreams"]):
                v["bitstreams"][i] = Bitstream(b)

    def serialize(self):
        if self._dirty is False:
            return
        d = {
            "data": self._data,
            "timestamp": ts(),
            "info": self._info,
        }
        with open(self._file, "w") as fout:
            json.dump(d, fout, indent=0, sort_keys=True)
