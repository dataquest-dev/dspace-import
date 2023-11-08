import logging
from ._utils import read_json, time_method, serialize, deserialize, progress_bar

_logger = logging.getLogger("pump.bundle")


class bundles:
    """
        Mapped tables: item2bundle, bundle
        SQL:
    """
    TYPE = 1

    def __init__(self, bundle_file_str: str, item2bundle_file_str: str):
        self._bundles = read_json(bundle_file_str)
        self._item2bundle = read_json(item2bundle_file_str)
        self._imported = {
            "bundles": 0,
        }
        self._id2uuid = {}

        if len(self._bundles) == 0:
            _logger.info(f"Empty input: [{bundle_file_str}].")
            return

        self._itemid2bundle = {}
        for e in self._item2bundle:
            self._itemid2bundle.setdefault(e['item_id'], []).append(e['bundle_id'])

        self._primary = {}
        for b in self._bundles:
            primary_id = b['primary_bitstream_id']
            if primary_id:
                self._primary[primary_id] = b['bundle_id']

    def __len__(self):
        return len(self._bundles)

    def uuid(self, b_id: int):
        return self._id2uuid[str(b_id)]

    @property
    def primary(self):
        return self._primary

    @property
    def imported(self):
        return self._imported['bundles']

    @time_method
    def import_to(self, dspace, metadatas, items):
        _logger.info(f"Importing bundles [{len(self)}]")

        for item_id, bundle_arr in progress_bar(self._itemid2bundle.items()):
            for bundle_id in bundle_arr:
                data = {}
                meta_bundle = metadatas.value(bundles.TYPE, bundle_id)
                if meta_bundle:
                    data['metadata'] = meta_bundle
                    data['name'] = meta_bundle['dc.title'][0]['value']

                try:
                    item_uuid = items.uuid(item_id)
                    if item_uuid is None:
                        _logger.critical(f'Item UUID not found for [{item_id}]')
                        continue
                    resp = dspace.put_bundle(item_uuid, data)
                    self._id2uuid[str(bundle_id)] = resp['uuid']
                    self._imported["bundles"] += 1
                except Exception as e:
                    _logger.error(f'put_bundle: [{item_id}] failed [{str(e)}]')

        _logger.info(f"Bundles [{self.imported}] imported")

    # =============

    def serialize(self, file_str: str):
        # not needed _itemid2bundle, _primary
        data = {
            "bundles": self._bundles,
            "item2bundle": self._item2bundle,
            "id2uuid": self._id2uuid,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        self._bundles = data["bundles"]
        self._item2bundle = data["item2bundle"]
        self._id2uuid = data["id2uuid"]
        self._imported = data["imported"]
