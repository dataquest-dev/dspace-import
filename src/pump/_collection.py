import logging
import re
from ._group import groups
from ._utils import read_json, time_method, serialize, deserialize, progress_bar, log_before_import, log_after_import

_logger = logging.getLogger("pump.collection")


class collections:
    """
        SQL:
            delete from cwf_collectionrole ; delete from collection ;
    """
    validate_table = [
        ["collection", {
            "nonnull": ["logo_bitstream_id"],
        }],
        ["collection2item", {
        }],
    ]

    TYPE = 3

    def __init__(self, col_file_str: str, com2col_file_str: str, metadata_file_str: str):
        self._col = read_json(col_file_str)
        self._com2col = read_json(com2col_file_str)
        self._imported = {
            "col": 0,
            "group": 0,
        }
        self._metadata_values = read_json(metadata_file_str)
        self._id2uuid = {}

        self._logos = {}
        self._groups_id2uuid = {}

        if len(self._col) == 0:
            _logger.info(f"Empty input collections: [{col_file_str}].")
            return

        if len(self._com2col) == 0:
            _logger.info(f"Empty input community2collection: [{com2col_file_str}].")
            return

        # because the role DEFAULT_READ is without old group id in collection
        self._col2group = {}
        col_def_read_rec = re.compile("COLLECTION_(.*)_DEFAULT_READ")
        for meta in self._metadata_values:
            if meta['resource_type_id'] != groups.TYPE:
                continue
            m_text = meta['text_value']
            m = col_def_read_rec.search(m_text)
            if m is None:
                continue
            self._col2group[int(m.group(1))] = meta['resource_id']

    def __len__(self):
        return len(self._col)

    def uuid(self, com_id: int):
        # NOTE: we have string indices
        return self._id2uuid.get(str(com_id), None)

    def group_uuid(self, g_id: int):
        # NOTE: we have string indices
        return self._groups_id2uuid.get(str(g_id), [])

    @property
    def logos(self):
        return self._logos

    @property
    def imported_cols(self):
        return self._imported['col']

    @property
    def imported_groups(self):
        return self._imported['group']

    @property
    def groups_id2uuid(self):
        return self._groups_id2uuid

    @time_method
    def import_to(self, dspace, handles, metadatas, coms):
        expected = len(self)
        log_key = "collections"
        log_before_import(log_key, expected)

        coll2com = {x['collection_id']: x['community_id'] for x in self._com2col}

        for col in progress_bar(self._col):
            col_id = col['collection_id']

            data = {}
            meta_col = metadatas.value(collections.TYPE, col_id)
            data['metadata'] = meta_col

            handle_col = handles.get(collections.TYPE, col_id)
            data['handle'] = handle_col

            # filter
            data = {k: v for k, v in data.items() if v is not None}

            param = {'parent': coms.uuid(coll2com[col_id])}

            try:
                resp = dspace.put_collection(param, data)
                col_uuid = resp['id']
                self._id2uuid[str(col_id)] = col_uuid
                self._imported["col"] += 1
            except Exception as e:
                _logger.error(f'put_collection: [{col_id}] failed [{str(e)}]')
                continue

            # add to collection2logo, if collection has logo
            if col['logo_bitstream_id'] is not None:
                self._logos[col_id] = col["logo_bitstream_id"]

            # greate group
            # template_item_id, workflow_step_1, workflow_step_3, admin are not implemented,
            # because they are null in all data
            ws2 = col['workflow_step_2']
            if ws2:
                try:
                    resp = dspace.put_collection_editor_group(col_uuid)
                    self._groups_id2uuid[str(ws2)] = [resp['id']]
                    self._imported["group"] += 1
                except Exception as e:
                    _logger.error(
                        f'put_collection_editor_group: [{col_id}] failed [{str(e)}]')

            subm = col['submitter']
            if subm:
                try:
                    resp = dspace.put_collection_submitter(col_uuid)
                    self._groups_id2uuid[str(subm)] = [resp['id']]
                    self._imported["group"] += 1
                except Exception as e:
                    _logger.error(
                        f'put_collection_submitter: [{col_id}] failed [{str(e)}]')

            if col_id in self._col2group:
                group_col = self._col2group[col_id]
                try:
                    resp = dspace.put_collection_bitstream_read_group(col_uuid)
                    self._groups_id2uuid.setdefault(str(group_col), []).append(resp['id'])
                    self._imported["group"] += 1
                except Exception as e:
                    _logger.error(
                        f'put_collection_bitstream_read_group: [{col_id}] failed [{str(e)}]')

                try:
                    resp = dspace.put_collection_item_read_group(col_uuid)
                    self._groups_id2uuid.setdefault(str(group_col), []).append(resp['id'])
                    self._imported["group"] += 1
                except Exception as e:
                    _logger.error(
                        f'put_collection_item_read_group: [{col_id}] failed [{str(e)}]')

        log_after_import(log_key, expected, self.imported_cols)

    # =============

    def serialize(self, file_str: str):
        data = {
            "id2uuid": self._id2uuid,
            "logos": self._logos,
            "groups_id2uuid": self._groups_id2uuid,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        # TODO(jm): support older cache files
        key = "id2uuid" if "id2uuid" in data else "col_created"
        self._id2uuid = data[key]
        self._logos = data["logos"]
        self._groups_id2uuid = data["groups_id2uuid"]
        self._imported = data["imported"]
