import logging
from ._utils import read_json, time_method, serialize, deserialize, progress_bar, log_before_import, log_after_import

_logger = logging.getLogger("pump.groups")


class groups:

    TYPE = 6
    DEF_GID_ANON = "0"
    DEF_GID_ADMIN = "1"

    def __init__(self, eperson_file_str: str, g2g_file_str: str):
        self._eperson = read_json(eperson_file_str)
        self._g2g = read_json(g2g_file_str)
        self._imported = {
            "eperson": 0,
            "group": 0,
            "g2g": 0,
            "default_groups": 0,
            "coll_groups": 0,
        }

        # created during import

        # all imported group
        self._group_id2uuid = {}

        if len(self._eperson) == 0:
            _logger.info(f"Empty input collections: [{eperson_file_str}].")

        if len(self._g2g) == 0:
            _logger.info(f"Empty input collections: [{g2g_file_str}].")

    @property
    def imported_eperson(self):
        return self._imported['eperson']

    @property
    def imported_g2g(self):
        return self._imported['g2g']

    @property
    def anonymous(self):
        return self.uuid(groups.DEF_GID_ANON)

    @property
    def admins(self):
        return self.uuid(groups.DEF_GID_ADMIN)

    def from_rest(self, dspace, ignore_other=False):
        """
            Load Administrator and Anonymous groups into dict.
            This data already exists in database.
            Remember its id.
        """
        res = dspace.fetch_existing_epersongroups()
        if res is None:
            return self

        other_groups = []
        for group in res:
            if group['name'] == 'Anonymous':
                self._group_id2uuid[groups.DEF_GID_ANON] = [group['id']]
                continue

            if group['name'] == 'Administrator':
                self._group_id2uuid[groups.DEF_GID_ADMIN] = [group['id']]
                continue

            other_groups.append(group)
        _logger.info(
            f"Loaded groups [{self._group_id2uuid}], other groups:[{len(other_groups)}]")
        return self

    def uuid(self, gid: int):
        assert isinstance(list(self._group_id2uuid.keys())[0], str)
        return self._group_id2uuid.get(str(gid), None)

    @time_method
    def import_to(self, dspace, metadatas, coll_groups, comm_groups):
        # Do not import groups which are already imported
        self._group_id2uuid.update(coll_groups)
        self._group_id2uuid.update(comm_groups)
        self._import_eperson(dspace, metadatas)
        self._import_group2group(dspace)

    def _import_eperson(self, dspace, metadatas):
        """
            Import data into database.
            Mapped tables: epersongroup
        """
        expected = len(self._eperson)
        log_key = "epersons"
        log_before_import(log_key, expected)

        grps = []

        for eg in progress_bar(self._eperson):
            g_id = eg['eperson_group_id']

            # group Administrator and Anonymous already exist
            # group is created with dspace object too
            if str(g_id) in (groups.DEF_GID_ADMIN, groups.DEF_GID_ANON):
                self._imported["default_groups"] += 1
                continue
            if str(g_id) in (self._group_id2uuid.keys()):
                self._imported["coll_groups"] += 1
                continue

            # get group metadata
            g_meta = metadatas.value(groups.TYPE, g_id)
            if 'dc.title' not in g_meta:
                _logger.error(f'Metadata for group [{g_id}] does not contain dc.title!')
                continue

            name = g_meta['dc.title'][0]['value']
            del g_meta['dc.title']

            # the group_metadata contains the name of the group
            data = {'name': name, 'metadata': g_meta}
            grps.append(name)
            try:
                # continue
                resp = dspace.put_eperson_group({}, data)
                self._group_id2uuid[str(g_id)] = [resp['id']]
                self._imported["eperson"] += 1
            except Exception as e:
                _logger.error(f'put_eperson_group: [{g_id}] failed [{str(e)}]')

        # sql_del = "delete from epersongroup where name='" + "' or name='".join(grps) + "' ;"
        # _logger.info(sql_del)

        log_after_import(f'{log_key} [known existing:{self._imported["default_groups"]}]',
                         expected, self.imported_eperson + self._imported["default_groups"])

    def _import_group2group(self, dspace):
        """
            Import data into database.
            Mapped tables: group2group
        """
        expected = len(self._g2g)
        log_key = "epersons g2g"
        log_before_import(log_key, expected)

        for g2g in progress_bar(self._g2g):
            parent_a = self.uuid(g2g['parent_id'])
            child_a = self.uuid(g2g['child_id'])
            if parent_a is None or child_a is None:
                _logger.critical(
                    f"Invalid uuid for [{g2g['parent_id']}] or [{g2g['child_id']}]")
                continue

            for parent in parent_a:
                for child in child_a:
                    try:
                        dspace.put_group2group(parent, child)
                        # TODO Update statistics when the collection has more group relations.
                        self._imported["g2g"] += 1
                    except Exception as e:
                        _logger.error(f'put_group2group: [{parent}][{child}] failed [{str(e)}]')


        log_after_import(log_key, expected, self.imported_g2g)

    # =============

    def serialize(self, file_str: str):
        data = {
            "eperson": self._eperson,
            "group_id2uuid": self._group_id2uuid,
            "imported": self._imported,
        }
        serialize(file_str, data)

    def deserialize(self, file_str: str):
        data = deserialize(file_str)
        self._eperson = data["eperson"]
        self._group_id2uuid = data["group_id2uuid"]
        self._imported = data["imported"]
