import logging
import os
import json
import shutil

from ._utils import time_method

from ._handle import handles
from ._metadata import metadatas

from ._group import groups
from ._community import communities
from ._collection import collections
from ._registrationdata import registrationdatas
from ._eperson import epersons
from ._eperson import groups as eperson_groups
from ._userregistration import userregistrations
from ._bitstreamformatregistry import bitstreamformatregistry
from ._license import licenses
from ._item import items
from ._tasklistitem import tasklistitems
from ._bundle import bundles
from ._bitstream import bitstreams
from ._resourcepolicy import resourcepolicies
from ._usermetadata import usermetadatas
from ._db import db, differ
from ._sequences import sequences

_logger = logging.getLogger("pump.repo")


def export_table(db, table_name: str, out_f: str):
    with open(out_f, 'w', encoding='utf-8') as fout:
        js = db.fetch_one(f'SELECT json_agg(row_to_json(t)) FROM "{table_name}" t')
        json.dump(js, fout)


class repo:
    @time_method
    def __init__(self, env: dict, dspace):

        self.raw_db_dspace_5 = db(env["db_dspace_5"])
        self.raw_db_utilities_5 = db(env["db_utilities_5"])

        # remove directory
        if os.path.exists(env["input"]["tempdbexport"]):
            shutil.rmtree(env["input"]["tempdbexport"])

        tables_db_5 = [x for arr in self.raw_db_dspace_5.all_tables() for x in arr]
        tables_utilities_5 = [x for arr in self.raw_db_utilities_5.all_tables()
                              for x in arr]

        def _f(table_name):
            """ Dynamically export the table to json file and return path to it. """
            os.makedirs(env["input"]["tempdbexport"], exist_ok=True)
            out_f = os.path.join(env["input"]["tempdbexport"], f"{table_name}.json")
            if table_name in tables_db_5:
                db = self.raw_db_dspace_5
            elif table_name in tables_utilities_5:
                db = self.raw_db_utilities_5
            else:
                _logger.warning(f"Table [{table_name}] not found in db.")
                raise NotImplementedError(f"Table [{table_name}] not found in db.")
            export_table(db, table_name, out_f)
            return out_f

        # load groups
        self.groups = groups(
            _f("epersongroup"),
            _f("group2group"),
        )
        self.groups.from_rest(dspace)

        # load handles
        self.handles = handles(_f("handle"))

        # load metadata
        self.metadatas = metadatas(
            env,
            dspace,
            _f("metadatavalue"),
            _f("metadatafieldregistry"),
            _f("metadataschemaregistry"),
        )

        # load community
        self.communities = communities(
            _f("community"),
            _f("community2community"),
        )

        self.collections = collections(
            _f("collection"),
            _f("community2collection"),
            _f("metadatavalue"),
        )

        self.registrationdatas = registrationdatas(
            _f("registrationdata")
        )

        self.epersons = epersons(
            _f("eperson")
        )

        self.egroups = eperson_groups(
            _f("epersongroup2eperson")
        )

        self.userregistrations = userregistrations(
            _f("user_registration")
        )

        self.bitstreamformatregistry = bitstreamformatregistry(
            _f("bitstreamformatregistry"), _f("fileextension")
        )

        self.licenses = licenses(
            _f("license_label"),
            _f("license_definition"),
            _f("license_label_extended_mapping"),
        )

        self.items = items(
            _f("item"),
            _f("workspaceitem"),
            _f("workflowitem"),
            _f("collection2item"),
        )

        self.tasklistitems = tasklistitems(
            _f("tasklistitem")
        )

        self.bundles = bundles(
            _f("bundle"),
            _f("item2bundle"),
        )

        self.bitstreams = bitstreams(
            _f("bitstream"),
            _f("bundle2bitstream"),
        )

        self.usermetadatas = usermetadatas(
            _f("user_metadata"),
            _f("license_resource_user_allowance"),
            _f("license_resource_mapping")
        )

        self.resourcepolicies = resourcepolicies(
            _f("resourcepolicy")
        )

        self.raw_db_7 = db(env["db_dspace_7"])

        self.sequences = sequences()

    def diff(self, to_validate=None):
        if to_validate is None:
            to_validate = [
                getattr(getattr(self, x), "validate_table")
                for x in dir(self) if hasattr(getattr(self, x), "validate_table")
            ]
        else:
            if not hasattr(to_validate, "validate_table"):
                _logger.warning(f"Missing validate_table in {to_validate}")
                return
            to_validate = [to_validate.validate_table]

        diff = differ(self.raw_db_dspace_5, self.raw_db_utilities_5,
                      self.raw_db_7, repo=self)
        diff.validate(to_validate)

    # =====
    def uuid(self, res_type_id: int, res_id: int):
        # find object id based on its type
        try:
            if res_type_id == self.communities.TYPE:
                return self.communities.uuid(res_id)
            if res_type_id == self.collections.TYPE:
                return self.collections.uuid(res_id)
            if res_type_id == self.items.TYPE:
                return self.items.uuid(res_id)
            if res_type_id == self.bitstreams.TYPE:
                return self.bitstreams.uuid(res_id)
            if res_type_id == self.bundles.TYPE:
                return self.bundles.uuid(res_id)
            if res_type_id == self.epersons.TYPE:
                return self.epersons.uuid(res_id)
            if res_type_id == self.groups.TYPE:
                arr = self.groups.uuid(res_id)
                if len(arr or []) > 0:
                    return arr[0]
        except Exception as e:
            return None
        return None
