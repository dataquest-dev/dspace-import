import argparse
import logging
import time
import os
import json
import sys

# Set up directories for imports
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

import utils

# load .env
dotenv_file = os.path.join(_this_dir, '../../src/', os.environ.get("ENVFILE", ".env"))
utils.load_env(dotenv_file)

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import Item
from utils import init_logging, update_settings  # noqa
logging.getLogger("dspace.client").setLevel(logging.WARNING)

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])


class iter_items_specific:
    def __init__(self, items, dspace_be):
        self.items = items
        self.dspace_be = dspace_be

    def __call__(self):
        for item_arr in self.items:
            uuid = item_arr[0]
            item_gen = self.dspace_be.iter_items(uuid=uuid)
            item_list = list(item_gen)
            yield item_list


def store_info(cache_file: str, d: dict, details: dict):
    new_d = {k: list(v) if isinstance(v, set) else v for k, v in d.items()}
    data = {
        "data": new_d,
        "details": details,
    }
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as fout:
        json.dump(data, fout, indent=2, sort_keys=True)
    _logger.info(f"Stored info to [{cache_file}]")


def update_item(item_d: dict):
    item = Item(item_d)
    if dspace_be.client.update_item(item):
        return True
    # Try to authenticate
    _logger.info("Reauthorization during item updating")
    if dspace_be.client.authenticate():
        dso = dspace_be.client.update_item(item)
        return dso is not None
    return False


class updater:

    ret_already_ok = 0
    ret_failed = 1
    ret_updated = 2
    ret_created = 3
    ret_invalid_meta = 4
    ret_empty_meta = 4

    def __init__(self, dspace_be, key: str, to: str, dry_run: bool = False):
        self._dspace_be = dspace_be
        self._key = key
        self._to = to
        self._dry_run = dry_run
        self._info = {
            "valid": [],
            "multiple": set(),
            "to_exists": set(),
            "updated": [],
            "error_updating": [],
            "error_creating": [],
            "created": [],
            "not_created": [],
        }

    @property
    def info(self):
        return self._info

    def update(self, item_d: dict) -> int:
        """
            Move metadata for items based on provided field.
        """
        item_mtd = item_d["metadata"]
        item = Item(item_d)
        uuid = item_d['uuid']

        # must exist and must be exactly one
        meta_from = item_mtd.get(self._key, None)
        if meta_from is None:
            return updater.ret_empty_meta

        if len(meta_from) != 1:
            _logger.critical(f"{uuid}: other than one value {meta_from}")
            self._info["multiple"].add(uuid)
            return updater.ret_invalid_meta

        existing_val = meta_from[0]["value"]

        # cannot exist
        meta_to = item_mtd.get(self._to, None)
        if meta_to is not None:
            _logger.critical(f"{uuid}: already exists [{meta_to}]")
            self._info["to_exists"].add(uuid)
            return updater.ret_invalid_meta

        # 1.st add new metadata
        added = (self._dry_run or
                 self._dspace_be.client.add_metadata(item, self._to, existing_val))
        if not added or added.uuid is None:
            _logger.critical(f"{uuid}: Error creating metadata")
            self._info["error_creating"].append((uuid, existing_val))
            return updater.ret_failed
        self._info["created"].append((uuid, existing_val))

        # delete
        try:
            if not self._dry_run:
                new_item = self._dspace_be.client.remove_metadata(item, self._key, 0)
                if not new_item or new_item.uuid is None:
                    _logger.critical(f"{uuid}: Error deleting metadata [{self._key}]")
                    self._info["error_updating"].append((uuid, existing_val))
                    return updater.ret_failed
        except Exception as e:
            _logger.error(f"{uuid}: Error deleting metadata [{self._key}]: {e}")
            self._info["error_updating"].append((uuid, existing_val))
            return updater.ret_failed

        return updater.ret_updated


def get_items_iterator(args, dspace_be):
    if args.only is None:
        return dspace_be.iter_items, False

    if not os.path.exists(args.only):
        _logger.error(f"File [{args.only}] does not exist")
        sys.exit(1)
    try:
        with open(args.only, "r") as fin:
            items = json.load(fin)
    except Exception:
        with open(args.only, "r", encoding="utf-8") as fin:
            items = [(x.strip(), None) for x in fin.read().splitlines()
                     if len(x.strip()) > 0 and not x.startswith("#")]
    _logger.info(f"Loaded [{len(items)}] items from [{args.only}]")
    return iter_items_specific(items, dspace_be), True


def get_dspace_con(args):
    user = os.environ.get("DSPACE_USER", args.user)
    password = os.environ.get("DSPACE_PASSWORD", args.password)
    endpoint = args.endpoint.rstrip("/")
    if "DSPACE_USER" in os.environ or "DSPACE_PASSWORD" in os.environ:
        _logger.info(f"Used environment variables: {user}")
    # Initialize DSpace backend
    feurl = args.feurl or endpoint.split("/server")[0]
    dspace_be = dspace.rest(endpoint, user, password, True)
    return dspace_be, feurl


def double_verify(dspace_be, ret_updated, from_key: str, to_key: str, orig_values) -> bool:
    if ret_updated not in (updater.ret_created, updater.ret_updated):
        _logger.error(f"{msg_header} returned unexpected value [{ret_updated}]")
        return False

    new_item = dspace_be._fetch(f'core/items/{uuid}', dspace_be.get, key=None)

    new_from_meta = [x['value'] for x in new_item.get("metadata", {}).get(from_key, [])]  # noqa
    if len(new_from_meta) != 0:
        _logger.error(f"{msg_header} not empty[{from_key}] after create/update")  # noqa
        return False

    new_to_meta = [x['value'] for x in new_item.get("metadata", {}).get(to_key, [])]  # noqa
    if len(new_to_meta) == 0 or new_to_meta != orig_values:
        _logger.error(f"{msg_header} empty [{to_key}] after create/update")  # noqa
        return False

    _logger.info(f"{msg_header} updated - {from_key} -> {to_key}")  # noqa
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Add metadata for DSpace items")
    parser.add_argument("--to",
                        type=str, required=True, help="Metadata field to be created.")
    parser.add_argument("--key",
                        type=str, required=True,
                        help="Metadata field(s) from which value(s) can be used, deleted afterwards.")
    parser.add_argument("--endpoint",
                        type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--feurl",
                        type=str, default=None)
    parser.add_argument("--user",
                        type=str, default=env["backend"]["user"])
    parser.add_argument("--password",
                        type=str, default=env["backend"]["password"])
    parser.add_argument("--dry-run",
                        action='store_true', default=False)
    parser.add_argument("--only",
                        type=str, default=None)
    def_result = os.path.join(_this_dir, "__results",
                              os.path.basename(env["log_file"]) + ".json")
    parser.add_argument("--result",
                        type=str, default=def_result)
    args = parser.parse_args()
    start = time.time()

    # output args from parse_args but without passwords
    args_dict = vars(args).copy()
    args_dict.pop("password", None)
    _logger.info(f"Arguments: {args_dict}")

    last_auth = time.time()
    dspace_be, feurl = get_dspace_con(args)
    upd = updater(dspace_be, args.key, args.to, dry_run=args.dry_run)
    iter_items, force = get_items_iterator(args, dspace_be)

    ret = {
        "len": 0,
        "failed": [],
    }
    re_auth_every = 60

    for i, items in enumerate(iter_items()):

        # re-auth
        if time.time() - last_auth > re_auth_every:
            _logger.info(f"Reauthorization [{i}] to dspace")
            dspace_be.client.authenticate()
            last_auth = time.time()

        for item in items:
            ret["len"] += 1
            uuid = item['uuid']
            item_url = f"{feurl}/items/{uuid}"
            msg_header = f"{ret['len']:5d}: Item [ {item_url} ]"
            orig_values = [x['value'] for x in item.get("metadata", {}).get(args.key, [])]

            ret_updated = upd.update(item)

            if ret_updated == updater.ret_already_ok:
                _logger.info(f"{msg_header}: already correct")
                continue

            # serious
            if ret_updated == updater.ret_failed:
                _logger.critical(f"{msg_header} failed to update metadata")
                continue
            if ret_updated == updater.ret_invalid_meta:
                _logger.warning(
                    f"{msg_header} does not have correct metadata [{orig_values}]")
                continue
            if ret_updated == updater.ret_empty_meta:
                _logger.warning(
                    f"{msg_header} does not have specified metadata [{args.from_mtd_field}]")
                continue

            if args.dry_run:
                _logger.info(f"{msg_header} updated - {orig_values} -> DRY-RUN")  # noqa
                continue

            # something changed, double verify
            if not double_verify(dspace_be, ret_updated, args.key, args.to, orig_values):
                ret['failed'].append((uuid, item_url, orig_values))

    store_info(args.result, upd.info, {"args_dict": args_dict})

    _logger.info(40 * "=")
    _logger.info("Item info:")
    limit = 50
    for k, v in upd.info.items():
        _logger.info(f"{k:20s}:{len(v):6d}: first {limit} items .. {list(v)[:limit]}...")

    _logger.info(40 * "=")
    _logger.info("Counts:")
    for k, v in ret.items():
        val = len(v) if isinstance(v, list) else v
        _logger.info(f"{k:20s}:{val:6d}")

    _logger.info(40 * "=")
    took = time.time() - start
    nice_took = time.strftime('%H:%M:%S', time.gmtime(took))
    _logger.info(f"Total time: {took:.2f} s [{nice_took}]")
