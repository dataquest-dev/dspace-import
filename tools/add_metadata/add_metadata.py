import argparse
import logging
import time
import os
import sys
from datetime import datetime
from collections import defaultdict

# Set up directories for imports
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

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


class date:
    invalid = defaultdict(int)
    invalid_but_converted = defaultdict(int)

    def __init__(self, d: str):
        self._input = d
        self._d = d

    @property
    def input(self) -> str:
        return self._input

    @property
    def value(self) -> str:
        return self._d

    def is_valid(self):
        """Check if the given string is a valid date."""
        try:
            datetime.strptime(self._d, '%Y-%m-%d')
            return True
        except ValueError as e:
            date.invalid[self._d] += 1
            if date.invalid[self._d] == 1:
                _logger.warning(f"[{self._d}] is not valid date. Error: {e}")
            return False

    def parse(self) -> bool:
        """Convert the value to a date format. Normalize date to 'YYYY-MM-DD' format, filling missing parts with '01'."""
        if len(self._d) < 1:
            return False

        formats = ['%Y/%m/%d', '%d/%m/%Y', '%Y.%m.%d', '%d.%m.%Y', '%Y',
                   '%Y-%m', '%m-%Y', '%Y/%m', '%m/%Y', '%Y.%m', '%m.%Y', '%d. %m. %Y']
        for fmt in formats:
            try:
                datetime_obj = datetime.strptime(self._d, fmt)
                # Normalize date to 'YYYY-MM-DD'
                if fmt in ['%Y-%m', '%Y/%m', '%Y.%m', '%m-%Y', "%m/%Y", "%m.%Y"]:
                    self._d = datetime_obj.strftime('%Y-%m-01')
                elif fmt == '%Y':
                    self._d = datetime_obj.strftime('%Y-01-01')
                else:
                    self._d = datetime_obj.strftime('%Y-%m-%d')
                return True
            except ValueError:
                # The test format does not match the input date format
                continue
        _logger.warning(f"Error converting [{self._d}] to date.")
        return False


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

    def __init__(self, dspace_be, from_mtd_fields: list, to_mtd_field: list, dry_run: bool = False):
        self._dspace_be = dspace_be
        self._from_mtd_fields = from_mtd_fields
        self._to_mtd_field = to_mtd_field
        self._dry_run = dry_run
        self._info = {
            "valid": [],
            "multiple": [],
            "invalid_date": [],
            "invalid_date_all": set(),
            "updated": [],
            "error_updating": [],
            "error_creating": [],
            "created": [],
            "not_created": [],
        }

    @property
    def cannot_parse(self):
        return self._info["invalid_date_all"]

    @property
    def info(self):
        return self._info

    def update_existing_metadata(self, item: dict, date_str: str):
        uuid = item['uuid']
        item_mtd = item["metadata"]

        id_str = f"Item [{uuid}]: [{self._to_mtd_field}]"
        # If there is more than one value, get only the first one
        date_val = date(date_str)
        if date_val.is_valid():
            self._info["valid"].append((uuid, date_val.input))
            return True

        parsed_ok = date_val.parse()
        if parsed_ok is False:
            _logger.error(f"{id_str}: cannot convert [{date_val.input}] to date")
            self._info["invalid_date"].append((uuid, date_val.input))
            return False

        # Convert date to correct format if necessary
        date.invalid_but_converted[date_val.input] += 1
        if date.invalid_but_converted[date_val.input] == 1:
            _logger.info(f"{id_str}: invalid date [{date_val.input}] converted")

        # Update the item metadata with the converted date
        item_mtd[self._to_mtd_field][0]["value"] = date_val.value
        item["metadata"] = item_mtd

        # Update the item in the database
        updated_ok = self._dry_run or update_item(item)
        if not updated_ok:
            _logger.error(f"{id_str}: error updating item")
            self._info["error_updating"].append((uuid, date_val.input))
            return False

        self._info["updated"].append((uuid, date_val.input))
        return True

    def add_new_metadata(self, item) -> bool:
        uuid = item['uuid']
        item_mtd = item["metadata"]

        for from_mtd in self._from_mtd_fields:
            date_meta = item_mtd.get(from_mtd, None)
            if date_meta is None:
                continue
            id_str = f"Item [{uuid}]: [{from_mtd}]"
            if len(date_meta) != 1:
                _logger.warning(f"{id_str}: more than one value {date_meta}")

            # If there is more than one value, get only the first one
            date_val = date(date_meta[0]["value"])
            # Convert date if necessary
            if not date_val.is_valid():
                if not date_val.parse():
                    self._info["invalid_date_all"].add(date_val.input)
                    continue

            _logger.debug(f"{id_str}: created...")

            # Update the item in the database

            added = (self._dry_run or
                     self._dspace_be.client.add_metadata(Item(item), self._to_mtd_field, date_val.value))

            if not added:
                _logger.warning(f"{id_str}: Error creating metadata")
                self._info["error_creating"].append((uuid, date_val.input))
                return False

            self._info["created"].append((uuid, date_val.input))
            return True

        self._info["not_created"].append((uuid, None))
        return False

    def update(self, item: dict) -> bool:
        """Create missing metadata for items based on provided fields."""
        item_mtd = item["metadata"]
        uuid = item['uuid']

        # Check if the target metadata field exists and is not empty
        date_meta = item_mtd.get(self._to_mtd_field, None)
        if date_meta is not None:
            val = date_meta[0]["value"]
            # Check if items have multiple values for to_mtd_value
            if len(date_meta) > 1:
                _logger.warning(
                    f'Item [{uuid}] has multiple values for {self._to_mtd_field}!')
                self._info["multiple"].append(uuid)
                if not self._dry_run:
                    val = ''
                    for i in range(len(date_meta)):
                        if len(val) == 0:
                            date_val = date(date_meta[i]["value"])
                            if date_val.is_valid() or date_val.parse():
                                val = date_val.value
                                continue
                        if val == '' and i == len(date_meta) - 1:
                            val = date_meta[i]["value"]
                            continue
                        dspace_be.client.remove_metadata(
                            Item(item), self._to_mtd_field, i)

                    # Reload item and metadata
                    item = dspace_be._fetch(f'core/items/{uuid}', dspace_be.get, None)
            return self.update_existing_metadata(item, val)
        else:
            return self.add_new_metadata(item)


class additional_stats:

    def __init__(self):
        self._titles = defaultdict(int)
        self._doubles = defaultdict(list)

    def update(self, item: dict):
        uuid = item['uuid']
        dc_titles = item['metadata'].get('dc.title', [])
        if len(dc_titles) > 0:
            self._titles[dc_titles[0]['value']] += 1
        key = 'dc.date.issued'
        if len(item['metadata'].get(key, [])) > 1:
            self._doubles[key].append(uuid)

    def print_info(self, show_limit=100):
        duplicates = {k: v for k, v in self._titles.items() if v > 1}
        _logger.info(
            f"Duplicates {len(duplicates)} ({sum(duplicates.values())})  (showing first {show_limit}):")
        for i, (k, v) in enumerate(duplicates.items()):
            if i >= show_limit:
                break
            _logger.info(f"Title [{k}] : {v}")
        if len(self._doubles) > 0:
            _logger.info("Multiple values when expecting at most 1:")
            for k, v in self._doubles.items():
                _logger.info(f"{k}: {v}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Add metadata for DSpace items")
    parser.add_argument("--to_mtd_field",
                        type=str, required=True, help="Metadata field to be created.")
    parser.add_argument("--from_mtd_field",
                        type=str, nargs='+', required=True,
                        help="Metadata field(s) from which value(s) can be used.")
    parser.add_argument("--endpoint", type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user", type=str, default=env["backend"]["user"])
    parser.add_argument("--password", type=str, default=env["backend"]["password"])
    parser.add_argument("--dry-run", action='store_true', default=False)
    args = parser.parse_args()
    _logger.info(f"Arguments: {args}")

    start = time.time()

    # Initialize DSpace backend
    dspace_be = dspace.rest(args.endpoint, args.user, args.password, True)

    upd = updater(dspace_be, args.from_mtd_field, args.to_mtd_field, dry_run=args.dry_run)

    stats = additional_stats()

    # Process items
    len_all_items = 0
    len_used_items = 0
    for items in dspace_be.iter_items():
        len_all_items += len(items)
        items = [item for item in items if not item['withdrawn'] and item['inArchive']]
        len_used_items += len(items)
        for item in items:
            stats.update(item)
            upd.update(item)

    _logger.info(40 * "=")
    _logger.info("Item info:")
    limit = 50
    for k, v in upd.info.items():
        _logger.info(f"{k:20s}:{len(v):6d}: first {limit} items .. {list(v)[:limit]}...")

    _logger.info(40 * "=")
    _logger.info("Date info")
    msgs = "\n\t".join(upd.cannot_parse)
    _logger.info(f"Cannot parse [{len(msgs)}]:\n\t{msgs}")
    inv_arr = [(v, f"[{k:15s}]: {v:4d}") for k, v in date.invalid.items()]
    inv_arr.sort(key=lambda x: x[0], reverse=True)
    msgs = "\n\t".join([x[1] for x in inv_arr])
    _logger.info(f"Date invalid [{len(msgs)}]:\n\t{msgs}")

    _logger.info(40 * "=")
    stats.print_info()

    _logger.info(40 * "=")
    _logger.info("Update statistics:")
    for k, v in upd.info.items():
        _logger.info(f"{k:25s}: {len(v):6d}")
    took = time.time() - start

    _logger.info(40 * "=")
    _logger.info("Counts:")
    _logger.info(f"Total items: {len_all_items}")
    _logger.info(f"Used items: {len_used_items}")
    # sets are not counted
    _logger.info(
        f"Sum of updates: {sum(len(x) for x in upd.info.values() if isinstance(x, list))}")

    _logger.info(40 * "=")
    _logger.info(
        f"Total time: {took:.2f} s [{time.strftime('%H:%M:%S', time.gmtime(took))}]")
