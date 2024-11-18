import argparse
import logging
import time
import re
import os
import sys
import json
from collections import defaultdict

# Set up directories for imports
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import Item  # noqa
from utils import init_logging, update_settings  # noqa
logging.getLogger("dspace.client").setLevel(logging.WARNING)

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

from _cache import cache


class stats:

    key_must_exist = "must_exist"
    key_missing = "missing"
    key_valid = "valid"
    key_invalid = "invalid"
    key_lens = "lens"
    key_unique = "unique"
    key_dates = "dates"
    key_format = "format"
    key_other = "other"
    key_bitstreams = "bitstreams"

    def_date_recs = {
        "yyyy": re.compile(r"^\d{4}$"),
        "yyyy-mm": re.compile(r"^\d{4}-\d{2}$"),
        "yyyy-mm-dd": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    }

    def __init__(self, spec: dict):
        self._len = 0
        self._bitstream_len = 0
        self._spec = spec
        self._info = {
            stats.key_must_exist: {
                stats.key_missing: {},
                stats.key_valid: defaultdict(int),
                stats.key_lens: defaultdict(int)
            },
            stats.key_unique: defaultdict(int),
            stats.key_dates: {
                stats.key_lens: defaultdict(int),
                stats.key_format: {
                    stats.key_other: [],
                },
            },
            stats.key_bitstreams: {
                stats.key_invalid: [],
            },
        }
        # make sure date format keys are present
        for key in stats.def_date_recs.keys():
            self._info[stats.key_dates][stats.key_format][key] = 0

    def __len__(self):
        return self._len

    @property
    def must_exist(self):
        return self._info[stats.key_must_exist]

    @property
    def unique(self):
        return self._info[stats.key_unique]

    @property
    def dates(self):
        return self._info[stats.key_dates]

    @property
    def bitstreams(self):
        return self._info[stats.key_bitstreams]

    def update(self, item: dict, bitstreams):
        self._len += 1
        uuid = item['uuid']

        bitstreams = bitstreams or []
        if len(bitstreams) > 0:
            invalid_chars = "()"
            for b in bitstreams:
                self._bitstream_len += 1
                if any([c in b.name for c in invalid_chars]):
                    self.bitstreams[stats.key_invalid].append(uuid)
                    break

        meta = item['metadata']
        for key in self._spec["must_exist"]:
            if key not in meta:
                self.must_exist[stats.key_missing].setdefault(key, []).append(uuid)
                _logger.debug(f"Key {key} not found for item {uuid}")
                continue
            self.must_exist[stats.key_valid][key] += 1
            self.must_exist[stats.key_lens][len(meta[key])] += 1

        for key in self._spec["unique"]:
            for m in meta.get(key, [{'value': None}]):
                self.unique[m['value']] += 1

        for key in self._spec["date"]:
            date_metas = meta.get(key, [])
            self.dates[stats.key_lens][len(date_metas)] += 1
            for m in date_metas:
                found = False
                m_val = m['value']
                for rec_key, rec in stats.def_date_recs.items():
                    if rec.search(m_val):
                        self.dates[stats.key_format][rec_key] += 1
                        found = True
                        break
                if not found:
                    self.dates[stats.key_format]["other"].append(m_val)

    def print_info(self, show_limit: int = 25):
        _logger.info(f"Total items: {self._len} total bitstreams: {self._bitstream_len}")
        _logger.info("Must exist keys [MISSING]:")
        for key, value in self.must_exist[stats.key_missing].items():
            _logger.info(f"  {key}: {len(value)} ... {value[:show_limit]}")
        _logger.info("Must exist keys [VALID]:")
        for key, value in self.must_exist[stats.key_valid].items():
            _logger.info(f"  {key}: {value}")
        _logger.info("Must exist keys [LENS]:")
        for key, value in self.must_exist[stats.key_lens].items():
            _logger.info(f"  {key}: {value}")

        _logger.info("-----")
        _logger.info("Unique keys:")
        ok = 0
        not_ok = 0
        for key, value in self.unique.items():
            if value == 1:
                ok += 1
                continue
            not_ok += 1
            if not_ok >= show_limit:
                # print only once
                if not_ok == show_limit:
                    _logger.info(f"  ... showing only {show_limit} items")
            else:
                _logger.info(f"  {key}: {value}")
        _logger.info(f"Unique keys [OK]: {ok}")

        _logger.info("-----")
        _logger.info("Date keys [FORMATS]:")
        for key, value in self.dates[stats.key_format].items():
            _logger.info(f"  {key}: {value}")
        _logger.info("Date keys [LENS]:")
        for key, value in self.dates[stats.key_lens].items():
            _logger.info(f"  {key}: {value}")

        _logger.info("-----")
        _logger.info(
            f"Invalid bitstream names: [{len(self.bitstreams[stats.key_invalid])}]")
        _logger.info(f"  {self.bitstreams[stats.key_invalid][:show_limit]}...")


class iterator:

    def __init__(self, dspace_be=None, cacher=None):
        self._cacher = cacher
        self._dspace_be = dspace_be
        self._len_all = 0

    @property
    def len_all(self):
        return self._len_all

    def iter_cache(self):
        for item, bitstreams in self._cacher.iter_items():
            self._len_all += 1
            yield item, bitstreams

    def iter_be(self):
        for items in self._dspace_be.iter_items():
            self._len_all += len(items)
            items = [item for item in items if not item['withdrawn'] and item['inArchive']]
            for item in items:
                bitstreams = None
                if args.fetch_bitstreams:
                    try:
                        uuid = item['uuid']
                        dso = Item(item)
                        bundles = dspace_be.client.get_bundles(parent=dso, size=100)
                        bitstreams = []
                        for bundle in bundles:
                            bitstreams.extend(dspace_be.client.get_bitstreams(
                                bundle=bundle, size=100))
                    except Exception as e:
                        _logger.error(
                            f"Error getting bitstreams for item {item['uuid']}: {e}")
                yield item, bitstreams

    def items(self):
        iter = self.iter_be if self._cacher is None \
            else self.iter_cache
        for item, bitstreams in iter():
            yield item, bitstreams


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Add metadata for DSpace items")
    parser.add_argument("--endpoint", type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user", type=str, default=env["backend"]["user"])
    parser.add_argument("--password", type=str, default=env["backend"]["password"])
    parser.add_argument("--spec", type=str, default=os.path.join(_this_dir, "spec.json"))
    parser.add_argument("--fetch-bitstreams", action="store_true", default=False)
    parser.add_argument("--cache-create", action="store_true", default=False)
    parser.add_argument("--cache-use", action="store_true", default=False)
    parser.add_argument("--cache-dir", type=str,
                        default=os.path.join(_this_dir, "__cache"))
    args = parser.parse_args()
    _logger.info(f"Arguments: {args}")

    if not os.path.exists(args.spec):
        _logger.error(f"Spec file does not exist: {args.spec}")
        sys.exit(1)

    start = time.time()
    auth = args.cache_use is False
    dspace_be = dspace.rest(args.endpoint, args.user, args.password, auth)

    with open(args.spec, "r", encoding="utf-8") as f:
        spec = json.load(f)
    statser = stats(spec)
    cacher = cache(dir=args.cache_dir, load=args.cache_use)
    if args.cache_create:
        cacher.add_info("args", vars(args))
        cacher.validate_save()

    iter = iterator(dspace_be, cacher if args.cache_use else None)
    len_processed_items = 0
    for item, bitstreams in iter.items():
        statser.update(item, bitstreams)
        if args.cache_create:
            cacher.add(item, bitstreams)
    cacher.serialize()

    _logger.info(40 * "=")
    statser.print_info()
    _logger.info(40 * "=")
    _logger.info(f"Total items: {iter.len_all}  Processed items: [{len(statser)}]")
    took = time.time() - start
    nice_took = time.strftime("%H:%M:%S", time.gmtime(took))
    _logger.info(f"Total time: {took:.2f} s [{nice_took}]")
