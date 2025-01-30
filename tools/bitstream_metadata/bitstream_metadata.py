import argparse
import logging
import os
import sys
import json

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from utils import init_logging, update_settings  # noqa
from _utils import read_json  # noqa: E402

logging.getLogger("dspace.client").setLevel(logging.WARNING)
_logger = logging.getLogger()

env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

"""
Check the size of bitstreams in a DSpace backend based on a list of UUIDs for bitstreams.
Log bitstreams with size of 0.
"""
class checker:
    def __init__(self, dsapce_be, data_path: str, cache_create: bool, cache_use: bool, cache_dir: str):
        """
        Initialize the checker class with DSpace backend, data path, and URL keys.
        """
        self._dspace_be = dsapce_be
        self._data = read_json(data_path)
        self._cache_create = cache_create
        self._cache_path = os.path.join(cache_dir, 'metadata.json')
        self._cache = {}
        if cache_use:
            self._cache = read_json(self._cache_path)
        self._info = {
            "invalid": []
        }

    @property
    def info(self):
        return self._info

    def get_bitstream_size(self, uuid: str) -> str:
        """
        Fetch the bitstream size from the DSpace backend using the UUID.
        """
        if self._cache and uuid in self._cache:
            resp = self._cache[uuid]
        else:
            url = f'core/bitstreams/{uuid}'
            resp = dspace_be._fetch(url, dspace_be.get, None)
        if not resp:
            logging.error(f"No response for {url}!")
            return None
        self._cache[uuid] = resp
        key = 'sizeBytes'
        if key not in resp:
            logging.error(f'Response does not contain {key}!. Response: {resp}')
            return None
        return resp[key]

    def check_json(self):
        """
        Check size of bitstream file and log results.
        """
        fetched = 0
        for uuid in self._data:
            # Get bitstream size of file
            size = self.get_bitstream_size(uuid)
            # Compare size
            if size == 0:
                logging.error(f"{uuid}: invalid!")
                self._info["invalid"].append(uuid)
            fetched += 1
            if fetched % 1000 == 0:
                logging.info(f"Successfully fetched: {fetched} bitstreams!")
        # Summarize the results
        if len(self._info['invalid']) > 0:
            logging.info(f"Invalid uuids: {', '.join(self._info['invalid'])}")
        else:
            logging.info("All checked bitstreams are correct!")
        if self._cache_create:
            with open(self._cache_path, 'w') as json_file:
                json.dump(self._cache, json_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Check sizeBytes of bitstreams")
    parser.add_argument("--endpoint", type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user", type=str, default=env["backend"]["user"])
    parser.add_argument("--password", type=str, default=env["backend"]["password"])
    parser.add_argument('--input-dir', type=str,
                        default=os.path.join(_this_dir, "data"),
                        help='Input directory for the JSON file')
    parser.add_argument('--JSON-name', type=str, default='bitstreamUUID.json')
    parser.add_argument("--cache-create", action="store_true", default=False)
    parser.add_argument("--cache-use", action="store_true", default=False)
    parser.add_argument("--cache-dir", type=str,
                        default=os.path.join(_this_dir, "__cache"))
    args = parser.parse_args()
    _logger.info(f"Arguments: {args}")

    # Initialize DSpace backend
    dspace_be = dspace.rest(args.endpoint, args.user, args.password, True)
    bitstream_checker = checker(dspace_be, os.path.join(
        args.input_dir, args.JSON_name), args.cache_create, args.cache_use, args.cache_dir)
    bitstream_checker.check_json()
