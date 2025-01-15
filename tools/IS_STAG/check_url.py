import argparse
import logging
import os
import sys
import re

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


class checker:
    def __init__(self, dsapce_be, data_path: str, new_key: str, cur_key: str):
        """
        Initialize the checker class with DSpace backend, data path, and URL keys.
        """
        self._dspace_be = dsapce_be
        self._data = read_json(data_path)
        self._new_key = new_key
        self._cur_key = cur_key
        self._info = {
            "invalid": []
        }

    @property
    def info(self):
        return self._info

    def extract_uuid(self, url: str) -> str:
        """
        Extract UUID from the URL using a regex pattern.
        """
        pattern = r'bitstreams/([a-f0-9\-]+)'
        match = re.search(pattern, url)
        if match:
            return match.group(1)
        logging.error(f"Url {url} doesn't contains pattern {pattern}!")
        return None

    def extract_name(self, url: str) -> str:
        """
        Extract the file name from the URL.
        """
        pattern = r'([^/]+)$'
        match = re.search(pattern, url)
        if match:
            return match.group(1)
        logging.error(f"Url {url} doesn't contains pattern {pattern}!")
        return None

    def get_bitstream_name(self, uuid):
        """
        Fetch the bitstream name from the DSpace backend using the UUID.
        """
        url = f'core/bitstreams/{uuid}'
        resp = dspace_be._fetch(url, dspace_be.get, None)
        if not resp:
            logging.error("None response!")
            return None
        key = 'name'
        if key not in resp:
            logging.error(f'Response does not contain {key}!. Response: {resp}')
            return None
        return resp[key]

    def compare_str(self, name_exp: str, name_got: str) -> bool:
        """
        Compare the expected file name with the actual one.
        """
        if name_exp == name_got:
            return True
        else:
            logging.error(
                f'Expected name {name_exp} is not match with got name {name_got}!')
            return False

    def check_json(self):
        """
        Check URLs in the JSON data, compare names and log results.
        """
        for prace_id, items in self._data.items():
            for item in items:
                # Extract bitstream UUID from new URL
                uuid = check.extract_uuid(item[self._new_key])
                if not uuid:
                    continue
                # Get bitstream name from DSpace
                new_name = check.get_bitstream_name(uuid)
                # Extract the current file name from the URL
                name = check.extract_name(item[self._cur_key])
                # Compare the extracted names
                if not check.compare_str(name, new_name):
                    logging.error(f"{prace_id}: incorrect!")
                    self._info["invalid"].append(prace_id)
                else:
                    logging.info(f"{prace_id}: OK!")
        # Summarize the results
        if len(self._info['invalid']) > 0:
            logging.info(f"Invalid prace_ids: {', '.join(self._info['invalid'])}")
        else:
            logging.info("All checked urls are correct!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Check url")
    parser.add_argument("--endpoint", type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user", type=str, default=env["backend"]["user"])
    parser.add_argument("--password", type=str, default=env["backend"]["password"])
    parser.add_argument('--input_dir', type=str,
                        default=os.path.join(_this_dir, "data"),
                        help='Input directory for the JSON file')
    parser.add_argument('--JSON_name', type=str,
                        help='Name of the JSON file')
    parser.add_argument('--new_key', type=str,
                        default="new_url",
                        help='New data key')
    parser.add_argument('--curr_key', type=str,
                        default="cur_url",
                        help='Current data key')
    args = parser.parse_args()
    _logger.info(f"Arguments: {args}")

    # Initialize DSpace backend
    dspace_be = dspace.rest(args.endpoint, args.user, args.password, True)
    check = checker(dspace_be, os.path.join(
        args.input_dir, args.JSON_name), args.new_key, args.curr_key)
    check.check_json()
