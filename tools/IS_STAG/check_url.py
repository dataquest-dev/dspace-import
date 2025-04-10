import argparse
import difflib
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
    def __init__(self, dspace_be, data_path: str, new_key: str, cur_key: str):
        """
        Initialize the checker class with DSpace backend, data path, and URL keys.
        """
        self._dspace_be = dspace_be
        self._data = read_json(data_path)
        self._new_key = new_key
        self._cur_key = cur_key
        self._info = {
            "invalid": []
        }
        # regex patterns
        self._uuid_pattern = (
            re.compile(r'bitstreams/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})(/download)?$'))
        self._name_pattern = re.compile(r'/([^/]+?)(?:\?.*)?$')

    @property
    def info(self):
        return self._info

    def extract_uuid(self, url: str) -> str:
        """
        Extract UUID from the URL using a regex pattern.
        """
        match = self._uuid_pattern.search(url)
        if match:
            # refers to the first capturing group from the regex pattern
            return match.group(1)
        logging.error(f"Could not extract UUID from URL: {url}")
        return None

    def extract_name(self, url: str) -> str:
        """
        Extract the file name from the URL.
        """
        match = self._name_pattern.search(url)
        if match:
            return match.group(1)
        logging.error(f"Could not extract file name from URL: {url}")
        return None

    def get_bitstream_name(self, uuid: str) -> str:
        """
        Fetch the bitstream name from the DSpace backend using the UUID.
        """
        url = f'core/bitstreams/{uuid}'
        resp = self._dspace_be._fetch(url, self._dspace_be.get, None)
        if not resp:
            logging.error("None response!")
            return None
        key = 'name'
        if key not in resp:
            logging.error(f'Response does not contain {key}!. Response: {resp}')
            return None
        return resp[key]

    def are_names_matching(self, name_exp: str, name_got: str) -> bool:
        """
        Compare the expected file name with the actual one.
        """
        if name_exp == name_got:
            return True
        else:
            # Show the difference to help debug issues with special characters
            diff = difflib.ndiff(name_exp, name_got)
            differences = ''.join(diff)
            logging.error(
                f'Names do not match:\nExpected: {name_exp}\nActual: {name_got}\nDifference: {differences}')
            return False

    def validate_urls(self):
        """
        Check URLs in the JSON data, compare names and log results.
        """
        for prace_id, items in self._data.items():
            for item in items:
                # Check if required keys exist
                if self._new_key not in item or self._cur_key not in item:
                    logging.error(f"{prace_id}: Missing required keys in item.")
                    self._info["invalid"].append(prace_id)
                    continue
                # Extract bitstream UUID from new URL
                uuid = self.extract_uuid(item[self._new_key])
                if not uuid:
                    self._info["invalid"].append(prace_id)
                    continue
                # Get bitstream name from DSpace
                new_name = self.get_bitstream_name(uuid)
                if not new_name:
                    self._info["invalid"].append(prace_id)
                    continue
                # Extract the current file name from the URL
                name = self.extract_name(item[self._cur_key])
                if not name:
                    self._info["invalid"].append(prace_id)
                    continue
                # Compare the extracted names
                if not self.are_names_matching(name, new_name):
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
    # Log arguments except password
    safe_args = vars(args).copy()
    if 'password' in safe_args:
        safe_args['password'] = '********'
    _logger.info(f"Arguments: {safe_args}")

    # Initialize DSpace backend
    dspace_be = dspace.rest(args.endpoint, args.user, args.password, True)
    url_checker = checker(dspace_be, os.path.join(
        args.input_dir, args.JSON_name), args.new_key, args.curr_key)
    url_checker.validate_urls()
