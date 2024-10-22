###
# This script import license, labels and mappings.
###
import argparse
import logging
import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
path_to_dspace_lib = os.path.join(_this_dir, "../../libs/dspace-rest-python")
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

import dspace  # noqa
import pump  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import License  # noqa
from utils import init_logging, update_settings  # noqa

from _license import licenses

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Import licenses to DSpace.")
    parser.add_argument('--input', type=str,
                        default=os.path.join(_this_dir, "data"),
                        help='Input directory for the JSON file')
    args = parser.parse_args()

    # Initialize DSpace backend
    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    _logger.info("Loading license import")
    licenses_imp = licenses(os.path.join(args.input, 'labels.json'), os.path.join(args.input, 'licenses.json'), os.path.join(args.input, 'mapping.json'))

    # import licenses
    _logger.info("Start license import")
    licenses_imp.import_to(env, dspace_be)
    _logger.info("End license import")