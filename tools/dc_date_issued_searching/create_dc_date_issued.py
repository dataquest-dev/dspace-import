import argparse
import logging
import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import Item
from utils import init_logging, update_settings  # noqa

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Add metadata for DSpace items")
    parser.add_argument("--to_mtd_field",
                        type=str, required=True, help="Metadata field that we want created.")
    parser.add_argument(
        "--from_mtd_field",
        type=str,
        nargs='+',  # Accept one or more values
        required=True,
        help="Metadata field(s) than value(s) can be used."
    )

    args = parser.parse_args()
    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    all_items = dspace_be.fetch_items()
    items = []
    # Check which items do not contain dc.date.issued
    for item in all_items:
        # Check if item is withdrawn or is not in archive
        if item['withdrawn'] or not item['inArchive']:
            continue
        mtd = item['metadata']
        if args.to_mtd_field not in mtd:
            items.append(item)

    # Create missing mtd
    from_mtd_field = args.from_mtd_field
    created = []
    no_created = []
    error_items = []
    for item in items:
        mtd = item["metadata"]
        found = False
        for from_mtd in from_mtd_field:
            if from_mtd in mtd:
                if len(mtd[from_mtd]) == 0:
                    _logger.info(
                        f"No values for metadata [{from_mtd}] of item [{item['uuid']}]")
                    break
                found = True
                _logger.info(
                    f"Metadata [{args.to_mtd_field}] replaced by [{from_mtd}] for item [{item['uuid']}]")
                val = mtd[from_mtd][0]["value"]
                r = dspace_be.client.add_metadata(Item(item), args.to_mtd_field, val)
                if r is not None:
                    created.append(item["uuid"])
                    break
                else:
                    logging.warning(
                        f"Error during creating metadata [{args.to_mtd_field}] for item [{item['uuid']}]")
                    error_items.append(item["uuid"])
        if not found:
            no_created.append(item["id"])
    _logger.info(f"Metadata [{args.to_mtd_field}] added to items: {created}")
    _logger.warning(f"Metadata [{args.to_mtd_field}] do not added to items: {no_created}")
    _logger.warning(
        f"Error during added metadata [{args.to_mtd_field}] to items: {error_items}")
