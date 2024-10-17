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


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Add metadata for DSpace items")
    parser.add_argument("--to_mtd_field",
                        type=str, required=True, help="Metadata field to be created.")
    parser.add_argument("--from_mtd_field",
                        type=str, nargs='+', required=True,
                        help="Metadata field(s) from which value(s) can be used.")
    return parser.parse_args()


def fetch_items(dspace_be):
    """Fetch items from DSpace backend, filtering out withdrawn or non-archived items."""
    all_items = dspace_be.fetch_items()
    return [
        item for item in all_items
        if not item['withdrawn'] and item['inArchive'] and args.to_mtd_field not in item['metadata']
    ]


def create_missing_metadata(dspace_be, items, from_mtd_fields, to_mtd_field):
    """Create missing metadata for items based on provided fields."""
    created, not_created, error_items = [], [], []

    for item in items:
        mtd = item["metadata"]
        found = False

        for from_mtd in from_mtd_fields:
            if from_mtd in mtd and mtd[from_mtd]:
                found = True
                val = mtd[from_mtd][0]["value"]
                _logger.info(
                    f"Metadata [{to_mtd_field}] replaced by [{from_mtd}] for item [{item['uuid']}]")

                # Add the new metadata
                if dspace_be.client.add_metadata(Item(item), to_mtd_field, val):
                    created.append(item["uuid"])
                else:
                    _logger.warning(
                        f"Error creating metadata [{to_mtd_field}] for item [{item['uuid']}]")
                    error_items.append(item["uuid"])

                break  # Stop searching once we find a valid field

        if not found:
            not_created.append(item["id"])

    return created, not_created, error_items


if __name__ == '__main__':
    args = parse_arguments()

    # Initialize DSpace backend
    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    # Fetch and filter items
    items_to_update = fetch_items(dspace_be)

    # Create missing metadata
    created, not_created, error_items = create_missing_metadata(
        dspace_be, items_to_update, args.from_mtd_field, args.to_mtd_field
    )

    # Log results
    _logger.info(f"Metadata [{args.to_mtd_field}] added to items: {created}")
    _logger.warning(f"Metadata [{args.to_mtd_field}] not added to items: {not_created}")
    _logger.warning(
        f"Error adding metadata [{args.to_mtd_field}] to items: {error_items}")
