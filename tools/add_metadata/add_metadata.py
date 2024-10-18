import argparse
import logging
import os
import sys
from datetime import datetime

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
    _logger.info(f"Number of fetched items: {len(all_items)}")
    return [
        item for item in all_items
        if not item['withdrawn'] and item['inArchive']
    ]


def is_valid_date(date: str):
    """Check if the given string is a valid date."""
    try:
        datetime.strptime(date, '%Y-%m-%d')
        return True
    except ValueError as e:
        _logger.warning(f"[{date}] is not valid date. Error: {e}")
        return False


def convert_to_date(value: str):
    """Convert the value to a date format. Normalize date to 'YYYY-MM-DD' format, filling missing parts with '01'."""
    formats = ['%Y/%m/%d', '%d/%m/%Y', '%Y.%m.%d', '%d.%m.%Y', '%Y',
               '%Y-%m', '%m-%Y', '%Y/%m', '%m/%Y', '%Y.%m', '%m.%Y']
    found = False
    for fmt in formats:
        try:
            datetime_obj = datetime.strptime(value, fmt)
            # Normalize date to 'YYYY-MM-DD'
            if fmt in ['%Y-%m', '%Y/%m', '%Y.%m']:
                return datetime_obj.strftime('%Y-%m-01')
            elif fmt == '%Y':
                return datetime_obj.strftime('%Y-01-01')
            return datetime_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    _logger.warning(f"Error converting [{value}] to date.")
    return None


def update_item(item: Item):
    if dspace_be.client.update_item(item):
        return item
    # Try to authenticate
    _logger.info("Reauthorization during item updating")
    if dspace_be.client.authenticate(retry=True):
        return dspace_be.client.update_item(item)
    return None


def process_metadata(dspace_be, items, from_mtd_fields, to_mtd_field):
    """Create missing metadata for items based on provided fields."""
    created, updated, not_created, error_items, ok_items = [], [], [], [], []

    for item in items:
        uuid = item['uuid']
        item_mtd = item["metadata"]

        if to_mtd_field in item_mtd and item_mtd[to_mtd_field]:
            val = item_mtd[to_mtd_field][0]["value"]
            if is_valid_date(val):
                ok_items.append(uuid)
                continue
            _logger.info(f"Item [{uuid}] has an invalid date in [{to_mtd_field}]: {val}")
            new_mtd = convert_to_date(val)
            if new_mtd is None:
                _logger.error(f"Cannot convert [{to_mtd_field}] "
                              f"to valid date for item [{uuid}]: {val}")
                error_items.append(uuid)
                continue
            item_mtd[to_mtd_field][0]["value"] = new_mtd
            item["metadata"] = item_mtd
            if update_item(Item(item)):
                updated.append(uuid)
            else:
                _logger.error(
                    f"Error updating [{to_mtd_field}] for item [{uuid}]")
                error_items.append(uuid)
        else:
            found = False
            for from_mtd in from_mtd_fields:
                if from_mtd in item_mtd and item_mtd[from_mtd]:
                    val = item_mtd[from_mtd][0]["value"]
                    if not is_valid_date(val):
                        val = convert_to_date(val)
                        if val is None:
                            _logger.warning(f"Cannot convert [{from_mtd}] "
                                            f"to valid date for item [{uuid}]: {val}")
                            continue
                    found = True
                    _logger.info(
                        f"Metadata [{to_mtd_field}] created from [{from_mtd}] for item [{uuid}]")
                    if dspace_be.client.add_metadata(Item(item), to_mtd_field, val):
                        created.append(uuid)
                    else:
                        _logger.warning(
                            f"Error creating metadata [{to_mtd_field}] for item [{uuid}]")
                        error_items.append(uuid)
                    break

            if not found:
                not_created.append(uuid)

    return created, updated, not_created, error_items, ok_items


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

    # Process items
    created, updated, not_created, error_items, ok_items = process_metadata(
        dspace_be, items_to_update, args.from_mtd_field, args.to_mtd_field
    )

    # Log results
    _logger.info(f"Items with correct [{args.to_mtd_field}]: {ok_items}")
    _logger.info(f"Items with created [{args.to_mtd_field}]: {created}")
    _logger.warning(f"Items where [{args.to_mtd_field}] was not created: {not_created}")
    _logger.warning(f"Items with errors during processing: {error_items}")

    _logger.info(f"Number of items to update: {len(items_to_update)}")
    _logger.info(f"Number of items with correct [{args.to_mtd_field}]: {len(ok_items)}")
    _logger.info(f"Number of items with updated [{args.to_mtd_field}]: {len(updated)}")
    _logger.info(f"Number of items with created [{args.to_mtd_field}]: {len(created)}")
    _logger.info(
        f"Number of items where [{args.to_mtd_field}] was not created: {len(not_created)}")
    _logger.info(f"Number of items with errors during processing: {len(error_items)}")
