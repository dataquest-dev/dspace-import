###
# This script retrieves all licenses, labels, and mappings from DSpace that meet the defined conditions and returns them in JSON format.
###

import argparse
import logging
import os
import json
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
path_to_dspace_lib = os.path.join(_this_dir, "../../libs/dspace-rest-python")
sys.path.insert(0, os.path.join(_this_dir, "../../src"))

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import License  # noqa
from utils import init_logging, update_settings  # noqa

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])


def fetch_licenses(dspace_be):
    """Fetch licenses from DSpace backend."""
    all_licenses = dspace_be.fetch_licenses()
    _logger.info(f"Number of fetched licenses: {len(all_licenses)}")
    return all_licenses


def filter_licenses(all_licenses, no_definition):
    """Filter licenses based on the no_definition criteria."""
    key = "definition"
    no_definition_set = set(no_definition)
    return [
        License(license)
        for license in all_licenses
        if key in license and not any(arg in license[key] for arg in no_definition_set)
    ]

def write_data_to_file(data, output_path):
    """Write the filtered data to a JSON file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Ensure output directory exists
    with open(output_path, 'w', encoding='utf-8') as fout:
        json.dump(data, fout, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Get DSpace licenses that meet condition.")
    parser.add_argument("--no_definition", type=str, nargs='+', required=True,
                        help="String that cannot be part of the license definition")
    parser.add_argument('--output', type=str,
                        default=os.path.join(_this_dir, "data"),
                        help='Output directory for the JSON file')
    args = parser.parse_args()

    # Initialize DSpace backend
    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    # Fetch and filter licenses
    all_licenses = fetch_licenses(dspace_be)
    filtered_licenses = filter_licenses(all_licenses, args.no_definition)
    # Collect unique license labels and extended license mappings
    added_ids = set()
    filtered_license_labels = []

    for license in filtered_licenses:
        # Function to add labels if they're unique
        def add_unique_label(label):
            if label and label.id not in added_ids:
                added_ids.add(label.id)
                filtered_license_labels.append(label)

        # Add the primary license label
        add_unique_label(license.licenseLabel)

        # Add extended license labels
        for ext in license.extendedLicenseLabel or []:
            add_unique_label(ext)

    # Create extended license mappings
    filtered_ext_mapping = [
        {'license_id': license.id, 'label_id': ext.id}
        for license in filtered_licenses
        for ext in license.extendedLicenseLabel or []
    ]

    _logger.info(f"Filtered licenses: {filtered_licenses}")
    _logger.info(f"Filtered license labels: {filtered_license_labels}")
    _logger.info(f"Filtered license extended mapping: {filtered_ext_mapping}")

    _logger.info(f"Number of filtered licenses: {len(filtered_licenses)}")
    _logger.info(f"Number of filtered license labels: {len(filtered_license_labels)}")
    _logger.info(f"Number of filtered license extended mapping: {len(filtered_ext_mapping)}")

    # Write the filtered data to the specified output file
    write_data_to_file([license.to_dict() for license in filtered_licenses], os.path.join(args.output, 'licenses.json'))
    write_data_to_file([license.to_dict() for license in filtered_license_labels], os.path.join(args.output, 'labels.json'))
    write_data_to_file(filtered_ext_mapping, os.path.join(args.output, 'mapping.json'))
