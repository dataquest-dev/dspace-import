###
# This script creates a submitter group in a collection and adds a specific subgroup to it.
###
import argparse
import logging
import os
import sys

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../src"))


import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace.impl.models import Item  # noqa
from dspace.impl.models import Community  # noqa
from utils import init_logging, update_settings  # noqa

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Update resource policies for DSpace items.")
    parser.add_argument("--group",
                        type=str, required=True, help="Group UUID to assign new group to.")
    args = parser.parse_args()

    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    # How many collections were updated
    cnt = {
        'len': 0,
        'submitter_failed': 0
    }

    start_page = 17
    for page in range(start_page, 1000):
        # Get all collections
        subcolls = dspace_be.client.get_collections(page=page, size=50)
        for coll in subcolls:
            cnt['len'] += 1
            _logger.info(f"************ - {cnt['len']} - ************")
            _logger.info(f"Collection: {coll.name} with UUID: {coll.uuid}")

            # Create a submitter group in the collection
            dspace_be.client.api_post(
                url=f'{env["backend"]["endpoint"]}core/collections/{coll.uuid}/submittersGroup', params={}, data={})
            # Fetch the UUID of the new submitter group
            response = dspace_be.client.api_get(
                url=f'{env["backend"]["endpoint"]}core/collections/{coll.uuid}/submittersGroup')
            parsed_response = response.json()
            submitter_group_uuid = parsed_response['uuid']

            _logger.info(f'Created submitter_group_uuid: {submitter_group_uuid}')

            # Add a subgroup to the new submitter group
            response_submitter = dspace_be.client.api_post(
                url=f'{env["backend"]["endpoint"]}eperson/groups/{submitter_group_uuid}/subgroups', params={},
                data=f'{env["backend"]["endpoint"]}eperson/groups/{args.group}',
                content_type='text/uri-list')
            # Check if the subgroup was added to the submitter group
            if response_submitter.status_code != 204:
                cnt['submitter_failed'] += 1
                _logger.error(
                    f'Failed adding a subgroup {args.group} to the submitter group {submitter_group_uuid}')

    _logger.info(f'Processing completed. Summary: {cnt}')
