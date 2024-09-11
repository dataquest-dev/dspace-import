###
# This script creates a submitter group in a collection and adds a specific subgroup to it.
###
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
    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    GROUP_UUID = "CHANGE ME"

    # How many collections were updated
    counter = 0
    submitter_failed = 0

    current_page = 17
    for page in range(0, 1000):
        # Get all collections
        subcolls = dspace_be.client.get_collections(page=current_page, size=50)
        for coll in subcolls:
            counter += 1
            _logger.info(f"************ - {counter} - ************")
            _logger.info(f"Collection: {coll.name} with UUID: {coll.uuid}")

            # Create a submitter group in the collection
            dspace_be.client.api_post(url=f'{env["backend"]["endpoint"]}core/collections/{coll.uuid}/submittersGroup', params={}, data={})
            # Fetch the UUID of the new submitter group
            response = dspace_be.client.api_get(url=f'{env["backend"]["endpoint"]}core/collections/{coll.uuid}/submittersGroup')
            parsed_response = response.json()
            submitter_group_uuid = parsed_response['uuid']

            _logger.info(f'Created submitter_group_uuid: {submitter_group_uuid}')

            # Add a subgroup to the new submitter group
            response_submitter = dspace_be.client.api_post(
                url=f'{env["backend"]["endpoint"]}eperson/groups/{submitter_group_uuid}/subgroups', params={},
                data=f'{env["backend"]["endpoint"]}eperson/groups/{GROUP_UUID}',
                content_type='text/uri-list')
            # Check if the subgroup was added to the submitter group
            if response_submitter.status_code != 204:
                submitter_failed += 1
                _logger.error(f'Failed adding a subgroup {GROUP_UUID} to the submitter group {submitter_group_uuid}')
        current_page += 1

    _logger.info(f"Total collections updated: {counter}")
    _logger.info(f"Total failed adding subgroups into submitter group: {submitter_failed}")
