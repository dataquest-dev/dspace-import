###
# This script creates a submitter group in a collection and adds a specific subgroup to it.
###
import argparse
import logging
import os
import sys
import time

_this_dir = os.path.dirname(os.path.abspath(__file__))
path_to_dspace_lib = os.path.join(_this_dir, "../../libs/dspace-rest-python")
sys.path.insert(0, path_to_dspace_lib)
sys.path.insert(0, os.path.join(_this_dir, "../../src"))

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import Item, Community  # noqa
from utils import init_logging, update_settings  # noqa

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

# Constants
PAGE_SIZE = 50
MAX_PAGES = 1000


def create_submitter_group_if_missing(dspace_be, endpoint, coll, group_uuid, stats):
    uuid = coll.uuid
    name = coll.name
    without_submitter = False
    submitter_without_group = False

    url = f'{endpoint}/core/collections/{uuid}/submittersGroup'
    _logger.info(f"URL: {url}")
    response = dspace_be.client.api_get(url=url)

    fetched_submitter_group_uuid = None
    # Check if the collection has a submitter group
    if response.status_code == 200:
        # 200 = OK, meaning submitter group exists
        response_json = response.json()
        fetched_submitter_group_uuid = response_json.get('uuid')
        if 'uuid' is not None:
            # Check if the submitter group has subgroups - there should be one which is passed as argument
            url = f'{endpoint}/eperson/groups/{fetched_submitter_group_uuid}/subgroups'

            # Get subgroups of the submitter group
            response = dspace_be.client.api_get(url=url)
            if response.status_code == 200:
                response_json = response.json()
                subgroups = response_json.get('_embedded', {}).get('subgroups', [])
                if not subgroups:
                    _logger.info(f"No subgroups found for collection: {name} (UUID: {uuid})")
                    stats['submitter_without_group'] += 1
                    submitter_without_group = True
                else:
                    # Check if the group passed as argument is already a subgroup of the submitter group
                    if not any(subgroup['uuid'] == group_uuid for subgroup in subgroups):
                        stats['submitter_without_group'] += 1
                        submitter_without_group = True
                    else:
                        stats['col_with_submitter'] += 1
            else:
                _logger.error(f"Error fetching subgroups for collection: {name} (UUID: {uuid})")
                _logger.error('Response: %s', response)
                stats['error'] += 1
                return
        else:
            _logger.info(f"No submitter group for collection: {name} (UUID: {uuid})")
            _logger.info('Response: %s', response)
            stats['col_without_submitter'] += 1
            without_submitter = True
    elif response.status_code == 204:
        # 204 = No Content, meaning no submitter group exists
        stats['col_without_submitter'] += 1
        _logger.info(f'URL {url}')
        _logger.info('Response: %s', response)
        without_submitter = True
    else:
        stats['error'] += 1
        _logger.error(f"Error fetching submitter group for {name}: {response}")
        return

    # If the collection does not have a submitter group or the submitter group does not have the specified subgroup,
    # create it
    if without_submitter is False and submitter_without_group is False:
        return  # Create submitter group if it does not exist

    submitter_uuid = fetched_submitter_group_uuid
    # If the collection does not have a submitter group, create it
    if submitter_without_group is False:
        _logger.info(f"Creating submitter group for collection: {name} (UUID: {uuid})")
        dspace_be.client.api_post(url=url, params={}, json={})

        # Get new submitter group UUID
        response = dspace_be.client.api_get(url=url)
        submitter_uuid = response.json().get("uuid")
        if not submitter_uuid:
            _logger.error(f"Failed to fetch new submitter group for {name}")
            stats['submitter_failed'] += 1
            return

        _logger.info(f"Created submitter group UUID: {submitter_uuid}")

    # Add subgroup
    subgroup_url = f'{endpoint}/eperson/groups/{submitter_uuid}/subgroups'
    subgroup_data = f'{endpoint}/eperson/groups/{group_uuid}'

    _logger.info(f"URL: {subgroup_url}")
    _logger.info(f"Data: {subgroup_data}")
    response = dspace_be.client.api_post_uri(url=subgroup_url, params={}, uri_list=subgroup_data)

    if response.status_code != 204:
        _logger.error(f"Failed to add subgroup {group_uuid} to submitter group {submitter_uuid}")
        _logger.error(f"Response: {response}")
        # _logger.error(f"Response content: {response.json()}")
        stats['submitter_failed'] += 1


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

    endpoint = env["backend"]["endpoint"]

    stats = {
        'len': 0,
        'submitter_failed': 0,
        'col_without_submitter': 0,
        'col_with_submitter': 0,
        'submitter_without_group': 0,
        'error': 0,
    }
    cycle_counter = 0

    for page in range(MAX_PAGES):
        collections = dspace_be.client.get_collections(page=page, size=PAGE_SIZE)
        if not collections:
            _logger.info(f'No more collections found after {page} pages.')
            break

        for coll in collections:
            stats['len'] += 1
            cycle_counter += 1
            _logger.info(f"Processing collection: {coll.name} (UUID: {coll.uuid})")
            create_submitter_group_if_missing(dspace_be, endpoint, coll, args.group, stats)

            # Delay after every 50 requests
            if cycle_counter % PAGE_SIZE == 0:
                _logger.info("Throttling: Sleeping for 1 second after 50 requests...")
                time.sleep(1)

    _logger.info(f'Processing complete. Summary: {stats}')
