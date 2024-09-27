###
# This script changes the policy of items in a community to a specific group. Bulk access.
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
from dspace.impl.models import Item, Community  # noqa
from utils import init_logging, update_settings  # noqa

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

if "DSPACE_REST_API" in os.environ:
    env["backend"]["endpoint"] = os.getenv("DSPACE_REST_API")
    env_backend_endpoint = env["backend"]["endpoint"]
    _logger.info(f"Loaded env.backend.endpoint from env DSPACE_REST_API."
                 f" Current value: {env_backend_endpoint}")


def update_resource_policy(dspace_be, resource_policy, item, bundle, group_id):
    if resource_policy is None:
        _logger.warning(
            f'No resource policy for bundle {bundle.uuid} in item uuid={item.uuid}')
        return

    _logger.info(
        f'Changing policy uuid={resource_policy["id"]} for item uuid={item.uuid} to group uuid={group_id}')
    r = dspace_be.client.update_resource_policy_group(resource_policy["id"], group_id)
    _logger.debug('Response: ' + str(r))


def get_all_items(col):
    """
        Get all items from collection
    """
    page = 0
    # pagination limit of 100, use 50
    size = 50
    items = []
    has_more = True
    while has_more:
        cur_items = dspace_be.client.get_items_from_collection(
            col.uuid, page=page, size=size)
        if cur_items is None:
            return items
        items += cur_items
        page += 1
    return items


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Update resource policies for DSpace items.")
    parser.add_argument("--group",
                        type=str, required=True, help="Group UUID to assign resource policies to.")
    parser.add_argument("--community",
                        type=str, required=True, help="Community UUID where items will be updated.")
    parser.add_argument("--bundle-name",
                        type=str, default="THUMBNAIL", help="Name of the bundle (e.g., ORIGINAL, THUMBNAIL).")
    parser.add_argument("--policy-of",
                        type=str, required=True, help="Update bundle policy of: bundle, item, bitstream.")
    args = parser.parse_args()

    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    com_to_update = Community({
        "id": args.community,
        "type": "community",
        "_links": {
            "collections": {
                "href": f'{dspace_be.endpoint}/core/communities/{args.community}/collections'
            }
        },
    })

    cnt = {
        "updated": 0,
        "without_file": 0,
        "without_item_r_policy": 0,
    }

    # Get all collections of the community
    cols_to_update = dspace_be.client.get_collections(community=com_to_update)
    for col in cols_to_update:
        _logger.info(f'Collection: {col.name}')
        items = get_all_items(col)
        _logger.info(f'Found{len(items)} in collection')

        for i, item in enumerate(items):
            _logger.debug(f'Item [{i + 1:04d}/{len(items)}]: {item.uuid}')
            bundle = dspace_be.client.get_bundle_by_name(args.bundle_name, item.uuid)

            if args.policy_of == "bundle":
                # If there is no bundle, skip the item - there is no file
                if bundle is None:
                    _logger.debug(
                        f'No {args.bundle_name} bundle for item uuid={item.uuid}')
                    cnt["without_file"] += 1
                    continue
                cnt["updated"] += 1
                update_resource_policy(dspace_be, None, item, bundle, args.group)

            if args.policy_of == "item":
                item_resource_policy = dspace_be.client.get_resource_policy(item.uuid)
                # If there is no item resource policy, skip the item
                if item_resource_policy is None:
                    _logger.debug(f'No resource policy for item uuid={item.uuid}')
                    cnt["without_item_r_policy"] += 1
                    continue
                cnt["updated"] += 1
                update_resource_policy(dspace_be, None, item, bundle, args.group)

            if args.policy_of == "bitstream":
                if bundle is None:
                    _logger.debug(
                        f'No {args.bundle_name} bundle for item uuid={item.uuid}')
                    cnt["without_file"] += 1
                    continue

                bitstreams = dspace_be.client.get_bitstreams(
                    bundle=bundle, page=0, size=200)
                for bitstream in bitstreams:
                    resource_policy = dspace_be.client.get_resource_policy(bitstream.uuid)
                    update_resource_policy(
                        dspace_be, resource_policy, item, bundle, args.group)
                    cnt["updated"] += 1

    _logger.info(f'Processing completed. Summary: {cnt}')
