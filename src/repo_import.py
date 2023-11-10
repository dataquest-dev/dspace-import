import sys
import time
import os
import argparse
import logging

import settings
import project_settings
from utils import init_logging, update_settings, exists_key, set_key

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

import dspace  # noqa
import pump  # noqa


def verify_disabled_mailserver():
    """
        Is the email server really off?
    """
    email_s_off = input("Please make sure your email server is turned off. "
                        "Otherwise unbearable amount of emails will be sent. "
                        "Is your EMAIL SERVER really OFF? (Y/N)")
    if email_s_off.lower() not in ("y", "yes"):
        _logger.critical("The email server is not off.")
        sys.exit()


def deserialize(resume: bool, obj, cache_file: str) -> bool:
    """
        If cache file exists, deserialize it and return True.
    """
    if not resume:
        return False

    if not os.path.exists(cache_file):
        return False
    obj.deserialize(cache_file)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Import data from previous version to current DSpace')
    parser.add_argument('--resume',
                        help='Resume by loading values into dictionary',
                        required=False, type=bool, default=True)
    parser.add_argument('--config',
                        help='Update configs',
                        required=False, type=str, action='append')

    args = parser.parse_args()
    s = time.time()

    for k, v in [x.split("=") for x in (args.config or [])]:
        _logger.info(f"Updating [{k}]->[{v}]")
        _1, prev_val = exists_key(k, env, True)
        if isinstance(prev_val, bool):
            new_val = str(v).lower() in ("true", "t", "1")
        elif prev_val is None:
            new_val = str(v)
        else:
            new_val = type(prev_val)(v)
        set_key(k, new_val, env)

    # just in case
    # verify_disabled_mailserver()

    # update based on env
    for k, v in env["cache"].items():
        env["cache"][k] = os.path.join(env["resume_dir"], v)

    # input data directory
    input_dir = env["input"]["datadir"]
    if not os.path.exists(input_dir):
        _logger.critical(f"Input directory [{input_dir}] does not exist - cannot import.")
        sys.exit(1)

    dspace_be = dspace.rest(
        env["backend"]["endpoint"],
        env["backend"]["user"],
        env["backend"]["password"],
        env["backend"]["authentication"]
    )

    _logger.info("Loading repo objects")
    repo = pump.repo(env, dspace_be)

    _logger.info("New instance database status:")
    repo.raw_db_7.status()
    _logger.info("Reference database dspace status:")
    repo.raw_db_dspace_5.status()
    _logger.info("Reference database dspace-utilities status:")
    repo.raw_db_utilities_5.status()

    _logger.info("Starting import")

    # import handles
    cache_file = env["cache"]["handle"]
    if deserialize(args.resume, repo.handles, cache_file):
        _logger.info(f"Resuming handle [{repo.handles.imported}]")
    else:
        repo.handles.import_to(dspace_be)
        repo.handles.serialize(cache_file)

    # import metadata
    cache_file = env["cache"]["metadataschema"]
    if deserialize(args.resume, repo.metadatas, cache_file):
        _logger.info(
            f"Resuming metadata [schemas:{repo.metadatas.imported_schemas}][fields:{repo.metadatas.imported_fields}]")
    else:
        repo.metadatas.import_to(dspace_be)
        repo.metadatas.serialize(cache_file)

    # import bitstreamformatregistry
    cache_file = env["cache"]["bitstreamformat"]
    if deserialize(args.resume, repo.bitstreamformatregistry, cache_file):
        _logger.info(
            f"Resuming bitstreamformatregistry [{repo.bitstreamformatregistry.imported}]")
    else:
        repo.bitstreamformatregistry.import_to(dspace_be)
        repo.bitstreamformatregistry.serialize(cache_file)

    # import community
    cache_file = env["cache"]["community"]
    if deserialize(args.resume, repo.communities, cache_file):
        _logger.info(
            f"Resuming community [coms:{repo.communities.imported_coms}][com2coms:{repo.communities.imported_com2coms}]")
    else:
        repo.communities.import_to(dspace_be, repo.handles, repo.metadatas)
        if len(repo.communities) == repo.communities.imported_coms:
            repo.communities.serialize(cache_file)

    # import collection
    cache_file = env["cache"]["collection"]
    if deserialize(args.resume, repo.collections, cache_file):
        _logger.info(
            f"Resuming collection [cols:{repo.collections.imported_cols}] [groups:{repo.collections.imported_groups}]")
    else:
        repo.collections.import_to(dspace_be, repo.handles,
                                   repo.metadatas, repo.communities)
        repo.collections.serialize(cache_file)

    # import registration data
    cache_file = env["cache"]["registrationdata"]
    if deserialize(args.resume, repo.registrationdatas, cache_file):
        _logger.info(f"Resuming registrationdata [{repo.registrationdatas.imported}]")
    else:
        repo.registrationdatas.import_to(dspace_be)
        repo.registrationdatas.serialize(cache_file)

    # import eperson groups
    cache_file = env["cache"]["epersongroup"]
    if deserialize(args.resume, repo.groups, cache_file):
        _logger.info(
            f"Resuming epersongroup [eperson:{repo.groups.imported_eperson}] [g2g:{repo.groups.imported_g2g}]")
    else:
        repo.groups.import_to(dspace_be, repo.metadatas)
        repo.groups.serialize(cache_file)

    # import eperson
    cache_file = env["cache"]["eperson"]
    if deserialize(args.resume, repo.epersons, cache_file):
        _logger.info(f"Resuming epersons [{repo.epersons.imported}]")
    else:
        repo.epersons.import_to(env, dspace_be, repo.metadatas)
        repo.epersons.serialize(cache_file)

    # import userregistrations
    cache_file = env["cache"]["userregistration"]
    if deserialize(args.resume, repo.userregistrations, cache_file):
        _logger.info(f"Resuming userregistrations [{repo.userregistrations.imported}]")
    else:
        repo.userregistrations.import_to(dspace_be, repo.epersons)
        repo.userregistrations.serialize(cache_file)

    # import group2eperson
    cache_file = env["cache"]["group2eperson"]
    if deserialize(args.resume, repo.egroups, cache_file):
        _logger.info(f"Resuming egroups [{repo.egroups.imported}]")
    else:
        repo.egroups.import_to(dspace_be, repo.groups, repo.epersons)
        repo.egroups.serialize(cache_file)

    # import licenses
    cache_file = env["cache"]["license"]
    if deserialize(args.resume, repo.licenses, cache_file):
        _logger.info(
            f"Resuming licenses [labels:{repo.licenses.imported_labels}] [licenses:{repo.licenses.imported_licenses}]")
    else:
        repo.licenses.import_to(env, dspace_be, repo.epersons)
        repo.licenses.serialize(cache_file)

    # import item
    cache_file = env["cache"]["item"]
    if deserialize(args.resume, repo.items, cache_file):
        _logger.info(f"Resuming items [{repo.items.imported}]")
        repo.items.import_to(cache_file, dspace_be, repo.handles,
                             repo.metadatas, repo.epersons, repo.collections)
    else:
        repo.items.import_to(cache_file, dspace_be, repo.handles,
                             repo.metadatas, repo.epersons, repo.collections)
        repo.items.serialize(cache_file)
        repo.items.raw_after_import(
            env, repo.raw_db_7, repo.raw_db_dspace_5, repo.metadatas)

    # import tasklists
    cache_file = env["cache"]["tasklistitem"]
    if deserialize(args.resume, repo.tasklistitems, cache_file):
        _logger.info(f"Resuming tasklistitems [{repo.tasklistitems.imported}]")
    else:
        repo.tasklistitems.import_to(dspace_be, repo.epersons, repo.items)
        repo.tasklistitems.serialize(cache_file)

    # import bundle
    cache_file = env["cache"]["bundle"]
    if deserialize(args.resume, repo.bundles, cache_file):
        _logger.info(f"Resuming bundles [{repo.bundles.imported}]")
    else:
        repo.bundles.import_to(dspace_be, repo.metadatas, repo.items)
        repo.bundles.serialize(cache_file)

    # import bitstreams
    cache_file = env["cache"]["bitstream"]
    if deserialize(args.resume, repo.bitstreams, cache_file):
        _logger.info(f"Resuming bitstreams [{repo.bitstreams.imported}]")
        repo.bitstreams.import_to(
            env, cache_file, dspace_be, repo.metadatas, repo.bitstreamformatregistry, repo.bundles, repo.communities, repo.collections)
    else:
        repo.bitstreams.import_to(
            env, cache_file, dspace_be, repo.metadatas, repo.bitstreamformatregistry, repo.bundles, repo.communities, repo.collections)
        repo.bitstreams.serialize(cache_file)

    # import usermetadata
    cache_file = env["cache"]["usermetadata"]
    if deserialize(args.resume, repo.usermetadatas, cache_file):
        _logger.info(f"Resuming usermetadatas [{repo.usermetadatas.imported}]")
    else:
        repo.usermetadatas.import_to(dspace_be, repo.bitstreams, repo.userregistrations)
        repo.usermetadatas.serialize(cache_file)

    # before importing of resource policies we have to delete all
    # created data
    repo.raw_db_7.delete_resource_policy()

    # import bitstreams
    cache_file = env["cache"]["resourcepolicy"]
    if deserialize(args.resume, repo.resourcepolicies, cache_file):
        _logger.info(f"Resuming resourcepolicies [{repo.resourcepolicies.imported}]")
    else:
        repo.resourcepolicies.import_to(env, dspace_be, repo)
        repo.resourcepolicies.serialize(cache_file)

    # migrate sequences
    repo.sequences.migrate(env, repo.raw_db_7, repo.raw_db_dspace_5,
                           repo.raw_db_utilities_5)

    took = time.time() - s
    _logger.info(f"Took [{round(took, 2)}] seconds to import all data")
    _logger.info(
        f"Made [{dspace_be.get_cnt}] GET requests, [{dspace_be.post_cnt}] POST requests.")

    _logger.info("New instance database status:")
    repo.raw_db_7.status()
    _logger.info("Reference database dspace status:")
    repo.raw_db_dspace_5.status()
    _logger.info("Reference database dspace-utilities status:")
    repo.raw_db_utilities_5.status()
