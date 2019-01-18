#!/usr/bin/env python
"""
A one-off script to back up and restore documents from/to an ES5 cluster via Elasticsearch's built-in snapshot feature.

In theory this should work when restoring an ES2 snapshot to an ES5 cluster.

"""
import argparse
import os
from datetime import datetime


from elasticsearch5.client import Elasticsearch as Elasticsearch5, TransportError as TransportError5


# Using the example indices and doc types from GOV.UK's search API
# https://github.com/alphagov/rummager/tree/master/config/schema/indexes


# This should be a folder registered in the elasticsearch.yml path.repo settings.
# If the repository location is specified as a relative path this path will be resolved against
# the first path specified in path.repo. If nothing is specified, the repo won't be created :(
# See https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html#_shared_file_system_repository
REPOSITORY_DIR = "/tmp"


# Using the GOV.UK indices as an example
INDICES = "govuk,government,detailed,metasearch,page-traffic"

ES5_TARGET_PORT = os.getenv('ES5_TARGET_HOST', 'http://localhost:9205')

es_client5 = Elasticsearch5([ES5_TARGET_PORT])


def create_repository(repository_name):
    # This will fail if REPOSITORY_DIR is not in the path.repo
    repo_settings = {
        "type": "fs",
        "settings": {
            "location": REPOSITORY_DIR,
            "compress": True
        }
    }
    es_client5.snapshot.create_repository(
        repository_name,
        repo_settings,
        verify=True
    )
    print('Creating repository "{}". Please wait a few moments.'.format(repository_name))


def create_snapshot(repository_name, indices=None):
    """
    https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.SnapshotClient.create

    Creates a snapshot of a cluster and puts it in a repository (which is created if it doesn't exist).

    Important: don't put snapshots from different versions of ES in the same repository.

    This may fail if any primary shards for the supplied indices are not available.

    :return: Snapshot file name (string)
    """
    today = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    # If repository doesn't exist, create it.
    try:
        es_client5.snapshot.get_repository(repository_name)
    except TransportError5:
        create_repository(repository_name)

    # Create snapshot in the repository. Add wait_for_completion=True to carry out synchronously
    snapshot_name = "{}-{}".format(repository_name, today)
    snapshot_settings = {
        "ignore_unavailable": True,  # Skips any indices above that aren't present
        "include_global_state": True  # Important for restoring - check 5.x docs about this...
    }
    if indices:
        # All indices included by default. Specify comma separated list to restrict to a subset of indices.
        snapshot_settings["indices"] = indices

    print("Creating snapshot '{}' in repository {}. Please wait a few moments.".format(snapshot_name, repository_name))
    es_client5.snapshot.create(
        repository_name,
        snapshot_name,
        snapshot_settings,
        wait_for_completion=True
    )
    return snapshot_name


def restore_from_snapshot(repository_name, snapshot_name, indices=None):
    # Restore a snapshot from the repository. Add wait_for_completion=True to carry out synchronously
    # An existing index can only be restored if it's closed and has the same number of shards as the index
    # in the snapshot.
    # TODO: Investigate changing index settings when doing a restore.
    restore_settings = {
        "ignore_unavailable": True,
        "include_global_state": True,
        "rename_pattern": "index_(.+)",
        "rename_replacement": "restored_index_$1"
    }
    if indices:
        restore_settings['indices'] = indices

    print("Restoring snapshot {} from repository {}. Please wait.".format(snapshot_name, repository_name))
    # TODO: this doesn't seem to be working properly when reopening an index
    es_client5.snapshot.restore(
        repository_name,
        snapshot_name,
        restore_settings,
        wait_for_completion=True
    )
    print("Snapshot restored.")


def snapshot_repo_status(repository_name):
    # Return information about all currently running snapshots for a repository
    print(es_client5.snapshot.status(repository_name))


def main(create=None, restore=None):
    """
    TODO: accept arg for name of snapshot to restore
    TODO: restore an ES2 snapshot to an ES5 cluster
    (see https://www.elastic.co/guide/en/elasticsearch/reference/5.2/modules-snapshots.html#_restoring_to_a_different_cluster)
    """
    REPO_NAME = 'my_snapshot_repo'
    SNAPSHOT_NAME = "{}-2018-12-20-15-27-01".format(REPO_NAME)
    if create:
        create_snapshot(REPO_NAME, indices=INDICES)
    if restore:
        restore_from_snapshot(REPO_NAME, SNAPSHOT_NAME, indices=INDICES)
    snapshot_repo_status(REPO_NAME)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--restore", action="store_true")
    args = parser.parse_args()
    main(create=args.create, restore=args.restore)
