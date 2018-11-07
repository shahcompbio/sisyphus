import sys
sys.path.append('..')
import os
import logging
from azure.storage.blob import BlockBlobService

import dbclients.tantalus

from utils import log_utils, saltant_utils
from log_utils import sentinel


tantalus_api = dbclients.tantalus.TantalusApi()

log = logging.getLogger('sisyphus')


def tag_datasets(datasets, tag_name):
    """
    Tag a list of datasets for later reference
    Args:
        datasets: list of sequence dataset IDs
        tag_name: name with which to tag the datasets
    Returns:
        Tag object with the given parameters
    """

    return tantalus_api.get_or_create('sequence_dataset_tag',
        name=tag_name, sequencedataset_set=list(datasets))


def transfer_files(jira, config, from_storage, to_storage, dataset_ids, results=False):
    if from_storage == to_storage:
        log.debug('No files transferred, to and from both {}'.format(from_storage))
        return

    tag_name = '{}_{}'.format(jira, from_storage)
    if results:
        tag_name += '_results'

    sentinel(
        'Tagging {} files'.format(from_storage),
        tag_datasets,
        dataset_ids,
        tag_name,
    )

    sentinel(
        'Transferring files from {} to {}'.format(from_storage, to_storage),
        saltant_utils.transfer_files,
        jira,
        config,
        tag_name,
        from_storage,
        to_storage,
    )
