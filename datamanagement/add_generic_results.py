#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import click
import json
import ast
import pandas as pd

from utils.constants import LOGGING_FORMAT
from utils.runtime_args import parse_runtime_args
from dbclients.basicclient import NotFoundError
from dbclients.tantalus import TantalusApi
import datamanagement.templates as templates
import datamanagement.transfer_files as transfer_files

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


@click.command()
@click.argument('filepaths', nargs=-1)
@click.option('--storage_name', required=True)
@click.option('--results_name', required=True)
@click.option('--results_type', required=True)
@click.option('--results_version', required=True)
@click.option('--sample_ids', multiple=True)
@click.option('--library_ids', multiple=True)
@click.option('--tag_name')
@click.option('--analysis_pk')
@click.option('--recursive', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def add_generic_results_cmd(
        filepaths, storage_name, results_name, results_type, results_version,
        sample_ids=(), library_ids=(), analysis_pk=None, recursive=False,
        tag_name=None, update=False, remote_storage_name=None):

    return add_generic_results(
        filepaths, storage_name, results_name, results_type, results_version,
        sample_ids=sample_ids, library_ids=library_ids, analysis_pk=analysis_pk,
        recursive=recursive, tag_name=tag_name, update=update,
        remote_storage_name=remote_storage_name,
    )


def add_generic_results(
        filepaths, storage_name, results_name, results_type, results_version,
        sample_ids=(), library_ids=(), analysis_pk=None, recursive=False,
        tag_name=None, update=False, remote_storage_name=None):

    tantalus_api = TantalusApi()

    sample_pks = []
    for sample_id in sample_ids:
        samples = tantalus_api.get(
            "sample",
            sample_id=sample_id,
        )
        sample_pks.append(samples['id'])

    library_pks = []
    for library_id in library_ids:
        librarys = tantalus_api.get(
            "dna_library",
            library_id=library_id,
        )
        library_pks.append(librarys['id'])

    #Add the file resource to tantalus
    file_resource_pks = []
    for filepath in filepaths:
        if recursive:
            logging.info("Recursing directory {}".format(filepath))
            add_filepaths = []
            for (dirpath, dirnames, filenames) in os.walk(filepath):
                for filename in filenames:
                    add_filepaths.append(os.path.join(dirpath, filename))

        else:
            add_filepaths = [filepath]

        for add_filepath in add_filepaths:
            extension = os.path.splitext(add_filepath)[1]
            if extension not in ('.txt', '.xls'):
                logging.info("Skipping file resource for {} to Tantalus".format(add_filepath))
                continue
            logging.info("Adding file resource for {} to Tantalus".format(add_filepath))
            resource, instance = tantalus_api.add_file(
                storage_name=storage_name,
                filepath=add_filepath,
                update=update,
            )
            file_resource_pks.append(resource["id"])

    if len(file_resource_pks) == 0:
        logging.warning("No files found in filepaths {}".format(filepaths))
        return

    results_dataset_fields = dict(
        name=results_name,
        results_type=results_type,
        results_version=results_version,
        analysis=analysis_pk,
        samples=sample_pks,
        libraries=library_pks,
        file_resources=file_resource_pks,
    )

    #Add the dataset to tantalus
    try:
        existing_results = tantalus_api.get("results", name=results_dataset_fields["name"])
    except NotFoundError:
        existing_results = None

    if update and existing_results is not None:
        logging.warning("results dataset {} exists, updating".format(results_dataset_fields["name"]))
        #HACK
        results_dataset_fields["file_resources"] = list(set(list(results_dataset_fields["file_resources"]) + list(existing_results["file_resources"])))
        results_dataset = tantalus_api.update("results", id=existing_results["id"], **results_dataset_fields)

    else:
        logging.info("creating results dataset {}".format(results_dataset_fields["name"]))
        results_dataset = tantalus_api.get_or_create("results", **results_dataset_fields)

    if tag_name is not None:
        tantalus_api.tag(tag_name, resultsdataset_set=[results_dataset["id"]])

    logging.info("Succesfully created sequence dataset with ID {}".format(results_dataset["id"]))

    if remote_storage_name is not None:
        transfer_files.transfer_dataset(
            tantalus_api, results_dataset['id'], "resultsdataset", storage_name, remote_storage_name)

    return results_dataset


if __name__=='__main__':
    add_generic_results_cmd()
