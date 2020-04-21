import os
import yaml
import logging
import click
import sys
import pandas as pd

import dbclients.tantalus
import workflows.analysis.base
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import

import pandas as pd
import dbclients.colossus


tantalus_api = dbclients.tantalus.TantalusApi()
colossus_api = dbclients.colossus.ColossusApi()


def get_colossus_tifs(library_id):
    cell_ids = {}
    sublibraries = colossus_api.list('sublibraries', library__pool_id=library_id)
    for sublibrary in sublibraries:
        cell_ids[(sublibrary['row'], sublibrary['column'])] = sublibrary['cell_id']

    colossus_tiffs = []
    sublibrary_briefs = colossus_api.list('sublibraries_brief', library__pool_id=library_id)
    for sublibrary_brief in sublibrary_briefs:
        for tif_num in ('1', '2'):
            colossus_tiffs.append(
                {
                    'file_ch': sublibrary_brief[f'file_ch{tif_num}'],
                    'row': str(sublibrary_brief['row']),
                    'column': str(sublibrary_brief['column']),
                    'cell_id': cell_ids[(sublibrary_brief['row'], sublibrary_brief['column'])],
                }
            )

    return pd.DataFrame(colossus_tiffs)


def get_tantalus_tifs(dataset_ids):

    tantalus_tiffs = []
    for dataset_id in dataset_ids:
        files = tantalus_api.list(
            'file_resource',
            resultsdataset__id=dataset_id,
            filename__endswith='.tif',
            fileinstance__storage__name='singlecellresults'
        )
        for file in files:
            tantalus_tiffs.append(
                {
                    'file_ch': f"\\{file['filename'].split('/')[-2]}\\{file['filename'].split('/')[-1]}",
                    'filename': file['filename'],
                    'file_resource_id': str(file['id'])
                }
            )
    return pd.DataFrame(tantalus_tiffs)


def get_tif_color_from_path(path):
    tif_color = path.split('_')[-1].replace('.tif', '').lower()
    assert tif_color in ['cyan', 'red', 'green']
    return tif_color


class MicroscopePreprocessing(workflows.analysis.base.Analysis):
    analysis_type_ = 'microscope_preprocessing'

    def __init__(self, *args, **kwargs):
        super(MicroscopePreprocessing, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        ### Search in colossus for the cell ids for the library given by args['library_id']
        ## Search according to a template for the location in singlecellresults hwere the data sits

        try:
            microscope_results_dataset = tantalus_api.list(
                'resultsdataset',
                libraries__library_id=args['library_id'],
                results_type='MICROSCOPE'
            )
        except:
            raise Exception(f"Error while retrieving Microscope result dataset for library {args['library_id']}")

        return [dataset["id"] for dataset in microscope_results_dataset]


    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        name = '{analysis_type}_{library_id}'.format(
            analysis_type=cls.analysis_type_,
            library_id=args['library_id'])

        return name

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        # storage_client = self.tantalus_api.get_storage_client(storages['working_inputs'])

        t_df = get_tantalus_tifs(self.analysis['input_results'])
        c_df = get_colossus_tifs(self.analysis['library_id'])
        merged_df = pd.merge(left=c_df, right=t_df, left_on='file_ch', right_on='file_ch')

        input_info = {'cell_images': {}}

        for index, row in merged_df.iterrows():
            color = get_tif_color_from_path(row['file_ch'])

            if row['cell_id'] not in input_info['cell_images'].keys():
                input_info['cell_images'][row['cell_id']] = {}
            input_info['cell_images'][row['cell_id']][f"{color}_filepath"] = row['filename']

        for cell_id in input_info['cell_images'].keys():
            tif_count = len(input_info['cell_images'][cell_id].keys())
            assert tif_count == 2, f'For cell_id "{cell_id}" expected 2 tifs but got {tif_count}'

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)


    def run_pipeline(
            self,
            scpipeline_dir,
            tmp_dir,
            inputs_yaml,
            context_config_file,
            docker_env_file,
            docker_server,
            dirs,
            run_options,
            storages,
    ):
        storage_client = self.tantalus_api.get_storage_client(storages["working_results"])
        out_path = os.path.join(storage_client.prefix, self.out_dir)

        # run the pipeline

    def create_output_results(self, storages, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        ### Results name 
        results_name = 'microscope_processed_{}'.format(
            self.args['library_id'])

        library_pk = self.tantalus_api.get(
            'dna_library', library_id=self.args['library_id'])['id']

        results = results_import.create_dlp_results(
            self.tantalus_api,
            self.out_dir,
            self.get_id(),
            results_name,
            [],
            [library_pk],
            storages['working_results'],
            update=update,
            skip_missing=skip_missing,
        )

        return [results['id']]

    @classmethod
    def create_analysis_cli(cls):
        cls.create_cli([
            'library_id',
        ])


workflows.analysis.base.Analysis.register_analysis(MicroscopePreprocessing)

if __name__ == '__main__':

    '''
    
    # search input results
    datasets = tantalus_api.list('resultsdataset', libraries__library_id='A98256B', results_type='MICROSCOPE')
    dataset_ids = [dataset["id"] for dataset in datasets]

    # generate inputs yaml
    t_df = get_tantalus_tifs(dataset_ids)
    c_df = get_colossus_tifs('A98256B')
    merged_df = pd.merge(left=c_df, right=t_df, left_on='file_ch', right_on='file_ch')
    # merged_df = pd.read_csv(r"/Users/havasove/Desktop/tifs.csv", converters={i: str for i in range(0, 100)})

    input_info = {'cell_images': {}}
    for index, row in merged_df.iterrows():
        color = get_tif_color_from_path(row['file_ch'])

        if row['cell_id'] not in input_info['cell_images'].keys():
            input_info['cell_images'][row['cell_id']] = {}
        input_info['cell_images'][row['cell_id']][f"{color}_filepath"] = row['filename']

    for cell_id in input_info['cell_images'].keys():
        tif_count = len(input_info['cell_images'][cell_id].keys())
        assert tif_count == 2, f'For cell_id "{cell_id}" expected 2 tifs but got {tif_count}'

    with open('/Users/havasove/Desktop/tifs_1.yaml', 'w') as inputs_yaml:
        yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)

    '''

    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    MicroscopePreprocessing.create_analysis_cli()
