from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from collections import defaultdict
from datetime import datetime
from dbclients.basicclient import NotFoundError
from models import AlignAnalysis, HmmcopyAnalysis
from workflows.utils import saltant_utils
from workflows.utils import file_utils, log_utils
import unanalyzed_data
import arguments
from jira import JIRA, JIRAError
import logging
import hashlib
import os

tantalus_api = TantalusApi()
colossus_api = ColossusApi()


def get_sequencings(library_id):
    library_info = colossus_api.get('library', pool_id=library_id)
    sequencings = [sequencing['id'] for sequencing in library_info['dlpsequencing_set']]
    return sequencings


def get_lanes_from_sequencings(sequencing_id_list):
    lanes = set()
    for sequencing_id in sequencing_id_list:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        for lane in sequencing['dlplane_set']:
            lanes.add(lane['id'])

    return list(lanes)


def check_library_for_analysis(library_id, aligner, analysis_type):
    '''
    Given a library, check if library is included in analysis and has all data imported. 
    If so, check if for existing analysis. Otherwise create analysis jira ticket.

    Args: 
        library_id (str): Library/pool id
        aligner (str): Either BWA_ALN_0_5_7 or BWA_MEM_0_7_6A (as 03/01/2019)
        analysis_type (str): Either align or hmmcopy (as 03/01/2019)
    '''

    library_info = colossus_api.get('library', pool_id=library_id)

    if library_info['exclude_from_analysis'] == True:
        print('Library {} is excluded from analysis; skipping'.format(library_id))
        return None

    taxonomy_id_map = {
        '9606':      'HG19',
        '10090':     'MM10',
    }

    taxonomy_id = library_info['sample']['taxonomy_id']
    reference_genome = taxonomy_id_map[taxonomy_id]

    sequencing_ids = get_sequencings(library_id)

    if not sequencing_ids:
        print('Library {} has no sequencings; skipping'.format(library_id))
        pass

    lanes = set()
    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)

        # Check if all lanes have been imported
        if sequencing['number_of_lanes_requested'] != 0 and len(sequencing['dlplane_set']) != sequencing['number_of_lanes_requested']:
            print("Not all data has been imported; skipping")
            return None

        for lane in sequencing['dlplane_set']:
            lanes.add(lane['flow_cell_id']) 

    lanes = ", ".join(sorted(lanes))
    lanes = hashlib.md5(lanes)
    lanes_hashed = "{}".format(lanes.hexdigest()[:8])
    analysis_name = "sc_{}_{}_{}_{}_{}".format(
        analysis_type, 
        aligner, 
        reference_genome, 
        library_id,
        lanes_hashed,
    )

    # Check if analysis already exists on Tantalus and colossus
    try:
        analysis = tantalus_api.get('analysis', name=analysis_name)
        print("Analysis already exists for {}; name: {}".format(library_id, analysis_name))
        analysis_info = dict(
            name = analysis_name,
            library_id = library_id,
            jira_ticket = analysis['jira_ticket'],
            analysis_created = True,
        )
    except NotFoundError:
        # Create jira ticket   
        jira_ticket = create_analysis_jira_ticket(library_id) 
        analysis_info = dict(
            name = analysis_name,
            library_id = library_id,
            jira_ticket = jira_ticket,
            analysis_created = False,
        )

    return analysis_info

def create_analysis_jira_ticket(library_id):
    '''
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        info (dict): Keys: library_id

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    '''

    JIRA_USER = os.environ['JIRA_USER']
    JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

    library = colossus_api.get('library', pool_id=info['library_id'])
    sample_id = library['sample']['sample_id']

    library_jira_ticket = library['jira_ticket']
    issue = jira_api.issue(library_jira_ticket)
    
    print('Creating analysis JIRA ticket for {} as sub task for {}'.format(info['library_id'], library_jira_ticket))

    sub_task = {
        'project': {'key': 'SC'},
        'summary': 'Analysis of LIB_{}_{}'.format(sample_id, info['library_id']),
        'issuetype' : { 'name' : 'Sub-task' },
        'parent': {'id': issue.key}
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key
    print('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, info['library_id']))

    return analysis_jira_ticket


def create_tantalus_analysis(name, jira_ticket, library_id, analysis_type):
    '''
    Create analysis objects on Tantalus

    Args:
        name (str): Name of analysis (should be in form sc_<analysis_type>_<aligner>_<ref_genome>_<library_id>_<hashed_lanes>)
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline
        analysis_type (str): Either align or hmmcopy (as of 03/01/2019)
    '''
    print('Creating analysis object {} on tantalus'.format(name))
    data = dict(
        name = name,
        analysis_type = analysis_type,
        jira_ticket = jira_ticket,
        library_id = library_id,
        status = 'idle',
    )
    analysis = tantalus_api.create('analysis', **data)

    return analysis


def create_colossus_analysis(library_id, jira_ticket, version):
    '''
    Create analysis objects on Colossus

    Args:
        library_id (str): Library/Pool id
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline
    '''

    taxonomy_id_map = {
        '9606':      'grch37',
        '10090':     'mm10',
    }

    library_id = library_id
    jira_ticket = jira_ticket

    library_info = colossus_api.get('library', pool_id='library_id')

    taxonomy_id = library_info['sample']['taxonomy_id']
    sequencings = [sequencing['id'] for sequencing in library_info['dlpsequencing_set']]
    lanes = get_lanes_from_sequencings(sequencings)

    print("Creating analysis information object for {}_{} on Colossus".format(library_info['sample_id'], library_info['library_id']))

    analysis_run = colossus_api.create('analysis_run',
        run_status='idle',
    )

    analysis = colossus_api.create('analysis_information',
        library=library,
        priority='M',
        analysis_jira_ticket=jira_ticket,
        version=version,
        sequencing=sequencings,
        reference_genome=taxonomy_id_map[taxonomy_id],
        analysis_run=analysis_run,
        aligner='A',
        smoothing='M',
        lanes=lanes,
    )

    colossus_api.update('analysis_run', analysis_run['id'], dlpanalysisinformation=analysis['id'])

    return analysis

def get_analyses_to_run(version, aligner):
    unaligned_data_libraries = unanalyzed_data.search_for_unaligned_data()
    no_hmmcopy_data_libraries = unanalyzed_data.search_for_no_hmmcopy_data()

    analyses_tickets = dict(
        align = [], 
        hmmcopy = []
    )

    for library_id in unaligned_data_libraries:
        analysis_info = check_library_for_analysis(library_id, aligner, 'align')

        if analysis_info is not None:
            jira_ticket = analysis_info['jira_ticket']
            analyses_tickets['align'].append(jira_ticket)     

            # TODO: Create analysis object on tantalus
            if analysis_info['analysis_created'] == False:
                tantalus_analysis = create_tantalus_analysis(analysis_info['name'], jira_ticket, analysis_info['library_id'], 'align')
                colossus_analysis = create_colossus_analysis(analysis_info['library_id'], jira_ticket, version)

    for library_id in no_hmmcopy_data_libraries:
        analysis_info = check_library_for_analysis(library_id, aligner, 'hmmcopy')

        if analysis_info is not None:
            jira_ticket = analysis_info['jira_ticket']
            analyses_tickets['hmmcopy'].append(jira_ticket)     

            # TODO: Create analysis object on tantalus
            if analysis_info['analysis_created'] == False:
                tantalus_analysis = create_tantalus_analysis(analysis_info['name'], jira_ticket, analysis_info['library_id'], 'hmmcopy')
                colossus_analysis = create_colossus_analysis(analysis_info['library_id'], jira_ticket, version)


if __name__ == '__main__':

    version = sys.argv[1] # may need to change
    aligner = sys.argv[2]

    aligner_map = {
        'A': 'BWA_ALN_0_5_7',
        'M': 'BWA_MEM_0_7_6A',
    }

    if aligner not in aligner_map.keys():
        raise Exception('Invalid aligner; choose A or M')

    aligner = aligner_map[aligner]

    config_path = os.path.join(os.environ['HEADNODE_AUTOMATION_DIR'], 'workflows/config/normal_config.json')
    config = file_utils.load_json(config_path)

    # test_names = ["test_{}".format(i) for i in range(3)]

    # for name in test_names:
    #     print("Running {} at {}".format(name, datetime.now()))
    #     saltant_utils.test(name, config)

    # raise Exception('done')

    analyses_to_run = get_analyses_to_run(version, aligner)

    # TODO: iterate through analyses tickets and use saltant to run analyses

    for align_analysis in analysis_to_run['align']:
        saltant_utils.run_align(align_analysis['jira_ticket'], version, config)

    for hmmcopy_analysis in analysis_to_run['hmmcopy']:
        saltant_utils.run_hmmcopy(hmmcopy_analysis['jira_ticket'], version, config)
