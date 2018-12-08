import sys
from sys import argv
import os
import json
import datamanagement.templates as templates
import string
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from datamanagement.utils.runtime_args import parse_runtime_args
from sets import Set
from utils.gsc import GSCAPI, get_sequencing_instrument

def reverse_complement(sequence):
    return str(sequence[::-1]).translate(string.maketrans("ACTGactg", "TGACtgac"))

def query_colossus_dlp_cell_info(colossus_api, library_id):

    sublibraries = colossus_api.get_colossus_sublibraries_from_library_id(library_id)

    cell_samples = {}
    for sublib in sublibraries:
        index_sequence = sublib["primer_i7"] + "-" + sublib["primer_i5"]
        cell_samples[index_sequence] = sublib["sample_id"]["sample_id"]

    return cell_samples

def query_colossus_dlp_rev_comp_override(colossus_api, library_id):
    library_info = colossus_api.query_libraries_by_library_id(library_id)

    rev_comp_override = {}
    for sequencing in library_info["dlpsequencing_set"]:
        for lane in sequencing["dlplane_set"]:
            rev_comp_override[lane["flow_cell_id"]] = sequencing["dlpsequencingdetail"][
                "rev_comp_override"
            ]

    return rev_comp_override

def decode_raw_index_sequence(raw_index_sequence, instrument, rev_comp_override):
    i7 = raw_index_sequence.split("-")[0]
    i5 = raw_index_sequence.split("-")[1]

    if rev_comp_override is not None:
        if rev_comp_override == "i7,i5":
            pass
        elif rev_comp_override == "i7,rev(i5)":
            i5 = reverse_complement(i5)
        elif rev_comp_override == "rev(i7),i5":
            i7 = reverse_complement(i7)
        elif rev_comp_override == "rev(i7),rev(i5)":
            i7 = reverse_complement(i7)
            i5 = reverse_complement(i5)
        else:
            raise Exception("unknown override {}".format(rev_comp_override))

        return i7 + "-" + i5

    if instrument == "HiSeqX":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    elif instrument == "HiSeq2500":
        i7 = reverse_complement(i7)
    elif instrument == "NextSeq550":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    else:
        raise Exception("unsupported sequencing instrument {}".format(instrument))

    return i7 + "-" + i5


# Script accepts 3 arguments
# Argv[1]: Sample ID
# Argv[2]: Library ID
# These two combined should make the external GSC identifier
# Argv[3]: Path of file to read list of filenames from to compare between Tantalus and GSC
if __name__ == '__main__':

    filename_pattern_map = {
        "_1.fastq.gz": (1, True),
        "_1_*.concat_chastity_passed.fastq.gz": (1, True),
        "_1_chastity_passed.fastq.gz": (1, True),
        "_1_chastity_failed.fastq.gz": (1, False),
        "_1_*bp.concat.fastq.gz": (1, True),
        "_2.fastq.gz": (2, True),
        "_2_*.concat_chastity_passed.fastq.gz": (2, True),
        "_2_chastity_passed.fastq.gz": (2, True),
        "_2_chastity_failed.fastq.gz": (2, False),
        "_2_*bp.concat.fastq.gz": (2, True),
    }

    gsc_api = GSCAPI()
    tantalus_api = TantalusApi()
    colossus_api = ColossusApi()

    sample_id = argv[1]
    library_id = argv[2]
    fastqs_to_check_path = argv[3]

    rev_comp_overrides = query_colossus_dlp_rev_comp_override(
        colossus_api, library_id
    )

    cell_samples = query_colossus_dlp_cell_info(colossus_api, library_id)

    library_infos = gsc_api.query(
        "library?external_identifier={}".format(sample_id + '_' + library_id)
    )

    if len(library_infos) == 0:
        logging.error('no libraries with external_identifier {} in gsc api'.format(sample_id + '_' + library_id))
        quit()
    elif len(library_infos) > 1:
        raise Exception(
            "multiple libraries with external_identifier {} in gsc api".format(
               sample_id + '_' + library_id
            )
        )

    library_info = library_infos[0]

    gsc_library_id = library_info["name"]

    fastq_infos = gsc_api.query("fastq?parent_library={}".format(gsc_library_id))
    print(len(fastq_infos))
    i = 0
    fastq_filesize = {}
    for fastq_info in fastq_infos:
        fastq_path = fastq_info["data_path"]
        flowcell_id = str(fastq_info['libcore']['run']['flowcell']['lims_flowcell_code'])
        lane_number = fastq_info['libcore']['run']['lane_number']

        flowcell_lane = flowcell_id
        if lane_number is not None:
            flowcell_lane = flowcell_lane + "_" + str(lane_number)

        sequencing_instrument = get_sequencing_instrument(
            fastq_info["libcore"]["run"]["machine"]
        )

        primer_id = fastq_info["libcore"]["primer_id"]
        primer_info = gsc_api.query("primer/{}".format(primer_id))
        raw_index_sequence = primer_info["adapter_index_sequence"]
        rev_comp_override = rev_comp_overrides.get(flowcell_lane)

        index_sequence = decode_raw_index_sequence(
            raw_index_sequence, sequencing_instrument, rev_comp_override
        )

        filename_pattern = fastq_info["file_type"]["filename_pattern"]
        read_end, passed = filename_pattern_map.get(filename_pattern, (None, None))

        try:
            cell_sample_id = cell_samples[index_sequence]
        except KeyError:
            raise Exception('unable to find index {} for flowcell lane {} for library {}'.format(
                index_sequence, flowcell_lane, dlp_library_id))

        if read_end is None:
            raise Exception("Unrecognized file type: {}".format(filename_pattern))

        if not passed:
            continue

        extension = ''
        compression = 'UNCOMPRESSED'
        if fastq_path.endswith('.gz'):
            extension = '.gz'
            compression = 'GZIP'
        elif not fastq_path.endswith('.fastq'):
            raise ValueError('unknown extension for filename {}'.format(fastq_path))

        tantalus_filename = templates.SC_WGS_FQ_TEMPLATE.format(
            primary_sample_id=sample_id,
            dlp_library_id=library_id,
            flowcell_id=flowcell_id,
            lane_number=lane_number,
            cell_sample_id=cell_sample_id,
            index_sequence=index_sequence,
            read_end=read_end,
            extension=extension,
        )

        fastq_filesize[tantalus_filename] = fastq_info
        i+=1
        print(i)
    
    with open(fastqs_to_check_path, 'rb') as f:
        fastqs_to_check_list = f.read().strip().split('\n')
    
    for path in fastqs_to_check_list:
        try:
            file_resource = tantalus_api.get('file_resource', filename=path)
            if(path in fastq_filesize.keys()):
                print(fastq_filesize[path]['data_path'])
                if(fastq_filesize[path]["file_size_bytes"] == file_resource['size']):
                    print('The size of {} matches in GSC and Shahlab at {} bytes'.format(path, file_resource['size']))
                else:
                    print('The size of {} does not match in GSC and Shahlab. GSC: {}, Shahlab: {}'.format(path, fastq_filesize[path]['file_size_bytes'], file_resource['size']))
            else:
                print("{} doesn't exist in GSC".format(path))
        except:
            print(path, 'not found in Tantalus')
