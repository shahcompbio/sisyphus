from dbclients.colossus import ColossusApi

if __name__ == '__main__':
	colossus_api = ColossusApi()
	sequence_list = []

	sequence_set = colossus_api.list("sequencing", dlpsequencingdetail__lanes_requested=True, dlpsequencingdetail__lanes_received=True)

	for sequence in sequence_set:
		print(sequence['library'])
		colossus_api.update('sequencingdetails', sequence['dlpsequencingdetail']['id'], lanes_received=False, lanes_requested=False)