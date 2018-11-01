from dbclients.colossus import ColossusApi

if __name__ == '__main__':
	colossus_api = ColossusApi()
	sequence_list = []

	sequence_set = colossus_api.list("sequencing", dlpsequencingdetail__lanes_requested=True)

	for sequence in sequence_set:
		if(sequence['dlpsequencingdetail']['lanes_received']):
			sequence_list.append(sequence)

	with open("library_ids.txt", "w") as textfile:
		for sequence in sequence_list:
			colossus_api.update('sequencingdetails', sequence['dlpsequencingdetail']['id'], lanes_received=False, lanes_requested=False)
			textfile.write(sequence['library'] + '\n')
