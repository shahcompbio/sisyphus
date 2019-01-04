import os
import requests
from datamanagement.utils.gsc import GSCAPI
import json

if __name__ == '__main__':
	gsc_url = os.environ.get('BIOAPPS_API')
	username = os.environ.get('GSC_API_USERNAME')
	password = os.environ.get('GSC_API_PASSWORD')
	token = requests.post(gsc_url + 'session', json={"password": password, "username": username}).json()['token']

	gsc_api = GSCAPI()

	gsc_api.query('/chipset')
