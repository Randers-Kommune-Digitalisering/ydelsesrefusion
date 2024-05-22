import requests
import logging
import urllib.parse

from utils.config import CUSTOM_DATA_CONNECTOR_HOST

def post_to_custom_data_connector(filename, file):
    encoded_filename = urllib.parse.quote(filename + '.csv')
    headers = {'overwrite': 'true','new-meta':'true'}
    multipart_form_data = {'file': (encoded_filename, file, 'text/csv')}

    r = requests.post('http://' + CUSTOM_DATA_CONNECTOR_HOST + '/in', files=multipart_form_data, headers=headers)
    if r.ok:
        logging.info(filename + ' uploaded to custom-data-connector')
    else:
        logging.error(r.status_code)
        logging.error(r.content)
        raise Exception('Failed to upload ' + filename)
