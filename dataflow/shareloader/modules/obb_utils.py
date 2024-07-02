import requests
import logging
from datetime import datetime
import apache_beam as beam


class OBBLoader(beam.DoFn):
    def __init__(self, key, period='annual', limit=10):
        self.patKey = key

    def to_list_of_vals(self, data_dict):
        return ','.join([str(data_dict[field]) for field in get_fields()])

    def process(self, elements):
        logging.info(f'Logging in to OBB.. pat={self.patKey}')
        logging.info(f'Obtianed :{res.shape}')
        return {}







