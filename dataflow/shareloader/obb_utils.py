import logging
import apache_beam as beam

from openbb import obb



class OBBLoader(beam.DoFn):
    def __init__(self, key, period='annual', limit=10):
        self.patKey = key

    def to_list_of_vals(self, data_dict):
        return ','.join([str(data_dict[field]) for field in get_fields()])

    def process(self, elements):
        logging.info(f'Logging in to OBB.. pat={self.patKey}')
        obb.account.login(pat=self.patKey)
        res =  obb.economy.money_measures(provider='federal_reserve').to_df()
        logging.info(f'Obtianed :{res.shape}')
        return res.to_dict('records')







