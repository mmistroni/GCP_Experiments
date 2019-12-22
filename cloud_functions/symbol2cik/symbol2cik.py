import logging

logger = logging.getLogger(__name__)
import urllib
import json
from pprint import pprint


class EdgarClient(object):
    '''cik
        Client for edgar online APIf
        Note that the online api only allows a max of x requests per minute
        so we'll need to run all this data offline by pausing between one request
        and the other
    '''

    @staticmethod
    def _extract_from_json(rows, row_num, key):
        row_values = rows[row_num - 1]['values']
        dict_for_key = [d for d in row_values if d['field'] == key]
        return dict_for_key[0]['value'] if dict else None

    @classmethod
    def download_cik(cls, symbol):
        logger.info('Downloading cik for %s', symbol)
        baseUrl = 'http://datafied.api.edgar-online.com/v2/companies?primarysymbols={symbol}&appkey=73m2m3mj3nytpzba54d6w882'
        amznUrl = baseUrl.format(symbol=symbol)
        response = urllib.request.urlopen(amznUrl)
        json_dict = json.load(response)
        pprint(json_dict)
        total_rows = json_dict['result']['totalrows']
        return cls._extract_from_json(json_dict['result']['rows'], total_rows - 1, 'cik')


def ticker2cik(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    input_path = request.path
    logger.info('Inside Function.path is:%s', input_path)

    if input_path and len(input_path.split('/')) > 1:
        logger.info('Path is:%s', input_path)
        arr = ''.join(input_path.split('/'))
        logger.info('Path is :%s', arr)
        for i, item in enumerate(input_path.split('/')):
            print ('at{} we got{}'.format(i, item))
        symbol = input_path.split('/')[1]

        cik_result = EdgarClient.download_cik(str(symbol))
        return json.dumps(cik_result)
    else:
        return 'Invalid Path{}'.format(request.path)
