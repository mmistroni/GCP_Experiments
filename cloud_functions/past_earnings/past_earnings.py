import logging

logger = logging.getLogger(__name__)

import json
import urllib


class EdgarClient(object):
    '''
        Client for edgar online API
        Note that the online api only allows a max of x requests per minute
        so we'll need to run all this data offline by pausing between one request
        and the other
    '''

    @staticmethod
    def _extract_financials(rows, row_num):
        meaningful_indicators = [
            'periodenddate',
            'fiscalyear',
            'fiscalquarter',
            'grossprofit',
            'ebit',
            'netincome',
            'cashandcashequivalents',
            'totalrevenue',
        ]
        fields = meaningful_indicators
        financial_indicators = []
        for rownum in range(0, row_num):
            row_values = rows[rownum]['values']
            dict_for_keys = [d for d in row_values if d['field'] in fields]
            financial_indicators.append(dict_for_keys)
        return financial_indicators

    @classmethod
    def getFinancialQuarterly(cls, symbol, numquarters):
        '''
            Retrieves quarterly earnings
            :param str symbol: a stock ticker
            :param int numquarters: Number of qurters
            :return a dictionary of financial data for last x quarters
        '''
        core_financial_quarterly = 'http://datafied.api.edgar-online.com/v2/corefinancials/qtr?primarysymbols={symbol}&numperiods={numperiods}&appkey=73m2m3mj3nytpzba54d6w882'
        appkey = '73m2m3mj3nytpzba54d6w882'
        fin_url = core_financial_quarterly.format(symbol=symbol,
                                                  numperiods=numquarters,
                                                  appkey=appkey)

        response = urllib.request.urlopen(fin_url)
        json_dict = json.load(response)
        total_rows = json_dict['result']['totalrows']

        all_dict = cls._extract_financials(json_dict['result']['rows'], total_rows)
        mapped = map(lambda l: dict((d['field'], d['value']) for d in l), all_dict)
        return json.dumps({'result': [item for item in mapped]})


def past_earnings(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    # does not work. need to pass params
    input_path = request.path
    logger.info('Inside Function.path is:%s', input_path)

    if input_path and len(input_path.split('/')) > 1:
        logger.info('Path is:%s', input_path)
        items = input_path.split('/')
        symbol, period = items[1:3]
        return EdgarClient.getFinancialQuarterly(symbol, int(period))

    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return 'Invalid Path:{}'.format(input_path)
