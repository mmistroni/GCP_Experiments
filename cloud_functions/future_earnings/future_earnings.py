from bs4 import BeautifulSoup
import requests
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class NasdaqClient(object):
    '''
      Nasdaq client to get quartery predictions
    '''

    @staticmethod
    def fetch_earning_estimates(ticker):
        eeurl = 'https://www.nasdaq.com/symbol/{ticker}/earnings-forecast'.format(ticker=ticker)
        r = requests.get(eeurl)
        data = r.text
        soup = BeautifulSoup(data)
        divs = soup.findAll("div", {"class": "genTable"})
        first = divs[0]
        heads = [str(r.contents[0]) for r in first.descendants if r.name == 'th' and not r.get('colspan')]
        rows = [r for r in first.find_all('tr')][1:]
        all_data = []
        for r in rows:
            cols = [str(c.string.replace(u'\xa0', u' ')) for c in r.find_all('td')[0:5]]
            all_data.append(tuple(cols))

        import pandas as pd
        labels = heads

        zipped_data = zip(labels, all_data)
        import json

        df = pd.DataFrame.from_records(all_data, columns=labels)
        return df.to_json()


def future_earnings(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    logger.info('REquest Path is:{}', request.path)
    if request.path and len(request.path.split('/')) > 0:
        ticker = request.path.split('/')[-1]
        logger.info('Requesting data for:{}', ticker)
        return NasdaqClient.fetch_earning_estimates(ticker)
    elif request_json and 'symbol' in request_json:
        ticker = request_json['symbol']
        return NasdaqClient.fetch_earning_estimates(ticker)
    else:
        logger.info('Path is:%s', request.path)
        return 'INvalid requestHello World:{}'.format(request.path)
