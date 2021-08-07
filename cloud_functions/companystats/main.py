import urllib
import json
import requests
import logging


def company_stats(ticker):
    base_url = 'https://financialmodelingprep.com/api/v3/company/profile/{ticker}'.format(ticker=ticker)

    try:
        data = requests.get(base_url).json()['profile']
        pdict = dict(price=data['price'], range=data['range'],
                     beta=data['beta'], industry=data['industry'], TICKER=ticker)
        ratings = requests.get('https://financialmodelingprep.com/api/v3/company/rating/{}'.format(ticker)).json()
        pdict['rating'] = ratings.get("rating")['recommendation'] if ratings.get("rating") else 'N/A'

        metrics = requests.get(
            'https://financialmodelingprep.com/api/v3/company/discounted-cash-flow/{}'.format(ticker)).json()
        pdict['dcf'] = metrics.get('dcf') or 'N/A'
    except Exception as  e:
        pdict = dict(price='N/A', range='N/A',
                     beta='N/A', industry='N/A', TICKER=ticker)

    return json.dumps(pdict)


def companystats(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()

    input_path = request.path.split('/')
    logging.info('Input Path is:{}'.format(input_path))
    if len(input_path) > 1:
        ticker = input_path[1]
        return company_stats(ticker)
    elif request_json and 'ticker' in request_json:
        return company_stats(ticker)
    else:
        return 'Invalid Request:{}'.format(requst.path)
