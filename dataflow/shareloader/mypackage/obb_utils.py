import logging
import apache_beam as beam

import requests


class OBBLoader(beam.DoFn):
    def __init__(self, key, pat):
        self.key = key
        self.pat = pat

    def process(self, elements):
        logging.info(f'Logging in to OBB.. pat={self.key}')
        from openbb import obb
        logging.info('activating obb')
        obb.account.login(pat=self.pat)

        results = []
        for ticker in elements.split(','):
            logging.info(f'Processing :{ticker}')
            try:
                dataDict = {}
                income_statement = requests.get(
                    'https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=10&apikey={key}'.format(
                        ticker=ticker, key=self.key)).json()
                all_eps = [d['eps'] for d in income_statement]

                if len(all_eps) >= 6:

                    if all_eps[4] > 0:
                        dataDict['epsGrowth5yrs'] = (all_eps[0] - all_eps[4]) / all_eps[4]

                    positive_eps = [e > 0 for e in all_eps]
                    dataDict['positiveEps'] = len(positive_eps)
                    dataDict['positiveEpsLast5Yrs'] = len([e > 0 for e in all_eps[0:5]])
                    latest = income_statement[0]
                    dataDict['netIncome'] = latest['netIncome']
                    dataDict['income_statement_date'] = latest['date']
                    dataDict['Ticker'] = ticker
                    results.append(dataDict)
            except Exception as e:
                logging.info(f'Exception processin {ticker}:{str(e)}')

        return results







