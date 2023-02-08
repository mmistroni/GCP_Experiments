from __future__ import absolute_import
import numpy as np
from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
from datetime import datetime, date
import logging
import apache_beam as beam
import pandas as pd
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_descriptive_and_technical, get_latest_stock_news, get_mm_trend_template, get_fmprep_historical
from .marketstats_utils import get_all_stocks
from apache_beam.io.gcp.internal.clients import bigquery

'''
Further source of infos
https://medium.com/@mancuso34/building-all-in-one-stock-economic-data-repository-6246dde5ce02
'''
class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fmprepkey')
        parser.add_argument('--mmrun')


class TrendTemplateLoader(beam.DoFn):
    #'https://medium.datadriveninvestor.com/how-to-create-a-premarket-watchlist-for-day-trading-263a760a31db'
    '''
            The current stock price is above both the 150-day (30-week) and the 200-day (40-week) moving average price lines.
            The 150-day moving average is above the 200-day moving average.
            The 200-day moving average line is trending up for at least 1 month (preferably 4–5 months minimum in most cases).
            The 50-day (10-week) moving average is above both the 150-day and 200-day moving averages.
            The current stock price is trading above the 50-day moving average.
            The current stock price is at least 30 percent above its 52-week low. (Many of the best selections will be 100 percent, 300 percent, or greater above their 52-week low before they emerge from a solid consolidation period and mount a large scale advance.)
            The current stock price is within at least 25 percent of its 52-week high (the closer to a new high the better).
            The relative strength ranking (as reported in Investor’s Business Daily) is no less than 70, and preferably in the 80s or 90s, which will generally be the case with the better selections.
            '''

    def __init__(self, key):
        self.key = key

    def best_fit_slope(self, y: np.array) -> float:
        '''
        Determine the slope for the linear regression line

        Parameters
        ----------
        y : TYPE
            The time-series to find the linear regression line for

        Returns
        -------
        m : float
            The gradient (slope) of the linear regression line
        '''

        x = np.arange(0, y.shape[0])

        x_bar = np.mean(x)
        y_bar = np.mean(y)

        return np.sum((x - x_bar) * (y - y_bar)) / np.sum((x - x_bar) ** 2)

    def get_mm_trendtemplate(self, ticker):

        try:
            res = get_mm_trend_template(ticker, self.key, numdays=1500)
            if res:
                df = pd.DataFrame(data=res, columns=list(res[0].keys()))
                # mvg a
                df['200_ma'] = df['close'].rolling(200).mean()
                df['52_week_high'] = df['close'].rolling(52 * 5).max()
                df['52_week_low'] = df['close'].rolling(52 * 5).min()
                df['150_ma'] = df['close'].rolling(150).mean()
                df['50_ma'] = df['close'].rolling(150).mean()
                df['slope'] = df['200_ma'].rolling(40).apply(self.best_fit_slope)
                df['pricegt50avg'] = df['close'] > df['50_ma']
                df['price30pctgt52wklow'] = df['close'] / df['52_week_low'] > 1.3
                df['priceWithin25pc52wkhigh'] = df['close'] / df['52_week_high'] > 0.8

                df['trend_template'] = (
                        (df['close'] > df['200_ma'])
                        & (df['close'] > df['150_ma'])
                        & (df['150_ma'] > df['200_ma'])
                        & (df['slope'] > 0)
                        & (df['50_ma'] > df['150_ma'])
                        & (df['50_ma'] > df['200_ma'])
                        & (df['pricegt50avg'] == True)
                        & (df['priceWithin25pc52wkhigh'] == True)
                        & (df['priceWithin25pc52wkhigh'] == True)
                )
                return df[['date', 'close', '200_ma', '150_ma', '50_ma', 'slope', '52_week_low', '52_week_high', 'trend_template']]
            else:
                return None
        except Exception as e:
            logging.info(f'exception in getting trendtemplatedata for {ticker}:{str(e)}')
            return None

    def process(self, elements):
        all_dt = []
        tickers_to_process = elements.split(',')
        logging.info('Ticker to process:{len(tickers_to_process}')

        excMsg = ''
        isException = False

        for idx, ticker in enumerate(tickers_to_process):
            # Not good. filter out data at the beginning to reduce stress load for rest of data
            try:
                mmdata = self.get_mm_trendtemplate(ticker)
                if mmdata is not None:
                    tt_filter = (mmdata['trend_template'] == True)
                    trending = mmdata[tt_filter]

                    if trending.shape[0] > 0:
                        data = trending.to_dict('records')
                        all_dt += data
            except Exception as e:
                excMsg = f"{idx}/{len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt


class PremarketLoader(beam.DoFn):
    #'https://medium.datadriveninvestor.com/how-to-create-a-premarket-watchlist-for-day-trading-263a760a31db'
    def __init__(self, key, microcap_flag=True, split=''):
        self.key = key
        self.microcap_flag = microcap_flag
        self.split = split


    def process(self, elements):
        all_dt = []
        tickers_to_process = elements.split(',')
        logging.info('Ticker to process:{len(tickers_to_process}')

        excMsg = ''
        isException = False

        for idx, ticker in enumerate(tickers_to_process):
            # Not good. filter out data at the beginning to reduce stress load for rest of data
            try:
                '''
                Float ≤ 20M.
                The Relative Volume (RVOL) should be equal to or greater than 2. RVOL is the average volume (over 15 or 60 days) divided by the day volume.
                $1.5 ≤ price ≤ $10
                The stocks should be a Gapper and the percentage of change should be greater than %5 if it’s a bear market and %10 if it’s a bull market. A Gapper means there is a gap between yesterday’s close and today’s open.
                The stock should have news or a catalyst for that high relative volume and gap.
                Volume ≥ 100K'''
                descr_and_tech = get_descriptive_and_technical(ticker, self.key)

                if descr_and_tech['open'] is not None \
                    and descr_and_tech['price'] is not None \
                    and descr_and_tech['sharesOutstanding'] is not None \
                    and descr_and_tech['volume'] is not None \
                    and descr_and_tech['avgVolume'] is not None \
                    and descr_and_tech['volume'] > 0 \
                    and descr_and_tech['previousClose'] > 0:
                    logging.info(f'Checks proceed for {ticker}')
                    # checking pct change
                    rVol = descr_and_tech['avgVolume'] /  descr_and_tech['volume']
                    change = descr_and_tech['open'] / descr_and_tech['previousClose']
                    vol = descr_and_tech['volume']
                    price = descr_and_tech['price']

                    if rVol >=2 and change >=0.05 \
                        and 1.5 < price <= 10:
                        logging.info(f'Adding:{descr_and_tech}')
                        stock_news = get_latest_stock_news(ticker, self.key)
                        descr_and_tech.update(stock_news)
                        descr_and_tech['rVolume'] = rVol
                        all_dt.append(descr_and_tech)
            except Exception as e:
                excMsg = f"{idx}/{len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt

class HistoricalMarketLoader(beam.DoFn):
    def __init__(self, key):
        self.key = key

    def process(self, elements):
        all_dt = []
        tickers_to_process = elements.split(',')
        logging.info('Ticker to process:{len(tickers_to_process}')

        excMsg = ''
        isException = False

        for idx, ticker in enumerate(tickers_to_process):
            # Not good. filter out data at the beginning to reduce stress load for rest of data
            try:
                res = get_fmprep_historical(ticker, self.key, numdays=1600, colname=None)
                data = [dict((k, v) for k, v in d.items() if k in ['date', 'symbol', 'open', 'adjClose', 'volume']) for
                        d in res]

                df = pd.DataFrame(data=data)
                df['symbol'] = ticker

                recs = df.to_dict('records')

                all_dt += recs
            except Exception as e:
                excMsg = f"{idx}/{len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                isException = True
                break
        if isException:
            raise Exception(excMsg)
        return all_dt




def combine_tickers(input):
    return ','.join(input)



def write_to_bucket(lines, sink):
    return (
            lines | 'Writing to bucket' >> sink
    )


def extract_trend_pipeline(p, fmpkey):
    return (p
            | 'Reading Tickers' >> beam.Create(get_all_stocks(fmpkey))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(TrendTemplateLoader(fmpkey))
    )


def extract_historical_pipeline(p, fmpkey):
    return (p
            | 'Reading Tickers' >> beam.Create(get_all_stocks(fmpkey))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(HistoricalMarketLoader(fmpkey))
    )



def extract_data_pipeline(p, fmpkey):
    return (p
            | 'Reading Tickers' >> beam.Create(get_all_stocks(fmpkey))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(PremarketLoader(fmpkey))
    )

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).

    pipeline_options = XyzOptions()

    timeout_secs = 18400
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)

    test_sink =beam.Map(logging.info)

    with beam.Pipeline(options=pipeline_options) as p:

        if pipeline_options.mmrun:
            if 'historical' in pipeline_options.mmrun:
                logging.info('Running historical ppln..')
                data = extract_trend_pipeline(p, pipeline_options.fmprepkey)
                destination = 'gs://mm_dataflow_bucket/inputs/historical_prices_5y_{}'.format(
                                        date.today().strftime('%Y-%m-%d %H:%M'))
                test_sink = beam.io.WriteToText(destination, num_shards=1)

            else:
                logging.info('Extracting trend pipeline')
                data = extract_trend_pipeline(p, pipeline_options.fmprepkey)
        else:
            data = extract_data_pipeline(p, pipeline_options.fmprepkey)

        data | test_sink







