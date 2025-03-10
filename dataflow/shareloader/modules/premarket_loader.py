from __future__ import absolute_import
import numpy as np
from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
from pandas.tseries.offsets import BDay
from datetime import datetime, date
import logging
import apache_beam as beam
import pandas as pd
from itertools import chain
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from .superperf_metrics import get_descriptive_and_technical, get_latest_stock_news, get_mm_trend_template, get_fmprep_historical
from .marketstats_utils import get_all_stocks
from apache_beam.io.gcp.internal.clients import bigquery
import argparse
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization

'''
Further source of infos
https://medium.com/@mancuso34/building-all-in-one-stock-economic-data-repository-6246dde5ce02
'''

HEADER_TEMPLATE = '<tr><th>AsOfDate</th><th>Ticker</th><th>Close</th><th>200D Mv Avg</th><th>150D Mv Avg</th><th>50D Mv Avg</th><th>52Wk Low</th><th>52Wk High</th><th>Trend Template</th></tr>'
ROW_TEMPLATE =  '<tr><td>{date}</td><td>{ticker}</td><td>{close}</td><td>{200_ma}</td><td>{150_ma}</td><td>{50_ma}</td><td>{52_week_low}</td><td>{52_week_high}</td><td>{trend_template}</td></tr>'


class MissedJoinerFn(beam.DoFn):
    def __init__(self):
        super(MissedJoinerFn, self).__init__()

    def process(self, row, **kwargs):
        right_dict = dict(kwargs['right_list'])
        logging.info(f'Processign row:{row}')
        left_key = row[0]
        if left_key not in  right_dict:
            yield (left_key, left_key)



class PreMarketCombineFn(beam.CombineFn):
  def create_accumulator(self):
    return [HEADER_TEMPLATE]

  def add_input(self, accumulator, input):
    formatted = ROW_TEMPLATE.format(**input)
    accumulator.append(formatted)
    return accumulator

  def merge_accumulators(self, accumulators):
    return chain(*accumulators)

  def extract_output(self, all_accumulators):
    data = [d for d in all_accumulators]
    return ''.join(data)

class PremarketEmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = ['mmistroni@gmail.com']
        self.key = key

    def _build_personalization(self, recipients):
        personalizations = []
        for recipient in recipients:
            logging.info('Adding personalization for {}'.format(recipient))
            person1 = Personalization()
            person1.add_to(Email(recipient))
            personalizations.append(person1)
        return personalizations


    def process(self, element):
        logging.info('Attepmting to send emamil to:{}, using key:{}'.format(self.recipients, self.key))
        template = "<html><body><table border='1'>{}</table></body></html>"
        content = template.format(element)
        print('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud_mm@outlook.com',
            to_emails=self.recipients,
            subject='Mark Minervini Trend Template Selection',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        self.send(message)


    def send(self, message):
        sg = SendGridAPIClient(self.key)
        response = sg.send(message)
        logging.info(f'Message is {message}')
        logging.info('Response is:{response}')



def parse_known_args(argv):
    """Parses args for the workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--fmprepkey')
    parser.add_argument('--mmrun')
    parser.add_argument('--numdays')
    parser.add_argument('--sendgridkey')
    parser.add_argument('--inputfile')
    return parser.parse_known_args(argv)


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

    def __init__(self, key, numdays='10', full_run=False):
        self.key = key
        self.numdays = int(numdays)
        self.full_run = full_run

    def get_fields(self):
        return ['date', 'ticker', 'close', '200_ma', '150_ma', '50_ma', 'slope', '52_week_low', '52_week_high', 'trend_template']

    def stringify(self, input_dict):
        data = [str(input_dict[f]) for f in self.get_fields()]
        return data



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
            res = get_mm_trend_template(ticker, self.key, numdays=self.numdays)

            #  need to find rank https://medium.datadriveninvestor.com/find-the-next-bull-market-winners-using-mark-minervinis-advice-4f82133ba4b2


            if res:
                df = pd.DataFrame(data=res, columns=list(res[0].keys()))
                # mvg a
                df['ticker'] = ticker
                df['200_ma'] = df['close'].rolling(200).mean()
                df['52_week_high'] = df['close'].rolling(52 * 5).max()
                df['52_week_low'] = df['close'].rolling(52 * 5).min()
                df['150_ma'] = df['close'].rolling(150).mean()
                df['50_ma'] = df['close'].rolling(50).mean()
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
                return  df[['date', 'ticker', 'close', '200_ma', '150_ma', '50_ma', 'slope', '52_week_low', '52_week_high', 'trend_template']]

            else:
                return None
        except Exception as e:
            logging.info(f'exception in getting trendtemplatedata for {ticker}:{str(e)}')
            return None

    def process(self, elements):
        all_dt = []
        tickers_to_process = elements.split(',')
        logging.info(f'Ticker to process:{len(tickers_to_process)}')
        logging.info(f'Is full run?{self.full_run}')


        excMsg = ''
        isException = False

        for idx, ticker in enumerate(tickers_to_process):
            # Not good. filter out data at the beginning to reduce stress load for rest of data
            # also need to use rsi
            # https://medium.datadriveninvestor.com/find-the-next-bull-market-winners-using-mark-minervinis-advice-4f82133ba4b2
            try:
                mmdata = self.get_mm_trendtemplate(ticker)
                if mmdata is not None:

                    if not self.full_run:
                        # Only selecting real deal
                        tt_filter = (mmdata['trend_template'] == True)
                        trending = mmdata[tt_filter]
                        if trending.shape[0] > 0:
                            logging.info(f'Found {trending.shape} records for {ticker}')
                            trending['asOfDate'] = pd.to_datetime(trending['date'])

                            max_tolerance = (date.today() - BDay(2))

                            logging.info(f'Max Lookback {max_tolerance}')

                            date_filter = trending.asOfDate > max_tolerance

                            filtered = trending[date_filter].drop('asOfDate', axis=1)
                            logging.info(f' input:{trending.shape}, output:{filtered.shape}')


                            records_dicts = filtered.to_dict('records')

                            if records_dicts:
                                all_dt += records_dicts
                    else:
                        logging.info('getting record and stringicy')
                        records_dicts = mmdata.to_dict('records')
                        stringified = []
                        for r in records_dicts:
                            csv = ','.join([str(r[k]) for k in
                                            ['date', 'ticker', 'close', '200_ma', '150_ma', '50_ma', 'slope',
                                             '52_week_low', '52_week_high',
                                             'trend_template']])
                            stringified.append(csv)

                        all_dt += stringified

            except Exception as e:
                excMsg = f"{idx}/{len(tickers_to_process)}Failed to process fundamental loader for {ticker}:{str(e)}"
                logging.info(excMsg)
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

                if recs:
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

def combine_result(input):

    res = '<br>'.join(input)
    return res


def get_yesterday_bq_data(p):
    yesterday = (date.today() - BDay(1)).date().strftime('%Y-%m-%d')
    edgar_sql = f"""SELECT DISTINCT TICKER FROM `datascience-projects.gcp_shareloader.mm_trendtemplate` 
                        WHERE AS_OF_DATE = PARSE_DATE("%F", '{yesterday}')

      """
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | 'Reading-MM yesteredayc' >> beam.io.Read(
                                beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))
              | 'Map to singledict' >> beam.Map(lambda t: (t, {}))
            )


def write_to_bucket(lines, sink):
    return (
            lines | 'Writing to bucket' >> sink
    )

def write_to_bigquery(p, bq_sink):
    return (p | 'Mapping to BQ Dict ' >> beam.Map(lambda in_dict: map_to_bq_dict(in_dict))
              | 'Writing to Sink ' >> bq_sink
              )

def extract_trend_pipeline(p, fmpkey, numdays=10, full_run=False):
    return (p
            | 'Reading Tickers' >> beam.Create(get_all_stocks(fmpkey))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(TrendTemplateLoader(fmpkey, numdays, full_run))
    )

def extract_full_run_pipeline(p, fmpkey, input_file, numdays):



    return (p
            | 'Reading Tickers' >> beam.io.textio.ReadFromText(input_file)
            | 'Converting to Tuple' >> beam.Map(lambda row: row.split(','))
            | 'Extracting only ticker and Industry' >> beam.Map(lambda item: (item[0]))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(TrendTemplateLoader(fmpkey, numdays, True))
    )



def extract_data_pipeline(p, fmpkey):
    return (p
            | 'Reading Tickers' >> beam.Create(get_all_stocks(fmpkey))
            | 'Combine all at fundamentals' >> beam.CombineGlobally(combine_tickers)
            | 'Getting fundamentals' >> beam.ParDo(PremarketLoader(fmpkey))
    )

def send_email_pipeline(p, sendgridkey):
    return (p
                | 'COMBINE everything' >> beam.CombineGlobally(PreMarketCombineFn())
                | 'Combine to string' >> beam.CombineGlobally(combine_result)
                | 'send pmk mail' >> beam.ParDo(PremarketEmailSender('mmistroni@gmail.com', sendgridkey))
            )

def map_to_bq_dict(input_dict):
    return dict(AS_OF_DATE=datetime.strptime(input_dict['date'], '%Y-%m-%d').date(),
                TICKER=input_dict.get('ticker', 'notcke'),
                CLOSE=input_dict.get('close', -1),
                MVG_AVG_200=input_dict.get('200_ma', -2),
                MVG_AVG_150=input_dict.get('150_ma', -3),
                MVG_AVG_50=input_dict.get('50_ma', -4),
                SLOPE = input_dict.get('slope', -5),
                WEEK_52_LOW = input_dict.get('52_week_low', -6),
                WEEK_52_HIGH=input_dict.get('52_week_high', -7),
                TREND_TEMPLATE = input_dict.get('trend_template', False)
                )

def find_dropped_tickers(p, todays_coll, sink):
    todays_remapped  = (todays_coll | 'Mapping to subset ' >> beam.Map(lambda in_dict: (in_dict['ticker'], dict(ticker=in_dict['ticker']))))
    yesterday_remapped = get_yesterday_bq_data(p)

    left_joined = (
                    todays_remapped
                    | 'MissedJoiner: JoinValues' >> beam.ParDo(MissedJoinerFn(),
                                                              right_list=beam.pvalue.AsIter(yesterday_remapped))
                    | 'to flat tpl' >> beam.Map(lambda tpl: tpl[1])
                    | 'out to sink' >> sink
            )

    res = (p | 'dummy line ' >> beam.Create(['----------------------  REMOVED TICKERS'])
                    | 'dummy to sink' >>sink)

    '''
    other_joined = (
            yesterday_remapped
            | 'Dropped from yesterda' >> beam.ParDo(MissedJoinerFn(),
                                                       right_list=beam.pvalue.AsIter(todays_remapped))
            | 'to flat tpl2' >> beam.Map(lambda tpl: tpl[1])
            | 'out to sink again' >> sink
    )
    '''


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    timeout_secs = 18400

    known_args, pipeline_args = parse_known_args(argv)
    pipeline_optionss = PipelineOptions(pipeline_args)
    pipeline_optionss.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_optionss.setMaxWorkflowRuntimeWalltimeSeconds(timeout_secs)

    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"

    test_sink = beam.Map(logging.info)
    bq_sink = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='mm_trendtemplate'),
        schema='AS_OF_DATE:DATE,TICKER:STRING,CLOSE:FLOAT,MVG_AVG_200:FLOAT,MVG_AVG_150:FLOAT, MVG_AVG_50:FLOAT,SLOPE:FLOAT,WEEK_52_HIGH:FLOAT,WEEK_52_LOW:FLOAT,TREND_TEMPLATE:BOOLEAN',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    with beam.Pipeline(options=pipeline_optionss) as p:

        if known_args.mmrun:
            if 'historical' in known_args.mmrun:
                logging.info('Running historical ppln..')
                data = extract_trend_pipeline(p, known_args.fmprepkey, known_args.numdays)
                destination = 'gs://mm_dataflow_bucket/datasets/supertrend_{}'.format(
                                        date.today().strftime('%Y-%m-%d %H:%M'))

                logging.info(f'Writing to {destination}')
                bucket_sink = beam.io.WriteToText(destination, num_shards=1,
                                                header='date,ticker,close,200_ma,150_ma,50_ma,slope,52_week_low,52_week_high,trend_template')

                data | bucket_sink

                send_email_pipeline(data, known_args.sendgridkey)

                write_to_bigquery(data, bq_sink)

            elif 'full_run' in known_args.mmrun:
                logging.info(f'Running historical ppln for:{known_args.inputfile}')

                all_tickers_5y = known_args.inputfile


                data = extract_full_run_pipeline(p, known_args.fmprepkey, all_tickers_5y, known_args.numdays, )
                destination = 'gs://mm_dataflow_bucket/datasets/historical_prices_5y_{}.csv'.format(
                    date.today().strftime('%Y-%m-%d %H:%M'))

                logging.info(f'Writing to {destination}')
                bucket_sink = beam.io.WriteToText(destination, num_shards=1,
                                                  header='date,ticker,close,200_ma,150_ma,50_ma,slope,52_week_low,52_week_high,trend_template')
                data | bucket_sink



            else:
                logging.info('Extracting trend pipeline')
                data = extract_trend_pipeline(p, known_args.fmprepkey, known_args.numdays)
                find_dropped_tickers(p, data, test_sink )

        else:
            data = extract_data_pipeline(p, known_args.fmprepkey)








