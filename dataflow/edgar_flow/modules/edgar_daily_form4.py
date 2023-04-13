import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions
from apache_beam.options.pipeline_options import SetupOptions
from .edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, \
            find_current_year, EdgarCombineFnForm4, ParseForm4
from datetime import date, datetime
from pandas.tseries.offsets import BDay
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from apache_beam.io.gcp.internal.clients import bigquery
from .edgar_utils import  get_edgar_table_schema, get_edgar_table_schema_form4,\
            get_edgar_daily_table_spec, get_edgar_daily_table_spec_form4
from .price_utils import get_current_price

class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(';')
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
        logging.info('Attepmting to send emamil to:{} at {}'.format(self.recipients, datetime.now()))
        template = "<html><body><table border='1' cellspacing='0' cellpadding='0' align='center'>" + \
         "<th>Ticker</th><th>Counts</th><th>ShareIncrease</th><th>TransactionPrice</th><th>TotalVolume</th><th>FilingURL</th>{}</table></body></html>"

        # cob, ticker, shares, increase, trans price, volume


        content = template.format(element)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='mmistroni@gmail.com',
            #to_emails=['mmistroni@gmail.com'],
            subject='Edgar Form4 Daily Filings (Insider Trading)',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)

bucket_destination = 'gs://mm_dataflow_bucket/outputs/daily/edgar_{}.csv'
form_type = '4'

EDGAR_URL = 'https://www.sec.gov/Archives/edgar/daily-index/{year}/{quarter}/master.{current}.idx'

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_value_provider_argument('--quarters', type=str,
                                           default='QTR1,QTR2,QTR3,QTR4')
        parser.add_argument('--key')

def find_current_quarter(current_date):
    quarter_dictionary = {
        "QTR1": [1, 2, 3],
        "QTR2": [4, 5, 6],
        "QTR3": [7, 8, 9],
        "QTR4": [10, 11, 12]
    }
    current_month = current_date.month
    logging.info('Fetching quarter for month:{}'.format(current_month))
    res=  [key for key, v in quarter_dictionary.items() if current_month in v][0]
    logging.info('Returning :{}'.format(res))
    return res

def find_current_day_url(sample):
    current_date = (datetime.now() - BDay(1))
    logging.info('Finding Edgar URL for {}  at:{}'.format(current_date, datetime.now().strftime('%Y-%m-%d')))
    current_quarter = find_current_quarter(current_date)
    current_year = find_current_year(current_date)
    master_idx_url = EDGAR_URL.format(quarter=current_quarter, year=current_year,
                                      current=current_date.strftime('%Y%m%d'))
    logging.info('Extracting data from:{}'.format(master_idx_url))
    return master_idx_url

def enhance_form_4(lines, qtr=''):
    # Need to find outstanding shares
    result = (
            lines
            | 'parsing form 4 filing_{}'.format(qtr) >> beam.ParDo(ParseForm4())

            #| 'Combining all ' >> beam.CombinePerKey(sum) # need a better one as now we have more items
            | 'Filtering out blanks_{}'.format(qtr) >> beam.Filter(lambda tpl: tpl[0][1] != '' and tpl[0][1] != 'N/A' )
            | 'Filtering out commasn_{}'.format(qtr) >> beam.Filter(lambda tpl: len(tpl[0][1].split(',')) == 1)
            | 'Mapping to tuple to be in line with mail templates_{}'.format(qtr) >>  beam.Map(
                                            lambda tpl: [tpl[0][0], tpl[0][1], tpl[1], #((cob_dt, trading_symbol), shares_acquired, share_increase, transaction_price, url)
                                                         tpl[2], tpl[3], tpl[4]])
            | 'Getting Current Market Price_{}'.format(qtr) >> beam.Map(lambda tpl: [tpl[0], tpl[1], tpl[2], #((cob_dt, trading_symbol), shares_acquired, share_increase, transaction_price, url)
                                                         tpl[3], tpl[4], tpl[5]] )
            | 'Getting Current Volumne_{}'.format(qtr) >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2], tpl[3], #cob, ticker, shares, increase, trans price, volume, url
                                                                 tpl[4],  tpl[2] * tpl[4] , tpl[5].split('/')[-1]))
            | 'Deduplicate elements_{}'.format(qtr) >> beam.Distinct()


    )
    return result

def run_my_pipeline(source, qtr=''):

    return (
            source
            | 'readFromText_{}'.format(qtr) >> beam.ParDo(ReadRemote())
            | 'map to Str_{}'.format(qtr) >> beam.Map(lambda line: str(line))
    )

def filter_form_4(source, qtr=''):
    return (
            source
            | 'Filter only form 4_{}'.format(qtr) >> beam.Filter(
                    lambda row: len(row.split('|')) > 4 and '4' in row.split('|')[2])
            | 'Generating form 4 file path_{}'.format(qtr) >> beam.Map(lambda row: (row.split('|')[3],
                                                                     '{}/{}'.format('https://www.sec.gov/Archives',
                                                                                    row.split('|')[4])))
            | 'replacing eol on form4_{}'.format(qtr) >> beam.Map(lambda p_tpl: (p_tpl[0], p_tpl[1][0:p_tpl[1].find('\\n')]))
    )

def send_email(lines, pipeline_options):
    email = (
            lines
            | 'Combining to get top 30' >> beam.CombineGlobally(EdgarCombineFnForm4())
            | 'SendEmail' >> beam.ParDo(EmailSender(pipeline_options.recipients, pipeline_options.key))
    )


def write_to_form4_bucket(lines, pipeline_options):

    bucket_destination = 'gs://mm_dataflow_bucket/outputs/edgar_daily_form4_{}.csv'.format(
        datetime.now().strftime('%Y%m%d%H%M'))
    return (
            lines
            | 'Map to  String' >> beam.Map(lambda lst: ','.join([str(i) for i in lst]))

            | 'WRITE TO BUCKET' >> beam.io.WriteToText(bucket_destination, header='date,ticker,count,price,share_incraese,volume,filing_file',
                                                       num_shards=1)


    )




def write_to_form4_bq(lines, form_name='form_4_daily_enhanced_test'):
    logging.info('writing to:{}'.format(form_name))
    big_query = (
            lines   ##cob, ticker, shares, increase, trans price, volume, url
            | 'Map to BQ FORM4 Dict' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                  TICKER=tpl[1],
                                                                  COUNT=int(tpl[2]),
                                                                  PRICE=float(tpl[4]) if tpl[4] else 0.0,
                                                                  VOLUME=float(tpl[5])))
            | 'Write to BigQuery F4' >> beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_edgar',
            tableId='form_4_daily_enhanced'),
        schema='COB:STRING,TICKER:STRING,COUNT:INTEGER,PRICE:FLOAT,VOLUME:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    timeout_secs = 10800
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"

    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)

    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        source = (p  | 'Startup' >> beam.Create(['start_token'])
                     |'Add current date' >> beam.Map(find_current_day_url)
                  )

        lines = run_my_pipeline(source)
        form4 = filter_form_4(lines)
        enhanced_data = enhance_form_4(form4)
        logging.info('Now sendig meail....')
        #send_email(enhanced_data, pipeline_options)
        write_to_form4_bucket(enhanced_data, pipeline_options)
        write_to_form4_bq(enhanced_data)

