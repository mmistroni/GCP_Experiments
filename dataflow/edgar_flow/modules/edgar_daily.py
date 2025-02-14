import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions, DebugOptions
from .edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker,  \
            find_current_year, EdgarCombineFn, ParseForm4, cusip_to_ticker2
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

        template = "<html><body><table><th>PeriodOfReport</th><th>Cusip</th><th>Ticker</th><th>Counts</th>{}</table></body></html>"
        content = template.format(element)
        print('Sending \n {}'.format(content))
        message = Mail(
            from_email='mmistroni@gmail.com',
            #to_emails=self.recipients,
            subject='Edgar Daily Filings',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)
        logging.info('Sending message,..,,.')
        response = sg.send(message)
        logging.info(f'Message sent {response.status_code}, {response.body}, {response.headers}')


bucket_destination = 'gs://mm_dataflow_bucket/outputs/daily/edgar_{}.csv'
form_type = '13F-HR'

EDGAR_URL = 'https://www.sec.gov/Archives/edgar/daily-index/{year}/{quarter}/master.{current}.idx'

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')
        parser.add_argument('--fmprepkey')

def find_current_quarter(current_date):
    quarter_dictionary = {
        "QTR1": [1, 2, 3],
        "QTR2": [4, 5, 6],
        "QTR3": [7, 8, 9],
        "QTR4": [10, 11, 12]
    }
    current_month = current_date.month
    print('Fetching quarter for month:{}'.format(current_month))
    return [key for key, v in quarter_dictionary.items() if current_month in v][0]


def combine_data(elements):
    return (elements
            | 'Filtering Empty Tuples For Email' >> beam.Filter(lambda tpl: bool(tpl))
            | 'Removing Reporter And Filing Counts' >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2]))  #asofdate,period,cusip, num of shares,reporter
            | 'Combining similar' >> beam.combiners.Count.PerElement()
            | 'Groupring' >> beam.MapTuple( lambda tpl, count: (tpl[0], tpl[1], tpl[2], count))
            | 'Converting Cusip to Ticker' >> beam.MapTuple(lambda cob, period, word, count: [cob, period, word, cusip_to_ticker(word), count]))


def enhance_data(lines):
    result = (
            lines
            | 'parsing form 13 filing' >> beam.ParDo(ParseForm13F())
                )
    return result

def run_my_pipeline(source):
    return (
            source
            | 'readFromText' >> beam.ParDo(ReadRemote())
            | 'map to Str' >> beam.Map(lambda line: str(line))
    )


def filter_form_13hf(source):
    lines = (
            source
            | 'Filter only form 13HF' >> beam.Filter(
                    lambda row: len(row.split('|')) > 4 and form_type in row.split('|')[2])
            | 'Generating Proper file path' >> beam.Map(lambda row: (row.split('|')[3],
                                                                     '{}/{}'.format('https://www.sec.gov/Archives',
                                                                                    row.split('|')[4])))
            | 'replacing eol on form13' >> beam.Map(lambda p_tpl: (p_tpl[0], p_tpl[1][0:p_tpl[1].find('\\n')]))
    )
    return enhance_data(lines)

def send_email(lines, pipeline_options):
    email = (
            lines
            | 'Combining to get top 30' >> beam.CombineGlobally(EdgarCombineFn())
            | 'SendEmail' >> beam.ParDo(EmailSender(pipeline_options.recipients, pipeline_options.key))
    )

def write_to_bigquery(lines):
    # eachline has cob, period, word, cusip_to_ticker(word), count
    return (
            lines
            | 'Filtering Empty Tuples BQ' >> beam.Filter(lambda tpl: bool(tpl))
            | 'Map to BQ Compatible Dict BQ' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                       PERIODOFREPORT=tpl[1],
                                                                       CUSIP=tpl[2],
                                                                       COUNT=int(tpl[4]),
                                                                       TICKER=tpl[3],
                                                                       ))

    )

def reformat_for_custom_bucket(lines):
    # eachline has asofdate,periodofreport,cusip,shares,reporter
    return (
            lines
            | 'Filtering Empty Tuples for custom bucket' >> beam.Filter(lambda tpl: bool(tpl))
            | 'Mapping To Ticker' >> beam.Map(lambda tpl: (tpl[0], tpl[1], tpl[2], tpl[3], tpl[4], cusip_to_ticker(tpl[2]) ) )
            
            
    )

def find_current_day_url(sample):
    current_date = (datetime.now() - BDay(1))
    logging.info('Finding Edgar URL for {}  at:{}'.format(current_date, datetime.now().strftime('%Y-%m-%d')))
    current_quarter = find_current_quarter(current_date)
    current_year = find_current_year(current_date)
    master_idx_url = EDGAR_URL.format(quarter=current_quarter, year=current_year,
                                      current=current_date.strftime('%Y%m%d'))
    logging.info('Extracting data from:{}'.format(master_idx_url))
    return master_idx_url




def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    timeout_secs = 10800
    experiment_value = f"max_workflow_runtime_walltime_seconds={timeout_secs}"

    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(DebugOptions).add_experiment(experiment_value)

    form13_dest = 'gs://mm_dataflow_bucket/outputs/edgar_form13_{}'.format(date.today().strftime('%Y-%m-%d'))

    sink =  beam.io.WriteToBigQuery(
                    bigquery.TableReference(
                        projectId="datascience-projects",
                        datasetId='gcp_edgar',
                        tableId='form_13hf_daily_enhanced'),
                    schema='COB:STRING,PERIODOFREPORT:STRING,CUSIP:STRING,COUNT:INTEGER,TICKER:STRING,PRICE:FLOAT,REPORTER:STRING,SHARES_HELD:STRING',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    detailed_sink = beam.io.WriteToText(form13_dest, header='date,periodofreport,cusip,count,reporter,ticker',
                                                       num_shards=1)
    
    with beam.Pipeline(options=pipeline_options) as p:
        source = (p  | 'Startup' >> beam.Create(['start_token'])
                    |'Add current date' >> beam.Map(find_current_day_url)
                  )
        lines = run_my_pipeline(source)
        enhanced_data = filter_form_13hf(lines)
        logging.info('Next step')
        form113 = combine_data(enhanced_data)
        #logging.info('Now sendig meail....')
        #send_email(form113, pipeline_options)
        with_extra_info = write_to_bigquery(form113)
        with_extra_info | 'WRite to BQ' >> sink
        sink_data = reformat_for_custom_bucket(enhanced_data)
        sink_data | 'Writing to Bucket' >>  detailed_sink
