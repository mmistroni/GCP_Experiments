import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, \
            find_current_year, EdgarCombineFn, ParseForm4
from datetime import date, datetime
from pandas.tseries.offsets import BDay
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from apache_beam.io.gcp.internal.clients import bigquery
from .edgar_utils import  get_edgar_table_schema, get_edgar_table_schema_form4,\
            get_edgar_daily_table_spec, get_edgar_daily_table_spec_form4

class EmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(',')
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
        logging.info('Attepmting to send emamil to:{}'.format(self.recipients))
        template = "<html><body><table border='1' cellspacing='0' cellpadding='0' align='center'><th>Cusip</th><th>Ticker</th><th>Counts</th>{}</table></body></html>"
        content = template.format(element)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud@mmistroni.com',
            #to_emails=self.recipients,
            subject='Edgar Form4 Daily Filings (Insider Trading)',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        logging.info(response.status_code, response.body, response.headers)

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

def enhance_form_4(lines):
    result = (
            lines
            | 'parsing form 4 filing' >> beam.ParDo(ParseForm4())
            | 'Combining all ' >> beam.CombinePerKey(sum)
            | 'Filtering out blanks' >> beam.Filter(lambda tpl: tpl[0][1] != '' and tpl[0][1] != 'N/A' )
            | 'Mapping to tuple to be in line with mail templates' >>  beam.Map(
                                            lambda tpl: [tpl[0][0], '' ,tpl[0][1], tpl[1]])
    )
    return result

def run_my_pipeline(source):

    return (
            source
            | 'readFromText' >> beam.ParDo(ReadRemote())
            | 'map to Str' >> beam.Map(lambda line: str(line))
    )

def filter_form_4(source):
    return (
            source
            | 'Filter only form 4' >> beam.Filter(
                    lambda row: len(row.split('|')) > 4 and '4' in row.split('|')[2])
            | 'Generating form 4 file path' >> beam.Map(lambda row: (row.split('|')[3],
                                                                     '{}/{}'.format('https://www.sec.gov/Archives',
                                                                                    row.split('|')[4])))
            | 'replacing eol on form4' >> beam.Map(lambda p_tpl: (p_tpl[0], p_tpl[1][0:p_tpl[1].find('\\n')]))
    )

def send_email(lines, pipeline_options):
    email = (
            lines
            | 'Combining to get top 30' >> beam.CombineGlobally(EdgarCombineFn())
            | 'SendEmail' >> beam.ParDo(EmailSender(pipeline_options.recipients, pipeline_options.key))
    )

def write_to_form4_bq(lines):
    big_query = (
            lines
            | 'Map to BQ FORM4 Dict' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                       TICKER=tpl[2],
                                                                  COUNT=tpl[3]))
            | 'Write to BigQuery F4' >> beam.io.WriteToBigQuery(
        get_edgar_daily_table_spec_form4(),
        schema=get_edgar_table_schema_form4(),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.info('starting pipeline..')
    with beam.Pipeline(options=pipeline_options) as p:
        source = (p  | 'Startup' >> beam.Create(['start_token'])
                     |'Add current date' >> beam.Map(find_current_day_url)
                  )

        lines = run_my_pipeline(source)
        form4 = filter_form_4(lines)
        enhanced_data = enhance_form_4(form4)
        logging.info('Now sendig meail....')
        send_email(enhanced_data, pipeline_options)
        write_to_form4_bq(enhanced_data)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
