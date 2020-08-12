import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, \
            find_current_year, EdgarCombineFn
from datetime import date, datetime
from pandas.tseries.offsets import BDay
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
from apache_beam.io.gcp.internal.clients import bigquery

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
        template = "<html><body><table><th>Cusip</th><th>Ticker</th><th>Counts</th>{}</table></body></html>"
        content = template.format(element)
        print('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud@mmistroni.com',
            #to_emails=self.recipients,
            subject='Edgar Daily Filings',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        print(response.status_code, response.body, response.headers)



bucket_destination = 'gs://mm_dataflow_bucket/outputs/daily/edgar_{}.csv'
form_type = '13F-HR'

EDGAR_URL = 'https://www.sec.gov/Archives/edgar/daily-index/{year}/{quarter}/master.{current}.idx'
def get_edgar_table_schema():
  edgar_table_schema = 'COB:STRING,CUSIP:STRING,COUNT:INTEGER,TICKER:STRING'
  return edgar_table_schema

def get_edgar_daily_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_13hf_daily')

class XyzOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--recipients', default='mmistroni@gmail.com')
        parser.add_argument('--key')

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

def enhance_data(lines):
    result = (
            lines
            | 'parsing edgar filing' >> beam.ParDo(ParseForm13F())
            | 'Combining similar' >> beam.combiners.Count.PerElement()
            | 'Groupring' >> beam.MapTuple(lambda cob, word, count: (cob, word, count))
            | 'Adding Cusip' >> beam.MapTuple(lambda cob, word, count: [cob, word, cusip_to_ticker(word), count])
    )
    return result

def run_my_pipeline(source):
    lines = (
            source
            | 'readFromText' >> beam.ParDo(ReadRemote())
            | 'map to Str' >> beam.Map(lambda line: str(line))
            | 'Filter only form 13HF' >> beam.Filter(
                    lambda row: len(row.split('|')) > 4 and form_type in row.split('|')[2])
            | 'Generating Proper file path' >> beam.Map(lambda row: (row.split('|')[3],
                                                                     '{}/{}'.format('https://www.sec.gov/Archives',
                                                                                    row.split('|')[4])))
            | 'replacing eol' >> beam.Map(lambda p_tpl: (p_tpl[0], p_tpl[1][0:p_tpl[1].find('\\n')]))
    )
    return enhance_data(lines)

def send_email(lines, pipeline_options):
    email = (
            lines
            | 'Combining to get top 30' >> beam.CombineGlobally(EdgarCombineFn())
            | 'SendEmail' >> beam.ParDo(EmailSender(pipeline_options.recipients, pipeline_options.key))
    )

def write_to_bigquery(lines):
    big_query = (
            lines
            | 'Map to BQ Compatible Dict' >> beam.Map(lambda tpl: dict(COB=tpl[0],
                                                                       CUSIP=tpl[1],
                                                                       TICKER=tpl[2],
                                                                       COUNT=tpl[3]))
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        get_edgar_daily_table_spec(),
        schema=get_edgar_table_schema(),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    current_date = date.today() - BDay(1)
    current_quarter = find_current_quarter(current_date)
    current_year = find_current_year(current_date)
    master_idx_url = EDGAR_URL.format(quarter=current_quarter, year=current_year,
                                    current=current_date.strftime('%Y%m%d'))
    logging.info('Extracting data from:{}'.format(master_idx_url))
    destination =   bucket_destination.format(current_date.strftime('%Y%m%d'))
    logging.info('Writing to:{}'.format(destination))

    with beam.Pipeline(options=pipeline_options) as p4:
        source = beam.Create([master_idx_url])
        lines = run_my_pipeline(source)
        send_email(lines, pipeline_options)
        write_to_bigquery(lines)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
