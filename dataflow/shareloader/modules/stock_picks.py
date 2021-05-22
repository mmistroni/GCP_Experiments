from __future__ import absolute_import
import requests

from datetime import date
from pandas.tseries.offsets import BDay
import logging
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from .bq_utils import get_table_schema, get_table_spec, map_to_bq_dict
from datetime import date
from .news_util import find_news_scores_for_ticker, df_to_dict, NewsEmailSender, combine_news, stringify_news, \
            enhance_with_price
from .bq_utils import get_news_table_schema, get_news_table_spec
import requests
from bs4 import BeautifulSoup
from datetime import datetime


class ParseRSS(beam.DoFn):
    def extract_data(self , item):
        cats = item.find_all('category')
        ticks = ','.join(c.get_text() for c in cats)
        print('TIKS ARE:{}'.format(ticks))
        title = item.title.get_text().lower()
        link = item.guid.get_text()
        itemDate = item.pubdate.get_text()
        try:
            dt = datetime.strptime(itemDate, '%a, %d %b %Y %H:%M:%S %z').date()
        except Exception as e:
            logging.info('Failed to parse date{}'.format(itemDate))
            dt = date.today()
        return (ticks, title, link, dt)

    def generate_initial_feeds(self ,feed_url):
        logging.info('Parsing URL:{}'.format(feed_url))
        page = requests.get(feed_url)
        soup = BeautifulSoup(page.content, 'lxml')
        items = soup.find_all('item')
        mapped = map(lambda i: self.extract_data(i), items)
        holder = []
        for item in mapped:
            #logging.info('Parsing:{}'.format(item))
            ticks = item[0]
            detail = item[1]
            link = item[2]
            pubDate = item[3]
            test_date = (date.today() - BDay(5)).date()
            if pubDate < test_date :
                logging.info('Skipping obsolete :{}({}) -  for {}'.format(pubDate, test_date, item))
                continue;
            date_str = pubDate.strftime('%Y-%m-%d')
            action = ''
            if feed_url.endswith('stock-ideas'):
                action = 'BUY' # long stoc ideas
            elif  'dividends' in feed_url:
                action = 'BUY(DIVIDEND)'
            else:
                if 'buy' in detail and 'sell' in detail:
                    action = 'INDECISE'
                elif 'buy' in detail:
                    action = 'BUY'
                elif 'sell' in detail:
                    action = 'SELL'
                else:
                    action = 'INDECISE'
            holder.append((date_str, ticks, detail, action, link))
        logging.info('returning:{}'.format(holder))
        return holder

    def process(self, element):
        logging.info('Processing element@{}'.format(element))
        return self.generate_initial_feeds(element)



def map_to_bq_dict(original_tuple):
    logging.info('... input dict is:{}'.format(original_tuple))
    return dict(     AS_OF_DATE=original_tuple[0],
                     TICKER=original_tuple[1],
                     HEADLINE=original_tuple[2],
                     ACTION=original_tuple[3],
                     LINK=original_tuple[4])

def write_data(data, sink):
    logging.info('Writing data....')
    return (data | 'MAP TO BigQuery' >> beam.Map(lambda t: map_to_bq_dict(t))
                 | sink)



def run_my_pipeline(source):
    return(source | 'Generate Initial Feeds' >> beam.Create(["https://seekingalpha.com/feed/stock-ideas/editors-picks",
                                                             "https://seekingalpha.com/feed/stock-ideas",
                                                             "https://seekingalpha.com/feed/dividends/editors-picks"
                                                             ])
                  | 'Getting feeds' >> beam.ParDo(ParseRSS())
                  | 'Mappibg to BQ Dict' >> beam.Map(lambda item: map_to_bq_dict(item))
                  | 'Write to BQ' >> beam.io.WriteToBigQuery(
                            bigquery.TableReference(
                                projectId="datascience-projects",
                                datasetId='gcp_shareloader',
                                tableId='seekingalpha_stock_pick'),
                            schema='AS_OF_DATE:STRING,TICKER:STRING,HEADLINE:STRING,ACTION:STRING,LINK:STRING',
                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))



def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        res = run_my_pipeline(p)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()