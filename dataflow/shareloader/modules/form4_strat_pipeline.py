import apache_beam as beam
import logging
from datetime import datetime, date
from .metrics import get_analyst_recommendations, get_historical_data_yahoo_2, get_date_ranges,\
                    get_return
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions
from pandas.tseries.offsets import BDay
from apache_beam.io import ReadFromText


def create_bigquery_ppln(p):
    cutoff_date = date(2020,10,1)
    logging.info('Cutoff is:{}'.format(cutoff_date))
    edgar_sql = """SELECT TICKER,COB 
FROM `datascience-projects.gcp_edgar.form_4_daily_historical`  
WHERE  PARSE_DATE("%F", COB) < PARSE_DATE("%F", '{cutoff}') GROUP BY TICKER, COB
  """.format(run_date=date.today().strftime('%Y-%m-%d'), cutoff=cutoff_date.strftime('%Y-%m-%d') )
    logging.info('executing SQL :{}'.format(edgar_sql))
    return (p | beam.io.Read(beam.io.BigQuerySource(query=edgar_sql, use_standard_sql=True))
                  |'Extractign only what we need..' >> beam.Map(
                lambda elem: (elem['TICKER'].strip(),
                              datetime.strptime(elem['COB'], '%Y-%m-%d').date()))
                  | 'Removing NA' >> beam.Filter(lambda tpl: tpl[0].lower() not in['na', 'none'])
                  | 'Adding cob Prices' >> beam.Map(lambda tpl: (tpl[0], tpl[1],
                                                                 get_historical_data_yahoo_2(tpl[0], '', tpl[1], tpl[1]) ))
                  | 'Filtering Volatile Stocks' >> beam.Filter(lambda tpl: tpl[2] >=10)
                  | 'Adding Return Date' >> beam.Map(lambda tpl:(tpl[0], tpl[1],
                                                                 (tpl[1] + BDay(7)).date(), tpl[2] ))
                  | 'Adding Log Return Date' >> beam.Map(lambda tpl:(tpl[0], tpl[1],
                                                                 tpl[2], tpl[3], get_return(tpl[0], tpl[1], tpl[2]) ))
                  | 'Mapping to CSV' >> beam.Map(lambda tpl: ','.join([str(e) for e in tpl]))
                )


def create_form4_vs_13_ppln(p):

    return (p | 'Read Source File' >> ReadFromText('gs://mm_dataflow_bucket/outputs/form13_hf_vs_form4.csv')
              |'Extractign only what we need..' >> beam.Map(lambda elem: elem.split(',')[1:])
              |'Filter out first row' >> beam.Filter(lambda arr: arr[1] != "F4_COB")
                  | 'Adding LogReturns' >> beam.Map(
                            lambda tpl: (tpl[0], tpl[3], tpl[4],
                                         get_return(tpl[0],
                                    datetime.strptime(tpl[3], '%Y-%m-%d').date(),
                                    datetime.strptime(tpl[4], '%Y-%m-%d').date())))
                |'Addign Original Price' >> beam.Map(
                        lambda tpl: (tpl[0], tpl[1], tpl[2], tpl[3], get_historical_data_yahoo_2(tpl[0], '', tpl[1], tpl[1])))

                | 'Mapping to CSV File' >> beam.Map(lambda tpl: ','.join([str(e) for e in tpl]))
                )



def write_data(data, sink):
    return  (
        data
           | sink
    )


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    destination = 'gs://mm_dataflow_bucket/outputs/form4_with_prices-{}.csv'.format(
        datetime.now().strftime('%Y%m%d-%H%M'))

    destination2 = 'gs://mm_dataflow_bucket/outputs/form4_vs_form13hf_with_prices-{}.csv'.format(
        datetime.now().strftime('%Y%m%d-%H%M'))

    logging.info('=== Starting. Writing to:{}'.format(destination))
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        #edgar_fills = create_bigquery_ppln(p)
        #sink =  beam.io.WriteToText(destination, header='ticker,cob,next_date,adj_close,return',
        #                                                num_shards=1)
        # result = ( edgar_fills  | sink)

        form4_vs_13 = create_form4_vs_13_ppln(p)
        sink2 = beam.io.WriteToText(destination2, header='ticker,cob,next_date,return,cob_price',
                                   num_shards=1)

        result2 = ( form4_vs_13  | sink2)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()