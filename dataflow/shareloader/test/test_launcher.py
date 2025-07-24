import unittest
from shareloader.modules.launcher import run_etoro_pipeline, run_test_pipeline,\
                                         StockSelectionCombineFn, run_swingtrader_pipeline, \
                                            run_sector_performance, FinvizCombineFn, send_email, create_row
from shareloader.modules.launcher_pipelines import run_extra_pipeline, run_newhigh_pipeline
from shareloader.modules.finviz_utils import  overnight_return
from pprint import pprint
import os
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import  BytesIO



class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_etoro(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            etoro = run_etoro_pipeline(p, key ,tolerance=0.001)

            #tester = run_test_pipeline(p, key)

            final = ( (etoro, etoro)
                      | 'FlattenCombine all' >> beam.Flatten()
                      | 'Combine' >> beam.CombineGlobally(StockSelectionCombineFn())
                      | 'Output' >> self.debugSink
        )

    
    def test_extras(self):
        key = os.environ['FMPREPKEY']

        with TestPipeline(options=PipelineOptions()) as p:
            result  = run_extra_pipeline(p, key, 0.0001)
            result | self.debugSink

    def test_newhigh(self):
        key = os.environ['FMPREPKEY']

        with TestPipeline(options=PipelineOptions()) as p:
            result  = run_newhigh_pipeline(p, key, 0.0001)
            result | self.debugSink


    def test_swingtrader(self):
        from shareloader.modules.obb_utils import AsyncProcess, create_bigquery_ppln, ProcessHistorical
        from datetime import date
        key = os.environ['FMPREPKEY']

        def combine_tickers(input):
            return ','.join([i for i in input if bool(i)])

        with TestPipeline(options=PipelineOptions()) as p:
            (p | 'Sourcinig overnight' >> beam.Create(['AMZN', 'AAPL'])#overnight_return())
                    #| 'Overnight returs' >> beam.Map(lambda d: d['Ticker'])
                    | 'Filtering' >> beam.Filter(lambda tick: tick is not None and '.' not in tick and '-' not in tick)
                    | 'Combine all tickers' >> beam.CombineGlobally(combine_tickers)
                    | 'Plus500YFRun' >> beam.ParDo(AsyncProcess({'key': key}, date.today(), price_change=0.0001))
                     |  self.debugSink
                    )

    def test_finviz_with_combiner(self):
        key = os.environ['FMPREPKEY']
        class ProcessStringFn(beam.DoFn):
            def process(self, element):
                # Process the string here
                yield element  # or yield processed_element

        with TestPipeline(options=PipelineOptions()) as p:
            etoro = run_etoro_pipeline(p, key)

            final = ((etoro, etoro)
                     | 'FlattenCombine all' >> beam.Flatten()
                     | 'Combine' >> beam.CombineGlobally(StockSelectionCombineFn())
                     )


            finviz = run_sector_performance(p) 
            premarket_results =  (finviz | 'mapping ' >> beam.Map(create_row)
                                         | beam.CombineGlobally(FinvizCombineFn())
                                         #| 'extracting' >> beam.ParDo(ProcessStringFn())
                                         #| 'to sink' >> self.debugSink
                                  )

            keyed_pcoll = final | beam.Map(lambda element: (1, element))
            keyed_pcoll2 = premarket_results | beam.Map(lambda element: (1, element))

            combined = ({'collection1': keyed_pcoll, 'collection2': keyed_pcoll2}
                        | beam.CoGroupByKey())

            send_email(combined,  os.environ['FMPREPKEY'])

    def test_get_historical(self):
        import pandas as pd
        from  scipy import stats
        key = os.environ['FMPREPKEY']
        ticker = 'AAPL'
        hist_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?apikey={}'.format(ticker, key)
        data = requests.get(hist_url).json().get('historical')
        df=  pd.DataFrame(data=data)[0:30][::-1]
        
        df = df.set_index('date')

        # Define window sizes for returns
        one_month = 21  # Approx. 21 trading days in a month
        three_months = 63  # Approx. 63 trading days in 3 months
        six_months = 126  # Approx. 126 trading days in 6 months
        one_year = 252  # Approx. 252 trading days in a year

        # Calculate returns (percentage change)
        df['1_Month_Return'] = (df['adjClose'] / df['adjClose'].shift(one_month) - 1) * 100
        df['3_Month_Return'] = (df['adjClose'] / df['adjClose'].shift(three_months) - 1) * 100
        df['6_Month_Return'] = (df['adjClose'] / df['adjClose'].shift(six_months) - 1) * 100
        df['1_Year_Return'] = (df['adjClose'] / df['adjClose'].shift(one_year) - 1) * 100
        
        df = df[['1_Month_Return', '3_Month_Return', '6_Month_Return', '1_Year_Return' ]]

        time_periods = ['1_Month_Return', '3_Month_Return', '6_Month_Return', '1_Year_Return' ]
        for row in df.index:
            for time_period in time_periods:
                df.loc[row, f'{time_period} Return Percentile'] = stats.percentileofscore(df[f'{time_period}'],
                                                                                           df.loc[row, f'{time_period}'])/100


        print(df.tail(3).T)

    



if __name__ == '__main__':
    unittest.main()
