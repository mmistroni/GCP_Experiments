import unittest
from shareloader.modules.launcher import run_etoro_pipeline, run_test_pipeline,\
                                         StockSelectionCombineFn, run_swingtrader_pipeline, \
                                            run_sector_performance, FinvizCombineFn, send_email, create_row
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
            etoro = run_etoro_pipeline(p, key)

            final = ( (etoro, etoro)
                      | 'FlattenCombine all' >> beam.Flatten()
                      | 'Combine' >> beam.CombineGlobally(StockSelectionCombineFn())
                      | 'Output' >> self.debugSink
        )

    
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
                    | 'Plus500YFRun' >> beam.ParDo(AsyncProcess({'key': key}, date.today(), price_change=0.07))
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


if __name__ == '__main__':
    unittest.main()
