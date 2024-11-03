import unittest
from shareloader.modules.launcher import run_etoro_pipeline, run_test_pipeline,\
                                         StockSelectionCombineFn
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
            etoro = run_etoro_pipeline(p)

            final = ( (etoro, etoro)
                      | 'FlattenCombine all' >> beam.Flatten()
                      | 'Combine' >> beam.CombineGlobally(StockSelectionCombineFn())
                      | 'Output' >> self.debugSink
        )




if __name__ == '__main__':
    unittest.main()
