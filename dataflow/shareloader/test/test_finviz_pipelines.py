import unittest
from shareloader.modules.finviz_pipelines import graham_defensive_pipeline, extra_watchlist_pipeline,\
                                                universe_pipeline


from pprint import pprint
import os
from shareloader.modules.obb_processes import AsyncProcessFinvizTester
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from openbb_yfinance.models.equity_quote import YFinanceEquityQuoteFetcher
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import  BytesIO
import asyncio
import logging
from openbb_finviz.models.equity_screener import FinvizEquityScreenerFetcher


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_get_graham_defensive(self):
        with TestPipeline(options=PipelineOptions()) as p:
            graham_defensive_pipeline(p) | self.debugSink

    def test_get_extra_watchlist(self):
        with TestPipeline(options=PipelineOptions()) as p:
            extra_watchlist_pipeline(p) | self.debugSink

    def test_universe(self):
        with TestPipeline(options=PipelineOptions()) as p:
            universe_pipeline(p)| self.debugSink

    def test_combo(self):
        with TestPipeline(options=PipelineOptions()) as p:
            one = graham_defensive_pipeline(p)
            two = universe_pipeline(p)
            ( (one, two) | 'FlattenCombine all' >> beam.Flatten()
                         | self.debugSink
              )

if __name__ == '__main__':
    unittest.main()
