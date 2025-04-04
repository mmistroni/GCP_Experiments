import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim, get_leaps,\
                                            get_graham_defensive, get_graham_enterprise,\
                                            get_extra_watchlist, get_new_highs, FinvizLoader, \
                                            get_high_low
from pprint import pprint
import os
from shareloader.modules.superperf_metrics import get_dividend_paid
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from shareloader.modules.obb_utils import AsyncProcess, AsyncProcessSP500Multiples, ProcessHistorical
from shareloader.modules.launcher import StockSelectionCombineFn
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference



import asyncio
import apache_beam as beam
from openai import OpenAI

class OpenAIClient:

    def __init__(self, openai_key):
        self.oai_key =  openai_key

    def process_request(self, request):
        try:
            openai.api_key = self.oai_key  # Set the API key for this call
            response = openai.Completion.create(
                engine="text-davinci-003",  # Or another available engine
                prompt=request,
                max_tokens=150,
            )
            return response.choices[0].text.strip()
        except Exception as e:
            return f"Error: {e}"


class SampleOpenAIHandler(ModelHandler):
  """DoFn that accepts a batch of images as bytearray
  and sends that batch to the Cloud Vision API for remote inference"""
  def __init__(self, oai_key):
      self.oai_key = oai_key

  def load_model(self):
    """Initiate the Google Vision API client."""
    """Initiate the OAI API client."""
    client = client = OpenAI(
    # This is the default and can be omitted
        api_key=self.oai_key,
    )
    return client


  def run_inference(self, batch, model, inference):


    response = model.responses.create(
          model="gpt-4o",
          instructions="You are a coding assistant that talks like a pirate.",
          input=batch[0],
      )
    return [response.output_text]



class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_sample_pipeline(self):
        credentials = {'key' : os.environ['FMPREPKEY']}
        cob = date(2024, 10, 4)
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['EBAY'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcess(credentials, cob ,price_change=0.00001))
                     | 'combining' >> beam.CombineGlobally(StockSelectionCombineFn())
                     | self.debugSink
                     )

    def test_sample_pipeline2(self):
        credentials = {'fmp_api_key' : os.environ['FMPREPKEY']}
        cob = date(2024, 10, 4)
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['shiller_pe_month', 'pe_month', 'earnings_growth_year'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcessSP500Multiples(credentials))
                     | 'Combine sp' >> beam.CombineGlobally(lambda x: '<br><br>'.join(x))
                     | self.debugSink
                     )

    def test_sample_pipeline3(self):
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create(['AAPL,NVDA,AMZN,T'])
                     | 'Run Loader' >> beam.ParDo(ProcessHistorical(os.environ['FMPREPKEY'], date.today()))
                     | self.debugSink
                     )
    def test_combine_pipeline(self):
        credentials = {'fmp_api_key' : os.environ['FMPREPKEY']}
        cob = date(2024, 10, 4)
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL,NVDA,AMZN,T'])
                     | 'Run Loader' >> beam.ParDo(AsyncProcess(credentials, cob ,price_change=0.001))
                     | 'maps' >> beam.Map(lambda d: d['ticker'])
                     | 'combiining' >> beam.CombineGlobally(lambda x: ','.join(x))
                     | 'Run LoaderHist' >> beam.ParDo(ProcessHistorical(os.environ['FMPREPKEY'], date.today()))
                     | self.debugSink)


    def test_llm_on_beamn(self):
        openai_key = os.environ['OPENAI_API_KEY']

        with beam.Pipeline() as pipeline:
            _ = (pipeline | "Create inputs" >> beam.Create(['What is one plus one?'])
                 | "Inference" >> RunInference(model_handler=SampleOpenAIHandler(openai_key))
                 | "Print image_url and annotation" >> beam.Map(print)
                 )

        assert openai_key is not None


if __name__ == '__main__':
    unittest.main()
