import unittest
import os
from apache_beam.testing.test_pipeline import TestPipeline
from datetime import date
from apache_beam.options.pipeline_options import PipelineOptions
from shareloader.modules.obb_utils import AsyncProcess, AsyncProcessSP500Multiples, ProcessHistorical
from shareloader.modules.launcher import StockSelectionCombineFn
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from datetime import datetime
import json
from shareloader.modules.launcher_pipelines import   run_etoro_pipeline
from shareloader.modules.launcher import  run_inference
from shareloader.modules.launcher_email import  send_email



import asyncio
import apache_beam as beam
import openai as openai

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
  def __init__(self, oai_key, llm_instructions):
      self.oai_key = oai_key
      self.llm_instructions = llm_instructions

  def load_model(self):
    """Initiate the Google Vision API client."""
    """Initiate the OAI API client."""
    client =  openai.OpenAI(
    # This is the default and can be omitted
        api_key=self.oai_key,
    )
    return client


  def run_inference(self, batch, model, inference):


    response = model.responses.create(
          model="gpt-4o",
          instructions=self.llm_instructions,
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
                 | "Inference" >> RunInference(model_handler=SampleOpenAIHandler(openai_key, "You are a coding assistant that talks like a pirate."))
                 | "Print image_url and annotation" >> beam.Map(print)
                 )

        assert openai_key is not None

    def test_anotherllm_on_bean(self):
        def combine_to_html_rows(elements):
            from functools import reduce
            combined = reduce(lambda acc, current: acc + current, elements, '')
            return combined

        key = os.environ['FMPREPKEY']
        openai_key = os.environ['OPENAI_API_KEY']

        def to_json_string(element):
            def datetime_converter(o):
                if isinstance(o, datetime):
                    return o.isoformat()  # Convert datetime to ISO 8601 string
                raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

            return json.dumps(element, default=datetime_converter)

        with TestPipeline(options=PipelineOptions()) as p:
            input2 = run_etoro_pipeline(p, key, 0.0001)

            template = '''
                            I will provide you a json string containing a list of stocks.
                            For each stock i will provide the following information
                            1 - prev_close: the previous close of the stock
                            2 - change: the change from yesterday
                            3 - ADX: the adx
                            4 - RSI : the RSI
                            5 - SMA20: the 20 day simple moving average
                            6 - SMA50: the 50 day simple moving average
                            7 - SMA200: the 200 day simple moving average
                            Based on that information, please find which stocks which are candidates to rise in next days.
                            Once you finish your analysis, please summarize your finding indicating, for each
                            stock what is your recommendation and why. 
                            At the end of the message, for the stocks  you recommend as buy or watch, you should generate
                            a json message with fields ticker, action (buy or watch) and an explanation.
                            OUtput this json String between tags <STARTJSON> and <ENDJSON>
                            Here is my json
            '''
            instructions = '''You are a powerful stock researcher that recommends stock that are candidate to buy.'''

            (input2 | "ToJson" >> beam.Map(to_json_string)
             | 'Combine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
             | 'anotheer map' >> beam.Map(lambda item: f'{template} \n {item}')

             | "Inference" >> RunInference(model_handler=SampleOpenAIHandler(openai_key,
                                                                               instructions))

             | "Print image_url and annotation" >> beam.Map(print)
             )
            # res = ( (input2, input2) |  "fmaprun" >> beam.Flatten()
            #        | 'tosink' >> self.debugSink)

    def test_anotherllm_on_bean2(self):
        key = os.environ['FMPREPKEY']
        openai_key = os.environ['OPENAI_API_KEY']

        with TestPipeline(options=PipelineOptions()) as p:
            input2 = run_etoro_pipeline(p, key, 0.0001)

            res = run_inference(input2, openai_key, beam.Map(print))
            res | "Print image_url and annotation" >> beam.Map(print)

            keyed_etoro = input2 | beam.Map(lambda element: (1, element))
            keyed_llm = res | 'mapping llm' >> beam.Map(lambda element: (1, element))
            combined = ({'collection1': keyed_etoro, 'collection2': [],
                         'collection3': keyed_llm}
                        | beam.CoGroupByKey())
            send_email(combined, 'abc')

if __name__ == '__main__':
    unittest.main()
