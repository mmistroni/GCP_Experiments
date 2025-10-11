import unittest

import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from shareloader.modules.sectors_pipelines import run_sector_loader_pipeline, run_sector_loader_finviz, run_pipelines
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from shareloader.modules.sectors_utils import SectorRankGenerator, get_sector_rankings, SectorsEmailSender, \
    get_finviz_performance, fetch_index_data
import yfinance as yf

from collections import OrderedDict

from datetime import date
import os
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Import the core components for ML inference in Beam.
from apache_beam.ml.inference.base import RunInference, PredictionResult
from shareloader.modules.beam_inferences import run_gemini_pipeline

# Import the specific model handler for Gemini on Vertex AI.
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string

from shareloader.modules.beam_inferences import run_gemini_pipeline

# Helper for iterating over collections.
from collections.abc import Iterable

# Python Package Version
from packaging.version import Version
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Import the core components for ML inference in Beam.
from apache_beam.ml.inference.base import RunInference, PredictionResult

# Import the specific model handler for Gemini on Vertex AI.
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string
from apache_beam.ml.inference.base import ModelHandler

# Helper for iterating over collections.
from collections.abc import Iterable

MODEL_NAME = "gemini-2.5-flash"

# --- Pipeline Configuration ---
# Number of threads to use for the local DirectRunner.
NUM_WORKERS = 1

class Check(beam.PTransform):
    def __init__(self, checker):
        self._checker = checker

    def expand(self, pcoll):
        print('Invoking sink....')
        assert_that(pcoll, self._checker)

class PostProcessor(beam.DoFn):
  """Parses the PredictionResult to extract a human-readable string."""
  def process(self, element: PredictionResult) -> Iterable[str]:
    """
    Extracts the generated text from the Gemini API response.

    The inference result from GeminiModelHandler is a tuple containing:
    ('sdk_http_response', [<google.cloud.aiplatform_v1.types.GenerateContentResponse>])

    We navigate this structure to get the final text.
    """
    # The original input prompt is stored in `element.example`
    input_prompt = element.example

    # The API response is in `element.inference`
    # Path to text: response -> candidates -> content -> parts -> text
    gemini_inference = element.inference
    print(f'element.inference is {gemini_inference}')
    print(gemini_inference[1])

    gemini_response = gemini_inference[1][0]
    print(f'Gemini Response:{gemini_response}')
    print(f'Gemini Resjponse Content:{gemini_response.content}')
    print(f'Gemini REsjponse Content Part:{gemini_response.content.parts}')
    print(f'Gemini REsjponse Content Part.0:{gemini_response.content.parts[0]}')
    print(f'Text:{gemini_response.content.parts[0].text}')
    # Only supported for genai package 1.21.1 or earlier
    output_text = gemini_response.content.parts[0].text

    # Yield a formatted string for printing
    yield f"Input:\n{input_prompt}\n\nOutput:\n{output_text.strip()}\n"
def run_pipeline(prompts, model_name, num_workers):
    """Constructs and runs the Beam pipeline for Gemini inference."""
    # 1. Define the Model Handler
    # This object knows how to communicate with the Vertex AI Gemini API.
    # `generate_from_string` is a helper that formats a simple string prompt
    # into the required API request format.
    model_handler = GeminiModelHandler(
        model_name=model_name,
        request_fn=generate_from_string,
        api_key=os.environ['GOOGLE_API_KEUY']
    )

    # 2. Set Pipeline Options
    # For local execution, we use the DirectRunner.
    # `direct_num_workers` controls the number of parallel threads.
    pipeline_options = PipelineOptions(
        direct_num_workers=num_workers
    )

    # 3. Construct the Pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Create a PCollection from our list of prompts.
        read_prompts = pipeline | "GetPrompts" >> beam.Create(prompts)

        # The core of our pipeline: apply the RunInference transform.
        # Beam will handle batching and parallel API calls.
        predictions = read_prompts | "RunInference" >> RunInference(model_handler)

        processed = predictions | "PostProcess" >> beam.ParDo(PostProcessor())

        # Parse the results to get clean text.
        processed | "PrintOutput" >> beam.Map(print)

    print("\n--- Pipeline finished ---")


class SampleGeminiAIHandler(ModelHandler):
  """DoFn that accepts a batch of images as bytearray
  and sends that batch to the Cloud Vision API for remote inference"""
  def __init__(self, oai_key, llm_instructions):
      self.oai_key = oai_key
      self.llm_instructions = llm_instructions

  def load_model(self):
    """Initiate the Google Vision API client."""
    """Initiate the OAI API client."""
    client =  GeminiModelHandler(
        model_name="gemini-2.5-flash",
        request_fn=generate_from_string,
        api_key=os.environ['GOOGLE_API_KEUY']
    )
    return client


  def run_inference(self, batch, model, inference):


    response = model.responses.create(
          model="gemini-2.5-flash",
          input=batch[0],
      )
    return [response.output_text]



class TestBeamInferencesr(unittest.TestCase):



    def test_anotherllm_on_bean(self):
        prompts = [
            "What is 1+2? Provide the response in a Json format following this schema: {'question': <prompt>, 'answer': <your_answer>}",
             "How is the weather in NYC in July?",
             "Write a short, 3-line poem about a robot learning to paint."

        ]
        run_pipeline(prompts, MODEL_NAME, NUM_WORKERS)


