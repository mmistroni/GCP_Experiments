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
from typing import Any, Sequence
from google.genai import Client as GenAIClient, types
from collections import OrderedDict

from datetime import date
import os

from shareloader.modules.beam_inferences import run_gemini_pipeline

# Helper for iterating over collections.
from collections.abc import Iterable

# Python Package Version
from packaging.version import Version
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

# Import the core components for ML inference in Beam.
from apache_beam.ml.inference.base import RunInference, PredictionResult

# Import the specific model handler for Gemini on Vertex AI.
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string
from shareloader.modules.dftester_utils import to_json_string
from shareloader.modules.beam_inferences import PostProcessor
MODEL_NAME = "gemini-2.5-flash"

# --- Pipeline Configuration ---
# Number of threads to use for the local DirectRunner.
NUM_WORKERS = 1


import json
import asyncio
import httpx
import google.auth.transport.requests
from google.oauth2 import id_token
from typing import Any, Dict, Iterable, Sequence
from apache_beam.ml.inference.base import RemoteModelHandler, RunInference, PredictionResult

from typing import Any, Dict, Iterable, Sequence
import httpx
import google.auth.transport.requests
from google.oauth2 import id_token
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RemoteModelHandler

import json
import httpx
import google.auth.transport.requests
from google.oauth2 import id_token
from typing import Any, Dict, Optional

# Correct imports for the current SDK
from apache_beam.ml.inference.base import RemoteModelHandler, PredictionResult


class CloudRunAgentHandler(RemoteModelHandler):
    def __init__(self, app_url: str, app_name: str, user_id: str):
        # We initialize with a model_id to ensure the metrics namespace is populated
        self._model_id = f"CloudRun_{app_name}"
        super().__init__()
        self.app_url = app_url
        self.app_name = app_name
        self.user_id = user_id

    def get_metrics_namespace(self) -> str:
        # Explicitly return the model_id as the namespace
        return self._model_id

    def create_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=60.0)

    def _get_token(self) -> str:
        auth_req = google.auth.transport.requests.Request()
        return id_token.fetch_id_token(auth_req, self.app_url)

    async def request(
            self,
            item: str,
            client: httpx.AsyncClient,
            inference_args: Optional[Dict[str, Any]] = None
    ) -> PredictionResult:
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        run_data = {
            "app_name": self.app_name,
            "user_id": self.user_id,
            "session_id": f"beam_task_{hash(item)}",
            "new_message": {"role": "user", "parts": [{"text": item}]},
            "streaming": False
        }

        response = await client.post(f"{self.app_url}/run_sse", headers=headers, json=run_data)

        raw_text = response.text.strip()
        data_lines = [l for l in raw_text.split('\n') if l.strip().startswith("data:")]

        if data_lines:
            try:
                import json
                last_json = json.loads(data_lines[-1][5:])
                final_text = last_json.get('content', {}).get('parts', [{}])[0].get('text', '')
                return PredictionResult(example=item, inference=final_text)
            except Exception as e:
                return PredictionResult(example=item, inference=f"Error: {e}")

        return PredictionResult(example=item, inference="No Data")



class Check(beam.PTransform):
    def __init__(self, checker):
        self._checker = checker

    def expand(self, pcoll):
        print('Invoking sink....')
        assert_that(pcoll, self._checker)

def generate_with_test_instructions(
    model_name: str,
    batch: Sequence[str],
    model: GenAIClient,
    inference_args: dict[str, Any]):
  return model.models.generate_content(
      model=model_name,
      contents=batch,
      config=types.GenerateContentConfig(
        system_instruction='''You are a helpful assistant. For every question asked, 
                         provide the response in a JSON format between   <STARTJSON> and </ENDJSON> tags
                      '''
      ),
      **inference_args)



def run_test_gemini_pipeline(p, google_key, prompts=None, custom_instructions=None):
    model_handler = GeminiModelHandler(
        model_name=MODEL_NAME,
        request_fn=generate_with_test_instructions,
        api_key=google_key
    )

    read_prompts = None
    if not prompts:
        logging.info('Generating pipeline prompts')

        read_prompts = (p | "gemini xxToJson" >> beam.Map(to_json_string)
                        | 'gemini xxCombine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
                        | 'gemini xxanotheer map' >> beam.Map(lambda item: f'{item}')
                        )
    else:
        pipeline_prompts = prompts
        read_prompts = p | "GetPrompts" >> beam.Create(pipeline_prompts)

    # The core of our pipeline: apply the RunInference transform.
    # Beam will handle batching and parallel API calls.
    predictions = read_prompts | "RunInference" >> RunInference(model_handler)

    # Parse the results to get clean text.
    return predictions | "PostProcess" >> beam.ParDo(PostProcessor())


class TestBeamInferencesr(unittest.TestCase):


    def test_anotherllm_on_bean(self):
        prompts = [
            "What is 1+2? ",
             "How is the weather in NYC in July?",
             "Write a short, 3-line poem about a robot learning to paint."

        ]
        key = os.environ['GOOGLE_API_KEUY']
        sink = beam.Map(print)
        with TestPipeline(options=PipelineOptions()) as p:
            res = run_test_gemini_pipeline(p, key, prompts=prompts)
            (res | "extracting" >> beam.Map(lambda it: it[it.find('<STARTJSON')+ 11: it.find('</END')])
                | 'out' >> sink)

    def test_cloudagent(self):
        from apache_beam.ml.inference.base import RunInference, PredictionResult

        agent_handler = CloudRunAgentHandler(
            app_url="https://stock-agent-service-682143946483.us-central1.run.app",
            app_name="stock_agent",
            user_id="user_123"
        )
        sink = beam.Map(print)
        with TestPipeline(options=PipelineOptions()) as pipeline:
            (pipeline | 'Sourcinig prompt' >> beam.Create(
                ["Run a technical analysis for today's stock picks and give me your recommendations"])
             | 'ClouodagentRun' >> RunInference(agent_handler)
             | sink
             )





