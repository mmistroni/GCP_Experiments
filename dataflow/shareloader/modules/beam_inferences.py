import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# Import the core components for ML inference in Beam.
from apache_beam.ml.inference.base import RunInference, PredictionResult
# Import the specific model handler for Gemini on Vertex AI.
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string
# Helper for iterating over collections.
from collections.abc import Iterable
from apache_beam.ml.inference.base import RunInference
from shareloader.modules.dftester_utils import to_json_string
from google.genai import types
from google.genai import Client as GenAIClient
import logging
from typing import Any, Sequence, Optional, Dict
import httpx
import google.auth.transport.requests
from google.oauth2 import id_token
from datetime import datetime
import random
from apache_beam.ml.inference.base import RemoteModelHandler, PredictionResult
import  logging
import apache_beam as beam
import json
import logging
from typing import Iterable



# Python Packag'gemini-2.0-flash-001'e Version
MODEL_NAME = "gemini-2.5-flash" #"gemini-2.5-flash"

# --- Pipeline Configuration ---
# Number of threads to use for the local DirectRunner.
NUM_WORKERS = 1
SYSTEM_INSTRUCTION_TEXT = (
    "You are a helpful and concise assistant. "
    "Your should provide response in Json Format"
)

TEMPLATE = '''
**CRITICAL INSTRUCTION: READ FIRST.**

You are a powerful stock researcher and statistician. You will be provided a JSON string containing a list of stock data.

**IF AND ONLY IF** the provided JSON string represents an **empty list or array** (i.e., it contains zero stock entries), you **MUST IMMEDIATELY STOP** and output only the following exact, single line of text:
**Cannot complete analysis as no stocks provided.**

**OTHERWISE (If the JSON contains stock data):**
You are to recommend stocks that are candidates to buy or to sell based on the following information provided for each stock:
1 - prev_close: the previous close of the stock
2 - change: the change from yesterday
3 - ADX: the adx
4 - RSI : the RSI
5 - SMA20: the 20 day simple moving average
6 - SMA50: the 50 day simple moving average
7 - SMA200: the 200 day simple moving average
8 - slope: the slope of linear regression for past 30 days.
9 - prev_obv: on balance volume from previous day
10 - current_obv: on balance volume for the current day
11 - previous_cmf: previous day Chaikin Money Flow (CMF) (20 days)
12 - current_cmf: current day Chaikin Money Flow (CMF) (20 days)
13 - obv_historical: on balance volumes for the last 20 days
14 - cmf_historical: cmf values for past 20 days
15 - trend_velocity_gap: difference between ema_8 and ema_21
16 - choppiness: choppiness indicator
17  -demarker: demarker indicator
18 - spx_choppiness: choppiness indicator  for  spx
**Analysis Tasks:**
1. Based on the data, identify which stocks are candidates to **rise** (BUY/WATCH) in the next days.
2. For any stock that has dropped more than 10%, evaluate if it is worth to **short sell** (SELL) them based on the same criteria.

**Final Output:**
Summarize your findings, indicating the recommendation (buy, watch, or sell) and the reason for each stock.
At the end of the message, you **must** generate a JSON message for all recommended stocks.
The JSON must have fields `ticker`, `action` (buy, watch, or sell), and `explanation`.
The JSON string should be written between a <STARTJSON> and <ENDJSON> tags.
'''


CONGRESS_TRADES_TEMPLATE = '''
ACT AS A QUANTITATIVE MARKET RISK SPECIALIST. Your task is to perform a rapid forensic analysis on a dataset of Congress trades provided in JSON format. The primary goal is to detect and quantify any abnormal concentration or market sentiment shifts based on asset class.
DATA INPUT: You will receive the trade data as a JSON object, where the key field for analysis is the stock 'TICKER'.
REQUIRED ANALYSIS STEPS:
Data Enrichment: Find the GICS Sector for each unique ticker.
Sentiment Classification: Classify all trades into two sentiment categories: 'Bullish/Cyclical' (Sectors associated with economic growth) and 'Defensive/Non-Cyclical' (Sectors that hold value during downturns).
Concentration Detection:
Calculate the percentage of total trades belonging to the single most active sector.
State how many times greater this percentage is compared to a uniform distribution across all sectors.
Sentiment Shift Quantification: Calculate the exact overall percentage split of trades between the 'Bullish' and 'Defensive' categories.
OUTPUT: Provide only the numerical metrics for the concentration and sentiment split, followed by a succinct professional summary detailing any detected unusual concentration or a significant sentiment shift.
'''

# --- Constants for PCollection Tags ---
NON_EMPTY_PROMPTS_TAG = 'non_empty_prompts'
EMPTY_PROMPT_TAG = 'empty_prompt'
# -------------------------------------

#

def generate_with_instructions(
    model_name: str,
    batch: Sequence[str],
    model: GenAIClient,
    inference_args: dict[str, Any]):
  return model.models.generate_content(
      model=model_name, 
      contents=batch,
      config=types.GenerateContentConfig(
        system_instruction=TEMPLATE
      ), 
      **inference_args)

def generate_for_congress(
    model_name: str,
    batch: Sequence[str],
    model: GenAIClient,
    inference_args: dict[str, Any]):
  return model.models.generate_content(
      model=model_name,
      contents=batch,
      config=types.GenerateContentConfig(
        system_instruction=CONGRESS_TRADES_TEMPLATE
      ),
      **inference_args)




def get_default_model_handler():
    pass


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
        
        try:
            gemini_inference = element.inference
            logging.info(f'element.inference is {gemini_inference}')

            logging.info(gemini_inference[1])
            
            gemini_response = gemini_inference[1][0]
            logging.info(f'Gemini Response:{gemini_response}')
            logging.info(f'Gemini Resjponse Content:{gemini_response.content}')
            logging.info(f'Gemini REsjponse Content Part:{gemini_response.content.parts}')
            logging.info(f'Gemini REsjponse Content Part.0:{gemini_response.content.parts[0]}')
            logging.info(f'Text:{gemini_response.content.parts[0].text}')
            # Only supported for genai package 1.21.1 or earlier
            # Only supported for genai package 1.21.1 or earlier
            output_text = gemini_response.content.parts[0].text
            # Yield a formatted string for printing
            yield f"Input:\n{input_prompt}\n\nOutput:\n{output_text.strip()}\n"
            
        except Exception as e:
            logging.error(f"Error processing element: {e}")
            yield f"Input:\n{input_prompt}\n\nOutput:\nError processing response.\n"


# 1. New DoFn to route based on prompt content
class ContentRouter(beam.DoFn):
    """
    Routes the single combined prompt string based on its content:
    If it's empty JSON (e.g., '[]'), it goes to the empty tag.
    Otherwise, it goes to the main tag.
    """

    def process(self, element):
        # Check if the combined string is empty or represents an empty list
        prompt_content = element.strip()
        if not prompt_content or prompt_content == '[]':
            # Route to the 'empty' path
            yield beam.pvalue.TaggedOutput(EMPTY_PROMPT_TAG, 'No data fetched from query')
        else:
            # Route to the 'non-empty' path for inference
            yield element


def run_gemini_pipeline(p, google_key, prompts=None):
    """
    Runs the Gemini pipeline with conditional handling for empty input data.
    """
    model_handler = GeminiModelHandler(
        model_name=MODEL_NAME,
        request_fn=generate_with_instructions,
        api_key=google_key
    )

    read_prompts = None
    if not prompts:
        logging.info('Generating pipeline prompts')
        # This creates the PCollection from the initial pipeline input
        # Note: CombineGlobally produces 0 elements if input is empty, or 1 element (the combined string)
        read_prompts = (p | "gemini xxToJson" >> beam.Map(to_json_string)
                        | 'gemini xxCombine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
                        | 'gemini xxanotheer map' >> beam.Map(lambda item: f'{item}')
                        )
    else:
        pipeline_prompts = prompts
        # This creates the PCollection from the Python list
        read_prompts = p | "GetPrompts" >> beam.Create(pipeline_prompts)

    # --- BEAM CONTENT-BASED ROUTING ---

    # 1. Route the single combined prompt element based on its content
    # This transform returns a dictionary of PCollections indexed by their tags
    router_results = read_prompts | 'ContentRouter' >> beam.ParDo(ContentRouter()).with_outputs(
        EMPTY_PROMPT_TAG, main=NON_EMPTY_PROMPTS_TAG)

    # Separate the PCollections based on the routing tags
    non_empty_prompts = router_results[NON_EMPTY_PROMPTS_TAG]
    no_data_results = router_results[EMPTY_PROMPT_TAG]

    # 2. Path 1: The Standard Analysis Flow (only runs on non-empty content)

    # Run Inference and Post-Process the results
    post_processed_results = (non_empty_prompts
                              | "RunInference" >> RunInference(model_handler)
                              | "PostProcess" >> beam.ParDo(PostProcessor())
                              )

    # DEBUG STEP: Add the logging step here as requested
    debug = post_processed_results | 'Debugging inference output' >> beam.Map(logging.info)

    # Clean the output to remove the 'Output: ' prefix
    analysis_results = post_processed_results | "CleanOutput" >> beam.Map(lambda it: it[it.find('Output:') + 7:])

    # 3. Path 2: The 'No Data' Handler Flow (contains the 'No data' string)

    # 4. Flatten the two PCollections together
    # If the prompt contained data, analysis_results has the output, and no_data_results is empty.
    # If the prompt was empty JSON ('[]'), analysis_results is empty, and no_data_results contains 'No data'.
    return (analysis_results, no_data_results) | 'FinalFlatten' >> beam.Flatten()

def run_gemini_congress_pipeline(p, google_key):
    model_handler = GeminiModelHandler(
        model_name=MODEL_NAME,
        request_fn=generate_for_congress,
        api_key=google_key
    )

    logging.info('Generating pipeline prompts')

    read_prompts = (p | "gemini xxToJson" >> beam.Map(to_json_string)
                    | 'gemini xxCombine jsons' >> beam.CombineGlobally(lambda elements: "".join(elements))
                    | 'gemini xxanotheer map' >> beam.Map(lambda item: f'{item}')
                    )
    predictions = read_prompts | "RunInference" >> RunInference(model_handler)

    # Parse the results to get clean text.
    llm_response = (predictions | "PostProcess" >> beam.ParDo(PostProcessor())
                    )

    debug = llm_response | 'Debugging inference output' >> beam.Map(logging.info)


import json
import re
import logging

class CloudRunPostProcessor(beam.DoFn):
    def process(self, element: Any) -> Iterable[str]:
        logging.info(f'===== Received Element Type: {type(element)}')
        logging.info(f'===== ELEMENT IS \n {element}')

        raw_text = None

        # Handle PredictionResult
        if hasattr(element, 'inference'):
            raw_text = element.inference
            if raw_text is None:
                logging.warning("inference field is None")
                yield "ERROR: Inference result was None"
                return
        # Handle direct string
        elif isinstance(element, str):
            raw_text = element
        # Skip lists (assume they are examples/prompt inputs)
        elif isinstance(element, list):
            logging.debug("Skipping list input (likely prompt/example)")
            # Don't return here — just don't process
            return  # It's safe to skip non-results
        else:
            logging.error(f"Unexpected element type: {type(element)}, value: {element}")
            yield f"ERROR: Unexpected type {type(element)}"
            return

        # Now safely process raw_text
        if not raw_text or not raw_text.strip():
            logging.warning("Empty raw_text received")
            yield "ERROR: Empty response from agent"
            return

        raw_text = raw_text.strip()

        # Emit final cleaned output
        if ("STOCKS ANALYSIS REPORT" in raw_text or 
            "Recommendation" in raw_text or 
            "yesterday's" in raw_text):
            yield f"FINAL OUTPUT:\n{raw_text}"
        else:
            yield f"CLEANED REPORT:\n{raw_text}"





class CloudRunAgentHandler(RemoteModelHandler):
    def __init__(self, app_url: str, app_name: str, user_id: str, metric_namespace: str):
        self._model_id = f"CloudRun_{app_name}"
        super().__init__(namespace=metric_namespace)
        self.app_url = app_url
        self.app_name = app_name
        self.user_id = user_id
        self.metric_namespace = metric_namespace

    def create_client(self) -> httpx.Client:
        # INCREASE TIMEOUT: Technical analysis takes time!
        return httpx.Client(timeout=300.0)

    def _get_token(self) -> str:
        # 1. Get the default credentials (service account identity)
        # This won't try to write to site-packages
        auth_req = google.auth.transport.requests.Request()

        try:
            # 2. Fetch the ID token specifically for the target app_url (Audience)
            return id_token.fetch_id_token(auth_req, self.app_url)
        except Exception as e:
            logging.error(f"Failed to fetch ID token: {e}")
            # Fallback for local testing if needed
            credentials, _ = google.auth.default()
            credentials.refresh(auth_req)
            return credentials.id_token
    # REMOVE 'async' here
    def request(
            self,
            item: list,
            client: httpx.Client,
            inference_args: Optional[Dict[str, Any]] = None
    ) -> PredictionResult:
        logging.info(f"------------- Running cloud urn req on item:{item} of type \n {type(item)}")
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Session Management
        session_id = f"beam_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        session_endpoint = f"{self.app_url}/apps/{self.app_name}/users/{self.user_id}/sessions/{session_id}"

        # 1. Register Session
        session_data = {"state": {"preferred_language": "English", "visit_count": 1}}
        try:
            client.post(session_endpoint, headers=headers, json=session_data)
        except Exception as e:
            logging.error(f"❌ Session Error: {e}")

        # 2. Run Agent Request
        # 2. Run Agent Request
        run_data = {
            "app_name": self.app_name,
            "user_id": self.user_id,
            "session_id": session_id,
            "new_message": {"role": "user", "parts": [{"text": item[0]}]},
            "streaming": False # Keep as False for easier parsing in Beam
        }

        # Use synchronous post
        # Use synchronous post
        response = client.post(f"{self.app_url}/run_sse", headers=headers, json=run_data)
        response.raise_for_status() # Good practice to catch 4xx/5xx errors

        raw_text = response.text.strip()
        # The split logic below handles the SSE format correctly
        data_lines = [l for l in raw_text.split('\n') if l.strip().startswith("data:")]

        if data_lines:
            try:
                import json
                # Get the very last data chunk
                last_json = json.loads(data_lines[-1][5:])

                # CRITIQUE FIX: Navigate the actual structure returned by your agent
                # Priority 1: Check stateDelta for the specific trading signal
                state_delta = last_json.get('actions', {}).get('stateDelta', {})
                final_text = state_delta.get('final_trade_signal')
                logging.info(f'======== Final Text is:\n {final_text}')
                # Priority 2: Fallback to standard text parts
                if not final_text:
                    parts = last_json.get('content', {}).get('parts', [{}])
                    final_text = parts[0].get('text', 'No text in response')
                logging.info(f'Passing to Next levvel Item:{item}\n Inference:{final_text}')
                return PredictionResult(example=item, inference=final_text)
            except Exception as e:
                logging.error(f"Mapping Error: {e}")
                return PredictionResult(example=item, inference=f"Mapping Error: {str(e)}")

        return PredictionResult(example=item, inference="No Data")
