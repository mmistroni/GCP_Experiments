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
from typing import Any, Sequence
# Python Packag'gemini-2.0-flash-001'e Version
MODEL_NAME = "gemini-2.5-flash" #"gemini-2.5-flash"

# --- Pipeline Configuration ---
# Number of threads to use for the local DirectRunner.
NUM_WORKERS = 1
SYSTEM_INSTRUCTION_TEXT = (
    "You are a helpful and concise assistant. "
    "Your should provide response in Json Format"
)

TEMPLATE = '''  You are a powerful stock researcher and statistician that recommends stock that are candidate to buy or to sell.
                I will provide you a json string containing a list of stocks.
                For each stock i will provide the following information
                1 - prev_close: the previous close of the stock
                2 - change: the change from yesterday
                3 - ADX: the adx
                4 - RSI : the RSI
                5 - SMA20: the 20 day simple moving average
                6 - SMA50: the 50 day simple moving average
                7 - SMA200: the 200 day simple moving average
                8 - slope, this will be slope of linear regression for past 30 days.
                9 - prev_obv: this is on balance volume from previous day
                10 - current_obv: this is the on balance volume for the current day
                11 - previous_cmf: this is the value for the previous day of  Chaikin Money Flow (CMF), calculated over previous 20 days
                12 - current_cmf: this is the value for the current  day of  Chaikin Money Flow (CMF), calculated over previous 20 days
                13 - obv_historical: these are the on balance volumes for the last 20 days
                14 - cmf_historical: these are the cmf values for past 20 days
                Based on that information, you will need to find which stocks which are candidates to rise in next days.
                If any of the stocks on the list have dropped more than 10%, then evaluate if it is worth to short sell them based on the
                same criterias
                If the json does not contain any elements, reply with this text:Cannot complete analysis as no stocks provided.
                Once you finish your analysis, please summarize your finding indicating, for each
                stock what is your recommendation and why. 
                At the end of the message, for the stocks  you recommend as buy or watch or sell, you should generate
                a json message with fields ticker, action (buy or watch or sell) and an explanation.
                The json string should be written between a <STARTJSON> and <ENDJSON> tags.
                
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


def run_gemini_pipeline(p, google_key, prompts=None):
    model_handler = GeminiModelHandler(
        model_name=MODEL_NAME,
        request_fn=generate_with_instructions,
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
        pipeline_prompts  = prompts
        read_prompts = p | "GetPrompts" >> beam.Create(pipeline_prompts)

    # The core of our pipeline: apply the RunInference transform.
    # Beam will handle batching and parallel API calls.
    predictions = read_prompts | "RunInference" >> RunInference(model_handler)
    
    # Parse the results to get clean text.
    llm_response =   (predictions | "PostProcess" >> beam.ParDo(PostProcessor())
                    )

    debug = llm_response | 'Debugging inference output' >> beam.Map(logging.info)


    return   llm_response  | "Excluding Inputs" >> beam.Map( lambda it: it[it.find('Output:') + 7:])


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


