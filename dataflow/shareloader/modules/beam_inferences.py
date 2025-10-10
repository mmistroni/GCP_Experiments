import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# Import the core components for ML inference in Beam.
from apache_beam.ml.inference.base import RunInference, PredictionResult
# Import the specific model handler for Gemini on Vertex AI.
from apache_beam.ml.inference.gemini_inference import GeminiModelHandler, generate_from_string
# Helper for iterating over collections.
from collections.abc import Iterable
from apache_beam.ml.inference.base import RunInference
import logging
# Python Package Version
MODEL_NAME = "gemini-2.5-flash"

# --- Pipeline Configuration ---
# Number of threads to use for the local DirectRunner.
NUM_WORKERS = 1


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


def run_gemini_pipeline(p, google_key):
    model_handler = GeminiModelHandler(
        model_name=MODEL_NAME,
        request_fn=generate_from_string,
        # project=PROJECT_ID,
        # location=LOCATION
        api_key=google_key
    )

    prompts = [
        "What is 1+2? Provide the response in a Json format following this schema: {'question': <prompt>, 'answer': <your_answer>}",
        #"How is the weather in NYC in July?",
        #"Write a short, 3-line poem about a robot learning to paint."
    ]

    read_prompts = p | "GetPrompts" >> beam.Create(prompts)

    # The core of our pipeline: apply the RunInference transform.
    # Beam will handle batching and parallel API calls.
    predictions = read_prompts | "RunInference" >> RunInference(model_handler) 
    
    # Parse the results to get clean text.
    processed = predictions | "LogPredictions" >> beam.Map(logging.info)

    


    # Parse the results to get clean text.
    #processed = predictions | "PostProcess" >> beam.ParDo(PostProcessor())

    # Print the final, formatted output to the console.
    # This is a simple "sink" for demonstration purposes.
    #_ = predictions | "PrintOutput" >> beam.Map(logging.info)


