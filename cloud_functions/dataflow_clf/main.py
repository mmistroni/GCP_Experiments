import requests
import json
from datetime import datetime
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials




def generate_api_client(input_params):
    print('Generating API CLIENT KEYS')
    credentials = GoogleCredentials.get_application_default()
    service = build('dataflow', 'v1b3', credentials=credentials)


    template_name = input_params['template']
    job_parameters = input_params['job_parameters']
    jobname=input_params['job_name']





    PROJECT = 'datascience-projects'
    BUCKET = 'mm_dataflow_bucket'
    TEMPLATE = template_name
    GCSPATH="gs://{bucket}/templates/{template}".format(bucket=BUCKET, template=TEMPLATE)
    BODY = {
       "jobName": "{}_{}".format(jobname, datetime.now().strftime('%Y%m%d-%H%M')),
       "parameters": job_parameters,
       "environment": {
           "tempLocation": "gs://{bucket}/temp".format(bucket=BUCKET)
       }
    }
    request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
    response = request.execute()
    return str(response)

def launch_dataflow(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    print('Input request was:{}'.format(request_json))
    response = generate_api_client(request_json)
    return response
