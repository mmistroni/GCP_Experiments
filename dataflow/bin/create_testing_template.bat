cd ..\testing
python -m edgar_main  --runner=dataflow --project=datascience-projects --template_location=gs://mm_dataflow_bucket/templates/testing_share_dataflow_template --temp_location=gs://mm_dataflow_bucket/temp --staging_location=gs://mm_dataflow_bucket/staging
