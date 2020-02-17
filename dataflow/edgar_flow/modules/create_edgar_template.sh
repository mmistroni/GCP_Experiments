#!/bin/bash
python -m edgar_main  --runner=dataflow --project=datascience-projets --template_location=gs://mm_dataflow_bucket/templates/edgar_dataflow_template --temp_location=gs://mm_dat
aflow_bucket/temp --staging_location=gs://mm_dataflow_bucket/staging --setup_file  ./setup.py