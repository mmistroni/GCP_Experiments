from __future__ import absolute_import

import logging
from apache_beam.io.gcp.internal.clients import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from .economic_utils import get_petrol_prices, get_latest_jobs_statistics, get_fruit_and_veg_prices,\
                            get_card_spending, get_gas_prices,get_electricity_prices, get_company_dissolutions,\
                            get_redundancies_notifications

# Check Datasets here
## https://beta.ukdataservice.ac.uk/datacatalogue/studies/?Search=#!?Search=House&Rows=10&Sort=1&DateFrom=440&DateTo=2022&CountryFacet=England&Page=1

class XyzOptions(PipelineOptions):
    pass

def run_jobstats_pipeline(pipeline):
    return (pipeline | 'start putcall ratio' >> beam.Create(['20210101'])
                        | 'Create jobs' >> beam.Map(lambda item:  get_latest_jobs_statistics())
                )
def kickoff_pipeline(pipeline):
    jobstats = run_jobstats_pipeline(pipeline)

    fruitandveg = (pipeline | 'Create fandv' >> beam.Create(get_fruit_and_veg_prices())
                   )

    pprices = (pipeline | 'Create pprices' >> beam.Create(get_petrol_prices())
               )

    ccard_spending = (pipeline | 'Create ccard' >> beam.Create(get_card_spending())
                   )

    gasprices = (pipeline | 'Create gasprices' >> beam.Create(get_gas_prices())
               )

    elecprices = (pipeline | 'Create elecprices' >> beam.Create(get_electricity_prices())
                 )

    dissolutions = (pipeline | 'Create dissolutions' >> beam.Create(get_company_dissolutions())
                 )

    redundancies = (pipeline | 'Create redundancies' >> beam.Create(get_redundancies_notifications())
                  )

    return (
            (jobstats, fruitandveg, pprices, ccard_spending, gasprices, elecprices, dissolutions, redundancies)
            | 'FlattenCombine all' >> beam.Flatten()
            | 'MAP Values' >> beam.Map(lambda d: dict(AS_OF_DATE=d['asOfDate'], LABEL=d['label'], VALUE=d['value']))

    )

def write(inputData):
    
    logSink = beam.Map(logging.info)

    (inputData | 'Writing ' >> logSink)

    bqSink2 = beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId="datascience-projects",
            datasetId='gcp_shareloader',
            tableId='tmpeconomy'),
            schema='AS_OF_DATE:DATE,LABEL:STRING,VALUE:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    
    (inputData | 'Writing to bq' >>  bqSink2)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = XyzOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        bqPipeline = kickoff_pipeline(p)
        logging.info('--------------------  writing to sink ----------')
        write(bqPipeline)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()