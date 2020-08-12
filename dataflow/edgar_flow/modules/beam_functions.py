from datetime import date
from apache_beam.io.gcp.internal.clients import bigquery

def map_to_year(path, ppln_year):
    return path.format(ppln_year)

def get_edgar_index_files():
    return [
     'https://www.sec.gov/Archives/edgar/full-index/{}/QTR1/master.idx'
     ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR2/master.idx'
     ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR3/master.idx'
     ,'https://www.sec.gov/Archives/edgar/full-index/{}/QTR4/master.idx'
            ]
def map_from_edgar_row(row):
    return(row.split('|')[3],
                 '{}/{}'.format('https://www.sec.gov/Archives', row.split('|')[4]))

def map_to_bucket_string(tpl):
    return '{},{},{},{}'.format(tpl['COB'],
                                     tpl['CUSIP'], tpl['TICKER'],
                                     tpl['COUNT'])
def map_to_bq_dict(tpl, year):
    return dict(EDGAR_YEAR=year,
                     RUN_DATE=date.today().strftime('%Y-%m-%d'),
                     COB=tpl['COB'],
                     CUSIP=tpl['CUSIP'],
                     TICKER=tpl['TICKER'],
                     COUNT=tpl['COUNT'],
                     INDUSTRY=tpl['INDUSTRY'],
                     BETA=tpl['BETA'],
                     DCF=tpl['DCF'])

### BIG QUERY CONFIGS
## BIG QUERY SCHEMA

def get_edgar_table_schema():
  edgar_table_schema = 'RUN_DATE:STRING,EDGAR_YEAR:STRING,COB:STRING,CUSIP:STRING,COUNT:INTEGER,TICKER:STRING,INDUSTRY:STRING,BETA:STRING,DCF:STRING'
  return edgar_table_schema

def get_edgar_table_spec():
  return bigquery.TableReference(
      projectId="datascience-projects",
      datasetId='gcp_edgar',
      tableId='form_13hf_data_enhanced')



