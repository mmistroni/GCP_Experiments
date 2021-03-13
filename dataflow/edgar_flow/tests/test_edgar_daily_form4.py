from mock import patch, Mock
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from mock import patch, Mock
from edgar_flow.modules.edgar_utils import  cusip_to_ticker, ParseForm4, EdgarCombineFn
from edgar_flow.modules.edgar_daily_form4 import find_current_day_url, run_my_pipeline,\
                        filter_form_4, enhance_form_4


import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestEdgarDailyForm4Pipeline(unittest.TestCase):
    '''
        <reportingOwnerRelationship>
            <isDirector>0</isDirector>    get title of whoever did it, and also if it is 10% owner
            <isOfficer>1</isOfficer>
            <isTenPercentOwner>0</isTenPercentOwner>
            <isOther>0</isOther>
            <officerTitle>EVP &amp; General Counsel</officerTitle>
        </reportingOwnerRelationship>

    '''

    def test_enhance_form4(self):

        with TestPipeline() as p:
            source = (p | 'Startup' >> beam.Create(['start_token'])
                      | 'Add current date' >> beam.Map(find_current_day_url)
                      )
            lines = run_my_pipeline(source)
            form4 = filter_form_4(lines)
            enhanced_data = enhance_form_4(form4)

    def test_find_current_day_url(self):
        from datetime import date
        from pandas.tseries.offsets import BDay
        dt  = date.today() - BDay(1)
        print('Current daY URL:{}'.format(find_current_day_url(dt)))



