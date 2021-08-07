from edgar_flow.modules.beam_functions import *
from edgar_flow.modules.edgar_utils import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import unittest

class TestBeamFunctions(unittest.TestCase):

    def get_test_dictionary(self):
        return dict(COB='testcob',
                         CUSIP='testcusip',
                         TICKER='testticker',
                         COUNT='tstcount',
                         INDUSTRY='tstindustry',
                         BETA='tstbeta',
                         DCF='testdcf')

    def test_map_to_year(self):
        sample_line = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR1/master.idx'
        test_year = '2018'
        expected_result = sample_line.format(test_year)
        res = map_to_year(sample_line, test_year)
        self.assertEqual(expected_result, res)

    def test_map_from_edgar_row(self):
        sample = "|First|13-HF|2018-01-24|edgar/testfile.txt|SomethingElse|"
        expected_result = ('2018-01-24', 'https://www.sec.gov/Archives/edgar/testfile.txt')
        self.assertEqual(expected_result, map_from_edgar_row(sample))


    def test_map_to_bucket_string(self):
        test_dict = self.get_test_dictionary()
        expected_str = '{},{},{},{}'.format(test_dict['COB'],
                                            test_dict['CUSIP'],
                                            test_dict['TICKER'],
                                            test_dict['COUNT'])
        self.assertEqual(expected_str, map_to_bucket_string(test_dict))

    def test_map_to_bq_dict(self):
        test_dict = self.get_test_dictionary()
        test_year = '2018'
        test_dict['EDGAR_YEAR'] = test_year
        res  = map_to_bq_dict(test_dict, test_year)

        for k, v in res.items():
            self.assertEqual(test_dict[k], res[k], 'Test failed for {}: expected:{}'.format(k, v))






