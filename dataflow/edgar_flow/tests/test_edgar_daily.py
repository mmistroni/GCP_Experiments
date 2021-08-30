import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from unittest.mock import patch, Mock
from edgar_flow.modules.edgar_utils import  cusip_to_ticker
from edgar_flow.modules.edgar_utils import ReadRemote, ParseForm13F, cusip_to_ticker, \
            find_current_year, EdgarCombineFn, ParseForm4
from edgar_flow.modules.edgar_daily import write_to_bigquery, combine_data

import unittest


class Check(beam.PTransform):
    def __init__(self, checker):
      self._checker = checker

    def expand(self ,pcoll):
      print('Invoking sink....')
      assert_that(pcoll, self._checker)


class TestEdgarDailyPipeline(unittest.TestCase):

    @patch('edgar_flow.modules.edgar_utils.cusip_to_ticker')
    def test_combine_elements1(self, cusip_mock):
        cusip_mock.return_value = 'foobar'

        sample_list = [('20201006', '88579Y101'), ('20201006', '88579Y101'), ('20201006', '002824100'), ('20201006', '00724F101'), ('20201006', '00971T101'), ('20201006', '00971T101'), ('20201006', '020002101'), ('20201006', '02079K305'), ('20201006', '023135106'), ('20201006', '023135106'), ('20201006', 'G02602103'), ('20201006', 'G02602103'), ('20201006', '025537101'), ('20201006', '025537101'), ('20201006', '025816109'), ('20201006', '025816109'), ('20201006', '03027X100'), ('20201006', '03027X100'), ('20201006', '031162100'), ('20201006', '031162100'), ('20201006', '037833100'), ('20201006', '037833100'), ('20201006', '038222105'), ('20201006', '038222105'), ('20201006', '00206R102'), ('20201006', '00206R102'), ('20201006', '052769106'), ('20201006', '052769106'), ('20201006', '053015103'), ('20201006', '053015103'), ('20201006', '075887109'), ('20201006', '115236101'), ('20201006', '115236101'), ('20201006', '127387108'), ('20201006', '127387108'), ('20201006', '166764100'), ('20201006', '166764100'), ('20201006', '171340102'), ('20201006', '17275R102'), ('20201006', '177376100'), ('20201006', '12572Q105'), ('20201006', '12572Q105'), ('20201006', '20030N101'), ('20201006', '20030N101'), ('20201006', '126408103'), ('20201006', '231021106'), ('20201006', '126650100'), ('20201006', '26614N102'), ('20201006', '26614N102'), ('20201006', '278865100'), ('20201006', '285512109'), ('20201006', '285512109'), ('20201006', '532457108'), ('20201006', '1429ELYNX'), ('20201006', '30231G102'), ('20201006', '30231G102'), ('20201006', '337738108'), ('20201006', '337738108'), ('20201006', '372460105'), ('20201006', '372460105'), ('20201006', '427866108'), ('20201006', '437076102'), ('20201006', '458140100'), ('20201006', '458140100'), ('20201006', '459200101'), ('20201006', '459506101'), ('20201006', '464287465'), ('20201006', '464287622'), ('20201006', '464287499'), ('20201006', '478160104'), ('20201006', '46625H100'), ('20201006', '512816109'), ('20201006', '512816109'), ('20201006', '524ESC9Q5'), ('20201006', '539830109'), ('20201006', '548661107'), ('20201006', '57636Q104'), ('20201006', '57636Q104'), ('20201006', '579780206'), ('20201006', '580135101'), ('20201006', '580135101'), ('20201006', '594918104'), ('20201006', '594918104'), ('20201006', '687380105'), ('20201006', '704326107'), ('20201006', '704326107'), ('20201006', '713448108'), ('20201006', '713448108'), ('20201006', '718172109'), ('20201006', '693475105'), ('20201006', '742718109'), ('20201006', '744573106'), ('20201006', '79466L302'), ('20201006', '820054104'), ('20201006', '78467X109'), ('20201006', '78462F103'), ('20201006', '78467Y107'), ('20201006', '855244109'), ('20201006', '871829107'), ('20201006', '872540109'), ('20201006', '872540109'), ('20201006', '891092108'), ('20201006', '891092108'), ('20201006', '89417E109'), ('20201006', '89417E109'), ('20201006', '907818108'), ('20201006', '91324P102'), ('20201006', '922908769'), ('20201006', '921937819'), ('20201006', '92343V104'), ('20201006', '92343V104'), ('20201006', '92826C839'), ('20201006', '92826C839'), ('20201006', '931142103'), ('20201006', '931142103'), ('20201006', '254687106'), ('20201006', '94106L109'), ('20201006', '94106L109'), ('20201006', '98956P102')]
        with TestPipeline() as p:
            ( p | beam.Create(sample_list)
                        | 'Combining similar' >> beam.combiners.Count.PerElement()
                        | 'Groupring' >> beam.MapTuple(lambda tpl, count: (tpl[0], tpl[1], count))
                        | 'Adding Cusip' >> beam.MapTuple(lambda cob, word, count: (cob, word, cusip_to_ticker(word), count))
                        | 'Beam mapp' >> beam.Map(print))

    def test_combine_elements(self):
        sample_list = [('2020-01-01', 'CUSIP1'),
                       ('2020-01-01', 'CUSIP2'),
                       ('2020-01-01', 'CUSIP1')]
        with TestPipeline() as p:
            res = (p
            | 'Create produce' >> beam.Create(
                sample_list)
            | 'Count unique elements' >> beam.combiners.Count.PerElement()
            | beam.Map(print))

    def test_combine_elements(self):
        sample_list = [[str(i), str(i), str(i), i] for i in range(1,20)]

        sink = Check(equal_to([sample_data1]))

        with TestPipeline() as p:
            res = (p
            | 'Create produce' >> beam.Create(
                sample_list)
            | 'Combining to get top 30' >> beam.CombineGlobally(EdgarCombineFn())
            | beam.Map(print))

    def test_write_to_bigquery(self):
            test_data = [('2021-08-26', '03-31-2021', 'G0750C108', '47375', '0001325091')]


            with TestPipeline() as p:
                 input = p | 'Start:' >> beam.Create(test_data)
                 res =  write_to_bigquery(input)
                 res | 'Printing' >> beam.Map(print)

    def test_enhance_data(self):
            test_data = [('2021-08-26', '03-31-2021', 'G0750C108', '1000', '0001325091'),
                         ('2021-08-26', '03-31-2021', 'G0750C108', '2000', '0001325091')]


            with TestPipeline() as p:
                 input = p | 'Start:' >> beam.Create(test_data)
                 res =  combine_data(input)
                 res | 'Printing' >> beam.Map(print)



