from shareloader.modules.marketstats_utils import Market52Week, is_above_52wk, \
    combine_movers, is_below_52wk, get_all_stocks
import apache_beam as beam
import unittest
from apache_beam.testing.test_pipeline import TestPipeline


class TestMarketDailyPipeline(unittest.TestCase):

    def test_market52weeks(self):
        samples = [('AMZN', 20.0, 1.0, 14.0, 9.0, 1 ), # MKT HIGHT
                   ('AAPL', 10.0, 1.0, 14.0, 9.0, 1),   # NOTHING
                   ('FB', 9.0, 1.0, 11.0, 10.0, 1),   # MLOW
                   ('MSFT', 8.0, 1.0, 14.0, 8.0, 1),  #MLOW
                   ('GE', 7.0, 1.0, 14.0, 9.0, 1)      #MLOW
                   ]

        with TestPipeline() as p:
            data = (p | 'Sampling data' >> beam.Create(samples)
                        | 'Find 52Week High' >> beam.Filter(is_above_52wk)
                        | 'Mapping Tickers1' >> beam.Map(lambda d: d[0])
                        | 'Combine Above' >> beam.CombineGlobally(combine_movers, label='Above 52wk high:')
                        |'ADD Label' >> beam.Map( lambda txt: 'Above 52 wk:{}'.format(txt))
                        | 'Print out' >> beam.Map(print)
                    )

            print('== Checking below...')
            data2 = (p | 'Sampling data2' >> beam.Create(samples)
                    | 'Find 52Week Low' >> beam.Filter(is_below_52wk)
                    | 'Mapping Tickers12' >> beam.Map(lambda d: d[0])
                    | 'Combine beow' >> beam.CombineGlobally(combine_movers, label='Above 52wk low:')
                    | 'ADD Label2' >> beam.Map(lambda txt: 'Below 52 wk:{}'.format(txt))
                    | 'Print out2' >> beam.Map(print)
                    )

    def test_get_all_stocks(self):
        all_stocks = get_all_stocks('<tests>')
        print(len(all_stocks))
        print([s for s in all_stocks if s in ['PXLG','PXMV','RGSE','LGCY', 'AMZN']])


