import unittest
from shareloader.modules.finviz_utils import get_universe_stocks, get_canslim, get_leaps,\
                                            get_graham_defensive, get_graham_enterprise,\
                                            get_extra_watchlist, get_new_highs, FinvizLoader, \
                                            get_high_low, overnight_return, get_advance_decline,\
                                            get_buffett_six, get_finviz_obb_data, get_advance_decline_sma, \
                                            AsyncProcessFinviz, _run_screener

from pprint import pprint
import os
from shareloader.modules.superperf_metrics import get_dividend_paid
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from openbb_yfinance.models.equity_quote import YFinanceEquityQuoteFetcher
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import  BytesIO



class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.debugSink = beam.Map(print)

    def test_canslim(self):
        res = get_canslim()
        pprint(res)

    def test_leaps(self):
        res = get_leaps()
        pprint(res)

    def test_universe(self):
        rres = get_universe_stocks()
        print(rres)


    def filter_defensive(self, input_dict):
        if ('debtOverCapital' in input_dict and input_dict['debtOverCapital'] < 0) \
             and ('dividendPaid' in input_dict and input_dict['dividendPaid']  == True) \
                 and ('epsGrowth' in input_dict and input_dict['epsGrowth'] >= 0.33) \
                 and ('positiveEps' in input_dict and  input_dict['positiveEps'] > 0) \
                 and ('priceToBookRatio' in input_dict and input_dict['priceToBookRatio'] > 0) :
            return True
        return False

    def filter_enterprise(self, input_dict):
        if ('debtOverCapital' in input_dict and input_dict['debtOverCapital'] < 0) \
             and ('dividendPaid' in input_dict and input_dict['dividendPaid']  == True) \
                 and ('epsGrowth' in input_dict and input_dict['epsGrowth'] >= 0.33) \
                 and ('positiveEps' in input_dict and  input_dict['positiveEps'] > 0) \
                 and ('priceToBookRatio' in input_dict and input_dict['priceToBookRatio'] > 0) :
            return True
        return False



    def test_gdefensive(self):
        key = os.environ['FMPREPKEY']

        res = get_graham_defensive(key)

        for data in res:
            if self.filter_defensive(data):
                pprint(data)

    def test_genterprise(self):
        key = os.environ['FMPREPKEY']

        res = get_graham_enterprise(key)

        print(res)

    def test_extra_watchlist(self):
        key = os.environ['FMPREPKEY']

        res = get_extra_watchlist()

        print(res)

    def test_new_high(self):

        res = get_new_highs()
        print(res)

    def test_finvizloader(self):
        key = os.environ['FMPREPKEY']
        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Run Loader' >> beam.ParDo(FinvizLoader(key))
                     | self.debugSink
                     )

    def test_highlow(self):
        res = get_high_low()
        print(res)


    def parse_xml_from_zip_http(self, url):
        """Parses XML from a ZIP file accessed via HTTP.

        Args:
            url: The URL of the ZIP file.

        Returns:
            The parsed XML root element.
        """

        try:
            # Download the ZIP file
            response = requests.get(url)
            response.raise_for_status()

            # Create a BytesIO object to store the ZIP file content
            zip_data = BytesIO(response.content)

            # Open the ZIP file
            with zipfile.ZipFile(zip_data) as zip_file:
                # Extract the XML file (assuming the first file is the XML)
                xml_file_name = zip_file.namelist()[1]
                xml_data = zip_file.read(xml_file_name)

            # Parse the XML data
            root = ET.fromstring(xml_data)

            return root

        except Exception as e:
            print(f"Error parsing XML from ZIP file: {e}")
            return None



    def test_disclosures(self):
        # Example usage
        url = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2024FD.zip"
        root = self.parse_xml_from_zip_http(url)

        if root:
            # Process the parsed XML elements
            doc_id_elements = root.findall(".//Member/DocID")
            doc_ids = [member.findtext("DocID")
                       for member in root.findall(".//Member")
                       if member.findtext("FilingType") == "P"]

            discl_urls = []
            for doc_id in doc_ids:
                base_url = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/{doc_id}.pdf'
                discl_urls.append(base_url)
            from pprint import pprint
            pprint(discl_urls)

            df = self.extract_tables_from_pdf('c:/Users/Marco/20025679.pdf')
            print(df)


    def test_overnight_return(self):
        res = overnight_return()
        pprint(res)

    def test_advancedecline(self):

        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['NASDAQ'])
                     | 'Run adLoader' >> beam.ParDo(lambda exch: get_advance_decline(exch))
                     | self.debugSink
                     )



    def test_buffettsix(self):

        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Run adLoader' >> beam.ParDo(get_buffett_six())
                     | self.debugSink
                     )

    def test_asyncfinviz(self):
        high_filter_dict = {'Change': 'Up',
                            'Exchange': 'NYSE'}
        low_filter_dict = {'Change': 'Down',
                           'Exchange': 'NYSE'}

        with TestPipeline(options=PipelineOptions()) as p:
            input = (p | 'Start' >> beam.Create(['AAPL'])
                     | 'Run adLoader' >> beam.ParDo(AsyncProcessFinviz(high_filter_dict,
                                                                       low_filter_dict))
                     | self.debugSink
                     )


    def test_obb_finviz(self):
        up_filter = 'Up'
        down_filter = 'Down'

        high_filter_dict = {'Change': up_filter,
                            'Exchange': 'NYSE'}
        low_filter_dict = {'Change': down_filter,
                           'Exchange': 'NYSE'}

        res = get_finviz_obb_data({}, high_filter_dict)

        self.assertTrue(len(res) > 1)

        res2 = get_finviz_obb_data({}, low_filter_dict)

        self.assertTrue(len(res2) > 1)

    def test_get_advancedecline(self):
        res = get_advance_decline('NYSE')
        print(res)


    def test_get_advancedecline(self):
        res = get_advance_decline_sma('NYSE', 200)
        print(res)

    def test_run_screener(self):
        up_filter = f'Price above SMA50'
        down_filter = f'Price below SMA50'
        key = f'50-Day Simple Moving Average'

        high_filter_dict = {key: up_filter,
                            'Exchange': 'NASDAQ'}
        low_filter_dict = { key: down_filter,
                           'Exchange': 'NASDAQ'}


        #highs = _run_screener(high_filter_dict)
        #high_ticks = ','.join([d['Ticker'] for d in highs])
        lows = _run_screener(low_filter_dict)
        low_ticks = ','.join([d['Ticker'] for d in lows])


    def test_quotefetcher(self):
        import asyncio
        class AsyncQuoteProcess(beam.DoFn):

            def __init__(self, credentials, fetcher):
                self.credentials = credentials
                self.fetcher = fetcher

            async def fetch_data(self, element: str):
                params = dict(symbol=element)
                data = await self.fetcher.fetch_data(params, self.credentials)
                dt =  [d.model_dump(exclude_none=True) for d in data]
                return dt
            def process(self, element: str):
                return asyncio.run(self.fetch_data(element))

        ticker = 'AAPL'

        with TestPipeline(options=PipelineOptions()) as p:
            quote = (p | 'Start Quote' >> beam.Create([ticker])
                     | 'Run Quote' >> beam.ParDo(AsyncQuoteProcess({}, YFinanceEquityQuoteFetcher))
                     | 'Print quote' >> self.debugSink)


if __name__ == '__main__':
    unittest.main()
