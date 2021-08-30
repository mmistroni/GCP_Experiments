
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from edgar_flow.modules.edgar_utils import fast_iter2, get_period_of_report
from edgar_flow.modules.edgar_utils import ReadRemote, ParseForm13F
from edgar_flow.modules.edgar_daily import write_to_bigquery

from xml.etree import ElementTree
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam

CATEGORIES = set(
    ['issuerTradingSymbol', 'transactionCode', 'transactionShares'])
SKIP_CATEGORIES = set(['phdthesis', 'mastersthesis', 'www'])
DATA_ITEMS = ["value"]


class TestEdgarUtils(unittest.TestCase):

    def test_read_remote(self):
        self.assertTrue(False)

    def test_read_all_from_text(self):
        self.assertTrue(False)

    def test_edgar_combine_fn(self):
        self.assertTrue(False)

    def test_parse_form_13f(self):
        self.assertTrue(False)


    def get_shares_acquired(self, root):
        ts = root.findall(".//transactionAmounts/transactionShares/value")
        return sum([int(t.text) for t in ts])

    def get_isdirector(self, root):
        ts = root.findall(".//reportingOwner/reportingOwnerRelationship/isDirector/value")
        items = [t.text for t in ts]
        print('ITEMS ARE:{}'.format(items))
        if items:
            return 'Director' if items[0] == '1' else ''
        return ''



    def get_shares_posttransaction(self, root):
        ts = root.findall(".//postTransactionAmounts/sharesOwnedFollowingTransaction/value")
        return sum([int(t.text) for t in ts])

    def get_transaction_price_per_share(self, root):
        ts = root.findall(".//transactionAmounts/transactionPricePerShare/value")
        return sum([float(t.text) for t in ts])



    def test_parsexml(self):
        with open('edgarform4.txt') as f:
            from pprint import pprint
            data = f.read()
        xml_str = data[data.rfind('<XML>') + 5: data.rfind("</XML>")].strip()
        some_file_like = BytesIO(xml_str.encode('utf-8'))
        print(xml_str)
        #context = etree.iterparse(xml_str)
        #context = etree.iterparse(some_file_like)
        #res = self.parse_xml(context)
        #print('We got:{}'.format(res))

        tree = ElementTree.ElementTree(ElementTree.fromstring(xml_str))
        root = tree.getroot()
        print(self.get_shares_posttransaction(tree))
        print(self.get_shares_acquired(tree))
        print(self.get_transaction_price_per_share(tree))
        print('IsDirector:{}'.format(self.get_isdirector(tree)))

    def test_parsexml2(self):
        from xml.etree import ElementTree
        with open('edgarform4.txt') as f:
            from pprint import pprint
            data = f.read()
        xml_str = data[data.rfind('<XML>') + 5: data.rfind("</XML>")].strip()
        xml_str = data[data.rfind('<XML>') + 5: data.rfind("</XML>")].strip()
        some_file_like = BytesIO(xml_str.encode('utf-8'))
        context = etree.iterparse(some_file_like)
        print(fast_iter2(context))


    def test_get_period_of_report(self):
        content = requests.get('https://www.sec.gov/Archives/edgar/data/1767306/0001420506-21-000026.txt',
                               headers={
                                   'User-Agent': 'WCorp Services mmistroni@gmail.com'
                               }
                               )
        data = content.text
        subset = data[data.find('<headerData>'): data.find("</headerData>") + 13]
        print(subset)
        from xml.etree import ElementTree
        tree = ElementTree.ElementTree(ElementTree.fromstring(subset))
        root = tree.getroot()
        tcodes = root.findall(".//periodOfReport")

        print(get_period_of_report(content))

    def test_extractInfo_from_form13(self):
        content = requests.get('https://www.sec.gov/Archives/edgar/data/1767306/0001420506-21-000026.txt',
                               headers={
                                   'User-Agent': 'WCorp Services mmistroni@gmail.com'
                               }
                               ).text
        xml_str = content[content.rfind('<XML>') + 5: content.rfind("</XML>")].strip()

        print(self.get_filing_data(xml_str))

    def test_extractInfo_from_form132(self):
        content = requests.get('https://www.sec.gov/Archives/edgar/data/1767306/0001420506-21-000026.txt',
                               headers={
                                   'User-Agent': 'WCorp Services mmistroni@gmail.com'
                               }
                               ).text
        xml_str = content[content.find('<XML>') + 5: content.find("</XML>")].strip()
        print(self.get_reporter(xml_str))

    def test_parse_form13f(self):
        with TestPipeline() as p:
            input = (p | 'Start' >> beam.Create([('2021-08-26', 'https://www.sec.gov/Archives/edgar/data/1325091/0001325091-21-000019.txt')])
                       | 'Parse Form 13F' >>  beam.ParDo(ParseForm13F())
                       | 'Print ouit ' >> beam.Map(print)
                     )


