
import unittest
import requests
from lxml import etree
from io import StringIO, BytesIO
from edgar_flow.modules.edgar_utils import fast_iter2, get_period_of_report
from xml.etree import ElementTree

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
        content = requests.get('https://www.sec.gov/Archives/edgar/data/1767306/0001420506-21-000026.txt')
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
        pass