
import unittest
from lxml import etree
from io import StringIO, BytesIO
from edgar_flow.modules.edgar_utils import fast_iter2
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

    def test_parsexml(self):
        with open('edgarform4.txt') as f:
            from pprint import pprint
            data = f.read()
        xml_str = data[data.rfind('<XML>') + 5: data.rfind("</XML>")].strip()
        some_file_like = BytesIO(xml_str.encode('utf-8'))
        #context = etree.iterparse(xml_str)
        context = etree.iterparse(some_file_like)
        res = self.parse_xml(context)
        print('We got:{}'.format(res))


    def parse_xml(self, tree):
        return fast_iter2(tree)
