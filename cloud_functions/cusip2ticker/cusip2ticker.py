from bs4.element import Tag
from bs4 import BeautifulSoup
import urllib
import json

class CusipClient(object):
    # alternative URL is https://search13f.com/securities/top/478160104
    @staticmethod
    def cusip_to_ticker(cusipNo):
        '''
              Retrieves ticker given a cusip
              :param str cusipNo: a string representing a cusip
              :return str: a string representing the ticker associated with the cusip
          '''
        cusipUrl = \
        'https://quotes.fidelity.com/myFidelity/SymLookup2.phtml?reqforlookup=REQUESTFORLOOKUP&rows=50&fromMyFidelity=false&for=stock&by=cusip&criteria={cusip}&submit=Search'\
        .format(cusip=cusipNo)
        r  = urllib.request.urlopen(cusipUrl)
        data = r.read()
        print (data)
        soup = BeautifulSoup(data)
        #1. get all td tags, where there is a SYMBL. shold bbe only 1
        tds = [col for col in  soup.find_all('td') if 'SYMBOL' in col.text and col.get('colspan') is None]
        candidate = tds[0].parent # gong t
        sibs = list(candidate.next_siblings)
        # get next row
        next_row = [row for row in sibs if isinstance(row, Tag)]
        vals = [td for tr_good in next_row for td in tr_good.children if isinstance(td, Tag) ]
        row_values =  [str(td.text.encode('utf-8')) for td in vals if bool(td.text)]
        return json.dumps(dict(name=str(row_values[0]), ticker=str(row_values[1])))


def cusip2ticker(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    input_path = request.path.split('/')
    if len(input_path) > 1:
        cusip = input_path[1]
        return CusipClient.cusip_to_ticker(cusip)
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return 'Invalid Request:{}'.format(requst.path)
