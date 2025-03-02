
import io
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date, datetime
import logging
import time
from .news_util import get_user_agent

ECONOMIC_QUERY = """SELECT *  FROM `datascience-projects.gcp_shareloader.tmpeconomy` 
                        WHERE LABEL IN  ('Diesel', 'Petrol', 'IT-JOB-VACANCIES',
                        'CREDIT-DEBIT-SPENDING',
                        'ELECTRICITY-PRICES',
                        'GAS-PRICES',
                        'COMPANY-DISSOLUTIONS',
                        'POTENTIAL-REDUNDANCIES',
                        'fruit-apples-gala(kg)','it-computing-software',
                        'fruit-pears-conference(kg)',
                        'vegetable-lettuce-cos(head)',
                        'vegetable-tomatoes-plum(kg)',
                        'vegetable-cauliflower-all(head)',
                        'vegetable-celery-all_washed(kg)',
                        'fruit-blueberries-blueberries(kg)',
                        'fruit-raspberries-raspberries(kg)',
                        'vegetable-asparagus-asparagus(kg)',
                        'vegetable-cucumbers-cucumbers(kg)',
                        'fruit-strawberries-strawberries(kg)',
                        'vegetable-carrots-topped_washed(kg)',
                        'vegetable-courgettes-courgettes(kg)',
                        'vegetable-sweetcorn-sweetcorn(head)',
                        'vegetable-spinach_leaf-loose_bunches(kg)'
                        )
                        AND AS_OF_DATE >=  PARSE_DATE("%F", "{oneMonthAgo}")
                        ORDER BY LABEL, AS_OF_DATE DESC 
                        """

'''
Utilities to track 13F, form 4a , cftc and senate trading

13F 
- Berkshire Hathaway
- Michael Burry (Scion Asset Management)  {'name': 'SCION ASSET MANAGEMENT, LLC', 'cik': '0001649339'}
- Bill Ackman (Pershing Square Capital Management)  'name': 'PERSHING SQUARE CAPITAL MANAGEMENT, L.P.', 'cik': '0001336528'}
- Cathy Wood ARKK   {'name': 'ARK INNOVATIONS, INC.', 'cik': '0001776927'}
- David Tepper (Appaloosa Management)   {'name': 'APPALOOSA MANAGEMENT LP', 'cik': '0001006438'}
- Stanley Druckenmiller (Duquesne Family Office)  'name': 'DUQUESNE FAMILY OFFICE LLC', 'cik': '0001536411'}


obb.equity.ownership.form_13f(symbol='NVDA', provider='sec')
# Enter a date (calendar quarter ending) for a specific report.
obb.equity.ownership.form_13f(symbol='BRK-A', date='2016-09-30', provider='sec')
# Example finding Michael Burry's filings.
cik = obb.regulators.sec.institutions_search("Scion Asset Management").results[0].cik
# Use the `limit` parameter to return N number of reports from the most recent.
obb.equity.ownership.form_13f(cik, limit=2).to_df()





'''

def get_13f_filings():
    pass

def get_form4a_filings():
    pass



