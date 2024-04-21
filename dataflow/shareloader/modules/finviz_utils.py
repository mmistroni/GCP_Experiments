## finviz utilities
#https://pypi.org/project/finvizfinance/
# https://finvizfinance.readthedocs.io/en/latest/
#https://medium.com/the-investors-handbook/the-best-finviz-screens-for-growth-investors-72795f507b91
from finvizfinance.screener.overview import Overview

'''
res = (input_dict.get('marketCap', 0) > 300000000) and (input_dict.get('avgVolume', 0) > 200000) \
        and (input_dict.get('price', 0) > 10) and (input_dict.get('eps_growth_this_year', 0) > 0.2) \
        and (input_dict.get('grossProfitMargin', 0) > 0) \
        and  (input_dict.get('price', 0) > input_dict.get('priceAvg20', 0))\
        and (input_dict.get('price', 0) > input_dict.get('priceAvg50', 0)) \
        and (input_dict.get('price', 0) > input_dict.get('priceAvg200', 0))  \
        and (input_dict.get('net_sales_qtr_over_qtr', 0) > 0.2) and (input_dict.get('returnOnEquity', 0) > 0) \
        and (input_dict.get('eps_growth_next_year', 0) > 0) and (input_dict.get('eps_growth_qtr_over_qtr', 0) > 0.2)
'''

def get_universe_stocks():
    foverview = Overview()
    filters_dict = {'Market Cap.':'Mega ($200bln and more)','Sector':'Basic Materials'}
    foverview.set_filter(filters_dict=filters_dict)
    df = foverview.screener_view()
    return df.head()




