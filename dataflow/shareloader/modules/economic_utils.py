import csv, urllib.request
import io
import pandas as pd
import csv, requests
import codecs
from bs4 import BeautifulSoup
import requests
from urllib.request import Request, urlopen
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import logging
import xlrd
from .news_util import get_user_agent

def get_fruit_and_veg_prices():
    baseUrl = 'https://www.gov.uk/government/statistical-data-sets/wholesale-fruit-and-vegetable-prices-weekly-average'
    req = requests.get(baseUrl)
    soup = BeautifulSoup(req.text, "html.parser")
    span = soup.find_all('span', {"class": "download"})[0]
    anchor = span.find_all('a', {"class": "govuk-link"})[0]
    link = anchor.get('href')
    r = requests.get(link).text
    dt = pd.read_csv(io.StringIO(r), header=0)
    dt['asOfDate'] = pd.to_datetime(dt['date'], infer_datetime_format=True).dt.date
    latest = dt[dt.asOfDate == dt.asOfDate.max()][['asOfDate', 'category', 'item', 'variety', 'price', 'unit']]
    latest['label'] = latest.apply(lambda r: f"{r['category']}-{r['item']}-{r['variety']}({r['unit']})", axis=1)
    latest['value'] = latest.price.apply(lambda valstr: float(valstr))

    return latest[['asOfDate', 'label', 'value']].to_dict('records')

def get_petrol_prices():
    url = 'https://www.gov.uk/government/statistics/weekly-road-fuel-prices'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    span = soup.find_all('span', {"class": "download"})[0]
    anchor = span.find_all('a', {"class": "govuk-link"})[0]
    link = anchor.get('href')
    r = requests.get(link).text
    dt = pd.read_csv(io.StringIO(r), header=2)[-1:][['Date','ULSP', 'ULSD']].to_dict('records')[-1]
    return [
                {'asOfDate' : datetime.strptime(dt['Date'], "%d/%m/%Y").date(),
                 'label' : 'Petrol',
                  'value': float(dt['ULSP'])},
                {'asOfDate': datetime.strptime(dt['Date'], "%d/%m/%Y").date(),
                 'label': 'Diesel',
                 'value': float(dt['ULSD'])}
                ]

def get_latest_url():
    url = "https://cy.ons.gov.uk/datasets/online-job-advert-estimates/editions"
    ## check this https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/bulletins/jobsandvacanciesintheuk/august2022
    ##  https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/onlinejobadvertestimates
    import requests
    from shareloader.modules.news_util import get_user_agent
    from bs4 import BeautifulSoup
    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/onlinejobadvertestimates'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    anchor = soup.find_all('a')
    links = [a for a in anchor if
             'Download Online job advert estimates' in a.get('aria-label', '') and '2022' in a.get('aria-label', '')]
    link = links[0].get('href')
    full_url = f'https://www.ons.gov.uk/{link}'
    return full_url


def get_latest_jobs_statistics():
    import datetime
    latestUrl = get_latest_url()
    logging.info(f'Latest URL from ONS is {latestUrl}')
    data = requests.get(latestUrl, headers={'User-Agent': get_user_agent()})
    workbook = xlrd.open_workbook(file_contents=data.content)
    sheet = workbook.sheet_by_name('Adverts by category YoY')
    num_cells = sheet.ncols - 1
    col_vals = [sheet.cell_value(r, 1) for r in range(2, sheet.nrows)]
    it_row = col_vals.index('IT / Computing / Software') + 2

    it_vacancies = sheet.cell_value(it_row + 2, num_cells)
    a1 = sheet.cell_value(rowx=2, colx=num_cells)
    a1_as_datetime = datetime.datetime(*xlrd.xldate_as_tuple(a1, workbook.datemode))

    return [{'label' : 'IT-JOB-VACANCIES',
            'asOfDate' : a1_as_datetime.strftime('%Y-%m-%d'),
            'value' : float(it_vacancies)}]

