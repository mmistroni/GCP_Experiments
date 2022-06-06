import csv, urllib.request
import io
import pandas as pd
import csv, requests
import codecs
from bs4 import BeautifulSoup
import requests
from urllib.request import Request, urlopen
from datetime import date

def get_fruit_and_veg_prices():
    baseUrl = 'https://www.gov.uk/government/statistical-data-sets/wholesale-fruit-and-vegetable-prices-weekly-average'
    req = requests.get(baseUrl)
    soup = BeautifulSoup(req.text, "html.parser")
    span = soup.find_all('span', {"class": "download"})[0]
    anchor = span.find_all('a', {"class": "govuk-link"})[0]
    link = anchor.get('href')
    r = requests.get(link).text
    dt = pd.read_csv(io.StringIO(r), header=0)
    dt['asOfDate'] = pd.to_datetime(dt['date'], infer_datetime_format=True)
    return dt[dt.asOfDate == dt.asOfDate.max()][['date', 'category', 'item', 'variety', 'price', 'unit']]

def get_petrol_prices():
    url = 'https://www.gov.uk/government/statistics/weekly-road-fuel-prices'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    span = soup.find_all('span', {"class": "download"})[0]
    anchor = span.find_all('a', {"class": "govuk-link"})[0]
    link = anchor.get('href')
    r = requests.get(link).text
    dt = pd.read_csv(io.StringIO(r), header=2)[-1:][['Date','ULSP', 'ULSD']]
    dt.set_index('Date', inplace=True)
    return dt

def get_latest_url():
    url = "https://cy.ons.gov.uk/datasets/online-job-advert-estimates/editions"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")
    anchor = soup.find_all('a', {"id": "edition-time-series"})[0]
    suffix = anchor.get('href')
    return f'https://download.ons.gov.uk/downloads{suffix}.csv'


def get_latest_jobs_statistics():
    latestUrl = get_latest_url()
    print(f'Latest URL from ONS is {latestUrl}')
    res = requests.get(latestUrl, headers={'User-Agent': 'Mozilla/5.0'})
    # 'https://download.ons.gov.uk/downloads/datasets/online-job-advert-estimates/editions/time-series/versions/20.csv'
    text = res.iter_lines()
    data = csv.reader(codecs.iterdecode(text, 'utf-8'), delimiter=',')
    headers = ['v4_1',	'Data Marking', 	'calendar-years',	'Time',	'uk-only',	'Geography', 'adzuna-jobs-category',	'AdzunaJobsCategory',	'week-number',	'Week']
    dataset = [d for d in data]
    jobs_dataset = pd.DataFrame(dataset, columns=headers)
    valid = jobs_dataset[(jobs_dataset.Time == str(date.today().year)) & (jobs_dataset.AdzunaJobsCategory.str.contains('Computing'))]
    valid[['wk', 'wkno']] = valid['week-number'].str.split(pat = '-', expand = True)
    filtered = valid[valid.v4_1.str.contains('.')]
    filtered['wkint'] = filtered.wkno.apply(lambda v: int(v))
    filtered = filtered.rename(columns={'v4_1': 'jobs'})[['calendar-years', 'wkint', 'adzuna-jobs-category', 'jobs']]
    return filtered.sort_values(by=['wkint'])
