
import io
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date, datetime
import logging
from .news_util import get_user_agent

ECONOMIC_QUERY = """SELECT *  FROM `datascience-projects.gcp_shareloader.tmpeconomy` 
                        WHERE LABEL IN  ('Diesel', 'Petrol', 'IT-JOB-VACANCIES',
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
    from bs4 import BeautifulSoup

    current_year = date.today().year

    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/onlinejobadvertestimates'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    anchor = soup.find_all('a')
    links = [a for a in anchor if
             'Download Online job advert estimates' in a.get('aria-label', '') and str(current_year) in a.get('aria-label', '')]
    link = links[0].get('href')
    full_url = f'https://www.ons.gov.uk/{link}'
    return full_url


def get_latest_jobs_statistics():
    import datetime
    import openpyxl
    from io import BytesIO
    latestUrl = get_latest_url()
    logging.info(f'Latest URL from ONS is {latestUrl}')
    data = requests.get(latestUrl, headers={'User-Agent': get_user_agent()})
    workbook = openpyxl.load_workbook(BytesIO(data.content))


    sheet = workbook.get_sheet_by_name('Adverts by category YoY')

    vacancies_names = [(sheet.cell(row=7, column=c).value) for c in range(2, sheet.max_column - 1)]


    it_column, _ = [(idx, v) for idx, v in enumerate(vacancies_names) if 'Computing' in v][0]
    logging.info(f'IT ROWS:{it_column}')

    it_vacancies = sheet.cell(row=sheet.max_row, column=it_column + 2).value
    asOfDate = sheet.cell(row=sheet.max_row, column=1).value

    return {'label' : 'IT-JOB-VACANCIES',
            'asOfDate' : asOfDate.strftime('%Y-%m-%d'),
            'value' : float(it_vacancies)}

