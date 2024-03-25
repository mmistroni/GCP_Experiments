
import io
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date, datetime
import logging
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


def get_fruit_and_veg_prices():
    baseUrl = 'https://www.gov.uk/government/statistical-data-sets/wholesale-fruit-and-vegetable-prices-weekly-average'
    req = requests.get(baseUrl)
    soup = BeautifulSoup(req.text, "html.parser")
    spans = soup.find_all('div', {"class": "gem-c-attachment__details"})
    if len(spans) > 0:
        try:
            for span in spans:
                anchor = span.find_all('a', {"class": "govuk-link"})[0]
                link = anchor.get('href')
                if link.endswith('csv'):
                    r = requests.get(link).text
                    dt = pd.read_csv(io.StringIO(r), header=0)
                    dt['asOfDate'] = pd.to_datetime(dt['date'], infer_datetime_format=True).dt.date
                    latest = dt[dt.asOfDate == dt.asOfDate.max()][['asOfDate', 'category', 'item', 'variety', 'price', 'unit']]
                    latest['label'] = latest.apply(lambda r: f"{r['category']}-{r['item']}-{r['variety']}({r['unit']})", axis=1)
                    latest['value'] = latest.price.apply(lambda valstr: float(valstr))
                    return latest[['asOfDate', 'label', 'value']].to_dict('records')
                else:
                    logging.info(f'Wrong link:{link}')
        except Exception as e:
            logging.info(f'exception in retrieving fruit and veg:{str(e)}')

    return []

def get_petrol_prices():
    try:
        url = 'https://www.gov.uk/government/statistics/weekly-road-fuel-prices'
        req = requests.get(url)
        soup = BeautifulSoup(req.text, "html.parser")

        spans = soup.find_all('div', {"class": "gem-c-attachment__details"})
        if len(spans) > 0:
            for span in spans:
                anchor = span.find_all('a', {"class": "govuk-link"})[0]
                link = anchor.get('href')
                if link.endswith('csv'):
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
    except Exception as e:
        logging.info(f'Problem in getting petro lprices {str(e)}')
        return []

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
    if links:
        link = links[0].get('href')
        full_url = f'https://www.ons.gov.uk/{link}'
        return full_url


def get_latest_jobs_statistics():
    import datetime
    import openpyxl
    from io import BytesIO
    ## TODO need to amennd this
    latestUrl = get_latest_url()
    if latestUrl:
        logging.info(f'Latest URL from ONS is {latestUrl}')

        df = pd.read_excel(
            latestUrl,
            sheet_name='Adverts by category YoY',
            storage_options={'User-Agent': 'Mozilla/5.0'}, header=6)

        data = df[['Date', 'IT / Computing / Software']].tail(1).to_dict('records')[0]



        return {'label' : 'IT-JOB-VACANCIES',
                'asOfDate' : data['Date'].date().strftime('%Y-%m-%d'),
                'value' : float(data['IT / Computing / Software'])
                }
    return {'label' : 'IT-JOB-VACANCIES',
            'asOfDate' : date.today().strftime('%Y-%m-%d'),
                'value' : float(0)}



def get_card_spending():
    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/ukspendingoncreditanddebitcards'
    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(req.text, "html.parser")
    hrefs = soup.find_all('a')
    latest = [link.get('href').split('?')[1] for link in hrefs if
              link.get('aria-label') is not None and 'Download' in link.get('aria-label')][0]
    dataset_url = f'https://www.ons.gov.uk/file?{latest}'

    latest = pd.read_excel(dataset_url,
                           storage_options={'User-Agent': 'Mozilla/5.0'}, sheet_name='Weekly CHAPS indices SA',
                           header=3).tail(1).to_dict('records')[0]

    cob = latest['Date'].strftime('%Y-%m-%d')
    latest_value = latest['Aggregate']

    return [{'label': 'CREDIT-DEBIT-SPENDING',
            'asOfDate': cob,
            'value': latest_value}]

def get_gas_prices():
    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/systemaveragepricesapofgas'

    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(req.text, "html.parser")
    hrefs = soup.find_all('a')
    latest = [link.get('href').split('?')[1] for link in hrefs if
              link.get('aria-label') is not None and 'Download' in link.get('aria-label')][0]
    dataset_url = f'https://www.ons.gov.uk/file?{latest}'

    latest = pd.read_excel(dataset_url,
                           storage_options={'User-Agent': 'Mozilla/5.0'}, sheet_name='Data', header=4)
    colnames = latest.columns[0:3]
    last_record = latest[colnames].tail(1).to_dict('records')[0]

    cob = last_record['Date'].strftime('%Y-%m-%d')
    latest = last_record['SAP actual day\n(p/kWh)']
    rolling = last_record['SAP preceding seven-day rolling average\n(p/kWh)']

    return [{'label': 'GAS-PRICES',
     'asOfDate': cob,
     'value': latest}]


def get_electricity_prices():
    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/systempriceofelectricity'

    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(req.text, "html.parser")
    hrefs = soup.find_all('a')
    latest = [link.get('href').split('?')[1] for link in hrefs if
              link.get('aria-label') is not None and 'Download' in link.get('aria-label')][0]
    dataset_url = f'https://www.ons.gov.uk/file?{latest}'

    latest = pd.read_excel(dataset_url,
                           storage_options={'User-Agent': 'Mozilla/5.0'}, sheet_name='Data', header=3)
    colnames = latest.columns[0:3]

    print(colnames)
    last_record = latest[colnames].tail(1).to_dict('records')[0]

    cob = last_record['Date'].strftime('%Y-%m-%d')
    latest = last_record['Daily average']

    return [{'label': 'ELECTRICITY-PRICES',
     'asOfDate': cob,
     'value': latest}]


def get_redundancies_notifications():

    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/advancednotificationofpotentialredundancies'

    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(req.text, "html.parser")
    hrefs = soup.find_all('a')
    latest = [link.get('href').split('?')[1] for link in hrefs if
              link.get('aria-label') is not None and 'Download' in link.get('aria-label')][0]
    dataset_url = f'https://www.ons.gov.uk/file?{latest}'

    latest = pd.read_excel(dataset_url,
                           storage_options={'User-Agent': 'Mozilla/5.0'}, sheet_name='Data', header=4)
    colnames = latest.columns[0:3]

    print(colnames)
    last_record = latest[colnames].tail(1).to_dict('records')[0]

    cob = last_record['Date'].strftime('%Y-%m-%d')
    latest = last_record['Potential redundancies']

    return [{'label': 'POTENTIAL-REDUNDANCIES',
     'asOfDate': cob,
     'value': latest}]


def get_company_dissolutions():
    url = 'https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/companyincorporationsandvoluntarydissolutions'

    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(req.text, "html.parser")
    hrefs = soup.find_all('a')
    latest = [link.get('href').split('?')[1] for link in hrefs if
              link.get('aria-label') is not None and 'Download' in link.get('aria-label')][0]
    dataset_url = f'https://www.ons.gov.uk/file?{latest}'

    latest = pd.read_excel(dataset_url,
                           storage_options={'User-Agent': 'Mozilla/5.0'}, sheet_name='Compulsory Dissolutions', header=4)
    colnames = latest.columns[0:3]

    print(colnames)
    last_record = latest[colnames].tail(1).to_dict('records')[0]

    cob = last_record['Week ending to'].strftime('%Y-%m-%d')
    latest = last_record['Weekly']

    return [{'label': 'COMPANY-DISSOLUTIONS',
     'asOfDate': cob,
     'value': latest}]









'''
New data to fetch

For loan delinquencies, we publish the direct debit failure rate provided from Vocalink and Pay.UK data.  This is broken down in the main categories of direct debit payments e.g. energy, mortgage ect.
https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/monthlydirectdebitfailurerateandaveragetransactionamount
For businesses bankrupt we do not publish this exactly, but we do publish data from Companies House about company dissolutions which may be of use -
https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/companyincorporationsandvoluntarydissolutions
We also publish the number of companies proposing redundancies, which may be another indicator of use –
https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/advancednotificationofpotentialredundancies
Our full list of published datasets can be found here -

https://www.ons.gov.uk/economy/economicoutputandproductivity/output/bulletins/economicactivityandsocialchangeintheukrealtimeindicators/29february2024/relateddata




'''


