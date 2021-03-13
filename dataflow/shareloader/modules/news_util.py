from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, date
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from functools import reduce
import pandas as pd
from pandas.tseries.offsets import BDay
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import apache_beam as beam
from .utils import get_out_of_hour_info

ROW_TEMPLATE =  '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'

def get_news_from_finviz(tickers):
    news_tables = {}
    for ticker in tickers:
        url = 'https://finviz.com/quote.ashx?t={}'.format(ticker)
        req = Request(url=url, headers={'user-agent': 'my-app/0.0.1'})
        response = urlopen(req)
        html = BeautifulSoup(response)
        print('Now parsing...')
        news_table = html.find(id='news-table')
        news_tables[ticker] = news_table

    parsed_news = []
    for file_name, news_table in news_tables.items():
        for x in news_table.findAll('tr'):

            text = x.a.get_text()
            date_scrape = x.td.text.split()
            link = x.a.get('href')

            if len(date_scrape) == 1:
                time = date_scrape[0]

            else:
                date = date_scrape[0]
                time = date_scrape[1]
            ticker = file_name.split('_')[0]

            parsed_news.append([ticker, date, time, text, link])
    return parsed_news

def get_sentiment_from_vader(sentence):
    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores(sentence)
    print("{:-<40} {}".format(sentence, str(score)))
    return score['compound']

def find_news_scores_for_ticker(tickers,  bus_days):
    logging.info('Finding last {} worth of news'.format(bus_days))
    try:
        parsed_news = get_news_from_finviz(tickers)
    except Exception as e:
        print('cant find naything for :{}'.format(tickers))
        return None

    columns = ['ticker', 'date', 'time', 'headline', 'link']
    parsed_and_scored_news = pd.DataFrame(parsed_news, columns=columns)
    parsed_and_scored_news['date'] = pd.to_datetime(parsed_and_scored_news.date).dt.date

    logging.info('Finding last {} worth of news for {} = {}'.format(bus_days, tickers,
                                                                    parsed_and_scored_news.shape))

    yday = date.today() - BDay(bus_days)
    filtered = parsed_and_scored_news[parsed_and_scored_news['date'] >= yday.date()]
    parsed_and_scored_news = filtered.groupby(['ticker'], as_index=False).agg({'headline': ''.join}, Inplace=True)
    scores = parsed_and_scored_news['headline'].apply(get_sentiment_from_vader).tolist()
    scores_df = pd.DataFrame(scores)
    parsed_and_scored_news = parsed_and_scored_news.join(scores_df, rsuffix='_right')
    return parsed_and_scored_news

def df_to_dict(df):
    df_dict = df.to_dict()
    res = dict((k, df_dict[k].get(0, '{}_NA'.format(k))) for k in df_dict.keys())
    logging.info('DF TO DICT is:{}'.format(res))
    return res

def enhance_with_price(dct, iexkey=None):
    nd = dct.copy()
    ticker = dct['ticker']
    logging.info('Enhancing with Price...')
    price, changeout_of_hour = get_out_of_hour_info(iexkey, ticker)
    nd['EXTENDED_PRICE'] = price
    nd['EXTENDED_CHANGE'] = changeout_of_hour
    logging.info('Returning:'.format(nd))
    return nd

class NewsEmailSender(beam.DoFn):
    def __init__(self, recipients, key):
        self.recipients = recipients.split(',')
        self.key = key

    def _build_personalization(self, recipients):
        personalizations = []
        for recipient in recipients:
            logging.info('Adding personalization for {}'.format(recipient))
            person1 = Personalization()
            person1.add_to(Email(recipient))
            personalizations.append(person1)
        return personalizations

    def process(self, element):
        msg = element
        logging.info('Attepmting to send emamil to:{} '.format(self.recipients))
        logging.info('Incoming message is:{}'.format(msg))
        template = \
            "<html><body><p> Today's headlines </p></br><table border='1' cellspacing='0' cellpadding='0' align='center'>" + \
             "<th>Ticker</th><th>Headline</th><th>Score</th><th>Extended Price</th><th>Extended Change</th>{}</table></body></html>"
        content = template.format(msg)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_portfolio_news@mmistroni.com',
            subject='News Sentiment analysis for {}'.format(date.today().strftime('%Y-%m-%d')),
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)
        logging.info('Now sending.....')
        response = sg.send(message)
        logging.info('Mail Sent:{}'.format(response.status_code))
        logging.info('Body:{}'.format(response.body))

def stringify_news(single_news):
    row_template = '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'
    logging.info('Stringifhying:{}'.format(single_news))
    res =  row_template.format(single_news.get('ticker'),
                               single_news.get('headline'),
                               single_news.get(0),
                               single_news.get('EXTENDED_PRICE'),
                               single_news.get('EXTENDED_CHANGE'))
    return res

def combine_news(elements):
    logging.info('Combining:{}'.format(elements))
    logging.info('Item is of type:{}'.format(type(elements)))
    return reduce(lambda acc, current: acc + current, elements, '')


