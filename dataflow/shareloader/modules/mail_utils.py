import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import apache_beam as beam
from functools import reduce

TEMPLATE = "<tr><td>{TICKER}</td><td>{START_PRICE}</td><td>{END_PRICE}</td><td>{PERFORMANCE}</td><td>{RATINGS}</td><td>{TOTAL_FILLS}</td></tr>"

class MonthlyEmailSender(beam.DoFn):
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
        logging.info('Attepmting to send emamil to:{}'.format(self.recipients))
        logging.info('We are going ot use key:{}'.format(self.key))
        template_start = "<html><body><head><style>table, th, td {border: 1px solid black;}</style>" + \
                    "<table><th>Ticker</th><th>Start Price</th><th>End Price</th>" + \
                    "<th>Performance</th><th>Ratings</th><th>Edgar Fills</th>"
        template_end = "</table></body></html>"
        content = template_start + element + template_end
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud@mmistroni.com',
            #to_emails=self.recipients,
            subject='Monthly Shares Run',
            html_content=content)
        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        try:
            sg = SendGridAPIClient(self.key)

            response = sg.send(message)
            logging.info(response.status_code, response.body, response.headers)
        except Exception as e :
            logging.info('Exception in sending email:{}'.format(str(e)))

def combine_data(elements):
    logging.info('Combining:{}'.format(elements))
    return reduce(lambda acc, current: acc + current, elements, '')


def send_mail(input, options):
    logging.info('Sending emailnow....')
    return (input
            | 'Map to Template' >> beam.Map(lambda row:  TEMPLATE.format(**row) if row else '')
            | 'Combine' >> beam.CombineGlobally(combine_data)
            | 'SendEmail' >> beam.ParDo(MonthlyEmailSender(options.recipients, options.sgridkey))
            )

STOCK_EMAIL_TEMPLATE = """<html>
                      <body>
                        <p> Stock Selection for {asOfDate} </p>
                        <br>
                        <ul>
                            <li>Defensive Stock: Criteria
                                <ul>
                                    <li>LARGE CAP</li>
                                    <li>Current Ratio >=2</li>
                                    <li>Debt Over Capital <0 </li>
                                    <li>Dividend always paid last 20 yrs</li>
                                    <li>EPS Growth past 5 yrs> 33%</li>
                                    <li>EPS always positive for last 5 years</li>
                                    <li>PE Ratio < 15 < 0</li>
                                    <li>PriceToBook Ratio < 1.5 < 0</li>
                                    <li>Institutional Owner Percentage < 60%</li>
                                </ul>
                            </li>
                            <li>Enteprise Stocks: Criteria
                                <ul>
                                    <li>LARGE CAP </li>
                                    <li>Current Ratio >= 1.5</li>
                                    <li>debtOverCapital < 1.2</li>
                                    <li>Some Dividends paid in the past</li>
                                    <li>EPS Growth Past 5 yrs > 0</li>
                                    <li>Positive EPS in past 5 yrs</li>
                                    <li>PE Ratio < 10</li>
                                    <li>Price to Book Ratio < 1.5</li>
                                    <li>InstitutuionalOwnership < 60%</li>
                                </ul>
                            </li>
                            <li>CANSLIM: Criteria
                                <ul>
                                    <li>EPS Growth this year > 20%</li>
                                    <li>EPS Growth next year > 20%</li>
                                    <li>EPS Growth QTR over QTR > 20%</li>
                                    <li>EPS Growth past 5 yrs  > 20%</li>
                                    <li>Gross Profit Margin and ROE > 0 </li>
                                    <li>Price > PriceAvg20  and PriceAvg200</li>
                                </ul>
                            </li>
                            <li>Below 10M stocks: Criteria
                                <ul>
                                    <li>Mid Cap</li>
                                    <li>EPS Growth  this year > 0</li>
                                    <li>EPS Growth qtr over qtr > 20%</li>
                                    <li>Net sales qtr over qtr > 25%</li>
                                    <li>ROE > 15%</li>
                                    <li>Price > PriceAvg200</li>
                                </ul>
                            </li>
                            <li>New Highs: Criteria
                                <ul>
                                    <li>EPS Growth this Year > 0</li>
                                    <li>EPS Growth next Year > 0</li>
                                    <li>Net sales qtr over qtr > 0</li>
                                    <li>Price > PriceAvg 50 , Price > PriceAvg200</li>
                                    <li>Net sales qtr over qtr > 0</li>
                                    <li>Price > All Time High</li>
                                </ul>
                            </li>
                            <li>Asset Play: Criteria
                                <ul>
                                    <li>BookValuePerShare * sharesOutstanding > marketCap</li>
                                    <li>MarketCap is   stockPrice * sharesOutstanding</li>
                                    <li>Idea is that eventually stockPrice will raise to be equal to bookValue per share
                                         
                                    </li>
                                </ul>
                            </li>
                            <li>Out of Favour*: Criteria
                                <ul>
                                    <li>Base Criteria of Defensive / Enterprise Stocks</li>
                                    <li>Price is below 200 days average</li>
                                    <li>Idea is that these are strong companies who are out of favour with investors nowadays</li>
                                </ul>
                            </li>
                        </uls>
                        <br>
                        <br>
                        <table border="1">
                            <thead>
                                <tr>
                                    <th>TICKER</th>
                                    <th>LABEL</th>
                                    <th>PRICE</th>
                                    <th>YEARHIGH</th>
                                    <th>YEARLOW</th>
                                    <th>PRICEAVG50</th>
                                    <th>PRICEAVG200</th>
                                    <th>BOOKVALUEPERSHARE</th>
                                    <th>CASHFLOWPERSHARE</th>
                                    <th>DIVIDEND PAID RATIO</th>
                                    <th>SELECTED NUMBER OF TIMES(LAST_QTR)</th>
                                    <th>STOCK BUY PRICE</th>
                                    <th>STOCK SELL PRICE</th>
                                    <th>EARNING_YIELD</th>
                                    <th>RETURN_ON_CAPITAL</th>
                                    <th>RSI</th>
                                </tr>
                            </thead>
                            <tbody>
                                {tableOfData}
                            </tbody>
                        </table>
                    </body>
                </html>

"""
