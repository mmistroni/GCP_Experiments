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
                        <ul>
                            <li>Defensive Stock: Criteria
                                <ul>
                                    <li></li>
                                    <li></li>
                                    <li></li>
                                </ul>
                            </li>
                            <li>Enteprise Stocks: Criteria
                                <ul>
                                    <li></li>
                                    <li></li>
                                    <li></li>
                                </ul>
                            </li>
                            <li>CANSLIM: Criteria
                                <ul>
                                    <li></li>
                                    <li></li>
                                    <li></li>
                                </ul>
                            </li>
                            <li>Below 10M stocks: Criteria
                                <ul>
                                    <li></li>
                                    <li></li>
                                    <li></li>
                                </ul>
                            </li>
                            <li>New Highs: Criteria
                                <ul>
                                    <li></li>
                                    <li></li>
                                    <li></li>
                                </ul>
                            </li>
                            <li>Asset Play: Criteria
                                <ul>
                                    <li></li>
                                    <li></li>
                                    <li></li>
                                </ul>
                            </li>
                        </uls>
                        <br>
                        <br>
                        {tableOfData}
                    </body>
                </html>

"""
