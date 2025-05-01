### Launcher Pipelines
import apache_beam as beam

from datetime import date
import argparse
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization
import logging

class EmailSender(beam.DoFn):
    def __init__(self, key):
        self.recipients = ['mmistroni@gmail.com']
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

        key, value_dict = element
        s_list = list(value_dict['collection1'])
        sec_list = list(value_dict['collection2'])
        stocks = list(value_dict['collection1'])[0].replace('\n', '') if s_list else ''
        sectors = list(value_dict['collection2'])[0].replace('\n', '') if sec_list else ''
        try:
            llm = list(value_dict['collection3'])[0].replace('\n', '<br>')
        except Exception as e:
            logging.info(f'Faile dto process llm:{str(e)}')
            llm = str(e)
        logging.info('Attepmting to send emamil to:{self.recipient} with diff {msg}')

        head_str  = '''
                    <head>
                        <style>
                            th, td {
                                text-align: left;
                                vertical-align: middle;
                                width: 25%;
                                padding: 8px;
                            }
                        </style>
                  </head>

                    '''

        template = \
            '''<html>
                  {}
                  <body>
                    <table>
                        <th>Name</th><th>Perf Week</th><th>Perf Month</th><th>Perf Quart</th><th>Perf Half</th><th>Perf Year</th><th>Recom</th><th>Avg Volume</th><th>Rel Volume</th>
                        {}
                    </table>
                    <br>
                    <table>
                       <th>WATCH</th><th>Ticker</th><th>PrevDate</th><th>Prev Close</th><th>Last Date</th><th>Last Close</th><th>Change</th><th>Adx</th><th>RSI</th><th>SMA20</th><th>SMA50</th><th>SMA200</th><th>Broker</th>
                       {}
                    </table>
                    <hr/>
                    <p>{}</p>
                </html>'''
        content = template.format(head_str, sectors, stocks, llm)
        logging.info('Sending \n {}'.format(content))
        message = Mail(
            from_email='gcp_cloud_mm@outlook.com',
            subject='Pre-Market Movers',
            html_content=content)

        personalizations = self._build_personalization(self.recipients)
        for pers in personalizations:
            message.add_personalization(pers)

        sg = SendGridAPIClient(self.key)

        response = sg.send(message)
        logging.info('Mail Sent:{}'.format(response.status_code))
        logging.info('Body:{}'.format(response.body))


def send_email(pipeline,  sendgridkey):
    return (pipeline | 'SendEmail' >> beam.ParDo(EmailSender(sendgridkey))
             )




