from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import botocore

from moengage.commons.loggers import Treysor


class EmailSender(object):
    #boto3.setup_default_session(profile_name='devops')
    client = boto3.client('ses', region_name='us-east-1')

    @staticmethod
    def sendEmail(subject, body, toAddresses, fromAddress='alerts@moengage.com'):
        try:
            EmailSender.client.send_email(
                Source=fromAddress,
                Destination={
                    'ToAddresses': toAddresses
                },
                Message={
                    'Subject': {
                        'Data': subject
                    },
                    'Body': {
                        'Html': {
                            'Data': body
                        }
                    }
                }
            )
        except botocore.exceptions.ClientError:
            Treysor().exception(action="send_email_alert", subject=subject, toAddresses=toAddresses)

    @staticmethod
    def sendRawEmail(subject, to_addresses, from_address='alerts@moengage.com',
                     body_plain='', body_html='',
                     attachment_file_name=None, attachment_file_path=None):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        if body_plain:
            part = MIMEText(body_plain, 'plain')
            msg.attach(part)
        if body_html:
            part = MIMEText(body_html, 'html')
            msg.attach(part)

        if attachment_file_path:
            part = MIMEApplication(open(attachment_file_path, 'rb').read())
            part.add_header('Content-Disposition', 'attachment', filename=attachment_file_name)
            msg.attach(part)
        try:
            return EmailSender.client.send_raw_email(RawMessage={'Data': msg.as_string()},
                                                     Source=from_address,
                                                     Destinations=to_addresses)
        except botocore.exceptions.ClientError:
            Treysor().exception(action="send_email_alert", subject=subject, toAddresses=to_addresses)
