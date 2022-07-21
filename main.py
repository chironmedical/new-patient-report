import logging
import os
import datetime
import time
import smtplib
from urllib.parse import quote
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

logger = logging.getLogger('report-generator')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s]  %(name)s: %(message)s', '%Y-%m-%dT%H:%M:%SZ')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

error_logger_handler = logging.FileHandler('error.log')
error_logger_handler.setLevel(logging.ERROR)
error_logger_handler.setFormatter(formatter)

info_logger_handler = logging.FileHandler('info.log')
info_logger_handler.setLevel(logging.INFO)
info_logger_handler.setFormatter(formatter)

logger.addHandler(console)
logger.addHandler(info_logger_handler)
logger.addHandler(error_logger_handler)

onramp_engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (
    quote(os.getenv('ONRAMP_DB_USER', 'cmsonramp')),
    quote(os.getenv('ONRAMP_DB_PASSWORD', 'cmsonramp')),
    os.getenv('ONRAMP_DB_HOST', 'localhost'),
    os.getenv('ONRAMP_DB_PORT', '3306'),
    os.getenv('ONRAMP_DB_NAME', 'cmsonramp'),
))

from_date = datetime.date.today() - datetime.timedelta(days=30)
from_date = datetime.datetime(
    year=from_date.year, month=from_date.month, day=1)
to_date = datetime.date.today()
to_date = datetime.datetime(year=to_date.year, month=to_date.month, day=1)

elapsed_start_time = time.time()
df = pd.read_sql(text('select patient_key, create_dtm from patient where create_dtm >= :from and create_dtm < :to'), onramp_engine.connect(), params={
    'from': from_date,
    'to': to_date
}, parse_dates=['create_dtm'])
elapsed_end_time = time.time()
logger.info('ran query in %.2f seconds', elapsed_end_time-elapsed_start_time)

filename = f'new_patient_{from_date.year}_{from_date.month:02}.xlsx'
df.to_excel(filename)
logger.info('saved query result in %s', filename)

username = os.getenv('SMTP_USERNAME')
password = os.getenv('SMTP_PASSWORD')

content = MIMEMultipart()
content["subject"] = f"{from_date.year}-{from_date.month:02} New Patient Report"
content["from"] = username
content["to"] = os.getenv('RECIPIENTS')

with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:
    with open(filename, 'rb') as zf:
        attachment = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        attachment.set_payload(zf.read())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        content.attach(attachment)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(username, password)
        logger.info('logged in SMTP server for user=%s', username)
        elapsed_start_time = time.time()
        smtp.send_message(content)
        elapsed_end_time = time.time()
        logger.info('payload delivered successfully in %.2f seconds', elapsed_end_time-elapsed_start_time)
        logger.info("%s_%02s new patient report have been generated and sent via email", from_date.year, from_date.month)
