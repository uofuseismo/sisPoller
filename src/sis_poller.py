#!/usr/bin/env python3
# Purpose: Polls SIS to check for metadata updates.
# Author: Ben Baker (University of Utah) distributed under the MIT
#         license.
import os
import argparse
import sys
import numpy as np
import pandas as pd
import logging
import datetime
import psycopg
from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import TIMESTAMP as sql_timestamp
from sqlalchemy import Text as sql_text
from sqlalchemy import Integer as sql_integer
from sqlalchemy import Column as sql_column
from sqlalchemy.sql import exists    
from email.message import EmailMessage
import smtplib

class XMLUpdate(declarative_base()):
    __tablename__ = "xml_update"
    xml_file = sql_column(sql_text, primary_key = True, nullable = False) 
    last_modified = sql_column(sql_timestamp, nullable = False) 
    def __repr__(self):
        return "<xml_update(xml_file={}, last_modified={})>".format(self.xml_file, self.last_modified) 

def create_email_message(subject : str,
                         contents : str,
                         to_address : str,
                         from_address : str = os.uname()[1]):
    """ 
    Creates the email message.

    subject : str
       The subject - e.g., SIS updates
    contents : str
       The contents - e.g., The automated email you recently received for event 60369962 was incorrect.
    to_address : str
       The recipient - e.g., alarm.at.seis.utah.edu
    from_address : str
       The originator address - e.g., aqmsrt1.at.seis.utah.edu 
    """
    if (subject is None):
        raise Exception("The subject is not defined")
    if (contents is None):
       raise Exception("The contents are not defined")
    if (to_address is None):
        raise Exception("The to_address is not defined")
    if (from_address is None):
        raise Exception("The from address is not defined")
    email_message = EmailMessage()
    email_message['Subject'] = subject
    email_message['From'] = from_address
    email_message['To'] = to_address 
    email_message.set_content(contents)
    return email_message

def send_email(message):
    """
    Sends the message.
 
    message : EmailMessage()
        The email message to send.  This should have all requisite information
        defined for the SMTP utility to send the message from the localhost.
    """
    with smtplib.SMTP('localhost') as s:
        s.send_message(message)
        s.quit()

def initialize_table(df, engine, search_path = None, logger = None, delete_table = True):
    Session = sessionmaker(bind = engine.connect())
    session = Session()
    if (search_path is not None):
        session.execute(text("SET search_path TO {}".format(search_path)))
    if (delete_table):
        if (logger):
            logger.info("Deleting xml_update table...")
        delete_statement = (delete(XMLUpdate).where())
        session.execute(delete_statement)
        session.commit()
 
    df.sort_values(['xml_file'], inplace = True)
    for i in range(len(df)):
        row = df.iloc[i]
        xml_update = XMLUpdate(xml_file = row['xml_file'],
                               last_modified = row['last_modified'])
        if (logger):
            logger.debug("Adding {}".format(xml_update))
        session.add(xml_update)
    session.commit()
    if (logger):
        logger.info("Successfully initialized xml_update table!")
    return '' 

def update_table(df, engine, search_path = None, logger = None):
    """
    Updates the XML timestamp table and reports on any stations that have
    metadata changes.
    """
    Session = sessionmaker(bind = engine.connect())
    session = Session()
    if (search_path is not None):
        session.execute(text("SET search_path TO {}".format(search_path)))
    update_iw = False
    update_us = False
    update_uu = False
    update_nn = False
    update_pb = False
    update_wy = False
    email_text = ''
    df.sort_values(['xml_file'], inplace = True)
    have_update = False
    n_updates = 0
    n_expected_updates = 0
    n_new = 0
    for i in range(len(df)):
        row = df.iloc[i]
        xml_update = XMLUpdate(xml_file = row['xml_file'],
                               last_modified = row['last_modified'])
        xml_file_exists = False
        try:
            xml_file_exists = session.query(exists().where(XMLUpdate.xml_file == xml_update.xml_file)).scalar()
        except Exception as e:
            if (logger is not None):
                logging.warning(e)
        if (not xml_file_exists):
            logging.debug("Adding {}".format(xml_update.xml_file))
            session.add(xml_update)
            n_new = n_new + 1
            email_text = email_text + "Adding: {}\n".format(xml_update.xml_file)
            have_update = True
        else:
            is_new = session.query(exists().where(
                 XMLUpdate.xml_file == xml_update.xml_file).where(
                 XMLUpdate.last_modified < xml_update.last_modified)).scalar()
            if (is_new):
                if ('IW_' in xml_update.xml_file):
                    update_iw = True
                elif ('NN_' in xml_update.xml_file):
                    update_nn = True
                elif ('UU_' in xml_update.xml_file):
                    update_uu = True
                elif ('PB_' in xml_update.xml_file):
                    update_pb = True
                elif ('US_' in xml_update.xml_file):
                    update_us = True
                elif ('WY_' in xml_update.xml_file):
                    update_wy = True
                else:
                    logging.warn("Unhandled network in file {}".format(xml_update.xml_file))
                email_text = email_text + "Updated: {}\n".format(xml_update.xml_file)
                update_statement = update(XMLUpdate).where(XMLUpdate.xml_file == xml_update.xml_file).values(last_modified = xml_update.last_modified)
                try:
                    result = session.execute(update_statement)
                    n_updates = n_updates + result.rowcount
                    have_update = True
                except Exception as e:
                    logging.error("Failed to update {} because {}".format(xml_update.xml_file, e))
                n_expected_updates = n_expected_updates + 1
    if (have_update):
        logging.debug("Committing updates...")
        session.commit()
        logging.info("Adding {} stations".format(n_new))
        logging.info("Updated {} stations (expected {} updates)".format(n_updates, n_expected_updates))
    else:
        logging.info("Nothing to update")
    if (update_uu):
        logging.info("Updated UU")
    if (update_wy):
        logging.info("Updated WY")
    if (update_iw):
        logging.info("Updated IW")
    if (update_nn):
        logging.info("Updated NN")
    if (update_pb):
        logging.info("Updated PB")
    if (update_us):
        logging.info("Updated US")
    return email_text

def fetch_xml_last_update_table(url, keeper_list = None):
    a = urlopen(url).read()
    soup = BeautifulSoup(a, 'html.parser')
    table = soup.find("table") #, { "class" : "lineItemsTable" })
    ds = []
    for row in table.findAll("tr"):
        cells = row.findAll("td")
        if (len(cells) == 5):
            station = cells[1].find(string=True)
            if (keeper_list is not None):
                keep = False
                for keeper_station in keeper_list:
                    if (keeper_station == station):
                        keep = True
                        break
                if (not keep):
                    continue
            last_modified = cells[2].find(string=True).strip()
            if (len(last_modified) > 0):
                timestamp = datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M')
                ds.append({'xml_file' : station,
                           'last_modified' : timestamp}) 
    # Loop
    df = pd.DataFrame(ds)
    return df

def send_message(host : str,
                 api_key : int,
                 topic : str,
                 alarm_name : str,
                 subject : str,
                 message : str,
                 message_identifier : str,
                 endpoint : str = 'Email',
                 source : str = os.uname()[1].split('.')[0],
                 logger = None):
    """ 
    Sends the SMS alarm to the API gateway.  

    Parameters
    ----------
    host : str
        The address of the API.
    api_key : str
        The API's access key.
    topic : str
        The SNS tpoic at AWS to which to publish this message.
        This can be 'test' or 'production'.
    subject : str
        The email's subject.
    message : str
        The 1400 character or less string to send to the users.
        If the message exceeds this length then it will be broken
        into 140 character pieces by AWS SNS's SMS client.
    message_identifier : str
        A unique message identifier.  This could be used for deduplication if
        the SNS is a FIFO queue.
    endpoint : str
        Something to append to the host API URL - e.g., 'SMS' or 'Email'.
    source : str
        The source machine sending the message.
    logger 
        The logger from Python's logging utility, e.g., import logging.
    """

    data = {'subject': subject,
            'message': message,
            'topic': topic.lower(),
            'messageIdentifier': message_identifier,
            'source': source,
           }
    headers = {'x-api-key': api_key,
               'Content-Type': 'application/json',
               'Accept' : 'application/json'}
    api_url = host
    if (len(endpoint) > 0):
        api_url = os.path.join(api_url, endpoint)
    if (logger):
        logger.info("Sending {} to {}...".format(alarm_name, endpoint) )
    response = requests.put(api_url, headers = headers, json = {'payload': data})
    try:
        json_info = response.json().dumps()
    except:
        json_info = ''
    if (response.ok):
        logger.info("Successfully submitted message to API.  {}".format(json_info))
    else:
        if (response.status_code == 403):
            raise Exception("Failed to access API with credentials")
        raise Exception("API request failed with {} {}".format(response.status_code, json_info))


def main():    
    parser = argparse.ArgumentParser(description = '''
Queries SIS and, if there was an update, sends an email to interested parties.\n

Example to check for StationXML updates using Postgres\n
    poller.py --recipient=user@domain1.com,user@domain2.com

Example to initialize StationXML update database\n
    poller.py -i

Example to check for StationXML updates using sqlite3:\n
    poller.py --sqlite3_file=xmlUpdates.sqlite3 --recipient=user@domain.com --verbosity=2\n

Note, to use Postgres you must set the following Postgres database environment variables:
    SIS_POLLER_READ_WRITE_USER
    SIS_POLLER_READ_WRITE_PASSWORD
    SIS_POLLER_DATABASE
    SIS_POLLER_HOST (by default this is localhost)
    SIS_POLLER_SCHEMA
    SIS_POLLER_PORT (by default this is 5432)

''',
                                   formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-s', '--sqlite3_file',
                        type = str,
                        help = 'If specified, then the updates are with respect to this sqlite3 file',
                        default = None) #os.path.join(os.getcwd(), 'sisXMLUpdates.sqlite3'))
    parser.add_argument('-r', '--recipient',
                        type = str,
                        help = 'The email recipeint of the poller',
                        default = None)
    parser.add_argument('-l', '--log_directory',
                        type = str,
                        help = 'The directory to which log file will be written',
                        default = os.path.join(os.getcwd(), 'logs'))
    parser.add_argument('-i', '--initialize_database',
                        action = argparse.BooleanOptionalAction,
                        help = 'If true then this will initialize the database and no update emails will be sent',
                        default = False)
    parser.add_argument('--station_xml_url',
                        type = str,
                        help = 'The URL where the SIS stationXML is housed',
                        default = 'https://files.anss-sis.scsn.org/production/FDSNStationXML1.1/')
    parser.add_argument('-t', '--test',
                        action = argparse.BooleanOptionalAction,
                        help = 'issue a test message',
                        default = False)
    parser.add_argument('-v', '--verbosity',
                        type = int,
                        help = 'controls the verbosity (1 errors/warnings, 2 errors/warnings/info, 3 errors/warnings/info/debug)',
                        default = 2)
    parser.add_argument('--email_url',
                        type = str,
                        help = 'the URL endpoint of the AWS email server',
                        default = os.getenv('AWS_API_SIS_URL'))
    parser.add_argument("--email_api_key',
                        type = str,
                        help = 'the corresponding API Key for the AWS email server',
                        default = os.getenv('AWS_API_SIS_ACCESS_KEY'))
    parser.add_argument("--email_topic',
                        type = str,
                        help = 'the SNS topic - this can be test or production',
                        default = 'production')
    args = parser.parse_args()

    if (not os.path.exists(args.log_directory)):
        os.makedirs(args.log_directory)
    assert os.path.exists(args.log_directory)

    log_file = os.path.join(args.log_directory, 'poller.log')

    base_stationxml_url = args.station_xml_url 

    log_level = logging.CRITICAL
    if (args.verbosity == 2):
        log_level = logging.INFO
    elif (args.verbosity >= 3):
        log_level = logging.DEBUG
    else:
        log_level = logging.CRITICAL

    logging.basicConfig(filename = log_file,
                        filemode = 'a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level = log_level,
                        datefmt='%Y-%m-%d %H:%M:%S')
    subject = 'SIS Test'
    update_message = ''
    if (not args.test):
        subject = 'SIS Update'
        message_identifier = 'sisUpdateMessage_{}'.format(np.random.randint(0, high=1000000))
        search_path = None
        if (args.sqlite3_file is not None):
            logging.info("Attempting to use sqlite3")
            if (not os.path.exists(args.sqlite3_file)):
                logging.error("sqlite3 file {} does not exist".format(args.sqlite3_file)) 
                sys.exit(1)
            try:
                engine = create_engine("sqlite:///" + args.sqlite3_file)
            except Exception as e:
                logging.error("sqlite3 failed with {}".format(e))
                sys.exit(1) 
        else:
            postgres_user = os.getenv("SIS_POLLER_READ_WRITE_USER")
            postgres_password = os.getenv("SIS_POLLER_READ_WRITE_PASSWORD")
            postgres_database = os.getenv("SIS_POLLER_DATABASE")
            postgres_schema = os.getenv("SIS_POLLER_SCHEMA")
            postgres_host = os.getenv("SIS_POLLER_HOST")
            postgres_port = os.getenv("SIS_POLLER_PORT")
            search_path = postgres_schema
            if (postgres_port is None):
                postgres_port = 5432
            else:
                postgres_port = int(postgres_port)
            if (postgres_host is None):
                postgres_host = 'localhost'
            logging.debug("Connecting to postgres...")
            connection_uri \
                = "postgresql+psycopg://{}:{}@{}:{}/{}?connect_timeout=20".format(
                     postgres_user, postgres_password,
                     postgres_host, postgres_port,
                     postgres_database) 
            try:
                engine = create_engine(connection_uri)
                logging.info("Connected to postgres database {}.{}".format(postgres_schema, postgres_database))
            except Exception as e:
                logging.critical("postgres failed with {}".format(e))
                sys.exit(1)

        uu_url = os.path.join(base_stationxml_url, 'UU/')
        wy_url = os.path.join(base_stationxml_url, 'WY/')
        iw_url = os.path.join(base_stationxml_url, 'IW/')
        us_url = os.path.join(base_stationxml_url, 'US/') 
        iw_keeper_list = ['IW_FLWY.xml',
                          'IW_IMW.xml',
                          'IW_LOHW.xml',
                          'IW_MOOW.xml',
                          'IW_REDW.xml',
                          'IW_RWWY.xml',
                          'IW_SNOW.xml',
                          'IW_TPAW.xml']
        us_keeper_list = ['US_AHID.xml',
                          'US_BOZ.xml',
                          'US_BW06.xml',
                          'US_DUG.xml',
                          'US_ELK.xml',
                          'US_HLID.xml',
                          'US_HWUT.xml',
                          'US_ISCO.xml',
                          'US_LKWY.xml',
                          'US_MVCO.xml',
                          'US_TPNV.xml',
                          'US_WUAZ.xml']
        logging.debug("Fetching UU SIS timestamps...")
        df = fetch_xml_last_update_table(uu_url, keeper_list = None)
        logging.debug("Fetching WY SIS timestamps...")
        df = pd.concat([df, fetch_xml_last_update_table(wy_url, keeper_list = None)])
        logging.debug("Fetching IW SIS timestamps...")
        df = pd.concat([df, fetch_xml_last_update_table(iw_url, keeper_list = iw_keeper_list)])
        logging.debug("Fetching US SIS timestamps...")
        df = pd.concat([df, fetch_xml_last_update_table(us_url, keeper_list = us_keeper_list)])
        df['last_modified'] = df['last_modified'].astype(np.int64)*1.e-9
        df['last_modified'] = df['last_modified'].astype(int) # These are low-resolution on the website
        df['last_modified'] = pd.to_datetime(df['last_modified'], unit='s')

        try:
            if (args.initialize_database):
                logging.info("Initializing database tables...")
                initialize_table(df, engine, search_path = search_path, logger = logging)
                update_message = ''
            else:
                logging.debug("Updating tables...")
                update_message = update_table(df, engine, search_path = search_path, logger = logging)
        except Exception as e:
            logging.error("Failed to update XML table.  Failed with {}".format(e))
            sys.exit(1)
    else:
        logging.info("Creating test email message")
        subject = 'SIS Test'
        update_message = 'Test email from SIS poller'
        message_identifier = 'sisTestMessage_{}'.format(np.random.randint(0, high=1000000))

    # Send email - if necessary
    if (len(update_message) > 0):
        try:
            logging.info("Emailing {}".format(args.recipient))
            send_message(host = args.email_url,
                         api_key = args.api_key,
                         topic = args.email_topic,
                         subject = subject,
                         message = update_message,
                         message_identifier = message_identifier,
                         endpoint = 'Email',
                         source = source,
                         logger = logging)

            """
            email_message = create_email_message(subject = 'SIS Updates',
                                                 contents = update_message,
                                                 to_address = args.recipient,
                                                 from_address = os.uname()[1])
            logging.debug("Sending message {}".format(email_message))
            #send_email(email_message)
            """
        except Exception as e:
            logging.error("Failed sending email with error {}".format(e))
            sys.exit(1) 
    sys.exit(0)

if __name__ == "__main__":
    main()
