import json
import ijson
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from ftplib import FTP
from zipfile import ZipFile

from dotenv import load_dotenv
from humanize import naturalsize

load_dotenv()

HOST = os.environ['FTP_HOST']
USER = os.environ['OZON_LOGIN']
PASSWORD = os.environ['OZON_PASSWORD']


def get_ftp_connection(host: str, user: str, password: str):
    """Get instance of FTP class with passed credentials."""
    ftp_conn = FTP(host, user, password)
    print('connected to ftp')

    return ftp_conn


def get_filelist(ftp_conn: FTP, directory: str) -> list:
    """Get list of all files in passed ftp directory."""
    ftp_conn.cwd(directory)

    filenames_raw = []
    ftp_conn.retrlines('LIST', callback=lambda x: filenames_raw.append(x.split()))
    ftp_conn.retrlines('LIST')

    return filenames_raw


def get_sbermm_prices(campaign_id: str) -> dict:
    feed_filename = f'{campaign_id}.xml'
    ftp_conn = FTP(HOST, USER, PASSWORD)
    print('connected to ftp')

    filenames_raw = get_filelist(ftp_conn, 'feeds')

    fn = next(filter(lambda x: x[8] == feed_filename, filenames_raw), None)

    if fn is None:
        print('there is no such feed ({feed_name}).')
        sys.exit(0)

    dt = f'{fn[5]} {fn[6]} {fn[7]}'
    print(f'{feed_filename}\t{dt} - {naturalsize(fn[4])}')

    with open(feed_filename, 'wb') as feed_file:
        ftp_conn.retrbinary(f'RETR {feed_filename}', feed_file.write)
        print('downloaded')

    mytree = ET.parse(feed_filename)
    myroot = mytree.getroot()
    offers: ET.Element = myroot.find('shop').find('offers')

    print(f'prices in feed: {len(offers)}')

    result = {
        'datetime': dt,
        'hit_link': f'https://ftp.puls.ru/feeds2sber/feeds/{feed_filename}',
        'offers': offers,
    }

    return result

    # os.remove(feed_file.name)


def get_sbermm_stores(campaign_id: str) -> dict:
    feed_filename = f'{campaign_id}.xml'
    ftp_conn = FTP(HOST, USER, PASSWORD)
    print('connected to ftp')

    filenames_raw = get_filelist(ftp_conn, 'feeds')

    fn = next(filter(lambda x: x[8].startswith(f'{campaign_id}_outlets'), filenames_raw), None)

    if fn is None:
        print(f'there is no such feed ({fn}).')
        sys.exit(0)

    filename_zip = fn[8]
    dt = f'{fn[5]} {fn[6]} {fn[7]}'
    print(f'{filename_zip}\t{dt} - {naturalsize(fn[4])}')

    with open(filename_zip, 'wb') as feed_file:
        ftp_conn.retrbinary(f'RETR {filename_zip}', feed_file.write)
        print('downloaded')

    with ZipFile(filename_zip, 'r') as zp_file:
        zp_file.extractall()
        print('unzipped')
        filename_json = filename_zip.replace('.zip', '.json')
    os.remove(feed_file.name)

    with open(filename_json) as js_file:
        outlets = json.load(js_file).get('outlets', [])
        print(len(outlets))
    # os.remove(js_file.name)

    print(f"stores in feed: {len(outlets)}")

    result = {
        'datetime': dt,
        'hit_link': f'https://ftp.puls.ru/feeds2sber/feeds/{filename_zip}',
        'outlets': outlets,
    }

    return result


def get_sbermm_stocks(campaign_id: str) -> dict:
    # feed_filename = f'{campaign_id}.xml'
    ftp_conn = FTP(HOST, USER, PASSWORD)
    print('connected to ftp')

    filenames_raw = get_filelist(ftp_conn, 'feeds')

    fn = next(filter(lambda x: x[8].startswith(f'{campaign_id}_stocks_full'), filenames_raw), None)

    if fn is None:
        print(f'there is no such feed ({fn}).')
        sys.exit(0)

    filename_zip = fn[8]
    dt = f'{fn[5]} {fn[6]} {fn[7]}'
    print(f'{filename_zip}\t{dt} - {naturalsize(fn[4])}')

    with open(filename_zip, 'wb') as feed_file:
        ftp_conn.retrbinary(f'RETR {filename_zip}', feed_file.write)
        print('downloaded')

    with ZipFile(filename_zip, 'r') as zp_file:
        zp_file.extractall()
        print('unzipped')
        filename_json = filename_zip.replace('.zip', '.json')
    os.remove(feed_file.name)

    with open(filename_json) as js_file:
        offers = next(ijson.items(js_file, 'outlets.item'))['offers']
    # os.remove(js_file.name)

    print(f"stocks in feed: {len(offers)}")

    result = {
        'datetime': dt,
        'hit_link': f'https://ftp.puls.ru/feeds2sber/feeds/{filename_zip}',
        'offers': offers,
    }

    return result

