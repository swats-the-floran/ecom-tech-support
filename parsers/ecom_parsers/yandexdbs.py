import json
import os
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Union

from dotenv import load_dotenv
from humanize import naturalsize
from sqlalchemy.orm import Session

from .base_mp import All1CParser
from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import (
    Base,
    PriceBase,
    StockBase,
)
from utils.ecom_elastic import get_stocks_yandexdbs_hits, get_stores_yandexdbs_hits
from utils.ecom_ftp import get_filelist, get_ftp_connection
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    generate_elk_query_link,
    get_datetimes,
    parse_datetime,
)

load_dotenv()

HOST = os.environ['FTP_HOST']
USER = os.environ['YANDEX_LOGIN']
PASSWORD = os.environ['YANDEX_PASSWORD']


@dataclass
class PriceYandex(PriceBase):
    # price_guid: Union[str, int] = ''
    price: int = 0
    hit_link: str = ''


@dataclass
class StockYandex(StockBase):
    endpoint: str = ''
    price_guid: Union[str, int] = ''
    region: str = ''
    hit_link : str = ''


@dataclass
class StoreYandex(Base):
    method_name: str = ''
    org_name: str = ''
    visibility: str = ''
    delivery_rules: str = ''
    outlet: str = ''
    address: str = ''
    hit_link: str = ''


_mp_settings = StandardMarketplaceSettings(
    stocks_instead_prices = False,
    moduleb2c=True,
)


class YandexdbsParser(All1CParser):

    def __init__(self,
        pg_session: Session,
        transaction_dt: str,
        marketplace: str,
        org_identifier: str,
        store_identifier: str = '',
        product_identifier: str = '',
    ) -> None:
        super().__init__(
            pg_session,
            transaction_dt,
            marketplace,
            org_identifier,
            store_identifier,
            product_identifier,
        )
        self.dt_price_mp = PriceYandex
        self.dt_stock_mp = StockYandex
        self.dt_store_mp = StoreYandex

        self.mp_settings = _mp_settings

    def _get_feed_prices(self):
        ftp_conn = get_ftp_connection(HOST, USER, PASSWORD)
        filenames_raw = get_filelist(ftp_conn, 'feeds')

        campaign_id = self.org_data['campaign_id']
        feed_filename = f'{campaign_id}.yml'
        fn = next(filter(lambda x: x[8] == f'{campaign_id}.yml', filenames_raw), None)

        if fn is None:
            print(f'there is no such feed ({fn}).')
            sys.exit(0)

        dt = f'{fn[5]} {fn[6]} {fn[7]}'
        print(f'{feed_filename}\t{dt} - {naturalsize(fn[4])}')

        with open(feed_filename, 'wb') as feed_file:
            ftp_conn.retrbinary(f'RETR {feed_filename}', feed_file.write)
            print('downloaded')

        mytree = ET.parse(feed_filename)
        myroot = mytree.getroot()
        prices: ET.Element = myroot.find('shop').find('offers')

        print(f'prices in feed: {len(prices)}')

        result = {
            'datetime': datetime.now(),
            'hit_link': f'https://ftp.puls.ru/feeds2yandex/feeds/{feed_filename}',
            'prices': prices,
        }

        return result

    def get_mp_prices(self) -> Generator[PriceYandex, None, None]:
        """Get prices sent from ecom to yandex market mp.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting yandexdbs data...')

        prices_count = 0
        offers = self._get_feed_prices()
        dt = offers['datetime']
        hit_link = offers['hit_link']
        prices_raw = offers['prices']

        for price_raw in prices_raw:
            if not self.product_identifiers or price_raw.attrib['id'] in self.product_identifiers:
                price = self.dt_price_mp(
                    direction='  e->',
                    datetime=dt,
                    org_name=self.passed_org_name,
                    product_identifier='\"' + str(price_raw.attrib['id']) + '\"',
                    price=price_raw.find('price').text,
                    hit_link=hit_link,
                )
                yield price
                prices_count += 1

        print(f'results: {prices_count}')

    def get_mp_stocks(self) -> Generator[StockYandex, None, None]:
        """Get and parse mp stocks data from elastic.

        Returns:
            List of stocks which are instances of corresponding dataclass.
        """
        print('\n' 'getting yandex data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 3)
        org_name_latin = self.org_data['org_name_latin']
        campaign_id = self.org_data['campaign_id']

        results_count = 0
        hits = get_stocks_yandexdbs_hits(begin_dt, end_dt, self.marketplace, campaign_id, org_name_latin)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            hit_custom = hit.transaction.custom
            if hasattr(hit_custom, 'request_data'):
                stocks_raw = json.loads(hit_custom.request_data)['skus']
                endpoint_type = 'push /stocks'
            else:
                stocks_raw = json.loads(hit_custom.response_content)['cart']['items']
                endpoint_type = '/cart'

            for stock_raw in stocks_raw:
                if endpoint_type == '/cart':
                    if not self.product_identifiers or stock_raw['offerId'] in self.product_identifiers:
                        stock = self.dt_stock_mp(
                            direction='  e->',
                            datetime=convert_timezone(hit_datetime, 'msc'),
                            endpoint=endpoint_type,
                            product_identifier='"' + stock_raw['offerId'] + '"',
                            quantity=stock_raw['count'],
                            price_guid=stock_raw['feedId'],
                            org_name=self.passed_org_name,
                            hit_link=hit_link,
                        )

                        yield stock
                        results_count += 1

                elif endpoint_type == 'push /stocks':
                    if not self.product_identifiers or stock_raw['sku'] in self.product_identifiers:
                        stock = self.dt_stock_mp(
                            direction='  e->',
                            datetime=convert_timezone(hit_datetime, 'msc'),
                            endpoint=endpoint_type,
                            product_identifier='"' + stock_raw['sku'] + '"',
                            quantity=stock_raw['items'][0]['count'],
                            org_name=self.passed_org_name,
                            hit_link=hit_link,
                        )

                        yield stock
                        results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'stocks_mp',
            begin_dt,
            end_dt,
            marketplace=self.marketplace,
            campaign_id=campaign_id,
            org_name_latin=org_name_latin,
        )
        print(f'kibana query link: {query_link}')

    def get_mp_stores(self) -> Generator[StoreYandex, None, None]:
        """Get and parse mp stores data from elastic.

        Returns:
            List of stores which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 24)
        campaign_id = self.org_data['campaign_id'] if self.org_data else '*'
        outlet = self.store_data['outlet'] if self.store_data else '*'

        results_count = 0
        hits = get_stores_yandexdbs_hits(begin_dt, end_dt, self.marketplace, campaign_id, outlet)

        for hit in hits:
            hit_link: str = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            req_data: str = hit.transaction.custom.request_data
            hit_dict = json.loads(req_data) if req_data != 'null' else {}
            transaction_name: str = hit.transaction.name
            method_name = transaction_name.split()[0]

            store = self.dt_store_mp(
                direction = '  e->',
                datetime = convert_timezone(hit_datetime, 'msc'),
                method_name = method_name,
                org_name = self.passed_org_name,
                visibility = hit_dict.get('visibility', ''),
                delivery_rules = hit_dict.get('deliveryRules' , ''),
                outlet = transaction_name[-14:-5],
                address = hit_dict['address']['street'] if hit_dict else '',
                hit_link = hit_link,
            )

            yield store

            results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'stores_mp',
            begin_dt,
            end_dt,
            marketplace=self.marketplace,
            campaign_id=campaign_id,
            outlet=outlet,
        )
        print(f'kibana query link: {query_link}')
