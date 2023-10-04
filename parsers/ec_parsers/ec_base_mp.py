import json
from dataclasses import dataclass
from datetime import datetime

from settings import StandardMarketplaceSettings
from utils.ecom_elastic import get_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    get_datetimes,
    parse_datetime,
)


@dataclass
class StockECClient:
    direction: str
    datetime: datetime
    quantity: int = 0
    product_identifier: str = ''
    # price: int = 0
    hit_link : str = ''


class BaseParser:
    """Constructor for all other parsers."""

    def __init__(self,
        transaction_dt: str,
        marketplace: str,
        store_identifier: str = '',
        product_identifier: str = '',
    ) -> None:
        """Some requisites are passed as arguments and some are being calculated during init.

        Args:
            transaction_dt: the end of period. start of period depends on settings.
            marketplace: MP latin name.
            store_identifier: guid or code.
            product_identifier: product data dict. Contains code, guid etc.
        """
        self.transaction_dt = transaction_dt
        self.marketplace = marketplace
        self.store_identifier = store_identifier
        self.product_identifier = product_identifier

        # dataclasses
        self.dt_ec_stock_client = StockECClient

        self.mp_settings = StandardMarketplaceSettings()


class StocksClientParser(BaseParser):

    def get_client_stocks(self):
        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_1c_stocks)
        endpoint = f'/v1.1/pharmacies/{self.store_identifier}/stocks'

        results_count = 0
        hits = get_hits(begin_dt, end_dt, endpoint, project='ecom-client', method='HTTP_REQUEST')

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            stocks_raw = json.loads(hit.log_processed.message)

            for stock_raw in stocks_raw:
                product_identifier_curr = str(stock_raw['product_id'])

                # if not self.product_identifiers or product_identifier_curr in self.product_identifiers:
                stock_record = self.dt_ec_stock_client(
                    direction = '->ec  ',
                    datetime = convert_timezone(hit_datetime, 'msc'),
                    quantity = stock_raw.get('quantity'),
                    # price = stock_raw.get('price'),
                    product_identifier=product_identifier_curr,
                    hit_link = hit_link,
                )

                yield stock_record
                results_count += 1

        print(f'results: {results_count}')


class StocksMPParser(BaseParser):

    def get_mp_stocks(self):
        pass
