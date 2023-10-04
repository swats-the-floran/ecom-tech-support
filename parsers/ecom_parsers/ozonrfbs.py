import json
from dataclasses import dataclass
from typing import Generator, Union

from sqlalchemy.orm.session import Session

from .base_mp import All1CParser, StocksMPParser, StoresMPParser
from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import PriceBase, StockBase
from utils.ecom_elastic import get_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    generate_elk_query_link,
    get_datetimes,
    parse_datetime,
)


# probably uses standard integration for stores, but no proves.
_mp_settings = StandardMarketplaceSettings(
    stocks_instead_prices=False,
    # prices mp vars
    prices_mp_endpoint='POST https://api-seller.ozon.ru/v1/product/import/prices',
    # stocks mp vars
    stocks_mp_endpoint='POST https://api-seller.ozon.ru/v2/products/stocks',
    product_var_name='offer_id',
    # stores mp vars
    store_guid_var_name='pharmacyId',
    deadline_date_var_name='deliverydate_min',
    delivery_date_var_name='deliverydate_max',
)


@dataclass
class PriceOzon(PriceBase):
    price_guid: Union[str, int] = ''
    price: int = 0
    errors: str = ''
    hit_link: str = ''


@dataclass
class StockOzon(StockBase):
    errors: str = ''
    price_guid: Union[str, int] = ''
    price_type: str = ''
    region: str = ''
    hit_link: str = ''


class OzonParser(All1CParser, StocksMPParser, StoresMPParser):
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
        self.dt_price_mp = PriceOzon
        self.dt_stock_mp = StockOzon

        self.mp_settings = _mp_settings

    def get_mp_prices(self) -> Generator[PriceOzon, None, None]:
        """Get prices sent from ecom to mp.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 24)
        product_var_name = self.mp_settings.product_var_name

        prices = []
        hits = get_hits(begin_dt, end_dt, self.mp_settings.prices_mp_endpoint, self.marketplace)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])
            price_guid: str = hit.labels.price_type

            req_prices = json.loads(hit.transaction.custom.request_data).get('prices', [])
            resp_prices = json.loads(hit.transaction.custom.response_content).get('result', [])

            # just in case of something unexpected
            if len(req_prices) != len(resp_prices):
                raise Exception('hit\'s request and response have different lengths.')

            for i in range(len(resp_prices)):
                resp_price = resp_prices[i]
                req_price = req_prices[i]

                product_identifier = resp_price[product_var_name]

                if not self.product_identifiers or product_identifier in self.product_identifiers:
                    price = self.dt_price_mp(
                        direction='  e->',
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        product_identifier='"' + product_identifier + '"',
                        price=req_price['price'],
                        errors=resp_price['errors'],
                        price_guid=price_guid,
                        org_name='',
                        hit_link=hit_link,
                    )

                    prices.append(price)

        if prices:
            prices = self._add_orgs_stocks_mp(prices)
            prices = filter(lambda p: p.org_name == self.passed_org_name, prices)

        results_count = 0
        for price in prices:
            yield price
            results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'prices_mp',
            begin_dt,
            end_dt,
            marketplace=self.marketplace,
        )
        print(f'kibana query link: {query_link}')

    def get_mp_stocks(self) -> Generator[StockOzon, None, None]:
        """Get and parse mp stocks data from elastic.

        Returns:
            List of stocks which are instances of corresponding dataclass.
        """
        print('\n' 'getting ozon data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 6)

        stocks = []
        hits = get_hits(begin_dt, end_dt, self.mp_settings.stocks_mp_endpoint, self.marketplace)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])
            price_guid: str = hit.labels.price_type

            req_stocks = json.loads(hit.transaction.custom.request_data).get('stocks', [])
            resp_stocks = json.loads(hit.transaction.custom.response_content).get('result', [])

            # just in case of something unexpected
            if len(req_stocks) != len(resp_stocks):
                raise Exception('hit\'s request and response have different lengths.')

            for i in range(len(resp_stocks)):
                resp_stock = resp_stocks[i]
                req_stock = req_stocks[i]

                product_identifier = resp_stock[self.mp_settings.product_var_name]

                if not self.product_identifiers or product_identifier in self.product_identifiers:
                    stock = self.dt_stock_mp(
                        direction='  e->',
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        product_identifier='"' + product_identifier + '"',
                        quantity=req_stock['stock'],
                        org_name='',
                        price_guid=price_guid,
                        errors=resp_stock['errors'],
                        hit_link=hit_link,
                    )

                    stocks.append(stock)

        if stocks:
            stocks = self._add_orgs_stocks_mp(stocks)
            stocks = filter(lambda s: s.org_name == self.passed_org_name, stocks)

        results_count = 0
        for stock in stocks:
            yield stock
            results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'stocks_mp',
            begin_dt,
            end_dt,
            marketplace=self.marketplace,
        )
        print(f'kibana query link: {query_link}')
