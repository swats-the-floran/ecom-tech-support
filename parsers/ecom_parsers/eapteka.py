import json
from dataclasses import dataclass
from typing import Generator, Union

from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import StockBase
from utils.ecom_elastic import get_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    generate_elk_query_link,
    get_datetimes,
    parse_datetime,
)


_mp_settings = StandardMarketplaceSettings(
    # stocks mp vars
    product_var_name='product_code',
    expiration_date_var_name='',
    price_region_var_name='region',
    base_filter='organization',
    data_var_name='request_data',
    stocks_mp_endpoint='*POST*apipartners.eapteka.ru/1_0/stock_changes*',
    # prices mp vars
    stocks_instead_prices=False,
    prices_mp_endpoint='*apipartners.eapteka.ru/1_0/price_changes*',
    # stores mp vars
    store_guid_var_name='id',
    price_guid_var_name='region',  # there is no price type field
    delivery_info_var_name='DeliveryInfo',
    deadline_date_var_name='OrderDeadlineDate',
    delivery_date_var_name='DeliveryDate',
    stores_mp_endpoint='*GET*apipartners.eapteka.ru/1_0/stores*',
    stores_mp_identifier='mp_store_guid',
)


@dataclass
class StockEapteka(StockBase):

    price_guid: Union[str, int] = ''
    errors: str = ''
    hit_link : str = ''


class EaptekaParser(StandardMarketplaceParser):

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
        self.dt_stock_mp = StockEapteka

        self.mp_settings = _mp_settings

    def get_mp_stocks(self) -> Generator[StockEapteka, None, None]:
        """Get and parse mp stocks data from elastic.

        Args:
            transaction_dt: the end of period. start of period depends on settings.
            marketplace: MP name.
            org_data: oranization data dict. Contains name, outlet, api ednpoint etc.
            product_data: product data dict. Contains code, guid etc.
        Returns:
            List of stocks which are instances of corresponding dataclass.
        """
        print('\n' 'getting eapteka data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 3)
        related_regions = self.org_data['related_region_codes']
        passed_org_name = self.org_data['org_name']

        product_var_name = self.mp_settings.product_var_name
        price_region_var_name = self.mp_settings.price_region_var_name
        data_var_name = self.mp_settings.data_var_name

        stocks = []
        hits = get_hits(begin_dt, end_dt, self.mp_settings.stocks_mp_endpoint, self.marketplace)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            hit_errors = json.loads(hit.transaction.custom.response_content)
            hit_errors = hit_errors['errors']

            stocks_raw = json.loads(hit.transaction.custom[data_var_name])

            for stock_raw in stocks_raw:
                if not self.product_identifiers or stock_raw[product_var_name] in self.product_identifiers:
                    product_code = stock_raw.get(product_var_name)

                    # search for correspoding error text if it exists
                    error = filter(lambda e: e.find(product_code) != -1, hit_errors)
                    error = next(error, '')

                    stock = self.dt_stock_mp(
                        direction='  e->',
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        org_name='',
                        product_identifier=product_code,
                        quantity=stock_raw.get('quantity'),
                        price_guid=stock_raw.get(price_region_var_name),
                        errors=error,
                        hit_link=hit_link,
                    )

                    stocks.append(stock)

        stocks = self._add_orgs_stocks_mp(stocks)

        if stocks:
            stocks = filter(lambda s: s.org_name == passed_org_name, stocks)

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
