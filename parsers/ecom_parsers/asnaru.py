from dataclasses import dataclass
from typing import Generator

import ijson
from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import PriceBase
from utils.ecom_elastic import get_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    generate_elk_query_link,
    get_datetimes,
    parse_datetime,
)


_mp_settings = StandardMarketplaceSettings(
    # prices mp vars
    stocks_instead_prices=False,
    prices_mp_endpoint='*asna.ru/ws/puls/v1.0/price_changes_async*',
    data_var_name='request_data',
    moduleb2c=True,
    # stocks mp vars
    product_var_name='product_id',
    price_region_var_name='region',
    base_filter='region',
    stocks_mp_endpoint='POST*asna.ru/ws/puls/v1.0/stock_changes_async*',
    stocks_mp_success_status='200',
    # stores mp vars
    store_guid_var_name='id',
    stores_mp_endpoint='GET*asna.ru/ws/puls/v1.0/stores*',
    stores_mp_success_status='200',
    stores_mp_identifier='mp_store_guid',
)


@dataclass
class PriceAsnaru(PriceBase):
    price_guid: str = ''
    price_b2c: float = 0
    price_b2b: float = 0
    vat_b2b: float = 0
    expiration_date: str = ''
    hit_link: str = ''


class AsnaruParser(StandardMarketplaceParser):
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
        self.dt_price_mp = PriceAsnaru

        self.mp_settings = _mp_settings

    def get_mp_prices(self) -> Generator[PriceAsnaru, None, None]:
        """Get prices sent from ecom to mp with standard integration.

        Args:
            pg_session: PostgreSQL session.
            transaction_dt: the end of period. start of period depends on settings.
            marketplace: MP name.
            org_data: oranization data dict. Contains name, outlet, api ednpoint etc.
            product_data: product data dict. Contains code, guid etc.
        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 3)

        related_regions = self.org_data['related_region_codes']
        product_var_name = self.mp_settings.product_var_name
        expiration_date_var_name = self.mp_settings.expiration_date_var_name
        price_region_var_name = self.mp_settings.price_region_var_name
        data_var_name = self.mp_settings.data_var_name  # request or response atm

        endpoint = self.mp_settings.prices_mp_endpoint

        prices = []
        hits = get_hits(begin_dt, end_dt, endpoint, self.marketplace)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            prices_raw = ijson.items(hit.transaction.custom[data_var_name], 'item')

            if data_var_name == 'response_content':
                prices_raw = prices_raw['results']

            # TODO: remove try...except when logging will be fixed
            try:
                for price_raw in prices_raw:
                    if not self.product_identifiers or price_raw[product_var_name] in self.product_identifiers:
                        price_obj = self.dt_price_mp(
                            direction='  e->',
                            datetime=convert_timezone(hit_datetime, 'msc'),
                            product_identifier=price_raw.get(product_var_name),
                            price_b2c=price_raw.get('price_b2c'),
                            price_b2b=price_raw.get('price_b2b'),
                            vat_b2b=price_raw.get('vat_b2b'),
                            price_guid=price_raw.get(price_region_var_name),
                            expiration_date=price_raw.get(expiration_date_var_name),
                            org_name=self.passed_org_name,
                            hit_link=hit_link,
                        )

                        # if self.mp_settings.check_none_regions:
                        #     # common error for aptekaforte - None as a region field
                        #     try:
                        #         price_obj.price_guid = int(price_raw.get(price_region_var_name))
                        #     except ValueError as e:
                        #         price_obj.price_guid = -1
                        #         print(str(e) + '\n' + hit_link)

                        prices.append(price_obj)
            except ijson.IncompleteJSONError:
                print('hit has not been logged completely.')
                continue

        prices = filter(lambda p: p.price_guid in related_regions, prices)

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
