import json
from dataclasses import dataclass
from typing import Generator

from sqlalchemy.orm.session import Session

from .base_mp import All1CParser, StocksMPParser
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
from utils.ecom_postgres import execute_query


_mp_settings = StandardMarketplaceSettings(
    # prices mp vars
    # prices_mp_endpoint='POST https://api-seller.ozon.ru/v1/product/import/prices',
    # stocks mp vars
    stocks_mp_endpoint='POST https://api.aptekamos.ru/Price/WPrice/WimportPrices',
    product_var_name='item_id',
    # stores mp vars
    # store_guid_var_name='pharmacyId',
    # deadline_date_var_name='deliverydate_min',
    # delivery_date_var_name='deliverydate_max',
)


@dataclass
class StockAptekamos(StockBase):
    operation: str = ''  # DELETE, UPDATE...
    price: int = 0
    org_address_guid: str = ''
    hit_link: str = ''


class AptekamosParser(All1CParser, StocksMPParser):
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
        self.dt_stock_mp = StockAptekamos

        self.mp_settings = _mp_settings

    def _add_orgs_stocks_mp(self, stocks: list[StockAptekamos]) -> list[StockAptekamos]:
        """Get organizations and add them in stocks list.

        Args:
            stocks: list of stocks objects.
        Returns:
            The same list but objects has org_name requisite filled.
        """
        print('getting organizations...')

        org_address_guids = set()
        for stock in stocks:
            if stock.org_address_guid:
                org_address_guids.add(stock.org_address_guid)

        # make string for sql query
        org_address_guids = "('" + "\', \'".join(org_address_guids) + "')"

        query_text = f'''
        select
            org_address.address_guid,
            organization.name
        from
            delivery_organizationaddress org_address
            inner join core_organization organization
                on org_address.organization_id = organization.id
        where
            org_address.address_guid in {org_address_guids}
        '''

        query_result = execute_query(self.pg_session, query_text)

        org_dict = {}
        for row in query_result:
            org_dict[str(row[0])] = row[1]

        org_address_guids = org_dict.keys()
        for stock in stocks:
            stock_org_address_guid = stock.org_address_guid
            stock.org_name = org_dict.get(stock_org_address_guid, '')

        return stocks

    def get_mp_stocks(self) -> Generator[StockAptekamos, None, None]:
        """Get prices sent from ecom to mp.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, 3)
        product_var_name = self.mp_settings.product_var_name

        stocks = []
        hits = get_hits(begin_dt, end_dt, self.mp_settings.stocks_mp_endpoint, self.marketplace)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            stocks_raw = json.loads(hit.transaction.custom.request_data)['prices']

            for stock_raw in stocks_raw:
                product_identifier = stock_raw[product_var_name]
                if not self.product_identifiers or product_identifier in self.product_identifiers:
                    stock = self.dt_stock_mp(
                        direction='  e->',
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        product_identifier='"' + product_identifier + '"',
                        operation=stock_raw['operation'],
                        price=stock_raw['price'],
                        quantity=stock_raw['qtty'],
                        org_address_guid=stock_raw['org_chenum'],
                        org_name='',
                        hit_link=hit_link,
                    )

                    stocks.append(stock)

        if stocks:
            stocks = self._add_orgs_stocks_mp(stocks)
            stocks = filter(lambda p: p.org_name == self.passed_org_name, stocks)

        results_count = 0
        for stock in stocks:
            yield stock
            results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'prices_mp',
            begin_dt,
            end_dt,
            marketplace=self.marketplace,
        )
        print(f'kibana query link: {query_link}')
