import json
from typing import Generator

import ijson
from sqlalchemy.orm.session import Session

from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import (
    Price1C,
    PriceStandard,
    Stock1C,
    StockStandard,
    Store1C,
    StoreStandard,
)
from utils.ecom_elastic import get_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    generate_elk_query_link,
    get_datetimes,
    parse_datetime,
)
from utils.ecom_postgres import (
    execute_query,
    get_marketplace_guid,
    get_organization_data,
    get_price_settings,
    get_prices_data,
    get_product_data,
    get_store_data,
)


class BaseParser:
    """Constructor for all other parsers."""

    def __init__(self,
        pg_session: Session,
        transaction_dt: str,
        marketplace: str,
        org_identifier: str,
        store_identifier: str = '',
        product_identifier: str = '',
    ) -> None:
        """Some requisites are passed as arguments and some are being calculated during init.

        Args:
            pg_session: PostgreSQL session.
            transaction_dt: the end of period. start of period depends on settings.
            marketplace: MP latin name.
            org_identifier: guid, name or outlet_id.
            store_identifier: guid or code.
            product_data: product data dict. Contains code, guid etc.
        """
        self.pg_session = pg_session
        self.store_data = get_store_data(pg_session, store_identifier)

        if not org_identifier:
            org_identifier = self.store_data.pop('org_name')
        self.org_data = get_organization_data(pg_session, org_identifier)

        self.transaction_dt = transaction_dt
        self.marketplace = marketplace
        self.org_endpoint = self.org_data['endpoint']
        self.passed_org_name = self.org_data['org_name']
        self.store_identifiers = self.store_data.values()
        self.product_identifiers = get_product_data(self.pg_session, product_identifier).values()

        # dataclasses
        self.dt_price_1c = Price1C
        self.dt_price_mp = StockStandard  # standard integrations send stocks and prices in the same endpoint.
        self.dt_stock_1c = Stock1C
        self.dt_stock_mp = StockStandard
        self.dt_store_1c = Store1C
        self.dt_store_mp = StoreStandard

        self.mp_settings = StandardMarketplaceSettings()



class Prices1CParser(BaseParser):
    """1C is just a direction. some prices come from b2c module instead of 1C."""

    def _add_orgs_price_1c(self, prices: list[Price1C]) -> list[Price1C]:
        """Add organizations to prices list.

        Args:
            prices: List of prices that will be enriched.
        Returns:
            List of enriched prices.
        """
        print('getting organizations...')

        prices_guids = list({price.price_guid for price in prices})
        price_org_dict = get_prices_data(self.pg_session, self.marketplace, prices_guids)

        for price in prices:
            price_guid = price.price_guid
            price_org_guids = price_org_dict.keys()

            if price_guid in price_org_guids:
                price.org_name = price_org_dict[price_guid]['org_name']
                price.price_type = price_org_dict[price_guid]['price_type']

        # TODO: check if i have to return something
        return prices

    def get_1c_prices(self) -> Generator[Price1C, None, None]:
        """Get and parse 1c stores data from elastic.

        Returns:
            List of price objects.
        """
        print('\n' 'getting 1c data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_1c_prices)

        prices_b2c = get_price_settings(self.pg_session, self.marketplace, self.passed_org_name)
        if all(prices_b2c.values()):
            data_var_name = self.mp_settings.moduleb2c_data_var_name
            username = self.mp_settings.moduleb2c_username
            endpoint = self.mp_settings.moduleb2c_endpoint
            b2c_used = True
        elif not any(prices_b2c.values()):
            data_var_name = self.mp_settings.prices_1c_data_var_name
            username = self.mp_settings.prices_1c_username
            endpoint = f'*{self.org_endpoint}/v1/PriceTime*'
            b2c_used = False
        else:
            data_var_name = ''
            username = f'("{self.mp_settings.prices_1c_username}" OR "{self.mp_settings.moduleb2c_username}")'
            endpoint = f'(*{self.org_endpoint}/v1/PriceTime* OR *{self.org_endpoint}/v1/PriceTime*)'

        hits = get_hits(begin_dt, end_dt, endpoint, username)
        prices = []

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            prices_raw = json.loads(hit.transaction.custom.response_content)
            if data_var_name:
                prices_raw = prices_raw[data_var_name]
            if not data_var_name and hasattr(prices_raw, self.mp_settings.prices_1c_data_var_name):
                prices_raw = prices_raw[self.mp_settings.prices_1c_data_var_name]
                b2c_used = False
            elif hasattr(prices_raw, self.mp_settings.moduleb2c_data_var_name):
                prices_raw = prices_raw[self.mp_settings.moduleb2c_data_var_name]
                b2c_used = True

            if b2c_used or self.mp_settings.price_guid_from_transaction_name:
                price_guid: str = hit.transaction.name[len(hit.transaction.name) - 37 : len(hit.transaction.name) - 1]
            else:
                price_guid: str = json.loads(hit.transaction.custom.request_data)['PriceTypeGuid']

            for price_raw in prices_raw:
                if not self.product_identifiers or price_raw['ProductGuid'] in self.product_identifiers:
                    prices.append(Price1C(
                        direction='->e  ',
                        b2c_used=b2c_used,
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        org_name=price_raw.get('org_name'),
                        product_identifier=price_raw.get('ProductGuid'),
                        price_guid=price_guid,
                        price_type=price_raw.get('price_type'),
                        vat=price_raw.get('Vat'),
                        price_inc_vat=price_raw.get('PriceIncVat'),
                        price_wo_vat=price_raw.get('PriceWoVat'),
                        price_promo=price_raw.get('PricePromo'),
                        hit_link=hit_link,
                    ))

        if prices:
            prices = self._add_orgs_price_1c(prices)
            prices = filter(lambda k: k.org_name == self.passed_org_name, prices)

        results_count = 0
        for price in prices:
            yield price
            results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'prices_1c',
            begin_dt,
            end_dt,
            endpoint,
            marketplace=self.marketplace,
        )
        print(f'kibana query link: {query_link}')


class Stocks1CParser(BaseParser):

    def get_1c_stocks(self) -> Generator[Stock1C, None, None]:
        """Get and parse 1c stocks data from elastic.

        Returns:
            List of stock objects.
        """
        print('\n' 'getting 1c data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_1c_stocks)
        endpoint = f'POST {self.org_endpoint}/v1/stocks*'

        # if mp_settings.uses_duplicates and product_data:
        #     original_product_guid = get_original_product(
        #         pg_session,
        #         marketplace,
        #         org_name,
        #         product_data['guid'],
        #     )
        #
        #     product_guids = get_related_products(
        #         pg_session,
        #         marketplace,
        #         org_name,
        #         original_product_guid,
        #     )
        #
        #     product_identifiers = list(product_identifiers)
        #     for product_guid in product_guids:
        #         product_identifiers.append(product_guid)

        results_count = 0
        hits = get_hits(begin_dt, end_dt, endpoint)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            stocks_raw = json.loads(hit.transaction.custom.response_content)
            stocks_raw = stocks_raw.get('Data', [])

            for stock_raw in stocks_raw:
                product_identifier_curr = stock_raw['ProductGuid']

                if not self.product_identifiers or product_identifier_curr in self.product_identifiers:
                    stock_record = Stock1C(
                        direction = '->e  ',
                        datetime = convert_timezone(hit_datetime, 'msc'),
                        quantity = stock_raw.get('Quantity'),
                        product_identifier = product_identifier_curr,
                        expiration_date = stock_raw.get('ExpirationDate'),
                        org_name = self.passed_org_name,
                        hit_link = hit_link,
                    )

                    yield stock_record
                    results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'stocks_1c',
            begin_dt,
            end_dt,
            endpoint,
            self.marketplace,
        )
        print(f'kibana query link: {query_link}')


class Stores1CParser(BaseParser):

    def _parse_delivery_info_1c(self, delivery_raw: dict) -> list[str]:
        """Parse delivery info from '/stores' hit."""
        dates_list = ['', '', '', '', '', '']
        if delivery_raw is None:
            return dates_list

        i = 0
        for element in delivery_raw:
            dates_list[i] = element.get('OrderDeadlineDate')
            i += 1
            dates_list[i] = element.get('DeliveryDate')
            i += 1

        return dates_list

    def get_1c_stores(self) -> Generator[Store1C, None, None]:
        """Get and parse 1c stores data from elastic.

        Returns:
            List of stores which are instances of corresponding dataclasses.
        """
        print('\n' 'getting 1c data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_1c_stores)
        marketplace_guid = get_marketplace_guid(self.pg_session, self.marketplace)
        endpoint = f'*{self.org_endpoint}/v1/stores/{marketplace_guid}*'

        results_count = 0
        hits = get_hits(begin_dt, end_dt, endpoint)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            stores_raw = json.loads(hit.transaction.custom.response_content)
            stores_raw = stores_raw.get('Data', [])

            for store_raw in stores_raw:
                if not self.store_identifiers or store_raw['AddressGuid'] in self.store_identifiers:
                    delivery_info = self._parse_delivery_info_1c(store_raw.get('DeliveryInfo'))

                    store = Store1C(
                        direction='->e  ',
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        org_name=self.passed_org_name,
                        store_guid=store_raw.get('AddressGuid'),
                        store_id=store_raw.get('AddressId'),
                        b2b_price_guid=store_raw.get('PriceTypeB2BGuid'),
                        b2c_price_guid=store_raw.get('PriceTypeB2СGuid'),  # С in B2С is cyrillic x_x
                        address=store_raw.get('Address', '')[:50] + '...',
                        deadline_date1=delivery_info[0],
                        delivery_date1=delivery_info[1],
                        deadline_date2=delivery_info[2],
                        delivery_date2=delivery_info[3],
                        deadline_date3=delivery_info[4],
                        delivery_date3=delivery_info[5],
                        hit_link=hit_link,
                    )

                    results_count += 1

                    yield store

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'stores_1c',
            begin_dt,
            end_dt,
            endpoint,
            marketplace_guid,
        )
        print(f'kibana query link: {query_link}')


class All1CParser(Prices1CParser, Stocks1CParser, Stores1CParser):
    pass


class PricesMPParser(BaseParser):

    def get_mp_prices(self) -> Generator[PriceStandard, None, None]:
        """Get prices sent from ecom to mp with standard integration.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_mp_prices)

        related_regions = self.org_data['related_region_codes']
        product_var_name = self.mp_settings.product_var_name
        expiration_date_var_name = self.mp_settings.expiration_date_var_name
        price_region_var_name = self.mp_settings.price_region_var_name
        data_var_name = self.mp_settings.data_var_name  # request or response atm

        endpoint = self.mp_settings.prices_mp_endpoint
        success_status = self.mp_settings.prices_mp_success_status

        prices = []
        hits = get_hits(begin_dt, end_dt, endpoint, self.marketplace, success_status)

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
                        price_obj = PriceStandard(
                            direction='  e->',
                            datetime=convert_timezone(hit_datetime, 'msc'),
                            product_identifier=price_raw.get(product_var_name),
                            price=price_raw.get('price'),
                            price_guid=price_raw.get(price_region_var_name),
                            expiration_date=price_raw.get(expiration_date_var_name),
                            # org_name': '',
                            hit_link=hit_link,
                        )

                        if self.mp_settings.check_none_regions:
                            # common error for aptekaforte - None as a region field
                            try:
                                price_obj.price_guid = int(price_raw.get(price_region_var_name))
                            except ValueError as e:
                                price_obj.price_guid = -1
                                print(str(e) + '\n' + hit_link)

                        if self.mp_settings.base_filter != 'organization':
                            price_obj.org_name = self.passed_org_name

                        prices.append(price_obj)
            except ijson.IncompleteJSONError:
                print('hit has not been logged completely.')
                continue

        if prices and self.mp_settings.base_filter == 'region':
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


class StocksMPParser(BaseParser):

    def _add_orgs_stocks_mp(self, stocks: list[StockStandard]) -> list[StockStandard]:
        """Get organizations and add them in stocks list.

        Args:
            stocks: list of stocks objects.
        Returns:
            The same list but objects has org_name requisite filled.
        """
        print('getting organizations...')

        price_guids = set()
        for stock in stocks:
            if stock.price_guid:
                price_guids.add(stock.price_guid)
            elif self.marketplace in ('uteka',) and stock.region:
                price_guids.add(stock.region)
        # make string for sql query
        price_guids = "('" + "\', \'".join(price_guids) + "')"

        if self.marketplace in ('aptekaforte',):
            query_text = f'''
            select
                co.guid,
                co.name
            from
                core_organization co
            where
                co.guid in {price_guids}
            '''

        else:
            query_text = f'''
            select
                price.guid,
                price.price_type,
                array_agg(org.name)
            from
                price_organizationprice price
                inner join
                    core_organization org
                        on price.organization_id = org.id
                        and price.guid in {price_guids}
            group by
                price.guid,
                price.price_type
            '''

        query_result = execute_query(self.pg_session, query_text)

        org_dict = {}
        for row in query_result:
            # TODO: make code more readable by learning sqlalchemy
            if self.marketplace == 'sozvezdie':
                org_dict[str(row[0])] = {
                    'price_type': '',
                    'org_name': row[1],
                }
            else:
                org_dict[str(row[0])] = {
                    'price_type': str(row[1]),
                    'org_name': row[[2][0]][0],
                }

        # query_result = query_result.mappings().all()

        org_price_guids = org_dict.keys()
        for stock in stocks:
            price_guid = stock.price_guid
            if price_guid in org_price_guids:
                stock.price_type = org_dict[price_guid]['price_type']
                stock.org_name = org_dict[price_guid]['org_name']

        return stocks

    def get_mp_stocks(self) -> Generator[StockStandard, None, None]:
        """Get and parse mp stocks data from elastic.

        Returns:
            List of stocks which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_mp_stocks)
        store_org_id = self.org_data['org_id']
        passed_org_name = self.org_data['org_name']
        related_regions = self.org_data['related_region_codes']

        product_var_name = self.mp_settings.product_var_name
        expiration_date_var_name = self.mp_settings.expiration_date_var_name
        price_region_var_name = self.mp_settings.price_region_var_name
        data_var_name = self.mp_settings.data_var_name  # request or response atm

        endpoint = self.mp_settings.stocks_mp_endpoint
        success_status = self.mp_settings.stocks_mp_success_status

        stocks = []
        hits = get_hits(begin_dt, end_dt, endpoint, self.marketplace, success_status)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])
            stocks_raw = json.loads(hit.transaction.custom[data_var_name])

            # if we get information from response we get hit dict from additional field
            if data_var_name == 'response_content':
                stocks_raw = stocks_raw['results']

            for stock_raw in stocks_raw:
                if not self.product_identifiers or stock_raw[product_var_name] in self.product_identifiers:
                    stock = StockStandard(
                        direction='  e->',
                        datetime=convert_timezone(hit_datetime, 'msc'),
                        product_identifier=stock_raw.get(product_var_name),
                        quantity=stock_raw.get('quantity'),
                        price=stock_raw.get('price'),
                        price_guid=stock_raw.get(price_region_var_name),
                        expiration_date=stock_raw.get(expiration_date_var_name),
                        # org_name='',
                        hit_link=hit_link,
                    )

                    if self.mp_settings.check_none_regions:
                        # common error for aptekaforte - None as in region field
                        try:
                            stock.price_guid = int(stock_raw.get(price_region_var_name))
                        except ValueError as e:
                            print(e)
                            print(hit_link)

                    if self.mp_settings.base_filter != 'organization':
                        stock.org_name = passed_org_name

                    stocks.append(stock)

        if stocks and self.mp_settings.base_filter == 'organization':
            stocks = self._add_orgs_stocks_mp(stocks)
            stocks = filter(lambda s: s.org_name == passed_org_name, stocks)

        elif stocks and self.mp_settings.base_filter == 'region':
            stocks = filter(lambda s: s.price_guid in related_regions, stocks)

        elif stocks and self.mp_settings.base_filter == 'organization_id':
            stocks = filter(lambda s: s.price_guid == store_org_id, stocks)

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


class StoresMPParser(BaseParser):

    def _parse_delivery_info_mp(self, delivery_raw: dict) -> list[str]:
        """Parse delivery info from /stores hit."""
        dates_list = ['', '', '', '', '', '',]
        if delivery_raw is None:
            return dates_list

        deadline_date_var_name = self.mp_settings.deadline_date_var_name
        delivery_date_var_name = self.mp_settings.delivery_date_var_name

        i = 0
        for element in delivery_raw:
            dates_list[i] = element.get(deadline_date_var_name)
            i += 1
            dates_list[i] = element.get(delivery_date_var_name)
            i += 1

        return dates_list

    def _add_orgs_stores_mp(self, stores: list[StoreStandard]) -> list[StoreStandard]:
        """Get organizations for and add them in stores.

        Args:
            stores: list of stores objects.
        Returns:
            The same list but objects has org_name requisite filled.
        """
        print('getting organizations...')

        stores_guids = list({store.store_guid for store in stores})
        # print(f'store guids set {len(stores_guids)}')

        if self.mp_settings.stores_mp_identifier == 'mp_store_guid':
            query_text = """
            select
                mp_store.marketplace_guid,
                organization.name
            from
                delivery_organizationaddress org_store
                inner join core_organization organization
                    on org_store.organization_id = organization.id
                inner join delivery_marketplacestore mp_store
                    on org_store.marketplace_store_id = mp_store.id
                    and mp_store.marketplace_guid in {guids}
            group by
                mp_store.marketplace_guid,
                organization.name
            """
        elif self.mp_settings.stores_mp_identifier == 'org_store_id':
            query_text = """
            select
                org_store.address_id,
                organization.name
            from
                delivery_organizationaddress org_store
                inner join core_organization organization
                    on org_store.organization_id = organization.id
                    and org_store.address_id in {guids}
            group by
                org_store.address_id,
                organization.name
            """
        else:
            query_text = """
            select
                org_store.address_guid,
                organization.name
            from
                delivery_organizationaddress org_store
                inner join core_organization organization
                    on org_store.organization_id = organization.id
                    and org_store.address_guid in {guids}
            group by
                org_store.address_guid,
                organization.name
            """

        chunks = (stores_guids[pos:pos + 500] for pos in range(0, len(stores_guids), 500))

        org_dict = {}

        for chunk in chunks:
            stores_guids_str = "('" + "\', \'".join(chunk) + "')"

            try:
                query_result = execute_query(
                    self.pg_session,
                    query_text.format(guids=stores_guids_str),
                )
            except StopIteration:
                pass
            else:
                for row in query_result:
                    row_store_guid = str(row[0])
                    row_org_name = row[1]
                    org_dict[row_store_guid] = row_org_name
                    # print(row_store_guid, row_org_name)

        org_price_guids = org_dict.keys()

        for store in stores:
            if store.store_guid in org_price_guids:
                store.org_name = org_dict[store.store_guid]

        return stores

    def get_mp_stores(self) -> Generator[StoreStandard, None, None]:
        """Get and parse mp stores data from elastic.

        Returns:
            List of stores which are instances of corresponding dataclass.
        """
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_mp_stores)
        endpoint = self.mp_settings.stores_mp_endpoint
        success_status = self.mp_settings.stores_mp_success_status

        store_guid_var_name = self.mp_settings.store_guid_var_name
        delivery_info_var_name = self.mp_settings.delivery_info_var_name
        # in case if price guid will be added again
        # price_guid_var_name = self.mp_settings.price_guid_var_name

        stores = []
        hits = get_hits(begin_dt, end_dt, endpoint, self.marketplace, success_status)

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])

            stores_raw = json.loads(hit.transaction.custom.response_content)

            if self.marketplace != 'eapteka':
                stores_raw = stores_raw['results']

            for store_raw in stores_raw:
                if not self.store_identifiers or store_raw[store_guid_var_name] in self.store_identifiers:
                    delivery_info = store_raw.get(delivery_info_var_name)
                    delivery_info = self._parse_delivery_info_mp(delivery_info)

                    store = StoreStandard(
                        direction = '  e->',
                        datetime = convert_timezone(hit_datetime, 'msc'),
                        store_guid = store_raw.get(store_guid_var_name),
                        # address = store_raw.get('title'),
                        address = store_raw.get('address', '')[:50] + '...',
                        deadline_date1 = delivery_info[0],
                        delivery_date1 = delivery_info[1],
                        deadline_date2 = delivery_info[2],
                        delivery_date2 = delivery_info[3],
                        deadline_date3 = delivery_info[4],
                        delivery_date3 = delivery_info[5],
                        # price_guid = store_raw.get(price_guid_var_name),
                        hit_link = hit_link,
                    )

                    stores.append(store)

        if stores:
            stores = self._add_orgs_stores_mp(stores)
            stores = list(filter(lambda s: s.org_name == self.passed_org_name, stores))

        results_count = 0
        for store in stores:
            yield store
            results_count += 1

        print(f'results: {results_count}')

        query_link = generate_elk_query_link(
            'stores_mp',
            begin_dt,
            end_dt,
            marketplace=self.marketplace,
        )
        print(f'kibana query link: {query_link}')


class AllMPParser(PricesMPParser, StocksMPParser, StoresMPParser):
    pass


class StandardMarketplaceParser(All1CParser, AllMPParser):
    pass
