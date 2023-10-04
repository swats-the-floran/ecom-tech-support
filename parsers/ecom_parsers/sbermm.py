"""
Integration uses feeds instead of api.
Prices feeds are in xml format and others are in json format.
"""

from dataclasses import dataclass
from typing import Generator

from sqlalchemy.orm import Session

from .base_mp import All1CParser
from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import (
    PriceBase,
    StockBase,
    StoreBase,
)
from utils.ecom_ftp import (
    get_sbermm_prices,
    get_sbermm_stocks,
    get_sbermm_stores,
)


@dataclass
class PriceSbermm(PriceBase):
    price: int = 0
    hit_link: str = ''


@dataclass
class StockSbermm(StockBase):
    price: int = 0
    hit_link: str = ''


@dataclass
class StoreSbermm(StoreBase):
    hit_link: str = ''


_mp_settings = StandardMarketplaceSettings(
    stocks_instead_prices = False,
)


class SbermmParser(All1CParser):

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
        self.dt_price_mp = PriceSbermm
        self.dt_stock_mp = StockSbermm
        self.dt_store_mp = StoreSbermm

        self.mp_settings = _mp_settings

    def get_mp_prices(self) -> Generator[PriceSbermm, None, None]:
        """Get prices sent from ecom to sbermegamarket mp. Prices come from feed so only current day is available.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting sbermm data...')

        campaign_id = self.org_data['sbermm_campaign_id']

        prices_count = 0
        offers = get_sbermm_prices(campaign_id)

        for offer in offers['offers']:
            if not self.product_identifiers or offer.attrib['id'] in self.product_identifiers:
                price = self.dt_price_mp(
                    direction='  e->',
                    datetime=offers['datetime'],
                    org_name=self.passed_org_name,
                    product_identifier=offer.attrib['id'],
                    price=offer.find('price').text,
                    hit_link=offers['hit_link'],
                )
                yield price
                prices_count += 1

        print(f'results: {prices_count}')

    def get_mp_stocks(self) -> Generator[StockSbermm, None, None]:
        """Get prices sent from ecom to sbermegamarket mp. Stocks come from feed so only current day is available.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting sbermm data...')

        campaign_id = self.org_data['sbermm_campaign_id']

        stocks_count = 0
        offers = get_sbermm_stocks(campaign_id)

        for offer in offers['offers']:
            if not self.product_identifiers or offer['offerId'] in self.product_identifiers:
                stock = self.dt_stock_mp(
                    direction='  e->',
                    datetime=offers['datetime'],
                    org_name=self.passed_org_name,
                    quantity=offer['quantity'],
                    product_identifier=offer['offerId'],
                    price=offer['price'],
                    hit_link=offers['hit_link'],
                )
                yield stock
                stocks_count += 1

        print(f'results: {stocks_count}')

    def get_mp_stores(self) -> Generator[StoreSbermm, None, None]:
        """Get prices sent from ecom to sbermegamarket mp. Stores come from feed so only current day is available.

        Returns:
            List of prices which are instances of corresponding dataclass.
        """
        print('\n' 'getting sbermm data...')

        campaign_id = self.org_data['sbermm_campaign_id']

        results_count = 0
        outlets = get_sbermm_stores(campaign_id)

        for outlet in outlets['outlets']:
            if not self.store_identifiers or outlet['identification']['id'] in self.store_identifiers:
                store = self.dt_store_mp(
                    direction='  e->',
                    datetime=outlets['datetime'],
                    org_name=self.passed_org_name,
                    store_guid=outlet['identification']['id'],
                    address=outlet['location']['address']['plain'][:50] + '...',
                    hit_link=outlets['hit_link'],
                )
                yield store
                results_count += 1

        print(f'results: {results_count}')
