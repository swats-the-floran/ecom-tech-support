import sys

from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings
from utils.ecom_dataclasses import PriceStandard


_mp_settings = StandardMarketplaceSettings(
    # stocks mp vars
    expiration_date_var_name='',
    price_region_var_name='region',
    base_filter='region',
    data_var_name='request_data',
    stocks_mp_endpoint='POST http://esb.production.puls.local/services/api/products/stocks',
    stocks_mp_success_status='202',
    # prices mp vars
    stocks_instead_prices=False,
    check_none_regions=True,
    prices_mp_endpoint='*POST*/services/api/products/prices*',
    prices_mp_success_status = '202',
    # stores mp vars
    store_guid_var_name='pharmacyId',
    price_guid_var_name='region',  # there is no price type field
    delivery_info_var_name='DeliveryInfo',
    deadline_date_var_name='OrderDeadlineDate',
    delivery_date_var_name='DeliveryDate',
    # prices 1c
    price_guid_from_transaction_name=True,
    moduleb2c=True,
)


class AptekaforteParser(StandardMarketplaceParser):
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
        self.dt_price_mp = PriceStandard

        self.mp_settings = _mp_settings

    def get_mp_stores(self) -> None:
        print('ecom does not pass stores to aptekaforte.')
        sys.exit(0)
