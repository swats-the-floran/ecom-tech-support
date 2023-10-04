import sys
from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings


_mp_settings = StandardMarketplaceSettings(
    # stocks mp vars
    product_var_name='productId',
    expiration_date_var_name='expirationDate',
    price_region_var_name='region',
    base_filter='region',
    # stores mp vars
    store_guid_var_name='pharmacyId',
    price_guid_var_name='',
    delivery_info_var_name='DeliveryInfo',
    deadline_date_var_name='OrderDeadlineDate',
    delivery_date_var_name='DeliveryDate',
    stores_mp_identifier='org_store_id',
)


class Apteka366Parser(StandardMarketplaceParser):
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
        self.mp_settings = _mp_settings

    def get_mp_prices(self) -> None:
        print('ecom does not pass prices to apteka36_6')
        sys.exit(0)
