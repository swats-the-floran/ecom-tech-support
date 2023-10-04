from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings


_mp_settings = StandardMarketplaceSettings(
    # stocks mp vars
    product_var_name='product_code',
    expiration_date_var_name='expirationDate',
    price_region_var_name='organization_id',
    base_filter='organization_id',
    data_var_name='request_data',
    stocks_mp_endpoint=
        'POST https://api.partners.esc.ru/1_0/stock_changes?api_key=4cdf72f3-bfcf-416a-8783-f73345d8ec6a',
    stocks_mp_success_status='200',
    # stores mp vars
    store_guid_var_name='id',
    price_guid_var_name='pricetype',
)


class SozvezdieParser(StandardMarketplaceParser):
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

