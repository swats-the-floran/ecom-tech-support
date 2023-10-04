from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings


_mp_settings = StandardMarketplaceSettings(
    # stocks mp vars
    product_var_name='productId',
    expiration_date_var_name='expirationDate',
    # stores mp vars
    store_guid_var_name='pharmacyId',
    delivery_info_var_name='deliveryinfo',
    deadline_date_var_name='orderdeadlinedate',
    delivery_date_var_name='deliverydate',
)


class FarmiyaParser(StandardMarketplaceParser):
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

