from sqlalchemy.orm.session import Session

from .base_mp import StandardMarketplaceParser
from settings import StandardMarketplaceSettings


_mp_settings = StandardMarketplaceSettings(
    # stocks mp vars
    period_mp_stocks = 0.1
)


class AloeParser(StandardMarketplaceParser):
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

