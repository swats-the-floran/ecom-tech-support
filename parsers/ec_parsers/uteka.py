import json

from .ec_base_mp import StocksClientParser, StocksMPParser
from settings import StandardMarketplaceSettings
from utils.ecom_elastic import get_hits
from utils.other import convert_timezone, generate_elk_doc_link, get_datetimes, parse_datetime

_mp_settings = StandardMarketplaceSettings(
    # stocks clent vars
    period_1c_stocks=24 * 30,
    period_mp_stocks=24 * 7,
)


class ECUtekaParser(StocksClientParser, StocksMPParser):

    def __init__(self,
        # pg_session: Session,
        transaction_dt: str,
        marketplace: str,
        # org_identifier: str,
        store_identifier: str = '',
        product_identifier: str = '',
    ) -> None:
        super().__init__(
            # pg_session,
            transaction_dt,
            marketplace,
            # org_identifier,
            store_identifier,
            product_identifier,
        )
        self.mp_settings = _mp_settings

    def get_mp_stocks(self):
        print('\n' 'getting mp data...')

        begin_dt, end_dt = get_datetimes(self.transaction_dt, self.mp_settings.period_mp_stocks)
        endpoint = f'/v1.0/stocks?storeId={self.store_identifier}&page=0&size=10000'

        results_count = 0
        hits = get_hits(begin_dt, end_dt, endpoint, project='ecom-client', method='HTTP_RESPONSE')

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            hit_datetime = parse_datetime(hit['@timestamp'])
            stocks_raw = json.loads(hit.log_processed.message)['results']

            for stock_raw in stocks_raw:
                product_identifier_curr = str(stock_raw['productId'])

                # if not self.product_identifiers or product_identifier_curr in self.product_identifiers:
                stock_record = self.dt_ec_stock_client(
                    direction = '->ec  ',
                    datetime = convert_timezone(hit_datetime, 'msc'),
                    quantity = stock_raw.get('quantity'),
                    # price = stock_raw.get('price'),
                    product_identifier=product_identifier_curr,
                    hit_link = hit_link,
                )

                yield stock_record
                results_count += 1

        print(f'results: {results_count}')

