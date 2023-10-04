"""Settings for marketplaces in stocks_*, stores_* and prices_* scripts."""

from pydantic import BaseSettings as PydanticBaseSettings


class BaseSettings(PydanticBaseSettings):
    """Unspecific settings.

    Args:
        stocks_instead_prices: standard integrations combine prices and stocks.
    """

    stocks_instead_prices: bool = True


class Prices1CSettings(BaseSettings):
    """Settings for prices_1c.py.

    Args:
        price_guid_from_transaction_name: defines how to get price guid from logs.
    atm only true for aptekaforte.
        prices_1c_data_var_name: default field to parse necessary data.
        prices_1c_username: default username.
        moduleb2c: shows if marketplace gets data from module b2c instead
    of 1C API. atm it is only aptekaforte.
        moduleb2c_data_var_name: moduleb2c related field to parse necessary data.
        moduleb2c_username: moduleb2c related username.
        moduleb2c_endpoint: moduleb2c related endpoint for elasticsearch filtration.
        period_1c_prices: how many hours before transaction time should be checked.
    """

    price_guid_from_transaction_name: bool = False

    prices_1c_data_var_name: str = 'Data'
    prices_1c_username: str = 'puls'

    moduleb2c: bool = False
    moduleb2c_data_var_name: str = 'results'
    moduleb2c_username: str = 'moduleb2c'
    moduleb2c_endpoint: str = '*/master-ecom-b2c.puls.ru/api/v1.0/price*'

    period_1c_prices: float = 24


class Stocks1cSetting(BaseSettings):
    """Settings for stocks_1c.py.

    Args:
        period_1c_stocks: how many hours before transaction time should be checked.
    """

    period_1c_stocks: float = 3


class Stores1cSetting(BaseSettings):
    """Settings for stocks_1c.py.

    Args:
        period_1c_stores: how many hours before transaction time should be checked.
    """

    period_1c_stores: float = 24


class All1CSettings(Prices1CSettings, Stocks1cSetting, Stores1cSetting):
    pass


class PricesMPSettings(BaseSettings):
    """Settings for prices_mp.py.

    Args:
        check_none_regions: aptekaforte has problems with None strings istead of
    retion codes. This option helps to avoid exceptions.
        prices_mp_endpoint: a filter for elasticsearch logs.
        prices_mp_success_status: only successfuls requests are being parsed.
        period_mp_prices: how many hours before transaction time should be checked.
    """

    check_none_regions: bool = False

    prices_mp_endpoint: str = ''
    prices_mp_success_status: str = '200'

    period_mp_prices: float = 24


class StocksMPSettings(BaseSettings):
    """Settings for stocks_mp.py.

    Args:
        product_var_name: field containing product identifier.
        expiration_date_var_name: field containing expiration date.
        price_region_var_name: field containing price guid or region code.
    needed for stocks records filtration.
        base_filter: type of stocks records filtration. atm 'organization'
    or 'region'. code should be changed and this field to be removed.
        uses_duplicates: should be True if marketplace uses active duplicate
    relations (https://ecom.puls-api.ru/dashboard/product/activeduplicaterelations/).
    atm is True only for aptekaforte and yandex market.
        data_var_name: field containing main hit data for mp stocks.
    note that some data comes in response and some comes in request parent fields.
        stocks_mp_endpoint: a filter for elasticsearch logs.
        stocks_mp_success_status: only successfuls requests are being parsed.
        period_mp_stocks: how many hours before transaction time should be checked.
    """

    product_var_name: str = 'product_id'
    expiration_date_var_name: str = 'expiration_date'
    price_region_var_name: str = 'price_type'
    base_filter: str = 'organization'
    uses_duplicates: bool = False
    data_var_name: str = 'response_content'

    stocks_mp_endpoint: str = 'GET restapi.v1_0.views.StocksView'
    stocks_mp_success_status: str = 'HTTP 2xx'

    period_mp_stocks: float = 1


class StoresMPSettings(BaseSettings):
    """Settings for stores_mp.py.

    Args:
        store_guid_var_name: field containing store identifier. atm is 'pharmacyId'
    or 'pharmacy_id' and 'id' for sozvezdie.
        price_guid_var_name: field containing filter data for stores. can be price
    identifier or region identifier. Empty if there should not be any filtration
    (i.e. apteka36_6).
        delivery_info_var_name: field containing list with 0-3 dicts with
    order deadline date and delivery date in each of them.
        deadline_date_var_name: field containing order deadline date.
        delivery_date_var_name: field containing delivery date.
        stores_mp_endpoint: a filter for elasticsearch logs.
        stores_mp_success_status: only successfuls requests are being parsed.
        period_mp_stores: how many hours before transaction time should be checked.
        stores_mp_identifier: main store identifier, i.e organization store guid or
    marketplace store guid.
    """

    store_guid_var_name: str = 'pharmacy_id'
    price_guid_var_name: str = 'price_type'
    delivery_info_var_name: str = 'delivery_info'
    deadline_date_var_name: str = 'order_deadline_date'
    delivery_date_var_name: str = 'delivery_date'

    stores_mp_endpoint: str = 'GET restapi.v1_0.views.StoreView'
    stores_mp_success_status: str = 'HTTP 2xx'

    period_mp_stores: float = 24
    stores_mp_identifier: str = 'org_store_tuid'


class AllMPSettings(PricesMPSettings, StocksMPSettings, StoresMPSettings):
    pass


class StandardMarketplaceSettings(All1CSettings, AllMPSettings):
    pass

