#!/usr/bin/env python

"""Functions getting data from postgresql database go here."""

import os
import sys
from contextlib import contextmanager
from typing import Generator, Union
from uuid import UUID

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session, sessionmaker
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

load_dotenv()

# there is no straight and unambiguous way to determine region code
# of organization from database at this moment (2022.06.18)
org_names_region_codes = {
    'ООО "ФК ПУЛЬС"': 77,
    'ООО "ПУЛЬС Ярославль"': 76,
    'ООО "ПУЛЬС Брянск"': 32,
    'ООО "ПУЛЬС СПб"': 78,
    'ООО "ПУЛЬС Волгоград"': 34,
    'ООО "ПУЛЬС Воронеж"': 36,
    'ООО "ПУЛЬС Казань"': 16,  # Tatarstan
    'ООО "ПУЛЬС Краснодар"': 23,
    'ООО "ПУЛЬС Хабаровск"': 27,
    'ООО "ПУЛЬС Иркутск"': 38,
    'ООО "ПУЛЬС Красноярск"': 24,
    'ООО "ПУЛЬС Екатеринбург"': 66,
    'ООО "ПУЛЬС Новосибирск"': 54,
    'ООО "ПУЛЬС Самара"': 63,
}


@contextmanager
def get_ssh_tunnel() -> Generator[SSHTunnelForwarder, None, None]:
    """Get ssh credentials and postgres db ip and create connection.

    Yields:
        Connected ssh tunnel.
    """
    # ssh settings
    ssh_host = os.environ['SSH_HOST']
    ssh_port = int(os.environ['SSH_PORT'])
    ssh_username = os.environ['SSH_USERNAME']
    ssh_password = os.environ['SSH_PASSWORD']
    pg_host = os.environ['POSTGRES_HOST']
    pg_port = int(os.environ['POSTGRES_PORT'])

    ssh_tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address=(pg_host, pg_port),
        allow_agent=False
    )

    try:
        ssh_tunnel.start()
    except BaseSSHTunnelForwarderError:
        print('could not connect to ssh')
        sys.exit(0)

    print('connected to ssh')

    yield ssh_tunnel

    ssh_tunnel.close()


@contextmanager
def get_postgres_session(ssh_tunnel: SSHTunnelForwarder) -> Generator[Session, None, None]:
    """Get db credentials, connects to the db and returns.

    Args:
        ssh_tunnel: active ssh tunnel to server with sql server.
    Yields:
        postgresql session.
    """
    # postgres settings
    db_host = os.environ['POSTGRES_HOST']
    db_port = os.environ['POSTGRES_PORT']
    db_username = os.environ['POSTGRES_USERNAME']
    db_password = os.environ['POSTGRES_PASSWORD']
    db_name = os.environ['POSTGRES_DBNAME']

    # connect to PostgreSQL
    db_port = str(ssh_tunnel.local_bind_port)
    engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    Session = sessionmaker(bind=engine)
    try:
        session = Session()
    except Exception:
        print('could not connect to PostgreSQL')
        sys.exit(0)

    print('connected to postgres')

    yield session

    session.close()


def execute_query(pg_session: Session, query: str) -> Result:
    """Wrapper for execute function that logs every database hit.

    Args:
        pg_session: Postgresql session.
        query: string with sql query.
    Returns:
        Data got from postgresql database.
    """
    query_result = pg_session.execute(query)
    print('executed sql query')

    return query_result


def get_prices_data(
    pg_session: Session,
    marketplace: str,
    price_guids: list[str],
) -> dict:
    """Enrich price data with price types and organization names.

    Args:
        pg_session: Postgresql session.
        marketplace: MP name.
        price_guids: List of price guids.
    Returns:
        Prices with related price type and org name.
    """
    price_guids_str = "('" + "\', \'".join(price_guids) + "')"

    query_text = f"""
    SELECT
        org_price.guid,
        org_price.price_type,
        org.name
    FROM price_organizationprice org_price
        INNER JOIN core_organization org
            ON org_price.organization_id = org.id
            AND org_price.guid IN {price_guids_str}
            AND org.name IS NOT NULL
        INNER JOIN marketplace_marketplace mp
            ON org_price.marketplace_id = mp.id
        INNER JOIN users_user user_
            ON user_.id = mp.api_user_id
            AND user_.username = '{marketplace}'
    """

    query_result = execute_query(pg_session, query_text)

    price_org_dict = {}
    for row in query_result:
        price_org_dict[str(row[0])] = {
            'price_type': row[1],
            'org_name': row[2],
        }

    return price_org_dict


def get_related_products(
    pg_session: Session,
    marketplace: str,
    org_name: str,
    original_product_guid: str,
) -> tuple[str]:
    """Get list of duplicated related to master product for passed mp and organization.

    Args:
        pg_session: Postgresql session.
        marketplace: MP name.
        org_name: Organization name.
        original_product_guid: Product guid.
    Returns:
        Tuple of related products' guids.
    """
    query_text = f"""
    SELECT
        product_fields.guid
    FROM product_activeduplicaterelations duplicates
        INNER JOIN product_marketplaceproduct product_filter
            ON duplicates.mp_product_original_id = product_filter.id
            AND duplicates.mp_product_original_id != duplicates.mp_product_related_id
            AND product_filter.guid = '{original_product_guid}'
        INNER JOIN marketplace_marketplace mp_filter
            ON duplicates.marketplace_id = mp_filter.id
            INNER JOIN users_user user_
                ON mp_filter.api_user_id = user_.id
                AND user_.username = '{marketplace}'
        INNER JOIN core_organization org_filter
            ON duplicates.organization_id = org_filter.id
            AND org_filter.name = '{org_name}'
        LEFT JOIN product_marketplaceproduct product_fields
            ON duplicates.mp_product_related_id = product_fields.id
    GROUP BY
        product_fields.guid
    """

    query_result = execute_query(pg_session, query_text)

    related_products = [original_product_guid,]  # may be missing in query_result
    for row in query_result:
        related_products.append(str(row[0]))
    related_products = tuple(related_products)

    print(f'related products: {related_products}')

    return related_products


def get_original_product(
    pg_session: Session,
    marketplace: str,
    org_name: str,
    related_product_guid: str,
) -> str:
    """Get original product by related product guid.

    Args:
        pg_session: Postgresql session.
        marketplace: MP name.
        org_name: Organization name.
        related_product_guid: Product guid.
    Returns:
        Related product's guid.
    """
    query_text = f"""
    SELECT
        product_fields.guid
    FROM product_activeduplicaterelations padr
        INNER JOIN product_marketplaceproduct product_filter
            ON padr.mp_product_original_id = product_filter.id
            AND product_filter.guid = '{related_product_guid}'
        INNER JOIN marketplace_marketplace mp_filter
            on padr.marketplace_id = mp_filter.id
            INNER JOIN users_user uu
                ON mp_filter.api_user_id = uu.id
                AND uu.username = '{marketplace}'
        INNER JOIN core_organization org_filter
            ON padr.organization_id = org_filter.id
            AND org_filter.name = '{org_name}'
        LEFT JOIN product_marketplaceproduct product_fields
            ON padr.mp_product_related_id = product_fields.id
    GROUP BY
        product_fields.guid
    """

    query_result = execute_query(
        pg_session,
        query_text,
    )

    original_product = str(next(query_result)[0])

    print(f'original product: {original_product}')

    return original_product


def get_marketplace_guid(
    pg_session: Session,
    marketplace: str,
) -> str:
    """Gets store guid by store id.

    Args:
        pg_session: Postgresql session.
        marketplace: MP name.
    Returns:
        Store guid.
    """
    query_text = f"""
    SELECT
        mp.guid
    FROM marketplace_marketplace mp
        INNER JOIN users_user user_
            ON mp.api_user_id = user_.id
            AND user_.username = '{marketplace}'
    """

    query_result = execute_query(pg_session, query_text)

    marketplace_guid = str(next(query_result)[0])

    print(f'marketplace guid {marketplace_guid}')

    return marketplace_guid


def get_product_data(pg_session: Session, identifier: str) -> dict:
    """Enrich product data - guid or id.

    Args:
        pg_session: Postgresql session.
        identifier: Product id or guid.
    Returns:
        Dict with product id and guid. If identifier is None - returns empty dict.
    """
    product_data = {}

    if identifier is None:
        return product_data

    try:
        UUID(identifier)
        product_data['guid'] = identifier
    except ValueError:
        product_data['code'] = identifier

    if product_data.get('guid'):
        query_text = f"""
        SELECT
            org_price.code
        FROM product_organizationproduct org_price
        WHERE
            org_price.guid = '{identifier}'
        """

        query_result = execute_query(
            pg_session,
            query_text,
        )
        product_code = str(next(query_result)[0])
        product_data['code'] = product_code

    elif product_data.get('code'):
        query_text = f"""
        SELECT
            org_price.guid
        FROM product_organizationproduct org_price
        WHERE
            org_price.code = '{identifier}'
        """

        query_result = execute_query(pg_session, query_text)
        product_guid = str(next(query_result)[0])
        product_data['guid'] = product_guid

    print(f'product identifiers: {product_data.values()}')

    return product_data


def get_store_data(pg_session: Session, identifier: str) -> dict:
    """Enrich store data by guid, id, outlet number.

    Args:
        pg_session: Postgresql session.
        identifier: store id or guid.
    Returns:
        Dict with store info. If identifier is None - returns empty dict.
    """
    store_data = {}

    if not identifier:
        return store_data

    try:
        # if code passed as a string
        UUID(identifier)
        store_data['guid'] = identifier.lower()
    except ValueError:
        pass

    if not store_data.get('guid'):
        # check if store code
        query_text = f"""
        SELECT
            org_address.address_guid
        FROM delivery_organizationaddress org_address
        WHERE
            org_address.address_id = '{identifier}'
        """

        try:
            query_result = next(execute_query(pg_session, query_text))
            store_guid = str(query_result[0])
            return get_store_data(pg_session, store_guid)
        except StopIteration:
            pass

        # check if yandex outlet id
        query_text = f"""
        SELECT
            org_address.address_guid
        FROM delivery_organizationaddress org_address
        WHERE
            org_address.outlet_id = {identifier}
        """

        try:
            query_result = next(execute_query(pg_session, query_text))
            store_guid = str(query_result[0])
            return get_store_data(pg_session, store_guid)
        except StopIteration:
            pass

    if store_data.get('guid') is None:
        raise Exception(f'Could not find organization by {identifier}')

    # marketplace with id = 12 is broken
    query_text = f"""
    SELECT
        org_address.address_id,
        org_address.outlet_id,
        org.name
    FROM delivery_organizationaddress org_address
        INNER JOIN core_organization org
            ON org_address.organization_id = org.id
            AND not org_address.marketplace_id = 12
            AND org_address.address_guid = '{identifier}'
    ORDER BY
        org_address.outlet_id
    LIMIT 1
    """

    # print(identifier)

    query_result = next(execute_query(pg_session, query_text))

    store_data['id'] = query_result[0]
    store_data['outlet'] = str(query_result[1]) if query_result[1] is not None else ''
    store_data['org_name'] = query_result[2]

    region_code = org_names_region_codes.get(store_data['org_name'])
    if region_code is None:
        region_code = 0

    store_data['region_code'] = region_code

    print('outlet id: ' + store_data['outlet'])

    return store_data


def get_organization_data(pg_session: Session, identifier: Union[str,int]) -> dict[str, str]:
    """Enrich organization data by guid, id, name, main region code,
    related region codes, endpoint, campaign id and latin name.

    Args:
        pg_session: Postgresql session.
        identifier: organization's main region, partial or full name or yandex campaign id.
    Returns:
        Dict with store info. If identifier is None - returns empty dict.
    """
    org_data = {}

    if not identifier:
        return org_data

    try:
        # if code passed as a string
        identifier = int(identifier)
    except ValueError:
        pass

    if isinstance(identifier, str):
        # check if organization name
        for key in org_names_region_codes.keys():
            if key.lower().find(identifier.lower()) != -1:
                org_data['org_name'] = key
                org_data['org_region_code'] = org_names_region_codes[key]
                break

    if isinstance(identifier, int):
        # check if region code
        region_codes_org_names = dict((v,k) for k,v in org_names_region_codes.items())
        if region_codes_org_names.get(identifier) is not None:
            org_name = region_codes_org_names[identifier]
            return get_organization_data(pg_session, org_name)

        # check if campaign id
        query_text = f"""
        SELECT
            org.name
        FROM marketplace_marketplaceapisettings mp_settings
            INNER JOIN price_organizationprice org_price
                ON mp_settings.price_type = org_price.guid
                AND mp_settings.campaign_id = '{identifier}'
            INNER JOIN core_organization org
                ON org_price.organization_id = org.id
        LIMIT 1
        """
        try:
            query_result = next(execute_query(pg_session, query_text))
        except StopIteration:
            pass
        else:
            org_name = query_result[0]
            return get_organization_data(pg_session, org_name)

    if org_data.get('org_name') is None:
        raise Exception(f'Could not find organization by {identifier}')

    # remember that multitoken price_type is text atm. it can be changed
    # by ecom developers -_-_-
    query_text = f"""
    SELECT
        org.endpoint,
        org.id,
        mp_settings.campaign_id AS yandex_campaign_id,
        token_.metadata AS orga_name_latin
    FROM marketplace_marketplaceapisettings mp_settings
        -- yandex stuff section
        INNER JOIN price_organizationprice org_price
            ON mp_settings.price_type = org_price.guid
            AND mp_settings.campaign_id IS NOT NULL
        INNER JOIN marketplace_marketplace mp
            ON mp_settings.marketplace_id = mp.id
        INNER JOIN users_user user_
            ON mp.api_user_id = user_.id
            AND user_.username = 'yandexdbs'
        LEFT JOIN multitoken_multitoken token_
            ON user_.id = token_.user_id
            AND mp_settings.price_type::text = token_.price_type::text
        -- id and api endpoint
        INNER JOIN core_organization org
            ON org_price.organization_id = org.id
            AND lower(org.name) LIKE lower('%{org_data['org_name']}%')
    LIMIT 1
    """

    query_result = next(execute_query(pg_session, query_text))
    org_data['endpoint'] = query_result[0]
    org_data['org_id'] = query_result[1]
    org_data['campaign_id'] = query_result[2]
    org_data['org_name_latin'] = query_result[3]
    org_data['related_region_codes'] = get_organization_regions(pg_session, org_data['org_name'])

    query_text = f"""
    SELECT
        mp_settings.campaign_id as sbermm_campaign_id
    FROM marketplace_marketplaceapisettings mp_settings
        INNER JOIN price_organizationprice org_price
            ON mp_settings.price_type = org_price.guid
            AND mp_settings.campaign_id IS NOT NULL
			AND org_price.organization_id = {org_data['org_id']}
        INNER JOIN marketplace_marketplace mp
            ON mp_settings.marketplace_id = mp.id
        INNER JOIN users_user user_
            ON mp.api_user_id = user_.id
            AND user_.username = 'sbermm'
        LEFT JOIN multitoken_multitoken token_
            ON user_.id = token_.user_id
            AND mp_settings.price_type::text = token_.price_type::text
    LIMIT 1
    """

    query_result = next(execute_query(pg_session, query_text))
    org_data['sbermm_campaign_id'] = query_result[0]

    print('\n' + 'getting organization data...')
    print('organization:\t\t ' + org_data['org_name'])
    print('endpoint:\t\t ' + org_data['endpoint'])
    print('region code:\t\t ' + str(org_data['org_region_code']))
    print('related region codes:\t ' + str(org_data['related_region_codes']))
    print('yandex campaign id:\t ' + org_data['campaign_id'])
    print('sbermm_campaign_id:\t ' + org_data['sbermm_campaign_id'])
    print('latin organization name: ' + org_data['org_name_latin'])

    return org_data


def get_order_data(pg_session: Session, identifier: str) -> dict:
    """Enrich order data by order identifier.

    Args:
        pg_session: Postgresql session.
        identifier: order's guid.
    Returns:
        Dict with store info. If identifier is None - returns empty dict.
    """
    order_data = {}

    if not identifier:
        return order_data

    try:
        UUID(identifier)
        order_data['guid'] = identifier

        query_text = f"""
        SELECT
            order_.created,
            order_.guid
        FROM order_order order_
        WHERE
            order_.guid = '{identifier}'
        """
    except ValueError:
        order_data['code'] = identifier

        query_text = f"""
        SELECT
            order_.created,
            order_.guid
        FROM order_order order_
        WHERE
            order_.marketplace_number = '{identifier}'
        """

    query_result = next(execute_query(pg_session, query_text))

    created_at, order_guid = query_result[0], query_result[1]
    order_data['created_at'] = created_at
    order_data['guid'] = str(order_guid)

    print(f'order {identifier} created at {created_at}')

    return order_data


def get_organization_regions(pg_session: Session, org_name: str) -> tuple[int]:
    """Get a list of orgnization related region codes.

    Organization can cover few regions and has a list of region codes related to it.

    Args:
        pg_session: Postgresql session.
        org_name: full and register sensitive organization name.
    Returns:
        list of organization's region codes.
    """
    query_text = f"""
    SELECT DISTINCT
        region.code
    FROM address_region region
        INNER JOIN delivery_organizationaddress org_address
            ON region.name = org_address.region
            AND region IS NOT NULL
        INNER JOIN core_organization org
            ON org_address.organization_id = org.id
            AND org.name = '{org_name}'
    ORDER BY
        region.code
    """

    query_result = execute_query(pg_session, query_text)

    region_codes = []
    for row in query_result:
        region_codes.append(int(row[0]))
    region_codes = tuple(region_codes)

    return region_codes


def get_price_settings(
    pg_session: Session,
    marketplace: str,
    org_name: str,
) -> dict[str, bool]:
    """Get price settings (use or not b2c module to load prices) as a dict.

    Args:
        pg_session: Postgresql session.
        marketplace: MP name.
        org_name: Organization name.
    Returns:
        Dict with price guids as keys and b2c usage flags as values.
    """
    query_text = f"""
    with actual_prices AS (
    SELECT
        org_price.date_at date_at,
        org_price.guid price_guid,
        org_price.price_type price_type,
        org.name org_name,
        org.id org_id,
        mp.id mp_id,
        user_.username mp_name
    FROM price_organizationprice org_price
        INNER JOIN core_organization org
            ON org_price.organization_id = org.id
            AND org.name = '{org_name}'
        INNER JOIN marketplace_marketplace mp
            ON org_price.marketplace_id = mp.id
        INNER JOIN users_user user_
            ON mp.api_user_id = user_.id
            AND user_.username = '{marketplace}'
    )
    SELECT
        price_unique.guid,
        mp_price_settings.enable_moduleb2c_prices,
        actual_prices.mp_name,
        actual_prices.org_name,
        actual_prices.date_at,
        actual_prices.price_type
    FROM marketplace_marketplacepricetypesettings mp_price_settings
        INNER JOIN price_organizationpriceunique org_price_unique
            ON mp_price_settings.org_price_unique_id = org_price_unique.id
        INNER JOIN price_priceunique price_unique
            ON org_price_unique.price_id = price_unique.id
        INNER JOIN actual_prices
            ON price_unique.guid = actual_prices.price_guid
            AND org_price_unique.organization_id  = actual_prices.org_id
            AND org_price_unique.marketplace_id = actual_prices.mp_id
            AND price_unique.price_type = actual_prices.price_type
    ORDER BY
        actual_prices.date_at
    """

    query_result = execute_query(pg_session, query_text)
    # we get pricelists for few days sorted by date and rewrite earlier keys values by newer keys values
    # until we get only latest pricelists.
    prices_settings_dict = {}
    for row in query_result:
        prices_settings_dict[str(row[0])] = row[1]

    print(prices_settings_dict)

    return prices_settings_dict


if __name__ == '__main__':
    print('testing postgres connect')

    with (get_ssh_tunnel() as ssh_tunnel,
            get_postgres_session(ssh_tunnel) as pg_session):

        result = execute_query(
            pg_session,
            'SELECT id FROM order_order WHERE id=100000'
        )
        print('[SUCCESS]' if next(result)[0] == 100000 else '[FAIL]')
