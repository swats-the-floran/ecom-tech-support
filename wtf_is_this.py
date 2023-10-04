#!/usr/bin/env python

from uuid import UUID

from sqlalchemy.orm.session import Session

from utils.ecom_postgres import get_postgres_session, get_ssh_tunnel


def detect_object(pg_session: Session, identifier: str) -> None:
    """Execute a list of queries with passed idenfier to find out what it is."""
    queries = {}

    try:
        UUID(identifier)
        uuid = True
    except ValueError:
        uuid = False

    if uuid:
        marketplace_query = f'''
        select
            *
        from
            marketplace_marketplace
        where
            guid = '{identifier}'
        '''
        queries['marketplace'] = marketplace_query


        organization_query = f'''
        select
            *
        from
            core_organization
        where
            guid = '{identifier}'
        '''
        queries['organization'] = organization_query


        address_query = f'''
        select
            *
        from
            delivery_organizationaddress
        where
            address_guid = '{identifier}'
        '''
        queries['address'] = address_query


        price_query = f'''
        select
            *
        from
            price_organizationprice
        where
            guid = '{identifier}'
        '''
        queries['price'] = price_query


        product_query = f'''
        select
            *
        from
            product_organizationproduct
        where
            guid = '{identifier}'
        '''
        queries['product'] = product_query

    else:
        token_query = f'''
        select
            uu.username,
            mt.*
        from
            multitoken_multitoken mt
            left join users_user uu
                on mt.user_id = uu.id
        where
            key = '{identifier}'
        '''
        queries['token'] = token_query


        token_query = f'''
        select
            *
        from
            core_organization
        where
            base_auth = '{identifier}'
        '''
        queries['base auth key for all organizations'] = token_query

        product_code_query = f'''
        select
            pop.*
        from
            product_organizationproduct pop
        where
            code = '{identifier}'
        '''
        queries['product'] = product_code_query

        yandex_campaign_id = f'''
        select
            mp_api.*
        from
            marketplace_marketplaceapisettings mp_api
        where
            mp_api.campaign_id = '{identifier}'
        '''
        queries['yandex campaign id'] = yandex_campaign_id

    for query_name, query_text in queries.items():
        query_result = pg_session.execute(query_text)
        obj = next(query_result, None)

        if obj is not None:
            print(query_name + ': [YES]\n')
            print(obj)
            break
        else:
            print(query_name + ': [NO]')


if __name__ == '__main__':
    identifier = input('please endter identifier to search: ')

    with (get_ssh_tunnel() as ssh_tunnel,
            get_postgres_session(ssh_tunnel) as pg_session):

        print()
        detect_object(pg_session, identifier)
