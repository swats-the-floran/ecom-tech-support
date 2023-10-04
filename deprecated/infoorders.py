#!/usr/bin/env python

"""This script is needed to speed up getting information about
order reject reasons when reject comes from 1C instead of Ecom.
"""

import argparse
import json

from sqlalchemy.orm.session import Session

from utils.ecom_elastic import get_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    get_datetimes,
    parse_datetime,
)
from utils.ecom_postgres import (
    get_order_data,
    get_postgres_session,
    get_ssh_tunnel,
)


def get_data(
    pg_session: Session,
    order_identifier: str,
) -> list:
    """Get and parse orders from elastic. Filter by passed order_guid argument.

    Args:
        pg_session: PostgreSQL session.
        order_guid: order that we're looking for.
    Returns:
        List with information about order errors."""
    print('\n' 'getting 1c data...')

    order_data = get_order_data(pg_session, order_identifier)
    transaction_datetime= order_data['created_at'].isoformat()[:-10] + 'Z'
    order_guid = order_data['guid']
    end_datetime, begin_datetime = get_datetimes(transaction_datetime, -10, 'minutes')
    endpoint = '*/infoorders*'

    orders = []

    hits = get_hits(begin_datetime, end_datetime, endpoint)
    for hit in hits:
        hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
        hit_datetime = parse_datetime(hit['@timestamp'])

        orders_raw = json.loads(hit.transaction.custom.response_content)
        for order_raw in orders_raw:
            if order_raw['OrderGuid'] == order_guid:
                orders.append({
                    'datetime': str(convert_timezone(hit_datetime, 'msc')),
                    'order_state': order_raw.get('OrderState'),
                    'reason_rejected': order_raw.get('ReasonRejected'),
                    'hit_link': hit_link,
                })

    print(f'results: {len(orders)}')

    # query_link = generate_elk_query_link(
    #     'stores_1c',
    #     begin_datetime,
    #     end_datetime,
    # )
    # print(f'kibana query link: {query_link}')

    return orders


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='gets /v1/infoorders data for 1c')
    parser.add_argument(
        '-o',
        '--order_guid',
        type=str,
        metavar='',
        required=False,
        help='order guid or marketplace_number.',
    )
    args = parser.parse_args()

    order_guid = args.order_guid

    with (
        get_ssh_tunnel() as ssh_tunnel,
        get_postgres_session(ssh_tunnel) as pg_session,
        # open('infoorders_data.csv', 'w', encoding='utf-8') as data_file,
    ):
        orders = get_data(pg_session, order_guid)

        fields_list = [
            'datetime',
            'order_state',
            'reason_rejected',
            'hit_link',
        ]

        # data_file_dw = csv.DictWriter(data_file, fields_list, delimiter='\t')
        # data_file_dw.writeheader()

        for order in orders:
            print(order)
            # data_file_dw.writerow(order)

        # print('\n' f'created file "{data_file.name}"')
