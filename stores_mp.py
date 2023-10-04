#!/usr/bin/env python

"""Script generates report about stores and schedule, if possible,
transferred from Ecom to a marketplace. Whith that data user can
check if Ecom sent it correctly.

Filtering by store is optional - you can filter by oranization instead.

Example of usage:
    ./stores_mp.py -d 2022-11-26T12:00:00.000Z -m mailru -o спб
"""

from dataclasses import fields

from parsers.ecom_parsers import marketplaces_map
from utils.ecom_argparse import get_args_stores
from utils.other import write_down_csv
from utils.ecom_postgres import get_postgres_session, get_ssh_tunnel


if __name__ == '__main__':
    args = get_args_stores()

    with (get_ssh_tunnel() as ssh_tunnel,
            get_postgres_session(ssh_tunnel) as pg_session):

        parser = marketplaces_map[args.marketplace](
            pg_session,
            args.datetime,
            args.marketplace,
            args.organization,
            args.store,
        )
        stores = parser.get_mp_stores()

        fields_list = [field.name for field in fields(parser.dt_store_mp)]

        write_down_csv('stores_data.csv', fields_list, stores)
