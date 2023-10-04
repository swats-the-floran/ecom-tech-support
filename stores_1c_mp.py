#!/usr/bin/env python

"""Script generates report about stores and schedule, if possible,
transferred from 1C to Ecom and from Ecom to a marketplace in
chronological order. Whith that data user can check if Ecom got
correct data and sent it correctly.

The data is aggregating by functions from stores_1c.py and stores_mp.py.

Filtering by store is optional - you can filter by oranization instead.

Example of usage:
    ./stores_1c_mp.py -d 2022-11-26T12:00:00.000Z -m mailru -o спб
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
        stores_1c = list(parser.get_1c_stores())
        stores_mp = list(parser.get_mp_stores())

        fields_list = [field.name for field in fields(parser.dt_store_1c)]
        fields_list.pop()  # remove 'hit_link' to avoid duplicate
        fields_list += [
            field for field in [field.name for field in fields(parser.dt_store_mp)] if field not in fields_list
        ]

        # TODO: implement chunks
        stores = stores_1c + stores_mp
        stores_sorted = sorted(stores, key=lambda p: p.datetime)

        write_down_csv('stores_data.csv', fields_list, stores_sorted)
