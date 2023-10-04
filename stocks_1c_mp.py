#!/usr/bin/env python

"""Script generates report about stocks transferred from 1C to Ecom
and from Ecom to a marketplace in chronological order. Whith that data
user can check if Ecom got correct data and sent it correctly.

The data is aggregating by functions from stocks_1c.py and stocks_mp.py.

Filtering by product is optional but there must be one of two filters -
organization or store. Though store is needed just to find its organization.

Example of usage:
    ./stocks_1c_mp.py -d 2022-11-26T12:00:00.000Z -m mailru -o спб -p 12345
"""

from dataclasses import fields

from parsers.ecom_parsers import marketplaces_map
from utils.ecom_argparse import get_args_stocks_prices
from utils.other import write_down_csv
from utils.ecom_postgres import get_postgres_session, get_ssh_tunnel


if __name__ == '__main__':
    args = get_args_stocks_prices()

    with (get_ssh_tunnel() as ssh_tunnel,
            get_postgres_session(ssh_tunnel) as pg_session):

        parser = marketplaces_map[args.marketplace](
            pg_session,
            args.datetime,
            args.marketplace,
            args.organization,
            args.store,
            args.product,
        )
        stocks_1c = list(parser.get_1c_stocks())
        stocks_mp = list(parser.get_mp_stocks())

        fields_list = [field.name for field in fields(parser.dt_stock_1c)]
        fields_list.pop()  # remove 'hit_link' to avoid duplicate
        fields_list += [
            field for field in [field.name for field in fields(parser.dt_stock_mp)] if field not in fields_list
        ]

        # TODO: implement chunks
        stocks = stocks_1c + stocks_mp
        stocks_sorted = sorted(stocks, key=lambda p: p.datetime)

        write_down_csv('stocks_data.csv', fields_list, stocks_sorted)
