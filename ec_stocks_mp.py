#!/usr/bin/env python

"""Script generates report about stocks transferred from 1C to Ecom.
Whith that data user can check if Ecom got correct data.

Filtering by product is optional but there must be one of two filters -
organization or store. Though store is needed just to find its organization.

Example of usage:
    ./stocks_1c.py -d 2022-11-26T12:00:00.000Z -m mailru -o спб -p 12345
"""

from dataclasses import fields

from parsers.ec_parsers import marketplaces_map
from utils.ecom_argparse import get_args_stocks_prices
from utils.other import write_down_csv


if __name__ == '__main__':
    args = get_args_stocks_prices()

    parser = marketplaces_map[args.marketplace](
        args.datetime,
        args.marketplace,
        args.store,
        args.product,
    )
    stocks = parser.get_mp_stocks()

    fields_list = [field.name for field in fields(parser.dt_ec_stock_client)]
    write_down_csv('stocks_data.csv', fields_list, stocks)
