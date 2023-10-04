from sshtunnel import argparse


def get_args_stores():
    parser = argparse.ArgumentParser(description='gets stocks data for mp')
    parser.add_argument(
        '-d',
        '--datetime',
        type=str,
        metavar='',
        required=True,
        help='transaction datetime i.e. 2022-04-05T01:58:45.430Z',
    )
    parser.add_argument(
        '-m',
        '--marketplace',
        type=str,
        metavar='',
        required=True,
        help='marketplace name',
    )

    org_store_group = parser.add_argument_group(
        title='region/store. only one option in this group can be selected',
    )
    mxg = org_store_group.add_mutually_exclusive_group(required=True)
    mxg.add_argument(
        '-o',
        '--organization',
        type=str,
        metavar='',
        default='',
        help='organization name or its region code like "77" for ФК Пульс.',
    )
    mxg.add_argument(
        '-s',
        '--store',
        type=str,
        metavar='',
        default='',
        help='store guid like "85a2ef11-d2a9-48bf-ad7e-4309f05c9ec4" ' \
             'or id like "МСК000246759".',
    )

    args = parser.parse_args()

    return args


def get_args_stocks_prices():
    parser = argparse.ArgumentParser(description='gets stocks data for mp')
    parser.add_argument(
        '-d',
        '--datetime',
        type=str,
        metavar='',
        required=True,
        help='transaction datetime i.e. 2022-04-05T01:58:45.430Z',
    )
    parser.add_argument(
        '-m',
        '--marketplace',
        type=str,
        metavar='',
        required=True,
        help='marketplace name',
    )

    org_store_group = parser.add_argument_group(
        title='region/store. only one option in this group can be selected',
    )
    mxg = org_store_group.add_mutually_exclusive_group(required=True)
    mxg.add_argument(
        '-o',
        '--organization',
        type=str,
        metavar='',
        help='organization name or its region code like "77" for ФК Пульс.',
    )
    mxg.add_argument(
        '-s',
        '--store',
        type=str,
        metavar='',
        help='store guid like "85a2ef11-d2a9-48bf-ad7e-4309f05c9ec4" ' \
             'or id like "МСК000246759".',
    )

    parser.add_argument(
        '-p',
        '--product',
        # nargs='+',
        type=str,
        metavar='',
        required=False,
        # default=tuple(),
        help='product guid or code',
    )
    args = parser.parse_args()

    return args
