"""Uncpecific shared functions for ecom-tech-support project."""

import csv
import logging
from dataclasses import asdict, fields
from datetime import datetime, timedelta
import os
from urllib.parse import urlparse


def write_down_csv(filename: str, fields_list: list[str], obj_list: list) -> None:
    """Create a *.csv file and write down passed data.

    Args:
        filename: name should be with extension.
        fields_list: columns that should be in the file.
        obj_list: list of dataclasses.
    """
    data_dir = 'd'
    filepath = os.path.join(data_dir, filename)

    with open(filepath, 'w', encoding='utf-8') as data_file:
        data_file_dw = csv.DictWriter(data_file, fields_list, delimiter='\t')
        data_file_dw.writeheader()

        for obj in obj_list:
            data_file_dw.writerow(asdict(obj))

    print('\n' f'created file "{data_file.name}"')


def parse_datetime(dt_str: str) -> datetime:
    """
    Parse datetime from string.

    Args:
        dt_str: Datetime in string format.
    Returns:
        datetime: Datetime in datetime format.
    Raises:
        Exception: if none of datetime formats is appliable for dt_str.
    """
    date_formats = (
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%b %d, %Y @ %H:%M:%S.%f',
    )

    dt = None
    for date_format in date_formats:
        try:
            dt = datetime.strptime(dt_str, date_format)
        except ValueError:
            pass
        else:
            break

    if dt is None:
        raise ValueError(f'unknown date fortmat: {dt_str}')

    return dt


def convert_timezone(dt: datetime, timezone: str) -> datetime:
    """Calculate datetime basing on passed datetime and direction of converting.

    Args:
        dt: Datetime that has to be converted.
        timezone: 'msc' for moscow timezone and 'utc' for utc timezone.
    Returns:
        Calculated datetime for passed timezone.
    """
    tz_delta = timedelta(hours=3)
    if timezone == 'msc':
        return dt + tz_delta
    elif timezone == 'utc':
        return dt - tz_delta

    return dt


def get_datetimes(
    transaction_datetime: str,
    time_period: int = 3,
    units: str = 'hours',
) -> tuple[str, str]:
    """Parse datetime in moscow timezone and calculate begin_datetime and end_datetime in utc.

    Args:
        transaction_datetime: str that will be converted to datetime and returned as end_datetime.
        time_period: Number of time units between begin and end datetimes.
        units: 'hours' or 'minutes'
    Returns:
        Tuple with two datetimes (begin and end of period) as strings for elastic query.
    """
    end_datetime = parse_datetime(transaction_datetime)

    if units == 'hours':
        td = timedelta(hours=time_period)
    elif units == 'minutes':
        td = timedelta(minutes=time_period)
    else:
        print('not supported time units. hours and minutes are supported now.')
        td = timedelta()

    end_datetime_utc = convert_timezone(end_datetime, 'utc')
    begin_datetime_utc = end_datetime_utc - td

    begin_datetime_str = begin_datetime_utc.isoformat()[:-3] + 'Z'
    end_datetime_str = end_datetime_utc.isoformat()[:-3] + 'Z'

    print(f'begin period {begin_datetime_str}')
    print(f'end period {end_datetime_str}')

    return (begin_datetime_str, end_datetime_str)


def generate_elk_doc_link(index: str, doc_id: str) -> str:
    """Generate elasticsearch document link with passed index and document id.

    Args:
        index: Elasticsearch index name.
        doc_id: Document id.
    Returns:
        Link to the elasticseach document.
    """
    if index.startswith('k8s-production'):
        return f'https://kibana.puls.ru/app/discover#/doc/fe7897d0-76ce-11ed-b039-8d71a039ee4d/{index}?id={doc_id}'

    return f'https://kibana.puls.ru/app/discover#/doc/3f2a04b0-a333-11eb-b29d-ad0c8e911d7c/{index}?id={doc_id}'


def generate_elk_query_link(
    script: str,
    begin_datetime: str,
    end_datetime: str,
    endpoint: str = '',
    marketplace: str = '',
    campaign_id: str = '*',
    outlet: str = '*',
    org_name_latin='*',
) -> str:
    """Generate kibana query link for stocks and stores scripts.

    Args:
        script: Name of script that calls this function.
        begin_datetime: Begin of the time period.
        end_datetime: End ot the time period.
        endpoint: API endpoint for which needs query link.
        marketplace: Marketplace name or guid.
        campaign_id: Yandex market campaign (oranization) id.
        outlet: Yandex market store id.
        org_name_latin: Organization name in latin for some endpoints.
    Returns:
        Link to the elasticseach query in kibana.
    """
    endpoint = urlparse(endpoint).path

    link = ''

    # /stores
    if script == 'stores_1c':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:puls),type:phrase),query:(match_phrase:(user.name:puls))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.type,negate:!f,params:(query:api.request),type:phrase),query:(match_phrase:(transaction.type:api.request)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:*GET*{endpoint}/v1/stores/{marketplace}'),sort:!(!('@timestamp',desc)))"
    elif script == 'stores_mp' and marketplace == 'yandexdbs':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:yandexdbs),type:phrase),query:(match_phrase:(user.name:yandexdbs)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:POST*/campaigns/{campaign_id}/outlets.json%20or%20transaction.name:PUT*/campaigns/{campaign_id}/outlets/{outlet}.json%20or%20transaction.name:DELETE*/campaigns/{campaign_id}/outlets/{outlet}.json'),sort:!(!('@timestamp',desc)))"
    elif script == 'stores_mp':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:{marketplace}),type:phrase),query:(match_phrase:(user.name:{marketplace}))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:url.path,negate:!f,params:(query:/v1.0/stores),type:phrase),query:(match_phrase:(url.path:/v1.0/stores)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"

    # /stocks
    elif script == 'stocks_1c':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:puls),type:phrase),query:(match_phrase:(user.name:puls))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.type,negate:!f,params:(query:api.request),type:phrase),query:(match_phrase:(transaction.type:api.request))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.result,negate:!f,params:(query:'200'),type:phrase),query:(match_phrase:(transaction.result:'200')))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:*POST*{endpoint}/v1/stocks*'),sort:!(!('@timestamp',desc)))"
    elif script == 'stocks_mp' and marketplace == 'sozvezdie':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:{marketplace}),type:phrase),query:(match_phrase:(user.name:{marketplace})))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:%22POST https://api.partners.esc.ru/1_0/stock_changes?api_key=4cdf72f3-bfcf-416a-8783-f73345d8ec6a%22'),sort:!(!('@timestamp',desc)))"
    elif script == 'stocks_mp' and marketplace == 'aptekaforte':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:{marketplace}),type:phrase),query:(match_phrase:(user.name:{marketplace})))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:%22POST http://esb.production.puls.local/services/api/products/stocks%22'),sort:!(!('@timestamp',desc)))"
    elif script == 'stocks_mp' and marketplace == 'yandexdbs':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(transaction.name,url.full),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:yandexdbs),type:phrase),query:(match_phrase:(user.name:yandexdbs)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:PUT*/v2/campaigns/{campaign_id}/offers/stocks.json or url.full:*/v1.0/yandex/{org_name_latin}/cart*'),sort:!(!('@timestamp',desc)))"
    elif script == 'stocks_mp':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:url.path,negate:!f,params:(query:/v1.0/stocks),type:phrase),query:(match_phrase:(url.path:/v1.0/stocks))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:{marketplace}),type:phrase),query:(match_phrase:(user.name:{marketplace})))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"

    # /prices
    elif script == 'prices_1c' and marketplace == 'aptekaforte':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:moduleb2c),type:phrase),query:(match_phrase:(user.name:moduleb2c)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:GET*/ecom-b2c.puls.ru/api/v1.0/price/*'),sort:!(!('@timestamp',desc)))"
    elif script == 'prices_1c':
        # link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.type,negate:!f,params:(query:api.request),type:phrase),query:(match_phrase:(transaction.type:api.request))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:puls),type:phrase),query:(match_phrase:(user.name:puls)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:GET*{endpoint}/v1/price/*'),sort:!(!('@timestamp',desc)))"
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.type,negate:!f,params:(query:api.request),type:phrase),query:(match_phrase:(transaction.type:api.request))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:puls),type:phrase),query:(match_phrase:(user.name:puls)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:POST*{endpoint}/v1/PriceTime'),sort:!(!('@timestamp',desc)))"
    elif script == 'prices_mp' and marketplace == 'aptekaforte':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.type,negate:!f,params:(query:api.request),type:phrase),query:(match_phrase:(transaction.type:api.request))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:aptekaforte),type:phrase),query:(match_phrase:(user.name:aptekaforte)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:POST*/services/api/products/prices*'),sort:!(!('@timestamp',desc)))"
    elif script == 'prices_mp' and marketplace == 'yandexdbs':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:yandexdbs),type:phrase),query:(match_phrase:(user.name:yandexdbs)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:POST*/campaigns/{campaign_id}/offer-prices/updates.json'),sort:!(!('@timestamp',desc)))"
    elif script == 'prices_mp' and marketplace == 'eapteka':
        link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}',to:'{end_datetime}'))&_a=(columns:!(),filters:!(('$state':(store:appState),exists:(field:transaction.custom.response_content),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.custom.response_content,negate:!f,type:exists,value:exists)),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:eapteka),type:phrase),query:(match_phrase:(user.name:eapteka)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:*apipartners.eapteka.ru%2F1_0%2Fprice_changes*'),sort:!(!('@timestamp',desc)))"
    elif script == 'prices_mp':
        # same as for stocks_mp
        link = generate_elk_query_link(
            'stocks_mp',
            begin_datetime,
            end_datetime,
            endpoint,
            marketplace,
        )

    elif script == 'infoorders':
        link = ''

    return link
