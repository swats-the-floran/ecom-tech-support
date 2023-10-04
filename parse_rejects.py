#!/usr/bin/env python

"""This script is needed for weekly rejected orders report.

Should be launched at mondays - it gets previous week period and
searches logs with orders rejected by Ecom in elasticsearch.

Does not include orders rejected by 1C after creating order record Ecom.
"""

import csv
import json
import time
from datetime import datetime, timedelta

from utils.ecom_elastic import get_rejects_hits
from utils.other import (
    convert_timezone,
    generate_elk_doc_link,
    parse_datetime,
)


def get_datetimes() -> tuple[datetime, datetime]:
    """Get two datetimes for previous week period from -7 days till -1 day."""
    today = datetime.today()

    begin_datetime = today.replace(hour=0, minute=0, second=0) - timedelta(days=7)
    end_datetime = today.replace(hour=23, minute=59, second=59) - timedelta(days=1)

    return (begin_datetime, end_datetime)


def get_grafana_link(begin_datetime: datetime, end_datetime: datetime) -> str:
    """Get grafana link for all marketplaces."""
    begin_unixtime = int(time.mktime(begin_datetime.timetuple()))
    end_unixtime = int(time.mktime(end_datetime.timetuple()))

    grafana_link = f'https://grafana.puls.ru/d/ENm7yVWnk/ecom-failures?orgId=1&viewPanel=64&from={begin_unixtime}000&to={end_unixtime}999'

    return grafana_link


def get_kibana_link(
    begin_dt: datetime,
    end_dt: datetime,
    marketplace: str=''
) -> str:
    """Get kibana query link for passed period."""
    begin_datetime = begin_dt.strftime('%Y-%m-%dT%H:%M:%S')
    end_datetime = end_dt.strftime('%Y-%m-%dT%H:%M:%S')

    if not marketplace:
        kibana_link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}.000',to:'{end_datetime}.999'))&_a=(columns:!(transaction.custom.response_content,user.name),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:service.environment,negate:!f,params:(query:prod),type:phrase),query:(match_phrase:(service.environment:prod))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:service.name,negate:!f,params:(query:ecom),type:phrase),query:(match_phrase:(service.name:ecom))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:labels.rejected_count,negate:!f,params:(gt:0),type:range),range:(labels.rejected_count:(gt:0)))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:%20*OrderView*'),sort:!(!('@timestamp',desc),!(user.name,desc)))"
    else:
        kibana_link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{begin_datetime}.000',to:'{end_datetime}.999'))&_a=(columns:!(transaction.custom.response_content,user.name),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:service.environment,negate:!f,params:(query:prod),type:phrase),query:(match_phrase:(service.environment:prod))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:service.name,negate:!f,params:(query:ecom),type:phrase),query:(match_phrase:(service.name:ecom))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:labels.rejected_count,negate:!f,params:(gt:0),type:range),range:(labels.rejected_count:(gt:0))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:user.name,negate:!f,params:(query:{marketplace}),type:phrase),query:(match_phrase:(user.name:{marketplace})))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name:%20*OrderView*'),sort:!(!('@timestamp',desc),!(user.name,desc)))"

    return kibana_link


def generate_report() -> None:
    """Generate two files from parsed elastic data - raw.csv and report.

    "raw.csv" file contains essential hit data like timestamp, marketplace,
    rejected, store_id and stuff like that.

    "report" file contains the body of weekly report in jira compatible
    formatting.
    """
    with open('raw.csv', 'w', encoding='utf-8') as raw_file, \
            open('report', 'w', encoding='utf-8') as report_file:

        raw_dw = csv.DictWriter(
            raw_file,
            [
                'timestamp',
                'mp',
                'errors',
                'hit_link',
                # 'order_id',
                # 'order_number',
                # 'store_id',
                # 'delivery_date',
            ],
            delimiter='\t',
        )
        raw_dw.writeheader()

        begin_datetime, end_datetime = get_datetimes()

        timezone_offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
        timezone_delta = timedelta(seconds=timezone_offset)

        begin_datetime_utc = begin_datetime + timezone_delta
        end_datetime_utc = end_datetime + timezone_delta

        print(begin_datetime_utc)
        print(end_datetime_utc)

        begin_dt = begin_datetime_utc.isoformat()[:-6] + '000Z'
        end_dt = end_datetime_utc.isoformat()[:-6] + '999Z'

        endpoint = 'POST restapi.v1_0.order.views.OrderView'

        hits = get_rejects_hits(begin_dt, end_dt, endpoint)

        grafana_link = get_grafana_link(begin_datetime, end_datetime)
        kibana_link = get_kibana_link(begin_datetime, end_datetime)
        report_file.write(f'[*Grafana*|{grafana_link}]\n')
        report_file.write(f'[*ELK*|{kibana_link}]\n')

        total_count = 0
        result_dict = {}

        for hit in hits:
            hit_link = generate_elk_doc_link(hit.meta.index, hit.meta.id)
            request = json.loads(hit.http.request.body.original)
            response = json.loads(hit.transaction.custom.response_content)
            mp = hit.user.name
            hit_datetime = parse_datetime(hit['@timestamp'])
            errors = ''

            if response.get('rejected') is not None:
                rejected = response['rejected']
                error_str = rejected[0]['error']
            elif response.get('error') is not None:
                error_str = response['error']
                rejected = error_str
            else:
                raise Exception(f'coult not parse error in the row:\n{hit}')

            if isinstance(rejected, list):
                for r in rejected:
                    # print(mp)
                    # print(type(request))
                    # print(request)
                    order_id = r['order_id']
                    if isinstance(request, list):
                        if mp == 'ozonrfbs':
                            position = next(
                                (pos for pos in request if pos['order_id'] == order_id),
                                None,
                            )
                        else:
                            position = next(
                                (pos for pos in request if pos['id'] == order_id),
                                None,
                            )
                    elif isinstance(request, dict):
                        position = request
                    else:
                        position = None

                    if position is not None:
                        errors += f"order_id: {position.get('id')}, "
                        errors += f"order_number: {position.get('number')}, "
                        errors += f"store_id: {position.get('store_id')}, "
                        errors += f"delivery_date: {position['delivery_date']}, "
                        errors += f"product_guid: {position['positions']}, "
                        errors += f"error: {error_str}"
                        errors += "\n\n"

            raw_row = {
                'timestamp': convert_timezone(hit_datetime, 'msc').isoformat()[:-3] + 'Z',
                'mp': mp,
                'errors': errors[:-2],  # TODO: replace with strip line
                'hit_link': hit_link,
            }
            raw_dw.writerow(raw_row)

            # hard to convert this string to dict so i use indexes
            detail_begin = error_str.find('ErrorDetail(string=\'')
            if detail_begin != -1:
                detail_begin += 20  # correction for 20 symbols in "ErrorDetail(string=\"'
                detail_end = error_str.find('\'', detail_begin)
                error_str = error_str[detail_begin : detail_end]

            if result_dict.get(mp) is None:
                result_dict[mp] = {}

            if result_dict[mp].get(error_str) is None:
                result_dict[mp][error_str] = 0

            result_dict[mp][error_str] += 1
            total_count += 1

        for mp, error_dict in result_dict.items():
            mp_link = get_kibana_link(begin_datetime, end_datetime, mp)
            report_file.write(f'\n[{mp}|{mp_link}]:\n')
            mp_total = 0

            for error_str, quantity in error_dict.items():
                report_file.write(f'  {quantity} отказов: {error_str}\n')
                mp_total += quantity

            report_file.write(f'*Итого по {mp}*: {mp_total}\n')

        strings_quantity = len(hits)
        if strings_quantity == total_count:
            report_file.write(f'\nВсего отказов: {strings_quantity}\n\n')
        else:
            report_file.write(f'\nНе совпало количество строк ({strings_quantity}) и обработанных отказов ({total_count}). Возможно стоит увеличить лимит хитов.')


if __name__ == '__main__':
    generate_report()
