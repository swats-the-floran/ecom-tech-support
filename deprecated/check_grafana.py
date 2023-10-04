import csv
import json
import requests
import time
from datetime import datetime, timedelta


URL = "http://elasticsearch-balancer.infra.puls.local/apm-*prod-ecom*/_doc/_search"
headers = {
    'Content-Type': 'application/json',
}


def get_data(begin_datetime, end_datetime):
    """
    getting data from elastic with utc datetimes.
    """
    timezone_offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    timezone_delta = timedelta(seconds=timezone_offset)

    begin_datetime_utc = begin_datetime + timezone_delta
    end_datetime_utc = end_datetime + timezone_delta

    json_body = """
    {
      "size": 10000,
      "sort": [
        {
          "@timestamp": {
            "order": "asc",
            "unmapped_type": "boolean"
          }
        }
      ],
      "query": {
        "bool": {
          "filter": [
            {
              "range": {
                "@timestamp": {
                  "gte": "{begin_datetime}.000Z",
                  "lte": "{end_datetime}.999Z",
                  "format": "strict_date_optional_time"
                }
              }
            },
            {
              "match_phrase": {
                "transaction.result": 200
              }
            },
            {
              "match_phrase": {
                "transaction.name": "POST https://apipartners.eapteka.ru/1_0/timetables?api_key=f01f4980a85041f9835aa0f5f5d691c7"
              }
            },
            {
              "match": {
                "service.environment": "prod"
              }
            },
            {
              "match": {
                "service.name": "ecom"
              }
            }
          ]
        }
      }
    }
    """.replace(
        '{begin_datetime}',
        begin_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S')
    ).replace(
        '{end_datetime}',
        end_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S')
    )

    resp = requests.post(URL, headers=headers, data=json_body)
    r_json = resp.json()

    print(resp.status_code)
    print('hits: ' + str(r_json['hits']['total']['value']))

    return r_json


def get_datetimes():
    """
    returns two dates for previous day.
    """
    today = datetime.today()

    begin_datetime = today - timedelta(days=1)
    end_datetime = today

    return (begin_datetime, end_datetime)


def get_grafana_link(begin_datetime, end_datetime):
    """
    returns grafana link for all marketplaces.
    """
    begin_unixtime = int(time.mktime(begin_datetime.timetuple()))
    end_unixtime = int(time.mktime(end_datetime.timetuple()))

    grafana_link = f"https://grafana.puls.ru/d/7o9DTyjMz/ecom-orders-alerts?orgId=1&from={begin_unixtime}000&to={end_unixtime}999&viewPanel=16"

    return grafana_link


def get_kibana_link(begin_datetime, end_datetime):
    """
    function returns kibana link for all or one separate marketplace
    """
    begin_datetime = begin_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    end_datetime = end_datetime.strftime('%Y-%m-%dT%H:%M:%S')

    kibana_link = f"https://kibana.puls.ru/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:{begin_datetime}.000,to:{end_datetime}.999))&_a=(columns:!(),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.result,negate:!f,params:(query:'200'),type:phrase),query:(match_phrase:(transaction.result:'200'))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',key:transaction.name,negate:!f,params:(query:'POST%20https:%2F%2Fapipartners.eapteka.ru%2F1_0%2Ftimetables%3Fapi_key%3Df01f4980a85041f9835aa0f5f5d691c7'),type:phrase),query:(match_phrase:(transaction.name:'POST%20https:%2F%2Fapipartners.eapteka.ru%2F1_0%2Ftimetables%3Fapi_key%3Df01f4980a85041f9835aa0f5f5d691c7')))),hideChart:!t,index:'3f2a04b0-a333-11eb-b29d-ad0c8e911d7c',interval:auto,query:(language:kuery,query:'transaction.name%20:%20*eapteka.ru%2F1_0%2Ftimetables*'),sort:!(!('@timestamp',desc)))"
    return kibana_link


def check_p2():
    """
    function checks schedule updating for eapteka.
    p2 in https://confluence.puls.ru/pages/viewpage.action?pageId=30878351
    """

    begin_datetime, end_datetime = get_datetimes()
    
    output = '2. Обновление расписания: '

    json_data = get_data(begin_datetime, end_datetime)
    # print(json_data)
    if json_data['hits']['hits']:
        print(output + '[OK]')
    else:
        print(output + '[ERROR]')

    grafana_link = get_grafana_link(begin_datetime, end_datetime)
    kibana_link = get_kibana_link(begin_datetime, end_datetime)

    print(grafana_link)
    print(kibana_link)


if __name__ == '__main__':
    check_p2()

