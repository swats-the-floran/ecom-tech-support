"""This module is about generating and executing elasticsearch requests
The perfect state is when only 'get_hits' function is used to get data from es.

An elasticsearch-dsl library is the preferred way to create es queries but atm
it can have problems with execution time. In that case a requests liraty is acceptable.
"""

import sys

from elasticsearch import Elasticsearch, RequestError
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.utils import AttrList


SOURCE_INCLUDES = [
    '_id',
    '_index',
    '@timestamp',
    'labels.marketplace_guid',
    'labels.price_type',
    'transaction.custom.request_data',
    'transaction.custom.response_content',
    'transaction.name',
    'transaction.result',
    'transaction.type',
    'user.name',
    'http.request.body.original',
]
ECOM_INDEX = 'apm-*prod-ecom-0*'
ECOM_CLIENT_INDEX = 'k8s-production-*'

es_client = Elasticsearch(
    hosts=['http://elasticsearch-balancer.infra.puls.local:80'],
    timeout=90,
    headers={'Accept-Encoding': 'gzip, deflate'},
)


def _create_dsl_query(
    begin_dt: str,
    end_dt: str,
    endpoint: str = '',
    username: str = 'puls',
    success_status: str = '200',
):
    """Get docs for 'transaction.name: {passed_endpoint}' request parsed into Hit objects.

    Args:
        begin_dt: Begin of a query period.
        end_dt: End of a query period.
        endpoint: Value for ES 'transaction.name' parameter.
        username: Value for ES 'user.name' parameter.
        success_status: Value for ES 'transaction.result' parameter. Atm only successful transactions are being parsed.
    Returns:
        A list of Hit objects. It is still necessary to parse json request and response body.
    """
    if endpoint.find('*') != -1:
        endpoint_filter = 'wildcard'
    else:
        endpoint_filter = 'match_phrase'

    dsl_query = Search(using=es_client, index=ECOM_INDEX) \
        .query('bool', filter=[
            Q('range', **{'@timestamp': {'gte': begin_dt, 'lte': end_dt}}),
            Q(endpoint_filter, transaction__name=endpoint),
            Q('match_phrase', user__name=username),
            Q('match', transaction__result=success_status),
            Q('match', transaction__type='api.request') |
            Q('match', transaction__type='request'),
        ]) \
        .source(includes=SOURCE_INCLUDES) \
        .sort('@timestamp') \
        .extra(size=10000)

    print(dsl_query.to_dict())

    return dsl_query


def _create_ecom_client_query(
    begin_dt: str,
    end_dt: str,
    endpoint: str = '',
    method: str = 'HTTP_REQUEST'
):
    """Get docs for 'transaction.name: {passed_endpoint}' request parsed into Hit objects.

    Args:
        begin_dt: Begin of a query period.
        end_dt: End of a query period.
        endpoint: Value for ES 'transaction.name' parameter.
        method: 'HTTP_REQUEST' by default, or 'HTTP_RESPONSE'
    Returns:
        A list of Hit objects. It is still necessary to parse json request and response body.
    """
    # if endpoint.find('*') != -1:
    #     endpoint_filter = 'wildcard'
    # else:
    #     endpoint_filter = 'match_phrase'

    dsl_query = Search(using=es_client, index=ECOM_CLIENT_INDEX) \
        .query('bool', filter=[
            Q('range', **{'@timestamp': {'gte': begin_dt, 'lte': end_dt}}),
            Q('match_phrase', log_processed__request_url=endpoint),
            Q('match', log_processed__tags=method),
        ]) \
        .sort('@timestamp') \
        .extra(size=10000)

    # print(dsl_query.to_dict())

    return dsl_query


def _execute_dsl_query(dsl_query) -> AttrList:
    """Execute query and log result."""
    try:
        resp = dsl_query.execute()
    except ConnectionError:
        print('Could not connect to elasticsearch. Check your vpn and internet connection.')
        sys.exit(0)
    except RequestError:
        print(f'Wrong elasticsearch request. \n{dsl_query.to_dict()}')
        sys.exit(0)

    print('executed elastic query')
    print('hits: ' + str(len(resp.hits)))

    return resp.hits


def get_hits(
    begin_dt: str,
    end_dt: str,
    endpoint: str,
    username: str = 'puls',
    success_status: str = '200',
    project: str = 'ecom',
    method: str = 'HTTP_REQUEST',
) -> AttrList:
    """Get docs for 'transaction.name: {passed_endpoint}' request parsed into Hit objects.

    Args:
        begin_dt: Begin of a query period.
        end_dt: End of a query period.
        endpoint: Value for ES 'transaction.name' parameter.
        username: Value for ES 'user.name' parameter.
        success_status: Value for ES 'transaction.result' parameter. Atm only successful transactions are being parsed.
        project: defines what request will be used. ecom by default or ecom-client.
    Returns:
        A list of Hit objects. It is still necessary to parse json request and response body.
    """
    if project == 'ecom-client':
        dsl_query = _create_ecom_client_query(begin_dt, end_dt, endpoint, method=method)
    else:
        dsl_query = _create_dsl_query(begin_dt, end_dt, endpoint, username, success_status)

    return _execute_dsl_query(dsl_query)


def get_rejects_hits(
    begin_dt: str,
    end_dt: str,
    endpoint: str,
) -> AttrList:
    """Get docs for rejected orders request parsed into Hit objects.

    Args:
        begin_dt: Begin of a query period.
        end_dt: End of a query period.
        endpoint: Value for ES 'transaction.name' parameter.
    Returns:
        A list of Hit objects. It is still necessary to parse json request and response body.
    """
    dsl_query = Search(using=es_client, index=ECOM_INDEX) \
        .query('bool', filter=[
            Q('range', **{'@timestamp': {'gte': begin_dt, 'lte': end_dt}}),
            Q('range', labels__rejected_count={'gt': 0, 'lt': None}),
            Q('match_phrase', transaction__name=endpoint),
        ]) \
        .source(includes=SOURCE_INCLUDES) \
        .sort('@timestamp') \
        .extra(size=10000)

    return _execute_dsl_query(dsl_query)


def get_stocks_yandexdbs_hits(
    begin_dt: str,
    end_dt: str,
    marketplace: str,
    campaign_id: str,
    org_name_latin: str,
) -> AttrList:
    """
    executes a preset query with arguments it gets and returns list of hits.
    """
    stocks_endpoint = f'PUT https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/offers/stocks.json'
    cart_endpoint = f'/v1.0/yandex/{org_name_latin}/cart'

    dsl_query = Search(using=es_client, index=ECOM_INDEX) \
        .query('bool', filter=[
            Q('range', **{'@timestamp': {'gte': begin_dt, 'lte': end_dt}}),
            Q(
                'bool',
                should=[
                    Q('match_phrase', transaction__name=stocks_endpoint),
                    Q('match_phrase', url__path=cart_endpoint),
                ],
                minimum_should_match=1,
            ),
            Q(
                'bool',
                should=[
                    Q('match', transaction__result='200'),
                    Q('match', transaction__result='HTTP 2xx'),
                ],
                minimum_should_match=1,
            ),
            Q('match', user__name=marketplace),
        ]) \
        .sort('@timestamp') \
        .extra(size=10000) \
        .source(includes=SOURCE_INCLUDES)

    print(dsl_query.to_dict())

    return _execute_dsl_query(dsl_query)


def get_stores_yandexdbs_hits(
    begin_dt: str,
    end_dt: str,
    marketplace: str,
    campaign_id: str,
    outlet: str,
) -> AttrList:
    """
    executes a preset query with arguments it gets and returns list of hits.
    """
    post_endpoint = f'POST https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/outlets/{outlet}.json'
    put_endpoint = f'PUT https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/outlets/{outlet}.json'
    del_endpoint = f'DELETE https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/outlets/{outlet}.json'

    dsl_query = Search(using=es_client, index=ECOM_INDEX) \
        .query('bool', filter=[
            Q('range', **{'@timestamp': {'gte': begin_dt, 'lte': end_dt}}),
            Q(
                'bool',
                should=[
                    Q('wildcard', transaction__name=post_endpoint),
                    Q('wildcard', transaction__name=put_endpoint),
                    Q('wildcard', transaction__name=del_endpoint),
                ],
                minimum_should_match=1,
            ),
            Q('match', transaction__result='200'),
            Q('match', user__name=marketplace),
        ]) \
        .sort('@timestamp') \
        .extra(size=10000) \
        .source(includes=SOURCE_INCLUDES)

    return _execute_dsl_query(dsl_query)
