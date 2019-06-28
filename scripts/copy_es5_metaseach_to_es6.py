#!/usr/bin/env python
"""
A one-off script to migrate documents from the ES5 metasearch index to ES6.

This is necessary because trying to republish all best bets from search-admin frequently fails due to transient network issues.

Requires two Elasticsearch clients: client5 and client6. These use different versions of the python client.

client5 fetches a page of docs from the old index, and client6 POSTs them to the new index. Simple!
"""
from elasticsearch5 import Elasticsearch as Elasticsearch5, TransportError as TransportError5
from elasticsearch6 import Elasticsearch as Elasticsearch6, TransportError as TransportError6
from elasticsearch6.helpers import bulk
from datetime import datetime
import os

# Using the example indices and doc types from GOV.UK's search API
# https://github.com/alphagov/rummager/tree/master/config/schema/indexes

INDEX = 'metasearch'
GENERIC_DOC_TYPE = 'generic-document'

ES5_HOST_PORT = os.getenv('ES5_ORIGIN_HOST', 'http://elasticsearch5:80')
ES6_TARGET_PORT = os.getenv('ES6_TARGET_HOST', 'http://elasticsearch6:80')

es_client5 = Elasticsearch5([ES5_HOST_PORT])
es_client6 = Elasticsearch6([ES6_TARGET_PORT])


def _prepare_docs_for_bulk_insert(docs):
    for doc in docs:
        yield {
            "_id": doc['_id'],
            "_source": doc['_source'],
        }

def bulk_index_documents_to_es6(documents):
    try:
        bulk(
            es_client6,
            _prepare_docs_for_bulk_insert(documents),
            index=INDEX,
            doc_type=GENERIC_DOC_TYPE,
            chunk_size=100
        )
    except TransportError6 as e:
        print("Failed to index documents: %s", str(e))


def fetch_documents(from_=0, page_size=100, scroll_id=None):
    try:
        if scroll_id is None:
            results = es_client5.search(INDEX, GENERIC_DOC_TYPE, from_=from_, size=page_size, scroll='2m')
            scroll_id = results['_scroll_id']
        else:
            results = es_client5.scroll(scroll_id=scroll_id, scroll='2m')
        docs = results['hits']['hits']
        return (scroll_id, docs)
    except TransportError5 as e:
        print("Failed to fetch documents: %s", str(e))
        return str(e), e.status_code


if __name__ == '__main__':
    start = datetime.now()

    dcount = es_client5.count(index=INDEX, doc_type=GENERIC_DOC_TYPE)['count']

    print('Preparing to index {} document(s) from ES5'.format(dcount))

    offset = 0
    page_size = 250
    scroll_id = None
    while offset <= dcount:
        scroll_id, docs = fetch_documents(from_=offset, page_size=page_size, scroll_id=scroll_id)

        print('Indexing documents {} to {} into ES6'.format(offset, offset+page_size))
        bulk_index_documents_to_es6(docs)

        offset += page_size

    print('Finished in {} seconds'.format(datetime.now() - start))
