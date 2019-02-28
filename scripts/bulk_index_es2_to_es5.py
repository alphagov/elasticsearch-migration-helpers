#!/usr/bin/env python
"""
A one-off script to migrate documents from an ES2 index to an ES5 index.

Requires two Elasticsearch clients: client2 and client5. These use different versions of the python client.

Also requires 'six' for Py2/Py3 compatibility.

client2 fetches a page of docs from the old index, and client5 POSTs them to the new index. Simple!
"""
from elasticsearch2 import Elasticsearch as Elasticsearch2, TransportError as TransportError2
from elasticsearch5 import Elasticsearch as Elasticsearch5, TransportError as TransportError5
from elasticsearch5.helpers import bulk
from datetime import datetime
from six import iteritems
import os

# Using the example indices and doc types from GOV.UK's search API
# https://github.com/alphagov/rummager/tree/master/config/schema/indexes

INDICES = [
    'page-traffic',
    'metasearch',
    'govuk',
    'government',
    'detailed',
]
DOC_TYPES = [
    'aaib_report',
    'asylum_support_decision',
    'best_bet',
    'business_finance_support_scheme',
    'cma_case',
    'contact',
    'countryside_stewardship_grant',
    'dfid_research_output',
    'drug_safety_update',
    'edition',
    'employment_appeal_tribunal_decision',
    'employment_tribunal_decision',
    'european_structural_investment_fund',
    'export_health_certificate',
    'hmrc_manual',
    'hmrc_manual_section',
    'international_development_fund',
    'maib_report',
    'manual',
    'manual_section',
    'medical_safety_alert',
    'page-traffic',
    'policy',
    'raib_report',
    'residential_property_tribunal_decision',
    'service_manual_guide',
    'service_manual_topic',
    'service_standard_report',
    'statutory_instrument',
    'tax_tribunal_decision',
    'utaac_decision'
]

GENERIC_DOC_TYPE = 'generic-document'

ES2_HOST_PORT = os.getenv('ES2_ORIGIN_HOST', 'http://localhost:9200')
ES5_TARGET_PORT = os.getenv('ES5_TARGET_HOST', 'http://localhost:9205')

es_client2 = Elasticsearch2([ES2_HOST_PORT])
es_client5 = Elasticsearch5([ES5_TARGET_PORT])


def count_docs_for_doctype(client, doc_type, index):
    return client.count(index=index, doc_type=doc_type)['count']


def index_individual_docs(index, docs):
    for doc in docs:
        result, status_code = index_document_to_es5(index, doc)
        if status_code != 200:
            print(result, status_code)


def _prepare_docs_for_bulk_insert(docs):
    for doc in docs:
        yield {
            "_id": doc['_id'],
            "_source": doc['_source'],
        }

def bulk_index_documents_to_es5(index_name, documents):
    try:
        bulk(
            es_client5,
            _prepare_docs_for_bulk_insert(documents),
            index=index_name,
            doc_type=GENERIC_DOC_TYPE,
            chunk_size=100
        )
    except TransportError5 as e:
        index_individual_docs(index_name, documents)


def index_document_to_es5(index_name, document):
    try:
        es_client5.index(
            index=index_name,
            id=document['_id'],
            doc_type=GENERIC_DOC_TYPE,
            body=document['_source'])
        return "acknowledged", 200
    except TransportError5 as e:
        print(
            "Failed to index the document %s: %s",
            document['_id'], str(e)
        )
        return str(e), e.status_code


def fetch_documents_from_es2(doc_type, from_=0, page_size=100, index_name='govuk', scroll_id=None):
    try:
        if scroll_id is None:
            results = es_client2.search(index_name, doc_type, from_=from_, size=page_size, scroll='2m')
            scroll_id = results['_scroll_id']
        else:
            results = es_client2.scroll(scroll_id=scroll_id, scroll='2m')
        docs = results['hits']['hits']
        return (scroll_id, docs)
    except TransportError2 as e:
        print(
            "Failed to fetch documents from %s: %s",
            index_name, str(e)
        )
        return str(e), e.status_code


def list_docs_for_each_doctype(index_name):
    es2_doc_counts = {}
    for doc_type in DOC_TYPES:
        doc_count = count_docs_for_doctype(es_client2, doc_type, index_name)
        if doc_count > 0:
            es2_doc_counts[doc_type] = doc_count
    print('{} doc type(s) found for {}'.format(len(es2_doc_counts), index_name))
    return es2_doc_counts


def copy_index(index_name_from, index_name_to):
    start = datetime.now()

    es2_doc_counts = list_docs_for_each_doctype(index_name_from)

    total = 0

    for doc_type, dcount in iteritems(es2_doc_counts):
        offset = 0
        page_size = 250

        print('Preparing to index {} {} document(s) from ES2 {} index'.format(dcount, doc_type, index_name_from))

        total += dcount

        scroll_id = None

        while offset <= dcount:
            scroll_id, docs = fetch_documents_from_es2(doc_type, from_=offset, page_size=page_size, index_name=index_name_from, scroll_id=scroll_id)

            for doc in docs:
                doc['_source']['document_type'] = doc_type

            print('Indexing documents {} to {} into ES5'.format(offset, offset+page_size))
            bulk_index_documents_to_es5(index_name_to, docs)

            offset += page_size

    print('Finished {} documents in {} seconds'.format(total, datetime.now() - start))


def main():
    """
    Bulk copy documents as-is from one index to the new index created by rummager.

    Assumes the target index has already been created with an appropriate mapping.
    # TODO: create target index if it doesn't exist

    Aliases/index names are treated the same.
    :return:
    """
    indices = []
    try:
        indices = [(index, es_client5.indices.get_alias("{}-*".format(index)).keys()[0]) for index in INDICES]
    except KeyError as err:
        print("Index does not exist in ES5 target host: {}".format(index))
    for index_name_from, index_name_to in indices:
        copy_index(index_name_from, index_name_to)


if __name__ == '__main__':
    main()
