#! /usr/bin/env python

import json
from os.path import join, dirname
import os

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk


def main():
    with open('/ta-data/ta_citations.json') as citations_file:
        citations = json.load(citations_file)
    repo = ElasticSearchRepository(
        os.getenv('ELASTIC_HOST'),
        os.getenv('ELASTIC_INDEX')
    )
    repo.insert_citations(citations)


class ElasticSearchRepository(object):
    def __init__(self, host=None, index=None):
        self._es = Elasticsearch(hosts=[f'{host}:9200'])
        self._index = index
        self._create_index()

    def _create_index(self):
        try:
            self._es.indices.delete(self._index)
        except NotFoundError:
            pass
        with open(join(dirname(__file__), 'index_settings.json')) as settings_file:
            self._es.indices.create(
                index=self._index, body=json.load(settings_file))
        self._es.indices.put_settings(body={'max_result_window': 70000})

    def insert_citations(self, citations):
        citations = map(lambda citation: {
            '_index': self._index,
            '_type': 'citation',
            '_id': citation['id'],
            **citation
        }, citations)
        bulk(self._es, citations, chunk_size=70000, request_timeout=200)


if __name__ == '__main__':
    main()
