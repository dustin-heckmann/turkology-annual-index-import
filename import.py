#! /usr/bin/env python

import json
import os
from os.path import join, dirname

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError


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
        if f'{index}_blue' in self._es.indices.get('*'):
            old, new = 'blue', 'green'
        else:
            old, new = 'green', 'blue'
        self._index = index
        self._index_new = f'{index}_{new}'
        self._index_old = f'{index}_{old}'
        self._create_index()

    def _create_index(self):
        with open(join(dirname(__file__), 'index_settings.json')) as settings_file:
            self._es.indices.create(
                index=self._index_new, body=json.load(settings_file))
        self._es.indices.put_settings(body={'max_result_window': 70000})

    def insert_citations(self, citations):
        citations = map(lambda citation: {
            '_index': self._index_new,
            '_type': 'citation',
            '_id': citation['id'],
            **citation
        }, citations)
        bulk(self._es, citations, chunk_size=70000, request_timeout=200)
        self._set_alias_to_new_index()
        self._remove_old_index()

    def _set_alias_to_new_index(self):
        self._es.indices.update_aliases({
            "actions": [
                {"add": {"index": self._index_new, "alias": self._index}},
            ]
        })

    def _remove_old_index(self):
        try:
            self._es.indices.update_aliases({
                "actions": [
                    {"remove": {"index": self._index_old, "alias": self._index}},
                ]
            })
            self._es.indices.delete(self._index_old)
        except NotFoundError:
            pass


if __name__ == '__main__':
    main()
