from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError

from error import *
from log import *

class Wes:
    def __init__(self):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = Elasticsearch()  # local instance

    def create_indice(self, indice_name):
        try:
            # rc = es.indices.create(index=indice_name, ignore=400)
            rc = self.es.indices.create(index=indice_name)
            LOG_OK(rc)
            # [DOCKER] {"type": "server", "timestamp": "2019-08-15T07:48:18,679+0000", "level": "INFO", "component": "o.e.c.m.MetaDataCreateIndexService", "cluster.name": "docker-cluster", "node.name": "9ed342ffe07d", "cluster.uuid": "481GmZMJSHu9Y6E-_7gqxQ", "node.id": "RoqxLVUpRy24ce_D3dRnKA",  "message": "[first_index] creating index, cause [api], templates [], shards [1]/[1], mappings []"  }
            # [PYTHON] {'acknowledged': True, 'shards_acknowledged': True, 'index': 'first_index'}
        except RequestError as e:
            LOG_ERR(e.error)

if __name__ == '__main__':
    wes = Wes()
    wes.create_indice("first_index")