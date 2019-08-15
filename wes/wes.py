from elasticsearch import Elasticsearch

from error import *
from log import *

class Wes:
    def __init__(self):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = Elasticsearch()  # local instance

    def ind_create(self, index) -> bool:
        try:
            rc = self.es.indices.create(index)
            LOG_OK(rc)
            # [DOCKER] {"type": "server", "timestamp": "2019-08-15T07:48:18,679+0000", "level": "INFO", "component": "o.e.c.m.MetaDataCreateIndexService", "cluster.name": "docker-cluster", "node.name": "9ed342ffe07d", "cluster.uuid": "481GmZMJSHu9Y6E-_7gqxQ", "node.id": "RoqxLVUpRy24ce_D3dRnKA",  "message": "[first_index] creating index, cause [api], templates [], shards [1]/[1], mappings []"  }
            # [PYTHON] {'acknowledged': True, 'shards_acknowledged': True, 'index': 'first_index'}
            return True

        except Exception as e:  # TODO is this ok???
            LOG_ERR(err_2_string(e))
            return False

    def ind_exist(self, index, params=None) -> bool:
        rc = self.es.indices.exists(index)
        LOG(index + " " + str(rc))
        return rc;

    def ind_delete(self, indice_name) -> bool:
        try:
            rc = self.es.indices.delete(index=indice_name)
            LOG(indice_name + " " + str(rc))
            return True;
        except Exception as e:  # TODO is this ok???
            LOG_ERR(err_2_string(e))
            return False



############################################################################################
# TEST
############################################################################################

class TestWes(unittest.TestCase):
    def test_basic(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"
        wes.ind_delete(ind_str)
        wes.ind_create(ind_str)
        wes.ind_create(ind_str)
        wes.ind_exist(ind_str)
        wes.ind_delete(ind_str)
        wes.ind_exist(ind_str)
        wes.ind_delete(ind_str)

if __name__ == '__main__':
    # unittest.main() run all test (imported too) :(
    unittest.main(TestWes())