from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params

import sys
from error import *
from log import *

class Wes:

    IND_CREATE = "IND_CREATE"
    IND_EXIST  = "IND_EXIST "
    IND_DELETE = "IND_DELETE"

    def __init__(self):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = Elasticsearch()  # local instance

    @query_params(
        "master_timeout",
        "timeout" "request_timeout",
        "wait_for_active_shards",
        "include_type_name",)
    def ind_create(self, index, body=None, params=None) -> bool:
        try:
            rc = self.es.indices.create(index, body=body, params=params)
            WES_SUCCESS(Wes.IND_CREATE, rc)
            return (True, rc)
        except Exception as e:  # TODO petee is this ok or be more specific???
            WES_EXCEPTION(Wes.IND_CREATE, e)
            return (False, e)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "include_defaults",
        "local",
    )
    def ind_exist(self, index, params=None):
        try:
            rc = self.es.indices.exists(index, params=params)
            WES_SUCCESS(Wes.IND_EXIST, rc)
            return (True, rc)
        except Exception as e:  # TODO petee is this ok or be more specific???
            WES_EXCEPTION(Wes.IND_EXIST, e)
            return (False, e)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "timeout",
        "master_timeout",
        "request_timeout",
    )
    def ind_delete(self, index, params=None):
        try:
            rc = self.es.indices.delete(index, params=params)
            WES_SUCCESS(Wes.IND_DELETE, rc)
            return (True, rc)
        except Exception as e:  # TODO petee is this ok or be more specific???
            WES_EXCEPTION(Wes.IND_DELETE, e)
            return (False, e)


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