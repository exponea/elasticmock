from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params

from error import *
from log import *

class WesDefs():
    # operation
    OP_IND_CREATE   = "OP_IND_CREATE "
    OP_IND_FLUSH    = "OP_IND_FLUSH  "
    OP_IND_REFRESH  = "OP_IND_REFRESH"
    OP_IND_EXIST    = "OP_IND_EXIST  "
    OP_IND_DELETE   = "OP_IND_DELETE "
    OP_DOC_ADD_UP   = "OP_DOC_ADDUP  "
    OP_DOC_GET      = "OP_DOC_GET    "
    OP_DOC_SEARCH   = "OP_DOC_SEARCH "

    # RC - 3 codes
    # - maybe useful later (low level could detect problem in data)
    # - e.g. no exception but status is wrong TODO discuss wit petee
    RC_EXCE    = "RC_EXCE"
    RC_NOK     = "RC_NOK"
    RC_OK      = "RC_OK"

class Wes(WesDefs):

    def __init__(self):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = Elasticsearch()  # local instance

    @query_params(
        "master_timeout",
        "timeout" "request_timeout",
        "wait_for_active_shards",
        "include_type_name",)
    def ind_create(self, index, body=None, params=None):
        try:
            rc = self.es.indices.create(index, body=body, params=params)
            WES_DB_OK(Wes.OP_IND_CREATE, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_IND_CREATE, e)
            return (Wes.RC_EXCE, e)

    def ind_create_result(self, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_IND_CREATE, f"KEY[{rc['index']}] ack[{rc['acknowledged']} - {rc['shards_acknowledged']}]")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_IND_CREATE, rc)
        else:
            raise ValueError(f"{Wes.OP_IND_CREATE} unknown status - {status}")

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
            WES_DB_OK(Wes.OP_IND_EXIST, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_IND_EXIST, e)
            return (Wes.RC_EXCE, e)

    def ind_exist_result(self, index, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_IND_EXIST, f"KEY[{index}] {rc}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_IND_EXIST, rc)
        else:
            raise ValueError(f"{Wes.OP_IND_EXIST} unknown status - {status}")

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
            WES_DB_OK(Wes.OP_IND_DELETE, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_IND_DELETE, e)
            return (Wes.RC_EXCE, e)

    def ind_delete_result(self, index, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_IND_DELETE, f"KEY[{index}] {rc['acknowledged']}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_IND_DELETE, rc)
        else:
            raise ValueError(f"{Wes.OP_IND_DELETE} unknown status - {status}")

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "force",
        "ignore_unavailable",
        "wait_if_ongoing",
    )
    def ind_flush(self, index=None, params=None):
        try:
            rc = self.es.indices.flush(index=index, params=params)
            WES_DB_OK(Wes.OP_IND_FLUSH, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_IND_FLUSH, e)
            return (Wes.RC_EXCE, e)

    def ind_flush_result(self, index, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_IND_FLUSH, f"KEY[{index}] {str(rc)}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_IND_FLUSH, rc)
        else:
            raise ValueError(f"{Wes.OP_IND_FLUSH} unknown status - {status}")

    @query_params("allow_no_indices",
                  "expand_wildcards",
                  "ignore_unavailable")
    def ind_refresh(self, index=None, params=None):
        try:
            rc = self.es.indices.refresh(index=index, params=params)
            WES_DB_OK(Wes.OP_IND_REFRESH, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_IND_REFRESH, e)
            return (Wes.RC_EXCE, e)

    def ind_refresh_result(self, index, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_IND_REFRESH, f"KEY[{index}] {str(rc)}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_IND_REFRESH, rc)
        else:
            raise ValueError(f"{Wes.OP_IND_REFRESH} unknown status - {status}")

    @query_params(
        "if_seq_no",
        "if_primary_term",
        "op_type",
        "parent",
        "pipeline",
        "refresh",
        "routing",
        "timeout",
        "timestamp",
        "ttl",
        "version",
        "version_type",
        "wait_for_active_shards",
    )
    def doc_addup(self, index, body, doc_type="_doc", id=None, params=None):
        # TODO petee 'id' is important for get - shouldn't be mandatory???
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        try:
            rc = self.es.index(index, body, doc_type=doc_type, id=id, params=params)
            WES_DB_OK(Wes.OP_DOC_ADD_UP, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_DOC_ADD_UP, e)
            return (Wes.RC_EXCE, e)

    def doc_addup_result(self, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_DOC_ADD_UP, f"KEY[{rc['_index']} <-> {rc['_type']} <-> {rc['_id']}] {rc['result']} {rc['_shards']}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_DOC_ADD_UP, rc)
        else:
            raise ValueError(f"{Wes.OP_DOC_ADD_UP} unknown status - {status}")

    @query_params(
        "_source",
        "_source_exclude",
        "_source_excludes",
        "_source_include",
        "_source_includes",
        "parent",
        "preference",
        "realtime",
        "refresh",
        "routing",
        "stored_fields",
        "version",
        "version_type",
    )
    def doc_get(self, index, id, doc_type="_doc", params=None):
        try:
            rc = self.es.get(index, id, doc_type=doc_type, params=params)
            WES_DB_OK(Wes.OP_DOC_GET, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_DOC_GET, e)
            return (Wes.RC_EXCE, e)

    def doc_get_result(self, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_DOC_GET, f"KEY[{rc['_index']} <-> {rc['_type']} <-> {rc['_id']}] {rc['_source']}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_DOC_GET, rc)
        else:
            raise ValueError(f"{Wes.OP_DOC_GET} unknown status - {status}")

    @query_params(
        "_source",
        "_source_exclude",
        "_source_excludes",
        "_source_include",
        "_source_includes",
        "allow_no_indices",
        "allow_partial_search_results",
        "analyze_wildcard",
        "analyzer",
        "batched_reduce_size",
        "ccs_minimize_roundtrips",
        "default_operator",
        "df",
        "docvalue_fields",
        "expand_wildcards",
        "explain",
        "from_",
        "ignore_throttled",
        "ignore_unavailable",
        "lenient",
        "max_concurrent_shard_requests",
        "pre_filter_shard_size",
        "preference",
        "q",
        "rest_total_hits_as_int",
        "request_cache",
        "routing",
        "scroll",
        "search_type",
        "seq_no_primary_term",
        "size",
        "sort",
        "stats",
        "stored_fields",
        "suggest_field",
        "suggest_mode",
        "suggest_size",
        "suggest_text",
        "terminate_after",
        "timeout",
        "track_scores",
        "track_total_hits",
        "typed_keys",
        "version",
    )
    def doc_search(self, index=None, body=None, params=None):
        try:
            rc = self.es.search(index=index, body=body, params=params)
            WES_DB_OK(Wes.OP_DOC_SEARCH, rc)
            return (Wes.RC_OK, rc)
        except Exception as e:
            WES_DB_ERR(Wes.OP_DOC_SEARCH, e)
            return (Wes.RC_EXCE, e)

    def doc_search_result(self, rc: tuple):
        status, rc = rc
        if status == Wes.RC_OK:
            WES_RC_OK(Wes.OP_DOC_SEARCH, f"NB REC[{rc['hits']['total']['value']} <-> {rc['hits']['hits']}")
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            WES_RC_NOK(Wes.OP_DOC_SEARCH, rc)
        else:
            raise ValueError(f"{Wes.OP_DOC_SEARCH} unknown status - {status}")


############################################################################################
# TEST
############################################################################################

class TestWes(unittest.TestCase):
    # def test_basic_ind(self):
    #     wes = Wes()
    #     ind_str = "first_pooooooooooooooo"
    #     wes.ind_delete(ind_str)
    #     wes.ind_create(ind_str)
    #     wes.ind_create(ind_str)
    #     wes.ind_exist(ind_str)
    #     wes.ind_delete(ind_str)
    #     wes.ind_exist(ind_str)
    #     wes.ind_delete(ind_str)

    def test_basic_doc(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))


        doc1 = {"city": "Bratislava1", "country": "slovakia"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2"}
        doc3 = {"city": "Bratislava3", "country": "slovakia"}

        # TODO petee explain success priority???
        # 1. exception
        # 2. IMPO_AU_1 vs. IMPO_AU_2 consider 'RC - 3 codes'
        #                                                                           MSE_NOTES:      IMPO_AU_1          IntperOper    IntChangedByUpd                            IMPO_AU_2
        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))  # MSE_NOTES: 'result': 'created' '_seq_no': 0  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type="any", id=2))  # MSE_NOTES: 'result': 'created' '_seq_no': 1  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))  # MSE_NOTES: 'result': 'created' '_seq_no': 2  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))  # MSE_NOTES: 'result': 'updated' '_seq_no': 3  '_version': 2,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},

        #                                                              MSE_NOTES:  IMPO_GET_1 ok/exc                                    IMPO_GET_2
        wes.doc_get_result(wes.doc_get(ind_str, 1, doc_type="any"))  # MSE_NOTES: 'found': True,                 '_seq_no': 0,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia1'}
        wes.doc_get_result(wes.doc_get(ind_str, 2, doc_type="any"))  # MSE_NOTES: 'found': True,                 '_seq_no': 1,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        wes.doc_get_result(wes.doc_get(ind_str, 3, doc_type="any"))  # MSE_NOTES: 'found': True,                 '_seq_no': 2,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        wes.doc_get_result(wes.doc_get(ind_str, 9, doc_type="any"))  # MSE_NOTES:  WesNotFoundError !!!

        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))  # MSE_NOTES: 'result': 'updated' '_seq_no': 4

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": {"match_all": {}}}))




if __name__ == '__main__':
    # unittest.main() run all test (imported too) :(
    unittest.main(TestWes())