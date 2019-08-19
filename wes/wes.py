from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params

from elasticsearch.exceptions import ImproperlyConfigured
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.exceptions import SerializationError
from elasticsearch.exceptions import TransportError
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConflictError
from elasticsearch.exceptions import RequestError
from elasticsearch.exceptions import ConnectionError
from elasticsearch.exceptions import SSLError
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.exceptions import AuthenticationException
from elasticsearch.exceptions import AuthorizationException

from log import *

class WesDefs():
    # operation
    OP_IND_CREATE   = "OP_IND_CREATE "
    OP_IND_FLUSH    = "OP_IND_FLUSH  "
    OP_IND_REFRESH  = "OP_IND_REFRESH"
    OP_IND_EXIST    = "OP_IND_EXIST  "
    OP_IND_DELETE   = "OP_IND_DELETE "
    OP_IND_GET_MAP  = "OP_IND_GET_MAP"
    OP_DOC_ADD_UP   = "OP_DOC_ADDUP  "
    OP_DOC_GET      = "OP_DOC_GET    "
    OP_DOC_SEARCH   = "OP_DOC_SEARCH "

    # RC - 3 codes
    # - maybe useful later (low level could detect problem in data)
    # - e.g. no exception but status is wrong TODO discuss wit petee
    RC_EXCE    = "RC_EXCE"
    RC_NOK     = "RC_NOK"
    RC_OK      = "RC_OK"

    def _WES_INT_ERR(self, oper, e, is_l1, LOG_FNC):
        # ImproperlyConfigured(Exception)
        # ElasticsearchException(Exception)
        # 	- SerializationError(ElasticsearchException)
        #	- TransportError(ElasticsearchException)
        # 		= ConnectionError(TransportError)       'N/A' status code
        #			=> SSLError(ConnectionError)
        #			=> ConnectionTimeout(ConnectionError)
        # 		= NotFoundError(TransportError)          404  status code
        # 		= ConflictError(TransportError)          409  status code
        # 		= RequestError(TransportError)           400  status code
        # 		= AuthenticationException(TransportError)401  status code
        # 		= AuthorizationException(TransportError) 403  status code
        if isinstance(e, ImproperlyConfigured):
            LOG_FNC(f"{oper} {str(e)}")
        elif isinstance(e, ElasticsearchException):
            if isinstance(e, SerializationError):
                LOG_FNC(f"{oper} {str(e)}")
            elif isinstance(e, TransportError):
                if isinstance(e, ConnectionError):
                    if isinstance(e, SSLError) or isinstance(e, ConnectionTimeout):
                        LOG_FNC(f"{oper} ConnectionError - {e.info}")
                    else:
                        LOG_FNC(f"{oper} ConnectionError (generic) - {e.info}")
                else:
                    # 		= NotFoundError(TransportError)          404  status code
                    # 		= ConflictError(TransportError)          409  status code
                    # 		= RequestError(TransportError)           400  status code
                    # 		= AuthenticationException(TransportError)401  status code
                    # 		= AuthorizationException(TransportError) 403  status code

                    # print(type(e.error)) => <class 'str'>
                    # print(type(e.info))  => <class 'dict'>
                    if is_l1:
                        LOG_FNC(f"{oper} {e.status_code} - {e.info} ")
                    else:
                        # special cases
                        if isinstance(e, NotFoundError) and oper == WesDefs.OP_IND_DELETE:
                            LOG_FNC(f"{oper} KEY[{e.info['error']['index']}] - {e.status_code} - {e.info['error']['type']}")
                        # special cases  - 'type': 'parsing_exception'
                        elif isinstance(e, RequestError) and oper == WesDefs.OP_DOC_SEARCH:
                            LOG_FNC(f"{oper} KEY[???] - {e.status_code} - {e.info['error']['type']} - {e.info['error']['reason']}")
                        # generic
                        else:
                            LOG_FNC(f"{oper} KEY[{e.info['_index']} <-> {e.info['_type']} <-> {e.info['_id']}] {str(e)}")
            else:
                LOG_ERR(f"{oper} Unknow L2 exception ... {str(e)}")
                raise (e)
        else:
            LOG_ERR(f"{oper} Unknow L1 exception ... {str(e)}")
            raise (e)

    def WES_RC_NOK(self, oper, e):
        self._WES_INT_ERR(oper, e, False, LOG_ERR)  # this is L2 - use error

    def WES_DB_ERR(self, oper, e):
        self._WES_INT_ERR(oper, e, True, LOG_WARN)  # this is L1 - only warn

    def WES_RC_OK(self, oper, rc):
        LOG_OK(f"{oper} {str(rc)}")

    def WES_DB_OK(self, oper, rc):
        LOG(f"{oper} {str(rc)}")

    def _operation_result(self, oper, rc: tuple, fmt_fnc_ok):
        status, rc_data = rc
        if status == Wes.RC_OK:
            self.WES_RC_OK(oper, fmt_fnc_ok(rc_data))
        elif status == Wes.RC_NOK:
            assert("not implemented") # TODO RC - 3 codes
        elif status == Wes.RC_EXCE:
            self.WES_RC_NOK(oper, rc_data)
        else:
            raise ValueError(f"{oper} unknown status - {status}")

    class Decor:
        @staticmethod
        def operation_exec(oper):
            def wrapper_mk(fnc):
                def wrapper(self, *args, **kwargs):
                    try:
                        rc = fnc(self, *args, **kwargs)
                        self.WES_DB_OK(oper, rc)
                        return (Wes.RC_OK, rc)
                    except Exception as e:
                        self.WES_DB_ERR(oper, e)
                        return (Wes.RC_EXCE, e)

                return wrapper
            return wrapper_mk

class Wes(WesDefs):

    def __init__(self):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = Elasticsearch()  # local instance

    @query_params(
        "master_timeout",
        "timeout" "request_timeout",
        "wait_for_active_shards",
        "include_type_name",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_CREATE)
    def ind_create(self, index, body=None, params=None):
        return self.es.indices.create(index, body=body, params=params)

    def ind_create_result(self, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{rc_data['index']}] ack[{rc_data['acknowledged']} - {rc_data['shards_acknowledged']}]"
        self._operation_result(Wes.OP_IND_CREATE, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "include_defaults",
        "local",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_EXIST)
    def ind_exist(self, index, params=None):
        return self.es.indices.exists(index, params=params)

    def ind_exist_result(self, index, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{index}] {rc_data}"
        self._operation_result(Wes.OP_IND_EXIST, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "timeout",
        "master_timeout",
        "request_timeout",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_DELETE)
    def ind_delete(self, index, params=None):
        return self.es.indices.delete(index, params=params)

    def ind_delete_result(self, index, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{index}] {rc_data['acknowledged']}"
        self._operation_result(Wes.OP_IND_DELETE, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "force",
        "ignore_unavailable",
        "wait_if_ongoing",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_FLUSH)
    def ind_flush(self, index=None, params=None):
        return self.es.indices.flush(index=index, params=params)

    def ind_flush_result(self, index, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{index}] {str(rc_data)}"
        self._operation_result(Wes.OP_IND_FLUSH, rc, fmt_fnc_ok)

    @query_params("allow_no_indices",
                  "expand_wildcards",
                  "ignore_unavailable")
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_REFRESH)
    def ind_refresh(self, index=None, params=None):
        return self.es.indices.refresh(index=index, params=params)

    def ind_refresh_result(self, index, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{index}] {str(rc_data)}"
        self._operation_result(Wes.OP_IND_REFRESH, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "local",
        "include_type_name",
        "master_timeout",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_GET_MAP)
    def ind_get_mapping(self, index=None, doc_type=None, params=None):
        return self.es.indices.get_mapping(index=index, doc_type=doc_type, params=params)

    def ind_get_mapping_result(self, index, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return str(rc_data)   # f"KEY[{index}] {str(rc_data)}"
        self._operation_result(Wes.OP_IND_GET_MAP, rc, fmt_fnc_ok)

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
        "wait_for_active_shards",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_ADD_UP)
    def doc_addup(self, index, body, doc_type="_doc", id=None, params=None):
        # TODO petee 'id' is important for get - shouldn't be mandatory???
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.index(index, body, doc_type=doc_type, id=id, params=params)

    def doc_addup_result(self, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{rc_data['_index']} <-> {rc_data['_type']} <-> {rc_data['_id']}] {rc_data['result']} {rc_data['_shards']}"
        self._operation_result(Wes.OP_DOC_ADD_UP, rc, fmt_fnc_ok)

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
        "version_type",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_GET)
    def doc_get(self, index, id, doc_type="_doc", params=None):
        return self.es.get(index, id, doc_type=doc_type, params=params)

    def doc_get_result(self, rc: tuple):
        def fmt_fnc_ok(rc_data) -> str:
            return f"KEY[{rc_data['_index']} <-> {rc_data['_type']} <-> {rc_data['_id']}] {rc_data['_source']}"
        self._operation_result(Wes.OP_DOC_GET, rc, fmt_fnc_ok)

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
        "version",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_SEARCH)
    def doc_search(self, index=None, body=None, params=None):
        return self.es.search(index=index, body=body, params=params)

    def doc_search_result(self, rc: tuple, is_per_line: bool = True):
        def fmt_fnc_ok_inline(rc_data) -> str:
            return f"NB REC[{rc_data['hits']['total']['value']}] <-> {rc_data['hits']['hits']}"

        def fmt_fnc_ok_per_line(rc_data) -> str:
            rec_list = rc_data['hits']['hits']
            rec = '\n' + '\n'.join([str(item) for item in rec_list])
            return f"NB REC[{rc_data['hits']['total']['value']}] <-> {rec}"

        fmt_fnc_ok = fmt_fnc_ok_per_line if is_per_line else fmt_fnc_ok_inline

        self._operation_result(Wes.OP_DOC_SEARCH, rc, fmt_fnc_ok)


############################################################################################
# TEST
############################################################################################

class TestWes(unittest.TestCase):
    def basic_ind(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"
        wes.ind_delete(ind_str)
        wes.ind_create(ind_str)
        wes.ind_create(ind_str)
        wes.ind_exist(ind_str)
        wes.ind_delete(ind_str)
        wes.ind_exist(ind_str)
        wes.ind_delete(ind_str)

    def basic_doc_and_query(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))


        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2", "sentence": "The SLOVAKA is a country"}
        doc3 = {"city": "Bratislava3", "country": "SLOVAKIA",  "sentence": "The slovakia is a country"}
        doc4 = {"city": "Bratislava4", "country": "SLOVAKIA4", "sentence": "The small country is slovakia"}
        doc5 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}

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
        wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type="any", id=4))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc5, doc_type="any", id=5))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        wes.doc_search_result(wes.doc_search())                                                             # MSE_NOTES: #1 search ALL in DB
        wes.doc_search_result(wes.doc_search(index=ind_str))                                                # MSE_NOTES: #2 search ALL in specific INDICE
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": {"match_all": {}}}))             # MSE_NOTES: #3 equivalent to #2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #4 hint list FILTER
                 "query": {"match_all": {}}                                                                 #               - 'from' - specify START element in 'hintLIST' witch MATCH query
               }                                                                                            #               - 'size' - specify RANGE/MAXIMUM from 'hintLIST' <'from', 'from'+'size')
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     #               E.G. "size": 0 == returns just COUNT


        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(inSensitive)
                #"query": {"match": {}}                                                                     #               - EXCEPTION:  400 - parsing_exception - No text specified for text query
                "query": {"match": {"country": "slovakia"}}                                                 #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
                "query": {"match": {"sentence": "slovakia"}}                                                #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 4


        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #6 QUERY(matchphrase) MATCH(subSentence+wholeWord+phraseOrder) CASE(in-sensitive)
                "query": {"match_phrase": {"country": "slovakia"}}                                          #              - ORDER IGNORE spaces and punctation !!!
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #6 QUERY(matchphrase) MATCH(subSentence+wholeWord+phraseOrder) CASE(in-sensitive)
                "query": {"match_phrase": {"sentence": "slovakia is"}}                                      #              - ORDER IGNORE spaces and punctation !!!
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2


        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #7 QUERY(term) MATCH(exact) CASE(in-sensitive)
                "query": {"term": {"country": "slovakia"}}                                                  #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #7 QUERY(term) MATCH(exact) CASE(in-sensitive)
                "query": {"term": {"sentence": "slovakia is"}}                                              #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2

        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #8 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
                "query": {"term": {"country": "slovakia"}}                                                  #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
               }                                                                                            #   -
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     #   -     == ORRESULTS: 2


    def test_complex_queries(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2", "sentence": "The SLOVAKA is a country"}
        doc3 = {"city": "Bratislava3", "country": "SLOVAKIA",  "sentence": "The slovakia is a country"}
        doc4 = {"city": "Bratislava4", "country": "SLOVAKIA4", "sentence": "The small country is slovakia"}
        doc5 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}

        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type="any", id=2))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type="any", id=4))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc5, doc_type="any", id=5))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        LOG_NOTI_L("--------------------------------------------------------------------------------------")

        q1 = {"match": {"sentence": "slovakia"}}                                                # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q1}))                #  RESULTS: 4
        q2 = {"match": {"sentence": "small"}}                                                   # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q2}))                #  RESULTS: 2


        body = {"query": {"bool": { "must_not": q2, "should": q1 }}}                            # MSE_NOTES: #8 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
                                                                                                #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                         #  RESULTS: 2




if __name__ == '__main__':
    # unittest.main() run all test (imported too) :(
    unittest.main(TestWes())