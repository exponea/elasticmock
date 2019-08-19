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
    OP_IND_PUT_MAP  = "OP_IND_PUT_MAP"
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
                        # special cases  - OP_DOC_SEARCH  'type': 'parsing_exception'
                        # special cases  - OP_IND_CREATE  'type': 'invalid_index_name_exception',
                        #                                 'reason': 'Invalid index name [first_IND1], must be lowercase'
                        elif isinstance(e, RequestError):
                            LOG_FNC(f"{oper} KEY[???] - {e.status_code} - {e.info['error']['type']} - {e.info['error']['reason']}")
                        # generic
                        else:
                            if e.status_code == 405:
                                LOG_FNC(f"{oper} KEY[???] - {e.status_code} - {e.info['error']}")
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

    def ind_get_mapping_result(self, rc: tuple, is_per_line: bool = True):
        def fmt_fnc_ok_inline(rc_data) -> str:
            return  f"MAPPING: <-> {str(rc_data)}"

        def fmt_fnc_ok_per_line(rc_data) -> str:
            rec = ''
            # print(type(rc_data))
            # print(rc_data)
            for rc_index in rc_data.keys():
                rc_index_str = f"IND[{rc_index}]"
                rec = rec + '\n' + rc_index_str

                # mapping not exist
                props = rc_data[rc_index].get('mappings', None)
                if len(props) == 0:
                    rec = rec + " : Missing mappings" + '\n'
                    continue
                else:
                    propsCheck = props.get('properties', None)
                    if propsCheck is None:
                        # doc_type is nested
                        nested_doc_type = list(props.keys())[0]
                        rc_doc_type_str = f" : DOC[{nested_doc_type}]"
                        rec = rec + rc_doc_type_str

                        props = list(props.values())[0] # probably one
                    rec = rec + '\n'
                    props = props.get('properties', None)
                    for prop in props:
                        rec = rec + str(prop) + ": " + str(props[prop]) + '\n'

            return f"MAPPING: {rec}"

        fmt_fnc_ok = fmt_fnc_ok_per_line if is_per_line else fmt_fnc_ok_inline

        self._operation_result(Wes.OP_IND_GET_MAP, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "master_timeout",
        "timeout",
        "request_timeout",
        "include_type_name",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_PUT_MAP)
    def ind_put_mapping(self, body, doc_type=None, index=None, params=None):
        # TODO petee 'index' is important - shouldn't be mandatory???
        # TODO petee 'doc_type' is important - shouldn't be mandatory???
        return self.es.indices.put_mapping(body, index=index, doc_type=doc_type, params=params)

    def ind_put_mapping_result(self, rc: tuple, is_per_line: bool = True):
        def fmt_fnc_ok(rc_data) -> str:
            return f"MAPPING: <-> {str(rc_data)}"
        self._operation_result(Wes.OP_IND_PUT_MAP, rc, fmt_fnc_ok)


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
            return f"NB REC[{rc_data['hits']['total']['value']}] <-> HITS[{rc_data['hits'].get('hits', 'hits empty')}] <-> AGGS[{rc_data.get('aggregations', 'aggs empty')}]"

        def fmt_fnc_ok_per_line(rc_data) -> str:
            rec_list = rc_data['hits']['hits']
            rec = ''
            rec = rec + '\nHITS:\n' + '\n'.join([str(item) for item in rec_list])
            rec = rec + '\nAGGS:\n'
            aggs = rc_data.get('aggregations', None)
            if aggs:
                for a in aggs.keys():
                    rec = rec + a + '\n'
                    for a_items in aggs[a]['buckets']:
                        rec = rec + str(a_items) + '\n'

            return f"NB REC[{rc_data['hits']['total']['value']}] : {rec}"

        fmt_fnc_ok = fmt_fnc_ok_per_line if is_per_line else fmt_fnc_ok_inline

        self._operation_result(Wes.OP_DOC_SEARCH, rc, fmt_fnc_ok)
