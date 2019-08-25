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

# for 'bulk'and 'scan' API
from elasticsearch import helpers
from elasticsearch.helpers.errors import BulkIndexError
from elasticsearch.helpers.errors import ScanError

from collections import namedtuple

from log import Log

__all__ = ["Wes", "ExecCode"]

ExecCode = namedtuple('ExecCode', 'status data fnc_params')

class WesDefs():
    # Elasticsearch version
    ES_VERSION_7_3_0 = '7.3.0'
    ES_VERSION_5_6_5 = '5.6.5'
    ES_VERSION_RUNNING = ES_VERSION_5_6_5

    def es_version_mismatch(self):
        raise ValueError(f"ES_VERSION_RUNNING is unknown - {self.ES_VERSION_RUNNING}")

    # indice operations
    OP_IND_CREATE   = "OP_IND_CREATE  : "
    OP_IND_FLUSH    = "OP_IND_FLUSH   : "
    OP_IND_REFRESH  = "OP_IND_REFRESH : "
    OP_IND_EXIST    = "OP_IND_EXIST   : "
    OP_IND_DELETE   = "OP_IND_DELETE  : "
    OP_IND_GET_MAP  = "OP_IND_GET_MAP : "
    OP_IND_PUT_MAP  = "OP_IND_PUT_MAP : "
    OP_IND_GET_TMP  = "OP_IND_GET_TMP : "
    OP_IND_PUT_TMP  = "OP_IND_PUT_TMP : "
    # document operations
    OP_DOC_ADD_UP   = "OP_DOC_ADD_UP  : "
    OP_DOC_UPDATE   = "OP_DOC_UPDATE  : "
    OP_DOC_GET      = "OP_DOC_GET     : "
    OP_DOC_EXIST    = "OP_DOC_EXIST   : "
    # batch operations
    OP_DOC_SEARCH   = "OP_DOC_SEARCH  : "
    OP_DOC_BULK     = "OP_DOC_BULK    : "
    OP_DOC_SCAN     = "OP_DOC_SCAN    : "
    OP_DOC_COUNT    = "OP_DOC_COUNT   : "

    # RC - 3 codes
    # - maybe useful later (low level could detect problem in data)
    # - e.g. no exception but status is wrong TODO discuss wit petee
    RC_EXCE    = "RC_EXCE"
    RC_NOK     = "RC_NOK"
    RC_OK      = "RC_OK"

    def _dump_exeption(self, oper, rc: ExecCode, is_l1, log_fnc, key_str='???'):
        # ImproperlyConfigured(Exception)
        # ElasticsearchException(Exception)
        # 	- SerializationError(ElasticsearchException)
        #   - BulkIndexError(ElasticsearchException)    NOT IN DOC
        #   - ScanError(ElasticsearchException)         NOT IN DOC
        #	- TransportError(ElasticsearchException)
        # 		= ConnectionError(TransportError)       'N/A' status code
        #			=> SSLError(ConnectionError)
        #			=> ConnectionTimeout(ConnectionError)
        # 		= NotFoundError(TransportError)          404  status code
        # 		= ConflictError(TransportError)          409  status code
        # 		= RequestError(TransportError)           400  status code
        # 		= AuthenticationException(TransportError)401  status code
        # 		= AuthorizationException(TransportError) 403  status code
        e = rc.data
        if isinstance(e, ImproperlyConfigured):
            log_fnc(f"{oper} {str(e)}")
        elif isinstance(e, ElasticsearchException):
            if isinstance(e, SerializationError):
                log_fnc(f"{oper} {str(e)}")
            elif isinstance(e, BulkIndexError):
                # MSE_NOTE:
                # exception behavior should be SUPPRESSED - some operations in batch could PASS
                # L2 result should check if error occurred (e.g. DOC_BULK operation)
                log_fnc(f"{oper} BulkIndexError - NB ERRORS[{len(e.errors)}] {str(e)}")
                raise e
            elif isinstance(e, ScanError):
                log_fnc(f"{oper} ScanError - {e.scroll_id} {str(e)}")
            elif isinstance(e, TransportError):
                if isinstance(e, ConnectionError):
                    if isinstance(e, SSLError) or isinstance(e, ConnectionTimeout):
                        log_fnc(f"{oper} ConnectionError - {e.info}")
                    else:
                        log_fnc(f"{oper} ConnectionError (generic) - {e.info}")
                else:
                    # 		= NotFoundError(TransportError)          404  status code
                    # 		= ConflictError(TransportError)          409  status code
                    # 		= RequestError(TransportError)           400  status code
                    # 		= AuthenticationException(TransportError)401  status code
                    # 		= AuthorizationException(TransportError) 403  status code

                    # print(type(e.error)) => <class 'str'>
                    # print(type(e.info))  => <class 'dict'>
                    if is_l1:
                        log_fnc(f"{oper} {e.status_code} - {e.info} ")
                    else:
                        if isinstance(e, NotFoundError):
                            if oper in {WesDefs.OP_IND_DELETE,
                                        WesDefs.OP_DOC_SCAN,
                                        WesDefs.OP_DOC_UPDATE}:
                                log_fnc(f"{oper} KEY[{e.info['error']['index']}] - {e.status_code} - {e.info['error']['type']}")
                            else:
                                log_fnc(f"{oper} KEY[{e.info['_index']} <-> {e.info['_type']} <-> {e.info['_id']}] {str(e)}")
                        elif isinstance(e, RequestError):
                            #
                            # OP_DOC_SEARCH  'type': 'parsing_exception'
                            # OP_IND_CREATE  'type': 'invalid_index_name_exception',
                            #                        'reason': 'Invalid index name [first_IND1], must be lowercase'

                            log_fnc(f"{oper} {key_str} - {e.status_code} - {e.info['error']['type']} - {e.info['error']['reason']}")
                        elif isinstance(e, AuthenticationException):
                            log_fnc(f"{oper} {key_str} - {e.status_code} - {e.info['error']['type']} - {e.info['error']['reason']}")
                        # generic
                        else:
                            if e.status_code == 405:
                                log_fnc(f"{oper} {key_str} - {e.status_code} - {e.info['error']}")
                            else:
                                log_fnc(f"{oper} KEY[{e.info['_index']} <-> {e.info['_type']} <-> {e.info['_id']}] {str(e)}")
            else:
                Log.err(f"{oper} Unknow L2 exception ... {str(e)}")
                raise e
        else:
            Log.err(f"{oper} Unknow L1 exception ... {str(e)}")
            raise e

    def _operation_result(self, oper: str, key_str: str, rc: ExecCode, fmt_fnc_ok=None, fmt_fnc_nok=None) -> ExecCode:
        if rc.status == Wes.RC_OK:
            Log.ok(f"{oper} {fmt_fnc_ok(rc)}")
        elif rc.status == Wes.RC_NOK:
            Log.err(f"{oper} {fmt_fnc_nok(rc)}")
        elif rc.status == Wes.RC_EXCE:
            self._dump_exeption(oper, rc, False, Log.err, key_str)  # this is L2 - use error
        else:
            raise ValueError(f"{oper} unknown status - {rc.status}")
        return rc

    class Decor:
        @staticmethod
        def operation_exec(oper):
            def wrapper_mk(fnc):
                def wrapper(self, *args, **kwargs) -> ExecCode:
                    try:
                        Log.log(f"{oper} args({str(args)}) args({str(kwargs)})")
                        rc = fnc(self, *args, **kwargs)
                        Log.log(f"{oper} {str(rc)}")                  # this is L1 - log as is
                        return ExecCode(status=WesDefs.RC_OK, data=rc, fnc_params=([*args], {**kwargs}))
                    except Exception as e:
                        rc = ExecCode(status=WesDefs.RC_EXCE, data=e, fnc_params=([*args], {**kwargs}))
                        self._dump_exeption(oper, rc, True, Log.warn)  # this is L1 - only warn
                        return rc
                return wrapper
            return wrapper_mk

    def helper_key_index(self, rc: ExecCode) -> str:
        # check if index in kwargs
        has_index = rc.fnc_params[1].get('index', None)
        if has_index:
            return f"[{has_index}]"
        else:
            # use what is inside positional
            return str(rc.fnc_params[0])

class Wes(WesDefs):

    # !!! keep always in sync !!!

    def __init__(self):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = Elasticsearch()  # local instance

    #####################
    # indice operations
    #####################
    @query_params(
        "master_timeout",
        "timeout" "request_timeout",
        "wait_for_active_shards",
        "include_type_name",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_CREATE)
    def ind_create(self, index, body=None, params=None):
        return self.es.indices.create(index, body=body, params=params)

    def ind_create_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} ack[{rcv.data['acknowledged']} - {rcv.data['shards_acknowledged']}]"
        return self._operation_result(Wes.OP_IND_CREATE, key_str, rc, fmt_fnc_ok)

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

    def ind_exist_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data}"
        return self._operation_result(Wes.OP_IND_EXIST, key_str, rc, fmt_fnc_ok)

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

    def ind_delete_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data['acknowledged']}"
        return self._operation_result(Wes.OP_IND_DELETE, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "force",
        "ignore_unavailable",
        "wait_if_ongoing",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_FLUSH)
    def ind_flush(self, index=None, params=None):
        return self.es.indices.flush(index=index, params=params)

    def ind_flush_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {str(rcv.data)}"
        return self._operation_result(Wes.OP_IND_FLUSH, key_str, rc, fmt_fnc_ok)

    @query_params("allow_no_indices",
                  "expand_wildcards",
                  "ignore_unavailable")
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_REFRESH)
    def ind_refresh(self, index=None, params=None):
        return self.es.indices.refresh(index=index, params=params)

    def ind_refresh_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {str(rcv.data)}"
        return self._operation_result(Wes.OP_IND_REFRESH, key_str, rc, fmt_fnc_ok)

    def ind_refresh_result_shard_nb_failed(self, rc: ExecCode) -> ExecCode:
        # TODO any idea what to check?
        return rc.data['_shards']['failed']

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

    def ind_get_mapping_result(self, rc: ExecCode) -> ExecCode:
        key_str = None
        if ExecCode.status == Wes.RC_OK:
            key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')} <-> {rc.fnc_params[1].get('doc_type', '_all')}]"
        else:
            key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec = ''
            for rc_index in rcv.data.keys():
                rec += '\n' + f"IND[{rc_index}]"

                # mappings not exist
                mappings = rcv.data[rc_index].get('mappings', None)
                if len(mappings) == 0:
                    rec += " : Missing mappings" + '\n'
                    continue
                else:
                    if Wes.ES_VERSION_RUNNING == Wes.ES_VERSION_7_3_0:
                        propsCheck = mappings.get('properties', None)
                        if propsCheck is None:
                            # doc_type is nested
                            nested_doc_type = list(mappings.keys())[0]
                            rc_doc_type_str = f" : DOC[{nested_doc_type}]"
                            rec = rec + rc_doc_type_str

                            mappings = list(mappings.values())[0]  # probably one
                        rec = rec + '\n'
                        mappings = mappings.get('properties', None)
                        for prop in mappings:
                            rec = rec + str(prop) + ": " + str(mappings[prop]) + '\n'
                    elif Wes.ES_VERSION_RUNNING == Wes.ES_VERSION_5_6_5:
                        for maps in mappings:
                            rec += '\n' + str(maps) + '\n'
                            for map in mappings[maps]:
                                rec += '-> ' + str(map) + '\n'
                                is_dict = mappings[maps][map]
                                if isinstance(is_dict, list) or isinstance(is_dict, dict):
                                    for item in is_dict:
                                        rec += '---> ' + str(item) + '\n'
                                else:
                                    rec += '---> ' + str(is_dict) + '\n'
                    else:
                        self.es_version_mismatch()

            return f"{key_str} MAPPING: {rec}"

        return self._operation_result(Wes.OP_IND_GET_MAP, key_str, rc, fmt_fnc_ok)

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

    def ind_put_mapping_result(self, rc: ExecCode, is_per_line: bool = True) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')} <-> {rc.fnc_params[1].get('doc_type', '_all')}]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"MAPPING: <-> {str(rcv.data)}"
        return self._operation_result(Wes.OP_IND_PUT_MAP, key_str, rc, fmt_fnc_ok)

    @query_params(
        "create",
        "flat_settings",
        "master_timeout",
        "order",
        "request_timeout",
        "timeout",
        "include_type_name",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_PUT_TMP)
    def ind_put_template(self, name, body, params=None):
        return self.es.indices.put_template(name=name, body=body, params=params)

    def ind_put_template_result(self, rc: ExecCode, is_per_line: bool = True) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"TEMPLATE: <-> {str(rcv.data)}"

        return self._operation_result(Wes.OP_IND_PUT_TMP, key_str, rc, fmt_fnc_ok)

    @query_params("flat_settings",
                  "local",
                  "master_timeout",
                  "include_type_name")
    @WesDefs.Decor.operation_exec(WesDefs.OP_IND_GET_TMP)
    def ind_get_template(self, name=None, params=None):
        return self.es.indices.get_template(name=name, params=params)

    def ind_get_template_result(self, rc: ExecCode, is_per_line: bool = True) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"TEMPLATE: <-> {str(rcv.data)}"

        return self._operation_result(Wes.OP_IND_GET_TMP, key_str, rc, fmt_fnc_ok)


    #####################
    # doc operations
    #####################
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

    def doc_addup_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['result']} {rcv.data['_shards']}"

        return self._operation_result(Wes.OP_DOC_ADD_UP, key_str, rc, fmt_fnc_ok)

    @query_params(
        "_source",
        "_source_exclude",
        "_source_excludes",
        "_source_include",
        "_source_includes",
        "fields",
        "if_seq_no",
        "if_primary_term",
        "lang",
        "parent",
        "refresh",
        "retry_on_conflict",
        "routing",
        "timeout",
        "timestamp",
        "ttl",
        "version",
        "version_type",
        "wait_for_active_shards",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_UPDATE)
    def doc_update(self, index, id, doc_type="_doc", body=None, params=None):
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.update(index, id, doc_type=doc_type, body=body, params=params)

    def doc_update_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['result']} {rcv.data['_shards']}"

        return self._operation_result(Wes.OP_DOC_UPDATE, key_str, rc, fmt_fnc_ok)

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

    def doc_get_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['_source']}"
        return self._operation_result(Wes.OP_DOC_GET, key_str, rc, fmt_fnc_ok)

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
    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_EXIST)
    def doc_exists(self, index, id, doc_type="_doc", params=None):
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.exists(index, id, doc_type=doc_type, params=params)

    def doc_exists_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data}"

        return self._operation_result(Wes.OP_DOC_EXIST, key_str, rc, fmt_fnc_ok)

    #####################
    # batch operations
    #####################
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

    def doc_search_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')} ]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec_list = rcv.data['hits']['hits']
            rec = ''
            rec = rec + '\nHITS:\n' + '\n'.join([str(item) for item in rec_list])
            rec = rec + '\nAGGS:\n'
            aggs = rcv.data.get('aggregations', None)
            if aggs:
                for a in aggs.keys():
                    rec = rec + a + '\n'
                    for a_items in aggs[a]['buckets']:
                        rec = rec + str(a_items) + '\n'

            return f"{key_str} NB REC[{self.doc_search_result_hits_nb(rcv)}] : {rec}"

        return self._operation_result(Wes.OP_DOC_SEARCH, key_str, rc, fmt_fnc_ok)

    def doc_search_result_hits_nb(self, rc: ExecCode):
        if self.ES_VERSION_RUNNING == self.ES_VERSION_7_3_0:
            return rc.data['hits']['total']['value']
        elif self.ES_VERSION_RUNNING == self.ES_VERSION_5_6_5:
            return rc.data['hits']['total']
        else:
            self.es_version_mismatch()

    def doc_search_result_hits_sources(self, rc: ExecCode):

        if self.ES_VERSION_RUNNING == self.ES_VERSION_7_3_0:
            self.es_version_mismatch()
        elif self.ES_VERSION_RUNNING == self.ES_VERSION_5_6_5:
            sources = rc.data['hits']['hits']
            # print("MISO---> : ", len(sources), type(sources), str(sources))
            return sources
        else:
            self.es_version_mismatch()


    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_BULK)
    def doc_bulk(self, actions, stats_only=False, *args, **kwargs):
        # The bulk() api accepts 'index', 'create', 'delete', 'update' actions.
        # '_op_type' field to specify an action ( DEFAULTS to 'index'):
        # -> 'index' and 'create' expect a 'source' on the next line, and have the same semantics as the 'op_type' parameter to the standard index API
        #    - 'create' will fail if a document with the same index exists already,
        #    - 'index' will add or replace a document as necessary.
        # -> 'delete' does not expect a source on the following line, and has the same semantics as the standard delete API.
        # -> 'update' expects that the partial doc, upsert and script and its options are specified on the next line.
        #
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # MSE_NOTES: - exception behavior should be SUPPRESSED - some operations in batch could PASS
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # It returns a tuple with summary information:
        # - number of successfully executed actions
        # - and either list of errors or number of errors if ``stats_only`` is set to ``True``.
        # Note: that by default we raise a ``BulkIndexError`` when we encounter an error so
        #  options like ``stats_only`` only apply when ``raise_on_error`` is set to ``False``.
        return helpers.bulk(self.es, actions, raise_on_error=False, stats_only=stats_only, *args, **kwargs)

    def doc_bulk_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        fmt_fnc_ok = None
        fmt_fnc_nok = None

        if rc.status == Wes.RC_OK:
            nb_ok = rc.data[0]
            err_list = [str(item) for item in rc.data[1]]
            nb_err = len(err_list)
            nb_total = nb_ok + nb_err
            ret_str_hdr = f"TOTAL[{nb_total}] <-> SUCCESS[{nb_ok}] <-> FAILED[{nb_err}]"
            ret_str_ftr = '\n' if nb_err else ''
            ret_str_ftr += '\n'.join(err_list) if nb_err else ''

            def fmt_fnc_ok(rcv: ExecCode) -> str:
                return f"{ret_str_hdr}  ok ... {ret_str_ftr}"

            def fmt_fnc_nok(rcv: ExecCode) -> str:
                return f"{ret_str_hdr}  err ... {ret_str_ftr}"

            status = Wes.RC_OK if len(rc.data[1]) == 0 else Wes.RC_NOK

        rc = ExecCode(status, rc.data, rc.fnc_params)
        return self._operation_result(Wes.OP_DOC_BULK, key_str, rc, fmt_fnc_ok, fmt_fnc_nok)

    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_SCAN)
    def doc_scan(self,
                 query=None,
                 scroll="5m",
                 raise_on_error=True,
                 preserve_order=False,
                 size=1000,
                 request_timeout=None,
                 clear_scroll=True,
                 scroll_kwargs=None,
                 ** kwargs): #index

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # MSE_NOTES: - exception behavior should be SUPPRESSED - some operations in batch could PASS
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # - it returns generator
        # - in case exception generator of generators
        return helpers.scan(self.es,
                            query=query,
                            scroll=scroll,
                            raise_on_error=raise_on_error,
                            preserve_order=preserve_order,
                            size=size,
                            request_timeout=request_timeout,
                            clear_scroll=clear_scroll,
                            scroll_kwargs=scroll_kwargs,
                            ** kwargs)

    def doc_scan_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        # MSE_NOTE it doesn't fire exception
        # in case NotFoundError(404) index
        try:
            for a in rc.data:
                break
        except Exception as e:
            rc = ExecCode(Wes.RC_EXCE, e, rc.fnc_params)

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec = ''
            nb_rec = 0
            for item in rcv.data:
                nb_rec += 1
                rec += '\n' + str(item)

            return f"SCAN NB[{nb_rec}] {rec}"

        return self._operation_result(Wes.OP_DOC_SCAN, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "analyze_wildcard",
        "analyzer",
        "default_operator",
        "df",
        "expand_wildcards",
        "ignore_unavailable",
        "ignore_throttled",
        "lenient",
        "min_score",
        "preference",
        "q",
        "routing",
        "terminate_after",)
    @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_COUNT)
    def doc_count(self, doc_type=None, index=None, body=None, params=None):
        return self.es.count(doc_type=doc_type, index=index, body=body, params=params)

    def doc_count_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            # return str(rcv.data)
            return f"{key_str} COUNT[{rcv.data['count']}]"

        return self._operation_result(WesDefs.OP_DOC_COUNT, key_str, rc, fmt_fnc_ok)

    @staticmethod
    def operation_mappers(operation: str):
        operation_mapper = {
            # indice operations
            Wes.OP_IND_CREATE:  [Wes.ind_create, Wes.ind_create_result],
            Wes.OP_IND_FLUSH:   [Wes.ind_flush, Wes.ind_flush_result],
            Wes.OP_IND_REFRESH: [Wes.ind_refresh, Wes.ind_refresh_result],
            Wes.OP_IND_EXIST:   [Wes.ind_exist, Wes.ind_exist_result],
            Wes.OP_IND_DELETE:  [Wes.ind_delete, Wes.ind_delete_result],
            Wes.OP_IND_GET_MAP: [Wes.ind_get_mapping, Wes.ind_get_mapping_result],
            Wes.OP_IND_PUT_MAP: [Wes.ind_put_mapping, Wes.ind_put_mapping_result],
            Wes.OP_IND_PUT_TMP: [Wes.ind_put_template, Wes.ind_put_template_result],
            Wes.OP_IND_GET_TMP: [Wes.ind_get_template, Wes.ind_get_template_result],
            # document operations
            Wes.OP_DOC_ADD_UP:  [Wes.doc_addup, Wes.doc_addup_result],
            Wes.OP_DOC_UPDATE:  [Wes.doc_update, Wes.doc_update_result],
            Wes.OP_DOC_GET:     [Wes.doc_get, Wes.doc_get_result],
            Wes.OP_DOC_EXIST:   [Wes.doc_exists, Wes.doc_exists],
            # batch operations
            Wes.OP_DOC_SEARCH:  [Wes.doc_search, Wes.doc_search_result],
            Wes.OP_DOC_BULK:    [Wes.doc_bulk, Wes.doc_bulk_result],
            Wes.OP_DOC_SCAN:    [Wes.doc_scan, Wes.doc_scan_result],
            Wes.OP_DOC_COUNT:   [Wes.doc_count, Wes.doc_count_result]
        }

        return operation_mapper.get(operation, None)
