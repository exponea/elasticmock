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
from elasticsearch.helpers.actions import expand_action

from log import Log
from mock_es import MockEs
from common import WesDefs, ExecCode

__all__ = ["Wes"]

class WesCommon():

    ES_VERSION_RUNNING = WesDefs.ES_VERSION_DEFAULT

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
        # 		????                                     405  status code
        # 		????                                     500  status code  500 - {'root_cause': [{'type': 'illegal_state_exception', 'reason': 'node [FbBtYVegTjGw-KtAzsxYcw] is not available'}
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

                    # print(type(e.error)) # => <class 'str'>
                    # print(type(e.info))  # => <class 'dict'>
                    if is_l1:
                        log_fnc(f"{oper} {e.status_code} - {e.info} - {e.error}")
                    else:
                        log_fnc(f"{oper} {key_str} {e.status_code} - {e.info} - {e.error}")
                    # TOO MESSY
                    #     if isinstance(e, NotFoundError):
                    #         if oper in {WesDefs.OP_IND_DELETE,
                    #                     WesDefs.OP_DOC_SCAN,
                    #                     WesDefs.OP_DOC_UPDATE}:
                    #             print('e.error -->', e.error)
                    #             print('e.info -->', e.info)
                    #             print('e.info[error] -->', e.info['error'])
                    #             print('e.info[error][index] -->', e.info['error']['index'])
                    #             print(e.status_code)
                    #             print('e.info[error][type] -->', e.info['error']['type'])
                    #             log_fnc(f"{oper} KEY[{e.info['error']['index']}] - {e.status_code} - {e.info['error']['type']}")
                    #         else:
                    #             log_fnc(f"{oper} KEY[{e.info['_index']} <-> {e.info['_type']} <-> {e.info['_id']}] {str(e)}")
                    #     elif isinstance(e, RequestError):
                    #         #
                    #         # OP_DOC_SEARCH  'type': 'parsing_exception'
                    #         # OP_IND_CREATE  'type': 'invalid_index_name_exception',
                    #         #                        'reason': 'Invalid index name [first_IND1], must be lowercase'
                    #
                    #         log_fnc(f"{oper} {key_str} - {e.status_code} - {e.info['error']['type']} - {e.info['error']['reason']}")
                    #     elif isinstance(e, AuthenticationException):
                    #         log_fnc(f"{oper} {key_str} - {e.status_code} - {e.info['error']['type']} - {e.info['error']['reason']}")
                    #     # generic
                    #     else:
                    #         if e.status_code == 405 or \
                    #            e.status_code == 500:
                    #             log_fnc(f"{oper} {key_str} - {e.status_code} - {e.info['error']}")
                    #         else:
                    #             log_fnc(f"{oper} KEY[{e.info['_index']} <-> {e.info['_type']} <-> {e.info['_id']}] {str(e)}")
            else:
                Log.err(f"{oper} Unknow L2 exception ... {str(e)}")
                raise e
        else:
            Log.err(f"{oper} Unknow L1 exception ... {str(e)}")
            raise e

    def _operation_result(self, oper: str, key_str: str, rc: ExecCode, fmt_fnc_ok=None, fmt_fnc_nok=None) -> ExecCode:
        if rc.status == WesDefs.RC_OK:
            Log.ok(f"{oper} {fmt_fnc_ok(rc)}")
        elif rc.status == WesDefs.RC_NOK:
            Log.err(f"{oper} {fmt_fnc_nok(rc)}")
        elif rc.status == WesDefs.RC_EXCE:
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

    def helper_key_name(self, rc: ExecCode) -> str:
        # check if index in kwargs
        has_index = rc.fnc_params[1].get('name', None)
        if has_index:
            return f"[{has_index}]"
        else:
            # use what is inside positional
            return str(rc.fnc_params[0])

class Wes(WesCommon):

    def __init__(self, use_mocked: bool = False):
        # self.es = Elasticsearch(HOST="http://localhost", PORT=9200)  # remote instance
        self.es = MockEs() if use_mocked else Elasticsearch() # local instance

    #####################
    # indice operations
    #####################
    @query_params(
        "master_timeout",
        "timeout" "request_timeout",
        "wait_for_active_shards",
        "include_type_name",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_CREATE)
    def ind_create(self, index, body=None, params=None):
        return self.es.indices.create(index, body=body, params=params)

    def ind_create_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} ack[{rcv.data['acknowledged']} - {rcv.data['shards_acknowledged']}]"
        return self._operation_result(WesDefs.OP_IND_CREATE, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "include_defaults",
        "local",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_EXIST)
    def ind_exist(self, index, params=None):
        return self.es.indices.exists(index, params=params)

    def ind_exist_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data}"
        return self._operation_result(WesDefs.OP_IND_EXIST, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "timeout",
        "master_timeout",
        "request_timeout",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_DELETE)
    def ind_delete(self, index, params=None):
        return self.es.indices.delete(index, params=params)

    def ind_delete_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data['acknowledged']}"
        return self._operation_result(WesDefs.OP_IND_DELETE, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "include_defaults",
        "local",
        "include_type_name",
        "master_timeout",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_GET)
    def ind_get(self, index, feature=None, params=None):
        return self.es.indices.get(index, feature=feature, params=params)

    def ind_get_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[0]}]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {WesDefs.dump2string_result_ind(rc.data, self)}"

        return self._operation_result(WesDefs.OP_IND_GET, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "force",
        "ignore_unavailable",
        "wait_if_ongoing",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_FLUSH)
    def ind_flush(self, index=None, params=None):
        return self.es.indices.flush(index=index, params=params)

    def ind_flush_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {str(rcv.data)}"
        return self._operation_result(WesDefs.OP_IND_FLUSH, key_str, rc, fmt_fnc_ok)

    @query_params("allow_no_indices",
                  "expand_wildcards",
                  "ignore_unavailable")
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_REFRESH)
    def ind_refresh(self, index=None, params=None):
        return self.es.indices.refresh(index=index, params=params)

    def ind_refresh_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"
        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {str(rcv.data)}"
        return self._operation_result(WesDefs.OP_IND_REFRESH, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_GET_MAP)
    def ind_get_mapping(self, index=None, doc_type=None, params=None):
        return self.es.indices.get_mapping(index=index, doc_type=doc_type, params=params)

    def ind_get_mapping_result(self, rc: ExecCode) -> ExecCode:
        key_str = None
        if ExecCode.status == WesDefs.RC_OK:
            key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')} <-> {rc.fnc_params[1].get('doc_type', '_all')}]"
        else:
            key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec = ''
            for rc_index in rcv.data.keys():
                prefix = f"IND[{rc_index}] {'mappings':>9} <-> "
                rec += WesDefs.dump2string_result_ind_mappings(rcv.data[rc_index].get('mappings', None), self, prefix)

            return f"{key_str} MAPPING: {rec}"

        return self._operation_result(WesDefs.OP_IND_GET_MAP, key_str, rc, fmt_fnc_ok)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "master_timeout",
        "timeout",
        "request_timeout",
        "include_type_name",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_PUT_MAP)
    def ind_put_mapping(self, body, doc_type=None, index=None, params=None):
        # TODO petee 'index' is important - shouldn't be mandatory???
        # TODO petee 'doc_type' is important - shouldn't be mandatory???
        return self.es.indices.put_mapping(body, index=index, doc_type=doc_type, params=params)

    def ind_put_mapping_result(self, rc: ExecCode, is_per_line: bool = True) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')} <-> {rc.fnc_params[1].get('doc_type', '_all')}]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"MAPPING: <-> {str(rcv.data)}"

        return self._operation_result(WesDefs.OP_IND_PUT_MAP, key_str, rc, fmt_fnc_ok)

    @query_params(
        "create",
        "flat_settings",
        "master_timeout",
        "order",
        "request_timeout",
        "timeout",
        "include_type_name",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_PUT_TMP)
    def ind_put_template(self, name, body, params=None):
        return self.es.indices.put_template(name=name, body=body, params=params)

    def ind_put_template_result(self, rc: ExecCode, is_per_line: bool = True) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"TEMPLATE: <-> {str(rcv.data)}"

        return self._operation_result(WesDefs.OP_IND_PUT_TMP, key_str, rc, fmt_fnc_ok)

    @query_params("flat_settings",
                  "local",
                  "master_timeout",
                  "include_type_name")
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_GET_TMP)
    def ind_get_template(self, name=None, params=None):
        return self.es.indices.get_template(name=name, params=params)

    def ind_get_template_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            res = WesDefs.dump2string_result_templates(rcv.data, self)
            return f"TEMPLATE: <-> {res}"

        return self._operation_result(WesDefs.OP_IND_GET_TMP, key_str, rc, fmt_fnc_ok)


    @query_params("allow_no_indices", "expand_wildcards", "ignore_unavailable", "local")
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_GET_ALIAS)
    def ind_get_alias(self, index=None, name=None, params=None):
        return self.es.indices.get_alias(index=index, name=name, params=params)

    def ind_get_alias_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}{self.helper_key_name(rc)}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            res = "\n" + str(rcv.data)
            return f"{key_str} ALIAS GET: {res}"

        return self._operation_result(WesDefs.OP_IND_GET_ALIAS, key_str, rc, fmt_fnc_ok)


    @query_params("master_timeout", "request_timeout", "timeout")
    @WesCommon.Decor.operation_exec(WesDefs.OP_IND_DEL_ALIAS)
    def ind_delete_alias(self, index, name, params=None):
        return self.es.indices.delete_alias(index, name, params=params)

    def ind_delete_alias_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{self.helper_key_index(rc)}{self.helper_key_name(rc)}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} ALIAS DEL: {str(rcv.data['acknowledged'])}"

        return self._operation_result(WesDefs.OP_IND_DEL_ALIAS, key_str, rc, fmt_fnc_ok)

    #####################
    # general
    #####################
    @query_params()
    @WesCommon.Decor.operation_exec(WesDefs.OP_GEN_PING)
    def gen_ping(self, params=None):
        return self.es.ping(params=params)

    def gen_ping_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"PING{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} - {rcv.data}"

        return self._operation_result(WesDefs.OP_GEN_PING, key_str, rc, fmt_fnc_ok)


    @query_params()
    @WesCommon.Decor.operation_exec(WesDefs.OP_GEN_INFO)
    def gen_info(self, params=None):
        return self.es.info(params=params)

    def gen_info_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"INFO{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} - {rcv.data}"

        return self._operation_result(WesDefs.OP_GEN_INFO, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_ADD_UP)
    def doc_addup(self, index, body, doc_type="_doc", id=None, params=None):
        # TODO petee 'id' is important for get - shouldn't be mandatory???
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.index(index, body, doc_type=doc_type, id=id, params=params)

    def doc_addup_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['result']} {rcv.data['_shards']}"

        return self._operation_result(WesDefs.OP_DOC_ADD_UP, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_UPDATE)
    def doc_update(self, index, id, doc_type="_doc", body=None, params=None):
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.update(index, id, doc_type=doc_type, body=body, params=params)

    def doc_update_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['result']} {rcv.data['_shards']}"

        return self._operation_result(WesDefs.OP_DOC_UPDATE, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_GET)
    def doc_get(self, index, id, doc_type="_doc", params=None):
        return self.es.get(index, id, doc_type=doc_type, params=params)

    def doc_get_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['_source']}"
        return self._operation_result(WesDefs.OP_DOC_GET, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_EXIST)
    def doc_exists(self, index, id, doc_type="_doc", params=None):
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.exists(index, id, doc_type=doc_type, params=params)

    def doc_exists_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data}"

        return self._operation_result(WesDefs.OP_DOC_EXIST, key_str, rc, fmt_fnc_ok)

    @query_params(
        "if_seq_no",
        "if_primary_term",
        "parent",
        "refresh",
        "routing",
        "timeout",
        "version",
        "version_type",
        "wait_for_active_shards",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_DEL)
    def doc_delete(self, index, id, doc_type="_doc", params=None):
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        return self.es.delete(index, id, doc_type=doc_type, params=params)

    def doc_delete_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"KEY[{rcv.data['_index']} <-> {rcv.data['_type']} <-> {rcv.data['_id']}] {rcv.data['result']} {rcv.data['_shards']}"

        return self._operation_result(WesDefs.OP_DOC_DEL, key_str, rc, fmt_fnc_ok)


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
        "analyze_wildcard",
        "analyzer",
        "conflicts",
        "default_operator",
        "df",
        "expand_wildcards",
        "from_",
        "ignore_unavailable",
        "lenient",
        "preference",
        "q",
        "refresh",
        "request_cache",
        "requests_per_second",
        "routing",
        "scroll",
        "scroll_size",
        "search_timeout",
        "search_type",
        "size",
        "slices",
        "sort",
        "stats",
        "terminate_after",
        "timeout",
        "version",
        "wait_for_active_shards",
        "wait_for_completion",)
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_DEL_QUERY)
    def doc_delete_by_query(self, index, body, params=None):
        return self.es.delete_by_query(index, body, params=params)

    def doc_delete_by_query_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data}"

        return self._operation_result(WesDefs.OP_DOC_DEL_QUERY, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_SEARCH)
    def doc_search(self, index=None, body=None, params=None):
        return self.es.search(index=index, body=body, params=params)

    def doc_search_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')} ]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec_list = self.doc_search_result_hits_sources(rc)
            rec = ''
            rec = rec + '\nHITS:\n' + '\n'.join([str(item) for item in rec_list])
            rec = rec + '\nAGGS:\n'
            aggs = rcv.data.get('aggregations', None)
            if aggs:
                for a in aggs.keys():
                    rec = rec + a + '\n'
                    for a_items in aggs[a]['buckets']:
                        rec = rec + str(a_items) + '\n'

            return f"{key_str} NB TOTAL[{self.doc_search_result_total_nb(rcv)}] NB HITS[{self.doc_search_result_hits_nb(rcv)}]: {rec}"

        return self._operation_result(WesDefs.OP_DOC_SEARCH, key_str, rc, fmt_fnc_ok)

    def doc_search_result_total_nb(self, rc: ExecCode):
        if self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            return rc.data['hits']['total']['value']
        elif self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            return rc.data['hits']['total']
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)

    def doc_search_result_hits_nb(self, rc: ExecCode):
        return len(self.doc_search_result_hits_sources(rc))

    def doc_search_result_hits_sources(self, rc: ExecCode):

        if self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)
        elif self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            sources = rc.data['hits']['hits']
            # print("MISO---> : ", len(sources), type(sources), str(sources))
            return sources
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)

    def doc_search_result_scroll_id(self, rc: ExecCode):

        if self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)
        elif self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            return rc.data.get('_scroll_id', None)
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)


    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_BULK)
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
        if isinstance(self.es, MockEs):
            # MSE_NOTES: decision to not make 'helpers' nesting e.g. 'es.helpers.bulk'
            return self.es.bulk(actions, raise_on_error=False, stats_only=stats_only, *args, **kwargs)
        else:
            return helpers.bulk(self.es, actions, raise_on_error=False, stats_only=stats_only, *args, **kwargs)

    def doc_bulk_result(self, rc: ExecCode) -> ExecCode:
        #key_str = f"KEY{rc.fnc_params[0]}"
        key_str = f"BULK RESULT"

        fmt_fnc_ok = None
        fmt_fnc_nok = None

        if rc.status == WesDefs.RC_OK:
            nb_ok = rc.data[0]
            err_list = [str(item) for item in rc.data[1]]
            nb_err = len(err_list)
            nb_total = nb_ok + nb_err
            ret_str_hdr = f"{key_str}  TOTAL[{nb_total}] <-> SUCCESS[{nb_ok}] <-> FAILED[{nb_err}]"
            ret_str_ftr = '\n' if nb_err else ''
            ret_str_ftr += '\n'.join(err_list) if nb_err else ''

            def fmt_fnc_ok(rcv: ExecCode) -> str:
                return f"{ret_str_hdr}  ok ... {ret_str_ftr}"

            def fmt_fnc_nok(rcv: ExecCode) -> str:
                return f"{ret_str_hdr}  err ... {ret_str_ftr}"

            status = WesDefs.RC_NOK if nb_err > 0 else WesDefs.RC_OK
            rc = ExecCode(status, rc.data, rc.fnc_params)

        rc = ExecCode(rc.status, rc.data, rc.fnc_params)
        return self._operation_result(WesDefs.OP_DOC_BULK, key_str, rc, fmt_fnc_ok, fmt_fnc_nok)

    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_BULK_STR)
    def doc_bulk_streaming(self,
                           actions,
                           chunk_size=500,
                           max_chunk_bytes=100 * 1024 * 1024,
                           raise_on_error=True,
                           expand_action_callback=expand_action,
                           raise_on_exception=True,
                           max_retries=0,
                           initial_backoff=2,
                           max_backoff=600,
                           yield_ok=True,
                           *args,
                           **kwargs):
        # !!! it returns generator !!!
        if isinstance(self.es, MockEs):
            # MSE_NOTES: decision to not make 'helpers' nesting e.g. 'es.helpers.bulk'
            return self.es.bulk_streaming(
                          actions,
                          chunk_size=chunk_size,
                          max_chunk_bytes=max_chunk_bytes,
                          raise_on_error=raise_on_error,
                          expand_action_callback=expand_action_callback,
                          raise_on_exception=raise_on_exception,
                          max_retries=max_retries,
                          initial_backoff=initial_backoff,
                          max_backoff=max_backoff,
                          yield_ok=yield_ok,
                          *args,
                          **kwargs)
        else:
            return helpers.streaming_bulk(self.es,
                          actions,
                          chunk_size=chunk_size,
                          max_chunk_bytes=max_chunk_bytes,
                          raise_on_error=raise_on_error,
                          expand_action_callback=expand_action_callback,
                          raise_on_exception=raise_on_exception,
                          max_retries=max_retries,
                          initial_backoff=initial_backoff,
                          max_backoff=max_backoff,
                          yield_ok=yield_ok,
                          *args,
                          **kwargs)

    def doc_bulk_streaming_result(self, rc: ExecCode) -> ExecCode:
        #key_str = f"KEY{rc.fnc_params[0]}"
        key_str = f"BULK STREAMING_RESULT"

        fmt_fnc = None

        if rc.status == WesDefs.RC_OK:
            # MSE NOTES:
            # !!! returns generator    !!!
            # !!! can't iterate again  !!!
            # repack to 'list'
            rc = MockEs.bulk_streaming_result_repack_gen2list(rc)

            def fmt_fnc(rcv: ExecCode) -> str:
                res = ''
                nb_total = 0
                nb_ok = 0
                nb_err = 0
                for item in rcv.data:
                    nb_total += 1
                    if item[0]:
                        nb_ok += 1
                    else:
                        nb_err += 1
                    res += '\n' + str(item)

                ret_str_hdr = f"TOTAL[{nb_total}] <-> SUCCESS[{nb_ok}] <-> FAILED[{nb_err}]"
                return f"{ret_str_hdr} {res}"

        return self._operation_result(WesDefs.OP_DOC_BULK_STR, key_str, rc, fmt_fnc, fmt_fnc)

    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_SCAN)
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
            rc = ExecCode(WesDefs.RC_EXCE, e, rc.fnc_params)

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec = ''
            nb_rec = 0
            for item in rcv.data:
                nb_rec += 1
                rec += '\n' + str(item)

            return f"SCAN NB[{nb_rec}] {rec}"

        return self._operation_result(WesDefs.OP_DOC_SCAN, key_str, rc, fmt_fnc_ok)

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
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_COUNT)
    def doc_count(self, doc_type=None, index=None, body=None, params=None):
        return self.es.count(doc_type=doc_type, index=index, body=body, params=params)

    def doc_count_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            # return str(rcv.data)
            return f"{key_str} COUNT[{rcv.data['count']}]"

        return self._operation_result(WesDefs.OP_DOC_COUNT, key_str, rc, fmt_fnc_ok)

    @query_params("scroll", "rest_total_hits_as_int", "scroll_id")
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_SCROLL)
    def doc_scroll(self, body=None, params=None):
        return self.es.scroll(body=body, params=params)

    def doc_scroll_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY[{rc.fnc_params[1].get('index', '_all')}]"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            rec_list = self.doc_scroll_result_hits_sources(rc)
            rec = ''
            rec = rec + '\nHITS:\n' + '\n'.join([str(item) for item in rec_list])
            rec = rec + '\nAGGS:\n'
            aggs = rcv.data.get('aggregations', None)
            if aggs:
                for a in aggs.keys():
                    rec = rec + a + '\n'
                    for a_items in aggs[a]['buckets']:
                        rec = rec + str(a_items) + '\n'

            return f"{key_str} NB TOTAL[{self.doc_scroll_result_total_nb(rcv)}] NB HITS[{self.doc_scroll_result_hits_nb(rcv)}]: {rec}"

        return self._operation_result(WesDefs.OP_DOC_SCROLL, key_str, rc, fmt_fnc_ok)

    def doc_scroll_result_total_nb(self, rc: ExecCode):
        if self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            return rc.data['hits']['total']['value']
        elif self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            return rc.data['hits']['total']
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)

    def doc_scroll_result_hits_nb(self, rc: ExecCode):
        return len(self.doc_scroll_result_hits_sources(rc))

    def doc_scroll_result_hits_sources(self, rc: ExecCode):

        if self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)
        elif self.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            sources = rc.data['hits']['hits']
            # print("MISO---> : ", len(sources), type(sources), str(sources))
            return sources
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)

    @query_params()
    @WesCommon.Decor.operation_exec(WesDefs.OP_DOC_SCROLL_CLEAR)
    def doc_clear_scroll(self, scroll_id=None, body=None, params=None):
        return self.es.clear_scroll(scroll_id=scroll_id, body=body, params=params)

    def doc_clear_scroll_result(self, rc: ExecCode) -> ExecCode:
        key_str = f"KEY{rc.fnc_params[0]}"

        def fmt_fnc_ok(rcv: ExecCode) -> str:
            return f"{key_str} {rcv.data}"

        return self._operation_result(WesDefs.OP_DOC_SCROLL_CLEAR, key_str, rc, fmt_fnc_ok)


    @staticmethod
    def operation_mappers(operation: str):
        operation_mapper = {
            # indice operations
            WesDefs.OP_IND_CREATE:          [Wes.ind_create, Wes.ind_create_result],
            WesDefs.OP_IND_FLUSH:           [Wes.ind_flush, Wes.ind_flush_result],
            WesDefs.OP_IND_REFRESH:         [Wes.ind_refresh, Wes.ind_refresh_result],
            WesDefs.OP_IND_EXIST:           [Wes.ind_exist, Wes.ind_exist_result],
            WesDefs.OP_IND_DELETE:          [Wes.ind_delete, Wes.ind_delete_result],
            WesDefs.OP_IND_GET:             [Wes.ind_get, Wes.ind_get_result],
            WesDefs.OP_IND_GET_ALIAS:       [Wes.ind_get_alias, Wes.ind_get_alias_result],
            WesDefs.OP_IND_DEL_ALIAS:       [Wes.ind_delete_alias, Wes.ind_delete_alias_result],
            WesDefs.OP_IND_GET_MAP:         [Wes.ind_get_mapping, Wes.ind_get_mapping_result],
            WesDefs.OP_IND_PUT_MAP:         [Wes.ind_put_mapping, Wes.ind_put_mapping_result],
            WesDefs.OP_IND_PUT_TMP:         [Wes.ind_put_template, Wes.ind_put_template_result],
            WesDefs.OP_IND_GET_TMP:         [Wes.ind_get_template, Wes.ind_get_template_result],
            # general
            WesDefs.OP_GEN_PING:            [Wes.gen_ping, Wes.gen_ping_result],
            WesDefs.OP_GEN_INFO:            [Wes.gen_info, Wes.gen_info_result],
            # document operations
            WesDefs.OP_DOC_ADD_UP:          [Wes.doc_addup, Wes.doc_addup_result],
            WesDefs.OP_DOC_UPDATE:          [Wes.doc_update, Wes.doc_update_result],
            WesDefs.OP_DOC_GET:             [Wes.doc_get, Wes.doc_get_result],
            WesDefs.OP_DOC_EXIST:           [Wes.doc_exists, Wes.doc_exists_result],
            WesDefs.OP_DOC_DEL:             [Wes.doc_delete, Wes.doc_delete_result],
            # batch operations
            WesDefs.OP_DOC_DEL_QUERY:       [Wes.doc_delete_by_query, Wes.doc_delete_by_query_result],
            WesDefs.OP_DOC_SEARCH:          [Wes.doc_search, Wes.doc_search_result],
            WesDefs.OP_DOC_BULK:            [Wes.doc_bulk, Wes.doc_bulk_result], # MSE_NOTES: RETURN FORMAT NOT MATCH EXPONEA
            WesDefs.OP_DOC_BULK_STR:        [Wes.doc_bulk_streaming, Wes.doc_bulk_streaming_result],
            WesDefs.OP_DOC_SCAN:            [Wes.doc_scan, Wes.doc_scan_result],
            WesDefs.OP_DOC_COUNT:           [Wes.doc_count, Wes.doc_count_result],
            WesDefs.OP_DOC_SCROLL:          [Wes.doc_scroll, Wes.doc_scroll_result],
            WesDefs.OP_DOC_SCROLL_CLEAR:    [Wes.doc_clear_scroll, Wes.doc_clear_scroll_result],
        }

        return operation_mapper.get(operation, None)
