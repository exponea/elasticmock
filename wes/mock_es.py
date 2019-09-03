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

__all__ = ["MockEs"]

class MockEsIndicesClient:
    pass

    def __init__(self):
        self.indices = MockEsIndicesClient()

    # #####################
    # # indice operations
    # #####################
    # @query_params(
    #     "master_timeout",
    #     "timeout" "request_timeout",
    #     "wait_for_active_shards",
    #     "include_type_name",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_CREATE)
    # def ind_create(self, index, body=None, params=None):
    #     return self.es.indices.create(index, body=body, params=params)
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "flat_settings",
    #     "ignore_unavailable",
    #     "include_defaults",
    #     "local",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_EXIST)
    # def ind_exist(self, index, params=None):
    #     return self.es.indices.exists(index, params=params)
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "ignore_unavailable",
    #     "timeout",
    #     "master_timeout",
    #     "request_timeout",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_DELETE)
    # def ind_delete(self, index, params=None):
    #     return self.es.indices.delete(index, params=params)
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "flat_settings",
    #     "ignore_unavailable",
    #     "include_defaults",
    #     "local",
    #     "include_type_name",
    #     "master_timeout",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_GET)
    # def ind_get(self, index, feature=None, params=None):
    #     return self.es.indices.get(index, feature=feature, params=params)
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "force",
    #     "ignore_unavailable",
    #     "wait_if_ongoing",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_FLUSH)
    # def ind_flush(self, index=None, params=None):
    #     return self.es.indices.flush(index=index, params=params)
    #
    #
    # @query_params("allow_no_indices",
    #               "expand_wildcards",
    #               "ignore_unavailable")
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_REFRESH)
    # def ind_refresh(self, index=None, params=None):
    #     return self.es.indices.refresh(index=index, params=params)
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "ignore_unavailable",
    #     "local",
    #     "include_type_name",
    #     "master_timeout",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_GET_MAP)
    # def ind_get_mapping(self, index=None, doc_type=None, params=None):
    #     return self.es.indices.get_mapping(index=index, doc_type=doc_type, params=params)
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "ignore_unavailable",
    #     "master_timeout",
    #     "timeout",
    #     "request_timeout",
    #     "include_type_name",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_PUT_MAP)
    # def ind_put_mapping(self, body, doc_type=None, index=None, params=None):
    #     # TODO petee 'index' is important - shouldn't be mandatory???
    #     # TODO petee 'doc_type' is important - shouldn't be mandatory???
    #     return self.es.indices.put_mapping(body, index=index, doc_type=doc_type, params=params)
    #
    # @query_params(
    #     "create",
    #     "flat_settings",
    #     "master_timeout",
    #     "order",
    #     "request_timeout",
    #     "timeout",
    #     "include_type_name",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_PUT_TMP)
    # def ind_put_template(self, name, body, params=None):
    #     return self.es.indices.put_template(name=name, body=body, params=params)
    #
    # @query_params("flat_settings",
    #               "local",
    #               "master_timeout",
    #               "include_type_name")
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_GET_TMP)
    # def ind_get_template(self, name=None, params=None):
    #     return self.es.indices.get_template(name=name, params=params)
    #
    # @query_params("allow_no_indices", "expand_wildcards", "ignore_unavailable", "local")
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_GET_ALIAS)
    # def ind_get_alias(self, index=None, name=None, params=None):
    #     return self.es.indices.get_alias(index=index, name=name, params=params)
    #
    # @query_params("master_timeout", "request_timeout", "timeout")
    # @WesDefs.Decor.operation_exec(WesDefs.OP_IND_DEL_ALIAS)
    # def ind_delete_alias(self, index, name, params=None):
    #     return self.es.indices.delete_alias(index, name, params=params)

class MockEs:

    def __init__(self):
        self.indices = MockEsIndicesClient()

    # #####################
    # # general
    # #####################
    # @query_params()
    # @WesDefs.Decor.operation_exec(WesDefs.OP_GEN_PING)
    # def gen_ping(self, params=None):
    #     return self.es.ping(params=params)
    #
    # @query_params()
    # @WesDefs.Decor.operation_exec(WesDefs.OP_GEN_INFO)
    # def gen_info(self, params=None):
    #     return self.es.info(params=params)
    #

    # #####################
    # # doc operations
    # #####################
    # @query_params(
    #     "if_seq_no",
    #     "if_primary_term",
    #     "op_type",
    #     "parent",
    #     "pipeline",
    #     "refresh",
    #     "routing",
    #     "timeout",
    #     "timestamp",
    #     "ttl",
    #     "version",
    #     "version_type",
    #     "wait_for_active_shards",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_ADD_UP)
    # def doc_addup(self, index, body, doc_type="_doc", id=None, params=None):
    #     # TODO petee 'id' is important for get - shouldn't be mandatory???
    #     # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
    #     return self.es.index(index, body, doc_type=doc_type, id=id, params=params)
    #
    # @query_params(
    #     "_source",
    #     "_source_exclude",
    #     "_source_excludes",
    #     "_source_include",
    #     "_source_includes",
    #     "fields",
    #     "if_seq_no",
    #     "if_primary_term",
    #     "lang",
    #     "parent",
    #     "refresh",
    #     "retry_on_conflict",
    #     "routing",
    #     "timeout",
    #     "timestamp",
    #     "ttl",
    #     "version",
    #     "version_type",
    #     "wait_for_active_shards",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_UPDATE)
    # def doc_update(self, index, id, doc_type="_doc", body=None, params=None):
    #     # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
    #     return self.es.update(index, id, doc_type=doc_type, body=body, params=params)

    # @query_params(
    #     "_source",
    #     "_source_exclude",
    #     "_source_excludes",
    #     "_source_include",
    #     "_source_includes",
    #     "parent",
    #     "preference",
    #     "realtime",
    #     "refresh",
    #     "routing",
    #     "stored_fields",
    #     "version",
    #     "version_type",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_GET)
    # def doc_get(self, index, id, doc_type="_doc", params=None):
    #     return self.es.get(index, id, doc_type=doc_type, params=params)
    #
    # @query_params(
    #     "_source",
    #     "_source_exclude",
    #     "_source_excludes",
    #     "_source_include",
    #     "_source_includes",
    #     "parent",
    #     "preference",
    #     "realtime",
    #     "refresh",
    #     "routing",
    #     "stored_fields",
    #     "version",
    #     "version_type",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_EXIST)
    # def doc_exists(self, index, id, doc_type="_doc", params=None):
    #     # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
    #     return self.es.exists(index, id, doc_type=doc_type, params=params)

    # @query_params(
    #     "if_seq_no",
    #     "if_primary_term",
    #     "parent",
    #     "refresh",
    #     "routing",
    #     "timeout",
    #     "version",
    #     "version_type",
    #     "wait_for_active_shards",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_DEL)
    # def doc_delete(self, index, id, doc_type="_doc", params=None):
    #     # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
    #     return self.es.delete(index, id, doc_type=doc_type, params=params)
    #
    #
    # #####################
    # # batch operations
    # #####################
    # @query_params(
    #     "_source",
    #     "_source_exclude",
    #     "_source_excludes",
    #     "_source_include",
    #     "_source_includes",
    #     "allow_no_indices",
    #     "analyze_wildcard",
    #     "analyzer",
    #     "conflicts",
    #     "default_operator",
    #     "df",
    #     "expand_wildcards",
    #     "from_",
    #     "ignore_unavailable",
    #     "lenient",
    #     "preference",
    #     "q",
    #     "refresh",
    #     "request_cache",
    #     "requests_per_second",
    #     "routing",
    #     "scroll",
    #     "scroll_size",
    #     "search_timeout",
    #     "search_type",
    #     "size",
    #     "slices",
    #     "sort",
    #     "stats",
    #     "terminate_after",
    #     "timeout",
    #     "version",
    #     "wait_for_active_shards",
    #     "wait_for_completion",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_DEL_QUERY)
    # def doc_delete_by_query(self, index, body, params=None):
    #     return self.es.delete_by_query(index, body, params=params)
    #
    #
    # @query_params(
    #     "_source",
    #     "_source_exclude",
    #     "_source_excludes",
    #     "_source_include",
    #     "_source_includes",
    #     "allow_no_indices",
    #     "allow_partial_search_results",
    #     "analyze_wildcard",
    #     "analyzer",
    #     "batched_reduce_size",
    #     "ccs_minimize_roundtrips",
    #     "default_operator",
    #     "df",
    #     "docvalue_fields",
    #     "expand_wildcards",
    #     "explain",
    #     "from_",
    #     "ignore_throttled",
    #     "ignore_unavailable",
    #     "lenient",
    #     "max_concurrent_shard_requests",
    #     "pre_filter_shard_size",
    #     "preference",
    #     "q",
    #     "rest_total_hits_as_int",
    #     "request_cache",
    #     "routing",
    #     "scroll",
    #     "search_type",
    #     "seq_no_primary_term",
    #     "size",
    #     "sort",
    #     "stats",
    #     "stored_fields",
    #     "suggest_field",
    #     "suggest_mode",
    #     "suggest_size",
    #     "suggest_text",
    #     "terminate_after",
    #     "timeout",
    #     "track_scores",
    #     "track_total_hits",
    #     "typed_keys",
    #     "version",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_SEARCH)
    # def doc_search(self, index=None, body=None, params=None):
    #     return self.es.search(index=index, body=body, params=params)
    #

    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_BULK)
    # def doc_bulk(self, actions, stats_only=False, *args, **kwargs):
    #     # The bulk() api accepts 'index', 'create', 'delete', 'update' actions.
    #     # '_op_type' field to specify an action ( DEFAULTS to 'index'):
    #     # -> 'index' and 'create' expect a 'source' on the next line, and have the same semantics as the 'op_type' parameter to the standard index API
    #     #    - 'create' will fail if a document with the same index exists already,
    #     #    - 'index' will add or replace a document as necessary.
    #     # -> 'delete' does not expect a source on the following line, and has the same semantics as the standard delete API.
    #     # -> 'update' expects that the partial doc, upsert and script and its options are specified on the next line.
    #     #
    #     # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #     # MSE_NOTES: - exception behavior should be SUPPRESSED - some operations in batch could PASS
    #     # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #     # It returns a tuple with summary information:
    #     # - number of successfully executed actions
    #     # - and either list of errors or number of errors if ``stats_only`` is set to ``True``.
    #     # Note: that by default we raise a ``BulkIndexError`` when we encounter an error so
    #     #  options like ``stats_only`` only apply when ``raise_on_error`` is set to ``False``.
    #     return helpers.bulk(self.es, actions, raise_on_error=False, stats_only=stats_only, *args, **kwargs)

    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_BULK_STR)
    # def doc_bulk_streaming(self,
    #                        actions,
    #                        chunk_size=500,
    #                        max_chunk_bytes=100 * 1024 * 1024,
    #                        raise_on_error=True,
    #                        expand_action_callback=expand_action,
    #                        raise_on_exception=True,
    #                        max_retries=0,
    #                        initial_backoff=2,
    #                        max_backoff=600,
    #                        yield_ok=True,
    #                        *args,
    #                        **kwargs):
    #     # !!! it returns generator !!!
    #     return helpers.streaming_bulk(self.es,
    #                                   actions,
    #                                   chunk_size=chunk_size,
    #                                   max_chunk_bytes=max_chunk_bytes,
    #                                   raise_on_error=raise_on_error,
    #                                   expand_action_callback=expand_action_callback,
    #                                   raise_on_exception=raise_on_exception,
    #                                   max_retries=max_retries,
    #                                   initial_backoff=initial_backoff,
    #                                   max_backoff=max_backoff,
    #                                   yield_ok=yield_ok,
    #                                   *args,
    #                                   **kwargs)

    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_SCAN)
    # def doc_scan(self,
    #              query=None,
    #              scroll="5m",
    #              raise_on_error=True,
    #              preserve_order=False,
    #              size=1000,
    #              request_timeout=None,
    #              clear_scroll=True,
    #              scroll_kwargs=None,
    #              ** kwargs): #index
    #
    #     # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #     # MSE_NOTES: - exception behavior should be SUPPRESSED - some operations in batch could PASS
    #     # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #     # - it returns generator
    #     # - in case exception generator of generators
    #     return helpers.scan(self.es,
    #                         query=query,
    #                         scroll=scroll,
    #                         raise_on_error=raise_on_error,
    #                         preserve_order=preserve_order,
    #                         size=size,
    #                         request_timeout=request_timeout,
    #                         clear_scroll=clear_scroll,
    #                         scroll_kwargs=scroll_kwargs,
    #                         ** kwargs)

    # @query_params(
    #     "allow_no_indices",
    #     "analyze_wildcard",
    #     "analyzer",
    #     "default_operator",
    #     "df",
    #     "expand_wildcards",
    #     "ignore_unavailable",
    #     "ignore_throttled",
    #     "lenient",
    #     "min_score",
    #     "preference",
    #     "q",
    #     "routing",
    #     "terminate_after",)
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_COUNT)
    # def doc_count(self, doc_type=None, index=None, body=None, params=None):
    #     return self.es.count(doc_type=doc_type, index=index, body=body, params=params)

    # @query_params("scroll", "rest_total_hits_as_int", "scroll_id")
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_SCROLL)
    # def doc_scroll(self, body=None, params=None):
    #     return self.es.scroll(body=body, params=params)

    # @query_params()
    # @WesDefs.Decor.operation_exec(WesDefs.OP_DOC_SCROLL_CLEAR)
    # def doc_clear_scroll(self, scroll_id=None, body=None, params=None):
    #     return self.es.clear_scroll(scroll_id=scroll_id, body=body, params=params)

