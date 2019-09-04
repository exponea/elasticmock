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

import json
import sys
PY3 = sys.version_info[0] == 3
if PY3:
    unicode = str

from elasticsearch.client.utils import SKIP_IN_PATH
from elasticmock.utilities import get_random_id, get_random_scroll_id

from common import WesDefs

from log import Log

__all__ = ["MockEs"]


class MockEsCommon:

    def __init__(self):
        self.documents_dict = {}
        self.scrolls = {}

    def raiseNotFound(self, e_list, idx):
        error_data = {'error': {'root_cause': e_list,
                                'type': 'index_not_found_exception',
                                'reason': 'no such index',
                                'resource.type': 'index_or_alias',
                                'resource.id': idx,
                                'index_uuid': '_na_',
                                'index': idx}}
        e_info = json.dumps(error_data)
        e_error = e_info
        raise NotFoundError(404, e_info, e_error)

    def raiseRequestError(self, e_list, idx):
        error_data = {'error': {'root_cause': e_list,
                                'type': 'index_already_exists_exception',
                                'reason': '???',
                                'resource.type': 'index_or_alias',
                                'resource.id': idx,
                                'index_uuid': '_na_',
                                'index': idx}}
        e_info = json.dumps(error_data)
        e_error = e_info
        raise RequestError(400, e_info, e_error)


    class Decor:
        @staticmethod
        def operation_mock(oper):
            def wrapper_mk(fnc):
                def wrapper(self, *args, **kwargs):
                        Log.notice3(f"{oper} is mock")
                        rc = fnc(self, *args, **kwargs)
                        MockEsCommon.dump_dict(oper, self)
                        return rc
                return wrapper
            return wrapper_mk

    @staticmethod
    def dump_dict(oper, obj):
        dict_to_print = MockEsCommon.get_dict(obj)
        dict_print = ''
        for index in dict_to_print.keys():
            dict_print += '\n' + str(index)
            docs = MockEsCommon.get_idx_docs(obj, index)
            for id in docs:
                dict_print += '\n' + str(docs[id])

        Log.log(f"{oper} is mock {dict_print}")

    @staticmethod
    def apply_all_indicies(operation, indices: list):
        if operation == WesDefs.OP_IND_GET_MAP:
            return True if len(indices) == 0 else False
        elif operation == WesDefs.OP_IND_DELETE:
            all_ind_values = ['_all', '*']
            for ind in indices:
                if ind in all_ind_values:
                    return True
            return False

    @staticmethod
    def normalize_index_to_list(obj, index):
        _dict = MockEsCommon.get_dict(obj)
        # Ensure to have a list of index
        if index is None:
            searchable_indexes = _dict.keys()
        elif isinstance(index, str) or isinstance(index, unicode):
            searchable_indexes = [index]
        elif isinstance(index, list):
            searchable_indexes = index
        else:
            # Is it the correct exception to use ?
            raise ValueError("Invalid param 'index'")

        # # Check index(es) exists
        # for searchable_index in searchable_indexes:
        #     if searchable_index not in self.documents_dict:
        #         raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(searchable_index))

        return searchable_indexes

    @staticmethod
    def get_idx_mappings(obj, idx):
        return MockEsCommon.get_dict(obj).get('mappings', {})

    @staticmethod
    def get_idx_settings(obj, idx):
        return MockEsCommon.get_dict(obj).get('settings', {})

    @staticmethod
    def get_idx_docs(obj, idx):
        return MockEsCommon.get_dict(obj)[idx]['docs']

    @staticmethod
    def get_dict(obj):
        parent_dict = obj.parent.documents_dict if hasattr(obj, 'parent') else obj.documents_dict
        return parent_dict

class MockEsIndicesClient:

    def __init__(self, parent):
        self.parent = parent

    #####################
    # indice operations
    #####################
    @query_params(
        "master_timeout",
        "timeout" "request_timeout",
        "wait_for_active_shards",
        "include_type_name",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_CREATE)
    def create(self, index, body=None, params=None):
        """
        Create an index in Elasticsearch.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`_

        :arg index: The name of the index
        :arg body: The configuration for the index (`settings` and `mappings`)
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        :arg request_timeout: Explicit operation timeout
        :arg wait_for_active_shards: Set the number of active shards to wait for
            before the operation returns.
        :arg include_type_name: Specify whether requests and responses should include a
            type name (default: depends on Elasticsearch version).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)

        if len(searchable_indexes) > 1:
            raise ValueError("'index' contains more indexes - it cannot be list ")

        mappings = body.get('mappings', {}) if body else {}
        settings = body.get('settings', {}) if body else {}

        if searchable_indexes[0] in self.parent.documents_dict:
            self.parent.raiseRequestError(['???'], searchable_indexes[0])
        else:
            self.parent.documents_dict[searchable_indexes[0]] = {
                'docs': {},
                'mappings': mappings,
                'settings': settings,
            }

        # TODO - add handling - maybe override is ok
        return {'acknowledged': 'True', 'shards_acknowledged': 'True'}

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "include_defaults",
        "local",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_EXIST)
    def exists(self, index, params=None):
        """
        Return a boolean indicating whether given index exists.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html>`_

        :arg index: A comma-separated list of index names
        :arg allow_no_indices: Ignore if a wildcard expression resolves to no
            concrete indices (default: false)
        :arg expand_wildcards: Whether wildcard expressions should get expanded
            to open or closed indices (default: open), default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg flat_settings: Return settings in flat format (default: false)
        :arg ignore_unavailable: Ignore unavailable indexes (default: false)
        :arg include_defaults: Whether to return all default setting for each of
            the indices., default False
        :arg local: Return local information, do not retrieve the state from
            master node (default: false)
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)

        if len(searchable_indexes) > 1:
            raise ValueError("'index' contains more indexes - it cannot be list for now")

        return searchable_indexes[0] in self.parent.documents_dict


    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "timeout",
        "master_timeout",
        "request_timeout",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_DELETE)
    def delete(self, index, params=None):
        """
        Delete an index in Elasticsearch
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html>`_

        :arg index: A comma-separated list of indices to delete; use `_all` or
            `*` string to delete all indices
        :arg allow_no_indices: Ignore if a wildcard expression resolves to no
            concrete indices (default: false)
        :arg expand_wildcards: Whether wildcard expressions should get expanded
            to open or closed indices (default: open), default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg ignore_unavailable: Ignore unavailable indexes (default: false)
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        :arg request_timeout: Explicit operation timeout
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)

        if MockEsCommon.apply_all_indicies(WesDefs.OP_IND_DELETE, searchable_indexes):
            self.parent.documents_dict = {}
            return {'acknowledged': True}
        else:
            err_list = []
            first_idx = None
            for idx in searchable_indexes:
                if idx in self.parent.documents_dict:
                    del self.parent.documents_dict[idx]
                else:
                    if first_idx is None:
                        first_idx = idx
                    e = {'type': 'index_not_found_exception',
                         'reason': 'no such index',
                         'resource.type': 'index_or_alias',
                         'resource.id': idx,
                         'index_uuid': '_na_',
                         'index': idx}
                    err_list.append(e)

            if len(err_list) == 0:
                return {'acknowledged': True}
            else:
                self.parent.raiseNotFound(err_list, first_idx)

    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "flat_settings",
    #     "ignore_unavailable",
    #     "include_defaults",
    #     "local",
    #     "include_type_name",
    #     "master_timeout",)
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_GET)
    # def ind_get(self, index, feature=None, params=None):
    #     return self.es.indices.get(index, feature=feature, params=params)


    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "force",
        "ignore_unavailable",
        "wait_if_ongoing",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_FLUSH)
    def flush(self, index=None, params=None):
        """
        Explicitly flush one or more indices.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html>`_

        :arg index: A comma-separated list of index names; use `_all` or empty
            string for all indices
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg force: Whether a flush should be forced even if it is not
            necessarily needed ie. if no changes will be committed to the index.
            This is useful if transaction log IDs should be incremented even if
            no uncommitted changes are present. (This setting can be considered
            as internal)
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg wait_if_ongoing: If set to true the flush operation will block
            until the flush can be executed if another flush operation is
            already executing. The default is true. If set to false the flush
            will be skipped iff if another flush operation is already running.
        """
        return True

    @query_params("allow_no_indices",
                  "expand_wildcards",
                  "ignore_unavailable")
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_REFRESH)
    def refresh(self, index=None, params=None):
        """
        Explicitly refresh one or more index, making all operations performed
        since the last refresh available for search.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html>`_

        :arg index: A comma-separated list of index names; use `_all` or empty
            string to perform the operation on all indices
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        """
        return {'_shards': {'total': 2, 'successful': 2, 'failed': 0}}

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "local",
        "include_type_name",
        "master_timeout",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_GET_MAP)
    def get_mapping(self, index=None, doc_type=None, params=None):
        """
        Retrieve mapping definition of index or index/type.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html>`_

        :arg index: A comma-separated list of index names
        :arg doc_type: A comma-separated list of document types
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg local: Return local information, do not retrieve the state from
            master node (default: false)
        :arg include_type_name: Specify whether requests and responses should include a
            type name (default: depends on Elasticsearch version).
        :arg master_timeout: Specify timeout for connection to master
        """
        # if index in SKIP_IN_PATH:
        #     raise ValueError("Empty value passed for a required argument 'index'.")

        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)

        for_all = MockEsCommon.apply_all_indicies(WesDefs.OP_IND_GET_MAP, searchable_indexes)

        ret_dict = {}
        for idx in self.parent.documents_dict.keys():
            if for_all or idx in searchable_indexes:
                ret_dict[idx] = {
                    'mappings': MockEsCommon.get_idx_mappings(self, idx),
                    'settings': MockEsCommon.get_idx_settings(self, idx)
                }

        return ret_dict
    #
    # @query_params(
    #     "allow_no_indices",
    #     "expand_wildcards",
    #     "ignore_unavailable",
    #     "master_timeout",
    #     "timeout",
    #     "request_timeout",
    #     "include_type_name",)
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_PUT_MAP)
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
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_PUT_TMP)
    # def ind_put_template(self, name, body, params=None):
    #     return self.es.indices.put_template(name=name, body=body, params=params)
    #
    # @query_params("flat_settings",
    #               "local",
    #               "master_timeout",
    #               "include_type_name")
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_GET_TMP)
    # def ind_get_template(self, name=None, params=None):
    #     return self.es.indices.get_template(name=name, params=params)
    #
    # @query_params("allow_no_indices", "expand_wildcards", "ignore_unavailable", "local")
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_GET_ALIAS)
    # def ind_get_alias(self, index=None, name=None, params=None):
    #     return self.es.indices.get_alias(index=index, name=name, params=params)
    #
    # @query_params("master_timeout", "request_timeout", "timeout")
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_DEL_ALIAS)
    # def ind_delete_alias(self, index, name, params=None):
    #     return self.es.indices.delete_alias(index, name, params=params)

class MockEs(MockEsCommon):

    def __init__(self):
        super().__init__()
        self.indices = MockEsIndicesClient(self)

    #####################
    # general
    #####################
    @query_params()
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_GEN_PING)
    def ping(self, params=None):
        return True

    @query_params()
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_GEN_INFO)
    def info(self, params=None):
        return {
            'status': 200,
            'cluster_name': 'MockEs',
            'version':
                {
                    'lucene_version': 'pip list elasticsearch 7.0.2',
                    'build_hash': 'pip list elasticsearch 7.0.2',
                    'number': 'pip list elasticsearch 7.0.2',
                    'build_timestamp': 'pip list elasticsearch 7.0.2',
                    'build_snapshot': False
                },
            'name': 'Nightwatch',
            'tagline': 'You Know, for Search'
        }

    # #####################
    # # doc operations
    # #####################
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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_ADD_UP)
    def index(self, index, body, doc_type="_doc", id=None, params=None):
        # TODO petee 'id' is important for get - shouldn't be mandatory???
        # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
        if index not in self.documents_dict:
            self.indices.create(index,
                                # body=body, TODO can be mappings settings included???
                                params=params)

        docs = MockEsCommon.get_idx_docs(self, index)
        doc_found = docs.get(id, None)
        if id is None:
            id = get_random_id()


        version = doc_found['_version'] if doc_found else 1

        MockEsCommon.get_idx_docs(self, index)[id] = {
            '_type': doc_type,
            '_id': id,
            '_source': body,
            '_index': index,
            '_version': version,
        }

        return {
            '_type': doc_type,
            '_id': id,
            'created':  False if doc_found else True,
            '_version': version,
            '_index': index,
            'result': 'updated' if doc_found else 'created',
            '_shards': {'total': 2, 'successful': 1, 'failed': 0}  # keep hardcoded
        }

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
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_UPDATE)
    # def doc_update(self, index, id, doc_type="_doc", body=None, params=None):
    #     # TODO petee 'doc_type' is important for get - shouldn't be mandatory???
    #     return self.es.update(index, id, doc_type=doc_type, body=body, params=params)

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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_GET)
    def get(self, index, id, doc_type="_doc", params=None):
        """
        Get a typed JSON document from the index based on its id.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html>`_

        :arg index: The name of the index
        :arg id: The document ID
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg parent: The ID of the parent document
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg realtime: Specify whether to perform the operation in realtime or
            search mode
        :arg refresh: Refresh the shard containing the document before
            performing the operation
        :arg routing: Specific routing value
        :arg stored_fields: A comma-separated list of stored fields to return in
            the response
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'external', 'external_gte', 'force'
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        result = None

        docs = MockEsCommon.get_idx_docs(self, index)

        for doc_id in docs:
            doc = docs[doc_id]
            if doc.get('_id') == id:
                if doc_type == '_all':
                    result = doc
                    break
                else:
                    if doc.get('_type') == doc_type:
                        result = doc
                        break

        if result:
            result['found'] = True
        else:
            error_data = {
                '_index': index,
                '_type': doc_type,
                '_id': id,
                'found': False
            }
            self.raiseNotFound([error_data], index)

        return result


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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_EXIST)
    def exists(self, index, id, doc_type="_doc", params=None):
        """
        Returns a boolean indicating whether or not given document exists in Elasticsearch.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html>`_

        :arg index: The name of the index
        :arg id: The document ID
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg parent: The ID of the parent document
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg realtime: Specify whether to perform the operation in realtime or
            search mode
        :arg refresh: Refresh the shard containing the document before
            performing the operation
        :arg routing: Specific routing value
        :arg stored_fields: A comma-separated list of stored fields to return in
            the response
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'external', 'external_gte', 'force'
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        result = False
        ind_dict = MockEsCommon.get_dict(self)
        if index in ind_dict:
            docs = MockEsCommon.get_idx_docs(self, index)
            if id in docs and docs[id]['_type'] == doc_type:
                result = True
        return result


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
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_DEL)
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
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_DEL_QUERY)
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
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SEARCH)
    # def doc_search(self, index=None, body=None, params=None):
    #     return self.es.search(index=index, body=body, params=params)
    #

    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_BULK)
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

    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_BULK_STR)
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

    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SCAN)
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
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_COUNT)
    # def doc_count(self, doc_type=None, index=None, body=None, params=None):
    #     return self.es.count(doc_type=doc_type, index=index, body=body, params=params)

    # @query_params("scroll", "rest_total_hits_as_int", "scroll_id")
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SCROLL)
    # def doc_scroll(self, body=None, params=None):
    #     return self.es.scroll(body=body, params=params)

    # @query_params()
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SCROLL_CLEAR)
    # def doc_clear_scroll(self, scroll_id=None, body=None, params=None):
    #     return self.es.clear_scroll(scroll_id=scroll_id, body=body, params=params)

