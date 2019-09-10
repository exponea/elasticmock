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


import re
import json


from elasticsearch.client.utils import SKIP_IN_PATH
from elasticmock.utilities import get_random_id, get_random_scroll_id

from common import WesDefs, ExecCode
from log import Log

__all__ = ["MockEs"]

from mock_db import MockDb
from mock_query import MockEsQuery

class MockEsCommon:

    def __init__(self, version):
        self.ES_VERSION_RUNNING = version

    @staticmethod
    def raiseNotFoundIdx(e_list, idx):
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

    @staticmethod
    def raiseNotFoundDoc(e_dict):
        error_data = {'error': {'root_cause': e_dict}}
        raise NotFoundError(404, e_dict, json.dumps(e_dict))

    @staticmethod
    def raiseRequestError(e_list, type, idx):
        error_data = {'error': {'root_cause': e_list,
                                'type': type,
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
                        Log.log(f"{oper} is mock")
                        rc = fnc(self, *args, **kwargs)
                        self.db.db_db_dump(oper)
                        return rc
                return wrapper
            return wrapper_mk

    @staticmethod
    def apply_all_indicies(operation, indices: list):
        if operation == WesDefs.OP_IND_GET_MAP:
            return True if len(indices) == 0 else False
        elif operation == WesDefs.OP_IND_DELETE or operation == WesDefs.OP_IND_PUT_MAP:
            all_ind_values = ['_all', '*']
            for ind in indices:
                if ind in all_ind_values:
                    return True
            return False
        elif operation == WesDefs.OP_DOC_SEARCH or operation == WesDefs.OP_DOC_COUNT:
            all_ind_values = ['_all', '']
            for ind in indices:
                if ind in all_ind_values:
                    return True
            return False
        else:
            raise ValueError(f"{operation} no handling provided")

    @staticmethod
    def check_running_version(obj, version) -> bool:
        return MockEsCommon.get_parent(obj).ES_VERSION_RUNNING == version

    @staticmethod
    def get_parent(obj):
        return obj.parent if hasattr(obj, 'parent') else obj

    def query_helper(self, query, index=None, doc_type=None, body=None, params=None):
        if not index:
            index = "_all"
        searchable_indexes = self.db.normalize_index_to_list(index)

        if MockEsCommon.apply_all_indicies(query.q_oper, searchable_indexes):
            searchable_indexes = self.db.db_db_indices_dict_get().keys()

        matches = []
        docs = self.db.db_api_docs_all(searchable_indexes, doc_type)
        for docs_idx, docs_dtype, docs_id, docs_doc in docs:
            if query.q_exec_on_doc(None, docs_idx, docs_doc, query.q_query_name, query.q_query_rules):
                matches.append(docs_doc)

        return matches, searchable_indexes

    @staticmethod
    def expand_action(data):
        """
        From one document or action definition passed in by the user extract the
        action/data lines needed for elasticsearch's
        :meth:`~elasticsearch.Elasticsearch.bulk` api.
        """
        # when given a string, assume user wants to index raw json
        if isinstance(data, (str, bytes)):
            return '{"index":{}}', data

        # make sure we don't alter the action
        data = data.copy()
        op_type = data.pop("_op_type", "index")
        action = {op_type: {}}
        for key in (
                "_index",
                "_parent",
                "_percolate",
                "_routing",
                "_timestamp",
                "routing",
                "_type",
                "_version",
                "_version_type",
                "_id",
                "retry_on_conflict",
                "pipeline",
        ):
            if key in data:
                if key == "_routing":
                    action[op_type]["routing"] = data.pop(key)
                else:
                    action[op_type][key] = data.pop(key)

        # no data payload for delete
        if op_type == "delete":
            return action, None
        else:
            return action, data.get("_source", data)

    @staticmethod
    def bulk_streaming_result_repack_gen2list(rc: ExecCode) -> ExecCode:
        # MSE NOTES:
        # !!! returns generator    !!!
        # !!! can't iterate again  !!!
        # repack to 'list'
        data = []
        status = WesDefs.RC_OK

        dbg_bulk_int = True

        while True:
            try:
                item = next(rc.data)
                if dbg_bulk_int:
                    Log.log(f'BULK_STR_INT cont OK ... {str(item)}')
                data.append(item)
            except StopIteration as e:
                if dbg_bulk_int:
                    Log.err(f'BULK_STR_INT break {str(type(e))} === {str(e)}')
                break
            except BulkIndexError as e:
                if dbg_bulk_int:
                    Log.warn(f'BULK_STR_INT cont ERR ... {str(type(e))} === {str(e)}')
                status = WesDefs.RC_NOK
                data.append((False, e))

        return ExecCode(status, data, rc.fnc_params)

class MockEsIndicesClient:

    def __init__(self, parent):
        self.parent = parent
        self.db     = parent.db

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

        searchable_indexes = self.db.normalize_index_to_list(index)

        if len(searchable_indexes) > 1:
            raise ValueError("'index' contains more indexes - it cannot be list ")

        if self.db.db_idx_has(searchable_indexes[0]):
            MockEsCommon.raiseRequestError(['???'], 'index_already_exists_exception', searchable_indexes[0])
        else:
            self.db.db_idx_set(searchable_indexes[0], body)

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

        searchable_indexes = self.db.normalize_index_to_list( index)

        if len(searchable_indexes) > 1:
            raise ValueError("'index' contains more indexes - it cannot be list for now")

        return self.db.db_idx_has(searchable_indexes[0])


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

        searchable_indexes = self.db.normalize_index_to_list( index)

        if MockEsCommon.apply_all_indicies(WesDefs.OP_IND_DELETE, searchable_indexes):
            self.db.db_db_clear()
            return {'acknowledged': True}
        else:
            err_list = []
            first_idx = None
            for idx in searchable_indexes:
                if not self.db.db_idx_del(idx):
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
                MockEsCommon.raiseNotFoundIdx(err_list, first_idx)

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "include_defaults",
        "local",
        "include_type_name",
        "master_timeout",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_GET)
    def get(self, index, feature=None, params=None):
        """
        The get index API allows to retrieve information about one or more indexes.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-index.html>`_

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
        :arg include_type_name: Specify whether requests and responses should include a
            type name (default: depends on Elasticsearch version).
        :arg master_timeout: Specify timeout for connection to master
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        result = {}
        searchable_indexes = self.db.normalize_index_to_list(index)
        for search_idx in searchable_indexes:
            if self.db.db_idx_has(search_idx):
                mappings = { }
                dtype_dict = self.db.db_idx_field_dtype_dict_get(search_idx)
                for si_dtype in dtype_dict.keys():
                    mappings[si_dtype] = self.db.db_dtype_field_mapsprop_get(search_idx, si_dtype)

                result[search_idx] = {
                    'aliases': {},  # TODO
                    'mappings': mappings,
                    'settings': self.db.db_idx_field_settings_get(search_idx)
                }
            else:
                MockEsCommon.raiseNotFoundIdx(['not found'], index)
        return result

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
        # TODO check later
        # if index in SKIP_IN_PATH:
        #      raise ValueError("Empty value passed for a required argument 'index'.")

        if MockEsCommon.check_running_version(self, WesDefs.ES_VERSION_5_6_5):
            if doc_type and ('include_type_name' in params):
                MockEsCommon.raiseRequestError([f"illegal_argument_exception - request [/{index}/_mapping/{doc_type}] contains unrecognized parameter [include_type_name]"],
                                               "illegal_argument_exception",
                                               index)
        else:
            WesDefs.es_version_mismatch(MockEsCommon.get_parent(self).ES_VERSION_RUNNING)

        searchable_indexes = self.db.normalize_index_to_list( index)

        for_all = MockEsCommon.apply_all_indicies(WesDefs.OP_IND_GET_MAP, searchable_indexes)

        ret_dict = {}
        for idx in self.db.db_db_indices_dict_get().keys():
            if for_all or idx in searchable_indexes:
                ret_dict[idx] = {
                    'mappings': self.db.db_idx_field_mappings_get(idx),
                    'settings': self.db.db_idx_field_settings_get(idx)
                }

        return ret_dict

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        "master_timeout",
        "timeout",
        "request_timeout",
        "include_type_name",)
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_PUT_MAP)
    def put_mapping(self, body, doc_type=None, index=None, params=None):
        """
        Register specific mapping definition for a specific type.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html>`_

        :arg doc_type: The name of the document type
        :arg body: The mapping definition
        :arg index: A comma-separated list of index names the mapping should be
            added to (supports wildcards); use `_all` or omit to add the mapping
            on all indices.
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        :arg request_timeout: Explicit operation timeout (For pre 7.x ES Clusters)
        :arg include_type_name: Specify whether requests and responses should include a
            type name (default: depends on Elasticsearch version).
        """
        for param in (body,):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        if MockEsCommon.check_running_version(self, WesDefs.ES_VERSION_5_6_5):
            if doc_type and ('include_type_name' in params):
                MockEsCommon.raiseRequestError([f"illegal_argument_exception - request [/{index}/_mapping/{doc_type}] contains unrecognized parameter [include_type_name]"],
                                               "illegal_argument_exception", index)
        else:
            WesDefs.es_version_mismatch(MockEsCommon.get_parent(self).ES_VERSION_RUNNING)

        searchable_indexes = self.db.normalize_index_to_list( index)
        if MockEsCommon.apply_all_indicies(WesDefs.OP_IND_PUT_MAP, searchable_indexes):
            searchable_indexes = self.db.db_db_indices_dict_get().keys()

        for idx in searchable_indexes:
            if MockEsCommon.check_running_version(self, WesDefs.ES_VERSION_5_6_5):
                # TODO should be improved - test works but ...
                if self.db.db_api_docs_all_is_not_empty([idx], doc_type):
                    # 400 - {'error': {'root_cause': [{'type': 'illegal_argument_exception', 'reason': 'unknown setting [index.properties.city.fields.keyword.ignore_above] please check that any required plugins are installed, or check the breaking changes documentation for removed settings'}], 'type': 'illegal_argument_exception', 'reason': 'unknown setting [index.properties.city.fields.keyword.ignore_above] please check that any required plugins are installed, or check the breaking changes documentation for removed settings'}, 'status': 400} - illegal_argument_exception
                    # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.
                    MockEsCommon.raiseRequestError(['INTERNAL COMMON  can\'t change mapping and setting'], 'illegal_argument_exception', idx)
                else:
                    self.db.db_idx_field_mappings_set(idx, body)
            else:
                WesDefs.es_version_mismatch(MockEsCommon.get_parent(self).ES_VERSION_RUNNING)

        return {'acknowledged': True}

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
    @query_params("flat_settings",
                  "local",
                  "master_timeout",
                  "include_type_name")
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_IND_GET_TMP)
    def get_template(self, name=None, params=None):
        """
        Retrieve an index template by its name.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html>`_

        :arg name: The name of the template
        :arg flat_settings: Return settings in flat format (default: false)
        :arg local: Return local information, do not retrieve the state from
            master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection to master
            node
        :arg include_type_name: Specify whether requests and responses should include a
            type name (default: depends on Elasticsearch version).
        """
        if name:
            if self.db.db_tmpl_has(name):
                return self.db.db_tmpl_get(name)
            else:
                MockEsCommon.raiseNotFoundIdx(['template not found'], name)
        else:
            return self.db.db_db_templates_dict_get()
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
        super().__init__(WesDefs.ES_VERSION_DEFAULT)
        self.db = MockDb(self.ES_VERSION_RUNNING)

        # es specific objects
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

        if id is None:
            id = get_random_id()

        doc = self.db.db_dtype_field_doc_key_set(index, doc_type, id, body)

        version = doc['_version']
        return {
            '_type': doc['_type'],
            '_id': doc['_id'],
            'created':  True if version == 1 else False,
            '_version': version,
            '_index': doc['_index'],
            'result': 'created' if version == 1 else 'updated',
            '_shards': {'total': 2, 'successful': 1, 'failed': 0}  # keep hardcoded
        }

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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_UPDATE)
    def update(self, index, id, doc_type="_doc", body=None, params=None):
        """
        Update a document based on a script or partial data provided.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html>`_

        :arg index: The name of the index
        :arg id: Document ID
        :arg body: The request definition using either `script` or partial `doc`
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg fields: A comma-separated list of fields to return in the response
        :arg if_seq_no:
        :arg if_primary_term:
        :arg lang: The script language (default: painless)
        :arg parent: ID of the parent document. Is is only used for routing and
            when for the upsert request
        :arg refresh: If `true` then refresh the effected shards to make this
            operation visible to search, if `wait_for` then wait for a refresh
            to make this operation visible to search, if `false` (the default)
            then do nothing with refreshes., valid choices are: 'true', 'false',
            'wait_for'
        :arg retry_on_conflict: Specify how many times should the operation be
            retried when a conflict occurs (default: 0)
        :arg routing: Specific routing value
        :arg timeout: Explicit operation timeout
        :arg timestamp: Explicit timestamp for the document
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'force'
        :arg wait_for_active_shards: Sets the number of shard copies that must
            be active before proceeding with the update operation. Defaults to
            1, meaning the primary shard only. Set to `all` for all shard
            copies, otherwise set to any non-negative value less than or equal
            to the total number of copies for the shard (number of replicas + 1)
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        doc = self.db.db_dtype_field_doc_key_get(index, doc_type, id)
        if doc is None:
            error_data = {
                '_index': index,
                '_type': doc_type,
                '_id': id,
                'found': False
            }
            MockEsCommon.raiseNotFoundDoc(error_data)
        else:
            Log.warn3(f"----- {str(body)}")
            doc_data = body.get('doc', None)
            script_data = body.get('script', None)
            if (doc_data is None) and (script_data is None):
                MockEsCommon.raiseRequestError(["'doc' or 'script' not specified for update"],
                                                "'doc' or 'script' not specified for update", index)
            else:
                if isinstance(doc_data, dict):
                    doc_backup = doc.copy()
                    doc_backup.update(body['doc'])
                    if doc == doc_backup:
                        result_str = 'noop'
                    else:
                        doc_backup['_version'] = doc_backup['_version'] + 1
                        doc = self.db.db_dtype_field_doc_key_set(index, doc_type, id, doc_backup)
                        result_str = 'updated'

                    return {
                        '_type': doc['_type'],
                        '_id': doc['_id'],
                        # 'created':  True if version == 1 else False,
                        '_version': doc['_version'],
                        '_index': doc['_index'],
                        'result': result_str,
                        '_shards': {'total': 2, 'successful': 1, 'failed': 0}  # keep hardcoded
                        }
                else:
                    raise ValueError("'script' not supported not")


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

        result = self.db.db_dtype_field_doc_key_get(index, doc_type, id)
        if result:
            result['found'] = True
        else:
            error_data = {
                '_index': index,
                '_type': doc_type,
                '_id': id,
                'found': False
            }
            MockEsCommon.raiseNotFoundDoc(error_data)

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

        return self.db.db_dtype_field_doc_key_has(index, doc_type, id)

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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_DEL)
    def delete(self, index, id, doc_type="_doc", params=None):
        """
        Delete a typed JSON document from a specific index based on its id.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html>`_

        :arg index: The name of the index
        :arg id: The document ID
        :arg if_primary_term: only perform the delete operation if the last
            operation that has changed the document has the specified primary
            term
        :arg if_seq_no: only perform the delete operation if the last operation
            that has changed the document has the specified sequence number
        :arg parent: ID of parent document
        :arg refresh: If `true` then refresh the effected shards to make this
            operation visible to search, if `wait_for` then wait for a refresh
            to make this operation visible to search, if `false` (the default)
            then do nothing with refreshes., valid choices are: 'true', 'false',
            'wait_for'
        :arg routing: Specific routing value
        :arg timeout: Explicit operation timeout
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'external', 'external_gte', 'force'
        :arg wait_for_active_shards: Sets the number of shard copies that must
            be active before proceeding with the delete operation. Defaults to
            1, meaning the primary shard only. Set to `all` for all shard
            copies, otherwise set to any non-negative value less than or equal
            to the total number of copies for the shard (number of replicas + 1)
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        doc = self.db.db_dtype_field_doc_key_get(index, doc_type, id)
        result = {'found': True if doc else False,
                  '_index': doc['_index'] if doc else index,
                  '_type': doc['_type'] if doc else doc_type,
                  '_id': doc['_id'] if doc else id
                  }
        if doc:
            result['_version'] = doc['_version']
            result['result']   =  'deleted'
            result['_shards']  = {'total': 2, 'successful': 1, 'failed': 0}
            self.db.db_dtype_field_doc_key_del(index, doc_type, id)
            return result
        else:
            MockEsCommon.raiseNotFoundDoc(result)

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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SEARCH)
    def search(self, index=None, body=None, params=None):
        """
        Execute a search query and get back search hits that match the query.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html>`_

        :arg index: A list of index names to search, or a string containing a
            comma-separated list of index names to search; use `_all`
            or empty string to perform the operation on all indices
        :arg body: The search definition using the Query DSL
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg allow_partial_search_results: Set to false to return an overall
            failure if the request would produce partial results. Defaults to
            True, which will allow partial results in the case of timeouts or
            partial failures
        :arg analyze_wildcard: Specify whether wildcard and prefix queries
            should be analyzed (default: false)
        :arg analyzer: The analyzer to use for the query string
        :arg batched_reduce_size: The number of shard results that should be
            reduced at once on the coordinating node. This value should be used
            as a protection mechanism to reduce the memory overhead per search
            request if the potential number of shards in the request can be
            large., default 512
        :arg ccs_minimize_roundtrips: Indicates whether network round-trips
            should be minimized as part of cross-cluster search requests
            execution, default 'true'
        :arg default_operator: The default operator for query string query (AND
            or OR), default 'OR', valid choices are: 'AND', 'OR'
        :arg df: The field to use as default where no field prefix is given in
            the query string
        :arg docvalue_fields: A comma-separated list of fields to return as the
            docvalue representation of a field for each hit
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg explain: Specify whether to return detailed information about score
            computation as part of a hit
        :arg from\\_: Starting offset (default: 0)
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg lenient: Specify whether format-based query failures (such as
            providing text to a numeric field) should be ignored
        :arg max_concurrent_shard_requests: The number of concurrent shard
            requests this search executes concurrently. This value should be
            used to limit the impact of the search on the cluster in order to
            limit the number of concurrent shard requests, default 'The default
            grows with the number of nodes in the cluster but is at most 256.'
        :arg pre_filter_shard_size: A threshold that enforces a pre-filter
            roundtrip to prefilter search shards based on query rewriting if
            the number of shards the search request expands to exceeds the
            threshold. This filter roundtrip can limit the number of shards
            significantly if for instance a shard can not match any documents
            based on it's rewrite method ie. if date filters are mandatory to
            match but the shard bounds and the query are disjoint., default 128
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg rest_total_hits_as_int: This parameter is used to restore the total hits as a number
            in the response. This param is added version 6.x to handle mixed cluster queries where nodes
            are in multiple versions (7.0 and 6.latest)
        :arg request_cache: Specify if request cache should be used for this
            request or not, defaults to index level setting
        :arg routing: A comma-separated list of specific routing values
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        :arg search_type: Search operation type, valid choices are:
            'query_then_fetch', 'dfs_query_then_fetch'
        :arg size: Number of hits to return (default: 10)
        :arg sort: A comma-separated list of <field>:<direction> pairs
        :arg stats: Specific 'tag' of the request for logging and statistical
            purposes
        :arg stored_fields: A comma-separated list of stored fields to return as
            part of a hit
        :arg suggest_field: Specify which field to use for suggestions
        :arg suggest_mode: Specify suggest mode, default 'missing', valid
            choices are: 'missing', 'popular', 'always'
        :arg suggest_size: How many suggestions to return in response
        :arg suggest_text: The source text for which the suggestions should be
            returned
        :arg terminate_after: The maximum number of documents to collect for
            each shard, upon reaching which the query execution will terminate
            early.
        :arg timeout: Explicit operation timeout
        :arg track_scores: Whether to calculate and return scores even if they
            are not used for sorting
        :arg track_total_hits: Indicate if the number of documents that match
            the query should be tracked
        :arg typed_keys: Specify whether aggregation and suggester names should
            be prefixed by their respective types in the response
        :arg version: Specify whether to return document version as part of a
            hit
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        query = MockEsQuery(WesDefs.OP_DOC_SEARCH, body)
        matches, searchable_indexes = self.query_helper(query,
                                                        index=index,
                                                        doc_type=None,  # search API does not have???
                                                        body=body,
                                                        params=params)

        result = {
            'hits': {
                'total': len(matches),
                'max_score': 1.0
            },
            '_shards': {
                # Simulate indexes with 1 shard each
                'successful': len(searchable_indexes),
                'failed': 0,
                'total': len(searchable_indexes)
            },
            'took': 1,
            'timed_out': False
        }

        hits = []
        for counter, match in enumerate(matches):
            # print(counter, " --- BEFORE: size", query.q_size, " from ", query.q_from,  match)
            if query.q_size and len(hits) >= query.q_size:
                break
            if query.q_from and counter < query.q_from:
                continue
            # print(counter, " --- AFTER  ", match)
            match['_score'] = 1.0
            hits.append(match)

        # build aggregations
        if query.q_aggs:
            aggregations = {}

            for aggregation, definition in body['aggs'].items():
                aggregations[aggregation] = {
                    # "doc_count_error_upper_bound": 0,
                    # "sum_other_doc_count": 0,
                    "buckets": []
                }

            if aggregations:
                result['aggregations'] = aggregations

        if 'scroll' in params:
            result['_scroll_id'] = str(get_random_scroll_id())
            params['size'] = int(params.get('size') if 'size' in params else 10)
            params['from'] = int(params.get('from') + params.get('size') if 'from' in params else 0)
            self.__scrolls[result.get('_scroll_id')] = {
                'index': index,
                #'doc_type': doc_type, TODO MSE
                'body': body,
                'params': params
            }
            hits = hits[params.get('from'):params.get('from') + params.get('size')]

        result['hits']['hits'] = hits

        return result

    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_BULK)
    def bulk(self, actions, stats_only=False, *args, **kwargs):
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
        """
        Helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
        a more human friendly interface - it consumes an iterator of actions and
        sends them to elasticsearch in chunks. It returns a tuple with summary
        information - number of successfully executed actions and either list of
        errors or number of errors if ``stats_only`` is set to ``True``. Note that
        by default we raise a ``BulkIndexError`` when we encounter an error so
        options like ``stats_only`` only apply when ``raise_on_error`` is set to
        ``False``.

        When errors are being collected original document data is included in the
        error dictionary which can lead to an extra high memory usage. If you need
        to process a lot of data and want to ignore/collect errors please consider
        using the :func:`~elasticsearch.helpers.streaming_bulk` helper which will
        just return the errors and not store them in memory.


        :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
        :arg actions: iterator containing the actions
        :arg stats_only: if `True` only report number of successful/failed
            operations instead of just number of successful and a list of error responses

        Any additional keyword arguments will be passed to
        :func:`~elasticsearch.helpers.streaming_bulk` which is used to execute
        the operation, see :func:`~elasticsearch.helpers.streaming_bulk` for more
        accepted parameters.
        """
        success, failed = 0, 0

        # list of errors to be collected is not stats_only
        errors = []

        # make streaming_bulk yield successful results so we can count them
        kwargs["yield_ok"] = True

        rc = ExecCode(WesDefs.RC_OK,
                      self.bulk_streaming(actions, *args, **kwargs),
                      ('Fake', 'Fake'))
        rc = MockEs.bulk_streaming_result_repack_gen2list(rc)
        for ok, item in rc.data:
            # go through request-response pairs and detect failures
            if not ok:
                if not stats_only:
                    errors.append(item)
                failed += 1
            else:
                success += 1

        rc = success, failed if stats_only else errors

        return rc

    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_BULK_STR)
    def bulk_streaming(self,
                       actions,
                       chunk_size=500,
                       max_chunk_bytes=100 * 1024 * 1024,
                       raise_on_error=True,
                       expand_action_callback=MockEsCommon.expand_action,
                       raise_on_exception=True,
                       max_retries=0,
                       initial_backoff=2,
                       max_backoff=600,
                       yield_ok=True,
                       *args,
                       **kwargs):
        """
        Streaming bulk consumes actions from the iterable passed in and yields
        results per action. For non-streaming usecases use
        :func:`~elasticsearch.helpers.bulk` which is a wrapper around streaming
        bulk that returns summary information about the bulk operation once the
        entire input is consumed and sent.

        If you specify ``max_retries`` it will also retry any documents that were
        rejected with a ``429`` status code. To do this it will wait (**by calling
        time.sleep which will block**) for ``initial_backoff`` seconds and then,
        every subsequent rejection for the same chunk, for double the time every
        time up to ``max_backoff`` seconds.

        :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
        :arg actions: iterable containing the actions to be executed
        :arg chunk_size: number of docs in one chunk sent to es (default: 500)
        :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
        :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`)
            from the execution of the last chunk when some occur. By default we raise.
        :arg raise_on_exception: if ``False`` then don't propagate exceptions from
            call to ``bulk`` and just report the items that failed as failed.
        :arg expand_action_callback: callback executed on each action passed in,
            should return a tuple containing the action line and the data line
            (`None` if data line should be omitted).
        :arg max_retries: maximum number of times a document will be retried when
            ``429`` is received, set to 0 (default) for no retries on ``429``
        :arg initial_backoff: number of seconds we should wait before the first
            retry. Any subsequent retries will be powers of ``initial_backoff *
            2**retry_number``
        :arg max_backoff: maximum number of seconds a retry will wait
        :arg yield_ok: if set to False will skip successful documents in the output
        """

        dbg_level = False

        actions = list(map(expand_action_callback, actions))
        result_ok = []
        result_nok = []

        for item in actions:
            meta, data = item

            mi = meta.get('index', None)
            md = meta.get('delete', None)
            mc = meta.get('create', None)
            mu = meta.get('update', None)

            m_index  = None
            m_type   = None
            m_id     = None
            m_doc    = data
            for m in [mi, md, mc, mu]:
                if m:
                    m_index = m.get('_index', None)
                    m_type  = m.get('_type', None)
                    m_id    = m.get('_id', None)
                    break

            if dbg_level:
                Log.log(f"{WesDefs.OP_DOC_BULK_STR} mi({mi}) md({md}) mc({mc}) mu({mu}) - {m_doc}")

            if mi or md or mc or mu:
                if mi or mc:
                    if mc:
                        doc_create = self.db.db_dtype_field_doc_key_get(m_index, m_type, m_id)
                        if doc_create:
                            result_nok.append({'create': {'_index': mc['_index'],
                                                          '_type': mc['_type'],
                                                          '_id': mc['_id'],
                                                          'status': 409,
                                                          'error': {
                                                              'type': 'version_conflict_engine_exception',
                                                              'reason': f"[{mc['_type']}] [{mc['_id']}]: version conflict, document already exists (current version [{doc_create['_version']}]",
                                                              'index_uuid': '????',
                                                              'shard': '3', 'index': f"{m_index}"
                                                          },
                                                          'data': f"{m_doc}"}})
                            continue
                    try:
                        rc_index = self.index(m_index, m_doc, doc_type=m_type, id=m_id)
                        if isinstance(rc_index, dict):
                            result_ok.append((True, rc_index))
                        else:
                            ValueError("'delete' return wrong type format")
                    except Exception as e:
                            result_nok.append(str(e))
                elif md:
                    try:
                        rc_delete = self.delete(m_index, m_id, doc_type=m_type)
                        if isinstance(rc_delete, dict):
                            result_ok.append((True, rc_delete))
                        else:
                            ValueError("'delete' return wrong type format")
                    except Exception as e:
                            result_nok.append(str(e))
                elif mu:
                    try:
                        rc_update = self.update(m_index, m_id, doc_type=m_type, body=m_doc)
                        if isinstance(rc_update, dict):
                            result_ok.append((True, rc_update))
                        else:
                            ValueError("'delete' return wrong type format")
                    except Exception as e:
                            result_nok.append(str(e))
            else:
                ValueError('unsupported data in action')

        len_ok = len(result_ok)
        len_nok = len(result_nok)
        if len_nok:
            result_ok.append((False, result_nok))

        if dbg_level:
            Log.log(f"{WesDefs.OP_DOC_BULK_STR} RESULT ok({len_ok}) nok({len_nok})")

        # !!! it returns generator !!!
        def gen_result(data_list):
            for state, value in data_list:
                if state:
                    yield (state, value)
                else:
                    raise BulkIndexError(value)

        return gen_result(result_ok)


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
    @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_COUNT)
    def count(self, doc_type=None, index=None, body=None, params=None):
        """
        Execute a query and get the number of matches for that query.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html>`_

        :arg index: A list of index names or a string containing a
            comma-separated list of index names to restrict the results to
        :arg body: A query to restrict the results specified with the Query DSL
            (optional)
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg analyze_wildcard: Specify whether wildcard and prefix queries
            should be analyzed (default: false)
        :arg analyzer: The analyzer to use for the query string
        :arg default_operator: The default operator for query string query (AND
            or OR), default 'OR', valid choices are: 'AND', 'OR'
        :arg df: The field to use as default where no field prefix is given in
            the query string
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg ignore_throttled: Whether specified concrete, expanded or aliased
            indices should be ignored when throttled
        :arg lenient: Specify whether format-based query failures (such as
            providing text to a numeric field) should be ignored
        :arg min_score: Include only documents with a specific `_score` value in
            the result
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg routing: Specific routing value
        """

        if body and "from" in body:
            MockEsCommon.raiseRequestError(['request does not support [from]'],
                                           'request does not support [from]', index)

        if body and "size" in body:
            MockEsCommon.raiseRequestError(['request does not support [size]'],
                                           'request does not support [size]', index)


        query = MockEsQuery(WesDefs.OP_DOC_COUNT, body)
        matches, searchable_indexes = self.query_helper(query,
                                                        index=index,
                                                        doc_type=doc_type,  # search API does not have???
                                                        body=body,
                                                        params=params)
        result = {
            'count': len(matches),
            '_shards': {
                'successful': 1,
                'failed': 0,
                'total': 1
            }
        }

        return result

    # @query_params("scroll", "rest_total_hits_as_int", "scroll_id")
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SCROLL)
    # def doc_scroll(self, body=None, params=None):
    #     return self.es.scroll(body=body, params=params)

    # @query_params()
    # @MockEsCommon.Decor.operation_mock(WesDefs.OP_DOC_SCROLL_CLEAR)
    # def doc_clear_scroll(self, scroll_id=None, body=None, params=None):
    #     return self.es.clear_scroll(scroll_id=scroll_id, body=body, params=params)

