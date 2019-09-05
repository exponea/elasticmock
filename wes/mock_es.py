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

import datetime
import re
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

class MockEsQuery:

    def __init__(self, operation, body):
        self.q_oper = operation
        self.q_query_rules = None
        self.q_size = None
        self.q_from = None
        self.q_query = None
        self.q_query_name = None
        self.q_level = 0

        self.parser(body)

    def parser(self, body):

        if body:
            self.q_size = body.get('size', None)
            self.q_from = body.get('from', None)
            self.q_query = body.get('query', None)

            if not self.q_query:
                raise ValueError("'query' missing in body")

            self.q_query_name = list(self.q_query.keys())

            if len(self.q_query_name) != 1:
                raise ValueError(f"body contains more queries {self.q_query_name}")

            self.q_query_name = self.q_query_name[0]
            self.q_query_rules = self.q_query[self.q_query_name]

        else:
            self.q_query_name = "match_all"
            self.q_query_rules = {}

        Log.log(f"{self.q_oper} Q_NAME: {self.q_query_name} - Q_RULES: {self.q_query_rules}")

    def get_indentation_string(self):
        return f"{self.q_oper} {'--'*self.q_level}>"


    def q_exec_on_doc(self, prefix, db_idx, db_doc, fnc_name, data_to_match) -> bool:
        self.q_level += 1
        q_xxx_fnc = self.q_mapper(fnc_name)
        rc = q_xxx_fnc(self, prefix, db_idx, db_doc, data_to_match)
        if self.q_level == 1:
            Log.log(f"{self.get_indentation_string()} {'MATCH' if rc else 'MISS '}  >>> idx:{db_idx} dos:{str(db_doc)}")
        self.q_level -= 1
        return rc

    def q_match_all(self, prefix, db_idx, db_doc, data_to_match) -> bool:
        rc = True
        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: match - RC-[{rc}]")
        return rc

    def q_match(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            val_in_query = data_to_match[feature].lower()

            res = (val_in_query == word.lower() for word in val_in_doc.split())
            if True not in res:
                rc = False

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: match - RC-[{rc}]")
        return rc

    def q_match_phrase(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            val_in_doc = val_in_doc.lower()
            val_in_query = data_to_match[feature].lower()

            # MSE_NOTE: 'in 'does not work for WHOLE WORDS
            # 'if val_in_query not in val_in_doc:'
            if re.search(r"\b{}\b".format(val_in_query), val_in_doc, re.IGNORECASE) is None:
                rc = False
                break

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: match_phrase - RC-[{rc}]")
        return rc

    def q_term(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            # MSE_NOTES: MATCH(exact BUT lookup is stored with striped WHITESPACES)
            val_in_doc = val_in_doc.lower().strip()
            val_in_query = data_to_match[feature].lower()

            if not val_in_doc == val_in_query:
                rc = False
                break

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: term - RC-[{rc}]")

        return rc

    def q_regexp(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            val_in_query = data_to_match[feature]

            if re.search(val_in_query, val_in_doc) is None:
                rc = False
                break

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: regexp - RC-[{rc}]")

        return rc

    def q_bool_helper(self, rules: tuple, data_to_match, db_idx, db_doc) -> bool:

        rules_name, rules_continue_fnc = rules
        rules_dict = data_to_match.get(rules_name, None)
        rc = None
        if rules_dict:
            rc = True
            for fnc_name in rules_dict.keys():
                data_to_match2 = rules_dict[fnc_name]
                rc_partial = self.q_exec_on_doc(rules_name, db_idx, db_doc, fnc_name, data_to_match2)
                keep_going = rules_continue_fnc(rc_partial)
                Log.log(f"{self.get_indentation_string()} Q_NAME: bool[{rules_name:8}] RC[{keep_going}]")
                if keep_going:
                    pass
                else:
                    rc = False
                    break
        return rc


    def q_bool(self, prefix, db_idx, db_doc, data_to_match):

        rc_must     = self.q_bool_helper(('must'    , lambda rc: rc), data_to_match, db_idx, db_doc)
        rc_filter   = self.q_bool_helper(('filter'  , lambda rc: rc), data_to_match, db_idx, db_doc)
        rc_must_not = self.q_bool_helper(('must_not', lambda rc: rc is False), data_to_match, db_idx, db_doc)
        rc_should   = self.q_bool_helper(('should'  , lambda rc: rc), data_to_match, db_idx, db_doc)

        res = f"{self.get_indentation_string()} Q_NAME: bool[rc_must({rc_must}) rc_filter({rc_filter}) rc_must_not({rc_must_not}) rc_should({rc_should})]"

        if rc_must_not is False:
            Log.log(f"{res} CASE(1)")
            return False

        if rc_must or rc_filter or rc_should:
            Log.log(f"{res} CASE(2)")
            return True

        Log.log(f"{res} CASE(3)")
        return False

    def q_mapper(self, query: str):
        q_mapper = {
            "match_all":    MockEsQuery.q_match_all,
            "match":        MockEsQuery.q_match,
            "match_phrase": MockEsQuery.q_match_phrase,
            "term":         MockEsQuery.q_term,
            "regexp":       MockEsQuery.q_regexp,
            "bool":         MockEsQuery.q_bool,
        }

        rc = q_mapper.get(query, None)
        if not rc:
            raise ValueError(f"not implemented {query}")
        return rc

class MockEsMeta:

    K_IDX_DOCS = 'idx_docs'
    K_IDX_DT   = 'idx_doc_types'
    K_DT_DOCS  = 'dt_docs'
    K_DT_MAPS  = 'dt_mappings'
    K_DT_SETS  = 'dt_settings'

    def __init__(self):
        # db = {
        #     'INDEX_1': {
        #         MockEsMeta.K_IDX_DOCS: {
        #             'id_doc11': ['doc_type_11',],
        #             'id_doc12': ['doc_type_11', 'doc_type_13'],
        #             'id_doc13': ['doc_type_11',],
        #         },
        #         MockEsMeta.K_IDX_DT: {
        #             'doc_type_11': {
        #                     MockEsMeta.K_DT_DOCS: { 'id_doc11': doc11, 'id_doc12': doc12_a },
        #                     MockEsMeta.K_DT_MAPS = 'doc_type_11_mappings',
        #                     MockEsMeta.K_DT_SETS = 'doc_type_11_settings',
        #             },
        #             'doc_type_12': {
        #                     MockEsMeta.K_DT_DOCS: {'id_doc13': doc13},
        #                     MockEsMeta.K_DT_MAPS = 'doc_type_12_mappings',
        #                     MockEsMeta.K_DT_SETS = 'doc_type_12_settings',
        #             }
        #             'doc_type_13': {
        #                     MockEsMeta.K_DT_DOCS: {'id_doc13': doc13, 'id_doc12': doc12_b},
        #                     MockEsMeta.K_DT_MAPS = 'doc_type_13_mappings',
        #                     MockEsMeta.K_DT_SETS = 'doc_type_13_settings',
        #             }
        #         }
        #     }
        # }
        self.documents_dict = {}
        self.scrolls = {}

    @staticmethod
    def meta_set_idx_mappings_settings(obj, idx, mappings_settings):
        MockEsCommon.meta_set_idx_mappings(obj, idx, mappings_settings['mappings'])
        MockEsCommon.meta_set_idx_settings(obj, idx, mappings_settings['settings'])

    @staticmethod
    def meta_set_idx_mappings(obj, idx, mappings):
        MockEsCommon.meta_db_get(obj)[idx]['mappings'] = mappings

    @staticmethod
    def meta_set_idx_settings(obj, idx, settings):
        MockEsCommon.meta_db_get(obj)[idx]['settings'] = settings

    @staticmethod
    def meta_db_idx_get_mappings(obj, idx):
        return MockEsCommon.meta_db_get(obj)[idx]['mappings']

    @staticmethod
    def meta_db_idx_get_settings(obj, idx):
        return MockEsCommon.meta_db_get(obj)[idx]['settings']

    # L1
    @staticmethod
    def meta_db_idx_get_docs(obj, idx):
        return MockEsCommon.meta_db_get(obj)[idx][MockEsMeta.K_IDX_DOCS]

    # L0
    @staticmethod
    def meta_db_get_indexes(obj):
        return MockEsCommon.meta_db_get(obj).keys()

    def meta_db_has_idx(obj, idx) -> bool:
        db = MockEsCommon.meta_db_get(obj)
        return True if idx in db else False

    def meta_db_set_idx(obj, idx, mappings, settings):
         MockEsCommon.meta_db_get(obj)[idx] = {
             MockEsMeta.K_IDX_DOCS: {},
             'mappings': mappings,
             'settings': settings,
         }

    def meta_db_del_idx(obj, idx) -> bool:
        if MockEsCommon.meta_db_has_idx(obj, idx):
            del MockEsCommon.meta_db_get(obj)[idx]
            return True
        else:
            return False

    @staticmethod
    def meta_db_clear(obj):
        MockEsCommon.meta_db_get(obj).clear()

    @staticmethod
    def meta_db_get(obj):
        parent = MockEsCommon.get_parent(obj)
        return parent.documents_dict

    # helper
    @staticmethod
    def get_parent(obj):
        return obj.parent if hasattr(obj, 'parent') else obj


class MockEsCommon(MockEsMeta):

    def __init__(self):
        super().__init__()
        self.ES_VERSION_RUNNING = WesDefs.ES_VERSION_DEFAULT

    @staticmethod
    def raiseNotFound(e_list, idx):
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
                        MockEsCommon.dump_dict(oper, self)
                        return rc
                return wrapper
            return wrapper_mk

    @staticmethod
    def dump_dict(oper, obj):
        dict_print = ''
        for index in MockEsCommon.meta_db_get_indexes(obj):

            dict_print += '\n' + str(index) + ' IND SETTS: '
            setts = MockEsCommon.meta_db_idx_get_settings(obj, index)
            for s in setts:
                dict_print += '\n' + str(setts[s])

            dict_print += '\n' + str(index) + ' IND MAPS: '
            idx_maps = MockEsCommon.meta_db_idx_get_mappings(obj, index)
            for idx_map in idx_maps:
                fields = idx_maps[idx_map]
                for field in fields:
                    dict_print += '\n' + '{:>12}: '.format(str(field)) + str(fields[field])

            dict_print += '\n' + str(index) + ' IND DOCS: '
            docs = MockEsCommon.meta_db_idx_get_docs(obj, index)
            for id in docs:
                dict_print += '\n' + str(docs[id])

        Log.notice(f"{oper} is mock {dict_print}")

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
        elif operation == WesDefs.OP_DOC_SEARCH:
            all_ind_values = ['_all', '']
            for ind in indices:
                if ind in all_ind_values:
                    return True
            return False
        else:
            raise ValueError(f"{operation} no handling provided")

    @staticmethod
    def check_running_version(obj, version) -> bool:
        return  MockEsCommon.get_parent(obj).ES_VERSION_RUNNING == version

    @staticmethod
    def normalize_index_to_list(obj, index):
        # Ensure to have a list of index
        if index is None:
            searchable_indexes = MockEsCommon.meta_db_get_indexes(obj)
        elif isinstance(index, str) or isinstance(index, unicode):
            searchable_indexes = [index]
        elif isinstance(index, list):
            searchable_indexes = index
        else:
            # Is it the correct exception to use ?
            raise ValueError("Invalid param 'index'")

        # # Check index(es) exists
        # for searchable_index in searchable_indexes:
        #     if searchable_index not in MockEsCommon.meta_db_get_indexes(self):
        #         raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(searchable_index))

        return searchable_indexes

    @staticmethod
    def mappings_properties_from_doc_body(doc_body) -> dict:
        mapping_dict = {
            'str': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
            'int': {'type': 'long'},
            'float': {'type': 'float'},
            'datetime': {'type': 'date', 'format': 'yyyy,MM,dd,hh,mm,ss'}
        }

        properties = {}
        for field in doc_body:
            data = doc_body[field]
            # print('MAPPINGS -->', field, type(data), doc_body[field])
            if isinstance(data, int):
                properties[field] = mapping_dict['int']
            elif isinstance(data, float):
                properties[field] = mapping_dict['float']
            elif isinstance(data, str):
                properties[field] = mapping_dict['str']
            elif isinstance(data, datetime.datetime):
                properties[field] = mapping_dict['datetime']
            else:
                raise ValueError(f"{type(data)} is not handled")

        #print('MAPPINGS -->', properties)

        return {"properties": properties}

    @staticmethod
    def mappings_settings_build_from_doc_body_data(doc_body) -> dict:
        create_body = {
            'mappings': MockEsCommon.mappings_properties_from_doc_body(doc_body),
            'settings': {}
        }

        Log.log(f"MAPS_SETS --> {create_body}")
        return create_body


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

        if searchable_indexes[0] in MockEsCommon.meta_db_get_indexes(self):
            MockEsCommon.raiseRequestError(['???'], 'index_already_exists_exception', searchable_indexes[0])
        else:
            MockEsCommon.meta_db_set_idx(self, searchable_indexes[0], mappings, settings)

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

        return searchable_indexes[0] in MockEsCommon.meta_db_get_indexes(self)


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
            MockEsCommon.meta_db_clear(self)
            return {'acknowledged': True}
        else:
            err_list = []
            first_idx = None
            for idx in searchable_indexes:
                if not MockEsCommon.meta_db_del_idx(self, idx):
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
                MockEsCommon.raiseNotFound(err_list, first_idx)

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

        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)

        for_all = MockEsCommon.apply_all_indicies(WesDefs.OP_IND_GET_MAP, searchable_indexes)

        ret_dict = {}
        for idx in MockEsCommon.meta_db_get_indexes(self):
            if for_all or idx in searchable_indexes:
                ret_dict[idx] = {
                    'mappings': MockEsCommon.meta_db_idx_get_mappings(self, idx),
                    'settings': MockEsCommon.meta_db_idx_get_settings(self, idx)
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

        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)
        if MockEsCommon.apply_all_indicies(WesDefs.OP_IND_PUT_MAP, searchable_indexes):
            searchable_indexes = MockEsCommon.meta_db_get_indexes(self)

        for idx in searchable_indexes:
            if MockEsCommon.check_running_version(self, WesDefs.ES_VERSION_5_6_5):
                if MockEsCommon.meta_db_idx_get_docs(self, idx):
                    # TODO should be improved
                    # 400 - {'error': {'root_cause': [{'type': 'illegal_argument_exception', 'reason': 'unknown setting [index.properties.city.fields.keyword.ignore_above] please check that any required plugins are installed, or check the breaking changes documentation for removed settings'}], 'type': 'illegal_argument_exception', 'reason': 'unknown setting [index.properties.city.fields.keyword.ignore_above] please check that any required plugins are installed, or check the breaking changes documentation for removed settings'}, 'status': 400} - illegal_argument_exception
                    # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.
                    MockEsCommon.raiseRequestError(['INTERNAL COMMON  can\'t change mapping and setting'], 'illegal_argument_exception', idx)
                else:
                    MockEsCommon.meta_set_idx_mappings(self, idx, body)
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

        if index not in MockEsCommon.meta_db_get_indexes(self):
            self.indices.create(index,
                                body=MockEsCommon.mappings_settings_build_from_doc_body_data(body),
                                params=params)
        else:
            # change only if empty
            if len(MockEsCommon.meta_db_idx_get_docs(self, index)) == 0 and \
               len(MockEsCommon.meta_db_idx_get_mappings(self, index)) == 0:
                MockEsCommon.meta_set_idx_mappings_settings(self, index, MockEsCommon.mappings_settings_build_from_doc_body_data(body))

            # TODO settings ???

        docs = MockEsCommon.meta_db_idx_get_docs(self, index)
        doc_found = docs.get(id, None)
        if id is None:
            id = get_random_id()


        version = doc_found['_version'] if doc_found else 1

        MockEsCommon.meta_db_idx_get_docs(self, index)[id] = {
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

        docs = MockEsCommon.meta_db_idx_get_docs(self, index)

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
            MockEsCommon.raiseNotFound([error_data], index)

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
        if index in MockEsCommon.meta_db_get_indexes(self):
            docs = MockEsCommon.meta_db_idx_get_docs(self, index)
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

        if not index:
            index = "_all"
        searchable_indexes = MockEsCommon.normalize_index_to_list(self, index)

        if MockEsCommon.apply_all_indicies(WesDefs.OP_DOC_SEARCH, searchable_indexes):
            searchable_indexes = MockEsCommon.meta_db_get_indexes(self)

        matches = []
        query = MockEsQuery(WesDefs.OP_DOC_SEARCH, body)

        for search_idx in searchable_indexes:
            docs = MockEsCommon.meta_db_idx_get_docs(self, search_idx)
            for doc_id in docs:
                doc = docs[doc_id]
                if query.q_exec_on_doc(None, search_idx, doc, query.q_query_name, query.q_query_rules):
                    matches.append(doc)

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
        if body is not None and 'aggs' in body:
            aggregations = {}

            for aggregation, definition in body['aggs'].items():
                aggregations[aggregation] = {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
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

