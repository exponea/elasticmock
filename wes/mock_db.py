from log import Log
from common import WesDefs, QueryDocMeta

import datetime
import sys
PY3 = sys.version_info[0] == 3
if PY3:
    unicode = str

class MockDb:
    K_DB_TMPL_D    = 'K_DB_TMPL_D'
    K_DB_ALIAS_D   = 'K_DB_ALIAS_D'
    K_DB_INDICE_D  = 'K_DB_INDICE_D'
    K_IDX_ALIAS_L  = 'K_IDX_ALIAS_L'
    K_IDX_MAP      = 'K_IDX_MAP'
    K_IDX_SET      = 'K_IDX_SET'
    K_IDX_DID2DTYPES_D    = 'K_IDX_DID2DTYPES_D'
    K_IDX_DTYPE_D  = 'K_IDX_DTYPE_D'
    K_DT_DOC_D     = 'K_DT_DOC_D'
    K_DT_MAPSPROP       = 'K_DT_MAPSPROP'
    K_DT_SET       = 'K_DT_SET'

    def __init__(self, running_version):
        self.documents_dict = {}
        self.db_db_clear()
        self.scrolls = {}

        self.ES_VERSION_RUNNING = running_version
        self.log_prefix = '>>DB<<'

    def _default_db_structure(self):
        return {
            MockDb.K_DB_TMPL_D:   {},
            MockDb.K_DB_ALIAS_D:  {},
            MockDb.K_DB_INDICE_D: {},
        }

    def _default_idx_structure(self, mappings, settings):
        return {
            MockDb.K_IDX_ALIAS_L: [],
            MockDb.K_IDX_MAP: mappings,
            MockDb.K_IDX_SET: settings,
            MockDb.K_IDX_DID2DTYPES_D: {},
            MockDb.K_IDX_DTYPE_D: {},
        }

    def _default_dtype_structure(self, dtype_mappings, dtype_settings):
        return {
            MockDb.K_DT_DOC_D: {},
            MockDb.K_DT_MAPSPROP: dtype_mappings,
            MockDb.K_DT_SET: dtype_settings,
         }

    def _check_lookup_chain(self, check_if_has, keys):

        int_oper = 'has' if check_if_has else 'get'

        dbg_chain_all = False
        dbg_chain_start_stop = False

        if dbg_chain_all or dbg_chain_start_stop:
            Log.log(f"{self.log_prefix} LOOKUP({int_oper})    : {str(keys)}")

        def lookup_failed(k, d):
            if dbg_chain_all or dbg_chain_start_stop:
                Log.err(f"{self.log_prefix} LOOKUP({int_oper}) nok: {str(keys)} key[{str(k)}] not in {str(d)}")
            return False if check_if_has else None

        def lookup_ok(k, d):
            if dbg_chain_all or dbg_chain_start_stop:
                Log.log(f"{self.log_prefix} LOOKUP({int_oper})  ok: {str(keys)} key[{str(k)}] in {str(d)}")
            return True if check_if_has else d[k]

        d = self.db_db_get()
        for k in keys:
            if k in d:
                if k == keys[-1]:
                    return lookup_ok(k, d)
                if dbg_chain_all:
                    Log.log(f"{self.log_prefix} IS_IN : {str(k)} - {str(d)}")
                d = d[k]
            else:
                return lookup_failed(k, d)

    ############################################################################
    ############################################################################
    ### HAS ###
    def db_dtype_field_doc_key_has(self, idx, dtype, doc_id):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D, doc_id])
    def db_dtype_field_doc_dict_has(self, idx, dtype):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D])
    def db_dtype_field_mapsprop_has(self, idx, dtype):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_MAPSPROP])
    def db_dtype_field_sets_has(self, idx, dtype):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_SET])

    ### GET ###
    def db_dtype_field_doc_key_get(self, idx, dtype, doc_id):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D, doc_id])
    def db_dtype_field_doc_dict_get(self, idx, dtype):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D])
    def db_dtype_field_mapsprop_get(self, idx, dtype):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_MAPSPROP])
    def db_dtype_field_sets_get(self, idx, dtype):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_SET])

    # ### SET/REM ###
    def db_dtype_field_doc_key_set(self, idx, dtype, doc_id, doc_body):

        # index level
        if not self.db_idx_has(idx):
            self.db_idx_set(idx, None)

        # dtype level
        if not self.db_idx_field_dtype_key_has(idx, dtype):
            if not self.db_idx_field_dtype_key_create(idx, dtype):
                raise ValueError(f"{idx} - {dtype}")

        mapsprop = self.db_dtype_field_mapsprop_get(idx, dtype)
        if not mapsprop:
            self.db_dtype_field_mapsprop_set(idx, dtype,
                                             self.mappings_properties_from_doc_body(doc_body))

        # doc level
        doc_old = self.db_dtype_field_doc_key_get(idx, dtype, doc_id)
        version = (doc_old['_version']+1) if doc_old else 1

        self.db_dtype_field_doc_dict_get(idx, dtype).update({doc_id: {
            '_type': dtype,
            '_id': doc_id,
            '_source': doc_body,
            '_index': idx,
            '_version': version,
        }})

        #Log.notice(' 4. --------------------------------------------------')
        return self.db_dtype_field_doc_key_get(idx, dtype, doc_id)

    def db_dtype_field_doc_key_del(self, idx, dtype, doc_id):
        if self.db_dtype_field_doc_key_has(idx, dtype, doc_id):
            del self.db_dtype_field_doc_dict_get(idx, dtype)[doc_id]
            return True
        else:
            return False

    def db_dtype_field_mapsprop_set(self, idx, dtype, mapsprop):
        if self.db_idx_field_dtype_key_has(idx, dtype):
            self.db_idx_field_dtype_key_get(idx, dtype)[MockDb.K_DT_MAPSPROP] = mapsprop
            return True
        else:
            return False


    ############################################################################
    ############################################################################
    def db_api_docs_all(self, indexes: list = None, dtype=None, not_empty: bool = False) -> list:
        doc_all = []
        db_dict = self.db_db_indices_dict_get()

        for db_idx in db_dict:
            if indexes is not None and (not (db_idx in indexes)):
                continue  # just skip

            if self.db_idx_field_dtype_dict_has(db_idx):
                d = self.db_idx_field_dtype_dict_get(db_idx)
                for db_dtype in d.keys():
                    if dtype is not None and dtype != db_dtype:
                        continue  # just skip

                    if self.db_dtype_field_doc_dict_has(db_idx, db_dtype):
                        d_docs = self.db_dtype_field_doc_dict_get(db_idx, db_dtype)
                        for doc_id in d_docs.keys():
                            doc_all.append(QueryDocMeta(db_idx, db_dtype, doc_id, d_docs[doc_id]))
                            if not_empty:
                                # exit ASAP
                                return doc_all
        return doc_all

    def db_api_docs_all_is_not_empty(self, indexes: list = None, dtype=None) -> bool:
        return True if len(self.db_api_docs_all(indexes, dtype, not_empty=True)) else False

    #############################################################
    ### HAS ###
    def db_idx_field_dtype_key_has(self, idx, dtype_key):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype_key])
    def db_idx_field_dtype_dict_has(self, idx):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D])
    def db_idx_field_did2dtypes_key_has(self, idx, doc_id):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DID2DTYPES_D, doc_id])
    def db_idx_field_did2dtypes_dict_has(self, idx):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DID2DTYPES_D])
    def db_idx_field_mappings_has(self, idx):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_MAP])
    def db_idx_field_settings_has(self, idx):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_SET])
    def db_idx_field_alias_has(self, idx):
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_ALIAS_L])
    def db_idx_has(self, idx) -> bool:
        return self._check_lookup_chain(True, [MockDb.K_DB_INDICE_D, idx])

    ### GET ###
    def db_idx_field_dtype_key_get(self, idx, dtype_key):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D, dtype_key])
    def db_idx_field_dtype_dict_get(self, idx):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DTYPE_D])
    def db_idx_field_did2dtypes_key_get(self, idx, doc_id):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DID2DTYPES_D, doc_id])
    def db_idx_field_did2dtypes_dict_get(self, idx):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_DID2DTYPES_D])
    def db_idx_field_mappings_get(self, idx):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_MAP])
    def db_idx_field_settings_get(self, idx):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_SET])
    def db_idx_field_alias_get(self, idx):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx, MockDb.K_IDX_ALIAS_L])

    def db_idx_get(self, idx):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D, idx])

    ### SET ###
    def db_idx_field_dtype_key_create(self, idx, dtype):
        if self.db_idx_field_dtype_dict_has(idx):
            self.db_idx_field_dtype_dict_get(idx)[dtype] = self._default_dtype_structure(None, None)
            return True
        else:
            return False

    def db_idx_field_mappings_set(self, idx, mappings) -> bool:
        if self.db_idx_field_mappings_has(idx):
            self.db_idx_field_mappings_get(idx).update(mappings)
            return True
        else:
            return False

    def db_idx_field_settings_set(self, idx, settings) -> bool:
        if self.db_idx_field_settings_has(idx):
            self.db_idx_field_settings_get(idx).update(settings)
            return True
        else:
            return False

    def db_idx_set(self, idx, map_set_body):
        int_settings = {'index': {
                'creation_date': f"{str(datetime.datetime.now().timestamp())}",
                'number_of_shards': '2',
                'number_of_replicas': '1',
                'uuid': '???',
                'version': {'created': '?????'},
                'provided_name': idx}
        }
        mappings = map_set_body.get('mappings', {}) if map_set_body else {}
        settings = map_set_body.get('settings', {}) if map_set_body else int_settings
        self.db_db_indices_dict_get()[idx] = self._default_idx_structure(mappings, settings)

    ### SET ###
    def db_idx_del(self, idx) -> bool:
        if self.db_idx_has(idx):
            del self.db_db_indices_dict_get()[idx]
            return True
        else:
            return False

    ############################################################################
    ############################################################################

    def db_alias_has(self, alias_name):
        return self._check_lookup_chain(True, [MockDb.K_DB_ALIAS_D, alias_name])

    def db_alias_get(self, alias_name):
        return self._check_lookup_chain(False, [MockDb.K_DB_ALIAS_D, alias_name])

    def db_alias_idx_set(self, alias_name, index_name):
        if not self.db_alias_has(alias_name):
            self.db_db_alias_dict_get()[alias_name] = []
        if not (index_name in self.db_db_alias_dict_get()[alias_name]):
            self.db_db_alias_dict_get()[alias_name].append(index_name)
            # TODO check if index exist - could be alias set before idx exist?
            return True
        else:
            # TODO should be re-set sameidx considered as True or False
            return True

    def db_alias_idx_rem(self, alias_name, index_name):
        alias_list = self.db_db_alias_dict_get()[alias_name]
        if alias_list is None:
            return False
        if index_name in alias_list:
            alias_list.remove(index_name)
            if len(alias_list) == 0:
                del self.db_db_alias_dict_get()[alias_name]
            return True
        else:
            return 2

    def db_alias_rem(self, alias_name):
        if self.db_alias_has(alias_name):
            del self.db_db_alias_dict_get()[alias_name]
            return True
        else:
            return False

    ############################################################################
    ############################################################################

    def db_templ_has(self, tmlp_name):
        return self._check_lookup_chain(True, [MockDb.K_DB_TMPL_D, tmlp_name])

    def db_templ_get(self, tmlp_name):
        return self._check_lookup_chain(False, [MockDb.K_DB_TMPL_D, tmlp_name])

    def db_templ_set(self, tmlp_name, body):
        self.db_db_templates_dict_get()[tmlp_name] = body
        return True

    def db_templ_rem(self, tmlp_name):
        if self.db_templ_has(tmlp_name):
            del self.db_db_templates_dict_get()[tmlp_name]
            return True
        else:
            return False

    ############################################################################
    ############################################################################
    def db_db_alias_dict_get(self):
        return self._check_lookup_chain(False, [MockDb.K_DB_ALIAS_D])

    def db_db_templates_dict_get(self):
        return self._check_lookup_chain(False, [MockDb.K_DB_TMPL_D])

    def db_db_indices_dict_get(self):
        return self._check_lookup_chain(False, [MockDb.K_DB_INDICE_D])

    def db_db_clear(self):
        self.documents_dict = self._default_db_structure()

    def db_db_get(self):
        return self.documents_dict

    ############################################################################
    ############################################################################
    def db_db_dump(self, oper):
        dict_print = ''
        for index in self.db_db_indices_dict_get().keys():
            idx_level = f"IND[{str(index)}]"
            dict_print += '\n'
            dict_print += idx_level
            setting = self.db_idx_field_settings_get(index)
            if setting:
                setting_level = f"{idx_level} SETTINGS[{str(setting)}]"
                dict_print += '\n'
                dict_print += setting_level

            map = self.db_idx_field_mappings_get(index)
            if map:
                map_level = f"{idx_level} MAPPINGS"
                dict_print += '\n'
                dict_print += WesDefs.dump2string_result_ind_mappings(map, self, map_level)

            docid2types_dict = self.db_idx_field_did2dtypes_dict_get(index)
            if docid2types_dict:
                dict_print += '\n' + f"{idx_level} D2T:"
                for index_docid2types in docid2types_dict:
                    dict_print += '\n'
                    dict_print += f"{str(index_docid2types)} - {str(docid2types_dict[index_docid2types])}"

            index_types_dict = self.db_idx_field_dtype_dict_get(index)
            if index_types_dict:
                for index_type in index_types_dict:
                    type_level = f"{idx_level} TYPE[{str(index_type)}]"
                    dict_print += '\n'
                    dict_print += type_level
                    dict_print += '\n'
                    dict_print += str(index_types_dict[index_type])

        Log.notice(f"{oper} is mock {dict_print}")

    def db_db_dump_per_idx(self, oper):
        dict_print = ''
        db = self.db_db_indices_dict_get()
        db_print = ''
        for index in db.keys():
            db_print += '\n' + str(index) + ' IND: ' + str(db[index])

        Log.log(f"{oper} is mock {db_print}")

    ############################################################################
    ############################################################################
    def normalize_index_to_list(self, index):
        # Ensure to have a list of index
        if index is None:
            searchable_indexes = self.db_db_indices_dict_get().keys()
        elif isinstance(index, str) or isinstance(index, unicode):
            searchable_indexes = [index]
        elif isinstance(index, list):
            searchable_indexes = index
        else:
            # Is it the correct exception to use ?
            raise ValueError("Invalid param 'index'")

        # # Check index(es) exists
        # for searchable_index in searchable_indexes:
        #     if searchable_index not in MockEsCommon.db_get_idx_all(self):
        #         raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(searchable_index))

        return searchable_indexes

    def mappings_properties_from_doc_body(self, doc_body) -> dict:
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

        return {"properties": properties}
