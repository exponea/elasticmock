from log import Log
from common import WesDefs

import datetime
import sys
PY3 = sys.version_info[0] == 3
if PY3:
    unicode = str

class MockDb:

    K_IDX_MAP      = 'K_IDX_MAP'
    K_IDX_SET      = 'K_IDX_SET'
    K_IDX_DID2DTYPES_D    = 'K_IDX_DID2DTYPES_D'
    K_IDX_DTYPE_D  = 'K_IDX_DTYPE_D'
    K_DT_DOC_D     = 'K_DT_DOC_D'
    K_DT_MAP       = 'K_DT_MAP'
    K_DT_SET       = 'K_DT_SET'

    def __init__(self, running_version):
        self.documents_dict = {}
        self.scrolls = {}

        self.ES_VERSION_RUNNING = running_version
        self.log_prefix = '>>DB<<'
        # db = {
        #         'INDEX_1': {
        #             MockDb.K_IDX_MAP: idx_mapping_data,
        #             MockDb.K_IDX_SET: idx_setting_data,
        #             MockDb.K_IDX_DID2DTYPES_D: {
        #                 id_doc11: [doc_type_11, ],
        #                 id_doc12: [doc_type_11, doc_type_13],
        #                 id_doc13: [doc_type_11, ],
        #             },
        #             MockDb.K_IDX_DTYPE_D: {
        #                 doc_type_11: {
        #                     MockDb.K_DT_DOC_D: {id_doc11: doc11, id_doc12: doc12_a},
        #                     MockDb.K_DT_MAP:   doc_type_11_mappings,
        #                     MockDb.K_DT_SET:   doc_type_11_settings,
        #                 },
        #                 doc_type_12: {
        #                     MockDb.K_DT_DOC_D: {id_doc13: doc13},
        #                     MockDb.K_DT_MAP: doc_type_12_mappings,
        #                     MockDb.K_DT_SET: doc_type_12_settings,
        #                 },
        #                 doc_type_13: {
        #                     MockDb.K_DT_DOC_D: {id_doc13: doc13, id_doc12: doc12_b},
        #                     MockDb.K_DT_MAP: doc_type_13_mappings,
        #                     MockDb.K_DT_SET: doc_type_13_settings,
        #                 },
        #             }
        #         }
        # }

    def _default_idx_structure(self, mappings, settings):
        return {
            MockDb.K_IDX_MAP: mappings,
            MockDb.K_IDX_SET: settings,
            MockDb.K_IDX_DID2DTYPES_D: {},
            MockDb.K_IDX_DTYPE_D: {},
        }

    def _default_dtype_structure(self, dtype_mappings, dtype_settings):
        return {
            MockDb.K_DT_DOC_D: {},
            MockDb.K_DT_MAP: dtype_mappings,
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

        d = self.db_idx_dict()
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
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D, doc_id])
    def db_dtype_field_doc_dict_has(self, idx, dtype):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D])
    def db_dtype_field_maps_has(self, idx, dtype):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_MAP])
    def db_dtype_field_sets_has(self, idx, dtype):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_SET])

    ### GET ###
    def db_dtype_field_doc_key_get(self, idx, dtype, doc_id):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D, doc_id])
    def db_dtype_field_doc_dict_get(self, idx, dtype):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D])
    def db_dtype_field_maps_get(self, idx, dtype):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_MAP])
    def db_dtype_field_sets_get(self, idx, dtype):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_SET])

    # ### SET/REM ###
    def db_dtype_create_default(self, idx, dtype):
        if self.db_idx_field_dtype_dict_has(idx):
            self.db_idx_field_dtype_dict_get(idx)[dtype] = self._default_dtype_structure(None, None)
            return True
        else:
            return False

    def db_dtype_field_doc_key_set(self, idx, dtype, doc_id, body):
        #Log.notice(' 1. --------------------------------------------------')
        doc_old = self.db_dtype_field_doc_key_get(idx, dtype, doc_id)
        version = (doc_old['_version']+1) if doc_old else 1

        #Log.notice(' 2. --------------------------------------------------')
        if not self.db_idx_field_dtype_key_has(idx, dtype):
            if not self.db_dtype_create_default(idx, dtype):
                raise ValueError(f"{idx} - {dtype}")

        #Log.notice(' 3. --------------------------------------------------')
        self.db_dtype_field_doc_dict_get(idx, dtype).update({doc_id: {
            '_type': dtype,
            '_id': doc_id,
            '_source': body,
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

    ############################################################################
    ############################################################################
    def db_api_docs_all(self, idx=None, dtype=None) -> list:
        doc_all = []
        db_dict = self.db_idx_dict()

        for db_idx in db_dict:
            if idx is not None and db_idx != idx:
                continue  # just skip

            if self.db_idx_field_dtype_dict_has(db_idx):
                d = self.db_idx_field_dtype_dict_get(db_idx)
                for db_dtype in d.keys():
                    if dtype is not None and dtype != db_dtype:
                        continue  # just skip

                    if self.db_dtype_field_doc_dict_has(db_idx, db_dtype):
                        d_docs = self.db_dtype_field_doc_dict_get(db_idx, db_dtype)
                        for doc_id in d_docs.keys():
                            doc_all.append([db_idx, db_dtype, doc_id, d_docs[doc_id]])
        return doc_all

    #############################################################
    ### HAS ###
    def db_idx_field_dtype_key_has(self, idx, dtype_key):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DTYPE_D, dtype_key])
    def db_idx_field_dtype_dict_has(self, idx):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DTYPE_D])
    def db_idx_field_did2dtypes_key_has(self, idx, doc_id):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DID2DTYPES_D, doc_id])
    def db_idx_field_did2dtypes_dict_has(self, idx):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_DID2DTYPES_D])
    def db_idx_field_mappings_has(self, idx):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_MAP])
    def db_idx_field_settings_has(self, idx):
        return self._check_lookup_chain(True, [idx, MockDb.K_IDX_SET])

    ### GET ###
    def db_idx_field_dtype_key_get(self, idx, dtype_key):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DTYPE_D, dtype_key])
    def db_idx_field_dtype_dict_get(self, idx):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DTYPE_D])
    def db_idx_field_did2dtypes_key_get(self, idx, doc_id):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DID2DTYPES_D, doc_id])
    def db_idx_field_did2dtypes_dict_get(self, idx):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_DID2DTYPES_D])
    def db_idx_field_mappings_get(self, idx):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_MAP])
    def db_idx_field_settings_get(self, idx):
        return self._check_lookup_chain(False, [idx, MockDb.K_IDX_SET])

    ### SET ###
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

    def db_idx_get(self, idx):
        return self._check_lookup_chain(False, [idx])

    def db_idx_has(self, idx) -> bool:
        return self._check_lookup_chain(True, [idx])

    #############################################################
    def db_idx_dict(self):
        return self.db_db_get()

    def db_idx_set(self, idx, mappings, settings):
         self.db_db_get()[idx] = self._default_idx_structure(mappings, settings)

    def db_idx_del(self, idx) -> bool:
        if self.db_idx_has(idx):
            del self.db_db_get()[idx]
            return True
        else:
            return False

    def db_db_clear(self):
        self.db_db_get().clear()

    def db_db_get(self):
        return self.documents_dict

    ############################################################################
    ############################################################################
    def db_db_dump(self, oper):
        dict_print = ''
        for index in self.db_idx_dict().keys():
            idx_level = f"IND[{str(index)}]"
            dict_print += '\n'
            dict_print += idx_level
            setting = self.db_idx_field_settings_get(index)
            if setting:
                setting_level = f"{idx_level} SET[{str(setting)}]"
                dict_print += '\n'
                dict_print += setting_level

            map = self.db_idx_field_mappings_get(index)
            if map:
                map_level = f"{idx_level} MAP"
                dict_print += '\n'
                dict_print += WesDefs.mappings_dump2str(map, self, map_level)

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
                    print(type_level)
                    dict_print += '\n'
                    dict_print += type_level
                    dict_print += '\n'
                    dict_print += str(index_types_dict[index_type])

        Log.notice(f"{oper} is mock {dict_print}")

    def db_db_dump_per_idx(self, oper):
        dict_print = ''
        db = self.db_db_get()
        db_print = ''
        for index in db.keys():
            db_print += '\n' + str(index) + ' IND: ' + str(db[index])

        Log.log(f"{oper} is mock {db_print}")

    ############################################################################
    ############################################################################
    def normalize_index_to_list(self, index):
        # Ensure to have a list of index
        if index is None:
            searchable_indexes = self.db_idx_dict().keys()
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

    def mappings_settings_build_from_doc_body_data(self, doc_body) -> dict:
        create_body = {
            'mappings': self.mappings_properties_from_doc_body(doc_body),
            'settings': {}
        }

        return create_body
