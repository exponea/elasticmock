from log import Log

class MockDb:

    K_IDX_MAP      = 'K_IDX_MAP'
    K_IDX_SET      = 'K_IDX_SET'
    K_IDX_DID2DTYPES_D    = 'K_IDX_DID2DTYPES_D'
    K_IDX_DTYPE_D  = 'K_IDX_DTYPE_D'
    K_DT_DOC_D     = 'K_DT_DOC_D'
    K_DT_MAP       = 'K_DT_MAP'
    K_DT_SET       = 'K_DT_SET'

    def __init__(self):
        self.documents_dict = {}
        self.scrolls = {}
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

    @staticmethod
    def _check_lookup_chain(check_if_has, obj, keys):

        dbg_lookup_chain = False

        def lookup_failed(k, d):
            if dbg_lookup_chain:
                Log.err(f" NOT_IN: {str(k)} - {str(d)}")
            return False if check_if_has else None

        def lookup_ok(k, d):
            if dbg_lookup_chain:
                Log.ok(f"IS_IN : {str(k)} - {str(d)}")
            return True if check_if_has else d[k]

        d = MockDb.db_idx_dict(obj)
        for k in keys:
            if k in d:
                if k == keys[-1]:
                    return lookup_ok(k, d)
                if dbg_lookup_chain:
                    Log.ok(f"IS_IN : {str(k)} - {str(d)}")
                d = d[k]
            else:
                return lookup_failed(k, d)

    ############################################################################
    ############################################################################
    @staticmethod
    def db_dtype_field_doc_key_has(obj, idx, dtype, doc_id):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D, doc_id])
    @staticmethod
    def db_dtype_field_doc_dict_has(obj, idx, dtype):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D])
    @staticmethod
    def db_dtype_field_maps_has(obj, idx, dtype):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_MAP])
    @staticmethod
    def db_dtype_field_sets_has(obj, idx, dtype):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_SET])

    @staticmethod
    def db_dtype_field_doc_key_get(obj, idx, dtype, doc_id):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D, doc_id])
    @staticmethod
    def db_dtype_field_doc_dict_get(obj, idx, dtype):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_DOC_D])
    @staticmethod
    def db_dtype_field_maps_get(obj, idx, dtype):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_MAP])
    @staticmethod
    def db_dtype_field_sets_get(obj, idx, dtype):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype, MockDb.K_DT_SET])

    ############################################################################
    ############################################################################
    @staticmethod
    def db_api_docs_all(obj, idx=None, dtype=None) -> list:
        doc_all = []
        db_dict = MockDb.db_idx_dict(obj)

        for db_idx in db_dict:
            if idx is not None and db_idx != idx:
                continue  # just skip

            if MockDb.db_idx_field_dtype_dict_has(obj, db_idx):
                d = MockDb.db_idx_field_dtype_dict_get(obj, db_idx)
                for db_dtype in d.keys():
                    if dtype is not None and dtype != db_dtype:
                        continue  # just skip

                    if MockDb.db_dtype_field_doc_dict_has(obj, db_idx, db_dtype):
                        d_docs = MockDb.db_dtype_field_doc_dict_get(obj, db_idx, db_dtype)
                        for doc_id in d_docs.keys():
                            doc_all.append([db_idx, db_dtype, doc_id, d_docs[doc_id]])
        return doc_all

    #############################################################
    @staticmethod
    def db_idx_field_dtype_key_has(obj, idx, dtype_key):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype_key])
    @staticmethod
    def db_idx_field_dtype_dict_has(obj, idx):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DTYPE_D])
    @staticmethod
    def db_idx_field_did2dtypes_key_has(obj, idx, doc_id):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DID2DTYPES_D, doc_id])
    @staticmethod
    def db_idx_field_did2dtypes_dict_has(obj, idx):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_DID2DTYPES_D])
    @staticmethod
    def db_idx_field_mappings_has(obj, idx):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_MAP])
    @staticmethod
    def db_idx_field_settings_has(obj, idx):
        return MockDb._check_lookup_chain(True, obj, [idx, MockDb.K_IDX_SET])

    @staticmethod
    def db_idx_field_dtype_key_get(obj, idx, dtype_key):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DTYPE_D, dtype_key])
    @staticmethod
    def db_idx_field_dtype_dict_get(obj, idx):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DTYPE_D])
    @staticmethod
    def db_idx_field_did2dtypes_key_get(obj, idx, doc_id):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DID2DTYPES_D, doc_id])
    @staticmethod
    def db_idx_field_did2dtypes_dict_get(obj, idx):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_DID2DTYPES_D])
    @staticmethod
    def db_idx_field_mappings_get(obj, idx):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_MAP])
    @staticmethod
    def db_idx_field_mappings_set(obj, idx, mappings) -> bool:
        if MockDb.db_idx_field_mappings_has(obj, idx):
            MockDb.db_idx_field_mappings_get(obj, idx).update(mappings)
            return True
        else:
            return False
    @staticmethod
    def db_idx_field_settings_get(obj, idx):
        return MockDb._check_lookup_chain(False, obj, [idx, MockDb.K_IDX_SET])
    @staticmethod
    def db_idx_field_settings_set(obj, idx, settings) -> bool:
        if MockDb.db_idx_field_settings_has(obj, idx):
            MockDb.db_idx_field_settings_get(obj, idx).update(settings)
            return True
        else:
            return False
    @staticmethod
    def db_idx_get(obj, idx):
        return MockDb._check_lookup_chain(False, obj, [idx])

    @staticmethod
    def db_idx_has(obj, idx) -> bool:
        return MockDb._check_lookup_chain(True, obj, [idx])

    #############################################################
    @staticmethod
    def db_idx_dict(obj):
        return MockDb.db_db_get(obj)

    @staticmethod
    def db_idx_set(obj, idx, mappings, settings):
         MockDb.db_db_get(obj)[idx] = {
             MockDb.K_IDX_MAP: mappings,
             MockDb.K_IDX_SET: settings,
             MockDb.K_IDX_DID2DTYPES_D: {},
             MockDb.K_IDX_DTYPE_D: {},
         }

    @staticmethod
    def db_idx_del(obj, idx) -> bool:
        if MockDb.db_idx_has(obj, idx):
            del MockDb.db_db_get(obj)[idx]
            return True
        else:
            return False

    @staticmethod
    def db_db_clear(obj):
        MockDb.db_db_get(obj).clear()

    @staticmethod
    def db_db_get(obj):
        return MockDb.get_parent(obj).documents_dict

    # helper
    @staticmethod
    def get_parent(obj):
        return obj.parent if hasattr(obj, 'parent') else obj

    ############################################################################
    ############################################################################
    @staticmethod
    def db_db_dump(oper, obj):
        dict_print = ''
        for index in MockDb.db_idx_dict(obj):
            dict_print += '\n' + str(index) + ' IND'

            setting = MockDb.db_idx_field_settings_get(obj, index)
            if setting:
                setting_level = f"IND[{str(index)}] MAP[{str(setting)}]"
                dict_print += '\n'
                dict_print += setting_level


            map = MockDb.db_idx_field_mappings_get(obj, index)
            if map:
                map_level = f"IND[{str(index)}] MAP[{str(map)}]"
                dict_print += '\n'
                dict_print += map_level


            docid2types_dict = MockDb.db_idx_field_did2dtypes_dict_get(obj, index)
            if docid2types_dict:
                dict_print += '\n' + f"IND[{str(index)}] D2T:"
                for index_docid2types in docid2types_dict:
                    dict_print += '\n'
                    dict_print += f"{str(index_docid2types)} - {str(docid2types_dict[index_docid2types])}"

            index_types_dict = MockDb.db_idx_field_dtype_dict_get(obj, index)
            if index_types_dict:
                for index_type in index_types_dict:
                    type_level = f"IND[{str(index)}] TYPE[{str(index_type)}]"
                    print(type_level)
                    dict_print += '\n'
                    dict_print += type_level
                    dict_print += '\n'
                    dict_print += str(index_types_dict[index_type])

        Log.notice(f"{oper} is mock {dict_print}")

    @staticmethod
    def db_db_dump_per_idx(oper, obj):
        dict_print = ''
        db = MockDb.db_db_get(obj)
        db_print = ''
        for index in db.keys():
            db_print += '\n' + str(index) + ' IND: ' + str(db[index])

        Log.log(f"{oper} is mock {db_print}")

    ############################################################################
    ############################################################################
