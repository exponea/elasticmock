from log import Log

class MockDb:

    K_IDX_MAP      = 'K_IDX_MAP'
    K_IDX_SET      = 'K_IDX_SET'
    K_IDX_DOC_ID2TYPES_D    = 'K_IDX_DOC_ID2TYPES_D'
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
        #             MockDb.K_IDX_DOC_ID2TYPES_D: {
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

    ############################################################################
    ############################################################################
    # L2 - K_IDX_DOC_ID2TYPES_D

    ############################################################################
    ############################################################################
    # L2 - MockDb.K_IDX_DTYPE_D
    @staticmethod
    def _meta_type_has_XXX(obj, idx, type, key):
        if MockDb.meta_idx_has_type(obj, idx, type):
            return key in MockDb.meta_idx_get_type(obj, idx, type)
        else:
            return False

    @staticmethod
    def _meta_type_get_XXX(obj, idx, type, key):
        if MockDb._meta_type_has_XXX(obj, idx, type, key):
            return MockDb.meta_idx_get_type(obj, idx, type)[key]
        else:
            return None
    ############################################################################
    @staticmethod
    def meta_type_has_doc(obj, idx, type, doc_id) -> bool:
        if MockDb.meta_type_has_doc_dict(obj, idx, type):
            return doc_id in MockDb.meta_type_get_doc_dict(obj, idx, type)
        else:
            return False
    @staticmethod
    def meta_type_has_doc_dict(obj, idx, type):
        return MockDb._meta_type_has_XXX(obj, idx, type, MockDb.K_DT_DOC_D)
    @staticmethod
    def meta_type_has_maps(obj, idx, type):
        return MockDb._meta_type_has_XXX(obj, idx, type, MockDb.K_DT_MAP)
    @staticmethod
    def meta_type_has_sets(obj, idx, type):
        return MockDb._meta_type_has_XXX(obj, idx, type, MockDb.K_DT_SET)
    ############################################################################
    @staticmethod
    def meta_type_get_doc(obj, idx, type, doc_id) -> bool:
        if MockDb.meta_type_has_doc_dict(obj, idx, type):
            return MockDb.meta_type_get_doc_dict(obj, idx, type)[doc_id]
        else:
            return None
    @staticmethod
    def meta_type_get_doc_dict(obj, idx, type):
        return MockDb._meta_type_get_XXX(obj, idx, type, MockDb.K_DT_DOC_D)
    @staticmethod
    def meta_type_get_maps(obj, idx, type):
        return MockDb._meta_type_get_XXX(obj, idx, type, MockDb.K_DT_MAP)
    @staticmethod
    def meta_type_get_sets(obj, idx, type):
        return MockDb._meta_type_get_XXX(obj, idx, type, MockDb.K_DT_SET)

    ############################################################################
    ############################################################################
    # L1 - index
    @staticmethod
    def _meta_idx_has_XXX(obj, idx, key) -> bool:
        idx_data = MockDb.meta_db_get_idx(obj, idx)
        return True if (idx_data and key in idx_data) else False
    @staticmethod
    def _meta_idx_get_XXX(obj, idx, key):
        if MockDb._meta_idx_has_XXX(obj, idx, key):
            return MockDb.meta_db_get_idx(obj, idx)[key]
        else:
            return None
    ############################################################################
    @staticmethod
    def meta_idx_has_type(obj, idx, type) -> bool:
        if MockDb.meta_idx_has_type_dict(obj, idx):
            return type in MockDb.meta_idx_get_type_all(obj, idx)
        else:
            return False
    @staticmethod
    def meta_idx_has_type_dict(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_DTYPE_D)

    @staticmethod
    def meta_idx_has_doc_id2types(obj, idx, doc_id) -> bool:
        if MockDb.meta_idx_has_doc_id2types_dict(obj, idx):
            return doc_id in MockDb.meta_idx_get_doc_id2types_dict(obj, idx)
        else:
            return False
    @staticmethod
    def meta_idx_has_doc_id2types_dict(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_DOC_ID2TYPES_D)
    @staticmethod
    def meta_idx_has_mappings(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_MAP)
    @staticmethod
    def meta_idx_has_settings(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_SET)
    ############################################################################
    @staticmethod
    def meta_idx_get_type(obj, idx, type):
        if MockDb.meta_idx_has_type(obj, idx, type):
            return MockDb.meta_idx_has_type(obj, idx, type)
        else:
            return None

    @staticmethod
    def meta_idx_get_type_dict(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_DTYPE_D)

    @staticmethod
    def meta_idx_get_doc_id2types(obj, idx, type):
        if MockDb.meta_idx_has_type(obj, idx, type):
            return MockDb.meta_idx_has_type(obj, idx, type)
        else:
            return None

    @staticmethod
    def meta_idx_get_doc_id2types_dict(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_DOC_ID2TYPES_D)

    @staticmethod
    def meta_idx_get_mappings(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_MAP)
    @staticmethod
    def meta_idx_get_settings(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_SET)


    ############################################################################
    ############################################################################
    # L0 - db
    @staticmethod
    def meta_db_get_idx_dict(obj):
        return MockDb.meta_db_get(obj)

    @staticmethod
    def meta_db_get_idx(obj, idx):
        if MockDb.meta_db_has_idx(obj, idx):
            return MockDb.meta_db_get(obj)[idx]
        else:
            return None

    @staticmethod
    def meta_db_has_idx(obj, idx) -> bool:
        db = MockDb.meta_db_get(obj)
        return True if idx in db else False

    @staticmethod
    def meta_db_set_idx(obj, idx, mappings, settings):
         MockDb.meta_db_get(obj)[idx] = {
             MockDb.K_IDX_MAP: mappings,
             MockDb.K_IDX_SET: settings,
             MockDb.K_IDX_DOC_ID2TYPES_D: {},
             MockDb.K_IDX_DTYPE_D: {},
         }

    @staticmethod
    def meta_db_del_idx(obj, idx) -> bool:
        if MockDb.meta_db_has_idx(obj, idx):
            del MockDb.meta_db_get(obj)[idx]
            return True
        else:
            return False

    @staticmethod
    def meta_db_clear(obj):
        MockDb.meta_db_get(obj).clear()

    @staticmethod
    def meta_db_get(obj):
        parent = MockDb.get_parent(obj)
        return parent.documents_dict

    # helper
    @staticmethod
    def get_parent(obj):
        return obj.parent if hasattr(obj, 'parent') else obj

    ############################################################################
    ############################################################################
    @staticmethod
    def meta_dump_db(oper, obj):
        dict_print = ''
        for index in MockDb.meta_db_get_idx_all(obj):
            for index_type in MockDb.meta_idx_get_type_all(obj, index):

                dict_print += '\n' + str(index) + ' - ' + str(index_type) + ' IND TYPE SETTS: '
                if MockDb.meta_type_get_settings(obj, index):
                    setts = MockDb.meta_idx_get_settings(obj, index)
                    for s in setts:
                        dict_print += '\n' + str(setts[s])

                dict_print += '\n' + str(index) + ' - ' + str(index_type) + ' IND TYPE MAPS: '
                if MockDb.meta_idx_get_mappings(obj, index):
                    idx_maps = MockDb.meta_idx_get_mappings(obj, index)
                    for idx_map in idx_maps:
                        fields = idx_maps[idx_map]
                        for field in fields:
                            dict_print += '\n' + '{:>12}: '.format(str(field)) + str(fields[field])

                dict_print += '\n' + str(index) + ' - ' + str(index_type) + ' IND TYPE DOCS: '
                docs = MockDb.meta_idx_get_type_docs(obj, index, index_type)

                for id in docs:
                    dict_print += '\n' + str(docs[id])

        Log.notice(f"{oper} is mock {dict_print}")

    @staticmethod
    def meta_dump_db_per_idx(oper, obj):
        dict_print = ''
        db = MockDb.meta_db_get_idx_dict(obj)
        db_print = ''
        for index in db.keys():
            db_print += '\n' + str(index) + ' IND: ' + str(db[index])

        Log.log(f"{oper} is mock {db_print}")