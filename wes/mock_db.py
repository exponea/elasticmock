from log import Log

class MockDb:

    K_IDX_MAPS      = 'K_IDX_MAPS'
    K_IDX_SETS      = 'K_IDX_SETS'
    K_IDX_DOCS      = 'K_IDX_DOCS'
    K_IDX_DTYPES    = 'K_IDX_DTYPES'
    K_DT_DOCS       = 'K_DT_DOCS'
    K_DT_MAPS       = 'K_DT_MAPS'
    K_DT_SETS       = 'K_DT_SETS'

    def __init__(self):
        # db = {
        #     'INDEX_1': {
        #         MockDb.K_IDX_MAPS: idx_mapping_data
        #         MockDb.K_IDX_SETS: idx_setting_data
        #         MockDb.K_IDX_DOCS: {
        #             'id_doc11': ['doc_type_11',],
        #             'id_doc12': ['doc_type_11', 'doc_type_13'],
        #             'id_doc13': ['doc_type_11',],
        #         },
        #         MockDb.K_IDX_DTYPES: {
        #             'doc_type_11': {
        #                     MockDb.K_DT_DOCS: { 'id_doc11': doc11, 'id_doc12': doc12_a },
        #                     MockDb.K_DT_MAPS = 'doc_type_11_mappings',
        #                     MockDb.K_DT_SETS = 'doc_type_11_settings',
        #             },
        #             'doc_type_12': {
        #                     MockDb.K_DT_DOCS: {'id_doc13': doc13},
        #                     MockDb.K_DT_MAPS = 'doc_type_12_mappings',
        #                     MockDb.K_DT_SETS = 'doc_type_12_settings',
        #             }
        #             'doc_type_13': {
        #                     MockDb.K_DT_DOCS: {'id_doc13': doc13, 'id_doc12': doc12_b},
        #                     MockDb.K_DT_MAPS = 'doc_type_13_mappings',
        #                     MockDb.K_DT_SETS = 'doc_type_13_settings',
        #             }
        #         }
        #     }
        # }
        self.documents_dict = {}
        self.scrolls = {}

    @staticmethod
    def meta_set_idx_mappings_settings(obj, idx, mappings_settings):
        MockDb.meta_set_idx_mappings(obj, idx, mappings_settings['mappings'])
        MockDb.meta_set_idx_settings(obj, idx, mappings_settings['settings'])


    ############################################################################
    ############################################################################
    # L2 - doc_type

    ############################################################################
    ############################################################################
    # L2 - MockDb.K_IDX_DTYPES
    @staticmethod
    def _meta_type_has_XXX(obj, idx, key):
        type_data = MockDb.meta_idx_has_type(obj, idx)
        return key in type_data

    @staticmethod
    def _meta_type_get_XXX(obj, idx, key):
        if MockDb._meta_type_has_XXX(obj, idx, key):
            return MockDb.meta_idx_get_type(obj, idx)[key]
        else:
            return None
    ############################################################################
    @staticmethod
    def meta_type_has_docs(obj, idx, type):
        return MockDb._meta_type_has_XXX(obj, idx, type, MockDb.K_DT_DOCS)
    @staticmethod
    def meta_type_has_maps(obj, idx, type):
        return MockDb._meta_type_has_XXX(obj, idx, type, MockDb.K_DT_MAPS)
    @staticmethod
    def meta_type_has_sets(obj, idx, type):
        return MockDb._meta_type_has_XXX(obj, idx, type, MockDb.K_DT_SETS)
    ############################################################################
    @staticmethod
    def meta_type_get_docs(obj, idx, type):
        return MockDb._meta_type_get_XXX(obj, idx, type, MockDb.K_DT_DOCS)
    @staticmethod
    def meta_type_get_maps(obj, idx, type):
        return MockDb._meta_type_get_XXX(obj, idx, type, MockDb.K_DT_MAPS)
    @staticmethod
    def meta_type_get_sets(obj, idx, type):
        return MockDb._meta_type_get_XXX(obj, idx, type, MockDb.K_DT_SETS)

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
        if MockDb.meta_idx_has_type_all(obj, idx):
            return type in MockDb.meta_idx_get_type_all(obj, idx)
        else:
            False

    @staticmethod
    def meta_idx_has_type_all(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_DTYPES)
    @staticmethod
    def meta_idx_has_mappings(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_MAPS)
    @staticmethod
    def meta_idx_has_settings(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_SETS)
    @staticmethod
    def meta_idx_has_docs(obj, idx) -> bool:
        return MockDb._meta_idx_has_XXX(obj, idx, MockDb.K_IDX_DOCS)
    ############################################################################
    @staticmethod
    def meta_idx_get_type_all(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_DTYPES)
    @staticmethod
    def meta_idx_get_mappings(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_MAPS)
    @staticmethod
    def meta_idx_get_settings(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_SETS)
    @staticmethod
    def meta_idx_get_docs(obj, idx):
        return MockDb._meta_idx_get_XXX(obj, idx, MockDb.K_IDX_DOCS)

    ############################################################################
    ############################################################################
    # L0 - db
    @staticmethod
    def meta_db_get_idx_all(obj):
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
             MockDb.K_IDX_MAPS: mappings,
             MockDb.K_IDX_SETS: settings,
             MockDb.K_IDX_DOCS: {},
             MockDb.K_IDX_DTYPES: {},
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
