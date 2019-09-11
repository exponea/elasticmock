import unittest
from common import TestCommon, WesDefs
from log import Log
from mock_db import MockDb

# document
doc11 = {'doc11_k': 'doc21_v'}
doc12_a = {'doc12_a_k': 'doc22_a_v'}
doc12_b = {'doc12_b_k': 'doc22_b_v'}
doc13 = {'doc13_k': 'doc23_v'}
doc14 = {'doc14_k': 'doc24_v'}
doc14_updated = {'doc24_k': 'doc14_v_UPDATED'}
doc15 = {'doc15_k': 'doc25_v'}

doc21 = {'doc21_k': 'doc21_v'}
doc22_a = {'doc22_a_k': 'doc22_a_v'}
doc22_b = {'doc22_b_k': 'doc22_b_v'}
doc23 = {'doc23_k': 'doc23_v'}
doc24 = {'doc24_k': 'doc24_v'}
doc24_updated = {'doc24_k': 'doc24_v_UPDATED'}
doc25 = {'doc25_k': 'doc25_v'}


# doc id
id_doc11 = 'id_doc11'
id_doc12 = 'id_doc12'
id_doc13 = 'id_doc13'
id_doc14 = 'id_doc14'
id_doc15 = 'id_doc15'

id_doc21 = 'id_doc21'
id_doc22 = 'id_doc22'
id_doc23 = 'id_doc23'
id_doc24 = 'id_doc24'
id_doc25 = 'id_doc25'

id_doc_bad = 'id_doc_bad'

# alias
alias_test_idx_1 = "alias_nice_index1"
alias_test_idx_2 = "alias_nice_index1"
alias_test_idx_3 = "alias_nice_index1"

# template
template_test_idx_1 = "template_nice_index1"
template_test_idx_2 = "template_nice_index1"
template_test_idx_3 = "template_nice_index1"


# indices
test_idx_1 = "nice_index1"
test_idx_2 = "nice_index2"
test_idx_3 = "nice_index3"
test_idx_bad = "bad_index"

# types
doc_type_11 = 'doc_type_11'
doc_type_12 = 'doc_type_12'
doc_type_13 = 'doc_type_13'

doc_type_21 = 'doc_type_21'
doc_type_22 = 'doc_type_22'
doc_type_23 = 'doc_type_23'

doc_type_bad = 'doc_type_bad'

# mappings
doc_type_11_mappings = 'doc_type_11_mappings'
doc_type_11_mappings_set = 'doc_type_11_mappings_SET_CHANGE'
doc_type_12_mappings = 'doc_type_12_mappings'
doc_type_13_mappings = 'doc_type_13_mappings'

doc_type_21_mappings = 'doc_type_11_mappings'
doc_type_21_mappings_set = 'doc_type_11_mappings_SET_CHANGE'
doc_type_22_mappings = 'doc_type_12_mappings'
doc_type_23_mappings = 'doc_type_13_mappings'

idx_1_mapping_data = {'mappings data key1': 'mappings data value1'}
idx_1_mapping_data_set = {'mappings data key1': 'mappings data value1__SET_CHANGE'}
idx_1_setting_data = {'settings data key1': 'settings data value1'}
idx_1_setting_data_set = {'settings data key1': 'settings data value1__SET_CHANGE'}

idx_2_mapping_data = {'mappings data key2': 'mappings data value2'}
idx_2_mapping_data_set = {'mappings data key2': 'mappings data value2__SET_CHANGE'}
idx_2_setting_data = {'settings data key2': 'settings data value2'}
idx_2_setting_data_set = {'settings data key2': 'settings data value2__SET_CHANGE'}

# settings
doc_type_11_settings = 'doc_type_11_settings'
doc_type_11_settings_set = 'doc_type_11_settings_SET_CHANGE'
doc_type_12_settings = 'doc_type_12_settings'
doc_type_13_settings = 'doc_type_13_settings'

doc_type_21_settings = 'doc_type_11_settings'
doc_type_21_settings_set = 'doc_type_11_settings_SET_CHANGE'
doc_type_22_settings = 'doc_type_12_settings'
doc_type_23_settings = 'doc_type_13_settings'

class TestMockDbHelper(TestCommon):

    def get_init_db(self):

        db = {
            MockDb.K_DB_TMPL_D: {},
            MockDb.K_DB_ALIAS_D: {
                alias_test_idx_1: [test_idx_1]
            },
            MockDb.K_DB_INDICE_D: {
                test_idx_1: {
                    MockDb.K_IDX_ALIAS_L: [alias_test_idx_1],
                    MockDb.K_IDX_MAP: idx_1_mapping_data,
                    MockDb.K_IDX_SET: idx_1_setting_data,
                    MockDb.K_IDX_DID2DTYPES_D: {
                        id_doc11: [doc_type_11, ],
                        id_doc12: [doc_type_11, doc_type_13],
                        id_doc13: [doc_type_11, ],
                    },
                    MockDb.K_IDX_DTYPE_D: {
                        doc_type_11: {
                            MockDb.K_IDX_ALIAS_L: [],
                            MockDb.K_DT_DOC_D: {id_doc11: doc11, id_doc12: doc12_a},
                            MockDb.K_DT_MAPSPROP:   doc_type_11_mappings,
                            MockDb.K_DT_SET:   doc_type_11_settings,
                        },
                        doc_type_12: {
                            MockDb.K_IDX_ALIAS_L: [],
                            MockDb.K_DT_DOC_D: {id_doc13: doc13},
                            MockDb.K_DT_MAPSPROP: doc_type_12_mappings,
                            MockDb.K_DT_SET: doc_type_12_settings,
                        },
                        doc_type_13: {
                            MockDb.K_IDX_ALIAS_L: [],
                            MockDb.K_DT_DOC_D: {id_doc13: doc13, id_doc12: doc12_b},
                            MockDb.K_DT_MAPSPROP: doc_type_13_mappings,
                            MockDb.K_DT_SET: doc_type_13_settings,
                        },
                    }
                },
                test_idx_2: {
                    MockDb.K_IDX_ALIAS_L: [],
                    MockDb.K_IDX_MAP: idx_2_mapping_data,
                    MockDb.K_IDX_SET: idx_2_setting_data,
                    MockDb.K_IDX_DID2DTYPES_D: {
                        id_doc21: [doc_type_21, ],
                        id_doc22: [doc_type_21, doc_type_23],
                        id_doc23: [doc_type_21, ],
                    },
                    MockDb.K_IDX_DTYPE_D: {
                        doc_type_21: {
                            MockDb.K_IDX_ALIAS_L: [],
                            MockDb.K_DT_DOC_D: {id_doc21: doc21, id_doc22: doc22_a},
                            MockDb.K_DT_MAPSPROP: doc_type_21_mappings,
                            MockDb.K_DT_SET: doc_type_21_settings,
                        },
                        doc_type_22: {
                            MockDb.K_IDX_ALIAS_L: [],
                            MockDb.K_DT_DOC_D: {id_doc23: doc23},
                            MockDb.K_DT_MAPSPROP: doc_type_22_mappings,
                            MockDb.K_DT_SET: doc_type_22_settings,
                        },
                        doc_type_23: {
                            MockDb.K_IDX_ALIAS_L: [],
                            MockDb.K_DT_DOC_D: {id_doc23: doc23, id_doc22: doc22_b},
                            MockDb.K_DT_MAPSPROP: doc_type_23_mappings,
                            MockDb.K_DT_SET: doc_type_23_settings,
                        },
                    }
                }
            }
        }

        return db

    def get_init_db_copy(self):
        return self.get_init_db().copy()

    def docs_dump(self, docs_all):
        Log.log('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-')
        Log.log(f" LEN: {len(docs_all)}")
        for idx_dtype_did_doc in docs_all:
            Log.log(f" ---> {str(idx_dtype_did_doc)}")
        Log.log('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-')


class TestMockDb(TestMockDbHelper):

    def setUp(self):
        self.db = MockDb(WesDefs.ES_VERSION_DEFAULT)

    def test_db_empty(self):

        self.db.db_db_clear()

        self.assertEqual(self.db.documents_dict, self.db.db_db_get())
        self.assertEqual({}, self.db.db_db_indices_dict_get())
        # check if cleanup works
        self.db.documents_dict = {'not_empty': 'not_empty'}
        self.db.db_db_clear()
        # L0
        self.assertEqual({}, self.db.db_db_indices_dict_get())
        self.assertEqual({}, self.db.db_db_templates_dict_get())
        self.assertEqual({}, self.db.db_db_alias_dict_get())

        self.assertEqual(False, self.db.db_idx_del(test_idx_1))
        self.assertEqual(False, self.db.db_idx_has(test_idx_1))
        self.assertEqual(None, self.db.db_idx_get(test_idx_1))

        # L1 - has
        self.assertEqual(False, self.db.db_idx_field_dtype_key_has(test_idx_1, doc_type_11))
        self.assertEqual(False, self.db.db_idx_field_dtype_dict_has(test_idx_1))
        self.assertEqual(False, self.db.db_idx_field_did2dtypes_key_has(test_idx_1, id_doc12))
        self.assertEqual(False, self.db.db_idx_field_did2dtypes_dict_has(test_idx_1))
        self.assertEqual(False, self.db.db_idx_field_mappings_has(test_idx_1))
        self.assertEqual(False, self.db.db_idx_field_settings_has(test_idx_1))
        # L1 - get
        self.assertEqual(None, self.db.db_idx_field_dtype_key_get(test_idx_1, doc_type_11))
        self.assertEqual(None, self.db.db_idx_field_dtype_dict_get(test_idx_1))
        self.assertEqual(None, self.db.db_idx_field_did2dtypes_key_get(test_idx_1, id_doc12))
        self.assertEqual(None, self.db.db_idx_field_did2dtypes_dict_get(test_idx_1))
        self.assertEqual(None, self.db.db_idx_field_mappings_get(test_idx_1))
        self.assertEqual(False, self.db.db_idx_field_mappings_set(test_idx_1, idx_1_mapping_data_set))
        self.assertEqual(None, self.db.db_idx_field_settings_get(test_idx_1))
        self.assertEqual(False, self.db.db_idx_field_settings_set(test_idx_1, idx_1_setting_data_set))
        # L2 - dtype - has
        self.assertEqual(False, self.db.db_dtype_field_doc_key_has(test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(False, self.db.db_dtype_field_doc_dict_has(test_idx_1, doc_type_11))
        self.assertEqual(False, self.db.db_dtype_field_mapsprop_has(test_idx_1, doc_type_11))
        self.assertEqual(False, self.db.db_dtype_field_sets_has(test_idx_1, doc_type_11))
        # L2 - dtype - get
        self.assertEqual(None, self.db.db_dtype_field_doc_key_get(test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(None, self.db.db_dtype_field_doc_dict_get(test_idx_1, doc_type_11))
        self.assertEqual(None, self.db.db_dtype_field_mapsprop_get(test_idx_1, doc_type_11))
        self.assertEqual(None, self.db.db_dtype_field_sets_get(test_idx_1, doc_type_11))

    def test_template(self):
        self.db.db_db_clear()
        self.assertEqual(False, self.db.db_templ_has(template_test_idx_1))
        self.assertEqual(False, self.db.db_templ_rem(template_test_idx_1))
        self.assertEqual(None, self.db.db_templ_get(template_test_idx_1))
        self.assertEqual(True, self.db.db_templ_set(template_test_idx_1, test_idx_1))
        self.assertEqual(test_idx_1, self.db.db_templ_get(template_test_idx_1))
        self.assertEqual(True, self.db.db_templ_rem(template_test_idx_1))
        self.assertEqual(None, self.db.db_templ_get(template_test_idx_1))

    def test_alias(self):
        self.db.db_db_clear()
        self.assertEqual(False, self.db.db_alias_has(alias_test_idx_1))
        self.assertEqual(False, self.db.db_alias_rem(alias_test_idx_1))
        self.assertEqual(None, self.db.db_alias_get(alias_test_idx_1))
        self.assertEqual(True, self.db.db_alias_idx_set(alias_test_idx_1, test_idx_1))
        self.assertEqual([test_idx_1], self.db.db_alias_get(alias_test_idx_1))
        self.assertEqual(True, self.db.db_alias_idx_set(alias_test_idx_1, test_idx_1))
        self.assertEqual([test_idx_1], self.db.db_alias_get(alias_test_idx_1))
        self.assertEqual(True, self.db.db_alias_idx_set(alias_test_idx_1, test_idx_2))
        self.assertEqual([test_idx_1, test_idx_2], self.db.db_alias_get(alias_test_idx_1))
        self.assertEqual(True, self.db.db_alias_idx_rem(alias_test_idx_1, test_idx_1))
        self.assertEqual([test_idx_2], self.db.db_alias_get(alias_test_idx_1))
        self.assertEqual(True, self.db.db_alias_idx_rem(alias_test_idx_1, test_idx_2))
        self.assertEqual(None, self.db.db_alias_get(alias_test_idx_1))

        self.assertEqual(True, self.db.db_alias_idx_set(alias_test_idx_1, test_idx_1))
        self.assertEqual(True, self.db.db_alias_idx_set(alias_test_idx_1, test_idx_2))
        self.assertEqual([test_idx_1, test_idx_2], self.db.db_alias_get(alias_test_idx_1))
        self.assertEqual(True, self.db.db_alias_rem(alias_test_idx_1))
        self.assertEqual(None, self.db.db_alias_get(alias_test_idx_1))

    def test_db_basic(self):

        self.db.db_db_clear()

        # add 3 indexes
        self.db.db_idx_set(test_idx_1, None)
        self.db.db_idx_set(test_idx_2, {'settings': doc_type_13_settings})
        self.db.db_idx_set(test_idx_3, None)

        self.db.db_db_dump_per_idx("OP_TEST")

        self.assertEqual(True, self.db.db_idx_has(test_idx_1))
        self.assertEqual(True, self.db.db_idx_has(test_idx_2))
        self.assertEqual(True, self.db.db_idx_has(test_idx_3))
        self.assertEqual(False, self.db.db_idx_has(test_idx_bad))

        # delete 2. indexes
        self.assertEqual(True, self.db.db_idx_del(test_idx_3))

        self.db.db_db_dump_per_idx("OP_TEST")
        self.assertEqual(True, self.db.db_idx_has(test_idx_1))
        self.assertEqual(True, self.db.db_idx_has(test_idx_2))
        self.assertEqual(False, self.db.db_idx_has(test_idx_3))
        self.assertEqual(False, self.db.db_idx_has(test_idx_bad))

        # delete 2. indexes again
        self.assertEqual(False, self.db.db_idx_del(test_idx_3))

        self.assertEqual(doc_type_13_settings, self.db.db_idx_field_settings_get(test_idx_2))


    def test_db_dumps(self):
        self.db.db_db_clear()
        self.db.documents_dict = self.get_init_db()

        Log.notice('--')
        self.db.db_db_dump_per_idx("OP_TEST")
        self.db.db_db_dump("OP_TEST")

    def test_db_init_data(self):
        self.db.db_db_clear()
        self.db.documents_dict = self.get_init_db()

        Log.notice('--')
        self.db.db_db_dump_per_idx("OP_TEST")
        self.db.db_db_dump("OP_TEST")

        DB = self.get_init_db()

        # L0
        self.assertEqual(DB, self.db.db_db_get())
        self.assertEqual(True, self.db.db_idx_has(test_idx_1))
        self.assertEqual(True, self.db.db_idx_del(test_idx_1))
        self.assertEqual(False, self.db.db_idx_has(test_idx_1))
        self.assertEqual(None, self.db.db_idx_get(test_idx_1))

        self.db.documents_dict = self.get_init_db()
        self.db.db_db_dump_per_idx("OP_TEST")

        # L1 - has
        self.assertEqual(True, self.db.db_idx_has(test_idx_1))
        self.assertEqual(False, self.db.db_idx_has(test_idx_bad))

        self.assertEqual(True, self.db.db_idx_field_dtype_key_has(test_idx_1, doc_type_11))
        self.assertEqual(False, self.db.db_idx_field_dtype_key_has(test_idx_1, doc_type_bad))

        self.assertEqual(True, self.db.db_idx_field_dtype_dict_has(test_idx_1))
        self.assertEqual(True, self.db.db_idx_field_did2dtypes_key_has(test_idx_1, id_doc12))
        self.assertEqual(False, self.db.db_idx_field_did2dtypes_key_has(test_idx_1, doc_type_bad))
        self.assertEqual(True, self.db.db_idx_field_did2dtypes_dict_has(test_idx_1))
        self.assertEqual(True, self.db.db_idx_field_mappings_has(test_idx_1))
        self.assertEqual(True, self.db.db_idx_field_settings_has(test_idx_1))
        # L1 - get
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11]
        self.assertEqual(check_val, self.db.db_idx_field_dtype_key_get(test_idx_1, doc_type_11))
        self.assertEqual(None, self.db.db_idx_field_dtype_key_get(test_idx_1, doc_type_bad))

        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][MockDb.K_IDX_DTYPE_D]
        self.assertEqual(check_val, self.db.db_idx_field_dtype_dict_get(test_idx_1))
        self.assertEqual(None, self.db.db_idx_field_dtype_dict_get(doc_type_bad))

        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][self.db.K_IDX_DID2DTYPES_D][id_doc12]
        self.assertEqual(check_val, self.db.db_idx_field_did2dtypes_key_get(test_idx_1, id_doc12))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][self.db.K_IDX_DID2DTYPES_D]
        self.assertEqual(check_val, self.db.db_idx_field_did2dtypes_dict_get(test_idx_1))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][self.db.K_IDX_MAP]
        self.assertEqual(check_val, self.db.db_idx_field_mappings_get(test_idx_1))
        self.assertEqual(True, self.db.db_idx_field_mappings_set(test_idx_1, idx_1_mapping_data_set))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][self.db.K_IDX_MAP]
        self.assertEqual(check_val, self.db.db_idx_field_mappings_get(test_idx_1))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][self.db.K_IDX_SET]
        self.assertEqual(check_val, self.db.db_idx_field_settings_get(test_idx_1))
        self.assertEqual(True, self.db.db_idx_field_settings_set(test_idx_1, idx_1_setting_data_set))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][self.db.K_IDX_SET]
        self.assertEqual(check_val, self.db.db_idx_field_settings_get(test_idx_1))

        # # L2 - dtype - has
        self.assertEqual(True, self.db.db_dtype_field_doc_key_has(test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(True, self.db.db_dtype_field_doc_dict_has(test_idx_1, doc_type_11))
        self.assertEqual(True, self.db.db_dtype_field_mapsprop_has(test_idx_1, doc_type_11))
        self.assertEqual(True, self.db.db_dtype_field_sets_has(test_idx_1, doc_type_11))
        # L2 - dtype - get
        Log.notice2('----------------------------')
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_DOC_D][id_doc12]
        self.assertEqual(check_val, self.db.db_dtype_field_doc_key_get(test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(None, self.db.db_dtype_field_doc_key_get(test_idx_1, doc_type_11, id_doc_bad))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_DOC_D]
        self.assertEqual(check_val, self.db.db_dtype_field_doc_dict_get(test_idx_1, doc_type_11))
        self.assertEqual(None, self.db.db_dtype_field_doc_dict_get(test_idx_1, id_doc_bad))

        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_MAPSPROP]
        self.assertEqual(check_val, self.db.db_dtype_field_mapsprop_get(test_idx_1, doc_type_11))
        check_val = DB[MockDb.K_DB_INDICE_D][test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_SET]
        self.assertEqual(check_val, self.db.db_dtype_field_sets_get(test_idx_1, doc_type_11))

    def db_operations_on_existing_idx__check_nb(self, nb_db, nb_list, idx):
        docs_all = self.db.db_api_docs_all()

        self.docs_dump(docs_all)

        self.assertEqual(nb_db, len(docs_all))

        self.assertEqual(0, len(self.db.db_api_docs_all(test_idx_bad)))
        self.assertEqual(nb_list[0], len(self.db.db_api_docs_all(idx)))
        self.assertEqual(nb_list[1], len(self.db.db_api_docs_all(idx, doc_type_11)))
        self.assertEqual(nb_list[2], len(self.db.db_api_docs_all(idx, doc_type_12)))
        self.assertEqual(0, len(self.db.db_api_docs_all(idx, doc_type_bad)))


    def test_db_operations_on_existing_idx(self):
        self.db.db_db_clear()
        self.db.documents_dict = self.get_init_db()

        # 1.
        self.db_operations_on_existing_idx__check_nb(10, [5,  # total on idx 'idx'
                                                         2,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                         1   # total on idx 'test_idx_1' , 'doc_type_12'
                                                         ],
                                                     test_idx_1)

        # 2. add new doc to idx
        doc_created = self.db.db_dtype_field_doc_key_set(test_idx_1, doc_type_12, id_doc14, doc14)
        self.assertEqual(1, doc_created['_version'])
        self.assertEqual(doc14, doc_created['_source'])
        self.db_operations_on_existing_idx__check_nb(11,
                                                     [6,  # total on idx 'test_idx_1'
                                                      2,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                      2   # total on idx 'test_idx_1' , 'doc_type_12'
                                                      ],
                                                     test_idx_1)

        # 3. update new doc
        doc_created = self.db.db_dtype_field_doc_key_set(test_idx_1, doc_type_12, id_doc14, doc14_updated)
        self.assertEqual(2, doc_created['_version'])
        self.assertEqual(doc14_updated, doc_created['_source'])
        self.db_operations_on_existing_idx__check_nb(11,
                                                     [6,  # total on idx 'test_idx_1'
                                                      2,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                      2   # total on idx 'test_idx_1' , 'doc_type_12'
                                                      ],
                                                     test_idx_1)
        # 3. delete new doc
        self.assertEqual(False, self.db.db_dtype_field_doc_key_del(test_idx_1, doc_type_12, id_doc_bad))
        self.assertEqual(True, self.db.db_dtype_field_doc_key_del(test_idx_1, doc_type_12, id_doc14))
        self.db_operations_on_existing_idx__check_nb(10,
                                                     [5,  # total on idx 'test_idx_1'
                                                      2,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                      1   # total on idx 'test_idx_1' , 'doc_type_12'
                                                      ],
                                                     test_idx_1)

        self.assertTrue(self.db.db_api_docs_all_is_not_empty([test_idx_1], doc_type_12))
        self.assertFalse(self.db.db_api_docs_all_is_not_empty([test_idx_1], doc_type_bad))

    def test_db_operations_on_non_existing_idx(self):
        self.db.db_db_clear()
        self.db.documents_dict = self.get_init_db()

        ### 1. ###
        # 1.a existing
        self.db_operations_on_existing_idx__check_nb(10, [5,  # total on idx 'test_idx_1'
                                                         2,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                         1   # total on idx 'test_idx_1' , 'doc_type_12'
                                                         ],
                                                     test_idx_1)

        # 1.b non-existing
        self.db_operations_on_existing_idx__check_nb(10, [0,  # total on idx 'test_idx_1'
                                                         0,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                         0   # total on idx 'test_idx_1' , 'doc_type_12'
                                                         ],
                                                     test_idx_3)

        ### 2. ###
        # add to existing and non existing
        doc_created = self.db.db_dtype_field_doc_key_set(test_idx_1, doc_type_12, id_doc14, doc14_updated)
        self.assertEqual(1, doc_created['_version'])
        self.assertEqual(doc14_updated, doc_created['_source'])

        doc_created = self.db.db_dtype_field_doc_key_set(test_idx_3, doc_type_12, id_doc14, doc14_updated)
        self.assertEqual(1, doc_created['_version'])
        self.assertEqual(doc14_updated, doc_created['_source'])

        # EXISTING - only total nb should be changed
        self.db_operations_on_existing_idx__check_nb(12,
                                                     [6,  # total on idx 'test_idx_1'
                                                      2,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                      2  # total on idx 'test_idx_1' , 'doc_type_12'
                                                      ],
                                                     test_idx_1)

        # NON-EXISTING - only total nb should be changed
        self.db_operations_on_existing_idx__check_nb(12, [1,  # total on idx 'test_idx_1'
                                                         0,  # total on idx 'test_idx_1' , 'doc_type_11'
                                                         1   # total on idx 'test_idx_1' , 'doc_type_12'
                                                         ],
                                                     test_idx_3)





if __name__ == '__main__':
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        # suite.addTest(TestMockDb("test_db_empty"))
        # suite.addTest(TestMockDb("test_db_basic"))
        # suite.addTest(TestMockDb("test_db_dumps"))
        # suite.addTest(TestMockDb("test_db_init_data"))
        # suite.addTest(TestMockDb("test_db_operations_on_existing_idx"))
        # suite.addTest(TestMockDb("test_db_operations_on_non_existing_idx"))

        runner = unittest.TextTestRunner()
        runner.run(suite)