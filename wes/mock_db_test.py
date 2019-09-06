import unittest
from common import TestCommon, WesDefs
from log import Log
from mock_db import MockDb

# document
doc11 = {'doc11_k': 'doc11_v'}
doc12_a = {'doc12_a_k': 'doc12_a_v'}
doc12_b = {'doc12_b_k': 'doc12_b_v'}
doc13 = {'doc13_k': 'doc13_v'}

# doc id
id_doc11 = 'id_doc11'
id_doc12 = 'id_doc12'
id_doc13 = 'id_doc13'
id_doc_bad = 'id_doc_bad'

# indices
test_idx_1 = "nice_index1"
test_idx_2 = "nice_index2"
test_idx_3 = "nice_index3"
test_idx_bad = "bad_index"

# types
doc_type_11 = 'doc_type_11'
doc_type_12 = 'doc_type_12'
doc_type_13 = 'doc_type_13'
doc_type_bad = 'doc_type_bad'

# mappings
doc_type_11_mappings = 'doc_type_11_mappings'
doc_type_12_mappings = 'doc_type_12_mappings'
doc_type_13_mappings = 'doc_type_13_mappings'

# settings
doc_type_11_settings = 'doc_type_11_settings'
doc_type_12_settings = 'doc_type_12_settings'
doc_type_13_settings = 'doc_type_13_settings'

class TestMockDbHelper(TestCommon):

    def get_init_db(self):

        idx_mapping_data = { 'mappings data key': 'mappings data value' }
        idx_setting_data = { 'settings data key': 'settings data value' }

        db = {
                test_idx_1: {
                    MockDb.K_IDX_MAP: idx_mapping_data,
                    MockDb.K_IDX_SET: idx_setting_data,
                    MockDb.K_IDX_DID2DTYPES_D: {
                        id_doc11: [doc_type_11, ],
                        id_doc12: [doc_type_11, doc_type_13],
                        id_doc13: [doc_type_11, ],
                    },
                    MockDb.K_IDX_DTYPE_D: {
                        doc_type_11: {
                            MockDb.K_DT_DOC_D: {id_doc11: doc11, id_doc12: doc12_a},
                            MockDb.K_DT_MAP:   doc_type_11_mappings,
                            MockDb.K_DT_SET:   doc_type_11_settings,
                        },
                        doc_type_12: {
                            MockDb.K_DT_DOC_D: {id_doc13: doc13},
                            MockDb.K_DT_MAP: doc_type_12_mappings,
                            MockDb.K_DT_SET: doc_type_12_settings,
                        },
                        doc_type_13: {
                            MockDb.K_DT_DOC_D: {id_doc13: doc13, id_doc12: doc12_b},
                            MockDb.K_DT_MAP: doc_type_13_mappings,
                            MockDb.K_DT_SET: doc_type_13_settings,
                        },
                    }
                }
        }

        return db

    def get_init_db_copy(self):
        return self.get_init_db().copy()


class TestMockDb(TestMockDbHelper):

    def setUp(self):
        self.db = MockDb()

    def test_db_empty(self):

        MockDb.db_db_clear(self.db)

        self.assertEqual(self.db, MockDb.get_parent(self.db))
        self.assertEqual({}, MockDb.db_idx_dict(self.db))
        # check if cleanup works
        self.db.documents_dict = {'not_empty': 'not_empty'}
        MockDb.db_db_clear(self.db)
        # L0
        self.assertEqual({}, MockDb.db_idx_dict(self.db))
        self.assertEqual(False, MockDb.db_idx_del(self.db, test_idx_1))
        self.assertEqual(False, MockDb.db_idx_has(self.db, test_idx_1))
        self.assertEqual(None, MockDb.db_idx_get(self.db, test_idx_1))
        # L1 - has
        self.assertEqual(False, MockDb.db_idx_field_dtype_key_has(self.db, test_idx_1, doc_type_11))
        self.assertEqual(False, MockDb.db_idx_field_dtype_dict_has(self.db, test_idx_1))
        self.assertEqual(False, MockDb.db_idx_field_did2dtypes_key_has(self.db, test_idx_1, id_doc12))
        self.assertEqual(False, MockDb.db_idx_field_did2dtypes_dict_has(self.db, test_idx_1))
        self.assertEqual(False, MockDb.db_idx_field_mappings_has(self.db, test_idx_1))
        self.assertEqual(False, MockDb.db_idx_field_settings_has(self.db, test_idx_1))
        # L1 - get
        self.assertEqual(None, MockDb.db_idx_field_dtype_key_get(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.db_idx_field_dtype_dict_get(self.db, test_idx_1))
        self.assertEqual(None, MockDb.db_idx_field_did2dtypes_key_get(self.db, test_idx_1, id_doc12))
        self.assertEqual(None, MockDb.db_idx_field_did2dtypes_dict_get(self.db, test_idx_1))
        self.assertEqual(None, MockDb.db_idx_field_mappings_get(self.db, test_idx_1))
        self.assertEqual(None, MockDb.db_idx_field_settings_get(self.db, test_idx_1))
        # L2 - dtype - has
        self.assertEqual(False, MockDb.db_dtype_field_doc_key_has(self.db, test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(False, MockDb.db_dtype_field_doc_dict_has(self.db, test_idx_1, doc_type_11))
        self.assertEqual(False, MockDb.db_dtype_field_maps_has(self.db, test_idx_1, doc_type_11))
        self.assertEqual(False, MockDb.db_dtype_field_sets_has(self.db, test_idx_1, doc_type_11))
        # L2 - dtype - get
        self.assertEqual(None, MockDb.db_dtype_field_doc_key_get(self.db, test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(None, MockDb.db_dtype_field_doc_dict_get(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.db_dtype_field_maps_get(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.db_dtype_field_sets_get(self.db, test_idx_1, doc_type_11))

    def test_db_basic(self):

        MockDb.db_db_clear(self.db)

        # add 3 indexes
        MockDb.db_idx_set(self.db, test_idx_1, None, None)
        MockDb.db_idx_set(self.db, test_idx_2, None, doc_type_13_settings)
        MockDb.db_idx_set(self.db, test_idx_3, None, None)

        MockDb.db_db_dump_per_idx("OP_TEST", self.db)

        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_1))
        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_2))
        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_3))
        self.assertEqual(False, MockDb.db_idx_has(self.db, test_idx_bad))

        # delete 2. indexes
        self.assertEqual(True, MockDb.db_idx_del(self.db, test_idx_3))

        MockDb.db_db_dump_per_idx("OP_TEST", self.db)
        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_1))
        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_2))
        self.assertEqual(False, MockDb.db_idx_has(self.db, test_idx_3))
        self.assertEqual(False, MockDb.db_idx_has(self.db, test_idx_bad))

        # delete 2. indexes again
        self.assertEqual(False, MockDb.db_idx_del(self.db, test_idx_3))

        self.assertEqual(doc_type_13_settings, MockDb.db_idx_field_settings_get(self.db, test_idx_2))


    def test_db_dumps(self):
        MockDb.db_db_clear(self.db)
        self.db.documents_dict = self.get_init_db()

        Log.notice('--')
        MockDb.db_db_dump_per_idx("OP_TEST", self.db)
        MockDb.db_db_dump("OP_TEST", self.db)

    def test_db_init_data(self):
        MockDb.db_db_clear(self.db)
        self.db.documents_dict = self.get_init_db()

        Log.notice('--')
        MockDb.db_db_dump_per_idx("OP_TEST", self.db)
        MockDb.db_db_dump("OP_TEST", self.db)

        DB = self.get_init_db()

        # L0
        self.assertEqual(DB, MockDb.db_idx_dict(self.db))
        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_1))
        self.assertEqual(True, MockDb.db_idx_del(self.db, test_idx_1))
        self.assertEqual(False, MockDb.db_idx_has(self.db, test_idx_1))
        self.assertEqual(None, MockDb.db_idx_get(self.db, test_idx_1))

        self.db.documents_dict = self.get_init_db()
        MockDb.db_db_dump_per_idx("OP_TEST", self.db)

        # L1 - has
        self.assertEqual(True, MockDb.db_idx_has(self.db, test_idx_1))
        self.assertEqual(False, MockDb.db_idx_has(self.db, test_idx_bad))

        self.assertEqual(True, MockDb.db_idx_field_dtype_key_has(self.db, test_idx_1, doc_type_11))
        self.assertEqual(False, MockDb.db_idx_field_dtype_key_has(self.db, test_idx_1, doc_type_bad))

        self.assertEqual(True, MockDb.db_idx_field_dtype_dict_has(self.db, test_idx_1))
        self.assertEqual(True, MockDb.db_idx_field_did2dtypes_key_has(self.db, test_idx_1, id_doc12))
        self.assertEqual(False, MockDb.db_idx_field_did2dtypes_key_has(self.db, test_idx_1, doc_type_bad))
        self.assertEqual(True, MockDb.db_idx_field_did2dtypes_dict_has(self.db, test_idx_1))
        self.assertEqual(True, MockDb.db_idx_field_mappings_has(self.db, test_idx_1))
        self.assertEqual(True, MockDb.db_idx_field_settings_has(self.db, test_idx_1))
        # L1 - get
        check_val = DB[test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11]
        self.assertEqual(check_val, MockDb.db_idx_field_dtype_key_get(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.db_idx_field_dtype_key_get(self.db, test_idx_1, doc_type_bad))

        check_val = DB[test_idx_1][MockDb.K_IDX_DTYPE_D]
        self.assertEqual(check_val, MockDb.db_idx_field_dtype_dict_get(self.db, test_idx_1))
        self.assertEqual(None, MockDb.db_idx_field_dtype_dict_get(self.db, doc_type_bad))

        check_val = DB[test_idx_1][MockDb.K_IDX_DID2DTYPES_D][id_doc12]
        self.assertEqual(check_val, MockDb.db_idx_field_did2dtypes_key_get(self.db, test_idx_1, id_doc12))
        check_val = DB[test_idx_1][MockDb.K_IDX_DID2DTYPES_D]
        self.assertEqual(check_val, MockDb.db_idx_field_did2dtypes_dict_get(self.db, test_idx_1))
        check_val = DB[test_idx_1][MockDb.K_IDX_MAP]
        self.assertEqual(check_val, MockDb.db_idx_field_mappings_get(self.db, test_idx_1))
        check_val = DB[test_idx_1][MockDb.K_IDX_SET]
        self.assertEqual(check_val, MockDb.db_idx_field_settings_get(self.db, test_idx_1))
        # # L2 - dtype - has
        self.assertEqual(True, MockDb.db_dtype_field_doc_key_has(self.db, test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(True, MockDb.db_dtype_field_doc_dict_has(self.db, test_idx_1, doc_type_11))
        self.assertEqual(True, MockDb.db_dtype_field_maps_has(self.db, test_idx_1, doc_type_11))
        self.assertEqual(True, MockDb.db_dtype_field_sets_has(self.db, test_idx_1, doc_type_11))
        # L2 - dtype - get
        Log.notice2('----------------------------')
        check_val = DB[test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_DOC_D][id_doc12]
        self.assertEqual(check_val, MockDb.db_dtype_field_doc_key_get(self.db, test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(None, MockDb.db_dtype_field_doc_key_get(self.db, test_idx_1, doc_type_11, id_doc_bad))
        check_val = DB[test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_DOC_D]
        self.assertEqual(check_val, MockDb.db_dtype_field_doc_dict_get(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.db_dtype_field_doc_dict_get(self.db, test_idx_1, id_doc_bad))

        check_val = DB[test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_MAP]
        self.assertEqual(check_val, MockDb.db_dtype_field_maps_get(self.db, test_idx_1, doc_type_11))
        check_val = DB[test_idx_1][MockDb.K_IDX_DTYPE_D][doc_type_11][MockDb.K_DT_SET]
        self.assertEqual(check_val, MockDb.db_dtype_field_sets_get(self.db, test_idx_1, doc_type_11))



if __name__ == '__main__':
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        # suite.addTest(TestMockDb("test_db_empty"))
        # suite.addTest(TestMockDb("test_db_basic"))
        # suite.addTest(TestMockDb("test_db_dumps"))
        # suite.addTest(TestMockDb("test_db_init_data"))
        runner = unittest.TextTestRunner()
        runner.run(suite)