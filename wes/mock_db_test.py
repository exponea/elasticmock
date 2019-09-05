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

# types
doc_type_11 = 'doc_type_11'
doc_type_12 = 'doc_type_12'
doc_type_13 = 'doc_type_13'

# mappings
doc_type_11_mappings = 'doc_type_11_mappings'
doc_type_12_mappings = 'doc_type_12_mappings'
doc_type_13_mappings = 'doc_type_13_mappings'

# settings
doc_type_11_settings = 'doc_type_11_settings'
doc_type_12_settings = 'doc_type_12_settings'
doc_type_13_settings = 'doc_type_13_settings'

# indices
test_idx_1 = "nice_index1"
test_idx_2 = "nice_index2"
test_idx_3 = "nice_index3"
test_idx_bad = "bad_index"

# doc_type
test_idx_1_doc_type = "doc_type__index1"
test_idx_2_doc_type = "doc_type__index2"
test_idx_3_doc_type = "doc_type_index3"

class TestMockDbHelper(TestCommon):

    def get_db(self):

        idx_mapping_data = { 'mappings data key': 'mappings data value' }
        idx_setting_data = { 'settings data key': 'settings data value' }

        db = {
                'INDEX_1': {
                    MockDb.K_IDX_MAP: idx_mapping_data,
                    MockDb.K_IDX_SET: idx_setting_data,
                    MockDb.K_IDX_DOC_ID2TYPES_D: {
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

class TestMockDb(TestMockDbHelper):

    def setUp(self):
        self.db = MockDb()

    def test_db_empty(self):

        MockDb.meta_db_clear(self.db)

        self.assertEqual(self.db, MockDb.get_parent(self.db))
        self.assertEqual({}, MockDb.meta_db_get_idx_dict(self.db))
        # check if cleanup works
        self.db.documents_dict = {'not_empty': 'not_empty'}
        MockDb.meta_db_clear(self.db)
        # L0
        self.assertEqual({}, MockDb.meta_db_get_idx_dict(self.db))
        self.assertEqual(False, MockDb.meta_db_del_idx(self.db, test_idx_1))
        self.assertEqual(False, MockDb.meta_db_has_idx(self.db, test_idx_1))
        self.assertEqual(None, MockDb.meta_db_get_idx(self.db, test_idx_1))
        # L1 - has
        self.assertEqual(False, MockDb.meta_idx_has_type(self.db, test_idx_1, test_idx_1_doc_type))
        self.assertEqual(False, MockDb.meta_idx_has_type_dict(self.db, test_idx_1))
        self.assertEqual(False, MockDb.meta_idx_has_doc_id2types(self.db, test_idx_1, id_doc12))
        self.assertEqual(False, MockDb.meta_idx_has_doc_id2types_dict(self.db, test_idx_1))
        self.assertEqual(False, MockDb.meta_idx_has_mappings(self.db, test_idx_1))
        self.assertEqual(False, MockDb.meta_idx_has_settings(self.db, test_idx_1))
        # L1 - get
        self.assertEqual(None, MockDb.meta_idx_get_type(self.db, test_idx_1, test_idx_1_doc_type))
        self.assertEqual(None, MockDb.meta_idx_get_type_dict(self.db, test_idx_1))
        self.assertEqual(None, MockDb.meta_idx_get_doc_id2types(self.db, test_idx_1, id_doc12))
        self.assertEqual(None, MockDb.meta_idx_get_doc_id2types_dict(self.db, test_idx_1))
        self.assertEqual(None, MockDb.meta_idx_get_mappings(self.db, test_idx_1))
        self.assertEqual(None, MockDb.meta_idx_get_settings(self.db, test_idx_1))
        # L2 - type - has
        self.assertEqual(False, MockDb.meta_type_has_doc(self.db, test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(False, MockDb.meta_type_has_doc_dict(self.db, test_idx_1, doc_type_11))
        self.assertEqual(False, MockDb.meta_type_has_maps(self.db, test_idx_1, doc_type_11))
        self.assertEqual(False, MockDb.meta_type_has_sets(self.db, test_idx_1, doc_type_11))
        # L2 - type - get
        self.assertEqual(None, MockDb.meta_type_get_doc(self.db, test_idx_1, doc_type_11, id_doc12))
        self.assertEqual(None, MockDb.meta_type_get_doc_dict(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.meta_type_get_maps(self.db, test_idx_1, doc_type_11))
        self.assertEqual(None, MockDb.meta_type_get_sets(self.db, test_idx_1, doc_type_11))

    def test_db_basic(self):

        MockDb.meta_db_clear(self.db)

        MockDb.meta_db_set_idx(self.db, test_idx_1, None, None)
        MockDb.meta_db_set_idx(self.db, test_idx_2, None, None)
        MockDb.meta_db_set_idx(self.db, test_idx_3, None, None)

        MockDb.meta_dump_db_per_idx("OP_TEST", self.db)

        self.assertEqual(True, MockDb.meta_db_has_idx(self.db, test_idx_1))
        self.assertEqual(True, MockDb.meta_db_has_idx(self.db, test_idx_2))
        self.assertEqual(True, MockDb.meta_db_has_idx(self.db, test_idx_3))
        self.assertEqual(False, MockDb.meta_db_has_idx(self.db, test_idx_bad))


        # MockDb.meta_db_set_idx(self.db, test_idx_1, None, None)
        # MockDb.meta_db_set_idx(self.db, test_idx_2, None, None)
        # MockDb.meta_db_set_idx(self.db, test_idx_3, None, None)
        # MockDb.meta_dump_db_per_idx("OP_TEST", self.db)


if __name__ == '__main__':
    if False:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        suite.addTest(TestMockDb("test_db_empty"))
        suite.addTest(TestMockDb("test_db_basic"))
        runner = unittest.TextTestRunner()
        runner.run(suite)