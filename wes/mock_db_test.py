import unittest
from common import TestCommon, WesDefs

from mock_db import MockDb

class TestMockEsHelper(TestCommon):

    def get_db(self):

        idx_mapping_data = { 'mappings data key': 'mappings data value' }
        idx_setting_data = { 'settings data key': 'settings data value' }

        doc11   = { 'doc11_k': 'doc11_v'}
        doc12_a = { 'doc12_a_k': 'doc12_a_v'}
        doc12_b = { 'doc12_b_k': 'doc12_b_v'}
        doc13   = { 'doc13_k': 'doc13_v'}

        db = {
                'INDEX_1': {
                    MockDb.K_IDX_MAPS: idx_mapping_data,
                    MockDb.K_IDX_SETS: idx_setting_data,
                    MockDb.K_IDX_DOCS: {
                        'id_doc11': ['doc_type_11', ],
                        'id_doc12': ['doc_type_11', 'doc_type_13'],
                        'id_doc13': ['doc_type_11', ],
                    },
                    MockDb.K_IDX_DTYPES: {
                        'doc_type_11': {
                            MockDb.K_DT_DOCS: {'id_doc11': doc11, 'id_doc12': doc12_a},
                            MockDb.K_DT_MAPS: 'doc_type_11_mappings',
                            MockDb.K_DT_SETS: 'doc_type_11_settings',
                        },
                        'doc_type_12': {
                            MockDb.K_DT_DOCS: {'id_doc13': doc13},
                            MockDb.K_DT_MAPS: 'doc_type_12_mappings',
                            MockDb.K_DT_SETS: 'doc_type_12_settings',
                        },
                        'doc_type_13': {
                            MockDb.K_DT_DOCS: {'id_doc13': doc13, 'id_doc12': doc12_b},
                            MockDb.K_DT_MAPS: 'doc_type_13_mappings',
                            MockDb.K_DT_SETS: 'doc_type_13_settings',
                        },
                    }
                }
        }

class TestMockEs(TestMockEsHelper):

    def test_pokus(self):
        meta = MockDb()

if __name__ == '__main__':
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        # suite.addTest(TestMockEs("test_general"))
        runner = unittest.TextTestRunner()
        runner.run(suite)