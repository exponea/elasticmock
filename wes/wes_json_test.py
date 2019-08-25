from wes import Wes, ExecCode
from log import Log
import unittest
from datetime import datetime

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

from zipfile import ZipFile
from pathlib import Path
import json

class TestWesJsonHelper(unittest.TestCase):

    def indice_cleanup_all(self, wes):
        self.assertEqual(Wes.RC_OK, wes.ind_delete_result(wes.ind_delete("_all")).status)

    def _helper_zip_unpack(self, path_to_zip, file_to_extract="all"):
        file = Path(path_to_zip)

        extracted_files = []

        if file.is_file():
            input_zip = ZipFile(path_to_zip)
            Log.log(str(input_zip.namelist()))
            for name in input_zip.namelist():
                if file_to_extract == "all" or name in file_to_extract:
                    # utf-8 is used here because it is a very common encoding, but you
                    # need to use the encoding your data is actually in.
                    # $ file elasticmock-testcases/1.json
                    # elasticmock-testcases/1.json: ASCII text, with very long lines
                    extracted_files.append([name, input_zip.read(name).decode("ascii")])

        return extracted_files

    def helper_split_zip_test(self, path_to_zip, file_to_extract="all"):

        raw_tests = self._helper_zip_unpack(path_to_zip, file_to_extract)

        print(raw_tests)
        tests = []
        for test_name, raw_test in raw_tests:
            split_test = raw_test.split('\n')
            split_list = [json.loads(test_cmd) for test_cmd in split_test if test_cmd.strip()]
            tests.append([test_name, split_list])
        return tests

    def helper_run_unpacked_test(self, wes: Wes, test_name: str, line: int, result, accessor, method, *args, **kwargs):

        group = 'IND' if accessor else 'DOC'

        Log.log(f"T[{test_name}] L[{line:3}] -> {group} "
                f"cmd({method} <-> args{args} <-> kwargs({kwargs})) result({result})")

        # BE SURE TO BE IN SYNCH WITH
        # Wes.operation_mappers
        # TODO which methods are used in exponea?
        method_mapper = {
            "IND": {
                "create"        : Wes.OP_IND_CREATE,
                "flush"         : Wes.OP_IND_FLUSH,
                "refresh"       : Wes.OP_IND_REFRESH,
                "exists"        : Wes.OP_IND_EXIST,
                "delete"        : Wes.OP_IND_DELETE,
                "get_mapping"   : Wes.OP_IND_GET_MAP,
                "put_mapping"   : Wes.OP_IND_PUT_MAP,
                "put_template"  : Wes.OP_IND_PUT_TMP,
                "get_template"  : Wes.OP_IND_GET_TMP,
            },
            'DOC': {
                "index"     : Wes.OP_DOC_ADD_UP,
                "get"       : Wes.OP_DOC_GET,
                "exists"    : Wes.OP_DOC_EXIST,

                "search"    : Wes.OP_DOC_SEARCH,
                "bulk"      : Wes.OP_DOC_BULK,
                "scan"      : Wes.OP_DOC_SCAN,
                "count"     : Wes.OP_DOC_COUNT,
            }
        }

        operation = method_mapper[group][method]

        #############################################################
        # OPERATIONS - VALUES FIXER
        #############################################################
        if wes.ES_VERSION_RUNNING == Wes.ES_VERSION_5_6_5:

            if operation == Wes.OP_DOC_ADD_UP:
                indice, doc_type = args
                Log.log(f"{operation} FIXER(  args) before : {args}")
                args = (indice,)
                Log.log(f"{operation} FIXER(  args) after  : {args}")

                kw_doc_type = kwargs.get('doc_type', None)
                Log.log(f"{operation} FIXER(kwargs) before : doc_type({kw_doc_type})")
                # FAKE as result of PYTHON API:
                #  EXCEPTION: 'reason': "Document mapping type name can't start with '_', found: [_doc]"
                #  API      : def index(self, index, body, doc_type="_doc", id=None, params=None):
                doc_type = kwargs['doc_type'] = doc_type
                Log.log(f"{operation} FIXER(kwargs) after  : doc_type({doc_type})")

            elif operation == Wes.OP_DOC_SEARCH:

                # FAKE as result of PYTHON API:
                # EXCEPTION: Unknow L1 exception ... doc_search() got an unexpected keyword argument 'doc_type'
                kw_doc_type = kwargs.get('doc_type', None)
                Log.log(f"{operation} FIXER(kwargs) before : doc_type({kw_doc_type})")
                del kwargs['doc_type']
                doc_type = kwargs.get('doc_type', None)
                Log.log(f"{operation} FIXER(kwargs) after  : doc_type({doc_type})")

            else:
                pass
        else:
            raise ValueError(f"ES_VERSION_RUNNING is unknown")

        wes_mappers = wes.operation_mappers(operation)
        # some test case operation not covered
        self.assertNotEqual(None, wes_mappers)
        wes_mappers_operation, wes_mappers_operation_result = wes_mappers
        Log.log(f"{operation} MAPPERS: {str(wes_mappers)}")
        wes_mappers_rc = wes_mappers_operation_result(wes, wes_mappers_operation(wes, *args, **kwargs))

        rc_result = ExecCode(Wes.RC_OK, result, wes_mappers_rc.fnc_params)
        if operation == Wes.OP_DOC_SEARCH:
            # 1. check nb match records
            self.assertEqual(wes.doc_search_result_hits_nb(rc_result),
                             wes.doc_search_result_hits_nb(wes_mappers_rc))

            # 2. check docs by content
            ext_sources = wes.doc_search_result_hits_sources(rc_result)
            wes_sources = wes.doc_search_result_hits_sources(wes_mappers_rc)
            for idx, wes_doc in enumerate(wes_sources):
                ext_doc = ext_sources[idx]
                self.assertDictEqual(ext_doc, wes_doc)

        elif operation == Wes.OP_IND_REFRESH:
            self.assertEqual(wes.ind_refresh_result_shard_nb_failed(rc_result),
                             wes.ind_refresh_result_shard_nb_failed(wes_mappers_rc))
        else:
            self.assertDictEqual(rc_result.data, wes_mappers_rc.data)


    def helper_run_unpacked_tests(self, wes, tests: list, is_interactive=False):
        for test_name, test_lines in tests:
            Log.notice(f"T[{test_name}] -  commands to execute {len(test_lines)}")
            for line, cmd in enumerate(test_lines):
                #Log.log(f"T[{test_name}] L[{line:3}] -> raw {cmd}")
                self.helper_run_unpacked_test(wes, test_name, line, cmd['result'], cmd['accessor'],
                                                      cmd['method'], *(cmd['args']), **(cmd['kwargs']))
                if is_interactive:
                    input("Press ENTER ...")

    def helper_json_parser(self, zip_path, tests: tuple):
        self.indice_cleanup_all(self.wes)
        tests = self.helper_split_zip_test(zip_path, tests)
        self.assertEqual(1, len(tests))
        self.helper_run_unpacked_tests(self.wes, tests)


class TestWesJson(TestWesJsonHelper):

    def setUp(self):
        self.wes = Wes()
        # self.index_name = 'test_index'
        # self.doc_type = 'doc-Type'
        # self.body = {'string': 'content', 'id': 1}

    def test_json_parser_passed(self):
        zip_path = "./exponea_tests/elasticmock-testcases.zip"
        tests = ('{}.json'.format(nb) for nb in range(0, 1))
        self.helper_json_parser(zip_path, tests)

    # method for tests to pass
    def json_parser_todo(self):
        zip_path = "./exponea_tests/elasticmock-testcases.zip"
        tests = ('1.json',)
        self.helper_json_parser(zip_path, tests)


if __name__ == '__main__':
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        suite.addTest(TestWes("json_parser_todo"))
        runner = unittest.TextTestRunner()
        runner.run(suite)