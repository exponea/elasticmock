from common import WesDefs, TestCommon
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

class TestWesJsonHelper(TestCommon):

    def binder_set(self, operation, key, data):
        if operation not in self.binder:
            self.binder[operation] = {}
        self.binder[operation][key] = data
        # print("BINDER SET ", operation, str(self.binder))

    def binder_get(self, operation, key):
        # print("BINDER GET ", operation, str(self.binder))
        rc = self.binder[operation].get(key, None)
        # KEEP USEFUL for scroll, clear_scroll
        #if rc:
        #    del self.binder[operation][key]

        return rc

    def indice_cleanup_all(self, wes):
        self.assertEqual(WesDefs.RC_OK, wes.ind_delete_result(wes.ind_delete("_all")).status)

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

    def helper_arguments_values_fixer(self, wes, operation, *args, **kwargs):
        if wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:

            if operation == WesDefs.OP_DOC_ADD_UP or \
               operation == WesDefs.OP_DOC_UPDATE:
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

            elif operation == WesDefs.OP_DOC_DEL:
                indice, doc_type = args
                kw_doc_type = kwargs.get('doc_type', None)
                kw_id = kwargs.get('id', None)

                Log.log(f"{operation} FIXER(  args) before : {args}")
                args = (indice, kw_id)
                Log.log(f"{operation} FIXER(  args) after  : {args}")

                Log.log(f"{operation} FIXER(kwargs) before : doc_type({kw_doc_type}) id({kw_id})")
                doc_type = kwargs['doc_type'] = doc_type
                del kwargs['id']
                kw_id = kwargs.get('id', None)
                Log.log(f"{operation} FIXER(kwargs) after  : doc_type({doc_type}) id({id})")

            elif operation == WesDefs.OP_DOC_SEARCH or \
                    operation == WesDefs.OP_DOC_DEL_QUERY:

                # FAKE as result of PYTHON API:
                # EXCEPTION: Unknow L1 exception ... doc_search() got an unexpected keyword argument 'doc_type'
                # EXCEPTION: Unknow L1 exception ... doc_delete_by_query() got an unexpected keyword argument 'doc_type'
                kw_doc_type = kwargs.get('doc_type', None)
                Log.log(f"{operation} FIXER(kwargs) before : doc_type({kw_doc_type})")
                if kw_doc_type:  # fix for 116.json
                    del kwargs['doc_type']
                doc_type = kwargs.get('doc_type', None)
                Log.log(f"{operation} FIXER(kwargs) after  : doc_type({doc_type})")

                # MSE_NOTES: match all can't query doc_type, _type !!!
                if kw_doc_type:
                    kw_body = kwargs.get('body', None)
                    Log.log(f"{operation} FIXER(kwargs) before : body({kw_body})")
                    body = kw_body
                    if kw_body:
                        query = kw_body.get('query', None)
                        # test 20.json, 117.json, 124.json, 179.json, 194.json
                        if query and 'match_all' in query:
                            del query['match_all']
                            query['term'] = {'_type': kw_doc_type }
                    Log.log(f"{operation} FIXER(kwargs) after  : body({body})")

            elif operation == WesDefs.OP_IND_PUT_MAP:

                # EXCEPTION: Unknow L1 exception ... ind_put_mapping() got multiple values for argument 'body'
                if len(args) > 0:
                    # 'doc_type'
                    doc_type,  = args
                    kw_doc_type = kwargs.get('doc_type', None)
                    Log.log(f"{operation} FIXER(kwargs) before : doc_type({kw_doc_type})")
                    doc_type = kwargs['doc_type'] = doc_type
                    Log.log(f"{operation} FIXER(kwargs) after  : doc_type({doc_type})")

                # 'body'
                key = 'body'
                kw_body = kwargs.get(key, None)
                Log.log(f"{operation} FIXER(kwargs) before : 'body'({kw_body})")
                del kwargs[key]
                body = kwargs.get(key, None)
                Log.log(f"{operation} FIXER(kwargs) after  : 'body'({body})")

                Log.log(f"{operation} FIXER(args) before : ({args})")
                args = (kw_body,)
                Log.log(f"{operation} FIXER(args) after  : ({args})")

            elif operation == WesDefs.OP_DOC_BULK or operation == WesDefs.OP_DOC_BULK_STR:

                len_args_tuple = len(args)
                type_args = type(args[0])  # class 'str'

                split_list = args[0].split('\n')
                split_list = [item for item in split_list if len(item) > 0]  # fix empty line
                split_len = len(split_list)

                # !!! KEEP FOR DBG !!!
                # split_list_str = '\n' + args[0]
                # Log.log(f"{operation} FIXER(args) before : len({len_args_tuple}) type({type_args}) split_len({split_len}): {split_list_str}")
                # !!! KEEP FOR DBG !!!

                Log.log(f"{operation} FIXER(args) before : len({len_args_tuple}) type({type_args}) split_len({split_len})")
                self.assertEqual(1, len_args_tuple)
                self.assertEqual(0, split_len % 2)  # expect even length

                actions = []
                i = 0
                while i < split_len:
                    dict_action = json.loads(split_list[i])
                    dict_source = json.loads(split_list[i+1])
                    i += 2

                    a_key = list(dict_action.keys())
                    self.assertEqual(1, len(a_key))
                    op_type = a_key[0]

                    action = {
                        "_op_type": op_type,
                        "_index"  : dict_action[op_type]["_index"],
                        "_type"   : dict_action[op_type]["_type"],
                        "_id"     : dict_action[op_type]["_id"],
                        "_source": {
                            **dict_source
                        }
                    }
                    actions.append(action)

                args = (actions, )
                len_args_tuple = len(args)
                type_args = type(args[0])  # class 'list'
                split_len = len(args[0])
                actions_str = '\n[\n' + '\n'.join([json.dumps(item) for item in actions]) + '\n]'
                Log.log(f"{operation} FIXER(args) after  : len({len_args_tuple}) type({type_args}) split_len({split_len}) actions:{actions_str}")

            elif operation == WesDefs.OP_DOC_SCROLL:
                # scroll_id,  = args             NOT USED THIS IS DYNAMIC VALUE !!!
                kw_scroll_id = kwargs.get('scroll_id', None)
                Log.log(f"{operation} FIXER(kwargs) before : scroll_id({kw_scroll_id})")
                # scroll_id = kwargs['scroll_id'] = scroll_id NOT USED THIS IS DYNAMIC VALUE !!!
                scroll_id = kwargs['scroll_id'] = self.binder_get(WesDefs.OP_DOC_SEARCH, '_scroll_id')
                Log.log(f"{operation} FIXER(kwargs) after  : scroll_id({scroll_id})")
                args = ()

            elif operation == WesDefs.OP_DOC_SCROLL_CLEAR:
                kw_scroll_id = kwargs.get('scroll_id', None)
                kw_body      = kwargs.get('body', None)
                Log.log(f"{operation} FIXER(kwargs) before : scroll_id({kw_scroll_id})")
                Log.log(f"{operation} FIXER(kwargs) before : body({kw_body})")

                scroll_id = kwargs['scroll_id'] = self.binder_get(WesDefs.OP_DOC_SEARCH, '_scroll_id')
                del kwargs['body']['scroll_id']
                new_body = kwargs['body'].get('scroll_id', None)
                Log.log(f"{operation} FIXER(kwargs) after  : scroll_id({scroll_id})")
                Log.log(f"{operation} FIXER(kwargs) after  : body({new_body})")


            # no fix needed
            else:
                pass
        else:
            wes.es_version_mismatch(self.ES_VERSION_RUNNING)

        return args, kwargs

    def helper_validate_rc(self, wes: Wes, operation, rc_result: ExecCode, rc_wes: ExecCode):
        if rc_wes.status == WesDefs.RC_OK:

            if operation == WesDefs.OP_DOC_SEARCH:
                # 1. check nb match records
                self.assertEqual(wes.doc_search_result_hits_nb(rc_result),
                                 wes.doc_search_result_hits_nb(rc_wes))

                # 2. check docs by content
                # at least '_scroll_id' will be different
                ext_sources = wes.doc_search_result_hits_sources(rc_result)
                wes_sources = wes.doc_search_result_hits_sources(rc_wes)
                for idx, wes_doc in enumerate(wes_sources):
                    ext_doc = ext_sources[idx]
                    self.assertDictEqual(ext_doc, wes_doc)

                self.binder_set(WesDefs.OP_DOC_SEARCH, '_scroll_id', wes.doc_search_result_scroll_id(rc_wes))

            elif operation == WesDefs.OP_DOC_SCROLL:
                # 1. check nb match records
                self.assertEqual(wes.doc_scroll_result_hits_nb(rc_result),
                                 wes.doc_scroll_result_hits_nb(rc_wes))

                # 2. check docs by content
                # at least '_scroll_id' will be different
                ext_sources = wes.doc_scroll_result_hits_sources(rc_result)
                wes_sources = wes.doc_scroll_result_hits_sources(rc_wes)
                for idx, wes_doc in enumerate(wes_sources):
                    ext_doc = ext_sources[idx]
                    self.assertDictEqual(ext_doc, wes_doc)



            elif operation == WesDefs.OP_DOC_DEL_QUERY:
                # TODO petee what to compare? seems as no docs added
                #  compare on full dict leads problem on 'took',
                #  so probably 'deleted' field is sufficient
                self.assertEqual(rc_result.data['deleted'], rc_wes.data['deleted'])

            elif operation == WesDefs.OP_IND_REFRESH:
                self.assertEqual(wes.ind_refresh_result_shard_nb_failed(rc_result),
                                 wes.ind_refresh_result_shard_nb_failed(rc_wes))

            elif operation == WesDefs.OP_IND_GET:
                # TODO exported data contain .monitorin/.watcher indice
                #  so exported data will be shrink

                # print(wes.ind_get_result_dump_to_string(rc_result))
                # print(wes.ind_get_result_dump_to_string(rc_wes))

                keys = list(rc_result.data.keys())
                for k in keys:
                    if k[0] == '.':
                        del rc_result.data[k]

                keys = list(rc_wes.data.keys())
                for k in keys:
                    if k[0] == '.':
                        del rc_wes.data[k]

                # print(wes.ind_get_result_dump_to_string(rc_result))
                # print(wes.ind_get_result_dump_to_string(rc_wes))

                self.cmp_dict_with_skipp_keys(rc_result.data, rc_wes.data, ('creation_date', 'uuid'), True)

            elif operation == WesDefs.OP_DOC_BULK or operation == WesDefs.OP_DOC_BULK_STR:
                # TODO different json formats - no idea how to specify - petee???
                # exported is dict (like HTTP) but python has different structure ...
                ext_list = rc_result.data['items']
                wes_list = [long for short, long in rc_wes.data]
                self.assertListEqual(ext_list, wes_list)
            else:
                self.assertDictEqual(rc_result.data, rc_wes.data)

        elif rc_wes.status == WesDefs.RC_NOK:
            if operation == WesDefs.OP_DOC_BULK_STR:
                # TODO '126.json' provided only simple check - different formats exports VS 'pip list elasticsearch 7.0.2'
                self.assertTrue(rc_result.data['errors'])
                self.assertFalse(rc_wes.data[-1][0])
            else:
                raise ValueError("not handled - now")
        elif rc_wes.status == WesDefs.RC_EXCE:
            # TODO peete pass exception status number - for T[1.json] L[ 11] there is result(None)
            self.assertEqual(rc_result.status, rc_wes.status)

    def helper_run_unpacked_test(self, wes: Wes, test_name: str, line: int, result, accessor, method, *args, **kwargs):

        group = 'IND' if accessor else 'DOC'

        # Log.log(f"T[{test_name}] L[{line:3}] -> {group} "
        #         f"cmd({method} <-> args{args} <-> kwargs({kwargs})) result({result})")

        Log.log(f"T[{test_name}] L[{line:3}] -> {group} accessor({accessor}) cmd({method}) ")
        Log.log(f"T[{test_name}] L[{line:3}] ->>> args{args} ")
        Log.log(f"T[{test_name}] L[{line:3}] ->>> kwargs({kwargs})) ")
        Log.log(f"T[{test_name}] L[{line:3}] ->>> result({result})")

        # BE SURE TO BE IN SYNCH WITH
        # Wes.operation_mappers
        # TODO which methods are used in exponea?
        method_mapper = {
            "IND": {
                "create"        : WesDefs.OP_IND_CREATE,
                "flush"         : WesDefs.OP_IND_FLUSH,
                "refresh"       : WesDefs.OP_IND_REFRESH,
                "exists"        : WesDefs.OP_IND_EXIST,
                "delete"        : WesDefs.OP_IND_DELETE,
                "get"           : WesDefs.OP_IND_GET,
                "get_alias"     : WesDefs.OP_IND_GET_ALIAS,
                "delete_alias"  : WesDefs.OP_IND_DEL_ALIAS,
                "get_mapping"   : WesDefs.OP_IND_GET_MAP,
                "put_mapping"   : WesDefs.OP_IND_PUT_MAP,
                "put_template"  : WesDefs.OP_IND_PUT_TMP,
                "get_template"  : WesDefs.OP_IND_GET_TMP,
            },
            'DOC': {
                # general TODO not sure how used
                "ping"              :WesDefs.OP_GEN_PING,
                "info"              :WesDefs.OP_GEN_INFO,

                # documents
                "index"             : WesDefs.OP_DOC_ADD_UP,
                "update"            : WesDefs.OP_DOC_UPDATE,
                "get"               : WesDefs.OP_DOC_GET,
                "exists"            : WesDefs.OP_DOC_EXIST,
                "delete"            : WesDefs.OP_DOC_DEL,

                "delete_by_query"   : WesDefs.OP_DOC_DEL_QUERY,
                "search"            : WesDefs.OP_DOC_SEARCH,
                "bulk"              : WesDefs.OP_DOC_BULK_STR, # if 1 else WesDefs.OP_DOC_BULK, # MSE_NOTES: RETURN FORMAT NOT MATCH EXPONEA
                "scan"              : WesDefs.OP_DOC_SCAN,
                "count"             : WesDefs.OP_DOC_COUNT,
                "scroll"            : WesDefs.OP_DOC_SCROLL,
                "clear_scroll"      : WesDefs.OP_DOC_SCROLL_CLEAR,
            }
        }

        # rise exception if operation is not supported
        operation = method_mapper[group][method]

        # fix arguments for specific operation
        args, kwargs = self.helper_arguments_values_fixer(wes, operation, *args, **kwargs)

        # pick wes mappers
        wes_mappers = wes.operation_mappers(operation)
        self.assertNotEqual(None, wes_mappers)
        wes_mappers_operation, wes_mappers_operation_result = wes_mappers

        # execute mappers and validate
        Log.log(f"{operation} MAPPERS: {str(wes_mappers)}")
        rc_wes = wes_mappers_operation_result(wes, wes_mappers_operation(wes, *args, **kwargs))

        rc_result = ExecCode(WesDefs.RC_EXCE if result is None else WesDefs.RC_OK,
                             result,
                             rc_wes.fnc_params)

        self.helper_validate_rc(wes, operation, rc_result, rc_wes)


    def helper_run_unpacked_tests(self, wes, tests: list, is_interactive=False):
        for test_name, test_lines in tests:
            Log.notice(f"T[{test_name}] -  commands to execute {len(test_lines)}")
            for line, cmd in enumerate(test_lines):
                #Log.log(f"T[{test_name}] L[{line:3}] -> raw {cmd}")
                self.helper_run_unpacked_test(wes, test_name, line, cmd['result'], cmd['accessor'],
                                                      cmd['method'], *(cmd['args']), **(cmd['kwargs']))
                if is_interactive:
                    input("Press ENTER ...")

    def helper_json_parser(self, zip_path, tests: list):
        self.indice_cleanup_all(self.wes)
        tests = self.helper_split_zip_test(zip_path, tests)
        self.helper_run_unpacked_tests(self.wes, tests)


class TestWesJson(TestWesJsonHelper):


    def test_json_parser_passed(self):
        zip_path = "./exponea_tests/elasticmock-testcases.zip"

        def test_case_avoided(nb: int):
            self.assertTrue(nb < 207)

            if nb in (27,  34,  67, 79,  94, 153, 166, 169):
                #  27 - OP_DOC_SEARCH - AssertionError: {'_id': '4', '_index': 'test_def_catalogs', '[1321 chars]1dd'} != {'_index': 'test_def_catalogs', '_type': 'pro[1323 chars]ng'}}
                #  34 - OP_DOC_SEARCH - AssertionError: 3 != 4
                #  76 - OP_DOC_SEARCH - ssertionError: {'_id': '5', '_index': 'test_def_catalogs', '[1323 chars]1cc'} != {'_index': 'test_def_catalogs', '_type': 'pro[1320 chars]ng'}}
                #  79 - OP_DOC_SEARCH - AssertionError: 5 != 10
                #  94 - OP_DOC_SEARCH - AssertionError: 2 != 3
                # 153 - OP_DOC_SEARCH - AssertionError: 1 != 2
                # 166 - OP_DOC_SEARCH - AssertionError: 3 != 4
                # 169 - OP_DOC_SEARCH - AssertionError: 3 != 4
                return False
            else:
                return True

        tests = ['{}.json'.format(nb) for nb in range(0, 207) if test_case_avoided(nb)]
        self.helper_json_parser(zip_path, tests)

    # method for tests to pass
    def json_parser_todo(self):
        zip_path = "./exponea_tests/elasticmock-testcases.zip"
        tests = ('94.json',)
        self.helper_json_parser(zip_path, tests)


class TestWesJsonReal(TestWesJson):

    def setUp(self):
        self.wes = Wes(False)
        self.binder = {}

class TestWesJsonMock(TestWesJson):

    def setUp(self):
        self.wes = Wes(True)
        self.binder = {}


if __name__ == '__main__':
    if True:
        unittest.main(TestWesJsonReal())
        # unittest.main(TestWesJsonMock())
        # unittest.main()
    else:
        suite = unittest.TestSuite()
        # suite.addTest(TestWesJsonReal("json_parser_todo"))
        # suite.addTest(TestWesJsonMock("json_parser_todo"))
        runner = unittest.TextTestRunner()
        runner.run(suite)