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

ind_str = "first_ind1"
ind_str2 = "first_ind2"
ind_str_doc_type = "first_ind1_docT"
ind_str_doc_type = "first_ind2_docT"

class TestWesHelper(unittest.TestCase):

    def _helper_exponea_zip_unpack(self, path_to_zip, file_to_extract="all"):
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

    def helper_exponea_split_zip_test(self, path_to_zip, file_to_extract="all"):

        raw_tests = self._helper_exponea_zip_unpack(path_to_zip, file_to_extract)

        print(raw_tests)
        tests = []
        for test_name, raw_test in raw_tests:
            split_test = raw_test.split('\n')
            split_list = [json.loads(test_cmd) for test_cmd in split_test if test_cmd.strip()]
            tests.append([test_name, split_list])
        return tests

    def helper_exponea_run_unpacked_test(self, wes: Wes, test_name: str, line: int, result, accessor, method, *args, **kwargs):

        group = 'IND' if accessor else 'DOC'

        Log.log(f"T[{test_name}] L[{line:3}] -> {group} "
                f"cmd({method} <-> args{args} <-> kwargs({kwargs})) result({result})")

        # BE SURE TO BE IN SYNCH WITH
        # Wes.operation_mappers
        # TODO which methods are used in exponea?
        method_mapper = {
            "IND": {
                "create"     : Wes.OP_IND_CREATE,
                "flush"      : Wes.OP_IND_FLUSH,
                "refresh"    : Wes.OP_IND_REFRESH,
                "exists"     : Wes.OP_IND_EXIST,
                "delete"     : Wes.OP_IND_DELETE,
                "get_mapping": Wes.OP_IND_GET_MAP,
                "put_mapping": Wes.OP_IND_PUT_MAP,
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

        wes_mappers = wes.operation_mappers(method_mapper[group][method])
        # some test case operation not covered
        self.assertNotEqual(None, wes_mappers)
        operation, operation_result = wes_mappers
        rc = operation_result(wes, operation(wes, *args, **kwargs)).data

        # TODO handle results

    def helper_exponea_run_unpacked_tests(self, wes, tests: list):
        for test_name, test_lines in tests:
            Log.notice(f"T[{test_name}] - number commands to execute {len(test_lines)}")
            for line, cmd in enumerate(test_lines):
                #Log.log(f"T[{test_name}] L[{line:3}] -> raw {cmd}")
                self. helper_exponea_run_unpacked_test(wes, test_name, line, cmd['result'], cmd['accessor'],
                                                       cmd['method'], *(cmd['args']), **(cmd['kwargs']))
                return





class TestWes(TestWesHelper):

    def force_reindex(self, wes):
        self.assertEqual(Wes.RC_OK, wes.ind_flush_result(wes.ind_flush(wait_if_ongoing=True)).status)
        self.assertEqual(Wes.RC_OK, wes.ind_refresh_result(wes.ind_refresh()).status)

        self.assertEqual(Wes.RC_OK, wes.ind_get_mapping_result(wes.ind_get_mapping()).status)

    def indice_create_exists(self, wes, ind_str):
        wes.ind_delete_result(wes.ind_delete(ind_str))
        self.assertEqual(Wes.RC_OK, wes.ind_create_result(wes.ind_create(ind_str)).status)
        self.assertEqual(True, wes.ind_exist_result(wes.ind_exist(ind_str)).data)

        self.assertEqual(Wes.RC_OK, wes.ind_get_mapping_result(wes.ind_get_mapping()).status)

    def test_indice_basic(self):
        wes = Wes()
        global ind_str

        #
        self.indice_create_exists(wes, ind_str)

        # re-create
        self.assertTrue(isinstance(wes.ind_create_result(wes.ind_create(ind_str)).data, RequestError))
        # unknown
        self.assertEqual(False, wes.ind_exist_result(wes.ind_exist("unknown ind_str")).data)

        self.assertEqual(True, wes.ind_delete_result(wes.ind_delete(ind_str)).data.get('acknowledged', False))
        self.assertEqual(False, wes.ind_exist_result(wes.ind_exist(ind_str)).data)
        self.assertTrue(isinstance(wes.ind_delete_result(wes.ind_delete(ind_str)).data, NotFoundError))


    def documents_create(self, wes, ind_str, doc_type):
        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2", "sentence": "The SLOVAKA is a country"}
        doc3 = {"city": "Bratislava3", "country": "SLOVAKIA",  "sentence": "The slovakia is a country"}
        doc4 = {"city": "Bratislava4", "country": "SLOVAKIA4", "sentence": "The small country is slovakia"}
        doc5 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}

        #  TODO priority???
        # 1. exception
        # 2. IMPO_AU_1 vs. IMPO_AU_2 consider 'RC - 3 codes'
        # add docs                                                                                                               MSE_NOTES:      IMPO_AU_1          IntperOper    IntChangedByUpd                            IMPO_AU_2
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type=doc_type, id=1)).data['result'])  # MSE_NOTES: 'result': 'created' '_seq_no': 0  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type=doc_type, id=2)).data['result'])  # MSE_NOTES: 'result': 'created' '_seq_no': 1  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type=doc_type, id=3)).data['result'])  # MSE_NOTES: 'result': 'created' '_seq_no': 2  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        self.assertEqual("updated", wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type=doc_type, id=3)).data['result'])  # MSE_NOTES: 'result': 'updated' '_seq_no': 3  '_version': 2,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type=doc_type, id=4)).data['result'])
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc5, doc_type=doc_type, id=5)).data['result'])

        self.force_reindex(wes)


    def test_documents_basic(self):
        wes = Wes()
        global ind_str
        global ind_str_doc_type

        self.indice_create_exists(wes, ind_str)
        self.documents_create(wes, ind_str, ind_str_doc_type)
        #                                                                                                  MSE_NOTES:  IMPO_GET_1 ok/exc                                    IMPO_GET_2
        self.assertEqual(Wes.RC_OK, wes.doc_get_result(wes.doc_get(ind_str, 1, doc_type=ind_str_doc_type)).status)  # MSE_NOTES: 'found': True,                 '_seq_no': 0,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia1'}
        self.assertEqual(Wes.RC_OK, wes.doc_get_result(wes.doc_get(ind_str, 2, doc_type=ind_str_doc_type)).status)  # MSE_NOTES: 'found': True,                 '_seq_no': 1,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        self.assertEqual(Wes.RC_OK, wes.doc_get_result(wes.doc_get(ind_str, 3, doc_type=ind_str_doc_type)).status)  # MSE_NOTES: 'found': True,                 '_seq_no': 2,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        self.assertTrue(isinstance(wes.doc_get_result(wes.doc_get(ind_str, 9, doc_type=ind_str_doc_type)).data, NotFoundError))  # MSE_NOTES:  WesNotFoundError !!!

        #MSE_NOTES: #1 400 - illegal_argument_exception - Rejecting mapping update to [first_ind1] as the final mapping would have more than 1 type: [any, any2]
        doc6 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}
        self.assertTrue(isinstance(wes.doc_addup_result(wes.doc_addup(ind_str, doc6, doc_type="any2", id=6)).data, RequestError))

        Log.notice2("------------------------------------------------------------------")
        self.assertTrue(wes.doc_exists_result(wes.doc_exists(ind_str, 5)).data)
        self.assertFalse(wes.doc_exists_result(wes.doc_exists(ind_str, 9)).data)

    def test_query_basic(self):
        wes = Wes()
        global ind_str
        global ind_str_doc_type

        self.indice_create_exists(wes, ind_str)
        self.documents_create(wes, ind_str, ind_str_doc_type)

        ###########################################################
        # QUERY(all)
        ###########################################################
        # MSE_NOTES: #1 search ALL in DB
        self.assertEqual(Wes.RC_OK, wes.doc_search_result(wes.doc_search()).status)
        # MSE_NOTES: #2 search ALL in specific INDICE
        self.assertEqual(5, wes.doc_search_result(wes.doc_search(index=ind_str)).data['hits']['total']['value'])
        # MSE_NOTES: #3 equivalent to #2
        body={"query": {"match_all": {}}}
        self.assertEqual(5, wes.doc_search_result(wes.doc_search(index=ind_str, body=body))
                         .data['hits']['total']['value'])

        ######################################################################################################################
        # MSE_NOTES: #4 hint list FILTER
        #               - 'from' - specify START element in 'hintLIST' witch MATCH query
        #               - 'size' - specify RANGE/MAXIMUM from 'hintLIST' <'from', 'from'+'size')
        #               E.G. "size": 0 == returns just COUNT
        ######################################################################################################################
        body = {"from": 0, "size": 2,
                "query": {"match_all": {}}}
        rc = wes.doc_search_result(wes.doc_search(index=ind_str, body=body))
        self.assertEqual(2, len(rc.data['hits']['hits']))

        ######################################################################################################################
        # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(inSensitive)
        ######################################################################################################################
        body = {"from": 0, "size": 10,
                #"query": {"match": {}} EXCEPTION:  400 - parsing_exception - No text specified for text query
                "query": {"match": {"country": "slovakia"}}}
        self.assertEqual(2, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        body = {"from": 0, "size": 10,
                "query": {"match": {"sentence": "slovakia"}}}
        self.assertEqual(4, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        ######################################################################################################################
        # MSE_NOTES: #6 QUERY(matchphrase) MATCH(subSentence+wholeWord+phraseOrder) CASE(in-sensitive)
        #              - ORDER IGNORE spaces and punctation !!!
        ######################################################################################################################
        body = {"from": 0, "size": 10,
                "query": {"match_phrase": {"country": "slovakia"}}}
        self.assertEqual(2, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        body = {"from": 0, "size": 10,
                "query": {"match_phrase": {"sentence": "slovakia is"}}}
        self.assertEqual(2, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        ######################################################################################################################
        # MSE_NOTES: #7 QUERY(term) MATCH(exact) CASE(in-sensitive)
        ######################################################################################################################
        body = {"from": 0, "size": 10,
                "query": {"term": {"country": "slovakia"}}}
        self.assertEqual(2, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        body = {"from": 0, "size": 10,
                "query": {"term": {"sentence": "slovakia is"}}}
        self.assertEqual(0, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])


    def test_complex_queries(self):
        wes = Wes()
        global ind_str
        global ind_str_doc_type

        self.indice_create_exists(wes, ind_str)
        self.documents_create(wes, ind_str, ind_str_doc_type)

        # 1 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        q1 = {"match": {"sentence": "slovakia"}}
        self.assertEqual(4, wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q1})).data['hits']['total']['value'])
        # 2 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        q2 = {"match": {"sentence": "small"}}                                                   # MSE_NOTES:
        self.assertEqual(2, wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q2})).data['hits']['total']['value'])

        Log.notice("--------------------------------------------------------------------------------------")
        ######################################################################################################################
        # MSE_NOTES: #3 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
        #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        ######################################################################################################################
        body = {"query": {"bool": {"must_not": q2, "should": q1}}}
        self.assertEqual(2, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        ######################################################################################################################
        # MSE_NOTES: #4 QUERY(regexp) MATCH(regexp) CASE(in-sensitive)
        ######################################################################################################################
        q3 = {"regexp": {"sentence": ".*"}}
        self.assertEqual(5, wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q3})).data['hits']['total']['value'])

    def test_mappings_get(self):
        # MSE_NOTES: mapping is process of defining how documents looks like (which fields contains, field types, how is filed indexed)
        # types of mapping fileds:
        # - keyword
        # - text
        # - datetime (is recognized based on format)
        #  = setting correct times help aggregations
        #  = u can't change mapping if docs present in IND
        wes = Wes()
        global ind_str
        global ind_str_doc_type

        self.indice_create_exists(wes, ind_str)
        self.documents_create(wes, ind_str, ind_str_doc_type)

        ind_str2 = "first_ind2"
        self.indice_create_exists(wes, ind_str2)

        self.assertEqual(2, len(wes.ind_get_mapping_result(wes.ind_get_mapping()).data.keys()))
        self.assertEqual(1, len(wes.ind_get_mapping_result(wes.ind_get_mapping(ind_str)).data.keys()))

        Log.notice("--------------------------------------------------------------------------------------")

        # MSE_NOTES: #1 400 - illegal_argument_exception - Types cannot be provided in get mapping requests, unless include_type_name is set to true.
        self.assertTrue(isinstance(wes.ind_get_mapping_result(wes.ind_get_mapping(ind_str, doc_type=ind_str_doc_type)).data, RequestError))
        self.assertEqual(1, len(wes.ind_get_mapping_result(wes.ind_get_mapping(ind_str, doc_type=ind_str_doc_type, include_type_name=True)).data.keys()))

    def test_mappings_get_put(self):
        # MSE_NOTES: mapping is process of defining how documents looks like (which fields contains, field types, how is filed indexed)
        # It enables in faster search retrieval and aggregations. Hence, your mapping defines how effectively you can handle your data.
        # A bad mapping can have severe consequences on the performance of your system."
        # types of mapping fields:
        # - keyword
        # - text
        # - datetime (is recognized based on format)
        #  = setting correct times help aggregations
        #  = u CANT CHANGE MAPPING if docs present in IND (DELETE IND FIRTS)
        wes = Wes()
        global ind_str
        global ind_str2
        global ind_str_doc_type
        global ind_str_doc_type2

        self.indice_create_exists(wes, ind_str)
        self.indice_create_exists(wes, ind_str2)

        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country", "datetime" : "2019,01,02,03,12,00"}
        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type=ind_str_doc_type, id=1))
        wes.ind_get_mapping_result(wes.ind_get_mapping())
        Log.notice("--------------------------------------------------------------------------------------")
        # {'first_ind1': {'mappings':
        map_new = {
            'properties': {'city': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                           'country': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                           # datetime': {'type': 'text',
                           #              'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                           # MSE_NOTES: #1 CHANGE TYPE+FORMAT
                           'datetime': {'type': 'date',
                                        'format': "yyyy,MM,dd,hh,mm,ss" },
                           'sentence': {'type': 'text',
                                        'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}}
          }
         # }, 'first_ind2': {'mappings': {}}}

        # OP_IND_PUT_MAP 405 - {'error': 'Incorrect HTTP method for uri [/_mapping] and method [PUT], allowed: [GET]', 'status': 405}
        self.assertEqual(405, wes.ind_put_mapping_result(wes.ind_put_mapping(map_new)).data.status_code)
        # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.
        self.assertEqual(400, wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str)).data.status_code)
        # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - mapper [datetime] of different type, current_type [text], merged_type [date]
        self.assertEqual(400, wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).data.status_code)

        # MSE_NOTES: #2 u can't change mapping if docs present in IND !!!
        self.indice_create_exists(wes, ind_str)
        # MSE_NOTES: #3 u should specified IND + DOC_TYPE !!!
        self.assertEqual(Wes.RC_OK, wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).status)

        Log.notice("--------------------------------------------------------------------------------------")
        # MSE_NOTES: #4 type was changed :)
        # IND[first_ind1]
        # city: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        # country: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        # datetime: {'type': 'date', 'format': 'yyyy,MM,dd,hh,mm,ss'}
        # sentence: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        #
        # IND[first_ind2]: Missing mappings
        self.assertEqual(2, len(wes.ind_get_mapping_result(wes.ind_get_mapping()).data.keys()))

    def test_aggregations(self):
        # MSE_NOTES: Date Histogram Aggregations,
        #     Aggregations are one of the most important application of Elasticsearch.
        #     It provides you with quick powerful analysis of your data! Below we have discussed aggregations over date values.,
        #     A lot of analysis happen on a time-series scales. For example: Quarterly sales of iphone across the world.
        #     Therefore it is essential to have an fast aggregation done over large dataset under different granular scales.
        # 	  ES provides such an aggregation via date histogram aggregation. The granularities over which you can do aggregations are:\,
        #     1. year,
        #     2. quater",
        #     "3. month,
        #     "4. hour,
        #     "5. week,
        #     "6. day",
        #     "7. hour",
        #     "8. minute",
        #     "9. second",
        #     "10. milisecond",
        # types of mapping fields:
        # - keyword
        # - text
        # - datetime (is recognized based on format)
        #  MSE_NOTES: setting correct times help aggregations
        #  MSE_NOTES: u CAN'T CHANGE MAPPING if docs present in IND (DELETE IND FIRTS)
        wes = Wes()
        global ind_str
        global ind_str_doc_type
        self.indice_create_exists(wes, ind_str)

        map_new = {
            'properties': {'city': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                           'country': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                           # datetime': {'type': 'text',
                           #              'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                           # MSE_NOTES: #1 CHANGE TYPE+FORMAT
                           'datetime': {'type': 'date',
                                        'format': "yyyy,MM,dd,hh,mm,ss" },
                        }
        }
        self.assertEqual(Wes.RC_OK, wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).status)

        doc1 = {"city":"Bangalore", "country": "India", "datetime":"2018,01,01,10,20,00"} #datetime format: yyyy,MM,dd,hh,mm,ss",
        doc2 = {"city":"London", "country": "England", "datetime":"2018,01,02,03,12,00"}
        doc3 = {"city":"Los Angeles", "country": "USA", "datetime":"2018,04,19,05,02,00"}
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type=ind_str_doc_type, id=1)).data['result'])
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type=ind_str_doc_type, id=2)).data['result'])
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type=ind_str_doc_type, id=3)).data['result'])

        self.force_reindex(wes)

        body = {"from": 0, "size": 10,"query": {"match_all": {}}}
        self.assertEqual(3, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        Log.notice("--------------------------------------------------------------------------------------")
        body = {"from": 0, "size": 10,                      # MSE_NOTES: #1 QUERY + AGGREGATION
                "query": {"match_all": {}},                 #
                "aggs": { "country":                        # agg on 'country' field
                            { "date_histogram":             # kind of agg (provided by ES)
                                {                           #
                                    "field": "datetime",    # add over 'datetime' fiels
                                    "interval": "year"      # interval
                                }
                            }
                        }
               }

        # AGGS:
        # country
        # {'key_as_string': '2018,01,01,12,00,00', 'key': 1514764800000, 'doc_count': 3}
        rc = wes.doc_search_result(wes.doc_search(index=ind_str, body=body))
        self.assertNotEqual(None, rc.data.get('aggregations', None))
        print("kuk", rc.data.get('aggregations'))
        self.assertEqual(1, len(rc.data['aggregations']['country']['buckets']))

        Log.notice("--------------------------------------------------------------------------------------")
        doc4 = {"city": "Sydney", "country": "Australia", "datetime": "2019,04,19,05,02,00"}
        self.assertEqual("created", wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type=ind_str_doc_type, id=4)).data['result'])

        self.force_reindex(wes)

        # AGGS:
        # country
        # {'key_as_string': '2018,01,01,12,00,00', 'key': 1514764800000, 'doc_count': 3}
        # {'key_as_string': '2019,01,01,12,00,00', 'key': 1546300800000, 'doc_count': 1}
        rc = wes.doc_search_result(wes.doc_search(index=ind_str, body=body))
        self.assertNotEqual(None, rc.data.get('aggregations', None))
        print("kuk", rc.data.get('aggregations'))
        self.assertEqual(2, len(rc.data['aggregations']['country']['buckets']))

    def test_bulk(self):
        # MSE_NOTES: for 'bulk' and 'scan' API IMPORT 'from elasticsearch import helpers'
        wes = Wes()
        global ind_str
        global ind_str_doc_type
        self.indice_create_exists(wes, ind_str)

        # INSERT 0,1,2,3,4
        actions = [
            {
                "_index": ind_str,
                "_type": ind_str_doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(0, 5)
        ]
        self.assertEqual(Wes.RC_OK, wes.doc_bulk_result(wes.doc_bulk(actions)).status)
        self.force_reindex(wes)

        body = {"query": {"match_all": {}}}
        self.assertEqual(5, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        Log.notice("--------------------------------------------------------------------------------------")

        # DELETE 3,4
        actions = [
            {
                '_op_type': "delete",
                "_index": ind_str,
                "_type": ind_str_doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(3, 5)
        ]
        self.assertEqual(Wes.RC_OK, wes.doc_bulk_result(wes.doc_bulk(actions)).status)
        self.force_reindex(wes)

        body = {"query": {"match_all": {}}}
        self.assertEqual(3, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        Log.notice("--------------------------------------------------------------------------------------")

        # EXISTS 0,1,2 INSERT 1,2,3 CONFLICT ON 1, 2
        actions = [
            {
                '_op_type': "create",
                "_index": ind_str,
                "_type": ind_str_doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(1, 4)
        ]
        self.assertEqual(Wes.RC_NOK, wes.doc_bulk_result(wes.doc_bulk(actions)).status)
        self.force_reindex(wes)

        # FINAL 0,1,2,3
        self.assertEqual(4, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])


    def test_scan(self):
        # MSE_NOTES: for 'bulk' and 'scan' API IMPORT 'from elasticsearch import helpers'
        wes = Wes()
        global ind_str
        global ind_str_doc_type
        self.indice_create_exists(wes, ind_str)

        actions = [
            {
                "_index": ind_str,
                "_type": ind_str_doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(0, 5)
        ]
        self.assertEqual(Wes.RC_OK, wes.doc_bulk_result(wes.doc_bulk(actions)).status)
        self.force_reindex(wes)

        body = {"query": {"match_all": {}}}
        self.assertEqual(5, wes.doc_search_result(wes.doc_search(index=ind_str, body=body)).data['hits']['total']['value'])

        Log.notice("--------------------------------------------------------------------------------------")
        self.assertEqual(Wes.RC_OK, wes.doc_scan_result(wes.doc_scan(query=body)).status)

        Log.notice("--------------------------------------------------------------------------------------")
        self.assertEqual(Wes.RC_EXCE, wes.doc_scan_result(wes.doc_scan(index='pako', query=body)).status)

    def test_count(self):
        wes = Wes()
        global ind_str
        global ind_str_doc_type
        self.indice_create_exists(wes, ind_str)
        self.documents_create(wes, ind_str, ind_str_doc_type)

        # 1.
        self.assertEqual(Wes.RC_OK, wes.doc_count_result(wes.doc_count()).status)

        # 2.
        # EXEPTION KEY[first_ind1] - 400 - parsing_exception - request does not support [from]
        body = {"from": 0, "size": 10,
                #"query": {"match": {}} EXCEPTION:  400 - parsing_exception - No text specified for text query
                "query": {"match": {"country": "slovakia"}}}
        self.assertEqual(Wes.RC_EXCE, wes.doc_count_result(wes.doc_count(index=ind_str, body=body)).status)

        # 3.
        # EXEPTION KEY[first_ind1] - 400 - parsing_exception - request does not support [size]
        body = {"size": 10,
                "query": {"match": {"country": "slovakia"}}}
        self.assertEqual(Wes.RC_EXCE, wes.doc_count_result(wes.doc_count(index=ind_str, body=body)).status)

        # 4.
        body = {"query": {"match": {"country": "slovakia"}}}
        self.assertEqual(2, wes.doc_count_result(wes.doc_count(index=ind_str, body=body)).data['count'])

    def test_json_parser(self):
        wes = Wes()
        zip_path = "/home/msestrie/MSE_PROJECT/PYTHON/CVICENIA/elasticmock/wes/exponea_tests/elasticmock-testcases.zip"
        tests = self.helper_exponea_split_zip_test(zip_path, ('0.json',))
        self.assertEqual(1, len(tests))
        self.helper_exponea_run_unpacked_tests(wes, tests)


if __name__ == '__main__':
    if True:
        unittest.main()
    else:
        suite = unittest.TestSuite()
        suite.addTest(TestWes("test_scan"))
        runner = unittest.TextTestRunner()
        runner.run(suite)