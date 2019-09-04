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

ind_str = "first_ind1"
ind_str2 = "first_ind2"
ind_str_doc_type = "first_ind1_docT"
ind_str_doc_type = "first_ind2_docT"

from common import WesDefs

class TestWesHelper(unittest.TestCase):

    def indice_cleanup_all(self, wes):
        self.assertEqual(WesDefs.RC_OK, wes.ind_delete_result(wes.ind_delete("_all")).status)

    def force_reindex(self, wes):
        self.assertEqual(WesDefs.RC_OK, wes.ind_flush_result(wes.ind_flush(wait_if_ongoing=True)).status)
        self.assertEqual(WesDefs.RC_OK, wes.ind_refresh_result(wes.ind_refresh()).status)

        self.assertEqual(WesDefs.RC_OK, wes.ind_get_mapping_result(wes.ind_get_mapping()).status)

    def indice_create_exists(self, wes, ind_str):
        wes.ind_delete_result(wes.ind_delete(ind_str))
        self.assertEqual(WesDefs.RC_OK, wes.ind_create_result(wes.ind_create(ind_str)).status)
        self.assertEqual(True, wes.ind_exist_result(wes.ind_exist(ind_str)).data)

        self.assertEqual(WesDefs.RC_OK, wes.ind_get_mapping_result(wes.ind_get_mapping()).status)

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


class TestWes(TestWesHelper):

    def setUp(self):
        self.wes = Wes()

    def test_general(self):

        global ind_str
        self.indice_cleanup_all(self.wes)

        self.assertEqual(True, self.wes.gen_ping_result(self.wes.gen_ping()).data)
        self.assertEqual(WesDefs.RC_OK, self.wes.gen_info_result(self.wes.gen_info()).status)


    def test_indice_basic(self):

        global ind_str
        self.indice_cleanup_all(self.wes)

        # create
        self.indice_create_exists(self.wes, ind_str)
        # re-create
        self.assertTrue(isinstance(self.wes.ind_create_result(self.wes.ind_create(ind_str)).data, RequestError))
        # unknown
        self.assertEqual(False, self.wes.ind_exist_result(self.wes.ind_exist("unknown ind_str")).data)
        # delete
        self.assertEqual(True, self.wes.ind_delete_result(self.wes.ind_delete(ind_str)).data.get('acknowledged', False))
        self.assertEqual(False, self.wes.ind_exist_result(self.wes.ind_exist(ind_str)).data)
        self.assertTrue(isinstance(self.wes.ind_delete_result(self.wes.ind_delete(ind_str)).data, NotFoundError))


    def test_documents_basic(self):

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)

        self.indice_create_exists(self.wes, ind_str)
        self.documents_create(self.wes, ind_str, ind_str_doc_type)
        #                                                                                                  MSE_NOTES:  IMPO_GET_1 ok/exc                                    IMPO_GET_2
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_get_result(self.wes.doc_get(ind_str, 1, doc_type=ind_str_doc_type)).status)  # MSE_NOTES: 'found': True,                 '_seq_no': 0,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia1'}
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_get_result(self.wes.doc_get(ind_str, 2, doc_type=ind_str_doc_type)).status)  # MSE_NOTES: 'found': True,                 '_seq_no': 1,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_get_result(self.wes.doc_get(ind_str, 3, doc_type=ind_str_doc_type)).status)  # MSE_NOTES: 'found': True,                 '_seq_no': 2,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        self.assertTrue(isinstance(self.wes.doc_get_result(self.wes.doc_get(ind_str, 9, doc_type=ind_str_doc_type)).data, NotFoundError))  # MSE_NOTES:  WesNotFoundError !!!

        # check docs existence
        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            self.assertTrue(self.wes.doc_exists_result(self.wes.doc_exists(ind_str, 5)).data)
            self.assertFalse(self.wes.doc_exists_result(self.wes.doc_exists(ind_str, 9)).data)
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            # TODO - python lib API  ( use 'doc_type' because default goes as '_doc' )
            self.assertTrue(self.wes.doc_exists_result(self.wes.doc_exists(ind_str, 5, doc_type=ind_str_doc_type)).data)
            self.assertFalse(self.wes.doc_exists_result(self.wes.doc_exists(ind_str, 9, doc_type=ind_str_doc_type)).data)
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)

        # check doc with different type vs. indice
        doc6 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}
        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            # MSE_NOTES: #1 400 - illegal_argument_exception - Rejecting mapping update to [first_ind1] as the final mapping would have more than 1 type: [any, any2]
            self.assertTrue(isinstance(self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc6, doc_type="any2", id=6)).data, RequestError))
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            # TODO - python lib API  ( use 'doc_type' because default goes as '_doc' )
            # MSE_NOTES: no EXEPTION !!!
            self.assertEqual('created', self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc6, doc_type="any2", id=6)).data['result'])
        else:
            WesDefs.es_version_mismatch(self.ES_VERSION_RUNNING)


    def test_query_basic(self):

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)

        self.indice_create_exists(self.wes, ind_str)
        self.documents_create(self.wes, ind_str, ind_str_doc_type)

        ###########################################################
        # QUERY(all)
        ###########################################################
        # MSE_NOTES: #1 search ALL in DB
        self.assertEqual(5, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search())))
        # MSE_NOTES: #2 search ALL in specific INDICE
        self.assertEqual(5, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str))))
        # MSE_NOTES: #3 equivalent to #2
        body={"query": {"match_all": {}}}
        self.assertEqual(5, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        ######################################################################################################################
        # MSE_NOTES: #4 hint list FILTER
        #               - 'from' - specify START element in 'hintLIST' witch MATCH query
        #               - 'size' - specify RANGE/MAXIMUM from 'hintLIST' <'from', 'from'+'size')
        #               E.G. "size": 0 == returns just COUNT
        ######################################################################################################################
        body = {"from": 1, "size": 2,
                "query": {"match_all": {}}}
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))
        self.assertEqual(2, len(self.wes.doc_search_result_hits_sources(rc)))

        ######################################################################################################################
        # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        ######################################################################################################################
        body = {"from": 0, "size": 10,
                #"query": {"match": {}} EXCEPTION:  400 - parsing_exception - No text specified for text query
                "query": {"match": {"country": "slovakia"}}}
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        body = {"from": 0, "size": 10,
                "query": {"match": {"sentence": "slovakia"}}}
        self.assertEqual(4, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        ######################################################################################################################
        # MSE_NOTES: #6 QUERY(matchphrase) MATCH(subSentence+wholeWord+phraseOrder) CASE(in-sensitive)
        #              - ORDER IGNORE spaces and punctation !!!
        ######################################################################################################################
        body = {"from": 0, "size": 10,
                "query": {"match_phrase": {"country": "slovakia"}}}
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        body = {"from": 0, "size": 10,
                "query": {"match_phrase": {"sentence": "slovakia is"}}}
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        ######################################################################################################################
        # MSE_NOTES: #7 QUERY(term) MATCH(exact BUT lookup is stored with striped WHITESPACES) CASE(in-sensitive)
        ######################################################################################################################
        body = {"from": 0, "size": 10,
                "query": {"term": {"country": "slovakia"}}}
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        body = {"from": 0, "size": 10,
                "query": {"term": {"country": "slovakia "}}}  # MSE_NOTES: MATCH(exact BUT lookup is stored with striped WHITESPACES)
        self.assertEqual(0, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))


        body = {"from": 0, "size": 10,
                "query": {"term": {"sentence": "slovakia is"}}}
        self.assertEqual(0, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))


    def test_complex_queries(self):

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)

        self.indice_create_exists(self.wes, ind_str)
        self.documents_create(self.wes, ind_str, ind_str_doc_type)

        # 1 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        q1 = {"match": {"sentence": "slovakia"}}
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body={"query": q1}))
        self.assertEqual(4, self.wes.doc_search_result_hits_nb(rc))
        documents = self.wes.doc_search_result_hits_sources(rc)
        for doc in documents:
            self.assertTrue(int(doc['_id']) in [1, 3, 4, 5])  # except doc _id==2

        # 2 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        q2 = {"match": {"sentence": "small"}}                                                   # MSE_NOTES:
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body={"query": q2}))
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(rc))
        documents = self.wes.doc_search_result_hits_sources(rc)
        for doc in documents:
            self.assertTrue(int(doc['_id']) in [4, 5])

        ######################################################################################################################
        # MSE_NOTES: #3 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
        #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        ######################################################################################################################
        body = {"query": {"bool": {"must_not": q2, "should": q1}}}
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(rc))
        documents = self.wes.doc_search_result_hits_sources(rc)
        for doc in documents:
            self.assertTrue(int(doc['_id']) in [1, 3])

        ######################################################################################################################
        # MSE_NOTES: #4 QUERY(regexp) MATCH(regexp) CASE()
        ######################################################################################################################
        q3 = {"regexp": {"sentence": "sma.*"}}
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body={"query": q3}))
        self.assertEqual(2, self.wes.doc_search_result_hits_nb(rc))
        documents = self.wes.doc_search_result_hits_sources(rc)
        for doc in documents:
            self.assertTrue(int(doc['_id']) in [4, 5])


    def test_mappings_get(self):
        # MSE_NOTES: mapping is process of defining how documents looks like (which fields contains, field types, how is filed indexed)
        # types of mapping fileds:
        # - keyword
        # - text
        # - datetime (is recognized based on format)
        #  = setting correct times help aggregations
        #  = u can't change mapping if docs present in IND

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)

        self.indice_create_exists(self.wes, ind_str)
        self.documents_create(self.wes, ind_str, ind_str_doc_type)

        ind_str2 = "first_ind2"
        self.indice_create_exists(self.wes, ind_str2)

        self.assertEqual(2, len(self.wes.ind_get_mapping_result(self.wes.ind_get_mapping()).data.keys()))
        self.assertEqual(1, len(self.wes.ind_get_mapping_result(self.wes.ind_get_mapping(ind_str)).data.keys()))

        Log.notice("--------------------------------------------------------------------------------------")


        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            # MSE_NOTES: #1 400 - illegal_argument_exception - Types cannot be provided in get mapping requests, unless include_type_name is set to true.
            self.assertTrue(isinstance(self.wes.ind_get_mapping_result(self.wes.ind_get_mapping(ind_str, doc_type=ind_str_doc_type)).data, RequestError))
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            # TODO - no exception raised in comparision with ES_VERSION_7_3_0
            self.assertEqual(WesDefs.RC_OK, self.wes.ind_get_mapping_result(self.wes.ind_get_mapping(ind_str, doc_type=ind_str_doc_type)).status)

            self.assertEqual(WesDefs.RC_OK, self.wes.ind_get_mapping_result(self.wes.ind_get_mapping(ind_str)).status)
        else:
            self.wes.es_version_mismatch(self.ES_VERSION_RUNNING)

        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            self.assertEqual(1, len(self.wes.ind_get_mapping_result(self.wes.ind_get_mapping(ind_str, doc_type=ind_str_doc_type, include_type_name=True)).data.keys()))
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            # TODO - 400 - illegal_argument_exception - request [/first_ind1/_mapping/first_ind2_docT] contains unrecognized parameter: [include_type_name]
            self.assertTrue(isinstance(self.wes.ind_get_mapping_result(self.wes.ind_get_mapping(ind_str, doc_type=ind_str_doc_type, include_type_name=True)).data, RequestError))
        else:
            self.wes.es_version_mismatch(self.ES_VERSION_RUNNING)

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

        global ind_str
        global ind_str2
        global ind_str_doc_type
        global ind_str_doc_type2
        self.indice_cleanup_all(self.wes)

        self.indice_create_exists(self.wes, ind_str)
        self.indice_create_exists(self.wes, ind_str2)

        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country", "datetime" : "2019,01,02,03,12,00"}
        self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc1, doc_type=ind_str_doc_type, id=1))
        self.wes.ind_get_mapping_result(self.wes.ind_get_mapping())
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

        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            # OP_IND_PUT_MAP 405 - {'error': 'Incorrect HTTP method for uri [/_mapping] and method [PUT], allowed: [GET]', 'status': 405}
            self.assertEqual(405, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new)).data.status_code)
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            self.assertEqual(400, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new)).data.status_code)
        else:
            self.wes.es_version_mismatch(self.ES_VERSION_RUNNING)

        # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.
        self.assertEqual(400, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str)).data.status_code)
        # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - mapper [datetime] of different type, current_type [text], merged_type [date]
        self.assertEqual(400, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).data.status_code)

        # MSE_NOTES: #2 u can't change mapping if docs present in IND !!!
        self.indice_create_exists(self.wes, ind_str)

        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            # MSE_NOTES: #3 u should specified IND + DOC_TYPE !!!
            self.assertEqual(WesDefs.RC_OK, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).status)
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            # TODO python lib API 400 - illegal_argument_exception - request [/first_ind1/_mapping/first_ind2_docT] contains unrecognized parameter: [include_type_name]
            self.assertEqual(WesDefs.RC_EXCE, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).status)
            self.assertEqual(WesDefs.RC_OK, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str)).status)
        else:
            self.wes.es_version_mismatch(self.ES_VERSION_RUNNING)

        Log.notice("--------------------------------------------------------------------------------------")
        # MSE_NOTES: #4 type was changed :)
        # IND[first_ind1]
        # city: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        # country: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        # datetime: {'type': 'date', 'format': 'yyyy,MM,dd,hh,mm,ss'}
        # sentence: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        #
        # IND[first_ind2]: Missing mappings
        self.assertEqual(2, len(self.wes.ind_get_mapping_result(self.wes.ind_get_mapping()).data.keys()))

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

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)

        self.indice_create_exists(self.wes, ind_str)

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

        if self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_7_3_0:
            # MSE_NOTES: #3 u should specified IND + DOC_TYPE !!!
            self.assertEqual(WesDefs.RC_OK, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).status)
        elif self.wes.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
            # TODO python lib API 400 - illegal_argument_exception - request [/first_ind1/_mapping/first_ind2_docT] contains unrecognized parameter: [include_type_name]
            self.assertEqual(WesDefs.RC_EXCE, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str, include_type_name=True)).status)
            self.assertEqual(WesDefs.RC_OK, self.wes.ind_put_mapping_result(self.wes.ind_put_mapping(map_new, doc_type=ind_str_doc_type, index=ind_str)).status)
        else:
            self.wes.es_version_mismatch(self.ES_VERSION_RUNNING)


        doc1 = {"city":"Bangalore", "country": "India", "datetime":"2018,01,01,10,20,00"} #datetime format: yyyy,MM,dd,hh,mm,ss",
        doc2 = {"city":"London", "country": "England", "datetime":"2018,01,02,03,12,00"}
        doc3 = {"city":"Los Angeles", "country": "USA", "datetime":"2018,04,19,05,02,00"}
        self.assertEqual("created", self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc1, doc_type=ind_str_doc_type, id=1)).data['result'])
        self.assertEqual("created", self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc2, doc_type=ind_str_doc_type, id=2)).data['result'])
        self.assertEqual("created", self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc3, doc_type=ind_str_doc_type, id=3)).data['result'])

        self.force_reindex(self.wes)

        body = {"from": 0, "size": 10,"query": {"match_all": {}}}
        self.assertEqual(3, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

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
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))
        self.assertNotEqual(None, rc.data.get('aggregations', None))
        print("kuk", rc.data.get('aggregations'))
        self.assertEqual(1, len(rc.data['aggregations']['country']['buckets']))

        Log.notice("--------------------------------------------------------------------------------------")
        doc4 = {"city": "Sydney", "country": "Australia", "datetime": "2019,04,19,05,02,00"}
        self.assertEqual("created", self.wes.doc_addup_result(self.wes.doc_addup(ind_str, doc4, doc_type=ind_str_doc_type, id=4)).data['result'])

        self.force_reindex(self.wes)

        # AGGS:
        # country
        # {'key_as_string': '2018,01,01,12,00,00', 'key': 1514764800000, 'doc_count': 3}
        # {'key_as_string': '2019,01,01,12,00,00', 'key': 1546300800000, 'doc_count': 1}
        rc = self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))
        self.assertNotEqual(None, rc.data.get('aggregations', None))
        print("kuk", rc.data.get('aggregations'))
        self.assertEqual(2, len(rc.data['aggregations']['country']['buckets']))

    def test_bulk(self):
        # MSE_NOTES: for 'bulk' and 'scan' API IMPORT 'from elasticsearch import helpers'

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)
        self.indice_create_exists(self.wes, ind_str)

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
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_bulk_result(self.wes.doc_bulk(actions)).status)
        self.force_reindex(self.wes)

        body = {"query": {"match_all": {}}}
        self.assertEqual(5, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

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
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_bulk_result(self.wes.doc_bulk(actions)).status)
        self.force_reindex(self.wes)

        body = {"query": {"match_all": {}}}
        self.assertEqual(3, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

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
        self.assertEqual(WesDefs.RC_NOK, self.wes.doc_bulk_result(self.wes.doc_bulk(actions)).status)
        self.force_reindex(self.wes)

        # FINAL 0,1,2,3
        self.assertEqual(4, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))


    def test_scan(self):
        # MSE_NOTES: for 'bulk' and 'scan' API IMPORT 'from elasticsearch import helpers'

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)
        self.indice_create_exists(self.wes, ind_str)

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
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_bulk_result(self.wes.doc_bulk(actions)).status)
        self.force_reindex(self.wes)

        body = {"query": {"match_all": {}}}
        self.assertEqual(5, self.wes.doc_search_result_hits_nb(self.wes.doc_search_result(self.wes.doc_search(index=ind_str, body=body))))

        Log.notice("--------------------------------------------------------------------------------------")
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_scan_result(self.wes.doc_scan(query=body)).status)

        Log.notice("--------------------------------------------------------------------------------------")
        self.assertEqual(WesDefs.RC_EXCE, self.wes.doc_scan_result(self.wes.doc_scan(index='pako', query=body)).status)

    def test_count(self):

        global ind_str
        global ind_str_doc_type
        self.indice_cleanup_all(self.wes)
        self.indice_create_exists(self.wes, ind_str)
        self.documents_create(self.wes, ind_str, ind_str_doc_type)

        # 1.
        self.assertEqual(WesDefs.RC_OK, self.wes.doc_count_result(self.wes.doc_count()).status)

        # 2.
        # EXEPTION KEY[first_ind1] - 400 - parsing_exception - request does not support [from]
        body = {"from": 0, "size": 10,
                #"query": {"match": {}} EXCEPTION:  400 - parsing_exception - No text specified for text query
                "query": {"match": {"country": "slovakia"}}}
        self.assertEqual(WesDefs.RC_EXCE, self.wes.doc_count_result(self.wes.doc_count(index=ind_str, body=body)).status)

        # 3.
        # EXEPTION KEY[first_ind1] - 400 - parsing_exception - request does not support [size]
        body = {"size": 10,
                "query": {"match": {"country": "slovakia"}}}
        self.assertEqual(WesDefs.RC_EXCE, self.wes.doc_count_result(self.wes.doc_count(index=ind_str, body=body)).status)

        # 4.
        body = {"query": {"match": {"country": "slovakia"}}}
        self.assertEqual(2, self.wes.doc_count_result(self.wes.doc_count(index=ind_str, body=body)).data['count'])

    def test_templates_get_put(self):

        #ind_str = 'test_def_catalogs'
        global ind_str

        ind_special_cleanup = 'test_def_*'
        ind_special_NEW = 'test_def_catalog_new_v2'
        self.wes.ind_delete_result(self.wes.ind_delete(ind_special_cleanup))

        body_exponea = {'body':
                            { 'mappings':
                                  {'catalog_item':
                                       {'date_detection': False, 'dynamic_templates': [
                                           {'strings': {'mapping': { 'fields': {'keyword': {'ignore_above': 256, 'type': 'keyword'},'raw': {'index': 'not_analyzed', 'type': 'string'}}, 'type': 'text'}, 'match': '*_string'}},
                                           {'has_value': {'mapping': {'type': 'boolean'},'match': '*_has_value'}},
                                           {'booleans': {'mapping': {'type': 'boolean'},'match': '*_boolean'}},
                                           {'numbers': {'mapping': {'type': 'float'},'match': '*_number'}},
                                           {'datetimes': {'mapping': {'type': 'float'},'match': '*_datetime'}},
                                           {'datetime_days': {'mapping': {'type': 'long'},'match': '*_datetime_day'}},
                                           {'datetime_months': {'mapping': {'type': 'long'},'match': '*_datetime_month'}},
                                           {'types': {'mapping': {'type': 'keyword'},'match': '*_type'}}]
                                        }
                                   },
                              'template': 'test_def_catalog_*_v2'
                            },
                        'name': 'def_catalog_v2'
                        }

        self.indice_create_exists(self.wes, ind_str)

        Log.notice("--------------------------------------------------------------------------------------")
        self.assertEqual(WesDefs.RC_OK, self.wes.ind_get_template_result(self.wes.ind_get_template()).status)
        self.assertEqual(WesDefs.RC_OK, self.wes.ind_put_template_result(self.wes.ind_put_template(**body_exponea)).status)
        self.assertEqual(WesDefs.RC_OK, self.wes.ind_get_template_result(self.wes.ind_get_template()).status)

        Log.notice("--------------------------------------------------------------------------------------")
        self.indice_create_exists(self.wes, ind_special_NEW)
        self.assertEqual(WesDefs.RC_OK, self.wes.ind_get_template_result(self.wes.ind_get_template()).status)

        self.wes.ind_delete_result(self.wes.ind_delete(ind_special_cleanup))

class TestWesReal(TestWes):

    def setUp(self):
        self.wes = Wes(False)

class TestWesMock(TestWes):

    def setUp(self):
        self.wes = Wes(True)

if __name__ == '__main__':
    if True:
        unittest.main(TestWesReal())
    else:
        suite = unittest.TestSuite()

        # suite.addTest(TestWesReal("test_general"))
        # suite.addTest(TestWesMock("test_general"))
        # suite.addTest(TestWesReal("test_indice_basic"))
        # suite.addTest(TestWesMock("test_indice_basic"))
        # suite.addTest(TestWesReal("test_documents_basic"))
        # suite.addTest(TestWesMock("test_documents_basic"))
        # suite.addTest(TestWesReal("test_query_basic"))
        # suite.addTest(TestWesMock("test_query_basic"))
        # suite.addTest(TestWesReal("test_complex_queries"))
        # suite.addTest(TestWesMock("test_complex_queries"))
        # suite.addTest(TestWesReal("test_mappings_get"))
        # suite.addTest(TestWesReal("test_mappings_get_put"))
        # suite.addTest(TestWesReal("test_aggregations"))
        # suite.addTest(TestWesReal("test_bulk"))
        # suite.addTest(TestWesReal("test_scan"))
        # suite.addTest(TestWesReal("test_count"))
        # suite.addTest(TestWesReal("test_templates_get_put"))

        runner = unittest.TextTestRunner()
        runner.run(suite)