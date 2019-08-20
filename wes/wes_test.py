from wes import Wes
from log import *

import unittest
#import time
from datetime import datetime

class TestWes(unittest.TestCase):
    def basic_ind(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"
        wes.ind_delete(ind_str)
        wes.ind_create(ind_str)
        wes.ind_create(ind_str)
        wes.ind_exist(ind_str)
        wes.ind_delete(ind_str)
        wes.ind_exist(ind_str)
        wes.ind_delete(ind_str)

    def basic_doc_and_query(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))


        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2", "sentence": "The SLOVAKA is a country"}
        doc3 = {"city": "Bratislava3", "country": "SLOVAKIA",  "sentence": "The slovakia is a country"}
        doc4 = {"city": "Bratislava4", "country": "SLOVAKIA4", "sentence": "The small country is slovakia"}
        doc5 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}

        # TODO petee explain success priority???
        # 1. exception
        # 2. IMPO_AU_1 vs. IMPO_AU_2 consider 'RC - 3 codes'
        #                                                                           MSE_NOTES:      IMPO_AU_1          IntperOper    IntChangedByUpd                            IMPO_AU_2
        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))  # MSE_NOTES: 'result': 'created' '_seq_no': 0  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type="any", id=2))  # MSE_NOTES: 'result': 'created' '_seq_no': 1  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))  # MSE_NOTES: 'result': 'created' '_seq_no': 2  '_version': 1,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))  # MSE_NOTES: 'result': 'updated' '_seq_no': 3  '_version': 2,    '_shards': {'total': 2, 'successful': 1, 'failed': 0},

        #                                                              MSE_NOTES:  IMPO_GET_1 ok/exc                                    IMPO_GET_2
        wes.doc_get_result(wes.doc_get(ind_str, 1, doc_type="any"))  # MSE_NOTES: 'found': True,                 '_seq_no': 0,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia1'}
        wes.doc_get_result(wes.doc_get(ind_str, 2, doc_type="any"))  # MSE_NOTES: 'found': True,                 '_seq_no': 1,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        wes.doc_get_result(wes.doc_get(ind_str, 3, doc_type="any"))  # MSE_NOTES: 'found': True,                 '_seq_no': 2,  '_source': {'city': 'Bratislava1', 'coutry': 'slovakia2'}
        wes.doc_get_result(wes.doc_get(ind_str, 9, doc_type="any"))  # MSE_NOTES:  WesNotFoundError !!!

        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))  # MSE_NOTES: 'result': 'updated' '_seq_no': 4
        wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type="any", id=4))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc5, doc_type="any", id=5))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        wes.doc_search_result(wes.doc_search())                                                             # MSE_NOTES: #1 search ALL in DB
        wes.doc_search_result(wes.doc_search(index=ind_str))                                                # MSE_NOTES: #2 search ALL in specific INDICE
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": {"match_all": {}}}))             # MSE_NOTES: #3 equivalent to #2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #4 hint list FILTER
                 "query": {"match_all": {}}                                                                 #               - 'from' - specify START element in 'hintLIST' witch MATCH query
               }                                                                                            #               - 'size' - specify RANGE/MAXIMUM from 'hintLIST' <'from', 'from'+'size')
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     #               E.G. "size": 0 == returns just COUNT


        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(inSensitive)
                #"query": {"match": {}}                                                                     #               - EXCEPTION:  400 - parsing_exception - No text specified for text query
                "query": {"match": {"country": "slovakia"}}                                                 #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #5 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
                "query": {"match": {"sentence": "slovakia"}}                                                #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 4


        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #6 QUERY(matchphrase) MATCH(subSentence+wholeWord+phraseOrder) CASE(in-sensitive)
                "query": {"match_phrase": {"country": "slovakia"}}                                          #              - ORDER IGNORE spaces and punctation !!!
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #6 QUERY(matchphrase) MATCH(subSentence+wholeWord+phraseOrder) CASE(in-sensitive)
                "query": {"match_phrase": {"sentence": "slovakia is"}}                                      #              - ORDER IGNORE spaces and punctation !!!
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2


        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #7 QUERY(term) MATCH(exact) CASE(in-sensitive)
                "query": {"term": {"country": "slovakia"}}                                                  #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #7 QUERY(term) MATCH(exact) CASE(in-sensitive)
                "query": {"term": {"sentence": "slovakia is"}}                                              #
               }                                                                                            #
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     # RESULTS: 2

        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        body = {"from": 0, "size": 10,                                                                      # MSE_NOTES: #8 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
                "query": {"term": {"country": "slovakia"}}                                                  #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
               }                                                                                            #   -
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                                     #   -     == ORRESULTS: 2


    def complex_queries(self):
        wes = Wes()
        ind_str = "first_pooooooooooooooo"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2", "sentence": "The SLOVAKA is a country"}
        doc3 = {"city": "Bratislava3", "country": "SLOVAKIA",  "sentence": "The slovakia is a country"}
        doc4 = {"city": "Bratislava4", "country": "SLOVAKIA4", "sentence": "The small country is slovakia"}
        doc5 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}

        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type="any", id=2))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type="any", id=4))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc5, doc_type="any", id=5))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        q1 = {"match": {"sentence": "slovakia"}}                                                # MSE_NOTES: #1 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q1}))                #  RESULTS: 4
        q2 = {"match": {"sentence": "small"}}                                                   # MSE_NOTES: #2 QUERY(match) MATCH(subSentence+wholeWord) CASE(in-sensitive)
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q2}))                #  RESULTS: 2

        LOG_NOTI_L("--------------------------------------------------------------------------------------")

        body = {"query": {"bool": { "must_not": q2, "should": q1 }}}                            # MSE_NOTES: #3 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
                                                                                                #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                         #  RESULTS: 2

        q3 = {"regexp": {"sentence": ".*"}}                                                     # MSE_NOTES: #4 QUERY(regexp) MATCH(regexp) CASE(in-sensitive)
                                                                                                #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q3}))                # RESULTS: 5

    def mappings_get(self):
        # MSE_NOTES: mapping is process of defining how documents looks like (which fields contains, field types, how is filed indexed)
        # types of mapping fileds:
        # - keyword
        # - text
        # - datetime (is recognized based on format)
        #  = setting correct times help aggregations
        #  = u can't change mapping if docs present in IND
        wes = Wes()
        ind_str  = "first_ind1"
        ind_str2 = "first_ind2"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

        wes.ind_delete_result(ind_str2, wes.ind_delete(ind_str2))
        wes.ind_create_result(wes.ind_create(ind_str2))
        wes.ind_exist_result(ind_str2, wes.ind_exist(ind_str2))

        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country"}
        doc2 = {"city": "Bratislava2", "country": "SLOVAKIA2", "sentence": "The SLOVAKA is a country"}
        doc3 = {"city": "Bratislava3", "country": "SLOVAKIA",  "sentence": "The slovakia is a country"}
        doc4 = {"city": "Bratislava4", "country": "SLOVAKIA4", "sentence": "The small country is slovakia"}
        doc5 = {"city": "Bratislava4", "country": "SLOVAKIA5", "sentence": "The small COUNTRy is slovakia"}

        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type="any", id=2))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type="any", id=4))

        # MSE_NOTES: #1 400 - illegal_argument_exception - Rejecting mapping update to [first_ind1] as the final mapping would have more than 1 type: [any, any2]
        wes.doc_addup_result(wes.doc_addup(ind_str, doc5, doc_type="any2", id=5))

        wes.ind_get_mapping_result(wes.ind_get_mapping())                             # RESULT 2
        wes.ind_get_mapping_result(wes.ind_get_mapping(ind_str))                      # RESULT 1

        # MSE_NOTES: #1 400 - illegal_argument_exception - Types cannot be provided in get mapping requests, unless include_type_name is set to true.
        wes.ind_get_mapping_result(wes.ind_get_mapping(ind_str, doc_type="any"))
        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        wes.ind_get_mapping_result(wes.ind_get_mapping(ind_str, doc_type="any", include_type_name=True))

    def mappings_get_put(self):
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
        ind_str  = "first_ind1"
        ind_str2 = "first_ind2"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

        wes.ind_delete_result(ind_str2, wes.ind_delete(ind_str2))
        wes.ind_create_result(wes.ind_create(ind_str2))
        wes.ind_exist_result(ind_str2, wes.ind_exist(ind_str2))

        doc1 = {"city": "Bratislava1", "country": "slovakia ", "sentence": "The slovakia is a country", "datetime" : "2019,01,02,03,12,00"}
        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))
        wes.ind_get_mapping_result(wes.ind_get_mapping())
        LOG_NOTI_L("--------------------------------------------------------------------------------------")
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
        wes.ind_put_mapping_result(wes.ind_put_mapping(map_new))
        # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - Types cannot be provided in put mapping requests, unless the include_type_name parameter is set to true.
        wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type="any", index=ind_str))
        # OP_IND_PUT_MAP KEY[???] - 400 - illegal_argument_exception - mapper [datetime] of different type, current_type [text], merged_type [date]
        wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type="any", index=ind_str, include_type_name=True))

        # MSE_NOTES: #2 u can't change mapping if docs present in IND
        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        # MSE_NOTES: #3 u should specified IND + DOC_TYPE !!!
        wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type="any", index=ind_str, include_type_name=True))

        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        wes.ind_get_mapping_result(wes.ind_get_mapping())
        # MSE_NOTES: #4 type was changed :)
        # IND[first_ind1]
        # city: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        # country: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        # datetime: {'type': 'date', 'format': 'yyyy,MM,dd,hh,mm,ss'}
        # sentence: {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}
        #
        # IND[first_ind2]: Missing
        # mappings

    def aggregations(self):
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
        ind_str = "first_ind1"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

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

        wes.ind_put_mapping_result(wes.ind_put_mapping(map_new, doc_type="any", index=ind_str, include_type_name=True))

        doc1 = {"city":"Bangalore", "country": "India", "datetime":"2018,01,01,10,20,00"} #datetime format: yyyy,MM,dd,hh,mm,ss",
        doc2 = {"city":"London", "country": "England", "datetime":"2018,01,02,03,12,00"}
        doc3 = {"city":"Los Angeles", "country": "USA", "datetime":"2018,04,19,05,02,00"}

        wes.doc_addup_result(wes.doc_addup(ind_str, doc1, doc_type="any", id=1))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc2, doc_type="any", id=2))
        wes.doc_addup_result(wes.doc_addup(ind_str, doc3, doc_type="any", id=3))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        wes.ind_get_mapping_result(wes.ind_get_mapping())

        body = {"from": 0, "size": 10,
                 "query": {"match_all": {}}
               }
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))

        LOG_NOTI_L("--------------------------------------------------------------------------------------")
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
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))
        # AGGS:
        # country
        # {'key_as_string': '2018,01,01,12,00,00', 'key': 1514764800000, 'doc_count': 3}

        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        doc4 = {"city":"Sydney", "country": "Australia", "datetime":"2019,04,19,05,02,00"}
        wes.doc_addup_result(wes.doc_addup(ind_str, doc4, doc_type="any", id=4))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))
        # AGGS:
        # country
        # {'key_as_string': '2018,01,01,12,00,00', 'key': 1514764800000, 'doc_count': 3}
        # {'key_as_string': '2019,01,01,12,00,00', 'key': 1546300800000, 'doc_count': 1}

    def bulk(self):
        # MSE_NOTES: for 'bulk' and 'scan' API IMPORT 'from elasticsearch import helpers'

        wes = Wes()
        ind_str = "first_ind1"
        doc_type = "my_doc_tupe"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

        actions = [
            {
                "_index": ind_str,
                "_type": doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(0, 5)
        ]

        #st = time.time()
        wes.doc_bulk_result(wes.doc_bulk(actions))
        #end = time.time()
        #print("total time", end-st)

        LOG_NOTI_L("--------------------------------------------------------------------------------------")

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        body = {"query": {"match_all": {}}}
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))

        actions = [
            {
                '_op_type': "delete",
                "_index": ind_str,
                "_type": doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(3, 5)
        ]

        #st = time.time()
        wes.doc_bulk_result(wes.doc_bulk(actions))
        #end = time.time()
        #print("total time", end-st)

        LOG_NOTI_L("--------------------------------------------------------------------------------------")

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        body = {"query": {"match_all": {}}}
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))


        actions = [
            {
                '_op_type': "create",
                "_index": ind_str,
                "_type": doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(2, 4)
        ]

        #st = time.time()
        wes.doc_bulk_result(wes.doc_bulk(actions))
        #end = time.time()
        #print("total time", end-st)
        #
        LOG_NOTI_L("--------------------------------------------------------------------------------------")

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        body = {"query": {"match_all": {}}}
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))

    def test_scan(self):
        # MSE_NOTES: for 'bulk' and 'scan' API IMPORT 'from elasticsearch import helpers'

        wes = Wes()
        ind_str = "first_ind1"
        doc_type = "my_doc_tupe"

        wes.ind_delete_result(ind_str, wes.ind_delete(ind_str))
        wes.ind_create_result(wes.ind_create(ind_str))
        wes.ind_exist_result(ind_str, wes.ind_exist(ind_str))

        actions = [
            {
                "_index": ind_str,
                "_type": doc_type,
                "_id": j,
                "_source": {
                    "any": "data" + str(j),
                    "timestamp": str(datetime.now())
                }
            }
            for j in range(0, 5)
        ]

        #st = time.time()
        wes.doc_bulk_result(wes.doc_bulk(actions))

        wes.ind_flush_result("_all", wes.ind_flush(index="_all", wait_if_ongoing=True))
        wes.ind_refresh_result("_all", wes.ind_refresh(index="_all"))

        body = {"query": {"match_all": {}}}
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))

        wes.doc_scan_result(wes.doc_scan(query=body))
        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        wes.doc_scan_result(wes.doc_scan(index='pako', query=body))

if __name__ == '__main__':
    # unittest.main() run all test (imported too) :(
    unittest.main(TestWes())