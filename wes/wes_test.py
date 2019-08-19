

from wes import Wes
from log import *
import unittest

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

    def test_basic_doc_and_query(self):
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


        body = {"query": {"bool": { "must_not": q2, "should": q1 }}}                            # MSE_NOTES: #3 QUERY(bool) MATCH(must, must_not, should) CASE(in-sensitive)
                                                                                                #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        wes.doc_search_result(wes.doc_search(index=ind_str, body=body))                         #  RESULTS: 2

        LOG_NOTI_L("--------------------------------------------------------------------------------------")
        q3 = {"regexp": {"sentence": ".*"}}                                                     # MSE_NOTES: #4 QUERY(regexp) MATCH(regexp) CASE(in-sensitive)
                                                                                                #   - must, must_not, should(improving relevance score, if none 'must' presents at least 1 'should' be present)
        wes.doc_search_result(wes.doc_search(index=ind_str, body={"query": q3}))                # RESULTS: 5

    def mappings(self):
        # MSE_NOTES: mapping is process of defining how documents looks like (which fields contains, field types, how is filed indexed)
        wes = Wes()
        ind_str = "first_pooooooooooooooo"

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

        wes.ind_get_mapping_result(ind_str, wes.ind_get_mapping(ind_str))


if __name__ == '__main__':
    # unittest.main() run all test (imported too) :(
    unittest.main(TestWes())