import re
import json
import sys
PY3 = sys.version_info[0] == 3
if PY3:
    unicode = str

from log import Log

__all__ = ["MockEsQuery"]

class MockEsQuery:

    def __init__(self, operation, body):
        self.q_oper = operation
        self.q_query_rules = None
        self.q_size = None
        self.q_from = None
        self.q_aggs = None
        self.q_query = None
        self.q_query_name = None
        self.q_level = 0

        self.parser(body)

    def parser(self, body):

        if body:
            self.q_size = body.get('size', None)
            self.q_from = body.get('from', None)
            self.q_aggs = body.get('aggs', None)
            self.q_query = body.get('query', None)

            if not self.q_query:
                raise ValueError("'query' missing in body")

            self.q_query_name = list(self.q_query.keys())

            if len(self.q_query_name) != 1:
                raise ValueError(f"body contains more queries {self.q_query_name}")

            self.q_query_name = self.q_query_name[0]
            self.q_query_rules = self.q_query[self.q_query_name]

        else:
            self.q_query_name = "match_all"
            self.q_query_rules = {}

        Log.log(f"{self.q_oper} Q_NAME: {self.q_query_name} - Q_RULES: {self.q_query_rules}")

    def get_indentation_string(self):
        return f"{self.q_oper} {'--'*self.q_level}>"


    def q_exec_on_doc(self, prefix, db_idx, db_doc, fnc_name, data_to_match) -> bool:
        self.q_level += 1
        q_xxx_fnc = self.q_mapper(fnc_name)
        rc = q_xxx_fnc(self, prefix, db_idx, db_doc, data_to_match)
        if self.q_level == 1:
            Log.log(f"{self.get_indentation_string()} {'MATCH' if rc else 'MISS '}  >>> idx:{db_idx} dos:{str(db_doc)}")
        self.q_level -= 1
        return rc

    def q_match_all(self, prefix, db_idx, db_doc, data_to_match) -> bool:
        rc = True
        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: match - RC-[{rc}]")
        return rc

    def q_match(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            val_in_query = data_to_match[feature].lower()

            res = (val_in_query == word.lower() for word in val_in_doc.split())
            if True not in res:
                rc = False

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: match - RC-[{rc}]")
        return rc

    def q_match_phrase(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            val_in_doc = val_in_doc.lower()
            val_in_query = data_to_match[feature].lower()

            # MSE_NOTE: 'in 'does not work for WHOLE WORDS
            # 'if val_in_query not in val_in_doc:'
            if re.search(r"\b{}\b".format(val_in_query), val_in_doc, re.IGNORECASE) is None:
                rc = False
                break

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: match_phrase - RC-[{rc}]")
        return rc

    def q_term(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            # MSE_NOTES: MATCH(exact BUT lookup is stored with striped WHITESPACES)
            val_in_doc = val_in_doc.lower().strip()
            val_in_query = data_to_match[feature].lower()

            if not val_in_doc == val_in_query:
                rc = False
                break

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: term - RC-[{rc}]")

        return rc

    def q_regexp(self, prefix, db_idx, db_doc, data_to_match) -> bool:

        rc = True
        for feature in data_to_match.keys():
            if feature[0] == '_':
                val_in_doc = db_doc[feature]
            else:
                val_in_doc = db_doc['_source'][feature]

            val_in_query = data_to_match[feature]

            if re.search(val_in_query, val_in_doc) is None:
                rc = False
                break

        if not prefix:
            Log.log(f"{self.get_indentation_string()} Q_NAME: regexp - RC-[{rc}]")

        return rc

    def q_bool_helper(self, rules: tuple, data_to_match, db_idx, db_doc) -> bool:

        rules_name, rules_continue_fnc = rules
        rules_dict = data_to_match.get(rules_name, None)
        rc = None
        if rules_dict:
            rc = True
            for fnc_name in rules_dict.keys():
                data_to_match2 = rules_dict[fnc_name]
                rc_partial = self.q_exec_on_doc(rules_name, db_idx, db_doc, fnc_name, data_to_match2)
                keep_going = rules_continue_fnc(rc_partial)
                Log.log(f"{self.get_indentation_string()} Q_NAME: bool[{rules_name:8}] RC[{keep_going}]")
                if keep_going:
                    pass
                else:
                    rc = False
                    break
        return rc


    def q_bool(self, prefix, db_idx, db_doc, data_to_match):

        rc_must     = self.q_bool_helper(('must'    , lambda rc: rc), data_to_match, db_idx, db_doc)
        rc_filter   = self.q_bool_helper(('filter'  , lambda rc: rc), data_to_match, db_idx, db_doc)
        rc_must_not = self.q_bool_helper(('must_not', lambda rc: rc is False), data_to_match, db_idx, db_doc)
        rc_should   = self.q_bool_helper(('should'  , lambda rc: rc), data_to_match, db_idx, db_doc)

        res = f"{self.get_indentation_string()} Q_NAME: bool[rc_must({rc_must}) rc_filter({rc_filter}) rc_must_not({rc_must_not}) rc_should({rc_should})]"

        if rc_must_not is False:
            Log.log(f"{res} CASE(1)")
            return False

        if rc_must or rc_filter or rc_should:
            Log.log(f"{res} CASE(2)")
            return True

        Log.log(f"{res} CASE(3)")
        return False

    def q_mapper(self, query: str):
        q_mapper = {
            "match_all":    MockEsQuery.q_match_all,
            "match":        MockEsQuery.q_match,
            "match_phrase": MockEsQuery.q_match_phrase,
            "term":         MockEsQuery.q_term,
            "regexp":       MockEsQuery.q_regexp,
            "bool":         MockEsQuery.q_bool,
        }

        rc = q_mapper.get(query, None)
        if not rc:
            raise ValueError(f"not implemented {query}")
        return rc
