import unittest

from log import Log

__all__ = ["TestCommon", "WesDefs"]

class TestCommon(unittest.TestCase):

    def cmp_dict_with_skipp_keys(self, rc: dict, wes: dict, skip_keys: tuple, dbg: bool = False):

        if dbg:
            Log.log(" -------")
            Log.log(str(rc))
            Log.log(str(wes))
            Log.log(" -------")

        rc_keys = rc.keys()
        wes_keys = wes.keys()
        self.assertEqual(rc_keys, wes_keys)

        for k in rc_keys:
            rc_sub = rc[k]
            wes_sub = wes[k]
            if isinstance(rc_sub, dict) and isinstance(wes_sub, dict):
                self.cmp_dict_with_skipp_keys(rc_sub, wes_sub, skip_keys, dbg)
            else:
                res = k + '\n' + str(rc_sub) + '\n' + str(wes_sub)
                if k in skip_keys:
                    Log.warn(res)
                else:
                    if dbg:
                        Log.log(res)
                    self.assertEqual(rc_sub, wes_sub)


class WesDefs():
    # Elasticsearch version
    ES_VERSION_7_3_0 = '7.3.0'
    ES_VERSION_5_6_5 = '5.6.5'
    ES_VERSION_DEFAULT = ES_VERSION_5_6_5

    def es_version_mismatch(self):
        raise ValueError(f"ES_VERSION_RUNNING is unknown - {self.ES_VERSION_RUNNING}")

    # indice operations
    OP_IND_CREATE       = "OP_IND_CREATE       : "
    OP_IND_FLUSH        = "OP_IND_FLUSH        : "
    OP_IND_REFRESH      = "OP_IND_REFRESH      : "
    OP_IND_EXIST        = "OP_IND_EXIST        : "
    OP_IND_DELETE       = "OP_IND_DELETE       : "
    OP_IND_GET          = "OP_IND_GET          : "
    OP_IND_GET_ALIAS    = "OP_IND_GET_ALIAS    : "
    OP_IND_DEL_ALIAS    = "OP_IND_DEL_ALIAS    : "
    OP_IND_GET_MAP      = "OP_IND_GET_MAP      : "
    OP_IND_PUT_MAP      = "OP_IND_PUT_MAP      : "
    OP_IND_GET_TMP      = "OP_IND_GET_TMP      : "
    OP_IND_PUT_TMP      = "OP_IND_PUT_TMP      : "
    # general
    OP_GEN_PING         = "OP_GEN_PING         : "
    OP_GEN_INFO         = "OP_GEN_INFO         : "
    # document operations
    OP_DOC_ADD_UP       = "OP_DOC_ADD_UP       : "
    OP_DOC_UPDATE       = "OP_DOC_UPDATE       : "
    OP_DOC_GET          = "OP_DOC_GET          : "
    OP_DOC_EXIST        = "OP_DOC_EXIST        : "
    OP_DOC_DEL          = "OP_DOC_DEL          : "
    # batch operations
    OP_DOC_DEL_QUERY    = "OP_DOC_DEL_QUERY    : "
    OP_DOC_SEARCH       = "OP_DOC_SEARCH       : "
    OP_DOC_BULK         = "OP_DOC_BULK         : "
    OP_DOC_BULK_STR     = "OP_DOC_BULK_STR     : "
    OP_DOC_SCAN         = "OP_DOC_SCAN         : "
    OP_DOC_COUNT        = "OP_DOC_COUNT        : "
    OP_DOC_SCROLL       = "OP_DOC_SCROLL       : "
    OP_DOC_SCROLL_CLEAR = "OP_DOC_SCROLL_CLEAR : "

    # RC - 3 codes
    # - maybe useful later (low level could detect problem in data)
    # - e.g. no exception but status is wrong TODO discuss wit petee
    RC_EXCE    = "RC_EXCE"
    RC_NOK     = "RC_NOK"
    RC_OK      = "RC_OK"

    @staticmethod
    def mappings_dump2str(mappings_data, obj_with_running, prefix=''):
        mappings = mappings_data.get('mappings', None)
        rec = ''
        if mappings is None or len(mappings) == 0:
            # mappings not exist
            rec += prefix + " - Missing mappings"
        else:
            if obj_with_running.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
                for maps in mappings:
                    rec += '\n' + prefix + str(maps) + '\n'
                    for int_map in mappings[maps]:
                        rec += '-> ' + '{:>12}: '.format(int_map) + str(mappings[maps][int_map]) + '\n'
            else:
                WesDefs.es_version_mismatch(obj_with_running.ES_VERSION_RUNNING)

        return rec