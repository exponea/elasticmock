import unittest
from collections import namedtuple

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

ExecCode = namedtuple('ExecCode', 'status data fnc_params')

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
    def dump2string_result_ind_mappings(mappings_data, obj_with_running, prefix):
        mappings = mappings_data.get('mappings', None)
        rec = ''
        if mappings is None or len(mappings) == 0:
            # mappings not exist
            rec += '\n' + f"{prefix}Missing mappings"
        else:
            if obj_with_running.ES_VERSION_RUNNING == WesDefs.ES_VERSION_5_6_5:
                for dtype_name in mappings:
                    prefix_dtype = f"{prefix} DTYPE[{dtype_name}]"
                    for internal_dtype in mappings[dtype_name]:
                        if internal_dtype == 'properties':
                            properties_dict = mappings[dtype_name][internal_dtype]
                            # TOO GENERIC tmp_str = f"{prefix_dtype} {internal_dtype} {str(properties_dict)}"
                            # TOO GENERIC rec += '\n' + tmp_str
                            for prop in properties_dict:
                                tmp_str = f"{prefix_dtype} {internal_dtype} PROP[{prop:>12} - {str(properties_dict[prop])}]"
                                rec += '\n' + tmp_str
                        elif internal_dtype == 'dynamic' or \
                             internal_dtype == 'date_detection' or \
                             internal_dtype == 'dynamic_templates':
                            general_dict = mappings[dtype_name][internal_dtype]
                            tmp_str = f"{prefix_dtype} {internal_dtype} {str(general_dict)}"
                            rec += '\n' + tmp_str
                        else:
                            raise ValueError(f"MAPPINGS unknown internal_dtype '{internal_dtype}'")
            else:
                WesDefs.es_version_mismatch(obj_with_running.ES_VERSION_RUNNING)

        return rec

    @staticmethod
    def dump2string_result_ind(data: dict, obj_with_running) -> str:
        res = ''
        cnt = 0
        for ind_name in data.keys():
            cnt += 1
            ind_dict = data[ind_name]
            #res += '\n' + ind_name + ' <-> ' + str(ind_dict)
            for ind_field in ind_dict.keys():
                prefix_ind_fields = f"IND[{ind_name}] {ind_field:>9} <-> "
                ind_field_dict = ind_dict[ind_field]
                # TOO GENERIC res += '\n' + prefix_ind_fields + str(ind_field_dict)
                if ind_field == 'aliases':
                    res += '\n' + prefix_ind_fields + str(ind_field_dict)
                elif ind_field == 'mappings':
                    res += WesDefs.dump2string_result_ind_mappings(ind_dict, obj_with_running, prefix=prefix_ind_fields)
                elif ind_field == 'settings':
                    res += '\n' + prefix_ind_fields + str(ind_field_dict)
                else:
                    ValueError(f"INDICE unknown ind_field {ind_field} ")

        return f"COUNT[{cnt}] {res}"

    @staticmethod
    def dump2string_result_templates(data: dict, obj_with_running) -> str:

        def dump2string_result_templates_single(tmpl_name, tmpl_dict):
            res = ''
            # res += '\n' + tmpl_name + ' <-> ' + str(tmpl_dict)
            for tmpl_field in tmpl_dict.keys():
                prefix_tmpl_fields = f"TMPL[{tmpl_name}] {tmpl_field:>9} <-> "
                tmpl_field_dict = tmpl_dict[tmpl_field]
                # TOO GENERIC res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                if tmpl_field == 'aliases':
                    res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                elif tmpl_field == 'mappings':
                    res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                    #res += WesDefs.dump2string_result_ind_mappings(tmpl_dict, obj_with_running, prefix=prefix_tmpl_fields)
                elif tmpl_field == 'settings':
                    res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                elif tmpl_field == 'order':
                    res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                elif tmpl_field == 'version':
                    res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                elif tmpl_field == 'template':
                    res += '\n' + prefix_tmpl_fields + str(tmpl_field_dict)
                else:
                    ValueError(f"TEMPLATE unknown tmpl_field {tmpl_field} ")
            return res

        res = ''
        cnt = 0
        for tmpl_name in data.keys():
            cnt += 1
            res += dump2string_result_templates_single(tmpl_name, data[tmpl_name])

        return f"COUNT[{cnt}] {res}"