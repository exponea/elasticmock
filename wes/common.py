class WesDefs():
    # Elasticsearch version
    ES_VERSION_7_3_0 = '7.3.0'
    ES_VERSION_5_6_5 = '5.6.5'

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