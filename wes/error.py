from log import *

from elasticsearch.exceptions import ImproperlyConfigured
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.exceptions import SerializationError
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

def WES_RC_OK(oper, rc):
    LOG_OK(f"{oper} {str(rc)}")

def WES_DB_OK(oper, rc):
    LOG(f"{oper} {str(rc)}")

def WES_INT_ERR(oper, e, LOG_FNC):
    # ImproperlyConfigured(Exception)
    # ElasticsearchException(Exception)
    # 	- SerializationError(ElasticsearchException)
    #	- TransportError(ElasticsearchException)
    # 		= ConnectionError(TransportError)       'N/A' status code
    #			=> SSLError(ConnectionError)
    #			=> ConnectionTimeout(ConnectionError)
    # 		= NotFoundError(TransportError)          404  status code
    # 		= ConflictError(TransportError)          409  status code
    # 		= RequestError(TransportError)           400  status code
    # 		= AuthenticationException(TransportError)401  status code
    # 		= AuthorizationException(TransportError) 403  status code
    if   isinstance(e, ImproperlyConfigured):
        LOG_FNC(f"{oper} {str(e)}")
    elif isinstance(e, ElasticsearchException):
        if   isinstance(e, SerializationError):
            LOG_FNC(f"{oper} {str(e)}")
        elif isinstance(e, TransportError):
            if isinstance(e, ConnectionError):
                if isinstance(e, SSLError) or isinstance(e, ConnectionTimeout):
                    LOG_FNC(f"{oper} ConnectionError - {e.info}")
                else:
                    LOG_FNC(f"{oper} ConnectionError (generic) - {e.info}")
            else:
                # 		= NotFoundError(TransportError)          404  status code
                # 		= ConflictError(TransportError)          409  status code
                # 		= RequestError(TransportError)           400  status code
                # 		= AuthenticationException(TransportError)401  status code
                # 		= AuthorizationException(TransportError) 403  status code

                # print(type(e.error)) => <class 'str'>
                # print(type(e.info))  => <class 'dict'>
                if LOG_FNC == LOG_WARN:
                    LOG_FNC(f"{oper} {e.status_code} - {e.info} ")
                else:
                    LOG_FNC(f"{oper} KEY[{e.info['_index']} <-> {e.info['_type']} <-> {e.info['_id']}] {str(e)}")
        else:
            LOG_ERR(f"{oper} Unknow L2 exception ... {str(e)}")
            raise (e)
    else:
        LOG_ERR(f"{oper} Unknow L1 exception ... {str(e)}")
        raise(e)


def WES_RC_NOK(oper, e):
    WES_INT_ERR(oper, e, LOG_ERR) # this is L1 - only warn

def WES_DB_ERR(oper, e):
    WES_INT_ERR(oper, e, LOG_WARN) # this is L1 - only warn
