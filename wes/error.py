from log import *

# elasticsearch - base
from elasticsearch.exceptions import ImproperlyConfigured as WesImproperlyConfigured
from elasticsearch.exceptions import SerializationError as WesSerializationError


# elasticsearch - ConnectionError
from elasticsearch.exceptions import ConnectionError

from elasticsearch.exceptions import ConnectionTimeout as WesConnectionTimeout
from elasticsearch.exceptions import SSLError as WesSSLError

# elasticsearch - TransportError
from elasticsearch.exceptions import TransportError

from elasticsearch.exceptions import RequestError as WesRequestError    # 400 status code
from elasticsearch.exceptions import ConflictError as WesConflictError   # 409 status code
from elasticsearch.exceptions import NotFoundError as WesNotFoundError   # 404 status code

# wes - placeholder - will I need it ???

def WES_SUCCESS(oper, rc):
    LOG_OK(f"{oper} {str(rc)}")

def WES_EXCEPTION(oper, e):
    if isinstance(e, TransportError):
        LOG_ERR(f"{oper} {e.status_code} - {e.error} - {e.info} ")
    elif isinstance(e, ConnectionError):
        LOG_ERR(f"{oper} {e.info}")
    elif isinstance(e, WesSerializationError):
        LOG_ERR(f"{oper} Serialization error")
    elif isinstance(e, WesImproperlyConfigured):
        LOG_ERR(f"{oper} Improperly configured")
    else:
        LOG_ERR(f"{oper} Unknow ...")
        raise(e)