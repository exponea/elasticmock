from elasticsearch import Elasticsearch

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

def err_2_string(e) -> str:
    if isinstance(e, TransportError):
        return f"{e.status_code} - {e.error} - {e.info} "
    elif isinstance(e, ConnectionError):
        return f"{e.info}"
    elif isinstance(e, WesSerializationError):
        return "Serialization error"
    elif isinstance(e, WesImproperlyConfigured):
        return "Improperly configured"
    else:
        raise("Unknow ...")