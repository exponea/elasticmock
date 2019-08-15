from elasticsearch import Elasticsearch

# elasticsearch - ConnectionError
from elasticsearch.exceptions import ConnectionTimeout as WesConnectionTimeout
from elasticsearch.exceptions import SSLError as WesSSLError

# elasticsearch - TransportError
from elasticsearch.exceptions import RequestError as WesSSLError    # 400 status code
from elasticsearch.exceptions import ConflictError as WesSSLError   # 409 status code
from elasticsearch.exceptions import NotFoundError as WesSSLError   # 404 status code

# wes - placeholder ???
