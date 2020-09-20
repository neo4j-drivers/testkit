"""
Shared utilities for writing tests
Common between integration tests using Neo4j and stub server.

Uses environment variables for configuration:

TEST_BACKEND_HOST  Hostname of backend, default is localhost
TEST_BACKEND_PORT  Port on backend host, default is 9876
"""

import os
from nutkit.backend import Backend

def get_backend_host_and_port():
    host = os.environ.get('TEST_BACKEND_HOST', '127.0.0.1')
    port = os.environ.get('TEST_BACKEND_PORT', 9876)
    return (host, port)

def new_backend():
    """ Returns connection to backend, caller is responsible for closing
    """
    host, port = get_backend_host_and_port()
    return Backend(host, port)

def get_driver_name():
    return os.environ['TEST_DRIVER_NAME']
