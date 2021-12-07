from .. import protocol
from .session import Session


class Driver:
    def __init__(self, backend, uri, auth_token, user_agent=None,
                 resolver_fn=None, domain_name_resolver_fn=None,
                 connection_timeout_ms=None, fetch_size=None,
                 max_tx_retry_time_ms=None, encrypted=None,
                 trusted_certificates=None, liveness_check_timeout_ms=None,
                 max_connection_pool_size=None,
                 connection_acquisition_timeout_ms=None):
        self._backend = backend
        self._resolver_fn = resolver_fn
        self._domain_name_resolver_fn = domain_name_resolver_fn
        req = protocol.NewDriver(
            uri, auth_token, userAgent=user_agent,
            resolverRegistered=resolver_fn is not None,
            domainNameResolverRegistered=domain_name_resolver_fn is not None,
            connectionTimeoutMs=connection_timeout_ms,
            fetchSize=fetch_size, maxTxRetryTimeMs=max_tx_retry_time_ms,
            encrypted=encrypted, trustedCertificates=trusted_certificates,
            liveness_check_timeout_ms=liveness_check_timeout_ms,
            max_connection_pool_size=max_connection_pool_size,
            connection_acquisition_timeout_ms=connection_acquisition_timeout_ms
        )
        res = backend.send_and_receive(req)
        if not isinstance(res, protocol.Driver):
            raise Exception("Should be Driver but was %s" % res)
        self._driver = res

    def verify_connectivity(self):
        req = protocol.VerifyConnectivity(self._driver.id)
        self._backend.send(req)
        while True:
            res = self._backend.receive()
            if isinstance(res, protocol.DomainNameResolutionRequired):
                addresses = self.resolve_domain_name(res.name)
                self._backend.send(
                    protocol.DomainNameResolutionCompleted(res.id, addresses)
                )
            elif isinstance(res, protocol.Driver):
                return
            else:
                raise Exception(
                    "Should be Driver or DomainNameResolutionRequired "
                    "but was: %s" % res
                )

    def supports_multi_db(self):
        req = protocol.CheckMultiDBSupport(self._driver.id)
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.MultiDBSupport):
            raise Exception("Should be MultiDBSupport")
        return res.available

    def close(self):
        req = protocol.DriverClose(self._driver.id)
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.Driver):
            raise Exception("Should be driver")

    def session(self, access_mode, bookmarks=None, database=None,
                fetch_size=None, impersonated_user=None):
        req = protocol.NewSession(
            self._driver.id, access_mode, bookmarks=bookmarks,
            database=database, fetchSize=fetch_size,
            impersonatedUser=impersonated_user
        )
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.Session):
            raise Exception("Should be session")
        return Session(self, self._backend, res)

    def resolve(self, address):
        return self._resolver_fn(address)

    def resolve_domain_name(self, name):
        return self._domain_name_resolver_fn(name)

    def update_routing_table(self, database=None, bookmarks=None):
        req = protocol.ForcedRoutingTableUpdate(
            self._driver.id, database=database, bookmarks=bookmarks
        )
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.Driver):
            return Exception("Should be Driver")

    def get_routing_table(self, database=None):
        req = protocol.GetRoutingTable(self._driver.id, database=database)
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.RoutingTable):
            raise Exception("Should be RoutingTable")
        return res

    def get_connection_pool_metrics(self, address):
        req = protocol.GetConnectionPoolMetrics(self._driver.id, address)
        res = self._backend.send_and_receive(req)
        if not isinstance(res, protocol.ConnectionPoolMetrics):
            raise Exception("Should be ConnectionPoolMetrics")
        return res
