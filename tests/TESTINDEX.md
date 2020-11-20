# Connectivity
TODO: error handling when no server available
TODO: ensure bolt URI connects to instance in cluster
TODO: Handling of lost connection during transaction

* Verification of URI schemes in regards to TLS expecations
  bolt, neo4j, bolt+s, neo4j+s, bolt+ssc, neo4j+ssc
  tls/securescheme.py, tls/selfsignedscheme.py, tls/unsecurescheme.py

* TLS version support
  TLS 1.1 1.2 1.3
  tls/tlsversions.py

* Handling of lost connections during session.Run operations
  disconnect, session, error handling, custom user-agent
  stub/sessiondisconnected.py

* Invocation of retry function one or more times
  retry, transaction, error handling, disconnect on commit
  stub/retry.py

# Protocol
* No-op sent from server to keep alive connections
  noop, chunking, transport, keep alive
  stub/transport.py
* Bolt versions
  protocol, bolt, version
  stub/versions.py

# Datatypes
TODO: path, relationsship, spatial, temporal
TODO: boundaries of strings, arrays, maps

* Verification that datatypes can be passed through the driver without losing any data, both
  from user code to the database and from the database to user code.
  null, int, string, array, map, dictionary, node
  neo4j/datatypes.py


# Transactions (explicit)
TODO: commit (persists)
TODO: rollback (does not persist)
* Verification that transaction optional parameters are sent on the wire
  tx metadata, timeout, bookmarks
  stub/txparamaters.py


# Transactional functions
TODO: commit (persists)
TODO: rollback (does not persist)


# Run in auto-commit
TODO: consume
TODO: summary
TODO: bookmark chaining

* Verification that results can be iterated correctly, with regards to
  nested results and fetch sizes.
  result, records, invalid query, error while streaming, nested, fetchsize
  neo4j/sessionrun.py
* Verification that session.Run optional parameters are sent on the wire
  tx metadata, timeout, bookmarks
  stub/sessionparamaters.py

# Run in transaction
TODO: nested results
TODO: summary
TODO: bookmark chaining

* Verification that results can be iterated correctly, with regards to
  nested results and fetch sizes.
  result, records, nested, fetchsize
  neo4j/txfuncrun.py

# Routing
* Verification that routing tables are retrieved (v3, v4) and that reader or writer
  are used. Checks enable/disable routing in hello message.
  routing, reader, writer, router
  stub/routing.py


