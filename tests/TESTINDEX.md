# Connectivity
TODO: error handling when no server available
TODO: ensure bolt URI connects to instance in cluster

* Verification of URI schemes in regards to TLS expecations
  bolt, neo4j, bolt+s, neo4j+s, bolt+ssc, neo4j+ssc
  tls/securescheme.py, tls/selfsignedscheme.py, tls/unsecurescheme.py

* TLS version support
  TLS 1.1 1.2 1.3
  tls/tlsversions.py


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


# Transactional functions
TODO: commit (persists)
TODO: rollback (does not persist)


# Run in auto-commit
TODO: nested results
TODO: consume
TODO: summary
TODO: bookmark chaining

* Verification that results can be iterated correctly.
  result, records, invalid query, error while streaming
  neo4j/sessionrun.py


# Run in transaction
TODO: nested results
TODO: summary
TODO: bookmark chaining


