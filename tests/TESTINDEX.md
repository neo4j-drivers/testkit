# Connectivity

* Verification of URI schemes in regards to TLS expecations
  bolt, neo4j, bolt+s, neo4j+s, bolt+ssc, neo4j+ssc
  tls/securescheme.py, tls/selfsignedscheme.py, tls/unsecurescheme.py

* TLS version support
  TLS 1.1 1.2 1.3
  tls/tlsversions.py

# Datatypes

* Verification that datatypes can be passed through the driver without losing any data, both
  from user code to the database and from the database to user code.
  null, int, string, array, map, dictionary, node
  neo4j/datatypes.py

  TODO: path, relationsship, spatial, temporal

# Transactions (explicit)

  TODO: commit (persists), rollback (does not persist)


# Transactional functions

  TODO: commit (persists), rollback (does not persist)


# Result iteration

  TODO: auto commit iteration nested results

  TODO: transaction with nested results
