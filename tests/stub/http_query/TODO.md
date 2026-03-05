# TODO

* [ ] Test APIs
  * [x] Explicit TX
    * [x] impersonation
    * [x] session auth
    * [x] access mode
    * [x] rollback
  * [x] TX functions
    * [x] impersonation
    * [x] session auth
    * [x] access mode
    * [x] rollback
  * [x] Auto commit (session run)
    * [x] impersonation
    * [x] session auth
    * [x] access mode
  * [x] bookmarks + session bookmark chaining
  * [ ] retires
  * [x] db selection
  * [ ] summary
    * [x] db
    * [x] query & parameters
    * [x] counters
    * [x] profile
    * [x] plan
    * [x] notifications
    * [x] server
      * [x] agent string
      * [x] address
  * [ ] verify connectivity
  * [ ] errors
  * [ ] auth manager
  * [x] Auth schemes
    * [x] basic and bearer
    * [x] (failing) kerberos, custom, basic with realm
  * [ ] client agent string
* [ ] Test cluster affinity header (must be optional)
* [ ] Basic ITs (probably only types and basic APIs)
* [ ] v1.1
  * [ ] test new types
  * [ ] vectors
  * [ ] unsupported type
  * [ ] support `+jsonl` content type
* [ ] features to be implemented server-side
  * [ ] notification filtering
  * [ ] Transaction config
    * [ ] meta data
    * [ ] tx timeout
  * [ ] summary
    * [ ] query type
    * [ ] timers
