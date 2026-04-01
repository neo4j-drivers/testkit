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
  * [x] retries
  * [x] db selection
    * [x] basic case
    * [x] driver rejects any DB name that doesn't match `^[a-zA-Z0-9.-]{3,}$`
  * [x] summary
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
  * [x] errors
    * [x] via all APIs
    * [x] different failure modes (only after some records)
    * [x] Only last error in list is raised
    * [x] Clients must be ready to handle 2xx status codes with errors
  * [x] TX rollback resulting in empty body with no content-type header
  * [ ] auth manager
  * [x] Auth schemes
    * [x] basic and bearer
    * [x] (failing) kerberos, custom, basic with realm
    * [x] driver rejects basic auth with usernames containing `:`
  * [x] client agent string is not transmitted
* [x] Test cluster affinity header (must be optional)
* [x] Basic ITs (probably only types and basic APIs)
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
