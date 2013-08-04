Results
    * Postgres: Query a returns PGresult struct with all information that's
    contents are valid as long as the struct exists, even after the connection
    is closed. Can simply return unique pointer. Supports random access.

    * SQLite: Retrieve one row at a time from the prepared statement object. No
    random access, forward only. Can't even return a result object since a
    second query of the same prepared statement will lose the state of the old
    result.
