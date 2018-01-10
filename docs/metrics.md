# Backbeat Metrics

Backbeat exposes various metric routes which return a response with an HTTP
code.

## Response Codes

```
+----------+------------------------------------------------------------------+
| Response | Details                                                          |
+==========+==================================================================+
|   200    | OK: success                                                      |
+----------+------------------------------------------------------------------+
|   403    | AccessDenied: request IP address must be defined in              |
|          |   'conf/config.json' in field 'server.healthChecks.allowFrom'    |
+----------+------------------------------------------------------------------+
|   404    | RouteNotFound: route must be valid                               |
+----------+------------------------------------------------------------------+
|   405    | MethodNotAllowed: the HTTP verb must be a GET                    |
+----------+------------------------------------------------------------------+
|   500    | InternalError: this could be caused by one of several            |
|          |   components: the api server, Kafka, Zookeeper, Redis, or one    |
|          |   of the Producers for a topic topic                             |
+----------+------------------------------------------------------------------+
```

## Routes

### `/_/metrics`

This route gathers all the metrics below and returns as one JSON object.

### `/_/metrics/backlog`

This route returns the replication backlog in number of objects and number of
total MB. Replication backlog represents the objects that have been queued up
to be replicated to another site, but the replication task has not been
completed yet for that object.

**Example Output**:

```
"backlog":{
    "description":"Number of incomplete replication operations (count) and
    number of incomplete MB transferred (size)",
    "results":{
        "count":4,
        "size":"6.12"
    }
}
```

### `/_/metrics/completions`

This route returns the replication completions in number of objects and number
of total MB transferred. Completions are only collected up to an `EXPIRY` time,
which is currently set to **15 minutes**.

**Example Output**:

```
"completions":{
    "description":"Number of completed replication operations (count) and number
    of MB transferred (size) in the last 900 seconds",
    "results":{
        "count":31,
        "size":"47.04"
    }
}
```

### `/_/metrics/throughput`

This route returns the current throughput in number of operations per second
(or number of objects replicating per second) and number of total MB completing
per second.

**Example Output**:

```
"throughput":{
    "description":"Current throughput for replication operations in ops/sec
    (count) and MB/sec (size)",
    "results":{
        "count":"0.00",
        "size":"0.00"
    }
}
```
