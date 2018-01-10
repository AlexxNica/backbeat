# Backbeat Healthcheck

Backbeat exposes a healthcheck and a deep healthcheck route which return a
response with an HTTP code.

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
|          |   components: the api server, Kafka, Zookeeper, or one of the    |
|          |   Producers for a topic                                          |
+----------+------------------------------------------------------------------+
```

## Routes

### `/_/healthcheck`

Basic healthchecks return details on the health of Kafka and its topics.

If no HTTP response error, the structure of the response is an array of 3
key:value objects.

One of these key:value objects will have keys of numbers, and values with an
object of keys with `nodeId`, `host`, and `port`. The number keys represent
each zookeeper node, and their respective details.

```
zookeeperNode: {
    nodeId: <value>,
    host: <value>,
    port: <value>
}
```

One of these key:value objects will have a key called `metadata`. The value
is an object where each key represents each zookeeper node, and each value holds
details on topic name, partition number, leader number, replica count, and
in-sync replicas per partition.

```
metadata: {
    zookeeperNode: {
        topic: <value>,
        partition: <value>,
        leader: <value>
    }
}
```

One of these key:value objects will have a key called `internalConnections`.
The value is an object with 3 keys: `isrHealth` which will be either "ok" or
"error", `zookeeper` which shows a status and details on the status (more about
zookeeper status codes
[here at node-zookeeper-client#state](https://github.com/alexguan/node-zookeeper-client#state)),
and `kafkaProducer` which will be either "ok" or "error" and checks the health
of all Producers for every topic.

```
internalConnections: {
    isrHealth: <ok || error>,
    zookeeper: {
        status: <ok || error>,
        details: {
            name: <value>,
            code: <value>
        }
    },
    kafkaProducer: {
        status: <ok || error>
    }
}
```

**Example Output**:
(NOTE: some sections in the example below are contracted to reduce redundancy)

```
[
    {
        "0":{
            "nodeId":0,
            "host":"server-node1",
            "port":9092
        },
        ...
        "4":{
            "nodeId":4,
            "host":"server-node5",
            "port":9092
        }
    },
    {
        "metadata": {
            "0":{
                "topic":"backbeat-replication",
                "partition":0,
                "leader":4,
                "replicas":[0,1,4],
                "isr":[1,4,0]
            },
            ...
            "4":{
                "topic":"backbeat-replication",
                "partition":4,
                "leader":3,
                "replicas":[0,3,4],
                "isr":[4,3,0]
            }
        }
    },
    {
        "internalConnections":{
            "isrHealth":"ok",
            "zookeeper":{
                "status":"ok",
                "details":{
                    "name":"SYNC_CONNECTED",
                    "code":3
                }
            },
            "kafkaProducer":{
                "status":"ok"
            }
        }
    }
]
```

### `/_/healthcheck/deep`

Deep healthchecks will return the health of every available partition for the
replication topic.

Rather than getting an internal status variable or calling an internal status
function to check the health of the replication topic, a deep healthcheck will
produce and consume a message directly to the replication topic for every
available partition.

If no HTTP response error, the structure of the response is a JSON object of:

```
topicPartition: <ok || error>,
...
timeElapsed: <value>
```

**Example Output**:

```
{
    "0":"ok",
    "1":"ok",
    "2":"error",
    "3":"ok",
    "4":"ok",
    "timeElapsed":560
}
```
