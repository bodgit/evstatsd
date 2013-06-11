evstatsd
========

Small libevent-based implementation of statsd.

It provides a JSON-serving webserver listening on port 8126 for viewing and
deleting metrics:

 $ curl -s -XGET http://localhost:8126/counters
 [
     "prefix.server.apache.bytes",
     "prefix.server.apache.response.200",
     "prefix.server.apache.response.301",
     "prefix.server.apache.response.302",
     "prefix.server.apache.response.304",
     "prefix.server.apache.response.400",
     "prefix.server.apache.response.403",
     "prefix.server.apache.response.404",
     "prefix.server.apache.response.405",
     "prefix.server.apache.response.408",
     "prefix.server.apache.response.500"
 ]

An individual metric can be viewed:

 $ curl -s -XGET http://localhost:8126/counters/prefix.server.apache.bytes
 {
     "last_modified": 1370874868,
     "name": "prefix.server.apache.bytes",
     "value": 55569425.0
 }

Issuing a DELETE request to the same rule will delete the metric.
