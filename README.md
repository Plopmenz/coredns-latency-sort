# Latency Sort

Any A/AAAA localhost originating DNS resolve request will ICMP ping all addresses. The first address to respond will be returned first.

Most applications use the first returned record, this makes sure they get the lowest latency one at the expense of a double roundtrip for the first request (and after the cache expires).

## Example Usage

```
. {
    cache
    forward . 1.1.1.1
    latency_sort
}
```
