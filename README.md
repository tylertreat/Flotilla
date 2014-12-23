# flotilla

Flotilla is a tool for testing message queues in more realistic environments. [Many benchmarks](https://github.com/tylertreat/mq-benchmarking) only measure performance characteristics on a single machine, sometimes with producers and consumers in the *same process*. The reality is this information is marginally useful, if at all, and often deceiving.

Testing anything at scale can be difficult to achieve in practice. It generally takes a lot of resources and often requires ad hoc solutions. Flotilla attempts to provide automated orchestration for benchmarking message queues in scaled-up configurations. Simply put, we can benchmark a message broker with arbitrarily many producers and consumers on arbitrarily many machines with a single command.

```shell
flotilla-client \
    --broker=kafka \
    --host=192.168.59.100:9000 \
    --peer-hosts=localhost:9000,192.168.59.101:9000,192.168.59.102:9000,192.168.59.103:9000 \
    --producers=5 \
    --consumers=3 \
    --num-messages=1000000
```

In addition to simulating more realistic testing scenarios, Flotilla also tries to offer more statistically meaningful results in the benchmarking itself. It relies on [HDR Histogram](http://hdrhistogram.github.io/HdrHistogram/) (or rather a [Go variant](https://github.com/codahale/hdrhistogram) of it) which supports recording and analyzing sampled data value counts at extremely low latencies.

## Caveats
