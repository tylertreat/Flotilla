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

It supports several message brokers out of the box:

- [Beanstalkd](http://kr.github.io/beanstalkd/)
- [NATS](http://nats.io/)
- [Kafka](http://kafka.apache.org/)
- [Kestrel](http://twitter.github.io/kestrel/)
- [ActiveMQ](http://activemq.apache.org/)
- [RabbitMQ](http://www.rabbitmq.com/)
- [NSQ](http://nsq.io/)
- [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs)

## Caveats

- *Not all brokers are created equal.* Flotilla is designed to make it easy to test drive different messaging systems, but comparing results between them can often be misguided.
- The latency of a message is measured as the time it's sent subtracted from the time it's received. This requires recording the clocks of both the sender and receiver. If you're running scaled-up, *distributed* tests, then the clocks aren't perfectly synchronized. *These benchmarks aren't perfect.*
- Related to the second point, measuring *anything* requires some computational overhead, which affects results. HDR Histogram tries to minimize this problem but can't remove it altogether.

## TODO

- Many message brokers, such as Kafka, are designed to operate in a clustered configuration for higher availability. Add support for these types of topologies. This gets us closer to what would be deployed in production.
- Many brokers support publishing batches of messages to boost throughput. Flotilla currently publishes a single message at a time as fast as possible, but this *hugely* affects throughput and typically doesn't reflect a production setting.
- Some broker clients provide back-pressure heuristics. For example, NATS allows us to slow down publishing if it determines the receiver is falling behind. This greatly improves throughput.
