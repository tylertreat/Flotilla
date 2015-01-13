# Flotilla

Flotilla is a **work-in-progress** tool for testing message queues in more realistic environments. [Many benchmarks](https://github.com/tylertreat/mq-benchmarking) only measure performance characteristics on a single machine, sometimes with producers and consumers in the *same process* even. The reality is this information is marginally useful, if at all, and often deceiving. This [blog post](http://www.bravenewgeek.com/benchmark-responsibly/) provides some more background on the motivation behind this project.

Testing anything at scale can be difficult to achieve in practice. It generally takes a lot of resources and often requires ad hoc solutions. Flotilla attempts to provide automated orchestration for benchmarking message queues in scaled-up configurations. Simply put, we can benchmark a message broker with arbitrarily many producers and consumers distributed across arbitrarily many machines with a single command.

```shell
flotilla-client \
    --broker=kafka \
    --host=192.168.59.100:9500 \
    --peer-hosts=localhost:9500,192.168.59.101:9500,192.168.59.102:9500,192.168.59.103:9500 \
    --producers=5 \
    --consumers=3 \
    --num-messages=1000000
    --message-size=5000
```

In addition to simulating more realistic testing scenarios, Flotilla also tries to offer more statistically meaningful results in the benchmarking itself. It relies on [HDR Histogram](http://hdrhistogram.github.io/HdrHistogram/) (or rather a [Go variant](https://github.com/codahale/hdrhistogram) of it) which supports recording and analyzing sampled data value counts at extremely low latencies. See the "Caveats" section below for potential benchmarking issues and areas for improvement.

Flotilla supports several message brokers out of the box:

- [Beanstalkd](http://kr.github.io/beanstalkd/)
- [NATS](http://nats.io/)
- [Kafka](http://kafka.apache.org/)
- [Kestrel](http://twitter.github.io/kestrel/)
- [ActiveMQ](http://activemq.apache.org/)
- [RabbitMQ](http://www.rabbitmq.com/)
- [NSQ](http://nsq.io/)
- [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs)

## Installation

Flotilla consists of two binaries: the server daemon and client. The daemon runs on any machines you wish to include in your tests. The client orchestrates and executes the tests. Note that the daemon makes use of [Docker](https://www.docker.com/) for running many of the brokers, so it must be installed on the host machine. If you're running OSX, use [boot2docker](http://boot2docker.io/).

To install the daemon, run:

```bash
$ go get github.com/tylertreat/flotilla/flotilla-server
```

To install the client, run:

```bash
$ go get github.com/tylertreat/flotilla/flotilla-client
```

## Usage

Ensure the daemon is running on any machines you wish Flotilla to communicate with:

```bash
$ flotilla-server
Flotilla daemon started on port 9500...
```

### Local Configuration

Flotilla can be run locally to perform benchmarks on a single machine. First, start the daemon with `flotilla-server`. Next, run a benchmark using the client:

```bash
$ flotilla-client --broker=rabbitmq
```

Flotilla will run everything on localhost.

### Distributed Configuration

With all daemons started, run a benchmark using the client and provide the peers you wish to communicate with:

```bash
$ flotilla-client --broker=rabbitmq --host=<ip> --peer-hosts=<list of ips>
```

For full usage details, run:

```bash
$ flotilla-client --help
```

### Running on OSX

Flotilla starts most brokers using a Docker container. This can be achieved on OSX using boot2docker, which runs the container in a VM. The daemon needs to know the address of the VM. This can be provided from the client using the `--docker-host` flag, which specifies the host machine (or VM, in this case) the broker will run on.

```bash
$ flotilla-client --broker=rabbitmq --docker-host=$(boot2docker ip)
```

## Caveats

- *Not all brokers are created equal.* Flotilla is designed to make it easy to test drive different messaging systems, but comparing results between them can often be misguided.
- Several brokers support publishing batches of messages to boost throughput (with a latency penalty). Some brokers don't support batching, so messages are published one at a time for these. This affects throughput significantly.
- The latency of a message is measured as the time it's sent subtracted from the time it's received. This requires recording the clocks of both the sender and receiver. If you're running scaled-up, *distributed* tests, then the clocks aren't perfectly synchronized. *These benchmarks aren't perfect.*
- Related to the above point, measuring *anything* requires some computational overhead, which affects results. HDR Histogram tries to minimize this problem but can't remove it altogether.
- There is currently no security built in. Use this tool *at your own risk*. The daemon runs on port 9500 by default.

## TODO

- Many message brokers, such as Kafka, are designed to operate in a clustered configuration for higher availability. Add support for these types of topologies. This gets us closer to what would be deployed in production.
- Some broker clients provide back-pressure heuristics. For example, NATS allows us to slow down publishing if it determines the receiver is falling behind. This greatly improves throughput.
- Replace use of `os/exec` with Docker REST API (how does this work with boot2docker?)
- Plottable data output.
- Integration with [Comcast](https://github.com/tylertreat/Comcast) for testing under different network conditions.
- Use [etcd](https://github.com/coreos/etcd) to provide shared configuration and daemon discovery
- Use [usl](https://github.com/codahale/usl) to populate a [Universal Scalability Law](http://www.perfdynamics.com/Manifesto/USLscalability.html) model
- Use [tinystat](https://github.com/codahale/tinystat) to compare benchmark runs and tease out statistical noise
