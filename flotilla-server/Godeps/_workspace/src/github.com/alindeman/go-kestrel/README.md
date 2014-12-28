# go.kestrel

A simple wrapper over kestrel's thrift API. Includes support for multiple
servers and retries.

It's still early days, so dig into client.go for all the details of what
you can do.

## Basic Usage

```go
import (
  "github.com/alindeman/go.kestrel"
  "time"
)

// assuming the thrift port is 2229
client := kestrel.NewClient("localhost", 2229)

item := []byte("Hello World")
nitems, err := client.Put("queue1", [][]byte{item})

items, err := client.Get("queue1", 1, 0, 1*time.Minute)
```

## Cluster Usage

```go
import (
  "github.com/alindeman/go.kestrel"
  "time"
)

// assuming the thrift port is 2229
clients := []*kestrel.Client{
  kestrel.NewClient("host1", 2229),
  kestrel.NewClient("host2", 2229),
  kestrel.NewClient("host3", 2229),
}
cluster := kestrel.NewClusterReader(clients)

ch := make(chan *kestrel.QueueItem, 10)
go cluster.ReadIntoChannel("queue1", ch)

item1 := <-ch
item2 := <-ch

cluster.Close()
```
