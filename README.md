# Proglog

## Background

- This is a Golang application that follow the instruction from [this book](https://pragprog.com/titles/tjgo/distributed-services-with-go/) to build a distributed log services.

## NOTES
Offset refers to the logical location of the entry in the file. E.g: if we have 2 entry entry0 & entry1, then entry0 would be at offset 0 and entry1 would be at offset1
Pos refers to the actual location of the entry in the file, representing by bytes
E.g: if we have 2 entry entry0 and entry1, each consists of 8bytes. Then entry0 would be at pos=0 and entry1 would be at pos=8

## Components
### Membership
- A component that handles server to server discovery using [Serf](https://www.serf.io/).
- Every instance will be managed by a Serf cluster and whenever there is a new insance joining or leaving the cluster, Serf ensure that every other instance in the cluster knows about this.
- Every instance in the cluster knows about the current state of the cluster (number of current working instance, which instance just joined/left, etc) at all times.
- The below flow demonstrate the flow used in [unit test](./internal//discovery//membership_test.go).
<img src="./asset/membership.svg">

### Replicator
- We store multiple copies of the log data when we have multiple servers in a Serf cluster, making our servcies more resillient to failures.
- When servers discover other servers, we want to trigger the servers to replicate.
- Our replication will be pull-based, with the replication component consuming from each discovered server and producing a copy to the local server.

### Agent
- Exports an Agent that manages the different components and processes that make up the services(replicator, membership, log, and server).
- We can just use the Agent to boot up the whole service instead of having to configuring each components.

## TODO
+ Understand gommap
+ Understand enc ?
+ Understand os.Truncate
+ Understand TLS and CA cert
+ Understand Casbin
+ Understand Serf