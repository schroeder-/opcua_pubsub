# Introduction

This is an [OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/) experimental Pubsub implementation.

The implementation is based on the foundation of the crate [opcua](https://github.com/locka99/opcua).

# State

Currently only standalone subscriber and publisher via UADP are supported, see examples standalone_subscriber and standalone_publisher. The subscriber is tested against [open62541](https://open62541.org/) 

Warning this crate is in prototyping stage! Library interfaces will like change.

# Features
* [x] Pubsub Messages
* [ ] Standalone Subscription
* [x] Standalone Publisher
* [x] integrated Subscription
* [x] integrated Publisher
* [ ] PubSub EventLoop
* [ ] Tokio support
## UADP (Pubsub via Multicast UDP)

* [x] UADP Data Messages
* [x] UADP Delta Messages
* [ ] UADP Events Messages
* [x] UADP Keepalive Message
* [ ] UADP Chunked Messages
* [ ] UADP Discovery
* [ ] UADP Security

## Integration Publisher with opcua server

* [x] PubSubConnection
* [x] PublishedDataSet
* [x] WriterGroup
* [x] DataSetWriter
* [ ] Information Model

## Integration Subscriber with opcua server

* [x] PubSubConnection
* [x] SubscripedDataSet
* [x] ReaderGroup
* [x] DataSetReader
* [x] Link DataSet with DataSource
* [ ] Information Model

## WriterGroup
* [ ] MessageOrdering
* [ ] Raw Structures
* [ ] KeepAlive
* [ ] DeltaFrames
* [ ] Max message length

## General
* [ ] Conform specs for alle points
* [ ] Expand integration tests and add more tests
# License

The code is licenced under [MPL-2.0](https://opensource.org/licenses/MPL-2.0). Like all open source code, you use this code at your own risk.

