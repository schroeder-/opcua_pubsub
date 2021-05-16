# Introduction

This is an [OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/) experimental Pubsub implementation.

The implementation is based on the foundation of the crate [opcua](https://github.com/locka99/opcua).

# State

Currently only standalone subscriber and publisher via UADP are supported, see examples standalone_subscriber and standalone_publisher. The subscriber is tested against [open62541](https://open62541.org/) 

# Features

* [x] Standalone Pubsub
* [] integrated Subscription
* [] integrated Publisher

## UADP (Pubsub via Multicast UDP)

* [x] UADP Data Messages
* [x] UADP Delta Messages
* [x] UADP Events Messages
* [x] UADP Keepalive Message
* [] UADP Chunked Messages
* [] UADP Discovery
* [] UADP Security

## Integration Publisher with opcua server

* [] PubSubConnection
* [] PublishedDataSet
* [] WriterGroupe
* [] DataSetWriter

## Integration Subscriber with opcua client

* [] PubSubConnection
* [] SubscripedDataSet
* [] ReaderGroupe
* [] DataSetReader

# License

The code is licenced under [MPL-2.0](https://opensource.org/licenses/MPL-2.0). Like all open source code, you use this code at your own risk.

