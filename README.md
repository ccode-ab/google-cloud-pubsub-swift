
# GoogleCloudPubSUB

This project is WIP and should not be used in production.

A Swift package as client for [Google Cloud PubSub](https://cloud.google.com/pubsub).

## Development

### Updating gRPC-generated Swift-soruces.

1. Make sure the submodule `googleapis` is checked out.
2. Make sure executable `protoc` is installed.
3. Make sure swift plugins for protoc is installed (`protoc-gen-swift` and `protoc-gen-swiftgrpc`)
4.
```bash
cd googleapis/
protoc google/pubsub/v1/*.proto \
  --swift_out=. \
  --swiftgrpc_out=Client=true,Server=false:.

rm ../Sources/GoogleCloudPubSub/gRPC\ Generated/*.swift
mv google/pubsub/v1/*.swift \
   ../Sources/GoogleCloudPubSub/gRPC\ Generated

```
