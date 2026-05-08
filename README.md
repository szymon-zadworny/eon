# Extensible Object Network reference implementation

This project implements an example P2P network using the ObjectParser interface
as described by the [EON protocol](https://szymon-zadworny.github.io/eon-proto/).

## Project structure
This Cargo workspace is currently divided into:
- `eon-client` - the main client to the network
- `libp2p-invert` - an inversion of control layer for libp2p. This will be moved
into a separate repository when it gets ready
- `objects` - the set of core EON objects

## Usage
Compile the whole workspace, then run the `eon-client` binary. You can either
supply the commands at runtime or by preloading them from a YAML file. Currently
supported commands:
- `Provide <path>` - provides a file from given path
- `Publish <path>` - publishes a file from given path to the network
- `Get <id>` - downloads an object from the network
- `Wait <duration>` - waits a given duration
- `WaitRandom` - waits a random duration
- `Quit` - gracefully exits the program

## Testing
Currently two testing backends are provided:
- [Kubernetes](https://github.com/szymon-zadworny/eon-k8s) - non-deterministic
results, mature support
- [Shadow](https://github.com/szymon-zadworny/eon-shadow) - deterministic results,
limited compatibility
