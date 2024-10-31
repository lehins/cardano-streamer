# cardano-streamer


## Installing

### Dependencies

In order to get working with Cardano a few C dependencies need to be installed first:

* `libsodium`
* `secp256k1`
* `libblst`


```shell
$ cabal update
$ cabal install
```

#### `Linux`

* Ubuntu

```shell
$ sudo apt install pkg-config autoconf automake libtool libsodium-dev zlib1g-dev
$ ./scripts/install-blst.sh
$ ./scripts/install-secp256k1.sh
```

* OpenSUSE

```
$ sudo zypper in pkg-config autoconf automake libtool libsodium-devel zlib-devel
$ ./scripts/install-blst.sh
$ ./scripts/install-secp256k1.sh
```

## Chain data


In order to get the latest chain data run this in [`cardano-node`](https://github.com/IntersectMBO/cardano-node) repo:

```shell
$ nix develop
$ cabal update
$ cabal build cardano-node
$ cabal run -- cardano-node run --topology configuration/cardano/mainnet-topology.json --database-path /path/to/chain/mainnet/db --socket-path /path/to/chain/mainnet/db/node.socket --host-addr 0.0.0.0 --port 3001 --config configuration/cardano/mainnet-config.json +RTS -N2 -A16m -qg -qb --disable-delayed-os-memory-return -RTS
```

## Executing

### Benchmarking

Here is an example on how to benchmark validation from one slot to another

```
cabal run -- cstreamer benchmark --config=/path/to/cardano-node/configuration/cardano/mainnet-config.json --chain-dir="/path/to/chain/mainnet/db" -r 72316895 -s 84844884 --suffix cstreamer --out-dir .
```

