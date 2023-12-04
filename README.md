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
