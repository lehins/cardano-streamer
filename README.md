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

```shell
$ sudo apt install pkg-config libsodium-dev
$ ./scripts/install-blst.sh
$ ./scripts/install-secp256k1.sh
```
