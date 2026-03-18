# cardano-streamer


## Installing

### Dependencies

In order to get working with Cardano a few C dependencies need to be installed first:

* `libsodium`
* `secp256k1`
* `libblst`

#### Using `nix`

- `nix flake update haskellNix` to update all of the Haskell ecosystem.
- `nix flake update haskellNix/hackage` for updating Hackage dependencies only - necessary when
  `index-state` is updated
- `nix flake update CHaP` for CHaP (Cardano Haskell Packages) - necessary when
  `index-state` is updated

#### Without `nix`

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


## Building

If using `nix`, first it is necessary to get into the nix shell:

```shell
$ nix develop
```

After that business is usual:

```shell
$ cabal update
$ cabal install
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

### Dump Snapshot

The `dump-snapshot` command reads a ledger snapshot from a Cardano node chain
database and emits a JSON document to stdout with the data needed to verify
epoch reward, treasury, and reserve calculations. It is primarily intended as
a reference dataset for alternative Cardano node implementations.

```shell
$ cstreamer dump-snapshot \
    --config=/path/to/cardano-node-config.json \
    --chain-dir=/path/to/chain/db \
    --start-slot=<slot-at-epoch-boundary> \
    > epoch-N-slot-S.json
```

The `--start-slot` value should point to a snapshot that was written by
`cardano-node` with `--write-snapshot <slot>`. If omitted, the tip of the
immutable chain is used.

#### JSON schema

| Field | Type | Description |
|-------|------|-------------|
| `epoch` | integer | Epoch number |
| `snapshotEraName` | string | Name of the era at the snapshot point |
| `protocolParams` | object | Protocol parameters relevant to reward calculations (see below) |
| `activeStake` | integer | Total lovelace delegated in the **go** snapshot (lovelace) |
| `eta` | number | Epoch performance multiplier: `min(1, blocksMade / expectedBlocks)`. Set to 1 when `d ≥ 0.8` |
| `expectedBlocks` | integer | Number of blocks expected this epoch: `(1 - d) * f * epochSize` |
| `rupdNext` | object | Reward update for the **next** epoch transition (see below) |
| `treasury` | integer | Current treasury balance (lovelace) |
| `reserves` | integer | Current reserves balance (lovelace) |
| `totalPools` | integer | Number of pools in the current pool distribution |
| `poolDistribution` | array | Per-pool stake fractions for the current epoch |
| `epochFees` | integer | Fees collected during the previous epoch (lovelace); feeds into `rPot` |
| `deposits` | object | Outstanding deposit obligations: `stakeKey`, `pool`, `dRep`, `proposal`, `total` (lovelace) |
| `instantaneousRewards` | object | Pending MIR transfers queued for the next epoch boundary (Shelley–Babbage only; always empty in Conway+). Fields: `iRReserves`, `iRTreasury` (credential→lovelace maps), `deltaReserves`, `deltaTreasury` (DeltaCoin adjustments). |
| `snapshots` | object | The three stake snapshots: `mark` (N+1), `set` (N), `go` (N-1) |

##### `protocolParams`

| Field | Description |
|-------|-------------|
| `rho` | Monetary expansion rate |
| `tau` | Treasury expansion rate |
| `d` | Decentralisation parameter (0 = fully decentralised) |
| `a0` | Pool pledge influence |
| `nOpt` | Desired number of pools (k parameter, used for saturation) |
| `minPoolCost` | Minimum pool operating cost (lovelace) |
| `protocolVersion` | `{ major, minor }` |

##### `rupdNext`

All amounts are in lovelace. The object shape depends on whether the reward
pulser was still in progress at the snapshot point:

**Pulsing / not-yet-started** (most common — the pulser is forced to
completion by `cardano-streamer`):

| Field | Description |
|-------|-------------|
| `deltaR1` | Amount taken from reserves (`rho × reserves`) |
| `deltaR2` | Unclaimed rewards returned to reserves (`rewardPot - totalDistributed`) |
| `deltaT1` | Amount sent to treasury (`tau × rPot`) |
| `rPot` | Total reward pot: `epochFees + deltaR1` |
| `rewardPot` | Amount available for distribution: `rPot - deltaT1` |
| `totalDistributed` | Total actually distributed to stake credentials |

**Complete** (pulser already finished before snapshot was taken):

| Field | Description |
|-------|-------------|
| `deltaT1` | Amount sent to treasury |
| `deltaR` | Net reserves change: `deltaR2 - deltaR1` (negative = net taken from reserves) |
| `totalDistributed` | Total distributed to stake credentials |

##### `snapshots`

Each of `mark`, `set`, and `go` contains:

| Field | Description |
|-------|-------------|
| `name` | Snapshot name (`mark`, `set`, or `go`) |
| `stake` | Map of stake credential → lovelace |
| `delegations` | Map of stake credential → pool ID |
| `poolParams` | Map of pool ID → pool registration parameters |
| `blocks` | Map of pool ID → blocks made this epoch (**mark** uses `nesBcur`; **go** uses `nesBprev`; **set** omits this field because the epoch N-2 block data is no longer retained) |

### Benchmarking

Here is an example on how to benchmark validation from one slot to another

```
cabal run -- cstreamer benchmark --config=/path/to/cardano-node/configuration/cardano/mainnet-config.json --chain-dir="/path/to/chain/mainnet/db" -r 72316895 -s 84844884 --suffix cstreamer --out-dir .
```

