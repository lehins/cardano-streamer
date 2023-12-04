#!/bin/bash
# I don't understand why this just vanishes.
export PATH=/usr/bin:$PATH

# 0.3.2
SECP256K1_REF=acf5c55ae6a94e5ca847e07def40427547876101

if [[ ! -d secp256k1/.git ]]; then
  git clone https://github.com/bitcoin-core/secp256k1

  cd secp256k1
  git switch $SECP256K1_REF --detach
  ./autogen.sh
  ./configure --enable-module-schnorrsig --enable-experimental
  make
  make check
else
  cd secp256k1
fi

sudo make install
