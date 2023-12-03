#!/bin/bash
# I don't understand why this just vanishes.
export PATH=/usr/bin:$PATH

# current ref from: 03.12.2023
SECP256K1_REF=d3e29db8bbf81600fe0a6bd70b12fe57a0121b83
PREFIX=/usr/local

sudo apt-get -y install autoconf automake libtool

if [[ ! -d secp256k1/.git ]]; then
  git clone https://github.com/bitcoin-core/secp256k1

  cd secp256k1
  git switch $SECP256K1_REF --detach
  ./autogen.sh
  ./configure $PREFIX --enable-module-schnorrsig --enable-experimental
  make
  make check
else
  cd secp256k1
fi

sudo make install
