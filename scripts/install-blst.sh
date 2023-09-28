#!/bin/bash
# I don't understand why this just vanishes.
export PATH=/usr/bin:$PATH

# current ref from: 25.11.2022
BLST_REF=03b5124029979755c752eec45f3c29674b558446
PREFIX=/usr/local

git clone https://github.com/supranational/blst
cd blst
git reset --hard $BLST_REF
./build.sh

sudo mkdir -p ${PREFIX}/lib/pkgconfig
sudo mkdir -p ${PREFIX}/include/blst
sudo cp bindings/{blst.h,blst_aux.h} ${PREFIX}/include/blst/
sudo cp -f libblst.{a,dll,so,dylib} ${PREFIX}/lib/

cat <<EOF > libblst.pc
prefix=${PREFIX}
exec_prefix=\${prefix}
libdir=\${prefix}
includedir=\${prefix}/include/blst

Name: libblst
Version: 0.3.10
Description: Multilingual BLS12-381 signature library

Cflags: -I\${includedir}
Libs: -L\${libdir} -lblst
EOF

sudo mv libblst.pc ${PREFIX}/lib/pkgconfig/libblst.pc

cd ../

ls -laF

rm -rf blst

# Add this to .bashrc:
export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH"
