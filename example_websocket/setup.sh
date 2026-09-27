#!/bin/bash
# One-time setup for example_websocket. The ofxSyncing core and example_osc
# don't need any of this. Safe to run again.

set -e
cd "$(dirname "$0")"
DIR=$PWD

# Poco isn't bundled with ofxPoco on Linux; it links against the system copy.
# ofxIO's DirectoryUtils.cpp includes <boost/version.hpp>. Skipped when
# already installed, so re-runs don't need sudo or a network.
if ! dpkg -s libpoco-dev >/dev/null 2>&1; then
    sudo apt-get install -y libpoco-dev
fi
if ! dpkg -s libboost-dev >/dev/null 2>&1; then
    sudo apt-get install -y libboost-dev
fi

cd ../..
ADDONS=$PWD

# ofxHTTP and the addons it depends on (ofxPoco ships with openFrameworks).
ARCH=$(uname -m)
clone() {
    if [ ! -d "$2" ]; then
        git clone $3 "$1" "$2"
    fi
}
clone https://github.com/n1ckfg/ofxMediaType ofxMediaType
clone https://github.com/n1ckfg/ofxNetworkUtils ofxNetworkUtils
clone https://github.com/n1ckfg/ofxSSLManager ofxSSLManager
if [ "$ARCH" = "aarch64" ]; then
    clone https://github.com/n1ckfg/ofxHTTP ofxHTTP "-b tezos-chain-trixie"
    clone https://github.com/n1ckfg/ofxIO ofxIO "-b of_0.12.1"
else
    clone https://github.com/n1ckfg/ofxHTTP ofxHTTP
    clone https://github.com/n1ckfg/ofxIO ofxIO
fi

# ofxPoco (bundled with oF 0.12.1) has no linuxaarch64 section in its
# addon_config.mk, so on 64-bit Pi OS no -lPoco* flags are emitted and every
# Poco symbol fails to link. Add the section if it isn't already there. This
# is the same patch as PiNaplpsPlayer's setup.sh.
if ! grep -q '^linuxaarch64:' ofxPoco/addon_config.mk; then
    python3 - <<'PATCH'
path = "ofxPoco/addon_config.mk"
with open(path) as f:
    text = f.read()

section = """linuxaarch64:
\tADDON_LDFLAGS = -lPocoNetSSL
\tADDON_LDFLAGS += -lPocoNet
\tADDON_LDFLAGS += -lPocoCrypto
\tADDON_LDFLAGS += -lPocoUtil
\tADDON_LDFLAGS += -lPocoJSON
\tADDON_LDFLAGS += -lPocoXML
\tADDON_LDFLAGS += -lPocoZip
\tADDON_LDFLAGS += -lPocoFoundation
\tADDON_LDFLAGS += -lcrypto
\tADDON_LDFLAGS += -lssl

msys2:"""

with open(path, "w") as f:
    f.write(text.replace("msys2:", section, 1))
print("patched ofxPoco/addon_config.mk for linuxaarch64")
PATCH
fi

# oF 0.12.1's ofTypes.h uses std::shared_ptr (ofPtr) without including
# <memory>, which breaks translation units that include it first. Also the
# same as PiNaplpsPlayer's.
FILE="$ADDONS/../libs/openFrameworks/types/ofTypes.h"
if ! grep -q "#include <memory>" "$FILE"; then
    if grep -q "#include <mutex>" "$FILE"; then
        sed -i '/#include <mutex>/a #include <memory>' "$FILE"
        echo "added #include <memory> to ofTypes.h"
    else
        echo "couldn't patch $FILE: no #include <mutex> to add <memory> after"
        exit 1
    fi
fi

cd "$DIR"
echo "example_websocket is ready: make, then ./bin/example_websocket"
