#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -d .tools/rustup ]; then
  export CARGO_HOME="$PWD/.tools/cargo" RUSTUP_HOME="$PWD/.tools/rustup"
  export PATH="$CARGO_HOME/bin:$PATH"
fi
if [ -d .tools/native ]; then
  export LIBCLANG_PATH="$PWD/.tools/native/usr/lib/llvm-18/lib"
  export LD_LIBRARY_PATH="$PWD/.tools/native/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
  export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.tools/native/usr/lib/llvm-18/lib/clang/18/include"
  export OPENSSL_INCLUDE_DIR="$PWD/.tools/native/usr/include"
  export OPENSSL_LIB_DIR="$PWD/.tools/native/usr/lib/x86_64-linux-gnu"
  export CFLAGS="-I$PWD/.tools/native/usr/include/x86_64-linux-gnu"
fi
export CARGO_BUILD_JOBS=4
export CARGO_TARGET_DIR="${SNIPER_BUILD_TARGET:-$PWD/target}"
cargo "$@"
