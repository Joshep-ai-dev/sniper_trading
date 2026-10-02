#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.tools/debs"
for archive in *.deb; do dpkg-deb -x "$archive" ../native; done
