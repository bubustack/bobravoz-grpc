#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMPDIR_ROOT="${TMPDIR:-/tmp}"

tmp_home="$(mktemp -d "${TMPDIR_ROOT%/}/staticcheck-home-XXXXXX")"
tmp_gocache="$(mktemp -d "${TMPDIR_ROOT%/}/staticcheck-gocache-XXXXXX")"
trap 'rm -rf "${tmp_home}" "${tmp_gocache}"' EXIT

# Staticcheck (and the go tool it invokes) insist on $HOME/Library/Caches on macOS.
# Point HOME to a writable temp dir while keeping the module cache/toolchain path stable.
mkdir -p "${tmp_home}/Library/Caches"
gomodcache="$(go env GOMODCACHE)"

(
	cd "${ROOT}"
	HOME="${tmp_home}" \
	GOMODCACHE="${gomodcache}" \
	GOCACHE="${tmp_gocache}" \
	staticcheck "$@"
)
