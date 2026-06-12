#!/usr/bin/env bash
# Re-vendor crates/wiretap-core/proto/ from the upstream Sui v2 protos at a
# pinned commit. Reproducible: bytes are identical for a given
# (SUI_COMMIT, SUI_RPC_REV) pair. The pinned revs are recorded in
# crates/wiretap-core/proto/REVISION after a successful run.
#
# The protos themselves live in MystenLabs/sui-rust-sdk (the MystenLabs/sui
# workspace pulls them via a git dependency in its top-level Cargo.toml). We
# read SUI's commit, parse the `sui-rpc` rev out of its Cargo.toml, and copy
# from that exact rev of sui-rust-sdk. Override either with env vars.
set -euo pipefail

SUI_COMMIT="${SUI_COMMIT:-719ac32d413bde983502e8ae3e5b36ff6aaa0989}"
SUI_RPC_REV_OVERRIDE="${SUI_RPC_REV:-}"
DEST_ROOT="$(cd "$(dirname "$0")/.." && pwd)/crates/wiretap-core/proto"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "[1/4] resolving sui-rpc rev from MystenLabs/sui @ $SUI_COMMIT"
git clone --depth 1 --filter=blob:none --sparse --quiet \
    https://github.com/MystenLabs/sui.git "$TMP/sui"
( cd "$TMP/sui" && git fetch --quiet --depth 1 origin "$SUI_COMMIT" \
                 && git checkout --quiet "$SUI_COMMIT" )

if [ -n "$SUI_RPC_REV_OVERRIDE" ]; then
    SUI_RPC_REV="$SUI_RPC_REV_OVERRIDE"
else
    SUI_RPC_REV="$(grep -E '^sui-rpc = \{.*sui-rust-sdk' "$TMP/sui/Cargo.toml" \
                   | sed -E 's/.*rev = "([0-9a-f]{40})".*/\1/' | head -n1)"
    if [ -z "$SUI_RPC_REV" ]; then
        echo "  ! could not parse sui-rpc rev from sui/Cargo.toml" >&2
        exit 1
    fi
fi
echo "    using sui-rust-sdk @ $SUI_RPC_REV"

echo "[2/4] sparse-checkout MystenLabs/sui-rust-sdk"
git clone --depth 1 --filter=blob:none --sparse --quiet \
    https://github.com/MystenLabs/sui-rust-sdk.git "$TMP/sdk"
( cd "$TMP/sdk" \
    && git fetch --quiet --depth 1 origin "$SUI_RPC_REV" \
    && git checkout --quiet "$SUI_RPC_REV" \
    && git sparse-checkout init --cone \
    && git sparse-checkout set crates/sui-rpc/vendored/proto )

SRC="$TMP/sdk/crates/sui-rpc/vendored/proto"
if [ ! -d "$SRC/sui/rpc/v2" ]; then
    echo "  ! expected $SRC/sui/rpc/v2 but it's missing" >&2
    exit 1
fi

echo "[3/4] copying proto tree into $DEST_ROOT (preserving import paths)"
rm -rf "$DEST_ROOT"
mkdir -p "$DEST_ROOT"
cp -R "$SRC/." "$DEST_ROOT/"

echo "[4/4] recording pinned revs in $DEST_ROOT/REVISION"
{
    echo "sui_commit: $SUI_COMMIT"
    echo "sui_rpc_rev: $SUI_RPC_REV"
    echo "vendored_at: $(date -u +%FT%TZ)"
} > "$DEST_ROOT/REVISION"

echo "done. Run: cargo build"
