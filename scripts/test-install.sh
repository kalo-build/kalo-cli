#!/bin/sh
# Hermetic checks for install.sh: a stub curl serves a fake release, and PATH
# holds only the tools install.sh needs (no sudo). Run: sh scripts/test-install.sh
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=$(mktemp -d)
trap 'chmod -R u+w "$T" 2>/dev/null; rm -rf "$T"' EXIT

if [ "$(id -u)" -eq 0 ]; then
    echo "SKIP: running as root, every directory is writable"
    exit 0
fi

# A bin dir with only what install.sh calls. No sudo on purpose.
BIN="$T/bin"
mkdir -p "$BIN"
for tool in sh mktemp tar gzip mv chmod mkdir rm dirname grep sed uname cat; do
    p=$(command -v "$tool") || { echo "missing tool: $tool"; exit 1; }
    ln -s "$p" "$BIN/$tool"
done

# Fake release: a kalo that prints a version line.
mkdir -p "$T/rel"
printf '#!/bin/sh\necho "kalo version 0.0.0-test"\n' > "$T/rel/kalo"
chmod +x "$T/rel/kalo"
tar -czf "$T/release.tar.gz" -C "$T/rel" kalo

# Stub curl: `curl -fsSL URL -o DEST` copies the fake release to DEST.
cat > "$BIN/curl" <<EOF
#!/bin/sh
while [ \$# -gt 0 ]; do
    case "\$1" in -o) shift; cat "$T/release.tar.gz" > "\$1" ;; esac
    shift
done
EOF
chmod +x "$BIN/curl"

# From here on a failed check is recorded, not fatal.
set +e
fail=0
check() { # name, condition result
    if [ "$2" -eq 0 ]; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi
}

run() { # HOME, install dir, extra PATH prefix; output in $T/out, exit code in $RC
    env -i HOME="$1" PATH="$3$BIN" KALO_VERSION=v0.0.0-test KALO_INSTALL_DIR="$2" \
        sh "$ROOT/install.sh" > "$T/out" 2>&1
    RC=$?
}

last_line() { tail -n 1 "$T/out" | sed 's/\x1b\[[0-9;]*m//g'; }

# 1. Target not writable, no sudo: falls back to ~/.local/bin, PATH hint last.
H="$T/h1"; mkdir -p "$H" "$T/ro"; chmod 0555 "$T/ro"
run "$H" "$T/ro/bin" ""
check "1 exit 0 without sudo on a read-only target" "$RC"
[ -x "$H/.local/bin/kalo" ]; check "1 kalo installed in \$HOME/.local/bin" $?
grep -q "Installing to $H/.local/bin instead" "$T/out"; check "1 says where it installed instead" $?
[ "$(last_line)" = "[WARN]   export PATH=\"$H/.local/bin:\$PATH\"" ]; check "1 last line is the export PATH hint" $?

# 2. Writable target already on PATH: installs there, no PATH hint.
H="$T/h2"; mkdir -p "$H/bin"
run "$H" "$H/bin" "$H/bin:"
check "2 exit 0 on a writable target" "$RC"
[ -x "$H/bin/kalo" ]; check "2 kalo installed in the target" $?
! grep -q "not in your PATH" "$T/out"; check "2 no PATH hint when the target is on PATH" $?

# 3. Writable target not on PATH (the page's line): PATH hint last.
H="$T/h3"; mkdir -p "$H"
run "$H" "$H/.local/bin" ""
check "3 exit 0 with KALO_INSTALL_DIR=\$HOME/.local/bin" "$RC"
[ "$(last_line)" = "[WARN]   export PATH=\"$H/.local/bin:\$PATH\"" ]; check "3 last line is the export PATH hint" $?

# 4. Nothing writable, no sudo: clear error, non-zero exit, nothing installed.
H="$T/ro/home"
run "$H" "$T/ro/bin" ""
[ "$RC" -ne 0 ]; check "4 non-zero exit when nothing is writable" $?
grep -q "sudo is not available. Set KALO_INSTALL_DIR" "$T/out"; check "4 error names KALO_INSTALL_DIR" $?

if [ "$fail" -ne 0 ]; then
    echo "--- last output ---"; cat "$T/out"
    exit 1
fi
echo "all install.sh checks passed"
