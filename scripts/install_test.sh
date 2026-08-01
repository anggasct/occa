#!/bin/sh
set -eu

# Shell harness for install.sh: stubs uname, curl, and opencode on PATH so
# every acceptance criterion is an exit-code or file-existence assertion.
# No network access and no real download happen.

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

pass() {
	echo "ok: $*"
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

stub_bin="$work/stub-bin"
mkdir -p "$stub_bin"
calls="$work/calls.log"
home="$work/home"
mkdir -p "$home"

cat > "$stub_bin/uname" <<'STUB'
#!/bin/sh
if [ "${1:-}" = "-m" ]; then
	echo "$FAKE_UNAME_M"
else
	echo "$FAKE_UNAME_S"
fi
STUB

cat > "$stub_bin/curl" <<'STUB'
#!/bin/sh
{
	echo "curl $*"
} >> "$CALLS_LOG"
prev=""
for arg in "$@"; do
	if [ "$prev" = "-o" ]; then
		printf 'stub-binary\n' > "$arg"
	fi
	prev="$arg"
done
STUB

cat > "$stub_bin/opencode" <<'STUB'
#!/bin/sh
exit 0
STUB
chmod +x "$stub_bin/uname" "$stub_bin/curl" "$stub_bin/opencode"

run_install() {
	(
		export PATH="$stub_bin:/usr/bin:/bin"
		export HOME="$home"
		export CALLS_LOG="$calls"
		export FAKE_UNAME_S="${1:-}"
		export FAKE_UNAME_M="${2:-}"
		shift 2 2>/dev/null || true
		export OCCA_INSTALL_DIR="${OCCA_INSTALL_DIR:-}"
		"$@"
	) 2>"$work/err.log" || return $?
	cat "$work/err.log" >&2
}

count_calls() {
	grep -c "$1" "$calls" 2>/dev/null || true
}

asset_for() {
	case "$1/$2" in
		Linux/x86_64) echo "occa_linux_amd64" ;;
		Linux/aarch64) echo "occa_linux_arm64" ;;
		Darwin/x86_64) echo "occa_darwin_amd64" ;;
		Darwin/arm64) echo "occa_darwin_arm64" ;;
	esac
}

for pair in "Linux x86_64" "Linux aarch64" "Darwin x86_64" "Darwin arm64"; do
	os=${pair% *}
	arch=${pair#* }
	inst="$work/inst-$os-$arch"
	mkdir -p "$inst"
	: > "$calls"
	rm -f "$inst/occa"

	if ! run_install "$os" "$arch" env OCCA_INSTALL_DIR="$inst" sh ./install.sh; then
		fail "$os/$arch: install exited non-zero"
	fi
	asset=$(asset_for "$os" "$arch")
	if [ "$(count_calls "$asset")" -ne 1 ]; then
		fail "$os/$arch: expected one download of $asset"
	fi
	if [ ! -x "$inst/occa" ]; then
		fail "$os/$arch: no executable occa at install path"
	fi
	pass "$os/$arch: binary installed"
done

#: OCCA_INSTALL_DIR overrides default and XDG_BIN_DIR.
inst="$work/inst-override"
xdg="$work/inst-xdg"
mkdir -p "$inst" "$xdg"
: > "$calls"
run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst" XDG_BIN_DIR="$xdg" sh ./install.sh
if [ ! -x "$inst/occa" ]; then
	fail "OCCA_INSTALL_DIR ignored"
fi
if [ -e "$xdg/occa" ]; then
	fail "binary landed in XDG_BIN_DIR despite OCCA_INSTALL_DIR"
fi
pass "OCCA_INSTALL_DIR wins over XDG_BIN_DIR"

#: opencode missing -> exactly one call to its installer.
: > "$calls"
mv "$stub_bin/opencode" "$work/opencode-hidden"
run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst" sh ./install.sh
if [ "$(count_calls "opencode.ai/install")" -ne 1 ]; then
	fail "expected exactly one opencode installer call"
fi
pass "opencode installer invoked once when missing"

#: opencode present -> no installer call.
: > "$calls"
mv "$work/opencode-hidden" "$stub_bin/opencode"
run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst" sh ./install.sh
if [ "$(count_calls "opencode.ai/install")" -ne 0 ]; then
	fail "opencode installer called despite binary on PATH"
fi
pass "opencode not reinstalled when present"

#: OCCA_VERSION pins the release download path.
inst_ver="$work/inst-ver"
mkdir -p "$inst_ver"
: > "$calls"
run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst_ver" OCCA_VERSION="v0.1.0" sh ./install.sh
if [ "$(count_calls "releases/download/v0.1.0/occa_linux_amd64")" -ne 1 ]; then
	fail "OCCA_VERSION did not pin the download URL"
fi
if [ ! -x "$inst_ver/occa" ]; then
	fail "no binary installed from pinned version"
fi
pass "OCCA_VERSION pins the release download"

#: OCCA_VERSION without the v prefix is normalized.
: > "$calls"
run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst_ver" OCCA_VERSION="0.1.0" sh ./install.sh
if [ "$(count_calls "releases/download/v0.1.0/occa_linux_amd64")" -ne 1 ]; then
	fail "bare version not normalized to v prefix"
fi
pass "bare version normalized to v prefix"

#: without OCCA_VERSION the default latest path is used.
: > "$calls"
run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst_ver" sh ./install.sh
if [ "$(count_calls "releases/latest/download/occa_linux_amd64")" -ne 1 ]; then
	fail "default download path is not latest"
fi
pass "default download path is latest"

#: unsupported platform fails with nothing installed.
inst_bad="$work/inst-bad"
mkdir -p "$inst_bad"
: > "$calls"
rm -f "$inst_bad/occa"
if run_install "Windows" "x86_64" env OCCA_INSTALL_DIR="$inst_bad" sh ./install.sh; then
	fail "unsupported OS did not fail"
fi
if ! grep -q "unsupported OS: Windows" "$work/err.log"; then
	fail "error message does not name the unsupported OS"
fi
if [ -e "$inst_bad/occa" ]; then
	fail "binary installed on unsupported platform"
fi
pass "unsupported OS fails cleanly"

# Idempotence: second run against the same install dir succeeds.
: > "$calls"
if ! run_install "Linux" "x86_64" env OCCA_INSTALL_DIR="$inst" sh ./install.sh; then
	fail "idempotence: second run failed"
fi
if [ "$(count_calls "occa_linux_amd64")" -ne 1 ]; then
	fail "idempotence: second run did not redownload/overwrite"
fi
pass "idempotence: repeat install exits 0"

echo "all tests passed"
