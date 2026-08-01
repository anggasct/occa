#!/bin/sh
set -eu

# OCCA install script. Detects the platform, downloads the matching release
# binary, and delegates the OpenCode prerequisite to its own official
# installer when it is not already on PATH.

RELEASE_BASE="${OCCA_RELEASE_BASE:-https://github.com/anggasct/occa/releases/latest/download}"

uname_os=$(uname -s)
uname_arch=$(uname -m)

case "$uname_os" in
	Linux) os="linux" ;;
	Darwin) os="darwin" ;;
	*)
		echo "occa: unsupported OS: $uname_os" >&2
		exit 1
		;;
esac

case "$uname_arch" in
	x86_64 | amd64) arch="amd64" ;;
	aarch64 | arm64) arch="arm64" ;;
	*)
		echo "occa: unsupported architecture: $uname_arch" >&2
		exit 1
		;;
esac

install_dir="${OCCA_INSTALL_DIR:-${XDG_BIN_DIR:-$HOME/bin}}"
mkdir -p "$install_dir"

asset="occa_${os}_${arch}"
url="${RELEASE_BASE}/${asset}"
tmp="${install_dir}/.occa-download-$$"
trap 'rm -f "$tmp"' EXIT

curl -fsSL "$url" -o "$tmp"
chmod +x "$tmp"
mv -f "$tmp" "$install_dir/occa"

if ! command -v opencode >/dev/null 2>&1; then
	curl -fsSL https://opencode.ai/install | bash
fi

echo "occa installed to ${install_dir}/occa"
