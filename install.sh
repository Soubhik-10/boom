#!/usr/bin/env bash
set -euo pipefail

repo="${BOOM_REPO:-Soubhik-10/boom}"
version="${BOOM_VERSION:-latest}"
install_dir="${BOOM_INSTALL_DIR:-${HOME}/.local/bin}"

os="$(uname -s)"
arch="$(uname -m)"
case "${os}:${arch}" in
  Linux:x86_64|Linux:amd64) asset="boom-linux-x86_64.tar.gz" ;;
  Linux:aarch64|Linux:arm64) asset="boom-linux-aarch64.tar.gz" ;;
  Darwin:x86_64|Darwin:amd64) asset="boom-macos-x86_64.tar.gz" ;;
  Darwin:arm64|Darwin:aarch64) asset="boom-macos-aarch64.tar.gz" ;;
  *) echo "Unsupported platform: ${os} ${arch}" >&2; exit 1 ;;
esac

if [[ "${version}" == "latest" ]]; then
  base_url="https://github.com/${repo}/releases/latest/download"
else
  version="${version#v}"
  base_url="https://github.com/${repo}/releases/download/v${version}"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
archive="${tmp_dir}/${asset}"
checksums="${tmp_dir}/SHA256SUMS"

curl --fail --silent --show-error --location "${base_url}/${asset}" --output "${archive}"
curl --fail --silent --show-error --location "${base_url}/SHA256SUMS" --output "${checksums}"

expected="$(awk -v file="${asset}" '$2 == file { print $1; exit }' "${checksums}")"
[[ -n "${expected}" ]] || { echo "Checksum entry not found for ${asset}" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "${archive}" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "${archive}" | awk '{print $1}')"
fi
[[ "${actual}" == "${expected}" ]] || { echo "Checksum verification failed" >&2; exit 1; }

mkdir -p "${install_dir}"
tar -xzf "${archive}" -C "${tmp_dir}"
install -m 0755 "${tmp_dir}/boom" "${install_dir}/boom"
echo "Installed boom to ${install_dir}/boom"
case ":${PATH}:" in
  *":${install_dir}:"*) ;;
  *) echo "Add ${install_dir} to PATH to invoke boom" ;;
esac
