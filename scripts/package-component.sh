#!/usr/bin/env bash
set -euo pipefail
umask 022

if [[ $# -ne 3 ]]; then
  echo "usage: $0 SEMVER GOARCH ABSOLUTE_OUTPUT_DIR" >&2
  exit 2
fi

version=$1
arch=$2
output_dir=$3

validate_semver() {
  local candidate=$1 prerelease build identifier
  if [[ ! $candidate =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+)(\.[0-9A-Za-z-]+)*)?(\+([0-9A-Za-z-]+)(\.[0-9A-Za-z-]+)*)?$ ]]; then
    return 1
  fi
  prerelease=${candidate%%+*}
  if [[ $prerelease == *-* ]]; then
    prerelease=${prerelease#*-}
    IFS=. read -r -a identifiers <<<"$prerelease"
    for identifier in "${identifiers[@]}"; do
      if [[ $identifier =~ ^[0-9]+$ && $identifier != 0 && $identifier == 0* ]]; then
        return 1
      fi
    done
  fi
  if [[ $candidate == *+* ]]; then
    build=${candidate#*+}
    [[ -n $build ]] || return 1
  fi
}

validate_semver "$version" || {
  echo "version must be a valid SemVer 2.0.0 value" >&2
  exit 2
}
if [[ $arch != amd64 && $arch != arm64 ]]; then
  echo "GOARCH must be amd64 or arm64" >&2
  exit 2
fi
if [[ $output_dir != /* || ! -d $output_dir ]]; then
  echo "output directory must be an existing absolute directory" >&2
  exit 2
fi

repo_root=$(git rev-parse --show-toplevel)
[[ $(pwd -P) == "$repo_root" ]] || {
  echo "run from repository root" >&2
  exit 2
}
output_dir=$(cd "$output_dir" && pwd -P)
case "$output_dir" in
  "$repo_root"|"$repo_root"/*)
    echo "output directory must be outside the repository" >&2
    exit 2
    ;;
esac
[[ -z $(git status --porcelain=v1 --untracked-files=all) ]] || {
  echo "refusing to package a dirty worktree" >&2
  exit 2
}

declared_version=$(tr -d '\r\n' <VERSION)
[[ $declared_version == "$version" ]] || {
  echo "requested version does not match VERSION" >&2
  exit 2
}
commit=$(git rev-parse HEAD)
epoch=$(git show -s --format=%ct HEAD)
host_arch=$(go env GOARCH)
if [[ $host_arch != "$arch" ]]; then
  echo "mining-pool uses native cgo; build on a $arch runner (current: $host_arch)" >&2
  exit 2
fi

stage=$(mktemp -d "$output_dir/.pool-${version}-${arch}.XXXXXX")
case "$stage" in
  "$output_dir"/.pool-*) ;;
  *) echo "unsafe stage path" >&2; exit 2 ;;
esac
cleanup() { rm -rf -- "$stage"; }
trap cleanup EXIT
install -d -m 0755 "$stage/bin"

release_ldflags="-s -w -X asicPool/internal/buildinfo.Version=${version} -X asicPool/internal/buildinfo.SourceCommit=${commit}"
CGO_ENABLED=1 GOOS=linux GOARCH="$arch" go build -buildvcs=true -trimpath \
  -ldflags "${release_ldflags} -X main.productVersion=${version}" \
  -o "$stage/bin/mining-pool" ./cmd/pool
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -buildvcs=true -trimpath \
  -ldflags "$release_ldflags" \
  -o "$stage/bin/poolworker" ./cmd/poolworker

verify_binary_source() {
  local binary=$1 build_info
  build_info=$(go version -m "$binary")
  if grep -F 'vcs.revision=' <<<"$build_info" >/dev/null; then
    grep -F "vcs.revision=$commit" <<<"$build_info" >/dev/null
    grep -F 'vcs.modified=false' <<<"$build_info" >/dev/null
  fi
  strings "$binary" | grep -F "$commit" >/dev/null
  strings "$binary" | grep -F "$version" >/dev/null
}
verify_binary_source "$stage/bin/mining-pool"
verify_binary_source "$stage/bin/poolworker"
[[ -z $(git status --porcelain=v1 --untracked-files=all) ]] || {
  echo "build modified the source worktree" >&2
  exit 2
}

printf '%s\n' "$commit" >"$stage/SOURCE_COMMIT"
printf '%s\n' "$version" >"$stage/COMPONENT_VERSION"
archive="$output_dir/pool-${version}-linux-${arch}.tar.gz"
[[ ! -e $archive ]] || {
  echo "refusing to replace $archive" >&2
  exit 2
}
find "$stage" -mindepth 1 -type f -printf '%P\0' \
  | LC_ALL=C sort -z \
  | tar --format=gnu --null --no-recursion -C "$stage" --files-from=- \
      --mtime="@$epoch" --owner=0 --group=0 --numeric-owner \
      --use-compress-program='gzip -n' -cf "$archive"
if tar -tzf "$archive" | grep -E '^\./|^/' >/dev/null; then
  echo "archive contains a non-canonical member path" >&2
  exit 2
fi
(cd "$output_dir" && sha256sum "$(basename "$archive")" >"$(basename "$archive").sha256")
echo "$archive"
