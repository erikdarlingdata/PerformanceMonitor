#!/usr/bin/env bash
# Release tests: .github/scripts/darling-helm-release.sh, which stamps, checks and packages the chart for
# .github/workflows/darling-helm-release.yml. No cluster and no registry: the image digest is a fixed fake value.
# Run from anywhere: bash Darling/helm/performancemonitor-darling/tests/release-tests.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

REPO_ROOT="$(cd "${CHART_DIR}/../../.." && pwd)"
SCRIPT="${REPO_ROOT}/.github/scripts/darling-helm-release.sh"
VERSION=3.99.0
TAG=v3.99.0
# Mixed case on purpose: ghcr names are lowercase, so the script must lowercase the owner.
OWNER=ErikDarlingData
DIGEST="sha256:$(printf 'a%.0s' $(seq 1 64))"
IMAGE="ghcr.io/erikdarlingdata/performancemonitor-darling"

# Helm never needs a cluster here; an empty kubeconfig keeps any accidental lookup off a real context.
KUBECONFIG="$(mktemp)"
export KUBECONFIG

WORK="$(mktemp -d)"
cleanup_work() { rm -rf "${WORK}" "${KUBECONFIG}"; }
# lib.sh's EXIT trap (on_exit) prints PASS/FAIL from the exit status; run the cleanup first, then hand it that status.
trap 'status=$?; set +e; cleanup_work; (exit ${status}); on_exit' EXIT

# package [chart dir] [extra args...]: runs the script into a fresh out dir; sets OUTDIR, STDOUT, STDERR.
package() {
  local chart="${1:-${CHART_DIR}}"
  shift || true
  OUTDIR="$(mktemp -d -p "${WORK}")"
  STDOUT="${OUTDIR}.stdout"
  STDERR="${OUTDIR}.stderr"
  if ! bash "${SCRIPT}" package --chart "${chart}" --tag "${TAG}" --version "${VERSION}" --owner "${OWNER}" \
    --digest "${DIGEST}" --out "${OUTDIR}" "$@" > "${STDOUT}" 2> "${STDERR}"; then
    echo "package failed:" >&2
    cat "${STDERR}" >&2
    return 1
  fi
}

# assert_release_fails <stderr substring> <script args...>: the script must exit non-zero and say why.
assert_release_fails() {
  local expected="$1" err
  shift
  err="$(mktemp -p "${WORK}")"
  if bash "${SCRIPT}" "$@" > /dev/null 2> "${err}"; then
    echo "assert_release_fails: the script succeeded, expected a failure containing: ${expected}" >&2
    exit 1
  fi
  if ! grep -qF -- "${expected}" "${err}"; then
    echo "assert_release_fails: stderr does not contain: ${expected}" >&2
    cat "${err}" >&2
    exit 1
  fi
}

# fails_with <substring> <tag> <version> <owner> <digest>: package with these inputs must be refused.
fails_with() {
  assert_release_fails "$1" package --chart "${CHART_DIR}" --tag "$2" --version "$3" --owner "$4" --digest "$5" \
    --out "$(mktemp -d -p "${WORK}")"
}

darling_image_of() {
  helm template "${RELEASE}" "$1" | yq 'select(.kind == "Deployment") | .spec.template.spec.containers[] | select(.name == "darling") | .image'
}

t stamps_four_values
package
assert_eq "$(yq '.version' "${OUTDIR}/chart/Chart.yaml")" "${VERSION}" "Chart.yaml version"
assert_eq "$(yq '.appVersion' "${OUTDIR}/chart/Chart.yaml")" "${VERSION}" "Chart.yaml appVersion"
assert_eq "$(yq '.image.tag' "${OUTDIR}/chart/values.yaml")" "${VERSION}" "values image.tag"
assert_eq "$(yq '.image.digest' "${OUTDIR}/chart/values.yaml")" "${DIGEST}" "values image.digest"
assert_eq "$(yq '.image.repository' "${OUTDIR}/chart/values.yaml")" "${IMAGE}" "values image.repository unchanged"

t package_renders_pinned_image
package
assert_eq "$(tail -n 1 "${STDOUT}")" "package=${OUTDIR}/performancemonitor-darling-${VERSION}.tgz" "last stdout line"
assert_eq "$(darling_image_of "${OUTDIR}/performancemonitor-darling-${VERSION}.tgz")" "${IMAGE}:${VERSION}@${DIGEST}" "rendered image"

t checksum_matches
package
(cd "${OUTDIR}" && sha256sum -c --quiet SHA256SUMS-helm.txt)
assert_eq "$(wc -l < "${OUTDIR}/SHA256SUMS-helm.txt" | tr -d ' ')" "1" "one checksum line"
assert_eq "$(awk '{print $2}' "${OUTDIR}/SHA256SUMS-helm.txt" | sed 's/^\*//')" "performancemonitor-darling-${VERSION}.tgz" "checksum names the package"

t package_has_no_cr
crlf="$(mktemp -d -p "${WORK}")/performancemonitor-darling"
cp -r "${CHART_DIR}" "${crlf}"
rm -rf "${crlf}/tests"
find "${crlf}" -type f \( -name '*.yaml' -o -name '*.tpl' -o -name '*.json' -o -name '*.txt' -o -name '*.md' -o -name '.helmignore' \) \
  -exec sed -i 's/\r$//; s/$/\r/' {} +
# CR bytes counted with tr, not grep: Git for Windows' grep drops CR before matching.
cr_count() { find "$1" -type f -exec cat {} + | tr -cd '\r' | wc -c | tr -d ' '; }
[ "$(cr_count "${crlf}")" -gt 0 ] || { echo "the CRLF fixture has no CR; the test would prove nothing" >&2; exit 1; }
package "${crlf}"
extracted="$(mktemp -d -p "${WORK}")"
tar -m -xzf "${OUTDIR}/performancemonitor-darling-${VERSION}.tgz" -C "${extracted}"
assert_eq "$(cr_count "${extracted}")" "0" "CR bytes in the package"

t checkout_untouched
before="$(git -C "${CHART_DIR}" status --porcelain --untracked-files=all -- .)"
package
after="$(git -C "${CHART_DIR}" status --porcelain --untracked-files=all -- .)"
assert_eq "${after}" "${before}" "chart directory unchanged by a run"

t refuses_non_release_tags
for bad in nightly v3.99 v3.99.0-rc1; do
  fails_with "not a release tag" "${bad}" "${VERSION}" "${OWNER}" "${DIGEST}"
done

t refuses_version_mismatch
fails_with "does not match" v3.99.1 "${VERSION}" "${OWNER}" "${DIGEST}"

t refuses_bad_digest
fails_with "digest" "${TAG}" "${VERSION}" "${OWNER}" "sha256:abc"
fails_with "digest" "${TAG}" "${VERSION}" "${OWNER}" "${VERSION}"

t refuses_foreign_repository
fails_with "image.repository" "${TAG}" "${VERSION}" someone-else "${DIGEST}"

t verify_stamp_catches_extra_change
original="$(mktemp -d -p "${WORK}")/c"
cp -r "${CHART_DIR}" "${original}"
stamped="$(mktemp -d -p "${WORK}")/c"
cp -r "${CHART_DIR}" "${stamped}"
VERSION="${VERSION}" yq -i '.version = strenv(VERSION) | .appVersion = strenv(VERSION)' "${stamped}/Chart.yaml"
VERSION="${VERSION}" DIGEST="${DIGEST}" yq -i '.image.tag = strenv(VERSION) | .image.digest = strenv(DIGEST)' "${stamped}/values.yaml"
bash "${SCRIPT}" verify-stamp "${original}" "${stamped}" "${VERSION}" "${DIGEST}"
yq -i '.resources.limits.memory = "8Gi"' "${stamped}/values.yaml"
assert_release_fails "resources" verify-stamp "${original}" "${stamped}" "${VERSION}" "${DIGEST}"

# decide: what a publish run does, from what the registry and the release already hold (#8870 review: a published
# version is never replaced unless the run asks for it).
decide() { bash "${SCRIPT}" decide --version "${VERSION}" "$@"; }
decision() { # decision <expected push> <expected sign> <expected upload> <decide args...>
  local want="push=$1 sign=$2 upload=$3" got
  shift 3
  got="$(decide "$@" | tr '\n' ' ' | sed 's/ $//')"
  assert_eq "${got}" "${want}" "decide $*"
}
ASSETS_BOTH="performancemonitor-darling-${VERSION}.tgz SHA256SUMS-helm.txt"

t decide_first_publish_does_everything
decision true true packaged --exists false --signed false --assets "" --overwrite false

t decide_complete_release_does_nothing
decision false false none --exists true --signed true --assets "${ASSETS_BOTH}" --overwrite false

t decide_existing_unsigned_version_is_signed_not_pushed
decision false true none --exists true --signed false --assets "${ASSETS_BOTH}" --overwrite false

t decide_missing_assets_come_from_the_published_chart
decision false false pulled --exists true --signed true --assets "" --overwrite false
decision false false pulled --exists true --signed true --assets "SHA256SUMS-helm.txt" --overwrite false

t decide_overwrite_replaces_everything
decision true true packaged --exists true --signed true --assets "${ASSETS_BOTH}" --overwrite true

t decide_refuses_assets_without_a_published_chart
assert_release_fails "overwrite" decide --version "${VERSION}" --exists false --signed false --assets "${ASSETS_BOTH}" --overwrite false

t decide_refuses_bad_flags
assert_release_fails "--exists" decide --version "${VERSION}" --exists maybe --signed false --assets "" --overwrite false

# same-chart: a chart already in the registry is only signed or attached when it is this release's chart (#8870 review).
t same_chart_accepts_the_same_chart
package
ours="${OUTDIR}/performancemonitor-darling-${VERSION}.tgz"
published="$(mktemp -d -p "${WORK}")/performancemonitor-darling-${VERSION}.tgz"
cp "${ours}" "${published}"
bash "${SCRIPT}" same-chart "${published}" "${ours}" "${DIGEST}"

t same_chart_refuses_another_digest
other_digest="sha256:$(printf 'b%.0s' $(seq 1 64))"
package
ours="${OUTDIR}/performancemonitor-darling-${VERSION}.tgz"
DIGEST_SAVED="${DIGEST}"; DIGEST="${other_digest}"; package; DIGEST="${DIGEST_SAVED}"
published="${OUTDIR}/performancemonitor-darling-${VERSION}.tgz"
assert_release_fails "overwrite: true" same-chart "${published}" "${ours}" "${DIGEST}"
assert_release_fails "digest" same-chart "${published}" "${ours}" "${DIGEST}"

t same_chart_refuses_another_template
package
ours="${OUTDIR}/performancemonitor-darling-${VERSION}.tgz"
changed="$(mktemp -d -p "${WORK}")/performancemonitor-darling"
cp -r "${CHART_DIR}" "${changed}"
printf '\n# not this release\n' >> "${changed}/templates/deployment.yaml"
package "${changed}"
published="${OUTDIR}/performancemonitor-darling-${VERSION}.tgz"
assert_release_fails "templates/deployment.yaml" same-chart "${published}" "${ours}" "${DIGEST}"
