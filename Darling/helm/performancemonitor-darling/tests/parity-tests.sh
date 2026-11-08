#!/usr/bin/env bash
# Parity tests: values the chart pins must equal the files they mirror in this repository. Run from anywhere:
# bash Darling/helm/performancemonitor-darling/tests/parity-tests.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

REPO_ROOT="$(cd "${CHART_DIR}/../../.." && pwd)"
COMPOSE="${REPO_ROOT}/Darling/compose/docker-compose.yml"
SS="${RELEASE}-performancemonitor-darling-store"

# The six -c pairs of a compose file's store command, without the leading `postgres`, as a JSON list.
compose_store_args() {
  yq -o=json -I=0 '.services.store.command | .[1:]' "$1"
}

chart_store_args() {
  doc StatefulSet "${SS}" | yq -o=json -I=0 '.spec.template.spec.containers[0].args'
}

t store_args_match_compose
render
assert_eq "$(chart_store_args)" "$(compose_store_args "${COMPOSE}")" "store args vs compose"
assert_eq "$(chart_store_args | yq 'length')" "12" "twelve args (six -c pairs)"

t parity_guard_catches_drift
drifted="$(mktemp)"
sed 's/work_mem=16MB/work_mem=32MB/' "${COMPOSE}" > "${drifted}"
if [ "$(compose_store_args "${drifted}")" = "$(chart_store_args)" ]; then
  rm -f "${drifted}"
  echo "the comparison did not notice a changed work_mem" >&2
  exit 1
fi
rm -f "${drifted}"

t store_image_matches_ci_pin
render
ci_pin="$(yq '.env.STORE_IMAGE' "${REPO_ROOT}/.github/workflows/darling-linux-store.yml")"
assert_eq "$(doc StatefulSet "${SS}" | yq '.spec.template.spec.containers[0].image')" "${ci_pin}" "store image vs CI pin"

t image_repo_matches_build
# The image= line of the darling-linux-publish job, with the repository owner resolved to the upstream owner.
built="$(grep -E '^\s*image="ghcr\.io/.*/performancemonitor-darling"' "${REPO_ROOT}/.github/workflows/build.yml" | head -1 \
  | sed -E 's/^[^"]*"//; s/"[^"]*$//; s/\$\{\{ github\.repository_owner \}\}/erikdarlingdata/')"
assert_eq "${built}" "ghcr.io/erikdarlingdata/performancemonitor-darling" "build.yml image name"
assert_eq "$(yq '.image.repository' "${CHART_DIR}/values.yaml")" "${built}" "chart image.repository vs build.yml"
