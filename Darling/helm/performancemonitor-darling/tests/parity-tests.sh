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

t release_workflow_logs_docker_in_before_signing
# cosign reads registry credentials from Docker's config only; helm registry login writes Helm's own file. build.yml logs
# Docker in before it signs the image, and the chart release must do the same before it pushes and signs the chart.
release_wf="${REPO_ROOT}/.github/workflows/darling-helm-release.yml"
step_index() { # index of the first publish step whose run block contains the fixed string $1, or -1
  yq ".jobs.publish.steps | to_entries | map(select(.value.run // \"\" | contains(\"$1\"))) | .[0].key // -1" "${release_wf}"
}
login_at="$(step_index 'docker login ghcr.io')"
sign_at="$(step_index 'cosign sign')"
push_at="$(step_index 'helm push')"
[ "${login_at}" -ge 0 ] || { echo "the release workflow never runs docker login ghcr.io" >&2; exit 1; }
[ "${sign_at}" -ge 0 ] || { echo "the release workflow has no cosign sign step" >&2; exit 1; }
[ "${login_at}" -le "${push_at}" ] || { echo "docker login ghcr.io comes after helm push" >&2; exit 1; }
[ "${login_at}" -lt "${sign_at}" ] || { echo "docker login ghcr.io comes after cosign sign" >&2; exit 1; }

t release_workflow_tool_pins_match_chart_ci
# The release lints and packages with the same helm and yq as the chart's CI, so a release behaves as its pull request did.
for pin in HELM_VERSION YQ_VERSION; do
  assert_eq "$(yq ".env.${pin}" "${release_wf}")" "$(yq ".env.${pin}" "${REPO_ROOT}/.github/workflows/darling-helm.yml")" "${pin} in the release workflow vs darling-helm.yml"
done

t release_workflow_follows_build
# workflow_run names the triggering workflow by its name: a renamed build.yml would silently stop the chart publishing.
assert_eq "$(yq '.on.workflow_run.workflows[0]' "${release_wf}")" "$(yq '.name' "${REPO_ROOT}/.github/workflows/build.yml")" "workflow_run target vs build.yml name"
