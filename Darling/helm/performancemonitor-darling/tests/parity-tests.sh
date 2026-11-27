#!/usr/bin/env bash
# Parity tests: values the chart pins must equal the files they mirror in this repository. Run from anywhere:
# bash Darling/helm/performancemonitor-darling/tests/parity-tests.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

REPO_ROOT="$(cd "${CHART_DIR}/../../.." && pwd)"
COMPOSE="${REPO_ROOT}/Darling/compose/docker-compose.yml"
SS="${RELEASE}-performancemonitor-darling-store"

# The twelve -c pairs of a compose file's store command, without the leading `postgres`, as a JSON list.
compose_store_args() {
  yq -o=json -I=0 '.services.store.command | .[1:]' "$1"
}

chart_store_args() {
  doc StatefulSet "${SS}" | yq -o=json -I=0 '.spec.template.spec.containers[0].args'
}

t store_args_match_compose
render
assert_eq "$(chart_store_args)" "$(compose_store_args "${COMPOSE}")" "store args vs compose"
assert_eq "$(chart_store_args | yq 'length')" "24" "twenty-four args (twelve -c pairs)"

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

t release_workflow_logs_docker_in_before_pushing_and_signing
# cosign reads registry credentials from Docker's config only; helm registry login writes Helm's own file. build.yml logs
# Docker in before it signs the image, and the chart release must do the same before it pushes and signs the chart.
release_wf="${REPO_ROOT}/.github/workflows/darling-helm-release.yml"
# The publish job's run blocks in step order, one line each; first_line <fixed string> is the first line holding it.
yq '.jobs.publish.steps[].run // ""' "${release_wf}" > "${OUT}"
first_line() { grep -n -F -m1 -- "$1" "${OUT}" | cut -d: -f1; }
login_at="$(first_line 'docker login ghcr.io')"
push_at="$(first_line 'helm push')"
sign_at="$(first_line 'cosign sign')"
for what in login_at push_at sign_at; do
  [ -n "${!what}" ] || { echo "the publish job has no ${what%_at} command" >&2; exit 1; }
done
[ "${login_at}" -lt "${push_at}" ] || { echo "docker login ghcr.io comes after helm push" >&2; exit 1; }
[ "${login_at}" -lt "${sign_at}" ] || { echo "docker login ghcr.io comes after cosign sign" >&2; exit 1; }

t release_workflow_write_rights_only_in_publish
# Only the publish job may write; every other job is read-only, and no checkout keeps its token (#8870 review).
assert_eq "$(yq '.jobs | has("package")' "${release_wf}")" "true" "a separate read-only package job"
for job in $(yq '.jobs | keys | .[]' "${release_wf}"); do
  perms="$(yq -o=json -I=0 ".jobs.${job}.permissions // {}" "${release_wf}")"
  if [ "${job}" = publish ]; then
    assert_eq "${perms}" '{"contents":"write","packages":"write","id-token":"write"}' "publish job permissions"
  else
    assert_eq "$(yq -o=json -I=0 ".jobs.${job}.permissions // {} | to_entries | map(select(.value != \"read\")) | from_entries" "${release_wf}")" '{}' "${job} job permissions are read-only"
  fi
done
assert_eq "$(yq '[.jobs[].steps[] | select(.uses // "" | test("^actions/checkout@")) | select(.with."persist-credentials" != false)] | length' "${release_wf}")" "0" "every checkout sets persist-credentials: false"

t release_workflow_publishes_only_from_main
# The signing identity names the workflow at refs/heads/main, so a run from any other ref must not publish.
assert_eq "$(yq '.jobs.publish.if' "${release_wf}" | grep -c "github.ref == 'refs/heads/main'")" "1" "publish job is gated on main"

t release_workflow_checks_tool_downloads
# Every helm or yq download is checked against a pinned sha256 before it runs: in each step, one `sha256sum -c` per download.
downloads=0
export RE='get\.helm\.sh|mikefarah/yq'
steps="$(yq '[.jobs[].steps[] | select(.run // "" | test(strenv(RE)))] | length' "${release_wf}")"
for ((k = 0; k < steps; k++)); do
  run="$(K="${k}" yq '[.jobs[].steps[] | select(.run // "" | test(strenv(RE)))] | .[env(K)].run' "${release_wf}")"
  fetched="$(grep -cE "curl .*(${RE})" <<<"${run}" || true)"
  checked="$(grep -c 'sha256sum -c' <<<"${run}" || true)"
  assert_eq "${checked}" "${fetched}" "sha256 checks vs downloads in download step ${k}"
  downloads=$((downloads + fetched))
done
[ "${downloads}" -ge 3 ] || { echo "found ${downloads} helm/yq downloads, expected at least 3; the check above proved nothing" >&2; exit 1; }

t release_workflow_verify_identity_is_exact
# The documented verify command names the exact workflow identity, never a regexp that a fork's branch name can match.
for f in "${release_wf}" "${REPO_ROOT}/docs/darling/kubernetes.md"; do
  if grep -q -- '--certificate-identity-regexp' "${f}"; then echo "${f} still uses --certificate-identity-regexp" >&2; exit 1; fi
  grep -q -F -- "darling-helm-release.yml@refs/heads/main" "${f}" || { echo "${f} does not name the exact signing identity" >&2; exit 1; }
done

t release_workflow_tool_pins_match_chart_ci
# The release lints and packages with the same helm and yq as the chart's CI, so a release behaves as its pull request did.
for pin in HELM_VERSION YQ_VERSION; do
  assert_eq "$(yq ".env.${pin}" "${release_wf}")" "$(yq ".env.${pin}" "${REPO_ROOT}/.github/workflows/darling-helm.yml")" "${pin} in the release workflow vs darling-helm.yml"
done

t release_workflow_follows_build
# workflow_run names the triggering workflow by its name: a renamed build.yml would silently stop the chart publishing.
assert_eq "$(yq '.on.workflow_run.workflows[0]' "${release_wf}")" "$(yq '.name' "${REPO_ROOT}/.github/workflows/build.yml")" "workflow_run target vs build.yml name"

t release_workflow_checks_a_published_chart_before_deciding
# A chart already in the registry is compared with this run's package before anything signs or attaches it (#8870 review).
yq '.jobs.publish.steps[].run // ""' "${release_wf}" > "${OUT}"
same_at="$(grep -n -F -m1 -- 'same-chart' "${OUT}" | cut -d: -f1)"
decide_at="$(grep -n -F -m1 -- ' decide ' "${OUT}" | cut -d: -f1)"
[ -n "${same_at}" ] || { echo "the publish job never runs same-chart" >&2; exit 1; }
[ -n "${decide_at}" ] || { echo "the publish job never runs decide" >&2; exit 1; }
[ "${same_at}" -lt "${decide_at}" ] || { echo "same-chart runs after decide" >&2; exit 1; }

t release_workflow_jobs_install_what_the_script_needs
# The script refuses to run without helm and yq; every job that calls it installs the pinned ones.
for job in $(yq '.jobs | keys | .[]' "${release_wf}"); do
  runs="$(yq ".jobs.${job}.steps[].run // \"\"" "${release_wf}")"
  grep -q -F 'darling-helm-release.sh' <<<"${runs}" || continue
  grep -q -F 'get.helm.sh' <<<"${runs}" || { echo "job ${job} runs the script but does not install helm" >&2; exit 1; }
  grep -q -F 'mikefarah/yq' <<<"${runs}" || { echo "job ${job} runs the script but does not install yq" >&2; exit 1; }
done
