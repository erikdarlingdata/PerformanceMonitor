#!/usr/bin/env bash
# #8017: the chart installed on a real cluster (kind), one namespace per case:
#   bundled_defaults            the bundled store, MCP on: Ready; /api/ping through a port-forward with the web token; /readyz;
#                               credentials/ is 700 and owned by 1654; the password key survives a pod restart.
#   external_plain_postgres     a plain postgres:17 whose owner is not a superuser: Ready, with the owner-fallback warning and
#                               no other store-login warning.
#   password_key_secret         passwordKey from a Secret: the key's fingerprint, no passwordKey: warning, the same after a restart.
#   degraded_at_start           the external store down at install: not Ready and never restarted for 90 s, Ready once it is up.
#   stopped_refused_config      a darling.json the service refuses: Running, not Ready, never restarted.
#   stopped_root_owned_volume   an existingClaim whose credentials/ a root-run release wrote (#8141 D1): Running, not Ready,
#                               never restarted, and the log says what to chown.
#
# Usage: kind-tests.sh <image>   (repository:tag of the Darling image already loaded into the cluster)
# Every kubectl and helm call names the context kind-darling (override with KIND_CONTEXT) and never uses another one.
# On a failure it prints describe, logs and events of the case's namespace; it runs every case and exits 1 if any failed.
set -euo pipefail

image="${1:?usage: kind-tests.sh <image>}"
repository="${image%:*}"
tag="${image##*:}"
if [ "${repository}" = "${image}" ] || [ -z "${tag}" ]; then
  echo "kind-tests: the image must be repository:tag, got ${image}" >&2
  exit 64
fi

CONTEXT="${KIND_CONTEXT:-kind-darling}"
CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE="${CHART_DIR}/tests/fixtures/plain-postgres.yaml"
RELEASE=pm
FULL="${RELEASE}-performancemonitor-darling"
SELECTOR="app.kubernetes.io/name=performancemonitor-darling,app.kubernetes.io/instance=${RELEASE}"
FP_RE='[0-9A-F]{4}(-[0-9A-F]{4}){3}'
# Longer than the liveness probe's periodSeconds x failureThreshold (20 x 3), so a liveness kill would have happened.
STABLE_SECONDS=70
WORK="$(mktemp -d)"
NS=""
PF_PID=""

k() { kubectl --context "${CONTEXT}" "$@"; }
# kubectl exec with the path arguments left alone (Git Bash rewrites "/x" arguments of native programs).
kx() { MSYS_NO_PATHCONV=1 kubectl --context "${CONTEXT}" -n "${NS}" exec "$@"; }
h() { helm --kube-context "${CONTEXT}" "$@"; }

if ! kubectl config get-contexts -o name | grep -qx "${CONTEXT}"; then
  echo "kind-tests: no kube context named ${CONTEXT}" >&2
  exit 64
fi

cleanup() {
  local rc=$?
  stop_port_forward
  rm -rf "${WORK}"
  exit "${rc}"
}
trap cleanup EXIT

fail() {
  echo "FAIL detail: $*" >&2
  return 1
}

stop_port_forward() {
  if [ -n "${PF_PID}" ]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
    wait "${PF_PID}" 2>/dev/null || true
    PF_PID=""
  fi
}

dump() {
  echo "::group::diagnostics of namespace ${NS}"
  k -n "${NS}" get pods -o wide || true
  k -n "${NS}" describe pod || true
  local p
  for p in $(k -n "${NS}" get pods -o name 2>/dev/null); do
    echo "--- logs of ${p} (all containers)"
    k -n "${NS}" logs "${p}" --all-containers --prefix --tail=300 || true
    echo "--- previous logs of ${p}"
    k -n "${NS}" logs "${p}" --all-containers --prefix --previous --tail=100 2>/dev/null || true
  done
  k -n "${NS}" get events --sort-by=.lastTimestamp || true
  echo "::endgroup::"
}

install() {
  h install "${RELEASE}" "${CHART_DIR}" -n "${NS}" \
    --set "image.repository=${repository},image.tag=${tag},image.pullPolicy=Never" "$@" >/dev/null
}

apply_fixture() {
  k -n "${NS}" apply -f "${FIXTURE}" >/dev/null
}

external_args() {
  printf '%s\n' --set store.mode=external --set store.external.host=plain-postgres --set store.external.database=darling \
    --set store.external.username=darling --set store.external.passwordSecret.name=plain-postgres \
    --set store.external.passwordSecret.key=password
}

# The name of the Darling pod (the newest one when a replacement is starting).
pod() {
  k -n "${NS}" get pods -l "${SELECTOR}" --sort-by=.metadata.creationTimestamp -o name | tail -n 1 | sed 's|^pod/||'
}

wait_for_pod() { # wait_for_pod [<name to skip>]: waits for a Darling pod other than the one named, prints its name
  local skip="${1:-}" p
  for _ in $(seq 1 120); do
    p="$(pod)"
    if [ -n "${p}" ] && [ "${p}" != "${skip}" ]; then
      echo "${p}"
      return 0
    fi
    sleep 2
  done
  fail "no Darling pod appeared"
}

wait_ready() { # wait_ready <pod> [timeout]
  k -n "${NS}" wait --for=condition=Ready "pod/$1" --timeout="${2:-600s}" >/dev/null || fail "pod $1 did not become Ready"
}

container_field() { # container_field <pod> <jsonpath field of the darling container status>
  k -n "${NS}" get pod "$1" -o jsonpath="{.status.containerStatuses[?(@.name==\"darling\")].$2}"
}

ready_of() {
  k -n "${NS}" get pod "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
}

wait_started() { # wait_started <pod>: the container runs and its startup probe passed
  local p="$1"
  for _ in $(seq 1 150); do
    if [ "$(container_field "${p}" started)" = "true" ]; then
      return 0
    fi
    sleep 2
  done
  fail "the darling container of ${p} never started"
}

wait_log() { # wait_log <pod> <fixed text> [seconds]
  local p="$1" text="$2" limit="${3:-240}" i=0
  while [ "${i}" -lt "${limit}" ]; do
    if k -n "${NS}" logs "${p}" -c darling 2>/dev/null | grep -qF -- "${text}"; then
      return 0
    fi
    sleep 5
    i=$((i + 5))
  done
  fail "${p}: the log never contained: ${text}"
}

# not_ready_never_restarted <pod> <seconds>: not Ready and restartCount 0 at every check for that long.
not_ready_never_restarted() {
  local p="$1" seconds="$2" i=0 ready restarts
  while :; do
    ready="$(ready_of "${p}")"
    restarts="$(container_field "${p}" restartCount)"
    if [ "${ready}" = "True" ]; then
      fail "${p} became Ready after ${i} s"
    fi
    if [ "${restarts}" != "0" ]; then
      fail "${p} restarted (restartCount ${restarts}) after ${i} s"
    fi
    if [ "${i}" -ge "${seconds}" ]; then
      break
    fi
    sleep 10
    i=$((i + 10))
  done
  echo "  ${p}: phase $(k -n "${NS}" get pod "${p}" -o jsonpath='{.status.phase}'), Ready ${ready}, restartCount ${restarts} after ${seconds} s"
}

fingerprint() { # fingerprint <log file>: the key id of the 'Password key <id> generated|loaded|published' line
  grep -oE "Password key ${FP_RE} (generated in|loaded from|published from)" "$1" | head -n 1 | grep -oE "${FP_RE}" || true
}

restart_pod() { # restart_pod <pod>: deletes it, prints the replacement's name once it is Ready
  local old="$1" new
  k -n "${NS}" delete pod "${old}" --wait=true >/dev/null
  new="$(wait_for_pod "${old}")"
  wait_ready "${new}"
  echo "${new}"
}

case_bundled_defaults() {
  install --set mcp.enabled=true
  local p log1 log2 fp1 fp2 token code body perm init_exit store_uid
  k -n "${NS}" rollout status "statefulset/${FULL}-store" --timeout=600s >/dev/null || fail "the store StatefulSet did not become ready"
  p="$(wait_for_pod)"
  wait_ready "${p}"

  init_exit="$(k -n "${NS}" get pod "${p}" -o jsonpath='{.status.initContainerStatuses[?(@.name=="render-config")].state.terminated.exitCode}')"
  echo "  render-config init container exit code: ${init_exit}"
  [ "${init_exit}" = "0" ] || fail "render-config exited ${init_exit}"
  store_uid="$(MSYS_NO_PATHCONV=1 k -n "${NS}" exec "${FULL}-store-0" -c store -- id -u)"
  echo "  store container runs as UID ${store_uid}"
  [ "${store_uid}" = "70" ] || fail "the store runs as ${store_uid}, not 70"

  token="$(k -n "${NS}" get secret "${FULL}" -o go-template='{{index .data "web-token"}}' | base64 -d)"
  [ -n "${token}" ] || fail "the chart Secret has no web-token"
  k -n "${NS}" port-forward "pod/${p}" 18153:5153 18154:5154 >"${WORK}/pf.log" 2>&1 &
  PF_PID=$!
  for _ in $(seq 1 30); do
    if curl -s -o /dev/null "http://127.0.0.1:18154/livez"; then
      break
    fi
    sleep 1
  done

  # The dashboard takes no bearer header: a script signs in the way the login form does (docs/darling/network-endpoints.md),
  # from the dashboard's own origin, and presents the session cookie. The port-forward arrives on the pod's loopback.
  code="$(curl -s -o /dev/null -w '%{http_code}' -c "${WORK}/jar" -H "Origin: http://127.0.0.1:18153" \
    --data-urlencode "token=${token}" http://127.0.0.1:18153/auth/login)"
  echo "  POST /auth/login: ${code}"
  [ "${code}" = "302" ] || fail "the sign-in answered ${code}, not 302"
  code="$(curl -s -o "${WORK}/ping" -w '%{http_code}' -b "${WORK}/jar" http://127.0.0.1:18153/api/ping)"
  body="$(cat "${WORK}/ping")"
  echo "  GET /api/ping: ${code} ${body}"
  [ "${code}" = "200" ] || fail "/api/ping answered ${code}"
  grep -qF '"status":"ok"' <<<"${body}" || fail "/api/ping body has no \"status\":\"ok\""

  code="$(curl -s -o "${WORK}/readyz" -w '%{http_code}' http://127.0.0.1:18154/readyz)"
  body="$(cat "${WORK}/readyz")"
  echo "  GET /readyz: ${code} ${body}"
  [ "${code}" = "200" ] && [ "${body}" = "ok" ] || fail "/readyz answered ${code} ${body}"
  stop_port_forward

  perm="$(kx "${p}" -c darling -- stat -c '%a %u' /var/lib/darling/credentials)"
  echo "  stat /var/lib/darling/credentials: ${perm}"
  [ "${perm}" = "700 1654" ] || fail "credentials/ is '${perm}', not '700 1654'"

  k -n "${NS}" logs "${p}" -c darling >"${WORK}/log1"
  fp1="$(fingerprint "${WORK}/log1")"
  grep -E "Password key ${FP_RE}" "${WORK}/log1" | sed 's/^/  start 1: /' || true
  [ -n "${fp1}" ] || fail "the first start logged no Password key line"

  p="$(restart_pod "${p}")"
  k -n "${NS}" logs "${p}" -c darling >"${WORK}/log2"
  fp2="$(fingerprint "${WORK}/log2")"
  grep -E "Password key ${FP_RE}" "${WORK}/log2" | sed 's/^/  start 2: /' || true
  [ "${fp1}" = "${fp2}" ] || fail "the password key changed across the restart: ${fp1} then ${fp2:-<none>}"

  if grep -hF "open to other users" "${WORK}/log1" "${WORK}/log2"; then
    fail "a start logged 'open to other users'"
  fi
}

case_external_plain_postgres() {
  apply_fixture
  k -n "${NS}" rollout status deploy/plain-postgres --timeout=300s >/dev/null || fail "the fixture store did not become ready"
  local args p log owner other
  mapfile -t args < <(external_args)
  install "${args[@]}"
  p="$(wait_for_pod)"
  wait_ready "${p}"
  log="${WORK}/log"
  k -n "${NS}" logs "${p}" -c darling >"${log}"
  owner="$(grep -cF "connects to the store as the owner login" "${log}" || true)"
  other="$(grep -F "connects to the store as" "${log}" | grep -vcF "connects to the store as the owner login" || true)"
  grep -F "connects to the store as the owner login" "${log}" | cut -c1-200 | sed 's/^/  /'
  echo "  owner-fallback warnings: ${owner}; other store-login warnings: ${other}"
  [ "${owner}" -ge 1 ] || fail "no owner-fallback warning"
  [ "${other}" = "0" ] || fail "another store-login warning: $(grep -F "connects to the store as" "${log}" | grep -vF "as the owner login")"
}

case_password_key_secret() {
  # The key is made as the image's user, inside the image, and written out on stdout: no bind mount, so it works the same on
  # a Linux runner and under Docker Desktop.
  MSYS_NO_PATHCONV=1 docker run --rm --user 1654 --entrypoint sh "${image}" -c \
    'dotnet PerformanceMonitor.Darling.Service.dll --new-password-key /tmp/password-key >&2 && cat /tmp/password-key' \
    >"${WORK}/password-key" 2>"${WORK}/new-key.out" || { cat "${WORK}/new-key.out" >&2; fail "--new-password-key failed"; }
  sed 's/^/  --new-password-key: /' "${WORK}/new-key.out"
  local made p fp1 fp2
  made="$(grep -oE "${FP_RE}" "${WORK}/new-key.out" | head -n 1 || true)"
  [ -n "${made}" ] || fail "--new-password-key printed no fingerprint"
  k -n "${NS}" create secret generic pk --from-file=password-key="${WORK}/password-key" >/dev/null

  apply_fixture
  k -n "${NS}" rollout status deploy/plain-postgres --timeout=300s >/dev/null || fail "the fixture store did not become ready"
  local args
  mapfile -t args < <(external_args)
  install "${args[@]}" --set passwordKey.existingSecret=pk
  p="$(wait_for_pod)"
  wait_ready "${p}"
  k -n "${NS}" logs "${p}" -c darling >"${WORK}/log1"
  grep -E "Password key ${FP_RE}" "${WORK}/log1" | sed 's/^/  start 1: /' || true
  fp1="$(fingerprint "${WORK}/log1")"
  [ "${fp1}" = "${made}" ] || fail "the service's key is ${fp1:-<none>}, not the key made: ${made}"
  if grep -F "passwordKey:" "${WORK}/log1"; then
    fail "the log has a passwordKey: line"
  fi

  p="$(restart_pod "${p}")"
  k -n "${NS}" logs "${p}" -c darling >"${WORK}/log2"
  grep -E "Password key ${FP_RE}" "${WORK}/log2" | sed 's/^/  start 2: /' || true
  fp2="$(fingerprint "${WORK}/log2")"
  [ "${fp2}" = "${fp1}" ] || fail "the key after the restart is ${fp2:-<none>}, not ${fp1}"
  if grep -F "passwordKey:" "${WORK}/log2"; then
    fail "the log after the restart has a passwordKey: line"
  fi
}

case_degraded_at_start() {
  apply_fixture
  k -n "${NS}" scale deploy/plain-postgres --replicas=0 >/dev/null
  k -n "${NS}" wait --for=delete pod -l app=plain-postgres --timeout=120s >/dev/null 2>&1 || true
  local args p
  mapfile -t args < <(external_args)
  install "${args[@]}"
  p="$(wait_for_pod)"
  wait_started "${p}"
  not_ready_never_restarted "${p}" 90
  k -n "${NS}" scale deploy/plain-postgres --replicas=1 >/dev/null
  wait_ready "${p}" 600s
  echo "  ${p}: Ready after the store came up, restartCount $(container_field "${p}" restartCount)"
  [ "$(container_field "${p}" restartCount)" = "0" ] || fail "${p} restarted"
}

case_stopped_refused_config() {
  apply_fixture
  k -n "${NS}" rollout status deploy/plain-postgres --timeout=300s >/dev/null || fail "the fixture store did not become ready"
  local args p
  mapfile -t args < <(external_args)
  # servers[0].host is empty: the service refuses the file at validation.
  install "${args[@]}" --set-json 'config={"servers":[{"name":"refused","host":""}]}'
  p="$(wait_for_pod)"
  wait_started "${p}"
  for _ in $(seq 1 60); do
    if kx "${p}" -c darling -- test -f /tmp/darling-stopped 2>/dev/null; then
      break
    fi
    sleep 2
  done
  kx "${p}" -c darling -- test -f /tmp/darling-stopped || fail "no stopped marker at /tmp/darling-stopped"
  echo "  stopped marker: $(kx "${p}" -c darling -- head -c 300 /tmp/darling-stopped | tr '\n' ' ')"
  not_ready_never_restarted "${p}" "${STABLE_SECONDS}"
}

case_stopped_root_owned_volume() {
  apply_fixture
  k -n "${NS}" rollout status deploy/plain-postgres --timeout=300s >/dev/null || fail "the fixture store did not become ready"
  # Test fixture only, never a chart template: a claim whose credentials/ a release that ran the image as root wrote.
  k -n "${NS}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: root-owned-state
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: batch/v1
kind: Job
metadata:
  name: seed-root-owned-state
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsUser: 0
        runAsGroup: 0
      containers:
        - name: seed
          image: ${image}
          imagePullPolicy: Never
          command:
            - sh
            - -c
            - mkdir -p /state/credentials && head -c 32 /dev/urandom > /state/credentials/password-key && chown -R 0:0 /state/credentials && chmod 700 /state/credentials && chmod 600 /state/credentials/password-key && ls -ln /state /state/credentials
          volumeMounts:
            - name: state
              mountPath: /state
      volumes:
        - name: state
          persistentVolumeClaim:
            claimName: root-owned-state
YAML
  k -n "${NS}" wait --for=condition=complete job/seed-root-owned-state --timeout=300s >/dev/null || fail "the seeding Job did not complete"
  k -n "${NS}" logs job/seed-root-owned-state | sed 's/^/  seed: /'
  local args p
  mapfile -t args < <(external_args)
  install "${args[@]}" --set persistence.existingClaim=root-owned-state
  p="$(wait_for_pod)"
  wait_started "${p}"
  wait_log "${p}" "chown"
  k -n "${NS}" logs "${p}" -c darling | grep -F "chown" | cut -c1-400 | sed 's/^/  /'
  not_ready_never_restarted "${p}" "${STABLE_SECONDS}"
}

cases=(
  bundled_defaults
  external_plain_postgres
  password_key_secret
  degraded_at_start
  stopped_refused_config
  stopped_root_owned_volume
)
if [ -n "${KIND_CASES:-}" ]; then
  read -r -a cases <<<"${KIND_CASES}"
fi

results=()
failed=0
for c in "${cases[@]}"; do
  NS="kind-${c//_/-}"
  echo "=== ${c} (namespace ${NS})"
  k create namespace "${NS}" >/dev/null
  set +e
  ( set -euo pipefail; trap stop_port_forward EXIT; "case_${c}" )
  rc=$?
  set -e
  stop_port_forward
  if [ "${rc}" = 0 ]; then
    echo "PASS ${c}"
    results+=("PASS ${c}")
  else
    echo "FAIL ${c}"
    results+=("FAIL ${c}")
    failed=1
    dump
  fi
  h uninstall "${RELEASE}" -n "${NS}" >/dev/null 2>&1 || true
  k delete namespace "${NS}" --wait=false >/dev/null 2>&1 || true
done

echo "=== summary"
printf '%s\n' "${results[@]}"
exit "${failed}"
