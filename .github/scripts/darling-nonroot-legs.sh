#!/usr/bin/env bash
# #8141: the Darling image runs as UID 1654. Three legs, each on its own compose-shaped store (the store's bootstrap superuser
# is POSTGRES_USER, as Darling/compose/docker-compose.yml has it, so the service provisions its least-privilege roles):
#   fresh        a new volume: the service starts as 1654 and owns a 700 credentials directory with every key file.
#   upgrade      a volume the previous (root) release wrote, handed over with the documented chown: same password key, and no
#                role credential the volume already held, nor the log-hash key, is generated again.
#   missed-step  the same volume WITHOUT the chown: the service stops, says what to run, and regenerates nothing; the web
#                dashboard (turned on for this leg) stays down instead of connecting as the store owner.
# Usage: darling-nonroot-legs.sh <image> <previous-image>
set -euo pipefail

image="${1:?usage: darling-nonroot-legs.sh <image> <previous-image>}"
previous="${2:?usage: darling-nonroot-legs.sh <image> <previous-image>}"
store_image="${STORE_IMAGE:-timescale/timescaledb:latest-pg17}"
prefix="nonroot-$$"
work="$(mktemp -d)"
leg="setup"
fp_re='[0-9A-F]{4}(-[0-9A-F]{4}){3}'

fail() {
  echo "::error::${leg}: $*"
  show_logs
  exit 1
}

show_logs() {
  local c
  for c in $(docker ps -a --filter "name=${prefix}" --format '{{.Names}}'); do
    echo "::group::${c} log"
    docker logs "${c}" 2>&1 | tail -n 60 || true
    echo "::endgroup::"
  done
}

cleanup() {
  local rc=$?
  docker ps -aq --filter "name=${prefix}" | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker volume ls -q --filter "name=${prefix}" | xargs -r docker volume rm -f >/dev/null 2>&1 || true
  docker network ls -q --filter "name=${prefix}" | xargs -r docker network rm >/dev/null 2>&1 || true
  rm -rf "${work}"
  exit "${rc}"
}
trap cleanup EXIT

host_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -m "$1"
  else
    echo "$1"
  fi
}

# new_store <leg>: a fresh network, volume and store; sets net, vol, store, DARLING_STORE_CONNECTION, config_file.
new_store() {
  local name="$1" password ready=0
  net="${prefix}-${name}-net"
  vol="${prefix}-${name}-vol"
  store="${prefix}-${name}-store"
  password="$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  export DARLING_STORE_CONNECTION="Host=${store};Port=5432;Username=darling;Password=${password};Database=darling"
  docker network create "${net}" >/dev/null
  docker volume create "${vol}" >/dev/null
  docker run -d --name "${store}" --network "${net}" \
    -e POSTGRES_USER=darling -e POSTGRES_DB=darling -e POSTGRES_PASSWORD="${password}" \
    "${store_image}" \
    postgres -c shared_preload_libraries=timescaledb,pg_stat_statements -c pg_stat_statements.track_utility=off \
      -c timescaledb.max_background_workers=74 -c max_worker_processes=85 -c work_mem=16MB \
      -c timescaledb.telemetry_level=off >/dev/null
  for _ in $(seq 1 60); do
    slog="$(docker logs "${store}" 2>&1)" || true
    if grep -q 'PostgreSQL init process complete' <<<"${slog}" \
      && docker exec "${store}" pg_isready -h 127.0.0.1 -U darling >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 2
  done
  [ "${ready}" = 1 ] || fail "the store did not become ready in 120 seconds"
  mkdir -p "${work}/${name}"
  config_file="${work}/${name}/darling.json"
  cat > "${config_file}" <<'JSON'
{
  "postgres": { "managed": false, "connectionString": "env:DARLING_STORE_CONNECTION" },
  "servers": [],
  "web": { "enabled": false },
  "mcp": { "enabled": false }
}
JSON
  chmod 644 "${config_file}"
}

# start_service <container> <image> [extra docker args...]: the service on the leg's store, volume at /var/lib/darling.
start_service() {
  local c="$1" img="$2"
  shift 2
  docker run -d --name "${c}" --network "${net}" \
    -e DARLING_STORE_CONNECTION -e DARLING_STOPPED_MARKER=/tmp/darling-stopped \
    -v "$(host_path "${config_file}"):/config/darling.json:ro" \
    -v "${vol}:/var/lib/darling" \
    "$@" "${img}" >/dev/null || fail "could not start ${c} from ${img}"
}

# wait_log <container> <extended regex>: the log-grep loop, capped at 180 s; fails when the container exits first.
wait_log() {
  local c="$1" re="$2" clog
  for _ in $(seq 1 90); do
    clog="$(docker logs "${c}" 2>&1)" || true
    if grep -Eq -- "${re}" <<<"${clog}"; then
      return 0
    fi
    if [ "$(docker inspect -f '{{.State.Running}}' "${c}")" != "true" ]; then
      fail "${c} stopped before logging: ${re}"
    fi
    sleep 2
  done
  fail "${c} did not log '${re}' in 180 seconds"
}

# fingerprint <container>: the password key fingerprint of the container's log.
fingerprint() {
  local flog
  flog="$(docker logs "$1" 2>&1)" || true
  grep 'Password key' <<<"${flog}" | grep -Eo "${fp_re}" | head -n 1 || true
}

log_of() { docker logs "$1" 2>&1; }

# ---- fresh ----
leg="fresh"
new_store fresh
start_service "${prefix}-fresh-svc" "${image}"
wait_log "${prefix}-fresh-svc" 'Least-privilege roles ready'
wait_log "${prefix}-fresh-svc" 'Retention evaluation at startup'
wait_log "${prefix}-fresh-svc" 'Password key [0-9A-F]{4}-[0-9A-F]{4}'
wait_log "${prefix}-fresh-svc" "Generated the store's log-hash key"
uid="$(docker exec "${prefix}-fresh-svc" id -u)" || fail "could not run id -u in the service container"
[ "${uid}" = 1654 ] || fail "the service runs as uid ${uid}, expected 1654"
mode="$(docker exec "${prefix}-fresh-svc" stat -c '%u %a' /var/lib/darling/credentials)" || fail "could not stat the credentials directory"
[ "${mode}" = "1654 700" ] || fail "credentials directory is '${mode}', expected '1654 700'"
files="$(docker exec "${prefix}-fresh-svc" ls /var/lib/darling/credentials)" || fail "could not list the credentials directory"
for f in password-key log-hash-key pg-admin-credential pg-viewer-credential pg-mcp-credential pg-web-credential; do
  grep -qx -- "${f}" <<<"${files}" || fail "credentials directory has no ${f} (has: $(echo "${files}" | tr '\n' ' '))"
done
echo "ok: fresh"
docker rm -f "${prefix}-fresh-svc" "${store}" >/dev/null
docker volume rm -f "${vol}" >/dev/null
docker network rm "${net}" >/dev/null

# ---- upgrade ----
leg="upgrade"
new_store upgrade
start_service "${prefix}-upgrade-old" "${previous}"
wait_log "${prefix}-upgrade-old" 'Retention evaluation at startup'
wait_log "${prefix}-upgrade-old" 'Password key [0-9A-F]{4}-[0-9A-F]{4}'
wait_log "${prefix}-upgrade-old" "Generated the store's log-hash key"
old_fp="$(fingerprint "${prefix}-upgrade-old")"
[ -n "${old_fp}" ] || fail "the previous image logged no password key fingerprint"
docker stop "${prefix}-upgrade-old" >/dev/null
before="$(docker run --rm --user 0 --entrypoint ls -v "${vol}:/var/lib/darling" "${image}" /var/lib/darling/credentials)" \
  || fail "could not list the previous image's credentials volume"
held="$(grep -E '^pg-.*-credential$' <<<"${before}" || true)"
[ -n "${held}" ] || fail "the previous image left no pg-*-credential file in the volume"
grep -qx 'log-hash-key' <<<"${before}" || fail "the previous image left no log-hash-key in the volume"
docker run --rm --user 0 --entrypoint chown -v "${vol}:/var/lib/darling" "${image}" -R 1654:1654 /var/lib/darling >/dev/null \
  || fail "the documented chown failed"
start_service "${prefix}-upgrade-new" "${image}"
wait_log "${prefix}-upgrade-new" 'Least-privilege roles ready'
wait_log "${prefix}-upgrade-new" 'Retention evaluation at startup'
wait_log "${prefix}-upgrade-new" 'Password key [0-9A-F]{4}-[0-9A-F]{4}'
new_fp="$(fingerprint "${prefix}-upgrade-new")"
[ "${new_fp}" = "${old_fp}" ] || fail "password key fingerprint changed: '${old_fp}' -> '${new_fp}'"
unew_log="$(log_of "${prefix}-upgrade-new")"
generated="$(grep -F "Generated the compose store's" <<<"${unew_log}" || true)"
for f in ${held}; do
  if grep -qF "/${f})" <<<"${generated}"; then
    fail "the upgrade generated ${f} again"
  fi
done
wait_log "${prefix}-upgrade-new" "Loaded the store's log-hash key"
unew_log="$(log_of "${prefix}-upgrade-new")"
if grep -qF "Generated the store's log-hash key" <<<"${unew_log}"; then
  fail "the upgrade generated the log-hash key again"
fi
echo "ok: upgrade (fingerprint ${new_fp}, kept: $(echo ${held}))"
docker rm -f "${prefix}-upgrade-old" "${prefix}-upgrade-new" "${store}" >/dev/null
docker volume rm -f "${vol}" >/dev/null
docker network rm "${net}" >/dev/null

# ---- missed-step ----
leg="missed-step"
new_store missed
start_service "${prefix}-missed-old" "${previous}"
wait_log "${prefix}-missed-old" 'Retention evaluation at startup'
docker stop "${prefix}-missed-old" >/dev/null || fail "could not stop ${prefix}-missed-old"
# The web dashboard on, with a token from a file in the leg's temp dir: its host must stay down, not connect as the owner.
token_file="${work}/missed/web_token"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "${token_file}"
chmod 644 "${token_file}"
cat > "${config_file}" <<'JSON'
{
  "postgres": { "managed": false, "connectionString": "env:DARLING_STORE_CONNECTION" },
  "servers": [],
  "web": { "enabled": true, "network": { "token": "file:/run/secrets/web_token" } },
  "mcp": { "enabled": false }
}
JSON
start_service "${prefix}-missed-new" "${image}" -v "$(host_path "${token_file}"):/run/secrets/web_token:ro"
wait_log "${prefix}-missed-new" 'belongs to user 0, but the service runs as user 1654'
log="$(log_of "${prefix}-missed-new")"
grep -qF 'chown darling -R 1654:1654 /var/lib/darling' <<<"${log}" || fail "the log does not name the chown command"
stopped=0
for _ in $(seq 1 30); do
  if docker exec "${prefix}-missed-new" test -e /tmp/darling-stopped 2>/dev/null; then
    stopped=1
    break
  fi
  sleep 2
done
[ "${stopped}" = 1 ] || fail "/tmp/darling-stopped did not appear within 60 seconds"
[ "$(docker inspect -f '{{.State.Running}}' "${prefix}-missed-new")" = "true" ] || fail "the container exited"
log="$(log_of "${prefix}-missed-new")"
if grep -qF -- '--reset-password-key' <<<"${log}"; then fail "the log suggests --reset-password-key"; fi
if grep -qF "Generated the compose store's" <<<"${log}"; then fail "the service generated a credential on a volume it cannot write"; fi
wait_log "${prefix}-missed-new" 'web dashboard not started this attempt: The credentials directory /var/lib/darling/credentials belongs to user 0'
log="$(log_of "${prefix}-missed-new")"
if grep -qF 'connects to the store as the owner' <<<"${log}"; then fail "the web dashboard connected as the store owner"; fi
echo "ok: missed-step"
