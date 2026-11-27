#!/usr/bin/env bash
# #9686: the linux-x64 tarball installed under systemd the way docs/darling/linux.md says, step by step, on a machine that
# has systemd (ubuntu-latest, or a lab machine). Every other Linux check runs the container image, so the tarball, the
# `darling` user, the folders of Step 5 and the unit of Step 8 are run nowhere else (#9678 was a log folder those steps never
# created). The leg follows the page's numbered steps by hand. The tarball's install-darling.sh and upgrade-darling.sh (#9688)
# do steps 5 to 8 and the upgrade themselves, and this leg does not run them.
#
# What differs from the page, and why:
#   Step 2  the page installs the .NET runtime with apt. The leg uses the dotnet already on the machine, and names its path
#           in the unit when that is not /usr/bin/dotnet, as Step 2 says to.
#   Step 3  the page installs PostgreSQL 18 and TimescaleDB with apt. The leg starts a TimescaleDB container published on
#           127.0.0.1 only, and runs the page's createuser, createdb and superuser statements inside it.
#   Step 4  there is no release to download: the tarball is the one the job packed, the way nightly.yml's linux job packs it.
#   Step 6  "servers" is empty (a runner reaches no SQL Server), and the health port is on so the leg has a port to probe.
#   sudo    the leg runs as root, so the page's `sudo` is dropped, and `sudo -u postgres` is `docker exec ... -U postgres`.
# A step that does not work as the page writes it gets the smallest workaround here, with a comment that says so.
#
# It installs and then removes a host-level Darling (the `darling` user, /opt/darling, /etc/darling,
# /usr/share/PerformanceMonitorDarling, darling.service), so it refuses to start on a machine that already has any of them.
#
# Usage: sudo bash darling-linux-systemd-leg.sh <PerformanceMonitorDarling-linux-x64 tarball>
# Environment: STORE_IMAGE, DARLING_SYSTEMD_STORE_NAME (darling-systemd-store), DARLING_SYSTEMD_STORE_PORT (55432),
#              DARLING_SYSTEMD_HEALTH_PORT (5154).
set -euo pipefail

usage="usage: darling-linux-systemd-leg.sh <PerformanceMonitorDarling-linux-x64 tarball>"
tarball="${1:?${usage}}"
store_image="${STORE_IMAGE:-timescale/timescaledb:2.32.1-pg17}"
store="${DARLING_SYSTEMD_STORE_NAME:-darling-systemd-store}"
store_port="${DARLING_SYSTEMD_STORE_PORT:-55432}"
health_port="${DARLING_SYSTEMD_HEALTH_PORT:-5154}"

unit_file=/etc/systemd/system/darling.service
key_dir=/etc/darling/darling-keys
log_dir=/usr/share/PerformanceMonitorDarling/logs
step="setup"
installing=0
store_started=0
started_at="$(date -u '+%Y-%m-%d %H:%M:%S')"

pass() { echo "PASS: $*"; }

fail() {
  echo "::error::${step}: $*"
  exit 1
}

show_logs() {
  echo "::group::journalctl -u darling"
  journalctl -u darling --no-pager --since "${started_at}" 2>&1 | tail -n 60 || true
  echo "::endgroup::"
  echo "::group::file log"
  cat "${log_dir}"/darling-service_*.log 2>/dev/null | tail -n 60 || true
  echo "::endgroup::"
  echo "::group::store log"
  docker logs "${store}" 2>&1 | tail -n 30 || true
  echo "::endgroup::"
}

# #9837: what a service that has not said it is ready waits on: its working folder, its inotify watches against the
# machine's limit, the unit's memory, each thread's name, state, kernel wait channel and syscall number, the main
# thread's kernel stack, its sockets, and the managed stacks when DARLING_DOTNET_STACK names a dotnet-stack executable
# (the workflow installs one, a hand run may have none).
show_start_sample() {
  local pid
  pid="$(systemctl show -p MainPID --value darling)"
  echo "::group::darling.service is still starting after $1 s (main pid ${pid})"
  if [ -n "${pid}" ] && [ "${pid}" != 0 ] && [ -d "/proc/${pid}" ]; then
    echo "working folder: $(readlink "/proc/${pid}/cwd" 2>/dev/null)"
    echo "inotify watches: $(cat /proc/"${pid}"/fdinfo/* 2>/dev/null | grep -c '^inotify' || true) (max_user_watches $(cat /proc/sys/fs/inotify/max_user_watches 2>/dev/null))"
    echo "unit memory: $(systemctl show -p MemoryCurrent --value darling 2>/dev/null) bytes"
    for task in /proc/"${pid}"/task/*; do
      printf '%s | %s | %s | %s\n' "$(cat "${task}/comm" 2>/dev/null)" \
        "$(sed -E 's/^.*\) ([A-Za-z]).*$/\1/' "${task}/stat" 2>/dev/null)" \
        "$(cat "${task}/wchan" 2>/dev/null)" "$(cut -d' ' -f1 "${task}/syscall" 2>/dev/null)"
    done | sort | uniq -c || true
    echo "main thread kernel stack:"
    cat "/proc/${pid}/stack" 2>/dev/null || true
    echo "sockets:"
    ss -tanpu 2>/dev/null | grep "pid=${pid}," || echo "none"
    if [ -n "${DARLING_DOTNET_STACK:-}" ] && [ -x "${DARLING_DOTNET_STACK}" ]; then
      timeout 30 "${DARLING_DOTNET_STACK}" report -p "${pid}" 2>&1 | head -n 300 || true
    fi
  fi
  echo "::endgroup::"
}

# The page's Uninstall section, with the store's dropdb and dropuser replaced by removing the container. It runs on every
# exit, so a failed run leaves the machine as clean as a passed one, and a passed run fails when anything is left.
cleanup() {
  local rc=$? left=""
  trap - EXIT
  if [ "${rc}" != 0 ] && [ "${installing}" = 1 ]; then
    show_logs
  fi
  if [ "${installing}" = 1 ]; then
    systemctl disable --now darling >/dev/null 2>&1 || true
    rm -f "${unit_file}"
    systemctl daemon-reload || true
    systemctl reset-failed darling >/dev/null 2>&1 || true
    rm -rf /opt/darling /opt/darling.previous
    rm -rf /etc/darling /usr/share/PerformanceMonitorDarling
    if id darling >/dev/null 2>&1; then
      userdel darling || true
    fi
  fi
  if [ "${store_started}" = 1 ]; then
    docker rm -f -v "${store}" >/dev/null 2>&1 || true
  fi
  if [ "${rc}" = 0 ]; then
    id darling >/dev/null 2>&1 && left="${left} user:darling"
    getent group darling >/dev/null 2>&1 && left="${left} group:darling"
    for path in /opt/darling /etc/darling /usr/share/PerformanceMonitorDarling "${unit_file}"; do
      [ -e "${path}" ] && left="${left} ${path}"
    done
    [ -n "$(docker ps -aq --filter "name=^${store}\$")" ] && left="${left} container:${store}"
    if [ -n "${left}" ]; then
      echo "::error::uninstall: the page's Uninstall steps left:${left}"
      rc=1
    else
      echo "PASS: uninstall left no user, group, folder, unit or container"
    fi
  fi
  exit "${rc}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

log_lines() { cat "${log_dir}"/darling-service_*.log 2>/dev/null | wc -l || true; }

log_count() { cat "${log_dir}"/darling-service_*.log 2>/dev/null | grep -cF -- "$1" || true; }

# wait_log <text> <count> <seconds>: the file log holds <text> at least <count> times. The service writes its file log from a
# buffer every 5 seconds, so a line is in the file a few seconds after the service logs it.
wait_log() {
  local text="$1" want="$2" seconds="$3"
  for _ in $(seq 1 "$((seconds / 2))"); do
    if [ "$(log_count "${text}")" -ge "${want}" ]; then
      return 0
    fi
    sleep 2
  done
  fail "the file log in ${log_dir} does not hold '${text}' ${want} time(s) after ${seconds} seconds"
}

# wait_health <seconds>: /readyz reaches 200 (the collection loop runs), then /livez answers ok.
wait_health() {
  local seconds="$1" ready="" live=""
  for _ in $(seq 1 "$((seconds / 2))"); do
    ready="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${health_port}/readyz" || true)"
    if [ "${ready}" = "200" ]; then
      break
    fi
    if ! systemctl is-active --quiet darling; then
      fail "the unit is '$(systemctl is-active darling || true)', not active, while waiting for /readyz"
    fi
    sleep 2
  done
  [ "${ready}" = "200" ] || fail "/readyz answered '${ready}', not 200, after ${seconds} seconds"
  live="$(curl -sS "http://127.0.0.1:${health_port}/livez" || true)"
  [ "${live}" = "ok" ] || fail "/livez answered '${live}', not 'ok'"
}

# ---- before anything is installed ----
[ "$(id -u)" = 0 ] || fail "run this as root (sudo bash $0 <tarball>)"
[ -f "${tarball}" ] || fail "no tarball at '${tarball}'. ${usage}"
[ -d /run/systemd/system ] || fail "this machine does not run systemd"
for tool in docker curl openssl runuser useradd userdel systemctl journalctl tar; do
  command -v "${tool}" >/dev/null 2>&1 || fail "this machine has no ${tool}"
done
found=""
id darling >/dev/null 2>&1 && found="${found} user:darling"
for path in /opt/darling /opt/darling.previous /etc/darling /usr/share/PerformanceMonitorDarling "${unit_file}"; do
  [ -e "${path}" ] && found="${found} ${path}"
done
[ -z "${found}" ] || fail "this machine already has a Darling install (${found# }). The leg installs one and removes it again, so it does not run here"
[ -z "$(docker ps -aq --filter "name=^${store}\$")" ] || fail "a container named ${store} already exists"
if port_open "${store_port}"; then fail "port ${store_port} (the store) is already in use"; fi
if port_open "${health_port}"; then fail "port ${health_port} (the health port) is already in use"; fi

# ---- Step 2: the .NET runtime ----
# The page's proof, on the dotnet this machine has: both 10.x runtimes. /usr/bin/dotnet is the page's path and is tried first.
step="step 2"
dotnet_path=""
for candidate in /usr/bin/dotnet "$(command -v dotnet || true)" "${DOTNET_ROOT:+${DOTNET_ROOT}/dotnet}" /usr/share/dotnet/dotnet /usr/lib/dotnet/dotnet; do
  if [ -n "${candidate}" ] && [ -x "${candidate}" ]; then
    runtimes="$("${candidate}" --list-runtimes 2>/dev/null || true)"
    if grep -q '^Microsoft\.AspNetCore\.App 10\.' <<<"${runtimes}" && grep -q '^Microsoft\.NETCore\.App 10\.' <<<"${runtimes}"; then
      dotnet_path="${candidate}"
      break
    fi
  fi
done
[ -n "${dotnet_path}" ] || fail "no dotnet on this machine lists both Microsoft.AspNetCore.App 10. and Microsoft.NETCore.App 10."
if [ "${dotnet_path}" != /usr/bin/dotnet ]; then
  echo "NOTE: dotnet is ${dotnet_path}, not /usr/bin/dotnet, so the unit names that path and the pre-flight finds it on PATH."
  PATH="$(dirname "${dotnet_path}"):${PATH}"
  export PATH
fi
pass "step 2: ${dotnet_path} lists the ASP.NET Core 10 and .NET 10 runtimes"

# ---- Step 3: the store ----
step="step 3"
store_password="$(openssl rand -hex 16)"
admin_password="$(openssl rand -hex 16)"
if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
  echo "::add-mask::${store_password}"
  echo "::add-mask::${admin_password}"
fi

# The page's settings, as far as a container takes them: the worker pair is the one darling-linux-store.yml and the compose
# file use, and listen_addresses stays the image's own, because the service reaches the container through the published port.
POSTGRES_PASSWORD="${admin_password}" docker run -d --name "${store}" -p "127.0.0.1:${store_port}:5432" \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD \
  "${store_image}" \
  postgres -c shared_preload_libraries=timescaledb,pg_stat_statements -c pg_stat_statements.track_utility=off \
    -c timescaledb.max_background_workers=74 -c max_worker_processes=85 -c work_mem=16MB \
    -c timescaledb.telemetry_level=off -c timezone=UTC -c random_page_cost=1.1 >/dev/null
store_started=1

# The image starts a temporary server on the socket only, runs its init scripts, and starts the real one. A pg_isready over
# TCP answers only from the real one, so wait for the init line first and then for TCP.
ready=0
for _ in $(seq 1 60); do
  # grep reads the whole log (no -q), so docker logs never writes to a closed pipe under pipefail (#9671).
  if docker logs "${store}" 2>&1 | grep 'PostgreSQL init process complete' >/dev/null \
    && docker exec "${store}" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
[ "${ready}" = 1 ] || fail "the store did not become ready in 120 seconds"

# "The owner role and the database". createuser asks for the password twice; with no terminal it reads both from stdin, so
# the password is never on a command line.
printf '%s\n%s\n' "${store_password}" "${store_password}" \
  | docker exec -i "${store}" createuser -U postgres --pwprompt darling 2>/dev/null \
  || fail "createuser --pwprompt darling failed"
docker exec "${store}" createdb -U postgres --owner=darling --template=template0 --encoding=UTF8 --locale=C darling \
  || fail "createdb darling failed"

# "Extensions and rights that need a superuser", the page's statements as written.
docker exec -i "${store}" psql -X -U postgres -d darling -v ON_ERROR_STOP=1 -q <<'SQL' || fail "the superuser statements failed"
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public;
GRANT pg_monitor TO darling;
REVOKE SELECT ON public.pg_stat_statements FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.pg_stat_statements(boolean) FROM PUBLIC;
GRANT SELECT ON public.pg_stat_statements TO pg_read_all_stats, darling;
GRANT EXECUTE ON FUNCTION public.pg_stat_statements(boolean) TO pg_read_all_stats, darling;
REVOKE SELECT ON public.pg_stat_statements_info FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.pg_stat_statements_info() FROM PUBLIC;
GRANT SELECT ON public.pg_stat_statements_info TO pg_read_all_stats, darling;
GRANT EXECUTE ON FUNCTION public.pg_stat_statements_info() TO pg_read_all_stats, darling;
SQL

extensions="$(docker exec "${store}" psql -X -U postgres -d darling -At -c "select extname from pg_extension")" \
  || fail "could not list the extensions"
preload="$(docker exec "${store}" psql -X -U postgres -d darling -At -c "show shared_preload_libraries")" \
  || fail "could not read shared_preload_libraries"
for name in timescaledb pg_stat_statements; do
  grep -qx -- "${name}" <<<"${extensions}" || fail "the darling database has no ${name} extension"
  grep -q -- "${name}" <<<"${preload}" || fail "shared_preload_libraries is '${preload}', without ${name}"
done
pass "step 3: the store has the darling role and database, both extensions and both preload libraries"

# ---- Step 5: extract, and create the user and folders ----
step="step 5"
installing=1
mkdir -p /opt/darling
tar -xzf "${tarball}" -C /opt/darling --no-same-owner --no-same-permissions

useradd --system --no-create-home --shell /usr/sbin/nologin darling
mkdir -p /etc/darling/darling-keys
mkdir -p /usr/share/PerformanceMonitorDarling/logs
chown root:darling /etc/darling
chmod 750 /etc/darling
chown darling:darling /etc/darling/darling-keys
chmod 700 /etc/darling/darling-keys
chown darling:darling /usr/share/PerformanceMonitorDarling/logs
chmod 700 /usr/share/PerformanceMonitorDarling/logs

version="$(/opt/darling/PerformanceMonitor.Darling.Service --version)" || fail "/opt/darling/PerformanceMonitor.Darling.Service --version failed"
[ -n "${version}" ] || fail "/opt/darling/PerformanceMonitor.Darling.Service --version printed nothing"
not_root="$(find /opt/darling ! -user root | sed -n '1,3p')"
[ -z "${not_root}" ] || fail "files under /opt/darling are not owned by root: $(echo ${not_root})"
pass "step 5: the apphost prints version ${version}, and root owns /opt/darling"

# ---- Step 6: darling.json and the secret files ----
step="step 6"
install -o darling -g darling -m 600 /dev/null /etc/darling/darling.json
install -o darling -g darling -m 600 /dev/null /etc/darling/store_connection
install -o darling -g darling -m 600 /dev/null /etc/darling/sql_password

cat > /etc/darling/darling.json <<JSON
{
  "postgres": {
    "managed": false,
    "connectionString": "file:/etc/darling/store_connection"
  },
  "servers": [],
  "web": { "enabled": false },
  "mcp": { "enabled": false },
  "health": { "enabled": true, "port": ${health_port} }
}
JSON
printf 'Host=127.0.0.1;Port=%s;Username=darling;Password=%s;Database=darling\n' "${store_port}" "${store_password}" \
  > /etc/darling/store_connection

for name in darling.json store_connection sql_password; do
  state="$(stat -c '%A %U' "/etc/darling/${name}")"
  [ "${state}" = "-rw------- darling" ] || fail "/etc/darling/${name} is '${state}', expected '-rw------- darling'"
done
pass "step 6: the three files in /etc/darling are -rw------- and owned by darling"

# ---- Step 7: pre-flight ----
step="step 7"
code=0
preflight="$(runuser -u darling -- env DARLING_CONFIG=/etc/darling/darling.json dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll --test-connection 2>&1)" || code=$?
echo "${preflight}" | tail -n 8
[ "${code}" = 0 ] || fail "--test-connection exited ${code}, not 0"
# With no servers the verb has no [PASS] line to print: it says that there is nothing to test, and exits 0.
grep -qF 'No monitored servers are configured yet' <<<"${preflight}" \
  || fail "--test-connection did not say that no monitored servers are configured"
pass "step 7: the pre-flight exits 0 as the darling user"

# ---- Step 8: the unit ----
step="step 8"
cat > "${unit_file}" <<UNIT
[Unit]
Description=PerformanceMonitor Darling
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=notify
ExecStart=${dotnet_path} /opt/darling/PerformanceMonitor.Darling.Service.dll
Environment=DARLING_CONFIG=/etc/darling/darling.json
User=darling
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
# #9837: how long this machine takes to resolve its own name, one of the waits a first start can sit in.
lookup_began="$(date +%s.%N)"
lookup="$(timeout 30 getent hosts "$(hostname)" | head -n 1 || true)"
echo "the machine's own name resolves to '${lookup:-nothing}' in $(awk -v a="${lookup_began}" -v b="$(date +%s.%N)" 'BEGIN { printf "%.2f", b - a }') s"
# The start runs in the background so the leg can look at the process while it starts: a start still running at 20, 50
# and 80 s prints what it waits on (#9837). systemctl's own exit code still decides the step.
start_began=${SECONDS}
systemctl enable --now darling &
starter=$!
next_sample=20
while kill -0 "${starter}" 2>/dev/null; do
  if [ $((SECONDS - start_began)) -ge "${next_sample}" ]; then
    show_start_sample "$((SECONDS - start_began))"
    next_sample=$((next_sample + 30))
  fi
  sleep 1
done
wait "${starter}" || fail "systemctl enable --now darling failed after $((SECONDS - start_began)) s"
start_seconds=$((SECONDS - start_began))
systemctl status darling --no-pager 2>&1 | head -n 6 || true

active="$(systemctl is-active darling || true)"
[ "${active}" = "active" ] || fail "the unit is '${active}', not active"
pid="$(systemctl show -p MainPID --value darling)"
[ -n "${pid}" ] && [ "${pid}" != 0 ] || fail "the unit has no main process"
owner="$(ps -o user= -p "${pid}" | tr -d ' ')"
[ "${owner}" = "darling" ] || fail "the main process (pid ${pid}) runs as '${owner}', not darling"
pass "step 8: darling.service is active after ${start_seconds} s, and its main process runs as darling"

# ---- Step 9: proof that it runs ----
# With no servers there is no Connected line and no collection_log row, so the proof is the health port and the three
# start lines of the file log.
step="step 9"
wait_health 240
pass "step 9: /readyz answers 200 and /livez answers ok on 127.0.0.1:${health_port}"

wait_log 'PerformanceMonitor Darling collection loop started' 1 60
log_file="$(ls "${log_dir}"/darling-service_*.log | sed -n '1p')"
today="$(date -u +%Y%m%d)"
yesterday="$(date -u -d yesterday +%Y%m%d)"
case "$(basename "${log_file}")" in
  "darling-service_${today}.log" | "darling-service_${yesterday}.log") ;;
  *) fail "the file log is ${log_file}, not darling-service_${today}.log" ;;
esac
owner="$(stat -c '%U' "${log_file}")"
[ "${owner}" = "darling" ] || fail "${log_file} is owned by '${owner}', not darling"
state="$(stat -c '%a %U' "${log_dir}")"
[ "${state}" = "700 darling" ] || fail "${log_dir} is '${state}', expected '700 darling'"
for text in 'Loaded configuration from /etc/darling/darling.json: 0 server(s)' 'Postgres store ready (schema v'; do
  [ "$(log_count "${text}")" -ge 1 ] || fail "the file log does not hold '${text}'"
done
pass "step 9: $(basename "${log_file}") is owned by darling and holds this start ($(log_lines) lines)"

# The key folder of Step 5's table: owner-only, and the service makes both of its key files in it on the first start, 600.
step="key folder"
state="$(stat -c '%a %U %G' "${key_dir}")"
[ "${state}" = "700 darling darling" ] || fail "${key_dir} is '${state}', expected '700 darling darling'"
keys="$(find "${key_dir}" -mindepth 1 -printf '%f %m %u %y\n' | sort)"
for name in password-key log-hash-key; do
  grep -q -- "^${name} " <<<"${keys}" || fail "${key_dir} has no ${name} after the first start (has: $(cut -d' ' -f1 <<<"${keys}" | tr '\n' ' '))"
done
wrong="$(grep -v ' 600 darling f$' <<<"${keys}" || true)"
[ -z "${wrong}" ] || fail "${key_dir} holds an entry that is not a 600 file owned by darling: $(echo ${wrong})"
pass "key folder: ${key_dir} is 700, and every file in it is 600 and owned by darling ($(cut -d' ' -f1 <<<"${keys}" | tr '\n' ' '))"

# ---- stop and start ----
step="stop"
systemctl stop darling || fail "systemctl stop darling failed"
active="$(systemctl is-active darling || true)"
[ "${active}" = "inactive" ] || fail "the unit is '${active}' after stop, not inactive"
if port_open "${health_port}"; then fail "port ${health_port} still answers after stop"; fi
pass "stop: the unit is inactive and port ${health_port} is closed"

step="start again"
lines_before="$(log_lines)"
systemctl start darling || fail "systemctl start darling failed"
wait_health 120
wait_log 'PerformanceMonitor Darling collection loop started' 2 60
lines_after="$(log_lines)"
[ "${lines_after}" -gt "${lines_before}" ] || fail "the file log did not grow (${lines_before} lines before the start, ${lines_after} after)"
pass "start again: the health port answers, and the file log grew from ${lines_before} to ${lines_after} lines"
