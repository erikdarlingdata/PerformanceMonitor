#!/bin/sh
# Tests of files/run-provision-roles.sh. POSIX sh, run under sh (dash on the CI runner):
#   sh Darling/helm/performancemonitor-darling/tests/run-provision-roles-tests.sh
# Each case runs the script with a stub psql first on PATH and a temp dir in place of /work.
set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/../files/run-provision-roles.sh"
ROOT="$(mktemp -d)"
STUBS="${ROOT}/bin"
WORK="${ROOT}/work"
OUT="${ROOT}/out.txt"
CURRENT=""
trap 'rm -rf "${ROOT}"' EXIT

begin() {
  if [ -n "${CURRENT}" ]; then
    echo "PASS ${CURRENT}"
  fi
  CURRENT="$1"
  rm -rf "${STUBS}" "${WORK}"
  mkdir -p "${STUBS}" "${WORK}"
  cat > "${WORK}/libpq.env" <<'ENV'
PGHOST='db.example.internal'
PGPORT='5432'
PGAPPNAME='darling-provision-roles'
ENV
}

fail() {
  echo "FAIL ${CURRENT}: $1" >&2
  exit 1
}

# Writes the stub psql from stdin.
stub_psql() {
  cat > "${STUBS}/psql"
  chmod +x "${STUBS}/psql"
}

# Runs the script; sets STATUS and leaves the merged output in ${OUT}.
run_script() {
  STATUS=0
  PATH="${STUBS}:${PATH}" WORK_DIR="${WORK}" sh "${SCRIPT}" > "${OUT}" 2>&1 || STATUS=$?
}

begin exit_code_is_kept
stub_psql <<'STUB'
#!/bin/sh
echo "psql: stopped"
exit 3
STUB
run_script
[ "${STATUS}" = 3 ] || fail "exit status ${STATUS}, expected 3"
[ "$(tail -n 1 "${OUT}")" = "provision-roles.sql exit code: 3" ] || fail "last line is: $(tail -n 1 "${OUT}")"
grep -qF "psql: stopped" "${OUT}" || fail "psql output is missing"

begin exit_code_zero
stub_psql <<'STUB'
#!/bin/sh
exit 0
STUB
run_script
[ "${STATUS}" = 0 ] || fail "exit status ${STATUS}, expected 0"
[ "$(tail -n 1 "${OUT}")" = "provision-roles.sql exit code: 0" ] || fail "last line is: $(tail -n 1 "${OUT}")"

begin verifier_is_masked
stub_psql <<'STUB'
#!/bin/sh
echo "LINE 1: ALTER ROLE web PASSWORD 'SCRAM-SHA-256\$4096:abc\$def:ghi'"
echo "stderr line SCRAM-SHA-256\$4096:zzz\$yyy:xxx here" >&2
exit 3
STUB
run_script
grep -qF 'SCRAM-SHA-256$***' "${OUT}" || fail "no masked verifier in the output"
if grep -qF '4096:abc' "${OUT}"; then fail "the stdout verifier is not masked"; fi
if grep -qF '4096:zzz' "${OUT}"; then fail "the stderr verifier is not masked"; fi
grep -qF "'SCRAM-SHA-256\$***'" "${OUT}" || fail "the closing quote was masked with the verifier"
grep -qF "stderr line SCRAM-SHA-256\$*** here" "${OUT}" || fail "text after the verifier was masked"
[ "${STATUS}" = 3 ] || fail "exit status ${STATUS}, expected 3"

begin flags
stub_psql <<STUB
#!/bin/sh
printf '%s\n' "\$@" > "${ROOT}/argv.txt"
exit 0
STUB
run_script
[ "${STATUS}" = 0 ] || fail "exit status ${STATUS}, expected 0"
printf '%s\n' -X -v ON_ERROR_STOP=1 -c "SET lock_timeout = '120s'" -f "${WORK}/provision-roles.sql" > "${ROOT}/want.txt"
cmp -s "${ROOT}/argv.txt" "${ROOT}/want.txt" || fail "argv is: $(tr '\n' ' ' < "${ROOT}/argv.txt")"

begin env
stub_psql <<STUB
#!/bin/sh
echo "\${PGHOST}|\${PGPASSFILE}|\${PGAPPNAME}|\${PGPASSWORD-unset}" > "${ROOT}/env.txt"
exit 0
STUB
run_script
[ "${STATUS}" = 0 ] || fail "exit status ${STATUS}, expected 0"
want="db.example.internal|${WORK}/pgpass|darling-provision-roles|unset"
[ "$(cat "${ROOT}/env.txt")" = "${want}" ] || fail "env is: $(cat "${ROOT}/env.txt"), expected ${want}"

begin inherited_libpq_variables_are_dropped
stub_psql <<STUB
#!/bin/sh
echo "\${PGPASSWORD-unset}|\${PGSERVICE-unset}|\${PGUSER-unset}|\${PGOPTIONS-unset}|\${PGHOST}|\${PGPASSFILE}" > "${ROOT}/env.txt"
exit 0
STUB
STATUS=0
PGPASSWORD=root PGSERVICE=other PGUSER=postgres PGOPTIONS='-c statement_timeout=1' \
  PATH="${STUBS}:${PATH}" WORK_DIR="${WORK}" sh "${SCRIPT}" > "${OUT}" 2>&1 || STATUS=$?
[ "${STATUS}" = 0 ] || fail "exit status ${STATUS}, expected 0"
want="unset|unset|unset|unset|db.example.internal|${WORK}/pgpass"
[ "$(cat "${ROOT}/env.txt")" = "${want}" ] || fail "env is: $(cat "${ROOT}/env.txt"), expected ${want}"

begin lock_timeout_error_reaches_the_log
stub_psql <<'STUB'
#!/bin/sh
echo 'psql:/work/provision-roles.sql:120: ERROR:  canceling statement due to lock timeout' >&2
exit 3
STUB
run_script
[ "${STATUS}" = 3 ] || fail "exit status ${STATUS}, expected 3"
grep -qF "canceling statement due to lock timeout" "${OUT}" || fail "the lock timeout error is missing from the log"

echo "PASS ${CURRENT}"
