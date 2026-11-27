#!/bin/sh
# Tests of files/write-store-logins.sh. POSIX sh, run under sh (dash on the CI runner):
#   sh Darling/helm/performancemonitor-darling/tests/write-store-logins-tests.sh
# Each case runs the script with a temp password directory and a temp output directory.
set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/../files/write-store-logins.sh"
ROOT="$(mktemp -d)"
PW="${ROOT}/pw"
OUTDIR="${ROOT}/out"
LOG="${ROOT}/log.txt"
CURRENT=""
BASE='Host=db.example.internal;Port=5432;Database=darling;SSL Mode=VerifyFull'
GOOD_WEB='webpassword-0123456789'
GOOD_MCP='mcppassword.0123456789'
trap 'rm -rf "${ROOT}"' EXIT

begin() {
  if [ -n "${CURRENT}" ]; then
    echo "PASS ${CURRENT}"
  fi
  CURRENT="$1"
  rm -rf "${PW}" "${OUTDIR}"
  mkdir -p "${PW}" "${OUTDIR}"
  printf '%s\n' "${GOOD_WEB}" > "${PW}/web"
  printf '%s\n' "${GOOD_MCP}" > "${PW}/mcp"
}

fail() {
  echo "FAIL ${CURRENT}: $1" >&2
  exit 1
}

# Runs the script with the base given as $1; sets STATUS and leaves stdout and stderr in ${LOG}.
run_script() {
  STATUS=0
  sh "${SCRIPT}" "$1" "${PW}" "${OUTDIR}" > "${LOG}" 2>&1 || STATUS=$?
}

# Git Bash on NTFS has no POSIX file modes (every file lists as r--r--r-- or rw-r--r--), so the mode check runs on Linux only.
WINDOWS_FS=0
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) WINDOWS_FS=1 ;;
esac

# The mode string of a file, from ls.
mode_of() {
  ls -l "$1" | cut -c2-10
}

begin writes_both_files_0400
run_script "${BASE}"
[ "${STATUS}" = 0 ] || fail "exit status ${STATUS}, expected 0: $(cat "${LOG}")"
[ "$(cat "${OUTDIR}/web")" = "${BASE};Username=web;Password=${GOOD_WEB}" ] || fail "web file is: $(cat "${OUTDIR}/web")"
[ "$(cat "${OUTDIR}/mcp")" = "${BASE};Username=mcp;Password=${GOOD_MCP}" ] || fail "mcp file is: $(cat "${OUTDIR}/mcp")"
if [ "${WINDOWS_FS}" = 0 ]; then
  [ "$(mode_of "${OUTDIR}/web")" = "r--------" ] || fail "web mode is $(mode_of "${OUTDIR}/web")"
  [ "$(mode_of "${OUTDIR}/mcp")" = "r--------" ] || fail "mcp mode is $(mode_of "${OUTDIR}/mcp")"
fi
# A second run (the init container restarts in the same pod) overwrites the read-only files.
run_script "${BASE}"
[ "${STATUS}" = 0 ] || fail "second run: exit status ${STATUS}: $(cat "${LOG}")"

begin drops_one_trailing_newline
printf '%s\n\n' "${GOOD_WEB}" > "${PW}/web"
run_script "${BASE}"
[ "${STATUS}" = 2 ] || fail "two trailing newlines: exit status ${STATUS}, expected 2"
printf '%s' "${GOOD_WEB}" > "${PW}/web"
run_script "${BASE}"
[ "${STATUS}" = 0 ] || fail "no trailing newline: exit status ${STATUS}: $(cat "${LOG}")"
[ "$(cat "${OUTDIR}/web")" = "${BASE};Username=web;Password=${GOOD_WEB}" ] || fail "web file is: $(cat "${OUTDIR}/web")"
[ "$(wc -l < "${OUTDIR}/web" | tr -d ' ')" = 0 ] || fail "the output file ends with a newline"

begin refuses_semicolon_naming_role_not_value
printf '%s\n' 'abcdefghijklmnop;Server=evil' > "${PW}/mcp"
run_script "${BASE}"
[ "${STATUS}" = 2 ] || fail "exit status ${STATUS}, expected 2"
grep -qF "role mcp" "${LOG}" || fail "the refusal does not name the role: $(cat "${LOG}")"
if grep -qF "evil" "${LOG}"; then fail "the refusal shows the value"; fi
if [ -e "${OUTDIR}/web" ] || [ -e "${OUTDIR}/mcp" ]; then fail "a file was written on a refusal"; fi

begin refuses_short
printf '%s\n' 'short-pw' > "${PW}/web"
run_script "${BASE}"
[ "${STATUS}" = 2 ] || fail "exit status ${STATUS}, expected 2"
grep -qF "role web" "${LOG}" || fail "the refusal does not name the role: $(cat "${LOG}")"
grep -qF "shorter than 16" "${LOG}" || fail "the refusal does not say short: $(cat "${LOG}")"
if grep -qF "short-pw" "${LOG}"; then fail "the refusal shows the value"; fi

begin refuses_carriage_return
printf '%s\r\n' "${GOOD_WEB}" > "${PW}/web"
run_script "${BASE}"
[ "${STATUS}" = 2 ] || fail "exit status ${STATUS}, expected 2"
grep -qF "role web" "${LOG}" || fail "the refusal does not name the role: $(cat "${LOG}")"
grep -qF "carriage return" "${LOG}" || fail "the refusal does not say carriage return: $(cat "${LOG}")"

begin refuses_empty_and_missing
: > "${PW}/web"
run_script "${BASE}"
[ "${STATUS}" = 2 ] || fail "empty: exit status ${STATUS}, expected 2"
grep -qF "role web is empty" "${LOG}" || fail "empty: $(cat "${LOG}")"
rm -f "${PW}/mcp"
printf '%s\n' "${GOOD_WEB}" > "${PW}/web"
run_script "${BASE}"
[ "${STATUS}" = 2 ] || fail "missing: exit status ${STATUS}, expected 2"
grep -qF "role mcp" "${LOG}" || fail "missing: $(cat "${LOG}")"

begin base_with_root_certificate_kept
run_script "${BASE};Root Certificate=/etc/darling/store-ca/ca.crt"
[ "${STATUS}" = 0 ] || fail "exit status ${STATUS}: $(cat "${LOG}")"
[ "$(cat "${OUTDIR}/mcp")" = "${BASE};Root Certificate=/etc/darling/store-ca/ca.crt;Username=mcp;Password=${GOOD_MCP}" ] \
  || fail "mcp file is: $(cat "${OUTDIR}/mcp")"

echo "PASS ${CURRENT}"
