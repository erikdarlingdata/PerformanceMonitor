#!/bin/sh
# Tests of files/render-config.sh. POSIX sh, run under sh (dash in the Darling image):
#   sh Darling/helm/performancemonitor-darling/tests/render-config-tests.sh
set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/../files/render-config.sh"
WORK="$(mktemp -d)"
TEMPLATE="${WORK}/darling.json"
OUTPUT="${WORK}/out.json"
ERR="${WORK}/err.txt"
CURRENT=""

begin() {
  if [ -n "${CURRENT}" ]; then
    echo "PASS ${CURRENT}"
  fi
  CURRENT="$1"
}

fail() {
  echo "FAIL ${CURRENT}: $1" >&2
  exit 1
}

trap 'rc=$?; if [ "${rc}" != 0 ]; then echo "FAIL ${CURRENT:-<setup>}" >&2; fi; rm -rf "${WORK}"' EXIT

# run_subst <placeholder> <value>: runs the script with one pair; sets STATUS.
run_subst() {
  rm -f "${OUTPUT}"
  STATUS=0
  DARLING_SUBST_0_PLACEHOLDER="$1" DARLING_SUBST_0_VALUE="$2" sh "${SCRIPT}" "${TEMPLATE}" "${OUTPUT}" 2> "${ERR}" || STATUS=$?
}

begin subst_plain_url
printf '{"url":"@@HOOK@@"}\n' > "${TEMPLATE}"
run_subst '@@HOOK@@' 'https://hooks.example.com/services/A/B'
[ "${STATUS}" = 0 ] || fail "exit ${STATUS}"
[ "$(cat "${OUTPUT}")" = '{"url":"https://hooks.example.com/services/A/B"}' ] || fail "output differs: $(cat "${OUTPUT}")"

begin subst_ampersand_and_slashes
printf '{"a":"@@U@@","b":"@@U@@"}\n' > "${TEMPLATE}"
run_subst '@@U@@' 'https://x.example/a?b=1&c=2/d'
[ "${STATUS}" = 0 ] || fail "exit ${STATUS}"
[ "$(cat "${OUTPUT}")" = '{"a":"https://x.example/a?b=1&c=2/d","b":"https://x.example/a?b=1&c=2/d"}' ] || fail "output differs: $(cat "${OUTPUT}")"

begin subst_refuses_pipe_backslash_quote_newline
printf '{"url":"@@HOOK@@"}\n' > "${TEMPLATE}"
NL='
'
for bad in 'a|b' 'a\b' 'a"b' "a${NL}b"; do
  run_subst '@@HOOK@@' "${bad}"
  [ "${STATUS}" = 2 ] || fail "value refused with exit ${STATUS}, expected 2"
  grep -qF '@@HOOK@@' "${ERR}" || fail "stderr does not name the placeholder"
  case "$(cat "${ERR}")" in
    *"${bad}"*) fail "stderr contains the value" ;;
  esac
  [ ! -e "${OUTPUT}" ] || fail "output written for a refused value"
done

begin subst_refuses_missing_placeholder
printf '{"url":"nothing"}\n' > "${TEMPLATE}"
run_subst '@@HOOK@@' 'https://ok.example'
[ "${STATUS}" != 0 ] || fail "missing placeholder accepted"
grep -qF '@@HOOK@@' "${ERR}" || fail "stderr does not name the placeholder"

begin subst_refuses_leftover
printf '{"url":"@@HOOK@@"}\n' > "${TEMPLATE}"
# A value that contains its own placeholder leaves the placeholder behind.
run_subst 'HOOK' 'HOOK'
[ "${STATUS}" != 0 ] || fail "leftover placeholder accepted"
grep -qF 'HOOK' "${ERR}" || fail "stderr does not name the placeholder"

echo "PASS ${CURRENT}"
CURRENT=""
