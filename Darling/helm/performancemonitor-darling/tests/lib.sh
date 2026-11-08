#!/usr/bin/env bash
# Shared helpers of the chart's render tests. Source it; needs helm and yq (mikefarah, v4) on PATH.
#
#   render [helm args...]      helm template of the chart into $OUT
#   doc <kind> <name>          one document from $OUT, selected by kind and name (never by position)
#   assert_eq <actual> <expected> <label>
#   assert_fails <message substring> [helm args...]   helm template must fail, and its output must contain the substring
#   t <name>                   starts a named test; the first failure prints the test name and exits 1

set -euo pipefail

CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE=t
OUT="$(mktemp)"
CURRENT_TEST=""
FAILED=0

finish_test() {
  if [ -n "${CURRENT_TEST}" ] && [ "${FAILED}" = 0 ]; then
    echo "PASS ${CURRENT_TEST}"
  fi
  CURRENT_TEST=""
}

on_exit() {
  local status=$?
  rm -f "${OUT}"
  if [ "${status}" != 0 ]; then
    FAILED=1
    echo "FAIL ${CURRENT_TEST:-<setup>}" >&2
  else
    finish_test
  fi
}
trap on_exit EXIT

t() {
  finish_test
  CURRENT_TEST="$1"
}

render() {
  helm template "${RELEASE}" "${CHART_DIR}" "$@" > "${OUT}"
}

doc() {
  yq "select(.kind == \"$1\" and .metadata.name == \"$2\")" "${OUT}"
}

assert_eq() {
  local actual expected="$2" label="$3"
  actual="$(printf '%s' "$1" | tr -d '\r')"
  if [ "${actual}" != "${expected}" ]; then
    echo "assert_eq failed: ${label}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    exit 1
  fi
}

assert_fails() {
  local expected="$1" output
  shift
  if output="$(helm template "${RELEASE}" "${CHART_DIR}" "$@" 2>&1)"; then
    echo "assert_fails: helm template succeeded, expected failure containing: ${expected}" >&2
    exit 1
  fi
  if ! grep -qF -- "${expected}" <<<"${output}"; then
    echo "assert_fails: the failure does not contain: ${expected}" >&2
    echo "${output}" >&2
    exit 1
  fi
}
