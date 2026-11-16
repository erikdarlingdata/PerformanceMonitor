#!/bin/sh
# Runs the prepared provision-roles.sql with psql, for the chart's store-roles Job (#8779).
#
#   run-provision-roles.sh        (no arguments; the init container has prepared /work)
#
# It
#   1. loads the non-secret libpq settings the init container wrote (/work/libpq.env) and points PGPASSFILE at the
#      pgpass file next to it; no password is ever in the environment or on a command line,
#   2. runs psql -X -v ON_ERROR_STOP=1 -f /work/provision-roles.sql, without -a, -e or -b, so a statement is never echoed,
#   3. masks every SCRAM-SHA-256 verifier in psql's output (an error's LINE n: context could show one),
#   4. prints "provision-roles.sql exit code: N" last and exits N, psql's own status.
# WORK_DIR overrides /work, for the tests. POSIX sh only: the default psql image is Alpine, which has no bash.
set -eu

work="${WORK_DIR:-/work}"
out="${work}/psql-output.txt"

set -a
. "${work}/libpq.env"
set +a
PGPASSFILE="${work}/pgpass"
export PGPASSFILE

# psql's status is read from the plain command, then its output is masked from the file: no pipeline, so no PIPESTATUS.
rc=0
psql -X -v ON_ERROR_STOP=1 -f "${work}/provision-roles.sql" > "${out}" 2>&1 || rc=$?

# A verifier runs up to a space or a quote. The pattern keeps the single quote out of the bracket by closing the quoting.
pat='SCRAM-SHA-256\$[^[:space:]'"'"'"]+'
sed -E "s/${pat}/SCRAM-SHA-256\$***/g" "${out}"
rm -f "${out}"

echo "provision-roles.sql exit code: ${rc}"
exit "${rc}"
