#!/bin/sh
# Writes the web and MCP connection strings for the chart's store-logins init container (#8779).
#
#   write-store-logins.sh <base> <password-dir> <out-dir>
#
#   <base>          the non-secret Npgsql prefix the chart renders:
#                   Host=..;Port=..;Database=..;SSL Mode=..[;Root Certificate=/etc/darling/store-ca/ca.crt]
#   <password-dir>  holds one file per role (web, mcp) with that role's password
#   <out-dir>       receives <role>, the whole connection string: <base>;Username=<role>;Password=<password>, mode 0400
#
# For each role it drops exactly one trailing newline from the password and applies the role-password rule (16 or more
# characters from A-Z a-z 0-9 . _ ~ + / = -, the rule of the store-roles Job). A refusal exits 2, names the role and never
# prints the value; no file is written unless both roles pass. POSIX sh only: it runs in the Darling image.
set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: write-store-logins.sh <base> <password-dir> <out-dir>" >&2
  exit 64
fi
base="$1"
pwdir="$2"
outdir="$3"

LC_ALL=C
export LC_ALL
nl='
'
cr="$(printf '\r')"

refuse() {
  echo "write-store-logins: $1" >&2
  exit 2
}

# Sets PW to the role's password, or refuses. The sentinel x keeps the command substitution from eating trailing newlines.
read_password() {
  role="$1"
  file="${pwdir}/${role}"
  if [ ! -r "${file}" ]; then
    refuse "The password file for role ${role} cannot be read."
  fi
  PW="$(cat "${file}"; echo x)"
  PW="${PW%x}"
  case "${PW}" in
    *"${nl}") PW="${PW%"${nl}"}" ;;
  esac
  if [ -z "${PW}" ]; then
    refuse "The password for role ${role} is empty."
  fi
  case "${PW}" in
    *"${cr}"*) refuse "The password for role ${role} contains a carriage return; write the file with Unix line endings." ;;
  esac
  if [ "${#PW}" -lt 16 ]; then
    refuse "The password for role ${role} is shorter than 16 characters."
  fi
  case "${PW}" in
    *[!A-Za-z0-9._~+/=-]*) refuse "The password for role ${role} has a character outside A-Z a-z 0-9 . _ ~ + / = -." ;;
  esac
}

for role in web mcp; do
  read_password "${role}"
done

umask 277
for role in web mcp; do
  read_password "${role}"
  # A read-only file from an earlier run of this container in the same pod cannot be opened for writing, so remove it.
  rm -f "${outdir}/${role}"
  printf '%s;Username=%s;Password=%s' "${base}" "${role}" "${PW}" > "${outdir}/${role}"
done
