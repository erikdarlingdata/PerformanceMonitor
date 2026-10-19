#!/bin/sh
# One-step setup for the Darling Docker Compose stack (#6140).
#
# Run it once from anywhere, then `docker compose up -d` in this folder:
#   ./setup.sh                # asks for the monitoring login's SQL password (hidden input)
#   DARLING_SQL_PASSWORD='...' ./setup.sh --no-prompt
#
# What it makes, only when the file is missing (a file that exists is never changed):
#   secrets/                  folder, mode 700
#   secrets/store_password.txt  random store password
#   secrets/store_connection    the store connection string, with that same password
#   secrets/web_token.txt       random dashboard access token
#   secrets/mcp_token.txt       random MCP token (only used once MCP is turned on)
#   secrets/sql_password.txt    the monitoring login's SQL password, from $DARLING_SQL_PASSWORD or a prompt
#   darling.json              a copy of darling.sample.json
# Each secret file is mode 600. This script never prints a secret.
#
# Options:
#   --no-prompt   never ask anything; with no $DARLING_SQL_PASSWORD, skip secrets/sql_password.txt and say so
#   -h, --help    show this text

set -eu

prompt=1
for arg in "$@"; do
  case "$arg" in
    --no-prompt) prompt=0 ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "setup.sh: unknown option '$arg' (try --help)" >&2
      exit 2
      ;;
  esac
done

# Work from this script's own folder, so it runs from any directory.
here=$(dirname -- "$0")
cd -- "$here"

secrets=secrets
umask 077

mkdir -p "$secrets"
chmod 700 "$secrets"

# random_string LENGTH: letters and digits only, so the value is safe inside a connection string.
random_string() {
  LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c "$1"
}

# write_secret FILE VALUE: a private file holding VALUE and a newline (references trim it).
write_secret() {
  printf '%s\n' "$2" > "$1"
  chmod 600 "$1"
}

made=
kept=

note_made() { made="$made $1"; }
note_kept() { kept="$kept $1"; }

store_password_file="$secrets/store_password.txt"
store_connection_file="$secrets/store_connection"

# The store password and the connection string repeat the same value, so they are made together.
# If only one of them exists, the missing one is rebuilt from the other so the two always agree.
if [ ! -e "$store_password_file" ] && [ ! -e "$store_connection_file" ]; then
  store_password=$(random_string 32)
  write_secret "$store_password_file" "$store_password"
  write_secret "$store_connection_file" \
    "Host=store;Port=5432;Username=darling;Password=$store_password;Database=darling"
  note_made "$store_password_file"
  note_made "$store_connection_file"
  store_password=
elif [ ! -e "$store_connection_file" ]; then
  store_password=$(tr -d '\r\n' < "$store_password_file")
  write_secret "$store_connection_file" \
    "Host=store;Port=5432;Username=darling;Password=$store_password;Database=darling"
  note_made "$store_connection_file"
  note_kept "$store_password_file"
  store_password=
elif [ ! -e "$store_password_file" ]; then
  store_password=$(sed -n 's/.*[Pp]assword=\([^;]*\).*/\1/p' "$store_connection_file" | tr -d '\r\n')
  if [ -n "$store_password" ]; then
    write_secret "$store_password_file" "$store_password"
    note_made "$store_password_file"
    note_kept "$store_connection_file"
  else
    echo "setup.sh: $store_connection_file has no Password= part, so $store_password_file was not made. Fix one of the two files by hand." >&2
  fi
  store_password=
else
  note_kept "$store_password_file"
  note_kept "$store_connection_file"
fi

for name in web_token.txt mcp_token.txt; do
  if [ -e "$secrets/$name" ]; then
    note_kept "$secrets/$name"
  else
    write_secret "$secrets/$name" "$(random_string 48)"
    note_made "$secrets/$name"
  fi
done

sql_password_file="$secrets/sql_password.txt"
sql_password_missing=0
if [ -e "$sql_password_file" ]; then
  note_kept "$sql_password_file"
else
  sql_password=${DARLING_SQL_PASSWORD:-}
  if [ -z "$sql_password" ] && [ "$prompt" = 1 ] && [ -t 0 ]; then
    printf "Password for the SQL Server login Darling monitors with (typing is hidden; empty to skip): " >&2
    stty_state=$(stty -g 2>/dev/null || true)
    restore_tty() { [ -n "$stty_state" ] && stty "$stty_state" 2>/dev/null || true; }
    trap 'restore_tty; exit 130' INT TERM
    stty -echo 2>/dev/null || true
    IFS= read -r sql_password || sql_password=
    restore_tty
    trap - INT TERM
    printf '\n' >&2
  fi
  if [ -n "$sql_password" ]; then
    write_secret "$sql_password_file" "$sql_password"
    note_made "$sql_password_file"
  else
    sql_password_missing=1
  fi
  sql_password=
fi

if [ -e darling.json ]; then
  note_kept darling.json
elif [ -e darling.sample.json ]; then
  cp darling.sample.json darling.json
  note_made darling.json
else
  echo "setup.sh: darling.sample.json is not in $(pwd), so darling.json was not made." >&2
fi

echo
if [ -n "$made" ]; then
  echo "Created:"
  for f in $made; do echo "  $f"; done
fi
if [ -n "$kept" ]; then
  echo "Already there, left as they were:"
  for f in $kept; do echo "  $f"; done
fi

if [ "$sql_password_missing" = 1 ]; then
  echo
  echo "Still needed: $sql_password_file, holding the password of the SQL Server login Darling monitors with."
  echo "Make it with:  DARLING_SQL_PASSWORD='<password>' ./setup.sh --no-prompt"
  echo "or put the password in that file yourself (chmod 600)."
fi

echo
echo "Next:"
echo "  1. Edit darling.json: set your SQL Server in \"servers\" (host, username) and any alerting."
echo "  2. docker compose up -d"
echo "  3. Open the dashboard at http://127.0.0.1:5153 on this machine."
echo "     Its access token is in $secrets/web_token.txt. Show it with:  cat $secrets/web_token.txt"
echo "MCP is off. To turn it on, set mcp.enabled to true in darling.json and uncomment mcp_token in docker-compose.yml (both places)."
