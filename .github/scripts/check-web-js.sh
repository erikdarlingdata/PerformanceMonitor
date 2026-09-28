#!/usr/bin/env bash
# Parses every web-dashboard JavaScript file as an ES module with `node --check`.
# Usage: check-web-js.sh [root]   (root defaults to the wwwroot/js tree of this checkout)
# Each file is copied to a temp .mjs first, so Node treats it as a module whatever the
# nearest package.json says. Exits non-zero when any file fails to parse, or none is found.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
root="${1:-$repo_root/Darling/PerformanceMonitor.Darling.Service/wwwroot/js}"

if [ ! -d "$root" ]; then
  echo "FAIL: directory not found: $root" >&2
  exit 2
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

count=0
failed=0
while IFS= read -r -d '' file; do
  count=$((count + 1))
  copy="$tmp/$count.mjs"
  cp "$file" "$copy"
  if err="$(node --check "$copy" 2>&1)"; then
    echo "OK   $file"
  else
    failed=$((failed + 1))
    echo "FAIL $file"
    # Node names the temp copy; show its message with the real path.
    printf '%s\n' "${err//$copy/$file}" | sed 's/^/     /'
  fi
done < <(find "$root" -type f -name '*.js' -print0 | sort -z)

if [ "$count" -eq 0 ]; then
  echo "FAIL: no .js files found under $root" >&2
  exit 2
fi

echo "$count file(s) checked, $failed failed"
[ "$failed" -eq 0 ]
