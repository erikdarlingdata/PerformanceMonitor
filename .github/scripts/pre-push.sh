#!/usr/bin/env bash
# pre-push.sh: a local copy of the gates CI runs first on a pull request, in three stages. Run it from the worktree
# root BEFORE EVERY PUSH (bash .github/scripts/pre-push.sh). It prints result lines only ([FAIL], Total:) and ends
# with one GATE GREEN / GATE RED line. GATE GREEN means these three stages passed; CI decides the rest (#5833).
#   1. Debug build of both test projects (Darling.Tests, Lite.Tests).
#   2. The whole Stage=Guard trait of both suites with -failSkips: exactly CI's guard-tests job.
#   3. The test classes this diff needs: the classes CI's test map selects (ci-select.py map-select over the newest
#      nightly test-map artifact, cached under PRE_PUSH_CACHE; FULL whenever a keep-full file changed, as in CI)
#      UNION every class in a test file that names a CHANGED SYMBOL (a member or type declared on a +/- line of the
#      diff, or the hunk's enclosing declaration, or an added file's type). Symbols are added in order of how many
#      test files name them; once the union would pass the cap the rest are dropped and listed, since a symbol named
#      by most of the suite is the suite (the laptop never runs a full suite: CI decides).
# Flags: --dry-run (steps 1-2 skipped, step 3 only lists the classes), --skip-build, --no-map (symbol set only).
# Environment: PRE_PUSH_CAP (class cap, default 250); PRE_PUSH_CACHE (test-map cache, default pre-push-cache in the
# repository's common .git directory, which every worktree shares and git never commits).
# Exit 1 on any failure. Live classes in the selection skip without a rig; the Skipped count is printed so you see it.
set -uo pipefail
DRY=0; BUILD=1; MAP=1; CAP=${PRE_PUSH_CAP:-250}
for a in "$@"; do case "$a" in --dry-run) DRY=1;; --skip-build) BUILD=0;; --no-map) MAP=0;; *) echo "unknown flag $a"; exit 2;; esac; done
root=$(git rev-parse --show-toplevel 2>/dev/null) || { echo "GATE RED: not inside a git worktree"; exit 1; }
cd "$root" || exit 1
[ -f .github/scripts/ci-select.py ] || { echo "GATE RED: $root has no .github/scripts/ci-select.py"; exit 1; }
# In a linked worktree .git is a file, so the default lives in the common .git directory, not "$root/.git".
gitdir=$(cd "$(git rev-parse --git-common-dir)" && pwd) || { echo "GATE RED: no .git directory found"; exit 1; }
CACHE="${PRE_PUSH_CACHE:-$gitdir/pre-push-cache}"; mkdir -p "$CACHE" || { echo "GATE RED: cannot create $CACHE"; exit 1; }
SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/pre-push.XXXXXX")
DARLING=Darling/Darling.Tests/Darling.Tests.csproj
LITE=Lite.Tests/Lite.Tests.csproj
TESTDIRS="Darling/Darling.Tests Lite.Tests"
red=()

run_suite() { # run_suite <label> <project> <args...>: prints result lines, records a red on failure
  local label=$1 proj=$2; shift 2
  local out="$SCRATCH/$label.log"
  dotnet run --project "$proj" -c Debug --no-build -- "$@" > "$out" 2>&1
  local rc=$?
  grep -E '\[FAIL\]|Total: ' "$out" | sed "s/^/  [$label] /" | cut -c1-200
  if [ $rc -ne 0 ]; then red+=("$label (exit $rc; log $out)"); fi
}

# 1. build
if [ $DRY -eq 0 ] && [ $BUILD -eq 1 ]; then
  for p in "$DARLING" "$LITE"; do
    if ! dotnet build "$p" -c Debug --nologo -v q > "$SCRATCH/build.log" 2>&1; then
      grep -E 'error [A-Z]+[0-9]+' "$SCRATCH/build.log" | sort -u | head -n 10 | cut -c1-200
      echo "GATE RED: build of $p failed (log $SCRATCH/build.log)"; exit 1
    fi
  done
  echo "build: both test projects built (Debug)"
fi

# 2. the whole Guard stage of both suites, as CI's guard-tests job runs it
if [ $DRY -eq 0 ]; then
  echo "guard: Stage=Guard of both suites"
  run_suite guard-darling "$DARLING" -trait Stage=Guard -failSkips
  run_suite guard-lite "$LITE" -trait Stage=Guard -failSkips
fi

# 3. the classes this diff needs
git fetch -q origin dev 2>/dev/null
base=$(git merge-base origin/dev HEAD) || { echo "GATE RED: no merge base with origin/dev"; exit 1; }
git diff --no-renames --name-only "$base" HEAD > "$SCRATCH/changed.txt"
echo "diff: $(wc -l < "$SCRATCH/changed.txt" | tr -d ' ') changed files since merge base ${base:0:9}"

declare -A want_d want_l
if [ $MAP -eq 1 ]; then
  mapfile=""
  for id in $(gh api "repos/erikdarlingdata/PerformanceMonitor/actions/workflows/nightly.yml/runs?status=success&per_page=8" \
      --jq '.workflow_runs[] | select(.event == "schedule" or .event == "workflow_dispatch") | .id' 2>/dev/null); do
    if [ -f "$CACHE/map-$id/test-map.json.gz" ]; then mapfile="$CACHE/map-$id/test-map.json.gz"; break; fi
    if gh run download "$id" -n test-map -D "$CACHE/map-$id" > /dev/null 2>&1 && [ -f "$CACHE/map-$id/test-map.json.gz" ]; then
      mapfile="$CACHE/map-$id/test-map.json.gz"; break
    fi
    rm -rf "$CACHE/map-$id"
  done
  if [ -z "$mapfile" ]; then
    echo "map: no nightly test-map artifact could be fetched; symbol set only"
  elif ! timeout 120 python .github/scripts/ci-select.py map-select --map "$mapfile" --event pull_request --base-ref dev \
        --base "$base" --files-from "$SCRATCH/changed.txt" --out "$SCRATCH/selection.json" > "$SCRATCH/map-select.log" 2>&1; then
    echo "map: map-select failed (log $SCRATCH/map-select.log); symbol set only"
  else
    for suite in darling lite; do
      python .github/scripts/ci-select.py map-keep --selection "$SCRATCH/selection.json" --suite $suite > "$SCRATCH/keep-$suite.txt" 2>&1
      if head -n1 "$SCRATCH/keep-$suite.txt" | grep -qE '^SELECTED [0-9]+$'; then
        while read -r c; do [ -n "$c" ] && { if [ $suite = darling ]; then want_d["$c"]=map; else want_l["$c"]=map; fi; }; done < <(tail -n +2 "$SCRATCH/keep-$suite.txt")
        echo "map: $(basename "$(dirname "$mapfile")"), $suite $(head -n1 "$SCRATCH/keep-$suite.txt")"
      else
        echo "map: $suite $(head -n1 "$SCRATCH/keep-$suite.txt" | cut -c1-140); symbol set only"
      fi
    done
  fi
fi

# the changed symbols: declarations on +/- lines and hunk headers of the diff, plus added files' type names
STOP='public|private|protected|internal|static|const|readonly|override|virtual|abstract|sealed|async|partial|record|class|enum|interface|struct|event|delegate|function|export|new|return|var|string|int|long|bool|double|void|if|else|while|for|foreach|using|get|set|init|value|true|false|null|this|base|typeof|nameof|default|params|await|switch|case|throw|catch|try|lock|yield|where|select|from|Task|List|Dictionary|Func|Action|Exception|Guid|DateTime|TimeSpan|Equals|ToString|GetHashCode|Dispose|Main'
git diff --no-renames -U0 "$base" HEAD -- '*.cs' '*.js' '*.xaml' > "$SCRATCH/diff.txt"
{ grep -E '^[+-][^+-]' "$SCRATCH/diff.txt" | sed 's/^[+-]//'; grep -E '^@@' "$SCRATCH/diff.txt" | sed -E 's/^@@[^@]*@@ ?//'; } \
  | grep -vE '^\s*(//|\*|/\*)' \
  | grep -E '\b(public|private|protected|internal|static|const|readonly|override|virtual|abstract|sealed|async|partial|record|class|enum|interface|struct|event|delegate|function|export)\b' \
  | grep -oE '\b[A-Za-z_][A-Za-z0-9_]*\s*(\(|\{|=|;|=>|<[A-Za-z0-9_, ]*>\s*\()' | sed -E 's/\s*(\(|\{|=|;|=>|<.*)$//' \
  | grep -vE "^($STOP)$" | grep -E '^.{4,}$' > "$SCRATCH/names.txt"
git diff --no-renames --name-only --diff-filter=A "$base" HEAD -- '*.cs' | sed -E 's#.*/##; s/\.xaml\.cs$//; s/\.cs$//' >> "$SCRATCH/names.txt"
sort -u -o "$SCRATCH/names.txt" "$SCRATCH/names.txt"
nnames=$(wc -l < "$SCRATCH/names.txt" | tr -d ' ')
files=""
if [ "$nnames" -gt 0 ]; then
  # one pass: every (test file, symbol) pair; then symbols in ascending order of files named, unioned under the cap
  grep -rwoF --include='*.cs' --exclude-dir=bin --exclude-dir=obj -f "$SCRATCH/names.txt" $TESTDIRS 2>/dev/null | sort -u > "$SCRATCH/pairs.txt"
  awk -F: -v cap="$CAP" -v out="$SCRATCH/files.txt" -v dropped="$SCRATCH/dropped.txt" '
    { n[$2]++; f[$2] = f[$2] SUBSEP $1 }
    END {
      m = 0; for (s in n) { order[++m] = s }
      for (i = 1; i <= m; i++) for (j = i + 1; j <= m; j++) if (n[order[j]] < n[order[i]] || (n[order[j]] == n[order[i]] && order[j] < order[i])) { t = order[i]; order[i] = order[j]; order[j] = t }
      total = 0; stop = 0
      for (i = 1; i <= m; i++) {
        s = order[i]; k = split(f[s], arr, SUBSEP); add = 0
        for (x = 2; x <= k; x++) if (!(arr[x] in seen)) add++
        if (!stop && total + add > cap) stop = 1
        if (stop) { print s " (" n[s] " files)" > dropped; nd++; continue }
        for (x = 2; x <= k; x++) if (!(arr[x] in seen)) { seen[arr[x]] = 1; print arr[x] > out; total++ }
        kept = kept " " s
      }
      printf "symbols: %d named by a test file, %d kept (%d test files):%s\n", m, m - nd, total, kept
    }' "$SCRATCH/pairs.txt" | cut -c1-400
  [ -s "$SCRATCH/dropped.txt" ] && echo "symbols dropped (over the $CAP-file cap, each named by most of the suite): $(tr '\n' ';' < "$SCRATCH/dropped.txt" | cut -c1-300)"
  files=$(cat "$SCRATCH/files.txt" 2>/dev/null)
fi
if [ -n "$files" ]; then
  # ONE awk pass over the kept files for "<file>\t<namespace>.<class>" (a grep per file is minutes on MSYS)
  echo "$files" | xargs -d '\n' -s 28000 awk '
      FNR == 1 { ns = "" }
      /^namespace[ \t]+[A-Za-z0-9_.]+/ { ns = $2; sub(/[;{].*$/, "", ns) }
      /^[ \t]*(public|internal)?[ \t]*((sealed|static|partial|abstract)[ \t]+)*class[ \t]+[A-Za-z0-9_]+/ {
        for (i = 1; i <= NF; i++) if ($i == "class") { c = $(i + 1); sub(/[^A-Za-z0-9_].*$/, "", c); print FILENAME "\t" (ns == "" ? "" : ns ".") c; break }
      }' 2>/dev/null | sort -u > "$SCRATCH/classes.txt"
  while IFS=$'\t' read -r f full; do
    [ -n "$full" ] || continue
    case "$f" in Lite.Tests/*) want_l["$full"]=sym;; *) want_d["$full"]=sym;; esac
  done < "$SCRATCH/classes.txt"
fi
echo "classes: darling ${#want_d[@]}, lite ${#want_l[@]} (symbols: $nnames candidates; lists in $SCRATCH)"

run_classes() { # run_classes <label> <project> <names...>: chunks of 150 -class arguments
  local label=$1 proj=$2; shift 2
  local -a chunk=(); local i=0
  for c in "$@"; do
    chunk+=(-class "$c"); i=$((i+1))
    if [ $i -ge 150 ]; then run_suite "$label" "$proj" "${chunk[@]}"; chunk=(); i=0; fi
  done
  [ ${#chunk[@]} -gt 0 ] && run_suite "$label" "$proj" "${chunk[@]}"
}
if [ $DRY -eq 1 ]; then
  printf '  darling: %s\n' "${!want_d[@]}" | sort
  printf '  lite: %s\n' "${!want_l[@]}" | sort
  echo "GATE DRY-RUN: nothing built or run"; exit 0
fi
[ ${#want_d[@]} -gt 0 ] && run_classes classes-darling "$DARLING" "${!want_d[@]}"
[ ${#want_l[@]} -gt 0 ] && run_classes classes-lite "$LITE" "${!want_l[@]}"

if [ ${#red[@]} -eq 0 ]; then
  echo "GATE GREEN: guard stage of both suites and ${#want_d[@]}+${#want_l[@]} selected classes passed (logs $SCRATCH)"; exit 0
fi
echo "GATE RED: ${red[*]}"; exit 1
