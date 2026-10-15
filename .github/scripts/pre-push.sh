#!/usr/bin/env bash
# pre-push.sh: a local copy of the gates CI runs first on a pull request, in three stages. Run it from the worktree
# root BEFORE EVERY PUSH (bash .github/scripts/pre-push.sh). It prints result lines only ([FAIL], Total:) and ends
# with one GATE GREEN / GATE RED / GATE STOP line. GATE GREEN means these three stages passed; CI decides the rest (#5833).
#   1. Debug build of both test projects (Darling.Tests, Lite.Tests).
#   2. The whole Stage=Guard trait of both suites with -failSkips: exactly CI's guard-tests job.
#   3. The test classes this diff needs: the classes CI's test map selects (ci-select.py map-select over the newest
#      nightly test-map artifact, cached under PRE_PUSH_CACHE; FULL whenever a keep-full file changed, as in CI)
#      UNION every class in a test file that names a CHANGED SYMBOL (a member or type declared on a +/- line of the
#      diff, or the hunk's enclosing declaration, or an added file's type). Symbols are added in order of how many
#      test files name them; once the union would pass the cap the rest are dropped and listed, since a symbol named
#      by most of the suite is the suite (the laptop never runs a full suite: CI decides).
#   Stage 2 already ran every Stage=Guard class, so stage 3 leaves them out: map-select gets --guard-stage-separate and
#   the symbol set drops the classes of `-list classes/json -trait Stage=Guard` (#5894). A diff of documentation files
#   only selects nothing (CI's docs fast path runs no test for it either) and the gate prints why; its Guard stage
#   still runs. A map older than the newest nightly's is named in the `map:` header line with both run ids.
#   The map and the symbol scan both name a class by its simple name; the runner's -class filter matches only the
#   full name and runs nothing for any other (Total: 0, exit 0), so every name is resolved against the build's own
#   class listing (-list classes/json), exactly as CI's shard scripts do, and run in batches under the command-line
#   limit (#5868).
# Flags: --dry-run (steps 1-2 skipped; step 3 lists the classes and each batch's class count and length, resolved
# against an existing Debug build when there is one), --skip-build, --no-map (symbol set only).
# Environment: PRE_PUSH_CAP (test-file cap of the symbol set, default 250); PRE_PUSH_MAX_CLASSES (the most classes of
# one project the laptop runs, default 400: above it the selection is the whole suite, a STOP); PRE_PUSH_ARG_BUDGET
# (the characters of -class arguments per batch, default 30000, as CI's run-darling-pg-shard.ps1); PRE_PUSH_SELECTION
# (a map-select selection JSON used instead of the nightly map, for replays and the script's tests); PRE_PUSH_CACHE
# (test-map cache, default pre-push-cache in the repository's common .git directory, which every worktree shares and
# git never commits).
# Outcomes: GATE GREEN (exit 0). GATE RED (exit 1): a build, a run or a class listing failed, or a run reported
# Total: 0 (it matched nothing, which the runner does not count as a failure). GATE STOP (exit 3): the selection is
# the whole suite, which the laptop never runs; the Guard stage still ran, and a red in it wins over the stop.
# Live classes in the selection skip without a rig; the Skipped count is printed so you see it.
set -uo pipefail
DRY=0; BUILD=1; MAP=1; CAP=${PRE_PUSH_CAP:-250}; MAXC=${PRE_PUSH_MAX_CLASSES:-400}; BUDGET=${PRE_PUSH_ARG_BUDGET:-30000}
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

run_suite() { # run_suite <label> <what> <project> <args...>: prints result lines, records a red on failure
  local label=$1 what=$2 proj=$3; shift 3
  local out="$SCRATCH/$label.log"
  dotnet run --project "$proj" -c Debug --no-build -- "$@" > "$out" 2>&1
  local rc=$?
  grep -E '\[FAIL\]|Total: ' "$out" | sed "s/^/  [$label] /" | cut -c1-200
  if [ $rc -ne 0 ]; then red+=("$label (exit $rc; log $out)"); return; fi
  # #5868: a filter that matches nothing exits 0 with Total: 0, so exit 0 alone is not a pass.
  local total; total=$(grep -oE 'Total: [0-9]+' "$out" | tail -n 1 | grep -oE '[0-9]+')
  if [ -z "$total" ] || [ "$total" -eq 0 ]; then
    echo "  [$label] $what ran 0 tests: the filter matched nothing"
    red+=("$label $what ran 0 tests (log $out)")
  fi
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
  run_suite guard-darling "Stage=Guard" "$DARLING" -trait Stage=Guard -failSkips
  run_suite guard-lite "Stage=Guard" "$LITE" -trait Stage=Guard -failSkips
fi

# 3. the classes this diff needs
git fetch -q origin dev 2>/dev/null
base=$(git merge-base origin/dev HEAD) || { echo "GATE RED: no merge base with origin/dev"; exit 1; }
git diff --no-renames --name-only "$base" HEAD > "$SCRATCH/changed.txt"
echo "diff: $(wc -l < "$SCRATCH/changed.txt" | tr -d ' ') changed files since merge base ${base:0:9}"

# Initialised empty: under set -u a declared but unassigned array is unbound, and an empty selection read as GATE RED.
declare -A want_d=() want_l=()
if [ $MAP -eq 1 ]; then
  # PRE_PUSH_SELECTION replaces the nightly map and map-select (a replay, or the script's own tests)
  mapfile=${PRE_PUSH_SELECTION:-}; ids=""
  if [ -n "$mapfile" ]; then
    cp "$mapfile" "$SCRATCH/selection.json" || { echo "GATE RED: cannot read PRE_PUSH_SELECTION $mapfile"; exit 1; }
  else
    ids=$(gh api "repos/erikdarlingdata/PerformanceMonitor/actions/workflows/nightly.yml/runs?status=success&per_page=8" \
      --jq '.workflow_runs[] | select(.event == "schedule" or .event == "workflow_dispatch") | .id' 2>/dev/null)
  fi
  newest=""; for id in $ids; do newest=$id; break; done
  for id in $ids; do
    if [ -f "$CACHE/map-$id/test-map.json.gz" ]; then mapfile="$CACHE/map-$id/test-map.json.gz"; break; fi
    if gh run download "$id" -n test-map -D "$CACHE/map-$id" > /dev/null 2>&1 && [ -f "$CACHE/map-$id/test-map.json.gz" ]; then
      mapfile="$CACHE/map-$id/test-map.json.gz"; break
    fi
    rm -rf "$CACHE/map-$id"
  done
  if [ -z "$mapfile" ]; then
    echo "map: no nightly test-map artifact could be fetched; symbol set only"
  elif [ -z "${PRE_PUSH_SELECTION:-}" ] && ! timeout 120 python .github/scripts/ci-select.py map-select --map "$mapfile" --event pull_request --base-ref dev \
        --guard-stage-separate --base "$base" --files-from "$SCRATCH/changed.txt" --out "$SCRATCH/selection.json" > "$SCRATCH/map-select.log" 2>&1; then
    echo "map: map-select failed (log $SCRATCH/map-select.log); symbol set only"
  else
    # #5894: name the map (run id, build time) and say so when a newer nightly exists that could not be used
    if [ -z "${PRE_PUSH_SELECTION:-}" ]; then
    built=$(python -c 'import gzip, json, sys
try:
    print(json.load(gzip.open(sys.argv[1], "rt", encoding="utf-8")).get("built_at", "unknown"))
except Exception:
    print("unknown")' "$mapfile" 2>/dev/null)
    stale=""; mapid=$(basename "$(dirname "$mapfile")"); mapid=${mapid#map-}
    if [ -n "$newest" ] && [ -z "${PRE_PUSH_SELECTION:-}" ] && [ "$mapid" != "$newest" ]; then stale=" (STALE: nightly run $newest is newer and its map could not be fetched, so a class added since $built shows as a new class)"; fi
    echo "map: nightly run $mapid, built $built$stale"
    fi
    if [ -f "$SCRATCH/selection.json" ] && python -c 'import json, sys; sys.exit(0 if json.load(open(sys.argv[1])).get("docs_only") is True else 1)' "$SCRATCH/selection.json"; then
      echo "map: $(python -c 'import json, sys; print(json.load(open(sys.argv[1])).get("reason", ""))' "$SCRATCH/selection.json"); the classes batches are skipped"
    fi
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
    case "$f" in Lite.Tests/*) want_l["${full##*[.+/]}"]=sym;; *) want_d["${full##*[.+/]}"]=sym;; esac
  done < "$SCRATCH/classes.txt"
fi
# #5868: the runner's -class filter takes a full name, and a simple name matches nothing (Total: 0, exit 0), so every
# selected name is resolved against the build's own class listing, keyed by the simple name split on [.+/] as
# test-map.py, ci-select.py and run-darling-pg-shard.ps1 key it. A name with no test class in the listing (a helper
# class of a symbol file, a class the map holds but this build no longer has) is counted, not run.
list_classes() { # list_classes <project> <outfile> [runner filter...]: one full class name per line, from the runner's -list classes/json
  local proj=$1 out=$2; shift 2
  dotnet run --project "$proj" -c Debug --no-build -- -list classes/json "$@" > "$out.raw" 2>&1 || return 1
  python -c '
import json, sys
rows = [l for l in open(sys.argv[1], encoding="utf-8", errors="replace") if l.lstrip().startswith("[")]
names = json.loads(rows[0]) if len(rows) == 1 else []
open(sys.argv[2], "w", encoding="utf-8", newline="\n").write("".join(n + "\n" for n in names))
sys.exit(0 if names else 1)' "$out.raw" "$out"
}
declare -A sel_n=() miss_n=(); resolved=1; guard_out=""
for suite in darling lite; do
  if [ $suite = darling ]; then proj=$DARLING; keys=("${!want_d[@]}"); else proj=$LITE; keys=("${!want_l[@]}"); fi
  : > "$SCRATCH/keys-$suite.txt"; : > "$SCRATCH/selected-$suite.txt"; : > "$SCRATCH/unlisted-$suite.txt"
  sel_n[$suite]=0; miss_n[$suite]=0
  [ ${#keys[@]} -gt 0 ] || continue
  printf '%s\n' "${keys[@]}" | sort > "$SCRATCH/keys-$suite.txt"
  if ! list_classes "$proj" "$SCRATCH/listing-$suite.txt"; then
    if [ $DRY -eq 1 ]; then
      # no Debug build to list: the batches below are cut from the simple names, so their lengths run short
      resolved=0; cp "$SCRATCH/keys-$suite.txt" "$SCRATCH/selected-$suite.txt"
    else
      echo "  [classes-$suite] the class listing of $proj failed (log $SCRATCH/listing-$suite.txt.raw)"
      red+=("classes-$suite class listing failed (log $SCRATCH/listing-$suite.txt.raw)")
    fi
  else
    awk -v miss="$SCRATCH/unlisted-$suite.txt" '
      NR == FNR { want[$0] = 1; next }
      { k = $0; sub(/.*[.+\/]/, "", k); if (k in want) { print; hit[k] = 1 } }
      END { for (w in want) if (!(w in hit)) print w > miss }' "$SCRATCH/keys-$suite.txt" "$SCRATCH/listing-$suite.txt" \
      | sort -u > "$SCRATCH/selected-$suite.txt"
    # #5894: stage 2 ran every Stage=Guard class of the suite, so one named by a changed symbol must not run again
    : > "$SCRATCH/guardlist-$suite.txt"
    if list_classes "$proj" "$SCRATCH/guardlist-$suite.txt" -trait Stage=Guard && [ -s "$SCRATCH/guardlist-$suite.txt" ]; then
      grep -vxFf "$SCRATCH/guardlist-$suite.txt" "$SCRATCH/selected-$suite.txt" > "$SCRATCH/selected-$suite.nog" || true
      guard_out="$guard_out $suite $(( $(wc -l < "$SCRATCH/selected-$suite.txt") - $(wc -l < "$SCRATCH/selected-$suite.nog") ))"
      mv "$SCRATCH/selected-$suite.nog" "$SCRATCH/selected-$suite.txt"
    fi
  fi
  sel_n[$suite]=$(wc -l < "$SCRATCH/selected-$suite.txt" | tr -d ' ')
  miss_n[$suite]=$(wc -l < "$SCRATCH/unlisted-$suite.txt" | tr -d ' ')
done
[ -n "$guard_out" ] && echo "guard-stage classes left out of the selection (stage 2 ran them):$guard_out"
echo "classes: darling ${sel_n[darling]}, lite ${sel_n[lite]} (selected names: darling ${#want_d[@]}, lite ${#want_l[@]}; symbols: $nnames candidates; lists in $SCRATCH)"
if [ $resolved -eq 0 ]; then
  echo "classes: not resolved, no Debug build to list (simple names; build once for the real batches)"
elif [ $((miss_n[darling] + miss_n[lite])) -gt 0 ]; then
  echo "unlisted: darling ${miss_n[darling]}, lite ${miss_n[lite]} selected names are no test class of this build (e.g. $(cat "$SCRATCH/unlisted-darling.txt" "$SCRATCH/unlisted-lite.txt" | sort | head -n 5 | tr '\n' ' ' | cut -c1-160))"
fi

# #5868: the laptop never runs a full suite, so a selection the size of the suite is a STOP, never a GREEN.
stop=0
if [ "${sel_n[darling]}" -gt "$MAXC" ] || [ "${sel_n[lite]}" -gt "$MAXC" ]; then stop=1; fi

# batches cut by the length of their -class arguments, under the command-line limit (32,767 characters on Windows)
for suite in darling lite; do
  rm -f "$SCRATCH"/batch-$suite-*.txt
  [ -s "$SCRATCH/selected-$suite.txt" ] || continue
  awk -v budget="$BUDGET" -v pre="$SCRATCH/batch-$suite-" '
    BEGIN { b = 1 }
    { cost = length("-class") + 1 + length($0) + 1
      if (n > 0 && len + cost > budget) { close(pre b ".txt"); b++; len = 0; n = 0 }
      print > (pre b ".txt"); len += cost; n++ }' "$SCRATCH/selected-$suite.txt"
done
nbatch=0
run_batches() { # run_batches <suite> <project>: every batch of the suite, even after one fails
  local suite=$1 proj=$2 i nb f c len; local -a names args
  nb=$(ls "$SCRATCH"/batch-$suite-*.txt 2>/dev/null | wc -l | tr -d ' ')
  for ((i = 1; i <= nb; i++)); do
    f="$SCRATCH/batch-$suite-$i.txt"; mapfile -t names < "$f"; args=(); len=0
    for c in "${names[@]}"; do args+=(-class "$c"); len=$((len + 8 + ${#c})); done
    nbatch=$((nbatch + 1))
    if [ $DRY -eq 1 ]; then
      echo "  [classes-$suite] batch $i of $nb: ${#names[@]} classes, $len characters of -class arguments"
    else
      run_suite "classes-$suite-$i" "batch $i of $nb (${#names[@]} classes)" "$proj" "${args[@]}"
    fi
  done
}

stopline="GATE STOP: selection is the whole suite (darling ${sel_n[darling]}, lite ${sel_n[lite]}): run the brief's classes; CI decides"
if [ $DRY -eq 1 ]; then
  sed 's/^/  darling: /' "$SCRATCH/selected-darling.txt"
  sed 's/^/  lite: /' "$SCRATCH/selected-lite.txt"
  run_batches darling "$DARLING"; run_batches lite "$LITE"
  if [ $stop -eq 1 ]; then echo "$stopline (over PRE_PUSH_MAX_CLASSES=$MAXC in one project; dry run, nothing built or run)"; exit 3; fi
  echo "GATE DRY-RUN: nothing built or run"; exit 0
fi
# a red Guard stage or listing already decides the gate, and a STOP runs no selected class at all
if [ ${#red[@]} -eq 0 ] && [ $stop -eq 0 ]; then
  run_batches darling "$DARLING"; run_batches lite "$LITE"
fi

if [ ${#red[@]} -gt 0 ]; then echo "GATE RED: ${red[*]}"; exit 1; fi
if [ $stop -eq 1 ]; then echo "$stopline (over PRE_PUSH_MAX_CLASSES=$MAXC in one project; no selected class ran; logs $SCRATCH)"; exit 3; fi
echo "GATE GREEN: guard stage of both suites and ${sel_n[darling]}+${sel_n[lite]} selected classes in $nbatch batches passed (logs $SCRATCH)"; exit 0
