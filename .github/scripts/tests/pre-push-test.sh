#!/usr/bin/env bash
# pre-push-test.sh: behavior tests of .github/scripts/pre-push.sh (#5868). Run from the repository root:
#   bash .github/scripts/tests/pre-push-test.sh
# Each case copies the gate into a throwaway git repository, puts a fake `dotnet` first on PATH (it lists the classes
# it is told to, prints the Total it is told to, and records every call), hands the gate a test-map selection through
# PRE_PUSH_SELECTION, and checks the gate's last line, its exit code and the calls it made. No real build or test run.
# CI does not run this file; run it after any change to pre-push.sh. Needs bash, git and python. PRE_PUSH_TEST_GATE
# names another copy of the gate to test (to watch a reverted fix fail).
set -uo pipefail
src=$(git rev-parse --show-toplevel) || exit 1
work=$(mktemp -d "${TMPDIR:-/tmp}/pre-push-test.XXXXXX")
fails=0

# the fake dotnet: `build` passes; `run` answers -list from FAKE_LIST_<suite>, -trait with FAKE_GUARD_TOTAL and -class
# with FAKE_CLASS_TOTAL, and appends its argument list (after --) to FAKE_LOG
mkdir -p "$work/bin"
cat > "$work/bin/dotnet" <<'FAKE'
#!/usr/bin/env bash
[ "$1" = build ] && exit 0
suite=darling; case "$*" in *Lite.Tests*) suite=lite;; esac
while [ $# -gt 0 ] && [ "$1" != -- ]; do shift; done; shift
echo "$suite $*" >> "$FAKE_LOG"
case "$1" in
  -list) case "$*" in
           *"-trait Stage=Guard"*) g="FAKE_GUARD_LIST_${suite^^}"; [ -n "${!g:-}" ] && cat "${!g}";;
           *) if [ $suite = darling ]; then cat "$FAKE_LIST_DARLING"; else cat "$FAKE_LIST_LITE"; fi;;
         esac;;
  -trait) echo "  Fake.Tests  Total: ${FAKE_GUARD_TOTAL:-7}, Errors: 0, Failed: 0";;
  -class) echo "  Fake.Tests  Total: ${FAKE_CLASS_TOTAL:-3}, Errors: 0, Failed: 0";;
esac
exit 0
FAKE
chmod +x "$work/bin/dotnet"

# a repository with the gate, both test folders, and one commit past origin/dev that changes no .cs file
repo="$work/repo"
mkdir -p "$repo/.github/scripts" "$repo/Darling/Darling.Tests" "$repo/Lite.Tests"
cp "${PRE_PUSH_TEST_GATE:-$src/.github/scripts/pre-push.sh}" "$repo/.github/scripts/pre-push.sh"
cp "$src/.github/scripts/ci-select.py" "$repo/.github/scripts/"
echo 'namespace Darling.Tests; public class AlphaTests { }' > "$repo/Darling/Darling.Tests/AlphaTests.cs"
echo 'namespace Lite.Tests; public class BetaTests { }' > "$repo/Lite.Tests/BetaTests.cs"
g() { git -C "$repo" -c user.name=pre-push-test -c user.email=pre-push-test@example.invalid "$@"; }
g init -q && g add -A && g commit -qm base && g update-ref refs/remotes/origin/dev HEAD
echo change > "$repo/notes.txt"; g add notes.txt && g commit -qm change

# run_gate <case> <selection json> <darling listing json> <lite listing json> [args...]: the gate's output in $out, exit in $rc
run_gate() {
  local name=$1; shift
  printf '%s\n' "$1" > "$work/$name-selection.json"
  printf '%s\n' "$2" > "$work/$name-list-darling.json"
  printf '%s\n' "$3" > "$work/$name-list-lite.json"
  shift 3
  : > "$work/$name-calls.log"
  out=$(cd "$repo" && PATH="$work/bin:$PATH" FAKE_LOG="$work/$name-calls.log" \
    FAKE_LIST_DARLING="$work/$name-list-darling.json" FAKE_LIST_LITE="$work/$name-list-lite.json" \
    PRE_PUSH_SELECTION="$work/$name-selection.json" PRE_PUSH_CACHE="$work/cache" bash .github/scripts/pre-push.sh "$@" 2>&1)
  rc=$?
  calls="$work/$name-calls.log"
}
check() { # check <case> <description> <condition result 0/1>
  if [ "$3" -eq 0 ]; then echo "PASS $1: $2"; else echo "FAIL $1: $2"; fails=$((fails + 1)); fi
}
last() { printf '%s\n' "$out" | tail -n 1; }

sel_small='{"full": false, "selected": {"darling": ["AlphaTests", "NestedTests", "HelperOnly"], "lite": ["BetaTests"]}}'
list_d='["Darling.Tests.AlphaTests", "Darling.Tests.Outer+NestedTests", "Darling.Tests.UnselectedTests"]'
list_l='["Lite.Tests.BetaTests"]'

# 1. the defect: every selected-class batch reports Total: 0 and exits 0. The gate must be RED and name the batch.
FAKE_CLASS_TOTAL=0 run_gate zero "$sel_small" "$list_d" "$list_l" --skip-build
check zero "exit 1" $([ $rc -eq 1 ]; echo $?)
check zero "last line is GATE RED" $(last | grep -q '^GATE RED: '; echo $?)
check zero "names the batch and its class count" $(printf '%s\n' "$out" | grep -qF '[classes-darling-1] batch 1 of 1 (2 classes) ran 0 tests'; echo $?)

# 2. the control: the same selection with real totals is GREEN, and the runner gets full names, nested ones included
FAKE_CLASS_TOTAL=3 run_gate green "$sel_small" "$list_d" "$list_l" --skip-build
check green "exit 0 and GATE GREEN" $([ $rc -eq 0 ] && last | grep -q '^GATE GREEN: '; echo $?)
check green "darling batch is the two full names" $(grep -qxF 'darling -class Darling.Tests.AlphaTests -class Darling.Tests.Outer+NestedTests' "$calls"; echo $?)
check green "lite batch is the full name" $(grep -qxF 'lite -class Lite.Tests.BetaTests' "$calls"; echo $?)
check green "the helper class is counted as unlisted, not run" $(printf '%s\n' "$out" | grep -q '^unlisted: darling 1, lite 0 '; echo $?)

# 3. a Guard stage that matches nothing is RED too
FAKE_GUARD_TOTAL=0 run_gate guard0 "$sel_small" "$list_d" "$list_l" --skip-build
check guard0 "exit 1 and GATE RED naming Stage=Guard" $([ $rc -eq 1 ] && last | grep -q '^GATE RED: guard-darling Stage=Guard ran 0 tests'; echo $?)

# 3b. #5894: a class the symbol set or the map names that carries Stage=Guard already ran in the Guard stage, so the
# batches leave it out and the gate says how many; a listing with no Guard class (the fake's default) drops nothing
printf '%s
' '["Darling.Tests.AlphaTests"]' > "$work/guard-list-darling.json"
FAKE_GUARD_LIST_DARLING="$work/guard-list-darling.json" FAKE_CLASS_TOTAL=3 run_gate guardout "$sel_small" "$list_d" "$list_l" --skip-build
check guardout "GATE GREEN" $([ $rc -eq 0 ] && last | grep -q '^GATE GREEN: '; echo $?)
check guardout "the Guard class is not run again" $(grep -qxF 'darling -class Darling.Tests.Outer+NestedTests' "$calls"; echo $?)
check guardout "the gate says how many it left out" $(printf '%s
' "$out" | grep -qF 'guard-stage classes left out of the selection (stage 2 ran them): darling 1'; echo $?)

# 4. a whole-suite selection (401 darling classes over the 400 default) is a STOP: exit 3, no selected class runs
names=$(seq -f 'C%03gTests' 1 401)
sel_big="{\"full\": false, \"selected\": {\"darling\": [$(printf '"%s",' $names | sed 's/,$//')], \"lite\": [\"BetaTests\"]}}"
list_big="[$(printf '"Darling.Tests.%s",' $names | sed 's/,$//')]"
run_gate stop "$sel_big" "$list_big" "$list_l" --skip-build
check stop "exit 3" $([ $rc -eq 3 ]; echo $?)
check stop "last line is the GATE STOP line" $(last | grep -qF "GATE STOP: selection is the whole suite (darling 401, lite 1): run the brief's classes; CI decides"; echo $?)
check stop "no -class run" $(! grep -q -- '-class' "$calls"; echo $?)
check stop "the Guard stage still ran" $(grep -c -- '^[a-z]* -trait Stage=Guard' "$calls" | grep -qx 2; echo $?)

# 4b. CI's FULL answer (a keep-full file changed) is the whole suite too, even when the symbol set is a few classes: a STOP,
# never a GREEN on that partial set (#6336; #6309 reported GREEN on 146 classes while CI ran everything)
run_gate full '{"full": true, "selected": {}, "why": {}, "seconds": {}, "total": {}}' "$list_d" "$list_l" --skip-build
check full "exit 3" $([ $rc -eq 3 ]; echo $?)
check full "last line is the GATE STOP line naming CI's FULL answer" $(last | grep -qF "GATE STOP: selection is the whole suite (CI's map answers FULL)"; echo $?)
check full "no -class run" $(! grep -q -- '-class' "$calls"; echo $?)
check full "the Guard stage still ran" $(grep -c -- '^[a-z]* -trait Stage=Guard' "$calls" | grep -qx 2; echo $?)

# 5. batches are cut by length: 401 classes under a 2000-character budget run in several batches, each under it
FAKE_CLASS_TOTAL=3 PRE_PUSH_MAX_CLASSES=1000 PRE_PUSH_ARG_BUDGET=2000 run_gate chunks "$sel_big" "$list_big" "$list_l" --skip-build
nb=$(grep -c '^darling -class ' "$calls")
longest=$(grep '^darling -class ' "$calls" | sed 's/^darling //' | awk '{ if (length($0) + 1 > m) m = length($0) + 1 } END { print m + 0 }')
ran=$(grep '^darling -class ' "$calls" | grep -o -- '-class ' | wc -l | tr -d ' ')
check chunks "GATE GREEN" $([ $rc -eq 0 ] && last | grep -q '^GATE GREEN: '; echo $?)
check chunks "several darling batches ($nb), longest $longest characters, at most 2000" $([ "$nb" -gt 1 ] && [ "$longest" -le 2000 ]; echo $?)
check chunks "every class ran once ($ran of 401)" $([ "$ran" -eq 401 ]; echo $?)

rm -rf "$work"
if [ $fails -gt 0 ]; then echo "pre-push-test: $fails FAILED"; exit 1; fi
echo "pre-push-test: all passed"
