#!/usr/bin/env bash
# install-pre-push-hook-test.sh: behavior tests of .github/scripts/install-pre-push-hook.sh (#7101). Run from the
# repository root:
#   bash .github/scripts/tests/install-pre-push-hook-test.sh
# Each case installs the hook into a throwaway clone, with a stand-in pre-push.sh that records its call and exits with the
# code it is told to, then pushes to a throwaway bare repository. CI does not run this file; run it after any change to
# the installer. Needs bash and git.
set -uo pipefail
src=$(git rev-parse --show-toplevel) || exit 1
work=$(mktemp -d "${TMPDIR:-/tmp}/install-hook-test.XXXXXX")
fails=0
check() { # check <case> <description> <condition result 0/1>
  if [ "$3" -eq 0 ]; then echo "PASS $1: $2"; else echo "FAIL $1: $2"; fails=$((fails + 1)); fi
}
g() { git -c user.name=hook-test -c user.email=hook-test@example.invalid "$@"; }

git init -q --bare "$work/remote.git"
repo="$work/repo"
mkdir -p "$repo/.github/scripts"
cp "$src/.github/scripts/install-pre-push-hook.sh" "$repo/.github/scripts/"
# the stand-in gate: records the directory it ran in, exits with the code in $work/gate-rc
cat > "$repo/.github/scripts/pre-push.sh" <<'GATE'
#!/usr/bin/env bash
pwd >> "$HOOK_TEST_LOG"
exit "$(cat "$HOOK_TEST_RC")"
GATE
g -C "$repo" init -q -b main && g -C "$repo" add -A && g -C "$repo" commit -qm base
g -C "$repo" remote add origin "$work/remote.git"
export HOOK_TEST_LOG="$work/gate.log" HOOK_TEST_RC="$work/gate-rc"
install() { out=$(cd "$1" && bash .github/scripts/install-pre-push-hook.sh "${@:2}" 2>&1); rc=$?; }
hook="$repo/.git/hooks/pre-push"

# 1. a fresh install writes an executable hook; a second run changes nothing and says so
install "$repo"
check fresh "exit 0 and the hook exists" $([ $rc -eq 0 ] && [ -x "$hook" ]; echo $?)
sum=$(cksum < "$hook")
install "$repo"
check twice "second run exits 0, says already installed, leaves the file as it was" $([ $rc -eq 0 ] && echo "$out" | grep -q 'already installed' && [ "$sum" = "$(cksum < "$hook")" ]; echo $?)

# 2. the hook runs the gate from the worktree root on a push, and a red gate blocks the push
echo 0 > "$HOOK_TEST_RC"; : > "$HOOK_TEST_LOG"
g -C "$repo" push -q origin main > /dev/null 2>&1; prc=$?
check green "a green gate lets the push through, and the gate ran in the worktree root" $([ $prc -eq 0 ] && [ "$(wc -l < "$HOOK_TEST_LOG" | tr -d ' ')" = 1 ]; echo $?)
echo change > "$repo/a.txt"; g -C "$repo" add a.txt; g -C "$repo" commit -qm a
echo 1 > "$HOOK_TEST_RC"
g -C "$repo" push -q origin main > /dev/null 2>&1; prc=$?
check red "a red gate (exit 1) blocks the push" $([ $prc -ne 0 ]; echo $?)
echo 3 > "$HOOK_TEST_RC"
g -C "$repo" push -q origin main > /dev/null 2>&1; prc=$?
check stop "GATE STOP (exit 3) does not block the push" $([ $prc -eq 0 ]; echo $?)

# 3. a push that only deletes a branch does not run the gate
g -C "$repo" push -q origin main:topic > /dev/null 2>&1
echo 1 > "$HOOK_TEST_RC"; : > "$HOOK_TEST_LOG"
g -C "$repo" push -q origin :topic > /dev/null 2>&1; prc=$?
check delete "a delete-only push passes and the gate did not run" $([ $prc -eq 0 ] && [ ! -s "$HOOK_TEST_LOG" ]; echo $?)

# 4. a linked worktree installs into (and finds) the clone's own hook, and the hook runs the worktree's gate
g -C "$repo" worktree add -q -b wt "$work/wt" main
rm -f "$hook"
install "$work/wt"
check worktree "install from a linked worktree writes the common hooks directory" $([ $rc -eq 0 ] && [ -x "$hook" ] && [ ! -e "$work/wt/.git/hooks" ]; echo $?)
echo 0 > "$HOOK_TEST_RC"; : > "$HOOK_TEST_LOG"; echo w > "$work/wt/w.txt"; g -C "$work/wt" add w.txt; g -C "$work/wt" commit -qm w
g -C "$work/wt" push -q origin wt > /dev/null 2>&1
check worktreegate "pushing from the linked worktree runs the gate in that worktree" $(grep -q 'wt$' "$HOOK_TEST_LOG"; echo $?)

# 5. a hook this script did not write is left alone, with a message and a failing exit
rm -f "$hook"; printf '#!/bin/sh\necho mine\n' > "$hook"; chmod +x "$hook"
sum=$(cksum < "$hook")
install "$repo"
check foreign "exit 1, names the hook, file untouched" $([ $rc -eq 1 ] && echo "$out" | grep -q 'not installed by this script' && [ "$sum" = "$(cksum < "$hook")" ]; echo $?)
install "$repo" --remove
check foreignremove "--remove leaves a foreign hook alone too" $([ $rc -eq 1 ] && [ -e "$hook" ]; echo $?)

# 6. --remove deletes the hook this script wrote, and is harmless when there is none
rm -f "$hook"; install "$repo"; install "$repo" --remove
check remove "--remove deletes our hook" $([ $rc -eq 0 ] && [ ! -e "$hook" ]; echo $?)
install "$repo" --remove
check removetwice "--remove with no hook exits 0" $([ $rc -eq 0 ]; echo $?)

# 7. a core.hooksPath is honored, relative paths included
g -C "$repo" config core.hooksPath .githooks
install "$repo"
check hookspath "the hook lands in the configured hooks directory" $([ $rc -eq 0 ] && [ -x "$repo/.githooks/pre-push" ]; echo $?)

rm -rf "$work"
if [ $fails -gt 0 ]; then echo "install-pre-push-hook-test: $fails FAILED"; exit 1; fi
echo "install-pre-push-hook-test: all passed"
