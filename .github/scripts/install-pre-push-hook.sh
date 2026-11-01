#!/usr/bin/env bash
# install-pre-push-hook.sh: wires .github/scripts/pre-push.sh into this clone as its git pre-push hook (#7101).
#   bash .github/scripts/install-pre-push-hook.sh             install (running it again is harmless)
#   bash .github/scripts/install-pre-push-hook.sh --remove    remove the hook this script installed
# Opt-in only: nothing runs this for you, because the gate builds and tests on the machine that pushes.
# Hooks live in the common git directory, so one install covers the clone and every linked worktree of it. The hook
# runs the pre-push.sh of the worktree you push from, and does nothing in a checkout that has none. It skips a push
# that only deletes branches. A GATE STOP (exit 3: the selection is the whole suite, which CI runs) does not block the
# push; GATE RED does. An existing pre-push hook this script did not write is left alone: it says so and exits 1.
set -uo pipefail
mode=install
case "${1:-}" in "") ;; --remove) mode=remove;; *) echo "usage: install-pre-push-hook.sh [--remove]"; exit 2;; esac
root=$(git rev-parse --show-toplevel 2>/dev/null) || { echo "not inside a git worktree"; exit 1; }
cd "$root" || exit 1
# git-path honors core.hooksPath and resolves to the common git directory from a linked worktree; it can be relative
hooks=$(git rev-parse --git-path hooks) || { echo "cannot find the hooks directory"; exit 1; }
mkdir -p "$hooks" || { echo "cannot create $hooks"; exit 1; }
hooks=$(cd "$hooks" && pwd) || exit 1
hook="$hooks/pre-push"
marker='# installed by .github/scripts/install-pre-push-hook.sh'

if [ -e "$hook" ] && ! grep -qF "$marker" "$hook"; then
  echo "$hook already exists and was not installed by this script. Left alone."
  echo "Move it aside or add a call to .github/scripts/pre-push.sh to it by hand, then run this again."
  exit 1
fi

if [ "$mode" = remove ]; then
  if [ -e "$hook" ]; then rm -f "$hook" && echo "removed $hook"; else echo "no pre-push hook at $hook; nothing to remove"; fi
  exit 0
fi

new=$(mktemp "${TMPDIR:-/tmp}/pre-push-hook.XXXXXX") || exit 1
trap 'rm -f "$new"' EXIT
cat > "$new" <<'HOOK'
#!/usr/bin/env bash
# installed by .github/scripts/install-pre-push-hook.sh (#7101). Remove it: bash .github/scripts/install-pre-push-hook.sh --remove
# Skip it for one push by giving git push the flag that skips hooks.
only_deletes=1
while read -r _ local_sha _ _; do
  [ -n "$local_sha" ] && [ -n "${local_sha//0/}" ] && only_deletes=0
done
[ $only_deletes -eq 1 ] && exit 0
root=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
[ -f "$root/.github/scripts/pre-push.sh" ] || exit 0
cd "$root" || exit 0
bash .github/scripts/pre-push.sh
rc=$?
if [ $rc -eq 3 ]; then echo "pre-push hook: GATE STOP does not block the push; CI runs the rest"; exit 0; fi
exit $rc
HOOK
if [ -e "$hook" ] && cmp -s "$new" "$hook"; then
  chmod +x "$hook"; echo "already installed at $hook"; exit 0
fi
cp "$new" "$hook" && chmod +x "$hook" && echo "installed $hook"
