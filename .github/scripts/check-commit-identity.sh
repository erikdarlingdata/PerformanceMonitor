#!/usr/bin/env bash
# Fail a pull request that carries a commit not authored AND committed under an allowlisted
# identity (#2891).
#
# Commit metadata is effectively permanent. Once a commit is on dev, changing its author needs a
# history rewrite that invalidates every existing clone and every open pull request's merge base,
# so this is a class where prevention is the only affordable control -- there is no cheap "fix it
# afterwards" path to fall back on. That asymmetry drives two choices below: the check fails
# closed, and it refuses to report a verdict it could not actually cover.
#
# Reads the commit list from the API rather than from a checkout of the pull request, because the
# calling workflow runs on pull_request_target and deliberately never checks out pull request head.
set -euo pipefail

REPO=${REPO:-${1:-}}
PR=${PR:-${2:-}}

if [ -z "$REPO" ] || [ -z "$PR" ]; then
  echo "::error::check-commit-identity.sh needs REPO (owner/name) and PR (number), by environment or as \$1 \$2." >&2
  exit 2
fi

# The allowlist. Pinned addresses, NOT a *@users.noreply.github.com pattern: that pattern admits
# any GitHub account's noreply address, including one belonging to a different identity, which is
# the exact failure this check exists to stop.
#
#   1. The maintainer's noreply address -- 1437 of dev's 1448 commits are authored by it.
#   2/3. GitHub's own committer, for merge commits it creates and for web-UI edits. This repo's
#      history uses noreply@github.com for all of them (426 commits, including all 346
#      GitHub-created merge commits); web-flow@github.com, the older form of the same thing, does
#      not appear here even once but is allowlisted anyway because it costs nothing and GitHub
#      still emits it on some paths. Without these two, the check fails on every merge commit and
#      every squash merge.
#   4. Dependabot, whose numeric prefix is its real global account id (verified against
#      /users/dependabot[bot]). .github/dependabot.yml has it opening grouped PRs against dev
#      weekly, and it has authored commits that merged as recently as 2026-08-31 -- omitting it
#      would turn this check into a self-inflicted red every Monday.
ALLOWED_EMAILS=$(cat <<'EOF'
2136037+erikdarlingdata@users.noreply.github.com
noreply@github.com
web-flow@github.com
49699333+dependabot[bot]@users.noreply.github.com
EOF
)

# Lower-cased on both sides of the comparison. Git preserves whatever case was configured, and the
# same account spelled with different capitalisation is still the same account -- a case-sensitive
# match would turn that into a false red without making the allowlist any narrower.
allow_json=$(printf '%s\n' "$ALLOWED_EMAILS" |
  jq -R -s 'split("\n") | map(select(length > 0) | ascii_downcase)')

# The per-commit comparison, done in jq rather than in shell text processing: the runner is
# ubuntu-latest and this gets developed on macOS, and BSD vs GNU sed/awk differences silently
# no-op rather than fail (a prior change here lost a substitution exactly that way).
#
# Emits one line per violating FIELD, not per commit, because author and committer are independent
# -- a rebase or an amend routinely leaves one correct and the other not, and a message that named
# only the commit would send you looking at the wrong half.
#
# It deliberately does NOT print the offending address. Actions logs on a public repo are public,
# so echoing it would publish the very identity this check exists to keep out of the history. The
# short SHA plus the field name is enough to look it up locally, which the remediation shows how
# to do. A missing or empty email renders as (unset) and counts as a violation: absent is not
# conforming.
FILTER='
  .[]
  | .sha[0:8] as $short
  | [ {field: "author",    email: (.commit.author.email    // "")},
      {field: "committer", email: (.commit.committer.email // "")} ][]
  | (.email | ascii_downcase) as $e
  | select(($allow | index($e)) == null)
  | "\($short)\t\(if .email == "" then "(unset) " else "" end)\(.field)"
'

# Pagination. The default page size is 30, so an unpaginated read of a long pull request would
# silently pass on its unread tail -- the same trust-a-truncated-result shape this repo has been
# bitten by elsewhere. per_page=100 is the endpoint's maximum; MAX_PAGES is only a runaway guard,
# and the coverage assertion after the loop is what actually guarantees completeness.
PER_PAGE=100
MAX_PAGES=10
page=1
pages_read=0
read_count=0
violations=''
seen_shas=''

while [ "$page" -le "$MAX_PAGES" ]; do
  # No retry and no soft-fail on an API error: under set -e a failed call ends the script red.
  # That is deliberate here. A false red costs one re-run; a false green is permanent, because the
  # commit it waved through cannot be fixed afterwards without a history rewrite. This is the
  # opposite of the review guard's lookup(), which degrades to a warning -- that guard reports on
  # something already done, this one is the gate.
  page_json=$(gh api -H 'Accept: application/vnd.github+json' \
    "repos/${REPO}/pulls/${PR}/commits?per_page=${PER_PAGE}&page=${page}")

  n=$(printf '%s' "$page_json" | jq 'length')
  [ "$n" -eq 0 ] && break

  pages_read=$((pages_read + 1))
  read_count=$((read_count + n))
  seen_shas=${seen_shas:+$seen_shas$'\n'}$(printf '%s' "$page_json" | jq -r '.[].sha')
  page_violations=$(printf '%s' "$page_json" | jq -r --argjson allow "$allow_json" "$FILTER")
  if [ -n "$page_violations" ]; then
    violations=${violations:+$violations$'\n'}$page_violations
  fi

  [ "$n" -lt "$PER_PAGE" ] && break
  page=$((page + 1))
done

# Coverage assertion. /pulls/{n}/commits caps at 250 regardless of paging, and this repo really
# does have pull requests above that line (several in the 300-900 range), so the cap is NOT
# hypothetical here. Reading fewer commits than the pull request declares means the check cannot
# make its guarantee, and a preventive gate that cannot cover its subject must say so rather than
# report a pass over the part it read.
declared=$(gh api -H 'Accept: application/vnd.github+json' "repos/${REPO}/pulls/${PR}" --jq '.commits')

if [ "$read_count" -lt "$declared" ]; then
  echo "::error::Commit identity could not be checked on all of this pull request: it declares" \
    "${declared} commits and the API returned ${read_count} (that endpoint caps at 250). The" \
    "unread tail is unverified, so this is a coverage failure, not an identity verdict. Split the" \
    "pull request, or verify by hand with:" \
    "git log --format='%h %ae %ce' origin/${BASE_REF:-dev}..HEAD"
  exit 1
fi

# Anchor what was read to the commit this event is actually about. `declared` and the commit list
# come from two endpoints in the same family, so a stale post-push view could leave them AGREEING
# with each other while both describe the PREVIOUS head -- and the coverage assertion above only
# compares them to each other, so it cannot see that. Requiring the event's head SHA to appear in
# what was actually read is the independent check, and it fails closed: if the API has not caught
# up, the anchor is missing and this goes red, which a re-run clears. Skipped when HEAD_SHA is not
# supplied (running this by hand against an arbitrary pull request), because there is no event to
# anchor to then. Review catch on the pull request that added this check.
if [ -n "${HEAD_SHA:-}" ]; then
  head_short=$(printf '%s' "$HEAD_SHA" | cut -c1-8)
  if ! printf '%s\n' "$seen_shas" | grep -qxF "$HEAD_SHA"; then
    echo "::error::Commit identity read ${read_count} commit(s), none of which is this event's head" \
      "commit (${head_short}). The API is most likely still serving a pre-push view of this pull" \
      "request, which would make any verdict here describe the wrong commits, so none is reported." \
      "Re-run this job."
    exit 1
  fi
fi

if [ -n "$violations" ]; then
  printf '%s\n' "$violations" | while IFS=$'\t' read -r short field; do
    echo "::error::Commit ${short} has a non-allowlisted ${field} email. Commit metadata cannot be" \
      "corrected after merge without a history rewrite, so it has to be fixed on this branch first."
  done

  cat >&2 <<'REMEDY'
To see what the offending commits actually carry (locally, not in this log):

  git log --format='%h %an <%ae> | %cn <%ce>' origin/dev..HEAD

To rewrite this branch's commits onto the intended identity and re-push:

  git config user.email "2136037+erikdarlingdata@users.noreply.github.com"
  git rebase --exec 'git commit --amend --no-edit --reset-author' origin/dev
  git push --force-with-lease

--reset-author sets BOTH the author and the committer, which is what this check needs: they are
separate fields and fixing only one leaves the check red.
REMEDY

  count=$(printf '%s\n' "$violations" | grep -c . || true)
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "- Commit identity: **${count} non-allowlisted field(s)** across ${read_count} commit(s)."
      echo "  See the step log for the short SHAs; the addresses are deliberately not printed."
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  exit 1
fi

echo "Commit identity OK: ${read_count} commit(s) over ${pages_read} page(s)${HEAD_SHA:+, anchored on the event head commit}, author and committer both allowlisted on every one."
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "- Commit identity: ${read_count} commit(s) checked, author and committer allowlisted on every one." >> "$GITHUB_STEP_SUMMARY"
fi
