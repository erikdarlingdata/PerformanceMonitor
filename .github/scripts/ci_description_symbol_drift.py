#!/usr/bin/env python3
"""Warn when a pull request description is not about its own diff (#2978).

Nothing else in CI reads the prose beside the code, and two near-misses in one session were
both that drift: a description describing a fix the merge had left behind, and a PR that
shipped only the fixture for a behaviour change its description claimed to make.

WHAT THIS DOES AND DOES NOT CATCH, measured over 110 real merged PRs from this repo:

  the naive form of this idea -- every backticked span, substring-matched against the diff --
  flags 97% of them (median 10 unmatched spans per PR). It is unusable here, because the house
  style situates a change by naming code around it: sibling readers, the pin family it joins,
  the constant it does not touch, the file a mutation was planted in and restored.

  So the verdict is NOT "some named symbol is absent" (31% of real PRs, still unusable). It is
  "NOTHING the description names is anywhere in the diff" -- the description is not about this
  change at all. That fires on 2.7% of real merged PRs (3 of 110), catches 65% of body/diff
  mismatches and 85% of effectively-empty diffs.

  It is close to BLIND to partial drift, which is what both motivating near-misses were: with a
  quarter of a PR's files removed it fires on 15% against a 3% floor. A rule that catches those
  reliably does not exist at a tolerable flag rate -- see the issue. This check earns its place
  on the wholesale case only, and its annotation says so rather than implying broader cover.

Advisory, always exit 0, and it must never be marked a required check.
"""
import argparse, json, os, re, sys

# ---------------------------------------------------------------- body preprocessing
FENCE = re.compile(r'(?ms)^[ \t]*(```|~~~).*?^[ \t]*\1[ \t]*$')
FENCE_UNTERMINATED = re.compile(r'(?ms)^[ \t]*(```|~~~).*\Z')
HTML_COMMENT = re.compile(r'(?s)<!--.*?-->')
QUOTE_LINE = re.compile(r'(?m)^[ \t]*>.*$')
# Tables in this repo's bodies are measurement tables -- per-site counts, red-first mutation
# rows -- and their cells name the files a mutation was planted in and then restored, i.e.
# symbols absent from the diff by construction.
TABLE_LINE = re.compile(r'(?m)^[ \t]*\|.*$')
HEADING = re.compile(r'(?m)^(#{1,6})[ \t]+(.+?)[ \t]*$')

# A heading announcing content about what the diff deliberately does NOT contain, or about how
# the change was proven rather than what it is. Dropping these sections is the single biggest
# win available: leaving them in takes the flag rate from 5% to 98% on the same corpus, because
# "Not verified" (11 of 40 recent bodies), "Red-first", "Not changed", "What deliberately did
# NOT change" and "No existing pin was disturbed" exist precisely to name absent things.
ABSENT_HEADING = re.compile(
    r"(?ix)"
    r"\bnot\b | n't\b | \bnever\b | \bno\b | \bnone\b |"
    r"unchanged | untouched | undisturbed | disturb | unaffected | unrelated |"
    r"deliberate | intentional | on \s+ purpose | by \s+ design |"
    r"left \s+ (alone|as|in) | \bstays?\b | remain |"
    r"verif | tested | testing | \btest\b | proof | proven | prove |"
    r"red[\s-]?first | mutation | mutant | measur | probe | control |"
    r"out \s+ of \s+ scope | follow[\s-]?up | future | later | deferred |"
    r"checklist | caveat | caution | risk | limitation |"
    r"prior \s+ art | background | history | historical | context |"
    r"alternativ | rejected | considered"
)

# How far past the lead prose to read. The claim about what a PR does lives in its opening
# paragraphs and its first substantive section; every section after that is elaboration, and
# elaboration is where absent symbols cluster. Measured on 110 merged PRs: lead alone abstains
# on 14 of 40 bodies for want of any symbol, lead+1 abstains on 1 and scores the same power,
# lead+all drops power from 48% to 34% by giving accidental matches more chances to land.
AFFIRMATIVE_SECTIONS = 1


def strip_noise(body):
    text = body or ''
    text = HTML_COMMENT.sub('\n', text)
    text = FENCE.sub('\n', text)
    text = FENCE_UNTERMINATED.sub('\n', text)   # an unclosed fence swallows the rest by design
    text = QUOTE_LINE.sub('', text)             # quoted review text is someone else's words
    text = TABLE_LINE.sub('', text)
    return text


def claim_scope(body, sections=AFFIRMATIVE_SECTIONS):
    """The part of a body that asserts what THIS diff does."""
    text = strip_noise(body)
    marks = list(HEADING.finditer(text))
    if not marks:
        return text
    kept = [text[:marks[0].start()]]            # lead prose always asserts
    taken = 0
    for i, mark in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        if ABSENT_HEADING.search(mark.group(2)):
            continue
        if taken >= sections:
            break
        kept.append(text[mark.end():end])
        taken += 1
    return '\n'.join(kept)


# ---------------------------------------------------------------- what counts as a symbol
BACKTICK = re.compile(r'``([^`]+)``|`([^`\n]+)`')
SOURCE_EXT = ('cs', 'csproj', 'sql', 'xaml', 'json', 'props', 'targets', 'sln', 'ps1', 'sh',
              'yml', 'yaml', 'md', 'cff', 'config', 'py')
HUMP = re.compile(r'[a-z0-9][A-Z]')
IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
MEMBER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$')
FILENAME = re.compile(r'^(?:[A-Za-z0-9_.+-]+/)*([A-Za-z0-9_.+-]+)\.([A-Za-z0-9]+)$')
TRAILING_LINENO = re.compile(r':\d+(?:-\d+)?$')
# Any of these means the span is not a bare symbol: a regex, a build command, an attribute, a
# glob, an assignment, an HTML/generic type, a shell flag. All appear backticked in real bodies.
REJECT_CHARS = set(' \t=*[]<>\\#%$|+?{}@"\'!;,~^&()')


def classify(span):
    """('file', name) for a source file, ('ident', full, member) for code, or None."""
    text = TRAILING_LINENO.sub('', span.strip()).rstrip('.,;:')
    if text.endswith('()'):                     # a method named as a call
        text = text[:-2]
    if not text or (REJECT_CHARS & set(text)):
        return None
    if text[0] in '/-':                         # /api/ping is a route literal; -flag is a switch
        return None
    named_file = FILENAME.match(text)
    if named_file and named_file.group(2).lower() in SOURCE_EXT:
        return ('file', named_file.group(1) + '.' + named_file.group(2))
    if '/' in text:
        return None
    if text.isupper():                          # wait types, env vars, HEAD, SQL keywords
        return None
    if MEMBER.match(text):
        parts = text.split('.')
        if any(p.isupper() or p.isdigit() for p in parts):
            return None
        # No component carries a case hump: a config or other-language key, not a symbol of
        # ours. core.autocrlf and shutil.copy2 were both false positives before this line.
        if not any(HUMP.search(p) for p in parts):
            return None
        return ('ident', text, parts[-1])
    if IDENT.match(text):
        # A single lowercase word -- dev, main, patch, true, net10 -- is prose, not a symbol.
        if not HUMP.search(text):
            return None
        return ('ident', text, text)
    return None


def described_symbols(body, sections=AFFIRMATIVE_SECTIONS):
    found = {}
    for match in BACKTICK.finditer(claim_scope(body, sections)):
        span = (match.group(1) or match.group(2)).strip()
        kind = classify(span)
        if kind:
            found.setdefault(kind, span)
    return found


# ---------------------------------------------------------------- what the diff touches
TOKEN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
CODE_EXT = ('.cs', '.sql', '.xaml', '.ps1', '.sh', '.py', '.csproj', '.props', '.targets',
            '.yml', '.yaml')


def diff_symbols(files):
    paths, basenames, idents = set(), set(), set()
    for entry in files:
        for name in (entry.get('filename'), entry.get('previous_filename')):
            if not name:
                continue
            paths.add(name.lower())
            basenames.add(name.rsplit('/', 1)[-1].lower())
            idents.update(TOKEN.findall(name.rsplit('/', 1)[-1]))
        for line in (entry.get('patch') or '').splitlines():
            if line.startswith('@@'):
                # The hunk header carries git's enclosing-scope hint -- the method or class the
                # change sits inside, which is very often exactly what the prose names.
                idents.update(TOKEN.findall(line))
            elif line[:1] in ('+', '-'):
                idents.update(TOKEN.findall(line[1:]))
            else:
                # Context lines count. A description saying "`Foo` now calls `Bar`" names Foo,
                # whose signature is context around the edit; without these, flags rise by 4pp
                # of pure noise.
                idents.update(TOKEN.findall(line))
    return paths, basenames, idents


def assess(body, files):
    """-> (verdict, missing, hit) where verdict is 'warn', 'clear' or 'abstain'."""
    if not files or not any((f.get('filename') or '').lower().endswith(CODE_EXT) for f in files):
        # A docs-or-config-only diff contributes no identifiers to intersect against, so any
        # verdict would be an artifact of the diff having nothing to say.
        return 'abstain', [], []
    paths, basenames, idents = diff_symbols(files)
    hit, missing = [], []
    for kind, span in described_symbols(body).items():
        if kind[0] == 'file':
            name = kind[1].lower()
            found = name in basenames or any(name in p for p in paths)
        else:
            _, full, member = kind
            found = member in idents or full.split('.')[0] in idents
        (hit if found else missing).append(span)
    if not hit and not missing:
        return 'abstain', [], []
    if hit:
        return 'clear', sorted(set(missing)), sorted(set(hit))
    return 'warn', sorted(set(missing)), []


# ---------------------------------------------------------------- self-test
FIXTURE_DIFF = [
    {'filename': 'Darling/PerformanceMonitor.Darling.Service/WidgetReader.cs',
     'patch': '@@ -10,6 +10,7 @@ public async Task ReadWidgetsAsync(\n'
              '     var command = connection.CreateCommand();\n'
              '+    command.CommandTimeout = StoreDeadlines.WidgetSeconds;\n'
              '     return await command.ExecuteReaderAsync();'},
]


def self_test():
    cases = []

    def check(name, condition):
        cases.append((name, bool(condition)))

    # The scoped claim naming a symbol in the diff is the normal, silent case.
    check('clear when the lead names a symbol in the diff',
          assess('`WidgetReader` now sets a deadline.', FIXTURE_DIFF)[0] == 'clear')
    # Nothing named is anywhere in the diff: the whole point of the check.
    check('warn when nothing named is in the diff',
          assess('`SprocketCache` now evicts on write.', FIXTURE_DIFF)[0] == 'warn')
    # One hit is enough to clear, even beside many misses -- this is the rule that takes the
    # flag rate from 31% to 2.7%, so it is pinned explicitly.
    check('one hit clears despite many misses',
          assess('`WidgetReader` mirrors `SprocketCache`, `GadgetPool` and `DoodadStore`.',
                 FIXTURE_DIFF)[0] == 'clear')
    # Absent-by-design sections must not contribute symbols.
    check('absent-heading sections are not read',
          assess('`WidgetReader` gains a deadline.\n\n## Not verified\n\n`SprocketCache` was '
                 'not exercised.\n\n## Red-first\n\n`GadgetPool.cs` was mutated and restored.',
                 FIXTURE_DIFF)[0] == 'clear')
    check('a body of only absent-heading sections abstains',
          assess('## Not changed\n\n`SprocketCache` is byte-identical.', FIXTURE_DIFF)[0]
          == 'abstain')
    # AFFIRMATIVE_SECTIONS is a tuned window, not a free parameter: widening it costs power
    # (48% -> 34%) by giving accidental matches more chances to land, so pin how far it reads.
    # Without this, changing the constant moves the flag rate and no assertion notices.
    check('only the first affirmative section is read',
          not any(span == 'SprocketCache' for span in described_symbols(
              '`WidgetReader` changes.\n\n## The fix\n\n`StoreDeadlines` moves.\n\n'
              '## The shape\n\n`SprocketCache` is the sibling.').values()))
    # Fenced code, blockquotes and measurement tables are not claims.
    for label, body in (('fenced code', '```\n`SprocketCache`\n```\n'),
                        ('blockquote', '> `SprocketCache` per review\n'),
                        ('table row', '| site | `SprocketCache` |\n')):
        check(f'{label} contributes no symbols', not described_symbols(body))
    # Shape rejections, each observed backticked in a real body.
    for span in ('SOS_SCHEDULER_YIELD', 'HEAD', 'net10.0', '[Theory]', 'MOVED=0',
                 r'\.CommandTimeout\s*=', 'dotnet build -c Debug', '/api/ping',
                 '*CommandTimeoutTests.cs', 'core.autocrlf', 'dev', '0/8'):
        check(f'rejected as a symbol: {span}', classify(span) is None)
    # Shape acceptances.
    for span in ('WidgetReader', 'StoreDeadlines.WidgetSeconds', 'ReadWidgetsAsync()',
                 'WidgetReader.cs', 'WidgetReader.cs:88'):
        check(f'accepted as a symbol: {span}', classify(span) is not None)
    # A line citation resolves to its file rather than being read as an identifier.
    check('line-cited file resolves to the file', classify('WidgetReader.cs:88') ==
          ('file', 'WidgetReader.cs'))
    # Docs-only diffs cannot support a verdict.
    check('docs-only diff abstains',
          assess('`SprocketCache` now evicts.',
                 [{'filename': 'CHANGELOG.md', 'patch': '@@ -1 +1 @@\n-a\n+b'}])[0] == 'abstain')
    check('empty body abstains', assess('', FIXTURE_DIFF)[0] == 'abstain')

    failed = [n for n, ok in cases if not ok]
    for name, ok in cases:
        print(f'  {"ok  " if ok else "FAIL"}  {name}')
    print(f'\n{len(cases) - len(failed)}/{len(cases)} self-test assertions passed')
    return 1 if failed else 0


# ---------------------------------------------------------------- corpus measurement
def measure(corpus_dir):
    """Re-derive the flag rate over fetched real PR bodies, so the number in the docstring is
    checkable rather than remembered. Expects <dir>/prs.json and <dir>/files/<number>.json as
    written by `gh pr list --json number,body` and `gh api .../pulls/<n>/files`."""
    prs = json.load(open(os.path.join(corpus_dir, 'prs.json')))
    counts = {'warn': 0, 'clear': 0, 'abstain': 0}
    flagged = []
    for pr in prs:
        path = os.path.join(corpus_dir, 'files', f"{pr['number']}.json")
        if not os.path.exists(path):
            continue
        verdict, missing, _ = assess(pr.get('body') or '', json.load(open(path)))
        counts[verdict] += 1
        if verdict == 'warn':
            flagged.append((pr['number'], missing))
    total = sum(counts.values())
    print(f'{total} PRs: warn {counts["warn"]} ({100.0 * counts["warn"] / max(1, total):.1f}%), '
          f'clear {counts["clear"]}, abstain {counts["abstain"]}')
    for number, missing in flagged:
        print(f'  warn #{number}: {", ".join(missing)}')
    return 0


# ---------------------------------------------------------------- entry point
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--corpus', metavar='DIR')
    parser.add_argument('--files', metavar='JSON',
                        help='changed files as returned by /pulls/<n>/files')
    parser.add_argument('--summary', metavar='PATH', help='append a report here')
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if args.corpus:
        return measure(args.corpus)

    # The body arrives through the environment, never through a command line or a workflow
    # expression: it is author-controlled text on a public repo, and interpolating it into a
    # shell would be a script-injection seam.
    body = os.environ.get('PR_BODY', '')
    files = json.load(open(args.files)) if args.files else []
    if isinstance(files, dict):
        files = files.get('files', [])
    # A page-per-element array is what `gh api --paginate --slurp` hands back for this endpoint.
    # The workflow flattens before writing, but tolerate the nested shape so a future caller
    # that does use --slurp gets a verdict rather than a silent zero-file abstain.
    if files and isinstance(files[0], list):
        files = [entry for page in files for entry in page]

    verdict, missing, hit = assess(body, files)
    print(f'verdict: {verdict} (named-and-present {len(hit)}, named-and-absent {len(missing)})')
    lines = []
    if verdict == 'warn':
        named = ', '.join(f'`{m}`' for m in missing)
        print(f'::warning title=PR description may not describe this diff::'
              f'Nothing the description names appears in the diff. It names {named}, and none of '
              f'them is in a changed file, an added or removed line, or an enclosing scope. '
              f'That is the shape of a description written for a different change -- or of a '
              f'merge that took the wrong head. Advisory only (#2978): if these are references '
              f'for context, ignore this.')
        lines = ['### PR description vs diff', '',
                 'Nothing the description names appears in the diff.', '',
                 *(f'- `{m}`' for m in missing), '',
                 'Advisory only. This check never blocks a merge, and it cannot see partial '
                 'drift -- a description whose *other* symbols are present clears it (#2978).']
    elif verdict == 'clear':
        also = f' {len(missing)} other named symbol(s) are not, which is normal.' if missing else ''
        lines = ['### PR description vs diff', '',
                 f'The description names {len(hit)} symbol(s) present in the diff.{also}']
    else:
        lines = ['### PR description vs diff', '',
                 'Abstained: the description names no checkable symbol, or the diff touches no '
                 'source file.']
    summary_path = args.summary or os.environ.get('GITHUB_STEP_SUMMARY')
    if summary_path:
        with open(summary_path, 'a', encoding='utf-8') as handle:
            handle.write('\n'.join(lines) + '\n')
    # Always 0. A description is not a build failure, and a mark that goes red on prose is one
    # people learn to ignore -- which costs more than it catches (the #2229 lesson).
    return 0


if __name__ == '__main__':
    sys.exit(main())
