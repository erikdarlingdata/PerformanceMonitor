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
  change at all. That fires on 0 of those 110, abstains on 6, catches 60% of body/diff
  mismatches and 99% of effectively-empty diffs. Zero is 0/110, i.e. below this corpus's
  resolution, not a promise: read it as low, not as none.

  It is close to BLIND to partial drift, which is what both motivating near-misses were: with a
  quarter of a PR's files removed it fires on 8%. A rule that catches those reliably does not
  exist at a tolerable flag rate -- see the issue. This check earns its place on the wholesale
  case only, and its annotation says so rather than implying broader cover.

Advisory, always exit 0, and it must never be marked a required check.
"""
import argparse, json, os, re, sys, tempfile

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

# How far past the lead prose to read, counted in affirmative sections. Reading DEEPER lowers
# the flag rate monotonically -- more symbols in scope means a higher chance one of them lands
# in the diff -- while power peaks in the middle and then decays, because a body whose every
# section is in scope almost always matches something. Measured over the same 110 merged PRs
# (flag rate / abstains / wholesale-mismatch power / empty-diff power):
#
#   lead only  0.9%  74  28%  33%      <- abstains on two thirds of PRs, no verdict to read
#   lead+1     2.7%  21  63%  84%
#   lead+2     1.8%   6  67%  98%      <- peak power
#   lead+3     0.9%   5  62%  99%      <- the knee: half the flags, power within noise of peak
#   lead+all   0.9%   5  58%  99%
#
# Three is the knee. Going deeper buys nothing and costs power; stopping shorter triples the
# flag rate or abandons two thirds of PRs undecided.
AFFIRMATIVE_SECTIONS = 3


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
# A dependency bump's diff is a version string. Its description talks about the PACKAGE -- the
# decoder it fixed, the type whose behaviour changed -- and every one of those names lives
# inside the upgraded package, so it cannot appear in the diff by definition. These files
# therefore contribute no vocabulary a description could match, and counting them as code turns
# every bump into a warning. This is the same reasoning as `json`'s absence from CODE_EXT below,
# applied to the two manifests that ARE on it.
DEPENDENCY_MANIFEST = re.compile(
    r'(packages\.lock\.json|Directory\.Packages\.props|nuget\.config|packages\.config)$', re.I)
# Narrower than SOURCE_EXT on purpose, and `json` is the gap that matters: a description may
# legitimately name `darling.json`, so json belongs on the DESCRIBED side, but a diff of only
# json is usually a dependency bump whose lock file shares no vocabulary with any prose. Such a
# body names types from inside the bumped package -- which cannot appear in the diff by
# definition -- so admitting json here converts a silent abstain into a false warning. Measured:
# adding it moves no verdict on 110 merged PRs (none is json-only), and turns the one lock-file
# change in that corpus from abstain into a warning naming a type in the upgraded package.
# `sln`, `config` and `cff` are excluded for the same reason with even less vocabulary.
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
    codeish = [f for f in files
               if (f.get('filename') or '').lower().endswith(CODE_EXT)
               and not DEPENDENCY_MANIFEST.search(f.get('filename') or '')]
    if not codeish:
        # A docs-only, config-only or dependency-bump diff contributes no identifiers to
        # intersect against, so any verdict would be an artifact of the diff having nothing
        # to say. Abstaining is the honest answer, and it is never a warning.
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
    deep = ('`WidgetReader` changes.\n\n## The fix\n\n`StoreDeadlines` moves.\n\n'
            '## The shape\n\n`GadgetPool` is one sibling.\n\n## The other shape\n\n'
            '`DoodadStore` is another.\n\n## Further out\n\n`SprocketCache` is context.')
    check('the third affirmative section is still read',
          any(span == 'DoodadStore' for span in described_symbols(deep).values()))
    check('the fourth affirmative section is not read',
          not any(span == 'SprocketCache' for span in described_symbols(deep).values()))
    # Fenced code, blockquotes and measurement tables are not claims.
    for label, body in (('fenced code', '```\n`SprocketCache`\n```\n'),
                        ('blockquote', '> `SprocketCache` per review\n'),
                        ('table row', '| site | `SprocketCache` |\n')):
        check(f'{label} contributes no symbols', not described_symbols(body))
    # Shape rejections, each observed backticked in a real body.
    # BASE64URL earns its place: it is the one ALL_CAPS shape the hump rule alone would ACCEPT,
    # because a digit followed by a capital ("4U") reads as a case hump. Without it the isupper
    # rejection is redundant with the hump rule for every span here and a mutation removing it
    # stays green -- which is how this row came to exist.
    for span in ('SOS_SCHEDULER_YIELD', 'HEAD', 'BASE64URL', 'net10.0', '[Theory]', 'MOVED=0',
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
    # json is on the described side but not the diff side, so a lock-file-only dependency bump
    # abstains instead of warning about a type that lives inside the upgraded package. Pinned
    # because the asymmetry reads like an oversight and the corpus holds no json-only PR to
    # catch a well-meant widening.
    check('config-json-only diff abstains (json is described-side only)',
          assess('`SprocketCache` now evicts on write.',
                 [{'filename': 'Lite/config/collection_schedule.json',
                   'patch': '@@ -1,3 +1,3 @@\n-  "IntervalSeconds": 60,\n'
                            '+  "IntervalSeconds": 30,'}])[0] == 'abstain')
    # Directory.Packages.props IS in CODE_EXT, so only DEPENDENCY_MANIFEST keeps a bump quiet.
    # Pinned separately from the json row above, or one exclusion covers for the other's removal.
    check('dependency-bump-only diff abstains rather than warning',
          assess('Takes the same build `WidgetHost` shipped, for the `GadgetOutput` decoder fix.',
                 [{'filename': 'Directory.Packages.props',
                   'patch': '@@ -1,3 +1,3 @@\n-    <PackageVersion Include="A" Version="0.2.0" />\n'
                            '+    <PackageVersion Include="A" Version="0.2.1" />'}])[0] == 'abstain')
    check('empty body abstains', assess('', FIXTURE_DIFF)[0] == 'abstain')
    # A lookup that failed must not be able to fail the job (#2309). The workflow's own `[]`
    # fallback depends on this, so pin every shape a broken diff arrives in -- and pin that each
    # one WARNS, or a silent empty read would abstain looking exactly like a clean no-op.
    said = []
    check('a missing diff file loads as empty rather than raising',
          load_files('/nonexistent/diffdesc-not-here.json', said.append) == [])
    check('a malformed diff file loads as empty rather than raising',
          load_files(__file__, said.append) == [])
    check('both unreadable-diff cases warn', len(said) == 2)
    with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as handle:
        json.dump([FIXTURE_DIFF, FIXTURE_DIFF], handle)     # the --slurp page-per-element shape
        nested = handle.name
    try:
        check('a page-per-element diff list is flattened',
              [f['filename'] for f in load_files(nested, said.append)]
              == [FIXTURE_DIFF[0]['filename']] * 2)
        check('flattening a well-formed nested list does not warn', len(said) == 2)
    finally:
        os.unlink(nested)

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


def load_files(path, warn=None):
    """The changed-file list, or an empty list plus a warning. Never raises.

    Same rule as the workflow's lookup handling (#2309): this check is advisory, so an
    unreadable or malformed diff must abstain loudly rather than exit non-zero and turn the
    mark red for a reason that says nothing about the description.

    `warn` is injectable so the self-test can assert the warning happened without emitting a
    real annotation -- two bogus "could not read the diff" annotations on every healthy run
    would be a check reporting its own tests as findings.
    """
    warn = warn if warn is not None else (lambda message: print(
        f'::warning title=Description drift could not read the diff::{message}'))
    if not path:
        return []
    try:
        with open(path, encoding='utf-8') as handle:
            files = json.load(handle)
    except (OSError, ValueError) as problem:
        warn(f'{problem}. The description was NOT compared against the diff on this run.')
        return []
    if isinstance(files, dict):
        files = files.get('files', [])
    if not isinstance(files, list):
        return []
    # A page-per-element array is what `gh api --paginate --slurp` hands back for this endpoint.
    # The workflow flattens before writing, but tolerate the nested shape so a future caller
    # that does use --slurp gets a verdict rather than a silent zero-file abstain.
    if files and isinstance(files[0], list):
        files = [entry for page in files for entry in page]
    return [f for f in files if isinstance(f, dict)]


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
    files = load_files(args.files)

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
