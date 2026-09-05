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
    # `testing` and a bare `test` are broader than "absence", and deliberately so: in this
    # repo a Testing or Verification section is where a change is PROVEN, which means it names
    # the files a mutation was planted in and restored, and the pins that were run but not
    # touched. That is the densest absent-symbol cluster in the corpus (13 `Verification` and 11
    # `Not verified` headings in 40 bodies). Narrowing these to negative forms only is
    # rate-neutral across all four measures, so there is nothing to buy, and the cost would be
    # readmitting exactly that cluster. It can only shift abstain-vs-clear, never produce a
    # warning.
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
        # The heading TITLE counts, not just the body under it. "## Changes to `WidgetReader.cs`"
        # is a claim, and it is a natural way to write one -- three corpus bodies name a symbol
        # only in a heading. Dropping it can invert a verdict rather than merely lose coverage:
        # if the heading holds the one symbol that IS in the diff, the body's remaining spans are
        # all misses and a clear becomes a warning. Only reached for affirmative headings, since
        # both guards above have already run.
        kept.append(mark.group(2))
        kept.append(text[mark.end():end])
        taken += 1
    return '\n'.join(kept)


# ---------------------------------------------------------------- what counts as a symbol
BACKTICK = re.compile(r'``([^`]+)``|`([^`\n]+)`')
# Build outputs and assets, rejected on their CASE-FOLDED suffix. The `islower()` test below
# covers any lowercase suffix, so this list exists purely for the capitalized spelling: `Dll`
# is not `islower()`, and a capitalized extension is shape-indistinguishable from a member
# name, which is why enumeration rather than a rule. The first twelve are the repo's own
# declared binary set from `.gitattributes`, so the two stay in step by construction.
NON_SOURCE_EXT = ('png', 'jpg', 'jpeg', 'gif', 'ico', 'dll', 'exe', 'pdb', 'snk', 'parquet',
                  'zip', 'duckdb', 'nupkg', 'msi', 'bak', 'log', 'dmp', 'sqlplan')
SOURCE_EXT = ('cs', 'csproj', 'sql', 'xaml', 'json', 'props', 'targets', 'sln', 'ps1', 'sh',
              'yml', 'yaml', 'md', 'cff', 'config', 'py', 'js', 'css', 'html')
# A case hump is what separates a symbol from a prose word. The second alternative is for
# acronym-prefixed names -- IOException, IPAddress, DbCommand -- which have no lowercase-to-
# uppercase transition anywhere and were read as prose without it.
HUMP = re.compile(r'[a-z0-9][A-Z]|[A-Z]{2}[a-z]')
IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
MEMBER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$')
FILENAME = re.compile(r'^(?:[A-Za-z0-9_.+-]+/)*([A-Za-z0-9_.+-]+)\.([A-Za-z0-9]+)$')
TRAILING_LINENO = re.compile(r':\d+(?:-\d+)?$')
# Any of these means the span is not a bare symbol: a regex, a build command, an attribute, a
# glob, an assignment, an HTML/generic type, a shell flag. All appear backticked in real bodies.
# CR and LF are in here as defence in depth, and they have NO behavioural witness today: the
# `.strip()` above removes a trailing newline, and an interior one already fails every
# acceptance path below, since each anchors with ^...$ and none sets DOTALL. Removing these two
# characters changes no verdict and fails no assertion -- measured, not assumed.
#
# They stay because the property is emergent rather than structural. The double-backtick branch
# of BACKTICK matches across lines, an accepted span is printed into a `::warning` workflow
# command, and a workflow command is single-line -- so "no author-controlled newline reaches a
# workflow command line" rests entirely on those patterns staying strict. This makes it rest on
# nothing. The presence of the characters is asserted directly, because no behaviour test can
# distinguish a redundant guard from a missing one.
REJECT_CHARS = set(' \t\r\n=*[]<>\\#%$|+?{}@"\'!;,~^&()')


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
    if named_file:
        suffix = named_file.group(2)
        if suffix.lower() in SOURCE_EXT:
            return ('file', named_file.group(1) + '.' + suffix)
        # A name wearing a non-source extension is a build output or an asset, and must be
        # rejected HERE: the MEMBER branch below would otherwise accept it and take the
        # EXTENSION as the member name, so `MyLib.dll` would count as found against any diff
        # whose text contains the token `dll` -- a false clear, from a span naming something
        # that is not source at all.
        #
        # Two tests, because neither covers the other. `islower()` catches any lowercase
        # suffix, listed or not, which is the common spelling and the open-ended half.
        # NON_SOURCE_EXT catches the capitalized spelling, `MyLib.Dll`, which `islower()`
        # cannot see -- and no shape rule can, because a capitalized extension is
        # indistinguishable from a member name. Real member paths survive both, since their
        # last component is neither lowercase nor a known extension
        # (`ServerTimeHelper.UtcOffsetMinutes`, `DarlingWebEndpoints.MapAll`).
        if suffix.islower() or suffix.lower() in NON_SOURCE_EXT:
            return None
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
# The web assets earn their place by measurement: the Darling service ships `wwwroot/js`, 8 of
# 110 corpus PRs touch one, and admitting them improves every axis -- abstains 6 to 4, wholesale
# power 60% to 62%, empty-diff 99% to 100%, flag rate unmoved at zero.
CODE_EXT = ('.cs', '.sql', '.xaml', '.ps1', '.sh', '.py', '.csproj', '.props', '.targets',
            '.yml', '.yaml', '.js', '.css', '.html')


def diff_symbols(files):
    basenames, idents = set(), set()
    for entry in files:
        for name in (entry.get('filename'), entry.get('previous_filename')):
            if not name:
                continue
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
    return basenames, idents


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
    basenames, idents = diff_symbols(files)
    hit, missing = [], []
    for kind, span in described_symbols(body).items():
        if kind[0] == 'file':
            # Exact basename, never substring containment against the whole path. `classify`
            # only ever yields a bare `basename.ext`, so an exact basename test already covers
            # a description that spelled out directories -- which leaves substring matching
            # contributing nothing but wrong answers: `Config.cs` would count as found against
            # a diff touching only `AppConfig.csproj`. That direction manufactures a false
            # CLEAR, and a false clear hides the one true positive this check exists for.
            # Measured: the two forms agree on all 110 corpus PRs, so precision here is free.
            found = kind[1].lower() in basenames
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
    check('a symbol named only in an affirmative heading is read',
          assess('## Changes to `WidgetReader.cs`\n\nProse that names nothing.',
                 FIXTURE_DIFF)[0] == 'clear')
    # The counterpart risk of reading heading titles: an ABSENT heading must not leak its own
    # symbols back in. `Not verified` sections exist to name things the diff does not contain,
    # and their titles are no different.
    check('a symbol named only in an absent-section heading is not read',
          not any(span == 'SprocketCache' for span in described_symbols(
              '`WidgetReader` changes.\n\n## Not verified: `SprocketCache`\n\nProse.').values()))
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
                 '*CommandTimeoutTests.cs', 'core.autocrlf', 'dev', '0/8',
                 # `data.Xml` and `foo.Bar` are the witnesses for the no-hump-dotted rule
                 # specifically: their suffix is not all-lowercase, so the non-source-extension
                 # test above lets them past, and no component carries a hump. `core.autocrlf`
                 # no longer reaches that rule -- the suffix test rejects it first -- so without
                 # these two rows a mutation removing the hump requirement stays green.
                 'data.Xml', 'foo.Bar'):
        check(f'rejected as a symbol: {span}', classify(span) is None)
    # Shape acceptances.
    for span in ('WidgetReader', 'StoreDeadlines.WidgetSeconds', 'ReadWidgetsAsync()',
                 'WidgetReader.cs', 'WidgetReader.cs:88'):
        check(f'accepted as a symbol: {span}', classify(span) is not None)
    # A line citation resolves to its file rather than being read as an identifier.
    check('line-cited file resolves to the file', classify('WidgetReader.cs:88') ==
          ('file', 'WidgetReader.cs'))
    # Docs-only diffs cannot support a verdict.
    # A non-source extension must be rejected outright, not fall through to MEMBER, which would
    # take the extension as the member name and match any diff mentioning that token.
    for span in ('MyLib.dll', 'WidgetHost.exe', 'Payload.zip', 'Icon.ico',
                 # The CAPITALIZED spelling is a separate rejection path: `Dll` is not
                 # `islower()`, so without NON_SOURCE_EXT these fall into MEMBER and take the
                 # extension as the member name. `MyLib.dll` passing is no evidence for these.
                 'MyLib.Dll', 'WidgetHost.Exe', 'Payload.Zip', 'Screenshot.PNG',
                 # An UNLISTED lowercase extension is what islower() still earns its place on.
                 'MyLib.foo'):
        check(f'rejected as a symbol: {span}', classify(span) is None)
    # Abstain, not warn: the span contributes no symbol at all, so there is nothing to check.
    # What matters is that it is not CLEAR -- before the suffix test this body counted as
    # described-and-found purely because the patch text contained the token `dll`.
    for span, token in (('MyLib.dll', 'dll'), ('MyLib.Dll', 'Dll')):
        check(f'a build output does not clear a diff that merely mentions `{token}`',
              assess(f'`{span}` is rebuilt.',
                     [{'filename': 'Lite/Services/Loader.cs',
                       'patch': f'@@ -1 +1 @@\n-var a = 1;\n+// load the {token} here'}])[0]
              == 'abstain')
    # A member path survives that test because its last component carries a capital.
    check('a dotted member path is not mistaken for a file',
          classify('ServerTimeHelper.UtcOffsetMinutes')
          == ('ident', 'ServerTimeHelper.UtcOffsetMinutes', 'UtcOffsetMinutes'))
    # Acronym-prefixed type names have no lowercase-to-uppercase transition at all.
    for span in ('IOException', 'IPAddress', 'DbCommand'):
        check(f'accepted as a symbol: {span}', classify(span) is not None)
    # A KNOWN GAP, asserted so it stays visible rather than latent: a snake_case T-SQL
    # identifier is not read as a symbol, because it carries no case hump. A description naming
    # only `dbo.collect_memory_pressure` therefore contributes nothing and the verdict abstains,
    # which is CONTRIBUTING.md's own commit-message spelling for a procedure.
    #
    # Not fixed, on the measurement: accepting underscore-separated lowercase words costs 4
    # points of wholesale power (60% to 56%) and buys no abstains back, because in a C#-shaped
    # body snake_case is what column names, wait types and config keys look like -- the
    # named-for-context class that drives false positives. The corpus cannot settle the T-SQL
    # side either way: ZERO of its 110 PRs touch `install/` or a `.sql` file, so the gain is
    # unmeasurable here while the loss is measured. Re-run the ladder once the corpus carries
    # install/ PRs and this may well flip.
    for span in ('collect_memory_pressure', 'dbo.collect_memory_pressure',
                 'collect.memory_pressure_events'):
        check(f'known gap, snake_case SQL identifier is not a symbol: {span}',
              classify(span) is None)
    # The web assets ARE read, both as described names and on the diff side.
    check('a web asset is read as a file symbol',
          classify('alerts.js') == ('file', 'alerts.js'))
    check('a js-only diff supports a verdict rather than abstaining',
          assess('`renderAlertRow` moves.',
                 [{'filename': 'Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/alerts.js',
                   'patch': '@@ -1 +1 @@\n-function renderAlertRow() {}\n'
                            '+function renderAlertRow(row) {}'}])[0] == 'clear')
    check('a same-suffix filename is not counted as found',
          assess('`Config.cs` gains a field.',
                 [{'filename': 'Lite/AppConfig.csproj',
                   'patch': '@@ -1 +1 @@\n-<X/>\n+<Y/>'}])[0] == 'warn')
    check('a description spelling out directories still matches on basename',
          assess('`Darling/PerformanceMonitor.Darling.Service/WidgetReader.cs` changes.',
                 FIXTURE_DIFF)[0] == 'clear')
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
    # An accepted span is printed into a single-line `::warning` workflow command, so no
    # author-controlled newline may survive classification. The double-backtick branch of
    # BACKTICK matches across lines, which is what makes this reachable at all.
    check('a double-backticked span carrying a newline yields no symbol',
          not described_symbols('``WidgetReader\nSprocketCache`` moves.'))
    check('an interior newline yields no symbol',
          classify('Widget\nReader') is None and classify('Widget\rReader') is None)
    # Asserted on the MECHANISM, not on behaviour. The two characters are redundant with the
    # anchored acceptance patterns, so every behavioural test passes with or without them --
    # which is exactly why their presence has to be stated to be kept.
    check('CR and LF are rejected explicitly, not only emergently',
          {'\r', '\n'} <= REJECT_CHARS)
    # A TRAILING newline is stripped rather than rejected, which is correct -- so the property
    # to hold is about the accepted VALUE, not about rejection: nothing that reaches the
    # annotation may contain a line break, whichever way the span was spelled.
    check('no accepted symbol carries a line break',
          all(not any(c in part for c in '\r\n' for part in kind[1:])
              for kind in (classify(s) for s in
                           ('WidgetReader\n', 'WidgetReader\r\n', ' WidgetReader.cs\n',
                            'StoreDeadlines.WidgetSeconds\n'))
              if kind is not None))
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
