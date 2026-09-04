/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Witnesses for <see cref="CSharpSourceWalker"/> itself (#2913) — the instrument five source pins
/// now depend on. A walker that reports less code than a file contains makes every one of those pins pass
/// vacuously: they assert that an offender count is ZERO or that an unguarded path does not exist, and an
/// edge the walk cannot see satisfies both without ever being looked at. So the walker carries its own
/// proofs, and each of them fails for the reason that actually broke the idiom.</para>
///
/// <para><b>Every fixture is compared against the walk it replaced, not against a description of it.</b>
/// <see cref="LegacyCodeMask"/> below is the pre-#2913 code, transcribed from the five copies rather than
/// paraphrased, so each witness states its claim as a DIFFERENCE: the legacy walk sees nothing here, this
/// one sees the call. A comment saying "holes are now preserved" cannot fail. These can, and they go red
/// the moment the walker stops preserving them — which is the whole reason the discarded form is kept here
/// instead of deleted. Same shape as <see cref="IlCallSiteScannerTests"/> for #2898's IL walk.</para>
///
/// <para><b>The fixtures are arranged, not borrowed.</b> #2913 is explicit about that and it is the right
/// call: a witness that leans on a real source file happening to contain the shape stops testing anything
/// the day that file is edited, and cannot be shown to have been red before the fix. The two exceptions are
/// <see cref="EveryFileTheSourcePinsScan_StaysBalancedAfterStripping"/> and
/// <see cref="TheStrippedTextIsCharacterAlignedWithItsInput"/>, which are corpus invariants rather than
/// construct witnesses — a desynchronised walk cannot stay balanced across hundreds of real files by luck,
/// and every one of these pins reports its offenders by LINE.</para>
/// </summary>
public sealed class CSharpSourceWalkerTests
{
    /// <summary>Joins fixture lines with <c>\n</c> explicitly rather than embedding newlines in a literal,
    /// so a fixture cannot change meaning with the checkout's line endings.</summary>
    private static string Lines(params string[] lines) => string.Join("\n", lines) + "\n";

    /// <summary>
    /// <para>#2913's own example, arranged as the reachability pins would meet it: a method whose ONLY call
    /// to a store read sits inside an interpolation. The legacy walk blanked the whole literal span, holes
    /// included, so this method had no call in it at all as far as any scan built on that walk could tell —
    /// and "no unguarded path to a store read exists" is satisfied by a path that cannot be seen.</para>
    ///
    /// <para>The second half of the fixture is the positive control the issue asks for: the same call
    /// written outside an interpolation, which BOTH walks see. Without it, "the legacy walk found nothing"
    /// would be equally consistent with the fixture not containing the call, or with the needle being
    /// wrong.</para>
    /// </summary>
    [Fact]
    public void ACallInsideAnInterpolationIsCode_WhichTheLegacyWalkCouldNotSee()
    {
        var fixture = Lines(
            """    private void Tick()""",
            """    {""",
            """        Log($"pending={_dataService.CountAsync()}");""",
            """    }""",
            "",
            """    private void Control()""",
            """    {""",
            """        Log(_dataService.CountAsync());""",
            """    }""");

        /* The control first: if this does not hold, nothing below means anything, because "not found" would
           not be evidence of blindness. */
        Assert.Equal(1, Occurrences(LegacyStrip(fixture), "_dataService.CountAsync()"));
        Assert.Equal(2, Occurrences(CSharpSourceWalker.StripCommentsAndStrings(fixture), "_dataService.CountAsync()"));

        /* And the claim, asked of the interpolated occurrence specifically rather than of the file. */
        var interpolated = fixture.IndexOf("""$"pending=""", StringComparison.Ordinal);

        Assert.False(
            IsCodeAt(LegacyCodeMask(fixture), fixture, interpolated, "_dataService"),
            "The legacy walk suddenly sees inside an interpolation, so this fixture no longer demonstrates "
            + "the gap #2913 was filed for and the assertion below proves nothing.");

        Assert.True(
            IsCodeAt(CSharpSourceWalker.CodeMask(fixture), fixture, interpolated, "_dataService"),
            "A call written inside an interpolation is invisible to the walker again. Every pin built on it "
            + "now under-reports, and under-reporting is what makes a zero-offender assertion vacuous.");
    }

    /// <summary>
    /// <para>Raw string literals, which the legacy walk did not know existed: it read the opening
    /// <c>"""</c> as an empty string <c>""</c> followed by the START of another one, so the delimiter itself
    /// desynchronised the parse and the body was then classified against the wrong state.</para>
    ///
    /// <para>The fixture puts a <c>"</c> inside the raw body, which is the whole reason a raw string gets
    /// used, and follows it with real code. The legacy walk pulls the quoted part of the body into the CODE
    /// stream — measured on the real corpus, 74,111 characters of SQL text across the viewer project, and
    /// 10,076 of them in <c>ViewerDataService.QueryStore.cs</c> alone — which is how a table name comes to
    /// look like an invocation to the reachability pin's <c>\b(\w+)\s*\(</c>.</para>
    ///
    /// <para>Three assertions, because three things can be wrong independently: the body's text must not be
    /// code, the code AFTER the literal must survive (a walker that "fixed" this by blanking more would
    /// satisfy the first assertion and break every pin), and the stripped text must come out bracket-balanced
    /// — the legacy walk's leak takes an unmatched <c>(</c> out of the SQL and into the code stream, which is
    /// what breaks the brace-matched method-body extraction the fleet-timer pin is built on.</para>
    /// </summary>
    [Fact]
    public void ARawStringLiteralIsOneToken_WhereTheLegacyWalkDesynchronisedOnItsDelimiter()
    {
        var fixture = Lines(
            """"    private const string Sql = """SELECT "count(x" FROM t;""";"""",
            """    private void After() { RealCall(); }""");

        Assert.True(
            IsCodeAt(LegacyCodeMask(fixture), fixture, 0, "count(x"),
            "The legacy walk no longer leaks the raw body's text into the code stream, so this fixture no "
            + "longer demonstrates the desynchronisation and the assertion below proves nothing.");

        Assert.False(
            IsCodeAt(CSharpSourceWalker.CodeMask(fixture), fixture, 0, "count(x"),
            "A raw string literal's body is code again — SQL text is being scanned as though it were C#.");

        /* The code AFTER the literal must survive. Blanked code is an edge a reachability walk cannot
           follow, so it reports the path safe. */
        Assert.Contains("RealCall()", CSharpSourceWalker.StripCommentsAndStrings(fixture), StringComparison.Ordinal);

        /* And the consequence, stated structurally: the leak carries the SQL's unmatched paren into the
           code stream. */
        Assert.Equal(1, Balance(LegacyStrip(fixture), '(', ')'));
        Assert.Equal(0, Balance(CSharpSourceWalker.StripCommentsAndStrings(fixture), '(', ')'));
    }

    /// <summary>
    /// An interpolated raw string's holes, in both the single- and multiple-<c>$</c> forms this codebase
    /// actually ships — <c>ViewerDataService.Waits.cs</c> builds its wait-trend SQL as
    /// <c>$$"""... wait_type IN ({{typeParams}}) ..."""</c>, so the <c>$$</c> form is not hypothetical here.
    ///
    /// <para>The third case is a measured C# rule rather than a guess: with two <c>$</c>, a SINGLE brace is
    /// literal output text, so a walker that treats any brace run as a hole opener reads <c>{notAHole}</c>
    /// as code. The compiler settled the neighbouring question too — <c>$"""{{"""</c> does not compile
    /// (CS9006, "does not start with enough '$' characters to allow this many consecutive opening braces as
    /// content"), which is why the doubled-brace ESCAPE is pinned below on the non-raw forms, where it is
    /// legal, rather than here.</para>
    /// </summary>
    [Fact]
    public void AnInterpolatedRawStringsHolesAreCode_AndItsBraceRunLengthDecidesWhatIsAHole()
    {
        var single = """"    var a = $"""x={Probe()}""";"""";
        var doubled = """"    var b = $$"""x={{Deeper()}}""";"""";
        var shortRun = """"    var c = $$"""x={notAHole}""";"""";

        Assert.DoesNotContain("Probe()", LegacyStrip(single), StringComparison.Ordinal);
        Assert.Contains("Probe()", CSharpSourceWalker.StripCommentsAndStrings(single), StringComparison.Ordinal);

        Assert.DoesNotContain("Deeper()", LegacyStrip(doubled), StringComparison.Ordinal);
        Assert.Contains("Deeper()", CSharpSourceWalker.StripCommentsAndStrings(doubled), StringComparison.Ordinal);

        Assert.DoesNotContain(
            "notAHole",
            CSharpSourceWalker.StripCommentsAndStrings(shortRun),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Nested brackets inside a hole, both shapes #2913 names. The indexer is the one that ends the hole
    /// early if <c>]</c> is not tracked; the conditional is the one whose <c>:</c> is NOT a format
    /// specifier, because it sits at bracket depth one — and C# requires that parenthesisation for exactly
    /// this reason, which is what makes "a colon at depth zero is a format specifier" a rule rather than a
    /// guess.
    /// </summary>
    [Fact]
    public void NestedBracketsInsideAHoleDoNotEndIt_AndADeepColonIsNotAFormatSpecifier()
    {
        var indexer = """    var a = $"v={dict[Key()]} tail";""";
        var conditional = """    var b = $"v={(flag ? Yes() : No())} tail";""";

        var walkedIndexer = CSharpSourceWalker.StripCommentsAndStrings(indexer);
        var walkedConditional = CSharpSourceWalker.StripCommentsAndStrings(conditional);

        Assert.Contains("dict[Key()]", walkedIndexer, StringComparison.Ordinal);
        Assert.Contains("flag ? Yes() : No()", walkedConditional, StringComparison.Ordinal);

        /* The literal text on the far side of the hole still has to be blanked, or the walker has stopped
           being a stripper — which would also satisfy the two Contains above. */
        Assert.DoesNotContain("tail", walkedIndexer, StringComparison.Ordinal);
        Assert.DoesNotContain("tail", walkedConditional, StringComparison.Ordinal);

        Assert.DoesNotContain("Key()", LegacyStrip(indexer), StringComparison.Ordinal);
        Assert.DoesNotContain("Yes()", LegacyStrip(conditional), StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>{{</c> and <c>}}</c> are escaped output braces, not holes. The two fixtures differ by exactly one
    /// brace on each side and must classify the same identifier oppositely — which is the only form of this
    /// assertion that cannot be satisfied by a walker that keeps everything, or by one that keeps nothing.
    /// </summary>
    [Fact]
    public void DoubledBracesAreEscapedText_AndAnOddBraceRunStillOpensAHole()
    {
        var escaped = """    var a = $"{{Hidden()}}";""";
        var hole = """    var b = $"{{{Hidden()}}}";""";

        Assert.DoesNotContain(
            "Hidden()",
            CSharpSourceWalker.StripCommentsAndStrings(escaped),
            StringComparison.Ordinal);

        Assert.Contains(
            "Hidden()",
            CSharpSourceWalker.StripCommentsAndStrings(hole),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A format specifier is output text, so <c>N0</c> must not enter the code stream while the expression
    /// ahead of it does. This is the false-POSITIVE direction — none of the regexes these pins carry would
    /// match <c>N0</c> today — and it is pinned anyway because a false positive fails a green build on
    /// correct code, which is the more expensive kind of wrong for a guard nobody is currently doubting.
    /// </summary>
    [Fact]
    public void AFormatSpecifierIsText_AndTheExpressionAheadOfItIsCode()
    {
        var fixture = """    var a = $"n={Count():N0} of {Total(),8}";""";

        var walked = CSharpSourceWalker.StripCommentsAndStrings(fixture);

        Assert.Contains("Count()", walked, StringComparison.Ordinal);
        Assert.DoesNotContain("N0", walked, StringComparison.Ordinal);

        /* An alignment is a constant EXPRESSION rather than format text, so it stays code — and the call
           ahead of the comma has to survive either way. */
        Assert.Contains("Total()", walked, StringComparison.Ordinal);
    }

    /// <summary>
    /// The verbatim forms. The doubled-quote escape is the one piece of the legacy walk that was already
    /// right, so the first pair is a REGRESSION guard both walks must satisfy. The interpolated forms are
    /// where they part company, and <c>@$"</c> is the ordering the legacy walk got wrong twice over: its
    /// verbatim branch tested for <c>@</c> followed immediately by <c>"</c>, so <c>@$"</c> fell through to
    /// the regular-string branch and was then scanned for backslash escapes a verbatim string does not have.
    /// </summary>
    [Fact]
    public void AVerbatimStringsDoubledQuoteEscapeSurvives_AndItsHolesBecomeCode()
    {
        var escapedQuote = """    var a = @"say ""hi"" now"; Kept();""";

        foreach (var stripped in new[]
                 {
                     LegacyStrip(escapedQuote),
                     CSharpSourceWalker.StripCommentsAndStrings(escapedQuote),
                 })
        {
            Assert.DoesNotContain("say", stripped, StringComparison.Ordinal);
            Assert.Contains("Kept()", stripped, StringComparison.Ordinal);
        }

        foreach (var fixture in new[]
                 {
                     """    var b = $@"v={Probe()}";""",
                     """    var c = @$"v={Probe()}";""",
                 })
        {
            Assert.DoesNotContain("Probe()", LegacyStrip(fixture), StringComparison.Ordinal);
            Assert.Contains("Probe()", CSharpSourceWalker.StripCommentsAndStrings(fixture), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <para>A <c>char</c> literal holding a quote. This is the one gap in #2913 that was NOT latent:
    /// <c>value.Contains('"')</c> is a real line in <c>ViewerServerTab.ChartContextMenu.cs</c>, inside the
    /// directory both viewer pins glob recursively, and the legacy walk read that quote as opening a string
    /// — blanking every character up to the next quote anywhere in the file, or to the end of the line in
    /// the one copy that had grown a newline stop.</para>
    ///
    /// <para>Blanked code is the dangerous direction: the fixture's call disappears, and a walk that cannot
    /// see a call reports the path safe.</para>
    /// </summary>
    [Fact]
    public void ACharLiteralHoldingAQuoteDoesNotOpenAString()
    {
        var fixture = Lines(
            """    private void Quote(string value)""",
            """    {""",
            """        if (value.Contains('"')) { Guarded(); }""",
            """    }""");

        Assert.DoesNotContain("Guarded()", LegacyStrip(fixture), StringComparison.Ordinal);
        Assert.Contains("Guarded()", CSharpSourceWalker.StripCommentsAndStrings(fixture), StringComparison.Ordinal);
    }

    /// <summary>
    /// <para><see cref="CSharpSourceWalker.StatementSpanFrom"/> is built on the same code mask as the
    /// stripper, and that is what this pins: a literal the stripper understands cannot end — or fail to
    /// end — a statement span either. Before consolidation the two walks were separate copies with separate
    /// string handling, so they could disagree about what a literal was.</para>
    ///
    /// <para>The fixture is the shape that costs a guard its meaning. An UNTIMED command is followed by a
    /// statement containing <c>'"'</c>; the legacy walk read that quote as opening a string, found no
    /// closing quote for the rest of the fixture, and ran the span to the end — so it picked up the NEXT
    /// command's deadline and reported the untimed site as clean. A false negative, and the whole span
    /// walker exists on the claim that a span "can never leak out of the member it started in".</para>
    /// </summary>
    [Fact]
    public void AStatementSpanCannotLeakPastACharLiteralQuote()
    {
        var fixture = Lines(
            """        var untimed = _dataSource.CreateCommand(Sql);""",
            """        var sep = value.Contains('"') ? 1 : 2;""",
            """        timed.CommandTimeout = 5;""");

        var start = fixture.IndexOf(".CreateCommand(", StringComparison.Ordinal);

        Assert.True(start > 0, "the fixture no longer contains a command construction");

        Assert.Contains(
            "CommandTimeout",
            LegacyStatementSpanFrom(fixture, start, statements: 2),
            StringComparison.Ordinal);

        Assert.DoesNotContain(
            "CommandTimeout",
            CSharpSourceWalker.StatementSpanFrom(fixture, start, statements: 2),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A semicolon in a comment still cannot end a span, which is the assertion the three timeout pins lean
    /// on most: this codebase's style puts an explanatory comment in exactly the gap between a
    /// <c>CreateCommand</c> and its deadline, and a semicolon inside one would report a correctly-timed site
    /// as an offender. Both walks agree here; it is carried so consolidation cannot quietly drop it.
    /// </summary>
    [Theory]
    [InlineData("        /* one; two */\n")]
    [InlineData("        // one; two\n")]
    [InlineData("        /// <c>one; two</c>\n")]
    public void ASemicolonInsideACommentDoesNotEndASpan(string gap)
    {
        var fixture = "        var command = _dataSource.CreateCommand(Sql);\n"
            + gap
            + "        command.CommandTimeout = 5;\n";

        var start = fixture.IndexOf(".CreateCommand(", StringComparison.Ordinal);

        Assert.Contains(
            "CommandTimeout",
            CSharpSourceWalker.StatementSpanFrom(fixture, start, statements: 2),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// <para>The corpus invariant, over every file the five pins actually read. With comments and literal
    /// text blanked, and hole DELIMITERS blanked with them, a well-formed C# file's remaining braces and
    /// parentheses balance to zero. A walk that loses its place on a delimiter cannot stay balanced across
    /// hundreds of real files by luck, and this is the only failure this file can express against real
    /// source rather than an arranged fixture.</para>
    ///
    /// <para>Measured against the legacy walk when this landed: two of these files came out unbalanced —
    /// <c>ViewerServerTab.ChartContextMenu.cs</c> (the <c>'"'</c> char literal, braces -1 and parens +2) and
    /// <c>StoreConnectionSelfTest.cs</c> (a <c>Split('"')</c> inside an interpolation hole, alongside raw
    /// strings, braces +3). Balance is what makes <c>ViewerFleetTimerGuardTests</c>' brace-matched
    /// method-body extraction mean anything, so those two were not a curiosity.</para>
    /// </summary>
    [Fact]
    public void EveryFileTheSourcePinsScan_StaysBalancedAfterStripping()
    {
        var files = ScannedSources();

        /* Floor, not an equality: these directories grow. A zero would satisfy the loop below without
           reading anything, which is the failure mode that makes a sweep vacuous. */
        Assert.True(
            files.Count >= 200,
            $"the sweep found only {files.Count} source file(s) — it is not reading the projects");

        var offenders = new List<string>();

        foreach (var path in files)
        {
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
            var braces = Balance(stripped, '{', '}');
            var parens = Balance(stripped, '(', ')');

            if (braces != 0 || parens != 0)
            {
                offenders.Add($"{Path.GetFileName(path)} (braces {braces}, parens {parens})");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} file(s) come out of the stripper unbalanced, which means the walk lost its "
            + "place on a delimiter and everything it classified past that point is against the wrong "
            + $"state: {string.Join(", ", offenders)}");
    }

    /// <summary>
    /// The stripper is character-aligned with its input and preserves every newline, because all five pins
    /// report an offender's LINE by counting <c>\n</c> in the stripped text up to the match index. A walk
    /// that dropped or added a character would report real offenders at the wrong line, which is worse than
    /// not reporting them: it sends the reader to innocent code.
    /// </summary>
    [Fact]
    public void TheStrippedTextIsCharacterAlignedWithItsInput()
    {
        var files = ScannedSources();

        Assert.True(files.Count >= 200, $"the sweep found only {files.Count} source file(s)");

        var offenders = new List<string>();

        foreach (var path in files)
        {
            var text = File.ReadAllText(path);
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(text);

            if (stripped.Length != text.Length)
            {
                offenders.Add($"{Path.GetFileName(path)} length {text.Length} -> {stripped.Length}");
                continue;
            }

            var before = text.Count(c => c == '\n');
            var after = stripped.Count(c => c == '\n');

            if (before != after)
            {
                offenders.Add($"{Path.GetFileName(path)} newlines {before} -> {after}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "the stripper is no longer character-aligned with its input, so every line number the pins "
            + $"report is off: {string.Join(", ", offenders)}");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        var i = haystack.IndexOf(needle, StringComparison.Ordinal);

        while (i >= 0)
        {
            count++;
            i = haystack.IndexOf(needle, i + 1, StringComparison.Ordinal);
        }

        return count;
    }

    /// <summary>Whether the first occurrence of <paramref name="needle"/> at or after
    /// <paramref name="from"/> is classified as code.</summary>
    private static bool IsCodeAt(bool[] mask, string text, int from, string needle)
    {
        Assert.True(from >= 0, "the fixture no longer contains the anchor this witness starts from");

        var at = text.IndexOf(needle, from, StringComparison.Ordinal);

        Assert.True(at >= 0, $"the fixture no longer contains '{needle}' after index {from}");

        return mask[at];
    }

    private static int Balance(string text, char open, char close)
    {
        var depth = 0;

        foreach (var c in text)
        {
            if (c == open)
            {
                depth++;
            }
            else if (c == close)
            {
                depth--;
            }
        }

        return depth;
    }

    /// <summary>
    /// The union of what the five consolidated pins read: the viewer project recursively, the storage and
    /// analysis projects' top level, and the six alert-pass files. Enumerated here rather than borrowed from
    /// any one pin, because the invariants above are properties of the WALKER over everything it is pointed
    /// at.
    /// </summary>
    private static List<string> ScannedSources()
    {
        var root = RepoRoot();
        var paths = new List<string>();

        var viewer = Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Viewer");

        paths.AddRange(Directory.EnumerateFiles(viewer, "*.cs", SearchOption.AllDirectories)
            .Where(p => !Path.GetRelativePath(viewer, p)
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(s => string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
                          || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase))));

        foreach (var project in new[]
                 {
                     "PerformanceMonitor.Darling.Storage",
                     "PerformanceMonitor.Darling.Analysis",
                 })
        {
            paths.AddRange(Directory.EnumerateFiles(
                Path.Combine(root, "Darling", project),
                "*.cs",
                SearchOption.TopDirectoryOnly));
        }

        var service = Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Service");

        paths.AddRange(new[]
        {
            "DarlingAlertReadAdapter.cs",
            "DarlingPostgresAlertReadAdapter.cs",
            "PgAlertStateStore.cs",
            "PgMuteRuleStore.cs",
            "PgAlertHistoryStore.cs",
            "DarlingSelfAlertEvaluator.cs",
        }.Select(f => Path.Combine(service, f)).Where(File.Exists));

        return paths.OrderBy(p => p, StringComparer.Ordinal).ToList();
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);

        return dir!;
    }

    // ── The walk being ruled out ──────────────────────────────────────────────────────────

    /// <summary>
    /// The pre-#2913 classification, transcribed from the copies in <c>ViewerCommandTimeoutTests</c>,
    /// <c>AnalysisPassCommandTimeoutTests</c> and <c>ViewerFleetTimerGuardTests</c>. Kept as the thing being
    /// ruled out rather than as a comment claiming it was wrong: every witness above states its claim as a
    /// difference between this and <see cref="CSharpSourceWalker"/>, so reverting the walker turns them red
    /// instead of turning them into descriptions of a fix nobody can check.
    /// </summary>
    private static bool[] LegacyCodeMask(string text)
    {
        var code = new bool[text.Length];
        var i = 0;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i = LegacySkipVerbatimString(text, i + 2);
                continue;
            }

            if (c == '"')
            {
                i = LegacySkipRegularString(text, i + 1);
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                var nl = text.IndexOf('\n', i);
                i = nl < 0 ? text.Length : nl;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? text.Length : close + 2;
                continue;
            }

            code[i] = true;
            i++;
        }

        return code;
    }

    /// <summary>The legacy walk's stripped text, from its mask, so the two walks are compared through the
    /// same projection and a difference cannot be an artefact of how the text was rebuilt.</summary>
    private static string LegacyStrip(string text)
    {
        var code = LegacyCodeMask(text);
        var sb = new StringBuilder(text.Length);

        for (var i = 0; i < text.Length; i++)
        {
            sb.Append(code[i] ? text[i] : text[i] == '\n' ? '\n' : ' ');
        }

        return sb.ToString();
    }

    private static int LegacySkipVerbatimString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '"')
            {
                if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return i;
    }

    private static int LegacySkipRegularString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '\\')
            {
                i += 2;
                continue;
            }

            if (text[i] == '"')
            {
                return i + 1;
            }

            i++;
        }

        return i;
    }

    /// <summary>The legacy statement-span walk, from <c>StorageCommandTimeoutTests</c>.</summary>
    private static string LegacyStatementSpanFrom(string text, int start, int statements)
    {
        var depth = 0;
        var seen = 0;
        var i = start;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i = LegacySkipVerbatimString(text, i + 2);
                continue;
            }

            if (c == '"')
            {
                i = LegacySkipRegularString(text, i + 1);
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                var nl = text.IndexOf('\n', i);
                i = nl < 0 ? text.Length : nl + 1;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = end < 0 ? text.Length : end + 2;
                continue;
            }

            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }
            else if (c == ';' && depth <= 0 && ++seen >= statements)
            {
                return text[start..(i + 1)];
            }

            i++;
        }

        return text[start..];
    }
}
