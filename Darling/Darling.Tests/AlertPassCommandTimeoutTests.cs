/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every read in the alert evaluation pass must carry an EXPLICIT command deadline (#2874).
///
/// <para>All forty-five commands across the six alert-pass types ran with no
/// <c>CommandTimeout</c>, so every one inherited Npgsql's undocumented 30 s default. On 2026-09-04
/// the forced-plan read failed five times on the production store, each surfacing as "Exception
/// while reading from stream" — how Npgsql renders its own deadline, and the exact misdiagnosis
/// #2826 exists to prevent.</para>
///
/// <para><b>Why this pin matches TWO construction shapes.</b> #2874's census counted 133 untimed
/// sites by scanning for <c>new NpgsqlCommand(</c>. That shape is not the only one: an
/// <c>NpgsqlDataSource</c> also hands out commands via <c>CreateCommand(sql)</c>, which inherits the
/// same default and which the census therefore missed entirely — four of this group's own sites are
/// that shape, in <c>DarlingPostgresAlertReadAdapter</c>. A pin keyed on one spelling would have
/// declared this family clean while a fifth of one file stayed on the inherited default, which is
/// the #2786 failure exactly: a guard that names the arm it was written for. Both shapes are matched
/// here, and the repo-wide recount is recorded on #2874.</para>
///
/// <para><b>Why this pin also walks a CALL GRAPH.</b> The scope above was a hardcoded list of six
/// filenames, and <c>DarlingWorker.cs</c> was not among them, so <c>ReadLatestCpuAsync</c> — the CPU
/// read <c>EvaluateAlertsAsync</c> performs before anything else in the pass — sat on the inherited
/// default while this guard declared the family clean (#2928 found it while censusing a neighbouring
/// regime). Adding that one filename would not do: <c>DarlingWorker.cs</c> holds seven command sites
/// belonging to four different budgets, so a whole-file sweep of it would fail on sites other groups
/// deliberately own. Adding the one <c>(file, member)</c> pair, the way
/// <c>CollectionSweepCommandTimeoutTests</c> does, would restate today's answer without expressing the
/// claim. So the mixed-regime files are scoped by their ENTRY POINT instead, and the members are
/// derived by walking the calls out of it — "runs inside <c>EvaluateAlertsAsync</c>" computed rather
/// than asserted, which is the only form of this scope that a future alert-pass helper cannot slip
/// past the same way this one did.</para>
///
/// <para>The VALUE is pinned separately below, and is bounded on both sides for reasons that do not
/// transfer from the two closed passes — see <c>DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds</c>.
/// The short version: this pass has no enclosing <c>CancelAfter</c>, so the per-command deadline is
/// the pass budget times the number of sequential reads, and it is deliberately set BELOW what it
/// inherited rather than above.</para>
/// </summary>
public sealed class AlertPassCommandTimeoutTests
{
    /// <summary>
    /// The six types DEDICATED to the alert evaluation pass — every command in them belongs to this
    /// budget, so they are swept whole-file. Files that mix this regime with others are not listed
    /// here; they go through <see cref="s_alertPassEntryPoints"/>.
    ///
    /// <para>Named explicitly rather than globbed, because "runs inside <c>EvaluateAlertsAsync</c>" is
    /// a budget boundary that no filename pattern expresses — a future file in this directory may belong to a different budget. That is not
    /// hypothetical: <c>PgPlanForceActionStore</c> looks like a member of this family and is not one.
    /// Its only caller is <c>PlanForceBot.RunAfterAnalysisAsync</c>, dispatched as the analysis pass's
    /// post-pass hook over the plain stopping token, so it shares the unbudgeted shape but runs on the
    /// analysis interval rather than this pass's 30 s cadence — a different upper bound, and therefore
    /// a different group.</para>
    ///
    /// <para>The same reasoning is what made this list INSUFFICIENT rather than wrong: it expresses
    /// "this whole file is alert-pass", which is true of these six and of nothing else the pass
    /// reads.</para>
    /// </summary>
    private static readonly string[] s_alertPassSources =
    {
        "DarlingAlertReadAdapter.cs",
        "DarlingPostgresAlertReadAdapter.cs",
        "PgAlertStateStore.cs",
        "PgMuteRuleStore.cs",
        "PgAlertHistoryStore.cs",
        "DarlingSelfAlertEvaluator.cs",
    };

    /// <summary>
    /// The pass's entry points in files that hold OTHER regimes too, as (file, member) pairs. The
    /// members swept are not these — they are every member reachable from these by a call, computed by
    /// <see cref="ReachableFrom"/>.
    ///
    /// <para>One entry today. <c>DarlingWorker.EvaluateAlertsAsync</c> is where the pass begins: it is
    /// dispatched from <c>ProcessServerSweepAsync</c> on <c>s_alertSweepInterval</c> with the plain
    /// stopping token, which is both why the pass has no enclosing budget and why everything it calls
    /// shares the bound derived for that budget.</para>
    ///
    /// <para><b>What the walk deliberately does NOT reach, measured rather than assumed.</b>
    /// <c>DarlingWorker.cs</c> has seven command sites and this scope claims exactly one. The other six
    /// are <c>TryRefreshPgStatementTextAsync</c> (2) and <c>ReadCollectorWatermarksAsync</c>, which are
    /// the collection sweep's (#2928); <c>ReadStoreSizeBytesAsync</c>, on the disk-check cadence; and
    /// <c>RunTestHypotheticalIndexAsync</c> / <c>RunExecuteActualPlanAsync</c>, which are the command
    /// plane. None is reachable from this entry point, so the derived scope excludes them without
    /// needing to name them — the property a whole-file sweep of this file could not have.</para>
    /// </summary>
    private static readonly (string File, string EntryPoint)[] s_alertPassEntryPoints =
    {
        ("DarlingWorker.cs", "EvaluateAlertsAsync"),
    };

    /// <summary>
    /// Command sites in the entry points' reachable members, counted so the census is a tripwire in
    /// both directions.
    ///
    /// <para>Downward it catches a walk that silently stopped reaching — a renamed helper, or an
    /// extractor returning an empty body, which is how a source-walking guard starts reporting clean on
    /// code it no longer reads. Upward it puts a person in front of a newly reachable site to decide
    /// whether it really shares this budget, rather than letting it inherit the answer from the call
    /// that happens to reach it.</para>
    /// </summary>
    private const int ExpectedEntryPointCommandSites = 1;

    /// <summary>
    /// Every way a command is constructed in this codebase. <c>new NpgsqlCommand(</c> is the shape
    /// #2810's pin matched; <c>.CreateCommand(</c> is the one it did not, and which hid four sites
    /// from #2874's census.
    ///
    /// <para>Two shapes are matched here that this family does not currently contain, and neither is
    /// speculative. The QUALIFIED construction <c>new Npgsql.NpgsqlCommand(</c> occurs once in the
    /// project — <c>DarlingWorker.ReadPgStatementTextAsync</c>, another regime's site, found while
    /// building the walk below — and is invisible to <c>new NpgsqlCommand\s*\(</c>, so an alert-pass
    /// read written that way would have been missed exactly as <c>ReadLatestCpuAsync</c> was. The bare
    /// <c>.CreateCommand</c> method group is carried for the reason
    /// <c>CollectionSweepCommandTimeoutTests</c> states: absent today is not a guard. Both leave the
    /// count at 45, so widening asserts nothing new about today's tree and costs nothing.</para>
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*NpgsqlCommand\s*\(|\.CreateCommand\s*\(|\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A member name followed by an optional generic argument list and then its parameter list.
    ///
    /// <para>The generic hole is not decoration — <c>WriteBatchAsync&lt;TRow&gt;(</c> is real in this
    /// project, and a pattern of <c>name\s*\(</c> would read zero hits as clean, which is the #2874
    /// trap of encoding the wrong shape. The lookbehind rejects a name preceded by <c>.</c>: a
    /// qualified name or a member access can never be a declaration, and without it
    /// <c>new Npgsql.NpgsqlCommand(</c> registers <c>NpgsqlCommand</c> as a member of this class.</para>
    /// </summary>
    private static readonly Regex s_memberSignature = new(
        @"(?<!\.)\b(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^<>()]*>)?\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_identifier = new(
        @"\b[A-Za-z_][A-Za-z0-9_]*\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Keywords that also read as <c>name (...) {</c>. Without these, every <c>if</c>, <c>foreach</c>
    /// and <c>using</c> block in the file registers as a member and the reachable set becomes the whole
    /// file — which would fail on the six sites this scope exists to EXCLUDE, and so would have been
    /// caught, but as a confusing false positive rather than as the shape error it is.
    /// </summary>
    private static readonly HashSet<string> s_notMemberNames = new(System.StringComparer.Ordinal)
    {
        "catch", "checked", "default", "do", "else", "fixed", "for", "foreach", "if", "lock", "nameof",
        "new", "return", "sizeof", "switch", "try", "typeof", "unchecked", "using", "when", "while",
    };

    /// <summary>
    /// The six DEDICATED files, swept whole — every command in them belongs to this budget.
    /// </summary>
    [Fact]
    public void EveryAlertPassCommand_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in AlertPassSources())
        {
            total += ScanForUntimedCommands(
                File.ReadAllText(path), Path.GetFileName(path), firstLine: 1, offenders);
        }

        Assert.True(total > 0, "the alert-pass scan matched no command constructions at all");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} alert-pass command(s) inherit Npgsql's 30s default instead of setting "
            + $"{nameof(DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds)}: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The mixed-regime files, swept over the members the pass actually REACHES.
    ///
    /// <para>This is the arm that was missing. <c>DarlingWorker.ReadLatestCpuAsync</c> is the pass's
    /// first read on every server on every 30 s tick, and it inherited Npgsql's 30 s default for the
    /// whole life of #2882 because the scope above is a list of filenames and its file is not on it.
    /// The scope here is not a longer list: it is the transitive closure of calls out of
    /// <c>EvaluateAlertsAsync</c>, so a read added to any member the pass reaches is covered on the day
    /// it is written rather than on the day someone remembers to extend an array.</para>
    /// </summary>
    [Fact]
    public void EveryAlertPassCommandReachedFromAnEntryPoint_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var (file, entryPoint) in s_alertPassEntryPoints)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(text);
            var members = MemberBodies(stripped);

            Assert.True(
                members.ContainsKey(entryPoint),
                $"the alert-pass entry point {entryPoint} has no block-bodied declaration in {file} — "
                + "a rename has moved the whole pass out from under this guard");

            var reachable = ReachableFrom(stripped, members, entryPoint);

            /* The positive control. Without it the walk could reach nothing but its own root and this
               test would report clean — which is precisely the failure the file-scoped scope above had,
               reproduced one level down. Named rather than counted: the reachable set's SIZE moves with
               any refactor of the Postgres predictors, while the site this pin exists for either is in
               the closure or the walk is broken. */
            Assert.Contains("ReadLatestCpuAsync", reachable);

            foreach (var member in reachable.OrderBy(m => m, System.StringComparer.Ordinal))
            {
                foreach (var (start, end) in members[member])
                {
                    total += ScanForUntimedCommands(
                        text[start..end], $"{file} {member}", LineOf(text, start), offenders);
                }
            }
        }

        /* Offenders BEFORE the census, so a real defect reports as a defect. Reversed, an
           added-and-untimed site would fail on the count and say nothing about the deadline it is
           missing. */
        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} alert-pass command(s) reachable from the pass entry point inherit "
            + "Npgsql's 30s default instead of setting "
            + $"{nameof(DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds)}: "
            + string.Join(", ", offenders));

        Assert.Equal(ExpectedEntryPointCommandSites, total);
    }

    /// <summary>
    /// The value, bounded on both sides.
    ///
    /// <para>ABOVE the measured worst case: the shipped queries timed against the production store's
    /// three busiest servers put the whole pass's cost in one read — the forced-plan check at
    /// 1,744.9 ms cold over ~6.0 GB, with every other read under 3 ms. A floor of 5 s keeps real
    /// headroom over that.</para>
    ///
    /// <para>BELOW the 30 s <c>s_alertSweepInterval</c> this pass runs on, so one stalled read still
    /// leaves the pass able to finish inside the interval that restarts it — and so the value stays
    /// under the default it replaces, which is the whole point of this change.</para>
    /// </summary>
    [Fact]
    public void TheAlertPassDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

        Assert.True(
            seconds >= 5,
            $"alert-pass deadline {seconds}s is at or under the measured 1.7s worst case with no "
            + "meaningful headroom — a stall a little worse than normal would fail the read");

        Assert.True(
            seconds < 30,
            $"alert-pass deadline {seconds}s is at or above Npgsql's inherited 30s default, so it "
            + "buys nothing: this pass has no enclosing CancelAfter, and the per-command value is "
            + "the pass budget times the number of sequential reads");
    }

    /// <summary>
    /// The scanner above is what forty-five sites' correctness is asserted through, so its own blind
    /// spots are worth pinning: a false positive here fails a green build on correct code.
    ///
    /// <para>Both cases are shapes that actually occur. The first is the <c>CreateCommand</c> site
    /// with an interposed explanatory comment containing a semicolon — the exact gap where this
    /// codebase's style puts one. The second is the verbatim-SQL construction, where the deadline sits
    /// twenty-odd lines below the opening and past any fixed line window.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "var command = _postgres.CreateCommand(Sql);\n"
        + "/* set separately here; a method result cannot take an initializer. */\n"
        + "command.CommandTimeout = 10;\n",
        true)]
    [InlineData(
        "var command = new NpgsqlCommand(@\"\nSELECT 1;\nSELECT 2;\n\", connection) { CommandTimeout = 10 };\n",
        true)]
    [InlineData(
        "var command = _postgres.CreateCommand(Sql);\n"
        + "await command.ExecuteNonQueryAsync();\n",
        false)]
    /* A comment that NAMES a deadline is not one. This case was green before the value regex moved onto
       the stripped span: the construction match excluded comments while the deadline test did not, so
       prose could satisfy it. False-negative direction - it reported success on the defect. */
    [InlineData(
        "var command = new NpgsqlCommand(Sql, connection);\n"
        + "/* no deadline needed here; CommandTimeout = 10 is applied by the caller. */\n"
        + "await command.ExecuteNonQueryAsync();\n",
        false)]
    public void TheScanner_SeesADeadlineThroughCommentsAndVerbatimSql(string source, bool expectedTimed)
    {
        /* Stripped, exactly as ScanForUntimedCommands does it - a fixture that walked the raw text would
           pass while the shipped scan failed, which is how this defect survived its own theory. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var ctor = s_commandCtor.Match(code);
        Assert.True(ctor.Success, "the fixture did not contain a command construction");

        var span = CSharpSourceWalker.StatementSpanFrom(code, ctor.Index, statements: 2);

        Assert.Equal(expectedTimed, s_setsTimeout.IsMatch(span));
    }

    /* The literal- and comment-aware walk this pin used to carry lives in CSharpSourceWalker as of
       #2913. It was one of five private copies, and all five blanked an interpolated string's HOLES along
       with the literal text around them, so a call written inside an interpolation was invisible to every
       scan built on them. The reasoning that shaped the walk moved there with it, and
       CSharpSourceWalkerTests carries the witnesses. Verbatim SQL in this family carries both
       semicolons and quote characters, which is why the span walker was never a naive scan for ';'. */

    /// <summary>
    /// The walk, pinned separately. It is the piece with no precedent in this pin's own history, and a
    /// closure that resolves to the WRONG members — or to none — would report clean on whatever it
    /// failed to read, which is exactly how the site this PR fixes stayed hidden.
    ///
    /// <para>The fixture carries every shape that can go wrong, so one green run is evidence about
    /// each. <c>RootAsync</c> must reach <c>DeeperAsync</c> TRANSITIVELY, through a member that
    /// contains no command of its own. <c>OtherRegimeAsync</c> holds an UNTIMED command and must not be
    /// reached, which is the claim that lets this scope be narrower than its file — get that wrong and
    /// the pin fails on sites #2928 and the command-plane group own. And the two constructions that
    /// look like declarations must not register as members: <c>new Dictionary&lt;int, int&gt;(4) { }</c>
    /// (an initialiser, not a body) and <c>new Npgsql.NpgsqlCommand(</c> (a qualified type name). Both
    /// were observed doing exactly that while this walk was being built.</para>
    ///
    /// <para><c>ExprBodiedAsync</c> is the shape review caught: an <c>=&gt;</c>-bodied member reached
    /// only by a METHOD-GROUP reference, holding an untimed command. Both halves are load-bearing
    /// together — before the fix it did not register as a member at all, so it was neither swept nor
    /// reported, and a count-based control could not see the hole. It is written the way the three real
    /// ones are (<c>Select(BuildPgDeadlockIncident)</c>), not as a call, because a method group is what
    /// the graph has to follow.</para>
    /// </summary>
    [Fact]
    public void TheReachabilityWalk_FollowsCallsAndStopsAtOtherRegimes()
    {
        const string Source =
            "class C\n"
            + "{\n"
            + "    async Task RootAsync()\n"
            + "    {\n"
            + "        var seen = new Dictionary<int, int>(4) { [1] = 2 };\n"
            + "        if (seen.Count > 0) { await ReachedAsync(); }\n"
            + "    }\n"
            + "    async Task ReachedAsync()\n"
            + "    {\n"
            + "        await DeeperAsync();\n"
            + "    }\n"
            + "    async Task DeeperAsync()\n"
            + "    {\n"
            + "        using var timed = new Npgsql.NpgsqlCommand(Sql, conn) { CommandTimeout = 10 };\n"
            + "        var mapped = rows.Select(ExprBodiedAsync).ToList();\n"
            + "        await Task.Run(async (x) => { await LocalHelper(x); });\n"
            + "        async Task LocalHelper(int x)\n"
            + "        {\n"
            + "            using var nested = new NpgsqlCommand(Sql, conn) { CommandTimeout = 10 };\n"
            + "        }\n"
            + "    }\n"
            + "    Task<int> ExprBodiedAsync(int id) => Run(new NpgsqlCommand(Sql, conn));\n"
            + "    async Task OtherRegimeAsync()\n"
            + "    {\n"
            + "        using var untimed = new NpgsqlCommand(Sql, conn);\n"
            + "    }\n"
            + "}\n";

        var stripped = CSharpSourceWalker.StripCommentsAndStrings(Source);
        var members = MemberBodies(stripped);

        Assert.DoesNotContain("Dictionary", members.Keys);
        Assert.DoesNotContain("NpgsqlCommand", members.Keys);
        Assert.DoesNotContain("if", members.Keys);

        /* The nested matches: a local function and an async lambda, both inside DeeperAsync's body.
           Neither may register as its own node, or the sweep scans their text twice - once through
           DeeperAsync's span and once through their own - and the census inflates. */
        Assert.DoesNotContain("LocalHelper", members.Keys);
        Assert.DoesNotContain("async", members.Keys);

        var reachable = ReachableFrom(stripped, members, "RootAsync");

        Assert.Equal(
            new[] { "DeeperAsync", "ExprBodiedAsync", "ReachedAsync", "RootAsync" },
            reachable.OrderBy(m => m, System.StringComparer.Ordinal).ToArray());

        /* The closure's own command census: THREE sites — the block-bodied timed one, the timed one
           inside the local function (reached through its enclosing member's span, counted ONCE), and the
           expression-bodied UNTIMED one, which is reported rather than skipped. Without the
           drop-nested-matches step this reads FOUR, because the local function's command is scanned
           through both spans. */
        var offenders = new List<string>();
        var total = 0;

        foreach (var member in reachable.OrderBy(m => m, System.StringComparer.Ordinal))
        {
            foreach (var (start, end) in members[member])
            {
                total += ScanForUntimedCommands(
                    Source[start..end], member, LineOf(Source, start), offenders);
            }
        }

        Assert.Equal(3, total);
        Assert.Equal(new[] { "ExprBodiedAsync:22" }, offenders.ToArray());

        /* The negative control, and the reason the whole design holds: the excluded member really does
           carry a defect the scanner would report. So "no offenders" above is the SCOPE working, not
           the scanner failing to see anything. */
        var excluded = new List<string>();
        var (otherStart, otherEnd) = members["OtherRegimeAsync"][0];

        Assert.Equal(
            1,
            ScanForUntimedCommands(
                Source[otherStart..otherEnd], "OtherRegimeAsync", LineOf(Source, otherStart), excluded));
        Assert.Single(excluded);
    }

    /// <summary>
    /// Command constructions in <paramref name="text"/> that set no deadline, appended to
    /// <paramref name="offenders"/>; returns how many constructions were examined.
    ///
    /// <para>ONE scanner for both scopes, deliberately. The file sweep and the call-graph sweep must
    /// not be able to disagree about what "carries a deadline" means, or a site that moves from one
    /// scope to the other changes answer without anything changing about the site.</para>
    ///
    /// <para>Matched over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output so a
    /// construction named in PROSE is not counted as a site — these files carry long explanatory
    /// comments about the commands beside them, and a mention would arrive as a false offender on
    /// correct code. Spans are then cut from the ORIGINAL text, which is sound because the strip is
    /// length-preserving; that is the same stripped-walk/raw-span split the shared walker uses.</para>
    ///
    /// <para>Scan to the END OF THE STATEMENT, not a fixed number of lines. A line window cannot work
    /// here and the first draft of this pin proved it: these sites embed verbatim SQL, so the
    /// construction routinely spans twenty-plus lines and the initializer that carries the deadline
    /// sits past any window small enough not to run into the following member. A window wide enough to
    /// catch it would instead read the NEXT command's deadline and call an untimed site clean — the
    /// failure that actually matters, because it reports success on the defect.</para>
    ///
    /// <para>The statement span is exact and needs no tuning: the deadline is either an object
    /// initializer on the construction, or — for the <c>CreateCommand</c> shape, whose method result
    /// cannot take one — the statement immediately after it, so two statements are examined. It is also
    /// why <c>ReadLatestCpuAsync</c> takes the initializer form rather than a trailing assignment: the
    /// <c>AddWithValue</c> call already occupies the second statement, so an assignment placed after it
    /// would sit outside this window and read as untimed.</para>
    /// </summary>
    private static int ScanForUntimedCommands(
        string text, string label, int firstLine, List<string> offenders)
    {
        var total = 0;

        /* ONE stripped copy, used for the construction match AND for the deadline test.

           The value regex used to run over a span cut from the RAW text while the construction regex ran
           over stripped output - half stripped, half not, in one method - so a comment was excluded from
           inventing a SITE but not from satisfying a DEADLINE. a comment reading "no deadline here; CommandTimeout = 10 is set
           by the caller" made an untimed command read clean, which is the false-negative
           direction: it reports success on the defect. Found by #2874's group D in its own pin and
           confirmed here; it masks nothing on the current tree (46 alert-pass sites, 0 affected, measured)
           and is fixed because the next one would be invisible.

           Stripping literal TEXT as well as comments is safe for THIS regex and only for this one: the
           deadline is always code (`CommandTimeout = <expr>`), never a value inside a string. A pin whose
           value regex must see literal contents would be broken by this line, so it is not a change to
           make family-wide by reflex. Line numbers survive because the strip preserves newlines. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);

        foreach (Match ctor in s_commandCtor.Matches(code))
        {
            total++;

            var span = CSharpSourceWalker.StatementSpanFrom(code, ctor.Index, statements: 2);

            if (!s_setsTimeout.IsMatch(span))
            {
                /* Reported as the line in the FILE, not an offset into the scanned region, so a member
                   scope's offender is as navigable as a whole-file scope's. */
                offenders.Add($"{label}:{firstLine + LineOf(code, ctor.Index) - 1}");
            }
        }

        return total;
    }

    /// <summary>
    /// Every block-bodied member declared in <paramref name="stripped"/>, as name to (start, end) spans
    /// covering the signature through the matching close brace.
    ///
    /// <para>Brace-matched over the STRIPPED text, because a brace inside verbatim SQL or a comment
    /// would otherwise close a body early — and <c>DarlingWorker</c> is full of both. Overloads are
    /// kept as multiple spans under one name and all of them are swept: a name-keyed walk cannot tell
    /// which overload a call resolves to, and sweeping all of them errs toward demanding a deadline
    /// rather than toward missing one.</para>
    ///
    /// <para><b>BOTH body shapes, and the expression-bodied one is not a nicety.</b> The first cut of
    /// this walk took only <c>) {</c>, so an <c>=&gt;</c>-bodied member never registered here at all —
    /// and <c>ReachableFrom</c>'s <c>ContainsKey</c> test then made it invisible rather than
    /// unreachable, silently, with no count moving. Review caught it, and it was already live on this
    /// exact call graph: <c>BuildPgDeadlockIncident</c>, <c>BuildPgBlockingIncident</c> and
    /// <c>BuildPgLongRunningQueryIncident</c> are all <c>internal static ... =&gt; new(...)</c>, all
    /// referenced by METHOD GROUP (<c>rows.Select(BuildPgDeadlockIncident)</c>) from members that are
    /// in the closure. They hold no commands, so no census moved and nothing failed — which is exactly
    /// what made it worth fixing: turning a small read into
    /// <c>private Task&lt;Foo&gt; ReadFooAsync(...) =&gt; ...;</c> is an ordinary refactor, and it
    /// would have been invisible to this pin the same way <c>ReadLatestCpuAsync</c> was invisible to
    /// the file-list scope this pin exists to replace. The same defect one level down.</para>
    ///
    /// <para>Note this is the OPPOSITE choice from
    /// <c>CollectionSweepCommandTimeoutTests.BodyStart</c>, deliberately. That helper uses the
    /// block-body test to pick one NAMED member's declaration out of its own call sites, so treating
    /// an expression-bodied match as "not a declaration" is right there. This one enumerates every
    /// declaration as a graph NODE, so a shape it cannot see is a hole in the graph.</para>
    ///
    /// <para><b>NESTED matches are dropped, and that is a correctness requirement rather than tidying.</b>
    /// Accepting both body shapes means a construct written INSIDE another member's body can match too —
    /// a local function, or an <c>async (x) =&gt; { ... }</c> lambda, whose span sits wholly within its
    /// enclosing member's span. Keeping both would make the sweep scan that text TWICE, once through the
    /// enclosing span and once through its own, inflating the census and duplicating any offender line;
    /// review flagged it, and probing the real file found exactly one such match today (an async lambda
    /// registering under the name <c>async</c>), reachable from nothing, so it was latent. Nothing is lost
    /// by dropping them: a local function is scoped to its containing member, so it can have no caller the
    /// enclosing span does not already cover, and its identifiers and its commands are both inside that
    /// span already. A nested TYPE's methods are unaffected — a type declaration has no parameter list, so
    /// it never matches here and its members stay top-level.</para>
    /// </summary>
    private static Dictionary<string, List<(int Start, int End)>> MemberBodies(string stripped)
    {
        var found = new List<(string Name, int Start, int End)>();

        foreach (Match signature in s_memberSignature.Matches(stripped))
        {
            var name = signature.Groups["name"].Value;

            if (s_notMemberNames.Contains(name) || PrecededByNew(stripped, signature.Index))
            {
                continue;
            }

            var end = DeclarationEnd(stripped, signature.Index);

            if (end < 0)
            {
                continue;
            }

            found.Add((name, signature.Index, end));
        }

        var members = new Dictionary<string, List<(int Start, int End)>>(System.StringComparer.Ordinal);

        foreach (var (name, start, end) in found)
        {
            if (found.Any(outer => outer.Start < start && end <= outer.End))
            {
                continue;
            }

            if (!members.TryGetValue(name, out var spans))
            {
                members[name] = spans = new List<(int Start, int End)>();
            }

            spans.Add((start, end));
        }

        return members;
    }

    /// <summary>
    /// The transitive closure of calls out of <paramref name="root"/>, within this file.
    ///
    /// <para>An edge is any IDENTIFIER in a member's body that names another member, rather than a
    /// parsed call expression. That OVER-approximates — a member passed as a method group, or named in
    /// an expression that is never invoked, still counts — and the direction is deliberate: an extra
    /// member in the closure can only demand a deadline on a site that does not need one, which fails
    /// loudly and is fixed by moving the site or the scope. A missing edge reports clean on a site that
    /// inherits 30 s, which is the failure this pin was written twice to prevent.</para>
    ///
    /// <para>Intra-file by design. The reads the pass makes through OTHER types —
    /// <c>DarlingAlertReadAdapter</c> and the four stores — are covered whole-file by
    /// <see cref="s_alertPassSources"/>, which is strictly broader than any closure over them would
    /// be.</para>
    /// </summary>
    private static HashSet<string> ReachableFrom(
        string stripped, Dictionary<string, List<(int Start, int End)>> members, string root)
    {
        var reached = new HashSet<string>(System.StringComparer.Ordinal);
        var pending = new Stack<string>();
        pending.Push(root);

        while (pending.Count > 0)
        {
            var current = pending.Pop();

            if (!reached.Add(current))
            {
                continue;
            }

            foreach (var (start, end) in members[current])
            {
                foreach (Match identifier in s_identifier.Matches(stripped[start..end]))
                {
                    if (members.ContainsKey(identifier.Value) && !reached.Contains(identifier.Value))
                    {
                        pending.Push(identifier.Value);
                    }
                }
            }
        }

        return reached;
    }

    /// <summary>
    /// Whether the name at <paramref name="at"/> is the type of a <c>new</c> expression rather than a
    /// declaration — <c>new Dictionary&lt;int, int&gt;(4) { [1] = 2 }</c> has the same
    /// <c>name (...) {</c> shape as a member and its braces open an initialiser.
    ///
    /// <para>The step back over <c>.</c>-separated segments is what makes it see <c>new</c> through a
    /// QUALIFIED type name. Without it <c>new Npgsql.NpgsqlCommand(</c> registers a member called
    /// <c>NpgsqlCommand</c>, which the walk then treats as callable — observed, and the reason this
    /// helper exists rather than a check of the single preceding word.</para>
    /// </summary>
    private static bool PrecededByNew(string stripped, int at)
    {
        var i = at - 1;

        while (true)
        {
            while (i >= 0 && char.IsWhiteSpace(stripped[i]))
            {
                i--;
            }

            if (i < 0 || stripped[i] != '.')
            {
                break;
            }

            i--;

            while (i >= 0 && char.IsWhiteSpace(stripped[i]))
            {
                i--;
            }

            while (i >= 0 && (char.IsLetterOrDigit(stripped[i]) || stripped[i] == '_'))
            {
                i--;
            }
        }

        var end = i + 1;

        while (i >= 0 && (char.IsLetterOrDigit(stripped[i]) || stripped[i] == '_'))
        {
            i--;
        }

        return stripped[(i + 1)..end] == "new";
    }

    /// <summary>
    /// Index just past the body of the declaration at <paramref name="at"/>, or -1 when what follows
    /// the parameter list is neither a block nor <c>=&gt;</c> — which is how a CALL to a member is told
    /// apart from a declaration of one.
    ///
    /// <para>The token after the parameter list is the whole discriminator, and it is sound in both
    /// directions: only a declaration can be followed by <c>=&gt;</c> there (a lambda's <c>=&gt;</c>
    /// has no <c>name(</c> before its parameter list, and <c>new T(...) { }</c> is rejected upstream by
    /// <see cref="PrecededByNew"/>), while a call is followed by <c>;</c>, <c>)</c>, <c>,</c> or
    /// <c>.</c>.</para>
    ///
    /// <para>The expression-bodied span is delegated to the SHARED walker rather than scanning for
    /// <c>;</c> here: an expression body can carry a block-bodied lambda, verbatim SQL and interpolated
    /// holes, and <see cref="CSharpSourceWalker.StatementSpanFrom"/> already counts only code
    /// characters at bracket depth zero or below — the reason it exists.</para>
    /// </summary>
    private static int DeclarationEnd(string stripped, int at)
    {
        var depth = 0;

        for (var i = stripped.IndexOf('(', at); i >= 0 && i < stripped.Length; i++)
        {
            if (stripped[i] == '(')
            {
                depth++;
            }
            else if (stripped[i] == ')' && --depth == 0)
            {
                for (var j = i + 1; j < stripped.Length; j++)
                {
                    if (char.IsWhiteSpace(stripped[j]))
                    {
                        continue;
                    }

                    if (stripped[j] == '{')
                    {
                        return BlockEnd(stripped, j);
                    }

                    return stripped[j] == '=' && j + 1 < stripped.Length && stripped[j + 1] == '>'
                        ? at + CSharpSourceWalker.StatementSpanFrom(stripped, at, statements: 1).Length
                        : -1;
                }

                return -1;
            }
        }

        return -1;
    }

    /// <summary>Index just past the <c>}</c> matching the brace at <paramref name="open"/>, or -1.</summary>
    private static int BlockEnd(string stripped, int open)
    {
        var depth = 0;

        for (var i = open; i < stripped.Length; i++)
        {
            if (stripped[i] == '{')
            {
                depth++;
            }
            else if (stripped[i] == '}' && --depth == 0)
            {
                return i + 1;
            }
        }

        return -1;
    }

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string SourcePath(string file)
    {
        var path = Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", file);

        /* A renamed or moved entry-point file must fail loudly rather than silently shrinking the scan
           to what still resolves — an empty sweep is how a guard starts reporting clean. */
        Assert.True(File.Exists(path), $"alert-pass entry point source not found: {path}");

        return path;
    }

    private static IEnumerable<string> AlertPassSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        var paths = s_alertPassSources
            .Select(f => Path.Combine(dir, f))
            .ToArray();

        /* A renamed or moved alert-pass type must fail loudly here rather than silently shrinking the
           scan to the files that still resolve — an empty or partial sweep is how a guard starts
           reporting clean on code it no longer reads. */
        foreach (var path in paths)
        {
            Assert.True(File.Exists(path), $"alert-pass source not found: {path}");
        }

        return paths;
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
}
