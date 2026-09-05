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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every command in <c>PerformanceMonitor.Darling.Viewer</c> must carry an EXPLICIT deadline (#2874).
/// All 193 of the project's command sites ran on Npgsql's undocumented 30 s default — a value nobody
/// chose, and the defect class behind three production failures (#2810, #2871, #2796): exceeding the
/// ceiling surfaces as <c>Exception while reading from stream</c>, which reads as a network fault
/// rather than a deadline.
///
/// <para><b>Directory-scoped, like the <c>.Storage</c> pin and unlike the alert-pass one.</b>
/// <c>AlertPassCommandTimeoutTests</c> enumerates six files because its claim is "runs inside
/// <c>EvaluateAlertsAsync</c>", a budget boundary no filename expresses. This pin's claim is the same
/// as <c>StorageCommandTimeoutTests</c>': every command this project creates must have had its
/// deadline chosen on purpose, whichever regime's constant that is. That is a property of the
/// project, so the sweep globs it and a future file is covered the day it appears rather than when
/// someone remembers to enlist it.</para>
///
/// <para><b>Values are pinned as BANDS, never as equalities.</b> Each band encodes the reasoning that
/// produced the number — the measured floor below it and the shared budget above it — so a future
/// re-derivation inside the band is free and a drift out of it has to argue with the derivation on
/// <see cref="ViewerCommandDeadlines"/>. Freezing the numbers themselves would couple three
/// deliberate values to one test, which is what the <c>.Storage</c> pin declined to do.</para>
/// </summary>
public sealed class ViewerCommandTimeoutTests
{
    /// <summary>
    /// Both ways a command is constructed in this codebase — <c>new NpgsqlCommand(</c> and
    /// <c>.CreateCommand(</c>. The second is the shape #2874's original census missed entirely, and
    /// here it is 191 of the 193 sites.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A <c>CreateCommand</c> handed over as a METHOD GROUP rather than called — the shape that is
    /// invisible to <see cref="s_commandCtor"/> because no <c>(</c> follows it, and the one command
    /// site in this project that survived the sweep of the other 192.
    /// </summary>
    private static readonly Regex s_commandFactoryHandoff = new(
        @"\.CreateCommand\s*[,)]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A joined fan-out — the shape whose reads run concurrently and therefore cannot be bounded by a
    /// deadline derived from one read in isolation (#3004).
    /// </summary>
    private static readonly Regex s_joinedFanOut = new(
        @"Task\s*\.\s*WhenAll\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The declaration that tells those reads how many of them there are.</summary>
    private static readonly Regex s_fanOutDeclaration = new(
        @"ViewerReadFanOut\s*\.\s*Of\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A fire-and-forget call — <c>_ = SomethingAsync();</c>. Two of them in one member is a fan-out; ONE
    /// is this project's ordinary single-flight-guarded refresh and is not.
    ///
    /// <para>Deliberately WIDER than the identical-looking expression in
    /// <c>ViewerFleetTimerGuardTests</c> and <c>ViewerFleetTimerFanOutPositionTests</c>, which use
    /// <c>(\w+)</c>: those two ask a question about named calls inside ONE known tick, so a name they
    /// cannot spell is a name they do not need. This is a project-wide census, and a census blind to
    /// <c>_ = Controller.ReadAsync();</c> would report a clean sweep over a shape it never looked at —
    /// which is the #3019 failure one level down.</para>
    ///
    /// <para><b>Unconstrained about the callee, unlike <see cref="s_deferredRead"/>, and that asymmetry is
    /// a deliberate trade in BOTH directions.</b> Requiring an <c>Async</c> suffix here is not available:
    /// three of this project's fifteen discarded call names are genuinely async without it —
    /// <c>OpenPlanTab</c> (<c>private async Task</c>), <c>OnHeatmapDrillDown</c> and
    /// <c>PlanViewerController.LoadPlanIntoSubTab</c> — so the suffix rule would reintroduce exactly the
    /// silent gap #3019 is about. The cost paid instead is a false-positive path: <c>_ =</c> compiles for
    /// any non-void call, so two discarded SYNCHRONOUS helpers in one member read as a two-wide fan-out and
    /// would be asked for a width they do not need. Nothing in this project does that today (every
    /// <c>_ = X(...)</c> site is a Task-returning call), the direction is a loud failure rather than a
    /// silent one, and the shape is pinned in
    /// <see cref="TheFanOutCensus_RecognisesEachShape_AndOnlyThose"/> so it reads as a known cost rather
    /// than a surprise. The deferred rule can afford the suffix because its shape —
    /// <c>var t = Call();</c> with no <c>await</c> — is otherwise indistinguishable from ordinary local
    /// assignment, which is most lines in the project.</para>
    /// </summary>
    private static readonly Regex s_fireAndForget = new(
        @"(^|[^A-Za-z0-9_])_\s*=\s*[A-Za-z_][A-Za-z0-9_\.]*\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A task STARTED into a local without being awaited there — <c>var t = SomethingAsync();</c>. Two of
    /// these with no <c>await</c> between them is the same fan-out as <c>Task.WhenAll(t1, t2)</c>, written
    /// with the awaits one per line instead of joined.
    ///
    /// <para><b>A bare <c>_</c> is excluded from the name, so this shape and
    /// <see cref="s_fireAndForget"/> stay disjoint.</b> <c>var _ = SomeAsync();</c> would otherwise satisfy
    /// both — <c>_</c> is a legal name here, and the discard scan matches the same characters — and one
    /// physical call would be counted into two different fan-out tallies, pairing against an unrelated
    /// deferred start on one side and an unrelated discard on the other. It belongs to the DISCARD shape:
    /// nothing holds the task, which is what a discard is. No site in this project spells it that way
    /// today. Pinned by <see cref="TheDiscardAndDeferredShapes_StayDisjoint"/>. Found in review.</para>
    /// </summary>
    private static readonly Regex s_deferredRead = new(
        @"(^|[^A-Za-z0-9_])(?:var|Task(?:\s*<[^;={}]*>)?)\s+(?!_\s*=)[A-Za-z_][A-Za-z0-9_]*\s*=\s*[A-Za-z_][A-Za-z0-9_\.]*Async\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>An <c>await</c> as a keyword, not as part of a longer identifier.</summary>
    private static readonly Regex s_await = new(
        @"(^|[^A-Za-z0-9_])await[^A-Za-z0-9_]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A member declaration with a body — accessibility, optional modifiers, return type, name, parameter
    /// list, an optional generic constraint clause, open brace. The unit fan-outs are paired within: see
    /// <see cref="EveryViewerFanOut_DeclaresItsWidth"/> for why the enclosing BLOCK is the wrong unit.
    ///
    /// <para><b>A generic method needs BOTH halves, and the constraint clause is only the visible one.</b>
    /// Its type parameters sit between the NAME and the parameter list, and its constraints sit between the
    /// parameter list and the BODY, so a tail of <c>\)\s*\{</c> against a bare name drops the member
    /// entirely. Three are shaped that way today — <c>ViewerSettingsFile.Load&lt;T&gt;</c> and
    /// <c>Save&lt;T&gt;</c>, and <c>SettingsWindow.TryReadSectionAsync&lt;T&gt;</c>. None holds a fan-out
    /// marker, so the walk stayed green while seeing less than it claimed; the first one to grow a marker
    /// would have tripped the unattributed assertion rather than being reported as an offender, which is a
    /// confusing red rather than the useful one.</para>
    ///
    /// <para><b>The type-parameter group excludes <c>=</c> and newlines, which is load-bearing.</b> Allowing
    /// them let it run from a field's declared type through <c>= new Dictionary&lt;...&gt;(comparer)</c> to
    /// the collection initializer's brace, reading <c>CollectorSchedulePresets.Presets</c> — a field — as a
    /// method whose body is the initializer. A phantom body is worse than a missing one here: <see
    /// cref="Owner"/> takes the OUTERMOST match, so one could swallow a real member's markers and attribute
    /// them to something that can never declare a width. Measured while widening this pattern, not
    /// hypothesised. Pinned by <see cref="TheMemberWalk_SeesAGenericMethodWithAConstraintClause"/>.
    /// Found in review.</para>
    /// </summary>
    private static readonly Regex s_memberSignature = new(
        @"(?:private|public|protected|internal)(?:\s+(?:static|async|override|virtual|sealed|new|partial|unsafe|extern))*"
        + @"\s+[A-Za-z_][A-Za-z0-9_<>,\.\[\]\?\s]*?\s(?<name>[A-Za-z_][A-Za-z0-9_]*)"
        + @"\s*(?:<[^;{}()=\n]*>)?\s*\([^;{}]*\)\s*(?:where\s[^{};]*)?\{",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The concurrency primitives this project does not use and whose fan-outs
    /// <see cref="EveryViewerFanOut_DeclaresItsWidth"/> therefore does not model. Pinned as ABSENT rather
    /// than handled — see <see cref="NoUnmodelledConcurrencyPrimitive_ReachesTheViewer"/>.
    /// </summary>
    private static readonly Regex s_unmodelledConcurrency = new(
        @"Task\s*\.\s*WhenAny\s*\(|Task\s*\.\s*Factory\s*\.|Parallel\s*\.\s*(?:For|Invoke)|\.\s*ContinueWith\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryViewerCommand_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in ViewerSources())
        {
            var text = File.ReadAllText(path);

            /* Both halves of the question are asked of STRIPPED text, which is character-aligned with its
               input so an offset means the same thing in either. A construction named only in prose is not a
               construction, and a deadline merely SPELLED in a comment is not a deadline - judged raw, a note
               explaining where the deadline used to be stands in for the deadline. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                total++;

                if (!CommandDeadlineScanner.SetsAnExplicitDeadline(code, ctor.Index))
                {
                    var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        /* 193 sites at the time this pin landed; the floor guards against the sweep silently reading
           an empty or wrong directory, not against refactors that change the count. */
        Assert.True(total >= 150, $"the viewer scan matched only {total} command constructions — the sweep is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} viewer command(s) inherit Npgsql's 30s default instead of an explicit deadline: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// No command may be created by a delegate this project hands to someone else, because a deadline
    /// set here is then the only one it can get.
    ///
    /// <para>This is the hole #2874's census could not see. <c>ViewerDataService.Blocking.cs</c> passed
    /// <c>connection.CreateCommand</c> as a bare method group into
    /// <c>PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync</c>, which constructs the command in a
    /// DIFFERENT project (<c>PerformanceMonitor.Darling.Analysis</c>) and sets its <c>CommandText</c>
    /// there. Neither of #2874's census regexes matches a method group — there is no <c>(</c> after it —
    /// so that site read as clean while inheriting the default, and the fix is a factory lambda that
    /// stamps the deadline before returning. Comments and string literals are stripped first, so the
    /// prose explaining that fix cannot fail this test.</para>
    /// </summary>
    [Fact]
    public void NoViewerCommand_IsCreatedByABareMethodGroupHandoff()
    {
        var offenders = new List<string>();

        foreach (var path in ViewerSources())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

            foreach (Match handoff in s_commandFactoryHandoff.Matches(code))
            {
                var line = code.Take(handoff.Index).Count(c => c == '\n') + 1;
                offenders.Add($"{Path.GetFileName(path)}:{line}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} site(s) hand CreateCommand over as a method group, so the callee builds an "
            + "untimed command this project cannot reach — pass a factory lambda that sets the deadline: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The interactive/refresh deadline, bounded on both sides — full derivation on
    /// <see cref="ViewerCommandDeadlines.InteractiveReadSeconds"/>. Short form: the heaviest shipped
    /// per-server read measured 3.01 s COLD on a V109 store seeded to production per-server density
    /// across the full 30-day retention horizon, and nothing encloses these reads, so the deadline is
    /// itself the budget and has to sit under the default it replaces rather than at it.
    /// </summary>
    [Fact]
    public void TheInteractiveReadDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ViewerCommandDeadlines.InteractiveReadSeconds;

        Assert.True(
            seconds > 3,
            $"interactive read deadline {seconds}s is at or under the 3.01 s worst measured shipped read "
            + "(TopQueriesSql over a 30-day custom range at production density) — it would fail a legitimate panel");

        Assert.True(
            seconds < 30,
            $"interactive read deadline {seconds}s is not meaningfully under the inherited Npgsql default it "
            + "replaces. These reads have no enclosing budget and compete for a ten-connection pool that one "
            + "control can take entirely, so every second granted here is a second the whole viewer can spend "
            + "holding permits while the user watches a spinner");
    }

    /// <summary>
    /// The command-plane deadline, pinned RELATIONALLY against the budget it actually sits inside
    /// rather than against a copied number: <c>PollCommandResultAsync</c> re-issues the poll until
    /// <see cref="ViewerDataService.DefaultCommandTimeout"/>, and checks that budget only BETWEEN
    /// iterations — so a read deadline at or above the loop's budget puts the read back in charge of
    /// how long the dialog waits, which is the defect the constant exists to close.
    /// </summary>
    [Fact]
    public void TheCommandPlaneDeadline_StaysWellInsideItsEnclosingBudget()
    {
        var seconds = ViewerCommandDeadlines.CommandPlaneSeconds;
        var enclosing = ViewerDataService.DefaultCommandTimeout.TotalSeconds;

        Assert.True(
            seconds >= 1,
            $"command-plane deadline {seconds}s leaves no room over the 3.9 ms cold single-row poll, and the "
            + "delete is what removes a credential-bearing args_json row");

        Assert.True(
            seconds * 2 < enclosing,
            $"command-plane deadline {seconds}s is not comfortably inside the {enclosing}s poll-loop budget it "
            + "runs under — the loop checks its budget only between iterations, so one read this long overshoots "
            + "the wait the dialog promises");
    }

    /// <summary>
    /// The connect-gate deadline. Bounded below by a catalog probe that does not grow with the store
    /// (79 ms cold for all 85 schema sentinels) and above by the connect preference's own ceiling —
    /// these are the first two statements after connect, they run before the window is usable, and
    /// both swallow their own failures, so overshooting mis-classifies the store silently.
    /// </summary>
    [Fact]
    public void TheConnectGateDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ViewerCommandDeadlines.ConnectGateSeconds;

        Assert.True(
            seconds >= 1,
            $"connect-gate deadline {seconds}s leaves no room over the 79 ms cold schema probe; a probe that "
            + "times out fails OPEN or SAFE, so it hides write affordances rather than reporting anything");

        Assert.True(
            seconds <= 60,
            $"connect-gate deadline {seconds}s exceeds the 60s ceiling the viewer's own connection-timeout "
            + "preference is clamped to, so the gate could outlast the connect it follows");
    }

    /// <summary>
    /// Scanner blind spots, pinned — a false positive here fails a green build on correct code.
    ///
    /// <para>The fourth case is the shape this group's own tooling got wrong on <c>.Storage</c>: an
    /// untimed command inside a <c>using (...) { }</c> STATEMENT, with the block's closing brace
    /// between it and the next command's deadline. The fifth is this project's real shape: the timed
    /// factory lambda that replaced the method-group hand-off, which must read as TIMED.</para>
    ///
    /// <para><b>The last three are the layouts the landed scan could not report</b>, and they are why the
    /// question is asked in two halves. Give the block above ONE body statement and the two-statement window
    /// spends its budget on that statement and on the statement after the block; put the timed command in the
    /// very next statement, or as the untimed header's own first body statement, and the deadline the scan
    /// finds belongs to the neighbour outright. The initializer is therefore read from the CONSTRUCTION span
    /// and only the assignment from the statement span. Cutting the statement span at the next construction
    /// instead would fail the SECOND case above, where two constructions legitimately share one deadline.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "var command = _dataSource.CreateCommand(Sql);\n"
        + "command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;\n",
        true)]
    [InlineData(
        "await using var command = databaseName == null\n"
        + "    ? _dataSource.CreateCommand(AllSql)\n"
        + "    : _dataSource.CreateCommand(ByDbSql);\n"
        + "command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;\n",
        true)]
    [InlineData(
        "var command = _dataSource.CreateCommand(Sql);\n"
        + "await command.ExecuteNonQueryAsync();\n",
        false)]
    [InlineData(
        "using (var untimed = new NpgsqlCommand(\"SELECT 1\", connection))\n"
        + "{\n"
        + "    a = (int)await untimed.ExecuteScalarAsync();\n"
        + "    b = a + 1;\n"
        + "}\n"
        + "using var next = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 10 };\n",
        false)]
    [InlineData(
        "() =>\n"
        + "{\n"
        + "    var command = connection.CreateCommand();\n"
        + "    command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;\n"
        + "    return command;\n"
        + "},\n",
        true)]
    [InlineData(
        "using (var untimed = new NpgsqlCommand(\"SELECT 1\", connection))\n"
        + "{\n"
        + "    await untimed.ExecuteNonQueryAsync(cancellationToken);\n"
        + "}\n"
        + "using var next = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 10 };\n",
        false)]
    [InlineData(
        "using var untimed = new NpgsqlCommand(Sql, connection);\n"
        + "using var next = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 10 };\n",
        false)]
    [InlineData(
        "using (var untimed = new NpgsqlCommand(Sql, connection))\n"
        + "{\n"
        + "    using var sibling = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 10 };\n"
        + "    await untimed.ExecuteNonQueryAsync(cancellationToken);\n"
        + "}\n",
        false)]
    [InlineData(
        "using var command = connection.CreateCommand();\n"
        + "/* the deadline used to be command.CommandTimeout = 10 here */\n"
        + "await command.ExecuteNonQueryAsync(cancellationToken);\n",
        false)]
    [InlineData(
        "using var command = connection.CreateCommand();\n"
        + "var doc = \"command.CommandTimeout = 10\";\n",
        false)]
    /* The sibling spelled as an ASSIGNMENT rather than an initializer. Every other sibling fixture in this
       family used the initializer form, which has no leading dot and so never reached the assignment regex -
       while the assignment spelling is the dominant one in this codebase. Found in review. */
    [InlineData(
        "using (var untimed = connection.CreateCommand())\n"
        + "{\n"
        + "    using var sibling = connection.CreateCommand();\n"
        + "    sibling.CommandTimeout = 10;\n"
        + "    await untimed.ExecuteNonQueryAsync(cancellationToken);\n"
        + "}\n",
        false)]
    public void TheScanner_JudgesTheSiteItself_NotItsNeighbours(string source, bool expectedTimed)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var ctor = s_commandCtor.Match(code);
        Assert.True(ctor.Success, "the fixture did not contain a command construction");

        Assert.Equal(expectedTimed, CommandDeadlineScanner.SetsAnExplicitDeadline(code, ctor.Index));
    }

    /// <summary>
    /// The method-group scan must read CODE, not prose: the comment explaining the factory fix names
    /// <c>connection.CreateCommand</c> followed by a comma in running text, and a scan that did not
    /// strip comments would fail the build on the very explanation of the fix.
    /// </summary>
    [Theory]
    [InlineData("        Append(connection.CreateCommand, rows);\n", true)]
    [InlineData("        /* not the bare connection.CreateCommand, but a factory. */\n", false)]
    [InlineData("        // pass connection.CreateCommand) here? no.\n", false)]
    [InlineData("        var s = \"connection.CreateCommand,\";\n", false)]
    [InlineData("        var command = connection.CreateCommand();\n", false)]
    public void TheFactoryHandoffScan_ReadsCodeNotProse(string source, bool expectedOffender)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        Assert.Equal(expectedOffender, s_commandFactoryHandoff.IsMatch(code));
    }

    /* The literal- and comment-aware walk this pin used to carry lives in CSharpSourceWalker as of
       #2913. It was one of five private copies, and all five blanked an interpolated string's HOLES along
       with the literal text around them, so a call written inside an interpolation was invisible to every
       scan built on them. The reasoning that shaped the walk moved there with it, and
       CSharpSourceWalkerTests carries the witnesses. */

    /// <summary>
    /// <para>A construction written ONLY in a comment or a literal is not a construction. The scan reads
    /// stripped text so it cannot report one: a phantom offender names a line where no edit can ever make
    /// the build pass, which is worse than a miss because it cannot be acted on. Not hypothetical — an
    /// earlier census in this family reported a bare <c>CreateCommand</c> method group that turned out to
    /// be the phrase in running prose inside a block comment.</para>
    ///
    /// <para>The last case is a FIFTH construction shape, <c>new Npgsql.NpgsqlCommand(</c>, which the
    /// unqualified pattern could not see at all. None exists in this project today; one exists elsewhere in
    /// the solution, so the shape is real and a pattern blind to it would report a clean sweep over a site
    /// it never looked at.</para>
    /// </summary>
    [Theory]
    [InlineData("var command = _dataSource.CreateCommand(Sql);", true)]
    [InlineData("/* these go through _dataSource.CreateCommand(Sql) and leave nothing open. */", false)]
    [InlineData("// TODO: replace with new NpgsqlCommand(Sql, connection)", false)]
    [InlineData("var doc = \"await using var c = _dataSource.CreateCommand(Sql);\";", false)]
    [InlineData("await using var command = new Npgsql.NpgsqlCommand(Sql, connection);", true)]
    public void TheConstructionScan_ReadsCodeNotProse(string source, bool expectedSite)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        Assert.Equal(expectedSite, s_commandCtor.IsMatch(code));
    }

    /// <summary>
    /// Every fan-out in this project declares how wide it is, so the reads inside it are bounded by
    /// <see cref="ViewerCommandDeadlines.FanOutReadSeconds"/> rather than by a solo read's ceiling (#3004).
    ///
    /// <para><b>A fan-out is N reads in flight together, not a syntax</b>, and this project spells that
    /// three ways. #3007 censused only the first, and #3019 is the hole that left: iterating
    /// <c>Task.WhenAll</c> matches gives a fan-out that never joins ZERO iterations, so that shape could
    /// not be reported however wrong it was — while being the very shape whose discovery took #3007's
    /// census from one site to seventeen.
    /// <list type="number">
    /// <item><b>Joined</b> — <c>await Task.WhenAll(...)</c>. One occurrence makes the member a fan-out.</item>
    /// <item><b>Fire-and-forget</b> — two or more <c>_ = SomethingAsync();</c>. One is this project's
    /// ordinary single-flight-guarded refresh (27 such sites, none of them fan-outs); two is a fan-out,
    /// because a discarded task has nobody to await it and is therefore still running when the next
    /// starts. That is also why an <c>await</c> between two discards does NOT separate them, and why this
    /// shape — unlike the deferred one below — takes no account of intervening awaits.</item>
    /// <item><b>Deferred</b> — two or more <c>var t = SomethingAsync();</c> starts with no <c>await</c>
    /// between them. Semantically the joined form with the awaits written one per line, and the spelling
    /// <c>ViewerServerTab.Blocking</c>, <c>.Charts</c> and <c>.RunningJobs</c> use; each of those three
    /// carries a doc comment stating in as many words that the reads run concurrently.</item>
    /// </list></para>
    ///
    /// <para><b>Paired by MEMBER BODY — not positionally, and not by enclosing block.</b> Both obvious
    /// rules are wrong on this codebase's real shapes, in opposite directions. Same-BLOCK pairing breaks on
    /// <c>CorrelatedTimelineLanesControl</c>, which declares its width in an outer <c>try</c> and awaits
    /// the join inside a nested one, so a single-level backward walk finds the inner brace and reports the
    /// widest fan-out in the project as an offender. Per-FILE positional counting — "the k-th join is
    /// preceded by at least k declarations", which is what this pin used to do — breaks the other way: it
    /// lets declarations LAUNDER across unrelated members of one file. <c>MainWindow.xaml.cs</c> carries
    /// three declarations against one join, so under the old rule the declaration that actually pairs with
    /// that join could be deleted and the two unrelated unjoined ones covered for it. A member body is
    /// immune to the nesting (the outer and nested <c>try</c> are the same member) and to the laundering (a
    /// declaration in one method cannot satisfy a fan-out in another).</para>
    ///
    /// <para><b>What this does NOT cover, stated because each gap is real.</b>
    /// <list type="bullet">
    /// <item>The declared WIDTH is not checked, only that a width is declared. <c>Of(n)</c>'s own summary
    /// makes the value the call site's responsibility.</item>
    /// <item>Mutually exclusive branches count as one fan-out: the three <c>switch</c> cases of
    /// <c>LoadBlockingAsync</c> never run together, and two discards in an <c>if</c>/<c>else</c> would
    /// count as a pair. This over-reports rather than under-reports.</item>
    /// <item>Within ONE member, several independent fan-outs share the credit of a single declaration —
    /// the positional residual, narrowed from per-file to per-member but not eliminated.</item>
    /// <item>Reads reached through a helper the member CALLS rather than fires are the helper's fan-out,
    /// not the caller's.</item>
    /// <item>The deferred run is broken by any <c>await</c> between two starts, including one nested inside
    /// a lambda rather than sequencing the starts at member level. That direction UNDER-reports — the same
    /// direction as #3019 itself — and is left because the joined and fire-and-forget rules overlap it and
    /// no member in this project is written that way; a depth-aware run would be the fix if one appears.</item>
    /// <item>Concurrency primitives this project does not use, pinned absent by
    /// <see cref="NoUnmodelledConcurrencyPrimitive_ReachesTheViewer"/> so that adding one goes red here
    /// rather than quietly widening this gap.</item>
    /// </list></para>
    /// </summary>
    [Fact]
    public void EveryViewerFanOut_DeclaresItsWidth()
    {
        var offenders = new List<string>();
        var fanOuts = 0;
        var members = 0;
        var unattributed = new List<string>();

        foreach (var path in ViewerSources())
        {
            var text = File.ReadAllText(path);

            /* Stripped, for the same reason the command scans are: this file's own prose names
               Task.WhenAll and ViewerReadFanOut.Of repeatedly, and a scan reading raw text would pair a
               join against an explanation of a declaration. BraceBalanced below is only correct over
               stripped text, which is that method's own stated contract. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            var bodies = MemberBodies(code);
            members += bodies.Count;

            var joins = s_joinedFanOut.Matches(code).Select(m => m.Index).ToArray();
            var discards = s_fireAndForget.Matches(code).Select(m => m.Index).ToArray();
            var deferred = s_deferredRead.Matches(code).Select(m => (m.Index, End: m.Index + m.Length)).ToArray();
            var declarations = s_fanOutDeclaration.Matches(code).Select(m => m.Index).ToArray();

            /* A marker inside no recognised member means the member walk missed a declaration shape, and a
               fan-out the walk cannot see is exactly the silence this pin exists to break. Reported rather
               than skipped. */
            foreach (var index in joins.Concat(discards).Concat(deferred.Select(d => d.Index)))
            {
                if (Owner(bodies, index) is null)
                {
                    unattributed.Add($"{Path.GetFileName(path)}:{Line(text, index)}");
                }
            }

            foreach (var body in bodies)
            {
                bool Mine(int index) => Owner(bodies, index) is { } owner && owner.Start == body.Start;

                var myJoins = joins.Where(Mine).ToArray();
                var myDiscards = discards.Where(Mine).ToArray();
                var myDeferred = deferred.Where(d => Mine(d.Index)).ToArray();

                var markers = new List<int>();

                if (myJoins.Length > 0)
                {
                    markers.Add(myJoins[0]);
                }

                if (myDiscards.Length >= 2)
                {
                    markers.Add(myDiscards[0]);
                }

                if (ConcurrentRun(code, myDeferred) >= 2)
                {
                    markers.Add(myDeferred[0].Index);
                }

                if (markers.Count == 0)
                {
                    continue;
                }

                fanOuts++;
                var first = markers.Min();

                if (!declarations.Any(d => Mine(d) && d < first))
                {
                    offenders.Add($"{Path.GetFileName(path)}:{Line(text, first)} ({body.Name})");
                }
            }
        }

        /* Three floors, because each one fails differently. The member floor catches a member regex that
           stopped matching (every fan-out then sits in no member and the sweep asserts over nothing); the
           fan-out floor catches the sweep reading an empty or wrong directory; the attribution floor
           catches a member walk that reads the project but drops the members the fan-outs are in. 21
           fan-outs across 1,234 member bodies in 191 files when this landed. */
        Assert.True(members >= 900, $"the member walk found only {members} member bodies — it is not reading the project");

        Assert.True(fanOuts >= 15, $"the fan-out census matched only {fanOuts} fan-out(s) — the sweep is not reading the project");

        Assert.True(
            unattributed.Count == 0,
            $"{unattributed.Count} fan-out marker(s) sit inside no recognised member body, so nothing required "
            + "them to declare a width. The member signature pattern has stopped matching a declaration shape "
            + "this project uses: " + string.Join(", ", unattributed));

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} fan-out(s) run their reads concurrently without declaring a width, so each "
            + "read is bounded by a ceiling derived from a read measured ALONE — which the ten-wide case sits "
            + "entirely above. Declare the width with ViewerReadFanOut.Of(n) before the first read: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The concurrency primitives <see cref="EveryViewerFanOut_DeclaresItsWidth"/> does not model are
    /// pinned ABSENT, which is what lets that pin claim "every" rather than "every one of three spellings".
    ///
    /// <para>This is the ratchet #3019 was really about. A census names the shapes it knows; the failure is
    /// not that the list is short but that a shape added later joins it silently and the guard stays green
    /// while covering less. Asserting the unmodelled primitives are unused converts that silence into a red
    /// build for whoever introduces one, and their options are then to model it above or to declare the
    /// width by hand — either way it is a decision someone makes rather than one nobody sees.</para>
    /// </summary>
    [Fact]
    public void NoUnmodelledConcurrencyPrimitive_ReachesTheViewer()
    {
        var offenders = new List<string>();

        foreach (var path in ViewerSources())
        {
            var text = File.ReadAllText(path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            foreach (Match hit in s_unmodelledConcurrency.Matches(code))
            {
                offenders.Add($"{Path.GetFileName(path)}:{Line(text, hit.Index)} ({hit.Value.Trim()})");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} site(s) use a concurrency primitive the fan-out census does not model, so a "
            + "fan-out spelled that way would never be asked to declare a width. Either teach "
            + "EveryViewerFanOut_DeclaresItsWidth the shape, or declare the width at the site and narrow this "
            + "pin deliberately: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The census classifier, on the shapes that decide whether it is honest. A false negative here is
    /// #3019 again; a false positive fails a green build on correct code.
    ///
    /// <para>The two that matter most are the last two. <b>Sequential start-await-start-await is NOT a
    /// fan-out</b> — nothing is in flight together — and a deferred rule that ignored the intervening
    /// <c>await</c> would report every such method. <b>Two discards separated by an await ARE</b> a
    /// fan-out, because discarding a task means nothing waits for it; the asymmetry between those two
    /// cases is the whole reason the deferred shape counts awaits and the discard shape does not.</para>
    /// </summary>
    [Theory]
    /* One discard: the project's single-flight refresh, 27 of them, not a fan-out. */
    [InlineData("_ = RefreshServerStatusAsync();\n", false)]
    /* Two: MainWindow.LoadServersAsync's connect pair. */
    [InlineData("_ = RefreshServerStatusAsync();\n_ = RefreshStoreSizeAsync();\n", true)]
    /* Dotted receiver — the shape the sibling pins' (\w+) form cannot see. */
    [InlineData("_ = Controller.LoadAsync();\n_ = Controller.SaveAsync();\n", true)]
    /* A join alone is a fan-out; one occurrence is enough. */
    [InlineData("await Task.WhenAll(a, b);\n", true)]
    /* Deferred pair, then awaited — ViewerServerTab.Charts.LoadTempDbAsync. */
    [InlineData("var trendTask = _dataService.GetTempDbTrendAsync(id);\nvar fileIoTask = _dataService.GetTempDbFileIoTrendAsync(id);\nvar trend = await trendTask;\n", true)]
    /* One deferred start is not a fan-out. */
    [InlineData("var only = _dataService.GetTempDbTrendAsync(id);\nvar rows = await only;\n", false)]
    /* Sequential: each awaited before the next starts, so nothing overlaps. */
    [InlineData("var a = _dataService.GetXAsync(id);\nvar ra = await a;\nvar b = _dataService.GetYAsync(id);\nvar rb = await b;\n", false)]
    /* Discards are NOT separated by an await — the discarded task is still running. */
    [InlineData("_ = RefreshServerStatusAsync();\nawait RefreshVisibleAsync();\n_ = PollAlertsAsync();\n", true)]
    /* Prose cannot make a fan-out. */
    [InlineData("/* two reads: _ = OneAsync(); _ = TwoAsync(); */\n", false)]
    /* Two SYNCHRONOUS discards read as a fan-out — the accepted cost of not constraining the callee, and
       the reason that trade is argued on s_fireAndForget rather than left to be rediscovered. Expected
       true: this is the classifier's behaviour, not a defect to be fixed by narrowing the shape. Found in
       review. */
    [InlineData("_ = TryParseFirst(out var first);\n_ = TryParseSecond(out var second);\n", true)]
    public void TheFanOutCensus_RecognisesEachShape_AndOnlyThose(string body, bool expectedFanOut)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings("private async Task Fixture()\n{\n" + body + "}\n");
        var bodies = MemberBodies(code);

        Assert.True(bodies.Count == 1, $"the fixture parsed to {bodies.Count} member bodies, not 1");

        var joins = s_joinedFanOut.Matches(code).Select(m => m.Index).ToArray();
        var discards = s_fireAndForget.Matches(code).Select(m => m.Index).ToArray();
        var deferred = s_deferredRead.Matches(code).Select(m => (m.Index, End: m.Index + m.Length)).ToArray();

        var isFanOut = joins.Length > 0 || discards.Length >= 2 || ConcurrentRun(code, deferred) >= 2;

        Assert.Equal(expectedFanOut, isFanOut);
    }

    /// <summary>
    /// The member walk sees a generic method: type parameters after the name, constraints before the body.
    /// A member the walk cannot see cannot be required to declare a width, which is #3019's failure
    /// reproduced inside the fix for it.
    ///
    /// <para><b>The constraint is spelled WITHOUT parentheses on purpose.</b> <c>where T : class, new()</c>
    /// contains a <c>)</c>, and the parameter-list group is greedy, so it absorbs the whole clause and the
    /// fixture then matches even with constraint support removed — it passes for the wrong reason. The real
    /// <c>SettingsWindow.TryReadSectionAsync&lt;T&gt;</c> is <c>where T : class</c>, paren-free, which is
    /// the shape that actually needs the clause admitted. The <c>new()</c> spelling is pinned separately
    /// below so both live layouts are covered. Found by a mutation that stayed green.</para>
    /// </summary>
    [Theory]
    /* Paren-free constraint — SettingsWindow.TryReadSectionAsync<T>. Sensitive to the constraint clause. */
    [InlineData("private static async Task<T?> TryReadSectionAsync<T>(Func<Task<T?>> read) where T : class\n", "TryReadSectionAsync")]
    /* Constraint on its own line, with new() — ViewerSettingsFile.Load<T> and Save<T>. */
    [InlineData("internal static SettingsObjectRead<T> Load<T>(string filePath, JsonSerializerOptions options)\n    where T : class, new()\n", "Load")]
    /* No constraint, type parameters only. */
    [InlineData("private static Task<T> PassThroughAsync<T>(Task<T> inner)\n", "PassThroughAsync")]
    public void TheMemberWalk_SeesAGenericMethod(string signature, string expectedName)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            signature
            + "{\n"
            + "    var firstTask = _dataService.GetOneAsync();\n"
            + "    var secondTask = _dataService.GetTwoAsync();\n"
            + "    return await firstTask ?? await secondTask;\n"
            + "}\n");

        var bodies = MemberBodies(code);

        Assert.True(bodies.Count == 1, $"the generic signature parsed to {bodies.Count} member bodies, not 1");
        Assert.Equal(expectedName, bodies[0].Name);

        /* And the fan-out inside it is attributed to that member rather than landing nowhere. */
        var deferred = s_deferredRead.Matches(code).Select(m => (m.Index, End: m.Index + m.Length)).ToArray();

        Assert.True(deferred.Length == 2, $"{deferred.Length} deferred read(s) parsed out of the fixture, not 2");
        Assert.All(deferred, d => Assert.NotNull(Owner(bodies, d.Index)));
        Assert.True(ConcurrentRun(code, deferred) >= 2, "the generic method's deferred pair did not read as a fan-out");
    }

    /// <summary>
    /// A FIELD whose initializer is a collection initializer is not a member body. The type-parameter group
    /// excludes <c>=</c> and newlines to keep it that way: allowing them ran the pattern from the field's
    /// declared type, through <c>= new Dictionary&lt;...&gt;(comparer)</c>, to the initializer's brace.
    ///
    /// <para>A phantom body is worse than a missing one. <see cref="Owner"/> takes the OUTERMOST match, so
    /// one spanning an initializer could swallow a real member's markers and attribute them to something
    /// that can never declare a width — a fan-out reported at a line where no edit makes the build pass.
    /// This is <c>CollectorSchedulePresets.Presets</c>'s real shape.</para>
    /// </summary>
    [Fact]
    public void TheMemberWalk_DoesNotReadAFieldInitializerAsAMember()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            "public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, int>> Presets =\n"
            + "    new Dictionary<string, IReadOnlyDictionary<string, int>>(StringComparer.OrdinalIgnoreCase)\n"
            + "    {\n"
            + "        [\"Aggressive\"] = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)\n"
            + "        {\n"
            + "            [\"wait_stats\"] = 1,\n"
            + "        },\n"
            + "    };\n");

        var bodies = MemberBodies(code);

        Assert.True(
            bodies.Count == 0,
            "a field's collection initializer parsed as "
            + $"{bodies.Count} member body(ies) ({string.Join(", ", bodies.Select(b => b.Name))}); Owner takes the "
            + "outermost body, so a phantom one here would claim a real member's fan-out markers");
    }

    /// <summary>
    /// The discard and deferred shapes never both claim one call. <c>var _ = SomeAsync();</c> is a DISCARD —
    /// nothing holds the task — and counting it twice would let one physical call inflate two separate
    /// fan-out tallies.
    /// </summary>
    [Fact]
    public void TheDiscardAndDeferredShapes_StayDisjoint()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings("var _ = RefreshServerStatusAsync();\n");

        Assert.True(s_fireAndForget.Matches(code).Count == 1, "the discard shape did not claim `var _ = SomeAsync();`");
        Assert.True(s_deferredRead.Matches(code).Count == 0, "the deferred shape also claimed `var _ = SomeAsync();`, so one call is double-booked");

        /* The ordinary deferred spelling still reads as deferred and NOT as a discard, so the exclusion
           above narrowed the right one. */
        var named = CSharpSourceWalker.StripCommentsAndStrings("var statusTask = RefreshServerStatusAsync();\n");

        Assert.True(s_deferredRead.Matches(named).Count == 1, "the deferred shape stopped matching a named task start");
        Assert.True(s_fireAndForget.Matches(named).Count == 0, "the discard shape claimed a named task start");
    }

    /// <summary>
    /// The longest run of deferred reads with no <c>await</c> between consecutive members of the run — the
    /// number actually in flight together. Two starts either side of an <c>await</c> are sequential and
    /// must not count; see <see cref="TheFanOutCensus_RecognisesEachShape_AndOnlyThose"/>.
    /// </summary>
    private static int ConcurrentRun(string code, (int Index, int End)[] deferred)
    {
        var best = 0;
        var run = 0;
        var previousEnd = -1;

        foreach (var (index, end) in deferred)
        {
            run = previousEnd >= 0 && !s_await.IsMatch(code[previousEnd..index]) ? run + 1 : 1;
            best = System.Math.Max(best, run);
            previousEnd = end;
        }

        return best;
    }

    /// <summary>
    /// Every member body in <paramref name="code"/>, which must be
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output — a brace in prose or in a literal
    /// is exactly what would unbalance the walk.
    /// </summary>
    private static List<(int Start, int End, string Name)> MemberBodies(string code)
    {
        var bodies = new List<(int Start, int End, string Name)>();

        foreach (Match signature in s_memberSignature.Matches(code))
        {
            /* The signature match ends ON the body brace, so search from there rather than from the
               parameter list: a parameter default can carry a brace. */
            var open = code.IndexOf('{', signature.Index + signature.Length - 1);

            if (open < 0)
            {
                continue;
            }

            bodies.Add((open, open + CSharpSourceWalker.BraceBalanced(code, open).Length, signature.Groups["name"].Value));
        }

        return bodies;
    }

    /// <summary>
    /// The OUTERMOST member body containing <paramref name="index"/>, or null. Outermost so that a read
    /// inside a lambda or a local function is attributed to the method that fans it out, which is where the
    /// width has to be declared.
    /// </summary>
    private static (int Start, int End, string Name)? Owner(List<(int Start, int End, string Name)> bodies, int index)
    {
        (int Start, int End, string Name)? owner = null;

        foreach (var body in bodies)
        {
            if (index > body.Start && index < body.End && (owner is null || body.Start < owner.Value.Start))
            {
                owner = body;
            }
        }

        return owner;
    }

    /// <summary>The 1-based line of <paramref name="index"/> in <paramref name="text"/>.</summary>
    private static int Line(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    /// <summary>
    /// A declared width must not outlive the reads it describes. A method-scoped <c>using var</c> runs to
    /// the closing brace, so a store read AFTER the join inherits a contention count that is over by the
    /// whole fan-out — and a nested fan-out below it multiplies against that stale count and lands on the
    /// pool ceiling. <c>ViewerReadFanOut.Scope.Release()</c> ends it at the join; this asserts it is called
    /// wherever the method goes on to await anything.
    ///
    /// <para><b>The join is matched as a STATEMENT, not a line</b>, which is what makes the scan sound. The
    /// two per-server fan-outs pass <c>Task.WhenAll(servers.Select(async item =&gt; ...))</c>, whose lambda
    /// body contains the very <c>await</c> that IS the fan-out — searching from the line would report both
    /// as offenders. Searching from the end of the parenthesised statement puts those awaits inside the
    /// join rather than after it. Conversely the search cannot be depth-restricted to the join's own brace
    /// level: <c>MainWindow</c>'s fleet-totals read sits inside a <c>try</c> block, so a same-depth rule
    /// would miss a real one. Both shapes are live in this project, and each rules out one of the two
    /// obvious implementations.</para>
    ///
    /// <para><b>Joined fan-outs only, unlike
    /// <see cref="EveryViewerFanOut_DeclaresItsWidth"/>.</b> "Outlives its join" needs a join to be
    /// measured against, and the unjoined shapes have none — <c>MainWindow.OnRefreshTimerTick</c>
    /// deliberately holds its scope to the end of the tick, because the visible-tab load below really does
    /// contend with the reads still in flight. So the release discipline is not merely unchecked for those
    /// shapes, it is a different question there with a different answer, and widening this scan to reach
    /// them would report that deliberate choice as a defect (#3019).</para>
    /// </summary>
    [Fact]
    public void NoFanOutScope_OutlivesItsJoin()
    {
        var offenders = new List<string>();
        var checked_ = 0;

        foreach (var path in ViewerSources())
        {
            var text = File.ReadAllText(path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            /* Only joins that a declaration precedes are in scope for this rule; a file with no fan-out
               declaration at all is EveryViewerFanOut_DeclaresItsWidth's business, not this test's. */
            var firstDeclaration = code.IndexOf("ViewerReadFanOut", System.StringComparison.Ordinal);

            foreach (Match join in s_joinedFanOut.Matches(code))
            {
                if (firstDeclaration < 0 || firstDeclaration > join.Index)
                {
                    continue;
                }

                var afterJoin = EndOfParenthesisedStatement(code, join.Index + join.Length - 1);
                if (afterJoin < 0)
                {
                    continue;
                }

                var methodEnd = EndOfEnclosingBlock(code, afterJoin);
                var window = code[afterJoin..methodEnd];
                var nextAwait = Regex.Match(window, @"(^|[^A-Za-z0-9_])await[^A-Za-z0-9_]");

                if (!nextAwait.Success)
                {
                    continue;
                }

                checked_++;

                var released = window[..nextAwait.Index].Contains(".Release()", System.StringComparison.Ordinal);
                if (!released)
                {
                    var line = text.Take(afterJoin).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        Assert.True(checked_ >= 3, $"only {checked_} join(s) were followed by a further await — the scan is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} fan-out scope(s) stay open past their join while the method reads the store again, "
            + "so those later reads are priced against contention that has already finished (and a nested fan-out "
            + "multiplies against it onto the pool ceiling). Call readFanOut.Release() right after the join: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The index just past the <c>;</c> that closes the statement whose opening <c>(</c> is at
    /// <paramref name="openParen"/>, or -1. Counts parens so a nested lambda cannot end it early.
    /// </summary>
    private static int EndOfParenthesisedStatement(string code, int openParen)
    {
        var depth = 0;

        for (var i = openParen; i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')')
            {
                depth--;

                if (depth == 0)
                {
                    var semi = code.IndexOf(';', i);
                    return semi < 0 ? -1 : semi + 1;
                }
            }
        }

        return -1;
    }

    /// <summary>
    /// The index of the <c>}</c> that closes the block containing <paramref name="from"/> — the method
    /// body, for a statement at method level. Terminates on the first unmatched close rather than clamping
    /// at zero, which is the walker bug that made two scanners miss real sites during #2888.
    /// </summary>
    private static int EndOfEnclosingBlock(string code, int from)
    {
        var depth = 0;

        for (var i = from; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}')
            {
                if (depth == 0)
                {
                    return i;
                }

                depth--;
            }
        }

        return code.Length;
    }

    /// <summary>
    /// The two ceilings answer different questions, and this is the pin that says so in the only terms
    /// available without a store: where each one sits relative to the CONCURRENT band #2901 measured
    /// (24.4-64.1 s per read for ten concurrent 30-day reads, against 3.01 s for the same read alone).
    ///
    /// <para>Deliberately NOT a timing test. The failure it guards needs a Windows host and a store dense
    /// enough to reach the band; a test that measured anything here would pass on a fast machine and prove
    /// nothing. What it asserts instead is arithmetic on shipped constants, which holds identically
    /// everywhere.</para>
    /// </summary>
    [Fact]
    public void TheFanOutDeadline_IsDerivedFromTheConcurrentBand_NotTheSoloOne()
    {
        var solo = ViewerCommandDeadlines.InteractiveReadSeconds;
        var widest = ViewerCommandDeadlines.FanOutReadSeconds(ViewerSettings.ManagedMaxPoolSize);

        Assert.True(
            solo < 24,
            $"the solo read deadline {solo}s has risen into the concurrent band (24.4-64.1 s). Either it is no "
            + "longer derived from the 3.01 s solo measurement, or the two regimes have been collapsed back "
            + "into one constant — which is the defect #3004 exists to undo, in the other direction");

        Assert.True(
            widest > 64,
            $"the fan-out deadline at the pool ceiling is {widest}s, which does not cover the 64.1 s worst read "
            + "measured for ten concurrent 30-day reads on a store at production density. A ceiling under the "
            + "measurement it is supposed to bound fails every read in the widest fan-out on every attempt, "
            + "auto-refresh included — a deadline nothing can finish under is not a deadline");
    }

    /// <summary>
    /// The fan-out ceiling can only ever grant MORE time than a solo read gets, and it stops growing where
    /// the permits run out. The clamp is what lets the two unbounded per-server fan-outs
    /// (<c>FinOpsTab.Loaders</c>'s inventory overlay, <c>MainWindow</c>'s overview cards) hand over a raw
    /// fleet count instead of a hand-capped guess, so it is load-bearing rather than defensive.
    /// </summary>
    [Fact]
    public void TheFanOutDeadline_IsMonotonicAndClampedAtThePool()
    {
        var pool = ViewerSettings.ManagedMaxPoolSize;
        var previous = 0;

        for (var width = -3; width <= pool * 4; width++)
        {
            var seconds = ViewerCommandDeadlines.FanOutReadSeconds(width);

            Assert.True(
                seconds >= ViewerCommandDeadlines.InteractiveReadSeconds,
                $"width {width} yields {seconds}s, under the solo floor — a declared fan-out must never buy a "
                + "read LESS time than the same read issued alone");

            Assert.True(seconds >= previous, $"width {width} yields {seconds}s, below width {width - 1}'s {previous}s");

            previous = seconds;
        }

        Assert.Equal(
            ViewerCommandDeadlines.FanOutReadSeconds(pool),
            ViewerCommandDeadlines.FanOutReadSeconds(pool * 4));
    }

    /// <summary>
    /// The ambient width itself: one when nobody declared a fan-out (so an unscoped read keeps exactly the
    /// solo ceiling), the declared value inside a scope, the enclosing value again after it, and the PRODUCT
    /// when scopes nest — because a read two scopes deep really does contend with both widths.
    /// </summary>
    [Fact]
    public void TheFanOutWidth_DefaultsToOne_AndNestsByMultiplying()
    {
        Assert.Equal(1, ViewerReadFanOut.CurrentWidth);

        using (ViewerReadFanOut.Of(3))
        {
            Assert.Equal(3, ViewerReadFanOut.CurrentWidth);

            using (ViewerReadFanOut.Of(2))
            {
                Assert.Equal(6, ViewerReadFanOut.CurrentWidth);
            }

            Assert.Equal(3, ViewerReadFanOut.CurrentWidth);
        }

        Assert.Equal(1, ViewerReadFanOut.CurrentWidth);

        /* A count from a runtime collection, which is what the two per-server fan-outs pass. */
        using (ViewerReadFanOut.Of(ViewerSettings.ManagedMaxPoolSize * 7))
        {
            Assert.Equal(ViewerSettings.ManagedMaxPoolSize, ViewerReadFanOut.CurrentWidth);
        }

        using (ViewerReadFanOut.Of(0))
        {
            Assert.Equal(1, ViewerReadFanOut.CurrentWidth);
        }
    }

    /// <summary>
    /// No command site stamps the solo constant directly. The sites take
    /// <see cref="ViewerCommandDeadlines.CurrentInteractiveReadSeconds"/> uniformly — including the reads no
    /// fan-out currently reaches, where the two are the same number — so that a future <c>Task.WhenAll</c>
    /// over any of them is bounded the day it is written rather than silently inheriting a solo ceiling.
    /// That uniformity is the whole reason the previous test's arithmetic reaches the reads at all, so it
    /// needs a pin of its own; without it a single new site spelled the old way is invisible.
    /// </summary>
    [Fact]
    public void NoViewerCommand_StampsTheSoloDeadlineDirectly()
    {
        var offenders = new List<string>();
        var stamps = 0;

        foreach (var path in ViewerSources())
        {
            var text = File.ReadAllText(path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            stamps += code.Split("CurrentInteractiveReadSeconds").Length - 1;

            var at = 0;
            while (true)
            {
                /* The solo name is a PREFIX of nothing, but it is a SUFFIX of the fan-out-aware one, so a
                   plain search for it matches every correct site too. Anchored on the assignment to make the
                   two distinguishable. */
                var index = code.IndexOf(
                    "CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds",
                    at,
                    System.StringComparison.Ordinal);

                if (index < 0)
                {
                    break;
                }

                offenders.Add($"{Path.GetFileName(path)}:{text.Take(index).Count(c => c == '\n') + 1}");
                at = index + 1;
            }
        }

        Assert.True(stamps >= 150, $"only {stamps} site(s) stamp the fan-out-aware deadline — the sweep is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command site(s) stamp InteractiveReadSeconds directly. That constant bounds a read "
            + "issued ALONE; a site spelled this way is not bounded by any fan-out it is called inside. Use "
            + "ViewerCommandDeadlines.CurrentInteractiveReadSeconds: "
            + string.Join(", ", offenders));
    }

    private static IEnumerable<string> ViewerSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");

        Assert.True(Directory.Exists(dir), $"viewer project directory not found: {dir}");

        /* RECURSIVE, unlike the .Storage pin's TopDirectoryOnly, minus the build outputs. The claim
           is "every command THIS PROJECT creates", and a file added under a future subdirectory is
           still this project's - the viewer already carries a Themes/ folder, so subdirectories are
           not hypothetical here. bin/ and obj/ are excluded by path segment rather than by name
           match, because that is where the generated .AssemblyInfo.cs and .g.cs land during a CI
           build and they are not source. */
        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(dir, p))
            .OrderBy(p => p, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 150, $"the viewer sweep found only {paths.Length} files — the project has moved");

        return paths;
    }

    /// <summary>
    /// True when a path sits under the project's <c>bin</c> or <c>obj</c> tree. Compared as PATH
    /// SEGMENTS, so a source file that merely has "obj" in its name is not excluded.
    /// </summary>
    private static bool IsBuildOutput(string projectDir, string path)
    {
        var relative = Path.GetRelativePath(projectDir, path);
        var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return segments.Any(s =>
            string.Equals(s, "bin", System.StringComparison.OrdinalIgnoreCase)
            || string.Equals(s, "obj", System.StringComparison.OrdinalIgnoreCase));
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
