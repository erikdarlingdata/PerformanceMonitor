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
