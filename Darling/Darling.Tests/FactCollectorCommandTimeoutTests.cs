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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every fact read must carry an EXPLICIT command deadline (#2810).
///
/// <para>All thirty-one commands in <c>PgFactCollector</c> ran with no <c>CommandTimeout</c>, so
/// every one of them inherited Npgsql's undocumented 30 s default. Nobody chose 30 s; it was simply
/// what happened. Measured on the production store after retention was restored (#2809) and after
/// #2827's dedup rewrite, the plan-regression read still crossed that ceiling on a COLD run — 34.7 s
/// and 32.4 s on the two largest servers, against 10.6-21.2 s for the same servers warm. A few
/// seconds of overshoot, on the cold/large combination only, which is exactly why it presented as
/// intermittent and why two prior changes aimed elsewhere: #2827 made the query 2.4x faster and
/// #2826 made the failure visible, but neither touched the ceiling it was failing against.</para>
///
/// <para><b>Why this pin is shaped over the FAMILY rather than the one site.</b> The defect was
/// reported against <c>PlanRegressionSql</c>. Fixing only that command would have been
/// scenario-shaped, and this repo has already paid twice for exactly that — the #2344 enumerated pin,
/// and <c>PgStatementText</c>, where a broken sibling arm survived a green suite because the test
/// named the arm it was written for. Thirty-one commands shared one missing value; the assertion is
/// therefore structural: EVERY command constructed in this collector sets a deadline, so the next one
/// somebody adds cannot quietly reintroduce the inherited default.</para>
///
/// <para>The value itself is pinned separately and deliberately below. It is bounded ABOVE by the
/// 120 s analysis pass budget that thirty collect methods share, not just below by how long the
/// slowest query takes — a detail that is easy to lose and expensive to relearn, because a timeout
/// raised past the pass budget converts "one fact is missing" into "every fact is missing".</para>
/// </summary>
public sealed class FactCollectorCommandTimeoutTests
{
    /// <summary>
    /// A command construction in this collector. Matched by shape rather than by variable name — the
    /// sites use both <c>cmd</c> and <c>command</c>, and a pin keyed on either spelling would miss
    /// half of them.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryFactCollectorCommandSetsAnExplicitTimeout()
    {
        var missing = new List<string>();
        var covered = 0;

        foreach (var file in FactCollectorSources())
        {
            var text = File.ReadAllText(file);

            /* Both halves of the question are asked of STRIPPED text, which is character-aligned with its
               input. This replaced a raw three-LINE window that could tell neither a construction from the
               same words in a comment, nor this site's deadline from the next site's, nor a real deadline
               from one merely spelled in prose. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;

                if (CommandDeadlineScanner.SetsAnExplicitDeadline(code, ctor.Index))
                {
                    covered++;
                }
                else
                {
                    missing.Add($"{Path.GetFileName(file)}:{line}");
                }
            }
        }

        Assert.True(
            missing.Count == 0,
            "These fact-collector commands carry no explicit CommandTimeout, so they inherit Npgsql's "
            + "undocumented 30 s default — the defect #2810 fixed, where a read that needed 34.7 s cold "
            + "failed on every cold pass and nobody had chosen the ceiling it failed against:"
            + Environment.NewLine + string.Join(Environment.NewLine, missing));

        /* A floor, not an equality: the point is that nobody adds a command without a deadline, and a
           pin that also broke on every ADDED collector would be a tax on writing one. The floor still
           fails loudly if the sweep is partially reverted, which is the regression that matters. */
        Assert.True(
            covered >= 31,
            $"Expected at least the 31 swept command sites to set a timeout; found {covered}. "
            + "A drop means sites were removed or the timeout was refactored out from under this pin.");
    }

    /// <summary>
    /// The value is bounded on BOTH sides, and the upper bound is the one that is easy to get wrong.
    ///
    /// <para>Below: the measured cold worst case was 34.7 s, so anything at or under that reproduces
    /// #2810. Above: <c>DarlingWorker.s_analysisTimeout</c> gives the whole analysis pass 120 s and
    /// thirty collect methods share it, so a per-command deadline approaching that figure lets ONE
    /// stalled read consume the pass and cost the server its other twenty-nine facts — a strictly
    /// worse failure than the one being fixed. This asserts the value stays in the band where both
    /// arguments hold.</para>
    /// </summary>
    [Fact]
    public void TheTimeoutClearsTheMeasuredColdWorstCaseWithoutOwningThePassBudget()
    {
        const int MeasuredColdWorstCaseSeconds = 35;
        const int AnalysisPassBudgetSeconds = 120;

        Assert.True(
            PgFactCollector.FactCommandTimeoutSeconds > MeasuredColdWorstCaseSeconds,
            $"The deadline ({PgFactCollector.FactCommandTimeoutSeconds}s) must clear the measured cold "
            + $"worst case ({MeasuredColdWorstCaseSeconds}s) or #2810 reproduces: the plan-regression "
            + "read took 34.7s cold on the largest production server.");

        Assert.True(
            PgFactCollector.FactCommandTimeoutSeconds <= AnalysisPassBudgetSeconds / 2,
            $"The deadline ({PgFactCollector.FactCommandTimeoutSeconds}s) must leave at least half of "
            + $"the {AnalysisPassBudgetSeconds}s analysis pass for the other twenty-nine collect "
            + "methods. One read that stalls must not cost a server every other fact.");
    }

    /// <summary>
    /// Scanner blind spots, pinned - a false positive here fails a green build on correct code. The first
    /// two are this assembly's real shapes; the next three are the layouts the raw three-line window this
    /// scan replaced could not report, where the deadline it found belonged to the NEXT construction. The
    /// last is why the span is not simply CUT at that next construction, which is the tempting one-line
    /// version of the same rule: two constructions can legitimately share ONE deadline. The last two are the
    /// value half of the same mistake: a deadline SPELLED in a comment or a literal is not a deadline, so the
    /// span is judged over STRIPPED source - this codebase quotes code in its prose constantly.
    /// </summary>
    [Theory]
    [InlineData(
        "var command = new NpgsqlCommand(Sql, connection) { CommandTimeout = 45 };\n",
        true)]
    [InlineData(
        "var command = connection.CreateCommand();\n"
        + "/* a method result cannot take an initializer; set it here. */\n"
        + "command.CommandTimeout = 45;\n",
        true)]
    [InlineData(
        "using (var untimed = new NpgsqlCommand(Sql, connection))\n"
        + "{\n"
        + "    await untimed.ExecuteNonQueryAsync(cancellationToken);\n"
        + "}\n"
        + "using var next = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 45 };\n",
        false)]
    [InlineData(
        "using var untimed = new NpgsqlCommand(Sql, connection);\n"
        + "using var next = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 45 };\n",
        false)]
    [InlineData(
        "using (var untimed = new NpgsqlCommand(Sql, connection))\n"
        + "{\n"
        + "    using var sibling = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 45 };\n"
        + "    await untimed.ExecuteNonQueryAsync(cancellationToken);\n"
        + "}\n",
        false)]
    [InlineData(
        "await using var command = filtered\n"
        + "    ? connection.CreateCommand(FilteredSql)\n"
        + "    : connection.CreateCommand(AllSql);\n"
        + "command.CommandTimeout = 45;\n",
        true)]
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
    /// A construction written ONLY in a comment or a literal is not a construction. The scan reads
    /// stripped text so it cannot report one: a phantom offender names a line where no edit can ever make
    /// the build pass. The last case is a fifth construction shape the unqualified pattern could not see.
    /// </summary>
    [Theory]
    [InlineData("var command = connection.CreateCommand();", true)]
    [InlineData("/* these go through connection.CreateCommand() and leave nothing open. */", false)]
    [InlineData("// TODO: replace with new NpgsqlCommand(Sql, connection)", false)]
    [InlineData("var doc = \"using var c = new NpgsqlCommand(Sql, connection);\";", false)]
    [InlineData("await using var command = new Npgsql.NpgsqlCommand(Sql, connection);", true)]
    public void TheConstructionScan_ReadsCodeNotProse(string source, bool expectedSite)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        Assert.Equal(expectedSite, s_commandCtor.IsMatch(code));
    }

    private static IEnumerable<string> FactCollectorSources()
    {
        var darling = Directory.GetFiles(
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis"),
            "PgFactCollector.*.cs");

        Assert.NotEmpty(darling);

        return darling.OrderBy(f => f, StringComparer.Ordinal);
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
