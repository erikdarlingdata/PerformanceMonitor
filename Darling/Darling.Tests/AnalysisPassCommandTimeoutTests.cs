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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// EVERY command in the analysis pass must carry an explicit deadline (#2871), not just the fact
/// collector's (#2810).
///
/// <para><b>What was still missing.</b> #2810 swept <c>PgFactCollector</c>'s thirty-one commands and
/// left the other thirty-two in the same pass inheriting Npgsql's undocumented 30 s default. One of
/// them was failing in production: on the dogfood box the <c>io_latency</c> baseline timed out
/// nineteen times over two days and EVERY sample landed between 30.1 s and 31.4 s. A fixed wall —
/// not a variable-duration fault, and not the 15 s connection timeout. The same read completes in
/// ~1.6 s normally, measured against the live store on the three busiest servers, so it only crosses
/// the ceiling when the store stalls. That is why it read as intermittent, and why #2820 (5.6x
/// faster) and #2826 (made the failure visible) both left it failing: neither touched the ceiling.</para>
///
/// <para><b>Why the pin is over the pass, not the file.</b> The reported site was one baseline query.
/// Pinning that alone would be scenario-shaped, and this repo has paid for that repeatedly — #2344's
/// enumerated pin, and <c>PgStatementText</c>, where a broken sibling arm survived a green suite
/// because the test named the arm it was written for. The 120 s budget is shared by the fact
/// collector, the anomaly detector, the baseline provider, the drill-down and the finding store, so
/// the invariant belongs to the pass: every command constructed anywhere in this assembly sets a
/// deadline, and the next one somebody adds cannot quietly reintroduce the inherited default.</para>
///
/// <para>This subsumes <c>FactCollectorCommandTimeoutTests</c>' structural half by construction;
/// that test is left in place because its value-band assertion pins a different constant.</para>
///
/// <para><b>#2874: the assembly-wide claim above was true and the scan still missed two sites.</b>
/// Both DMV-snapshot fallback reads handed <c>connection.CreateCommand</c> to
/// <c>PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync</c> as a bare METHOD GROUP, which builds the
/// command in this same assembly but sets only its <c>CommandText</c> - so the deadline could only
/// come from the hand-off, and no <c>(</c> follows a method group for either census regex to match.
/// The sweep read 71 sites as covered while 73 existed. <see cref="s_commandCtor"/> now matches both
/// construction shapes and <see cref="NoAnalysisPassCommand_IsCreatedByABareMethodGroupHandoff"/>
/// closes the shape neither can see, mirroring the viewer's pin. Note which half of this was actually
/// unbounded: the scheduled pass arms <c>context.CancellationToken</c> with
/// <c>DarlingWorker.s_analysisTimeout</c>'s 120 s <c>CancelAfter</c> (<c>DarlingWorker</c> is the only
/// caller that passes it), so a stuck read there was bounded and merely ate the whole pass rather than
/// the collector's own budget. But <c>AnalysisContext.CancellationToken</c> defaults to
/// <c>CancellationToken.None</c>, and every on-demand entry point omits it: <c>analyze_server</c>
/// reaches BOTH reads (<c>CollectFactsAsync</c> and then <c>EnrichFindingsAsync</c>), while
/// <c>get_analysis_facts</c>, <c>compare_analysis</c> and <c>audit_config</c> reach the fact
/// collector's. On those paths the read had no deadline AND no token, and relied entirely on Npgsql's
/// undocumented default - so the fix is what bounds them, not a nicety on top of a token that was
/// already doing the work.</para>
/// </summary>
public sealed class AnalysisPassCommandTimeoutTests
{
    /* Matched by SHAPE, not by variable name: the sites use cmd, command, peakCmd, rateCmd,
       contribCmd, queryCmd and dmvCommand, and a pin keyed on any spelling would miss most of them.
       BOTH construction shapes, as the viewer's pin does - `.CreateCommand(` was absent from #2871's
       census and is how the two DMV-snapshot factories build their commands. */
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A <c>CreateCommand</c> handed over as a METHOD GROUP rather than called - invisible to
    /// <see cref="s_commandCtor"/> because no <c>(</c> follows it, and the shape both remaining
    /// #2874 sites in this assembly were built on.
    /// </summary>
    private static readonly Regex s_commandFactoryHandoff = new(
        @"\.CreateCommand\s*[,)]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryAnalysisPassCommandSetsAnExplicitTimeout()
    {
        var missing = new List<string>();
        var covered = 0;

        foreach (var file in AnalysisSources())
        {
            var lines = File.ReadAllLines(file);

            for (var i = 0; i < lines.Length; i++)
            {
                if (!s_commandCtor.IsMatch(lines[i]))
                {
                    continue;
                }

                /* The deadline may sit in the object initializer on the construction line or be
                   assigned in the following statement — both spellings are in use here. */
                var window = string.Join(
                    "\n",
                    lines.Skip(i).Take(Math.Min(3, lines.Length - i)));

                if (s_setsTimeout.IsMatch(window))
                {
                    covered++;
                }
                else
                {
                    missing.Add($"{Path.GetFileName(file)}:{i + 1}");
                }
            }
        }

        Assert.True(
            missing.Count == 0,
            "These analysis-pass commands carry no explicit CommandTimeout, so they inherit Npgsql's "
            + "undocumented 30 s default — the defect #2871 fixed, where the io_latency baseline hit "
            + "that ceiling nineteen times in two days while the query itself runs in ~1.6 s:"
            + Environment.NewLine + string.Join(Environment.NewLine, missing));

        /* A floor rather than an equality, so adding a collector is not taxed; it still fails loudly
           if the sweep is partially reverted, which is the regression that matters.

           73, not 71: widening s_commandCtor to the `.CreateCommand(` shape (#2874) made two sites
           VISIBLE that always existed, so the true count moved without any command being added. A
           floor left at the old number would have let both of them vanish again silently - which is
           precisely what this assertion is for, and it is the reason a floor has to be re-measured
           whenever the scan's reach changes rather than only when code is added. */
        Assert.True(
            covered >= 73,
            $"Expected at least the 73 analysis-pass command sites to set a timeout; found {covered}. "
            + "A drop means sites were removed or the deadline was refactored out from under this pin.");
    }

    /// <summary>
    /// The value is bounded on both sides, and the upper bound is the one that binds.
    ///
    /// <para><b>Below.</b> Every observed failure sat at 30.1-31.4 s, so anything at or under 31 s
    /// reproduces #2871. That record is RIGHT-CENSORED — each run was killed at the ceiling, so it
    /// cannot say whether the read wanted 35 s or 300 s. The deadline is therefore chosen as twice
    /// what it had rather than as a fitted value.</para>
    ///
    /// <para><b>Above.</b> <c>DarlingWorker.s_analysisTimeout</c> gives the pass 120 s, shared by the
    /// fact collector's thirty-one reads, up to eleven baseline computations per server, the anomaly
    /// detector and the drill-down. A per-command deadline approaching that figure lets ONE stalled
    /// command consume the pass and cost the server every other fact — strictly worse than the
    /// failure being fixed, which loses one metric.</para>
    /// </summary>
    [Fact]
    public void TheAnalysisDeadlineClearsTheObservedCeilingWithoutOwningThePassBudget()
    {
        const int ObservedCeilingSeconds = 31;
        const int AnalysisPassBudgetSeconds = 120;

        Assert.True(
            DarlingAnalysisService.AnalysisCommandTimeoutSeconds > ObservedCeilingSeconds,
            $"The deadline ({DarlingAnalysisService.AnalysisCommandTimeoutSeconds}s) must clear the "
            + $"observed {ObservedCeilingSeconds}s ceiling or #2871 reproduces: nineteen io_latency "
            + "baseline failures all landed between 30.1s and 31.4s.");

        Assert.True(
            DarlingAnalysisService.AnalysisCommandTimeoutSeconds <= AnalysisPassBudgetSeconds / 2,
            $"The deadline ({DarlingAnalysisService.AnalysisCommandTimeoutSeconds}s) must leave at "
            + $"least half of the {AnalysisPassBudgetSeconds}s pass for everything else in it. One "
            + "stalled command must not cost a server every other fact and baseline.");
    }

    /// <summary>
    /// No command in this pass may be created by a delegate the assembly hands to someone else,
    /// because a deadline set at the hand-off is then the only one that command can get.
    ///
    /// <para>This is the hole #2871's census could not see, and it is the same one #2874 found in the
    /// viewer. <c>PgFactCollector.Waits.cs</c> and <c>PgDrillDownCollector.Blocking.cs</c> both passed
    /// <c>connection.CreateCommand</c> as a bare METHOD GROUP into
    /// <c>PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync</c>, which sets only <c>CommandText</c> on
    /// the result - so both reads ran on Npgsql's undocumented 30 s default while
    /// <see cref="EveryAnalysisPassCommandSetsAnExplicitTimeout"/> read them as clean, there being no
    /// <c>(</c> for either census regex to match. The fix at both sites is a factory lambda that stamps
    /// the deadline before returning, each with the constant that governs its OWN pass rather than one
    /// shared number.</para>
    ///
    /// <para><b>Scoped to this assembly, not repo-wide.</b> The same shape appears four more times in
    /// <c>Lite</c> - <c>DuckDbFactCollector.Waits.cs</c>, <c>DrillDownCollector.Blocking.cs</c> and two
    /// in <c>LocalDataService.Blocking.cs</c> - but those build <c>DuckDBCommand</c>, and
    /// <c>RemoteCollectorService.GetLastCollectedTimeAsync</c>'s doc comment already records why that is
    /// a different question: <c>DuckDBCommand.CommandTimeout</c> defaults to <b>0, meaning no limit</b>,
    /// so there is no inherited ceiling to exceed and nothing to cancel. There is no equivalent defect
    /// there to fix, and sweeping those four would assert a bound that cannot be shown to bind anything -
    /// a pin that cannot fail. Whether a DuckDB command wants a deadline at all is a separate question
    /// with its own measurement, and that comment asks the next reader to re-confirm the 0 default
    /// before relying on it.</para>
    ///
    /// <para>Comments and string literals are blanked first. <b>Stated precisely: that stripping is
    /// DEFENSIVE here, not load-bearing.</b> Three comments in this assembly name
    /// <c>connection.CreateCommand</c> in running text while explaining why it is no longer passed
    /// bare, but all three are followed by a backtick or <c>&lt;/c&gt;</c> rather than by <c>,</c> or
    /// <c>)</c>, so the unstripped scan finds zero hits today - measured, not assumed. It is carried
    /// anyway because the viewer's sibling pin DID trip on its own explanation, one comma is the whole
    /// difference, and a pin that fails the build on the prose describing its fix is the most annoying
    /// possible false positive. <see cref="TheFactoryHandoffScan_ReadsCodeNotProse"/> pins the
    /// behaviour so the stripper cannot be dropped as dead weight.</para>
    /// </summary>
    [Fact]
    public void NoAnalysisPassCommand_IsCreatedByABareMethodGroupHandoff()
    {
        var offenders = new List<string>();

        foreach (var file in AnalysisSources())
        {
            var code = StripCommentsAndStrings(File.ReadAllText(file));

            foreach (Match handoff in s_commandFactoryHandoff.Matches(code))
            {
                var line = code.Take(handoff.Index).Count(c => c == '\n') + 1;
                offenders.Add($"{Path.GetFileName(file)}:{line}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} site(s) hand CreateCommand over as a method group, so the callee builds "
            + "an untimed command this assembly cannot reach - pass a factory lambda that stamps the "
            + "deadline this pass's own constant specifies: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The method-group scan must read CODE, not prose. Three comments in this assembly name
    /// <c>connection.CreateCommand</c> while explaining why it is no longer passed bare, and a scan
    /// that did not strip comments would fail the build on the explanation of its own fix.
    /// </summary>
    [Theory]
    [InlineData("        Append(connection.CreateCommand, rows);\n", true)]
    [InlineData("        Append(connection.CreateCommand);\n", true)]
    [InlineData("        /* not the bare connection.CreateCommand, but a factory. */\n", false)]
    [InlineData("        // pass connection.CreateCommand) here? no.\n", false)]
    [InlineData("        /// <c>connection.CreateCommand</c> method group, which inherits the default\n", false)]
    [InlineData("        var s = \"connection.CreateCommand,\";\n", false)]
    [InlineData("        var command = connection.CreateCommand();\n", false)]
    public void TheFactoryHandoffScan_ReadsCodeNotProse(string source, bool expectedOffender)
    {
        var code = StripCommentsAndStrings(source);

        Assert.Equal(expectedOffender, s_commandFactoryHandoff.IsMatch(code));
    }

    /// <summary>
    /// The stamping factory must read as TIMED through the same line window the sweep uses - the
    /// deadline sits on the statement after the <c>CreateCommand()</c> call inside the lambda, which is
    /// the shape both fixed sites now carry. The bare method group must read as UNMATCHED by the ctor
    /// regex entirely, which is precisely why the scan above has to exist.
    /// </summary>
    [Theory]
    [InlineData(
        "                () =>\n"
        + "                {\n"
        + "                    var dmvCommand = connection.CreateCommand();\n"
        + "                    dmvCommand.CommandTimeout = FactCommandTimeoutSeconds;\n"
        + "                    return dmvCommand;\n"
        + "                },\n",
        true, true)]
    [InlineData(
        "                connection.CreateCommand, rows, context.ServerId,\n"
        + "                context.CancellationToken);\n",
        false, false)]
    public void TheCtorScan_SeesTheStampingFactory_AndNotTheMethodGroup(
        string source, bool expectedMatched, bool expectedTimed)
    {
        var lines = source.Split('\n');
        var matched = false;
        var timed = false;

        for (var i = 0; i < lines.Length; i++)
        {
            if (!s_commandCtor.IsMatch(lines[i]))
            {
                continue;
            }

            matched = true;
            var window = string.Join("\n", lines.Skip(i).Take(Math.Min(3, lines.Length - i)));
            timed = s_setsTimeout.IsMatch(window);
            break;
        }

        Assert.Equal(expectedMatched, matched);
        Assert.Equal(expectedTimed, timed);
    }

    /* Blanks out comments and string literals while preserving newlines, so a regex meant for code
       cannot match prose or a literal. Newlines survive because offenders are reported by line. The
       CI-proven copy from ViewerCommandTimeoutTests, kept as a private copy the way the sibling pins
       keep theirs - extracting a shared helper across five test files is a refactor those lanes
       should take together or not at all. */

    private static string StripCommentsAndStrings(string text)
    {
        var sb = new StringBuilder(text.Length);
        var i = 0;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                var end = SkipVerbatimString(text, i + 2);
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            if (c == '"')
            {
                var end = SkipRegularString(text, i + 1);
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                var nl = text.IndexOf('\n', i);
                var end = nl < 0 ? text.Length : nl;
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var end = close < 0 ? text.Length : close + 2;
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            sb.Append(c);
            i++;
        }

        return sb.ToString();
    }

    private static void Blank(StringBuilder sb, string text, int start, int end)
    {
        for (var j = start; j < end; j++)
        {
            sb.Append(text[j] == '\n' ? '\n' : ' ');
        }
    }

    private static int SkipVerbatimString(string text, int i)
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

    private static int SkipRegularString(string text, int i)
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

    private static IEnumerable<string> AnalysisSources()
    {
        var files = Directory.GetFiles(
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis"),
            "*.cs",
            SearchOption.TopDirectoryOnly);

        Assert.NotEmpty(files);

        return files.OrderBy(f => f, StringComparer.Ordinal);
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
