/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2412: <c>App.AnalysisTimeoutSeconds</c> cancelled nothing. The scheduler raced
/// <c>AnalyzeAsync</c> against a <c>Task.Delay</c> and, on losing, logged one warning and moved
/// on while the pass kept running — so the setting bounded how long the LOOP waited and nothing
/// about the analysis itself.
///
/// <para>The consequence was worse than the mislabelled knob. The in-flight guard is released only
/// on true completion (correctly — that is what stops a hung server piling up tasks), so a pass
/// that never completed left its marker set for the life of the process and that server was
/// skipped by <c>continue</c> on every later cycle, silently, with the single original warning the
/// only trace it ever left.</para>
///
/// <para>Two layers guard the repair. The behavioural pair proves a pass carrying a cancelled
/// budget ABANDONS rather than running to completion — and abandons for that reason rather than
/// being turned away by the 24-hour data gate, which would make the assertion vacuous. The source
/// pin holds the three properties of the scheduler that no test in this suite can reach, because
/// <c>CollectionBackgroundService</c> needs a whole host to instantiate.</para>
/// </summary>
public sealed class AnalysisBudgetTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public AnalysisBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>
    /// The discriminator is <c>LastAnalysisTime</c>. Every path that RUNS — a completed pass, and
    /// the insufficient-data gate — stamps it. Only the abandon path returns without stamping, so
    /// a null there cannot be produced by a pass that merely found nothing to say.
    ///
    /// <para>The control runs first, on the same seed and the same context, and is what stops this
    /// passing vacuously: it proves the pipeline reaches the end of a pass over this data, so the
    /// null that follows is the cancellation and not a broken fixture.</para>
    /// </summary>
    [Fact]
    public async Task AnalyzeAsync_WithACancelledBudget_AbandonsInsteadOfCompleting()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var control = new AnalysisService(_duckDb) { MinimumDataHours = 0 };
        await control.AnalyzeAsync(TestDataSeeder.CreateTestContext());
        Assert.NotNull(control.LastAnalysisTime);

        var service = new AnalysisService(_duckDb) { MinimumDataHours = 0 };
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var context = TestDataSeeder.CreateTestContext();
        context.CancellationToken = cts.Token;

        var findings = await service.AnalyzeAsync(context);

        Assert.Empty(findings);
        Assert.Null(service.InsufficientDataMessage);
        Assert.Null(service.LastAnalysisTime);
        Assert.False(service.IsAnalyzing);
    }

    /// <summary>
    /// The scheduler calls the four-argument overload, so the token has to survive the hop from
    /// that signature onto the context the pipeline actually reads. Drop that one assignment and
    /// this pass would run the whole pipeline over an empty window and stamp
    /// <c>LastAnalysisTime</c> — which is exactly what the assertion below refuses.
    /// </summary>
    [Fact]
    public async Task AnalyzeAsync_CarriesTheCallersBudgetOntoTheContext()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var service = new AnalysisService(_duckDb) { MinimumDataHours = 0 };
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var findings = await service.AnalyzeAsync(
            TestDataSeeder.TestServerId, TestDataSeeder.TestServerName, hoursBack: 4, cts.Token);

        Assert.Empty(findings);
        Assert.Null(service.LastAnalysisTime);
    }

    /// <summary>
    /// Three properties, each of which the fix is worthless without.
    ///
    /// <para>The pass must leave the loop's thread. DuckDB.NET implements no async execution, so
    /// every read completes synchronously on the calling thread and an <c>AnalyzeAsync</c> invoked
    /// inline ran to completion BEFORE the timeout race was reached — the race could never fire for
    /// the phase that would actually be slow.</para>
    ///
    /// <para>Something must raise the cancellation at the budget, or the token is decoration.</para>
    ///
    /// <para>And the skip branch must report. That bare <c>continue</c> is the whole defect: it is
    /// the line that made a permanently wedged server invisible.</para>
    /// </summary>
    [Fact]
    public void ScheduledAnalysis_OffloadsThePass_ArmsTheBudget_AndReportsAStuckServer()
    {
        /* Line endings are normalised because the assertions below span lines and the working
           copy is CRLF on Windows and LF elsewhere. */
        var source = File.ReadAllText(
            Path.Combine(FindRepoDirectory(Path.Combine("Lite", "Services")), "CollectionBackgroundService.cs"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The offload and the token have to be on the SAME call — a Task.Run somewhere in the file
           and an AnalyzeAsync somewhere else would satisfy two independent Contains checks while
           leaving the pass running inline on the loop's thread. */
        Assert.Matches(
            new Regex(@"Task\.Run\(\s*\(\)\s*=>\s*analysisService\.AnalyzeAsync\(serverId, serverName, hoursBack: 4, cts\.Token\)"),
            source);

        Assert.Contains(
            "passCts.CancelAfter(timeout)",
            source,
            StringComparison.Ordinal);

        Assert.Contains(
            "ReportStuckAnalysis(serverId, serverName, timeout);",
            source,
            StringComparison.Ordinal);

        /* The reporting has to back off. Scheduled analysis runs on a 30-minute default cadence, so
           a fixed repeat would be either slower than the cadence or one line per cycle forever. */
        Assert.Contains(
            "StuckAnalysisMaxBackoffDoublings",
            source,
            StringComparison.Ordinal);

        /* And the shutdown hold has to wait on an UNCANCELLED token. Offloading the pass onto the
           pool is what makes the hold necessary at all — the store work can now still be in flight
           when the loop is told to stop — and handing the already-fired stopping token to the wait
           would collapse it instantly, which looks identical to having waited. */
        Assert.Contains(
            "await analyzeTask.WaitAsync(AnalysisUnwindGrace, CancellationToken.None);",
            source,
            StringComparison.Ordinal);
    }

    private static string FindRepoDirectory(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new DirectoryNotFoundException($"Could not locate {relativePath} above {AppContext.BaseDirectory}");
    }
}
