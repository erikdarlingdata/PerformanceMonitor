/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5574: the pure gates around the perfmon_stats chunk re-group. The live behaviour (settings change, newest-first
/// re-group, budget, no-op second pass) needs a TimescaleDB 2.14.1+ store and is the part still to be written as a
/// live class beside <c>CollectionLogSegmentByLiveTests</c>.
/// </summary>
public sealed class PerfmonRegroupGateTests
{
    private const string Wanted = "server_id, counter_name";

    [Fact]
    public void ThePerfmonSegmentBy_IsServerAndCounter_InTheSpellingTheConvergenceReadJoinsWith()
    {
        Assert.Equal("server_id, counter_name", TimescaleSupport.PerfmonStatsSegmentBy);
        Assert.Equal("server_id,counter_name", TimescaleSupport.PerfmonStatsChunkSegmentBy);
        Assert.Equal(TimescaleSupport.PerfmonStatsSegmentBy, TimescaleSupport.CompressionSegmentByFor("perfmon_stats"));
        Assert.Equal(TimescaleSupport.PerfmonStatsSegmentBy, TimescaleSupport.CompressionSegmentByFor("collect.perfmon_stats"));
        Assert.Contains("compress_segmentby = 'server_id, counter_name'", TimescaleSupport.EnableCompressionSql("perfmon_stats"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("2.13.1", true, Wanted, false)]
    [InlineData("2.14.0", false, Wanted, false)]
    public void TheRegroup_DoesNotRun_BelowTheFloor_OrWithoutThePerChunkView(string version, bool hasView, string segmentBy, bool expectOpen)
    {
        var reason = TimescaleSupport.PerfmonRegroupBlockedReason(TimescaleSupport.ParseTimescaleVersion(version), hasView, segmentBy);
        Assert.Equal(expectOpen, reason is null);
    }

    [Fact]
    public void TheRegroup_DoesNotRun_UntilTheHypertableCarriesTheNewGrouping_AndRunsOtherwise()
    {
        var version = TimescaleSupport.ParseTimescaleVersion("2.30.1");
        Assert.NotNull(TimescaleSupport.PerfmonRegroupBlockedReason(version, true, "server_id"));
        Assert.NotNull(TimescaleSupport.PerfmonRegroupBlockedReason(version, true, null));
        Assert.Null(TimescaleSupport.PerfmonRegroupBlockedReason(version, true, Wanted));

        /* An unknown version is not a reason: the view test and the segmentby test decide. */
        Assert.Null(TimescaleSupport.PerfmonRegroupBlockedReason(null, true, Wanted));
        Assert.NotNull(TimescaleSupport.PerfmonRegroupBlockedReason(null, false, Wanted));
    }

    [Fact]
    public void ThePerChunkSettingsView_ArrivedIn2_14_1_NotIn2_14_0()
    {
        Assert.Equal(new Version(2, 14, 1), TimescaleSupport.PerChunkCompressionSettingsViewFrom);
        Assert.True(TimescaleSupport.PerChunkCompressionSettingsViewFrom > TimescaleSupport.CompressionSettingsChangeWithCompressedChunksFrom);
    }

    /// <summary>The hourly pass must never wait for a rewrite: <c>StartIfIdle</c> returns at once even while the run is
    /// still opening its connection, a second start while one is in flight does nothing, and a run that fails ends
    /// without throwing so the next hourly start can try again.</summary>
    [Fact]
    public async Task TheDrain_StartsWithoutWaiting_RunsOnce_AndANewRunFollowsAFailedOne()
    {
        var release = new TaskCompletionSource<NpgsqlConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var opens = 0;
        var logger = new CapturingTestLogger();
        var drain = new PerfmonRegroupDrain(token =>
        {
            Interlocked.Increment(ref opens);
            return new ValueTask<NpgsqlConnection>(release.Task);
        }, logger);

        Assert.True(drain.StartIfIdle(CancellationToken.None));
        Assert.False(drain.StartIfIdle(CancellationToken.None));

        release.SetException(new InvalidOperationException("no store"));
        await drain.Completion!.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.True(drain.Completion.IsCompletedSuccessfully);
        Assert.Equal(1, Volatile.Read(ref opens));
        Assert.Contains(logger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains("no store", StringComparison.Ordinal));
        Assert.True(drain.StartIfIdle(CancellationToken.None));
        await drain.Completion.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(2, Volatile.Read(ref opens));
    }

    /// <summary>The service stopping ends a run that is still opening its connection, quietly.</summary>
    [Fact]
    public async Task TheDrain_StopsPromptly_WhenTheServiceStops()
    {
        using var stop = new CancellationTokenSource();
        var logger = new CapturingTestLogger();
        var drain = new PerfmonRegroupDrain(async token =>
        {
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("unreachable");
        }, logger);

        Assert.True(drain.StartIfIdle(stop.Token));
        stop.Cancel();
        await drain.Completion!.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.True(drain.Completion.IsCompletedSuccessfully);
        Assert.DoesNotContain(logger.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal));
    }

    /// <summary>
    /// The wiring, as source text because the pass is an instance method that needs a store: the loop is started from the
    /// hourly pass only (never the start path), after the convergence list, behind the TimescaleDB flag, and the pass
    /// does not run the rewrite itself; shutdown joins it.
    /// </summary>
    [Fact]
    public void TheHourlyPass_StartsTheDrain_AfterTheList_BehindTheTimescaleFlag_AndNeverAwaitsTheRewrite()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var passStart = worker.IndexOf("private async Task ConvergeStoreObjectsAsync(", StringComparison.Ordinal);
        Assert.True(passStart > 0);
        var pass = worker.Substring(passStart, worker.IndexOf("private PerfmonRegroupDrain? _perfmonRegroupDrain;", passStart, StringComparison.Ordinal) - passStart);
        var loopEnd = pass.IndexOf("RunStoreObjectConvergenceStepAsync(connection, step, tally, _logger, budget.Token, hourly: true);", StringComparison.Ordinal);
        var start = pass.IndexOf("StartPerfmonRegroupDrain(cancellationToken);", StringComparison.Ordinal);
        var gate = pass.IndexOf("if (timescaleAvailable)", loopEnd, StringComparison.Ordinal);
        Assert.True(loopEnd > 0 && gate > loopEnd && start > gate, "the drain must start after the list, behind the TimescaleDB flag");
        Assert.DoesNotContain("RegroupPerfmonChunkAsync", pass, StringComparison.Ordinal);
        Assert.DoesNotContain("await StartPerfmonRegroupDrain", pass, StringComparison.Ordinal);
        Assert.Equal(1, worker.Split("StartPerfmonRegroupDrain(", StringSplitOptions.None).Length - 2);
        Assert.DoesNotContain("perfmon chunk re-group", worker, StringComparison.Ordinal);
        Assert.Contains("_perfmonRegroupDrain?.Completion is { IsCompleted: false }", worker, StringComparison.Ordinal);
    }
}
