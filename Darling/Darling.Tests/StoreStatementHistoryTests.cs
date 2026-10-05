/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The store statement history's pure parts (#5097): the epoch decision and the SQL text's shape. No store.</summary>
public sealed class StoreStatementHistoryTests
{
    private static readonly DateTime Previous = new(2026, 1, 1, 10, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void NoPreviousCapture_Rebaselines() =>
        Assert.Equal(StoreStatementHistory.Epoch.Rebaseline, StoreStatementHistory.DecideEpoch(null, null, Previous));

    [Fact]
    public void TheSameStamp_IsTheSameEpoch() =>
        Assert.Equal(StoreStatementHistory.Epoch.SameEpoch, StoreStatementHistory.DecideEpoch(Previous, Previous.AddDays(-3), Previous.AddDays(-3)));

    [Fact]
    public void BothStampsAbsent_IsTheSameEpoch() =>
        Assert.Equal(StoreStatementHistory.Epoch.SameEpoch, StoreStatementHistory.DecideEpoch(Previous, null, null));

    [Fact]
    public void AStampAfterThePreviousCapture_IsAResetInsideTheInterval() =>
        Assert.Equal(StoreStatementHistory.Epoch.ResetInsideInterval, StoreStatementHistory.DecideEpoch(Previous, Previous.AddDays(-3), Previous.AddMinutes(20)));

    [Fact]
    public void AStampBeforeThePreviousCapture_ThatDiffers_Rebaselines() =>
        Assert.Equal(StoreStatementHistory.Epoch.Rebaseline, StoreStatementHistory.DecideEpoch(Previous, Previous.AddDays(-3), Previous.AddDays(-1)));

    [Fact]
    public void AStampThatVanished_Rebaselines() =>
        Assert.Equal(StoreStatementHistory.Epoch.Rebaseline, StoreStatementHistory.DecideEpoch(Previous, Previous.AddDays(-3), null));

    [Fact]
    public void TheSnapshot_ReadsOnlyTheReaderFunction_RanksByTimeAndBindsTheCap()
    {
        Assert.DoesNotContain("pg_stat_statements", StoreStatementHistory.SnapshotSql + StoreStatementHistory.CaptureCurrentSql, StringComparison.Ordinal);
        Assert.Contains("config.store_statement_stats()", StoreStatementHistory.CaptureCurrentSql, StringComparison.Ordinal);
        Assert.Contains("config.store_statement_stats_info()", StoreStatementHistory.InfoSql, StringComparison.Ordinal);
        Assert.Contains("queryid IS NOT NULL", StoreStatementHistory.SnapshotSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY a.delta_total_exec_ms DESC", StoreStatementHistory.SnapshotSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $2", StoreStatementHistory.SnapshotSql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"LIMIT\s+\d", StoreStatementHistory.SnapshotSql);
    }

    [Fact]
    public void TheRetentionDeletes_AreOnCaptureTime_AndTheBaselineIsNeverPurged()
    {
        Assert.Matches(@"WHERE capture_time < \$1", StoreStatementHistory.HistoryRetentionDeleteSql);
        Assert.Matches(@"WHERE capture_time < \$1", StoreStatementHistory.CaptureRetentionDeleteSql);
        Assert.Equal(90, StoreStatementHistory.RetentionDays);
        Assert.Equal(100, StoreStatementHistory.TopStatements);
    }

    [Fact]
    public void EveryCommandInTheFile_SetsADeadline_ThroughOneHelper()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "StoreStatementHistory.cs");
        var constructions = Regex.Matches(source, @"new\s+NpgsqlCommand\b|\bnew\(sql, connection, transaction\)");
        Assert.Single(constructions);
        Assert.Contains("CommandTimeout = SnapshotTimeoutSeconds", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWorkerRunsTheSnapshot_InItsOwnCatch_BeforeTheCollectorCostFlush()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var call = worker.IndexOf("await StoreStatementHistory.SnapshotAsync(", StringComparison.Ordinal);
        var flush = worker.IndexOf("await _collectorCost.FlushAsync(", StringComparison.Ordinal);
        Assert.True(call > 0 && call < flush, "the snapshot sits before the collector-cost flush");

        var tail = worker[call..flush];
        Assert.Contains("catch (Exception ex) when (ex is not OperationCanceledException)", tail, StringComparison.Ordinal);
        Assert.Contains("_storeStatementHistoryWarned", tail, StringComparison.Ordinal);
        Assert.Contains("LogWarning", tail, StringComparison.Ordinal);
        Assert.Contains("LogDebug", tail, StringComparison.Ordinal);
    }
}
