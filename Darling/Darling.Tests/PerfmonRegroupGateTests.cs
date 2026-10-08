/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
}
