/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store does NOT have one timestamp frame, so this pins the frame PER COLUMN against the read path
/// that depends on it. Both facts below are load-bearing in opposite directions, which is the whole reason
/// this file exists rather than a single project-wide rule.
///
/// <para><b><c>cpu_utilization_stats.sample_time</c> is deliberately the monitored server's LOCAL wall
/// clock.</b> <c>ViewerDataService.Cpu</c> says so outright — "Unlike every other stored column,
/// sample_time is the MONITORED SERVER'S LOCAL wall clock (SYSDATETIME() on the server, minus each
/// ring-buffer sample's age), NOT naive UTC" — and #1262 de-skews it in SQL per collection batch, while
/// Lite windows it through a purpose-named <c>GetTimeRangeServerLocal</c> and shifts it by
/// <c>ServerTimeHelper.UtcOffsetMinutes</c>. Converting that column to UTC breaks Lite's CPU chart on both
/// the query window and the plotted x-position. Darling would survive only because #1262's per-batch offset
/// self-calibrates to zero, which makes the regression silent on one app and visible on the other — the
/// worst shape for catching it.</para>
///
/// <para><b><c>memory_pressure_events.sample_time</c> must be UTC.</b> Same ring-buffer arithmetic, but
/// NEITHER reader corrects for a local frame: Darling feeds it straight to
/// <c>ViewerTimeHelper.ForDisplay</c>, which takes naive-UTC input, and Lite windows it with the UTC-based
/// <c>GetTimeRange</c> and then adds <c>UtcOffsetMinutes</c> when plotting. Both want UTC, so the collector
/// was the side that was wrong.</para>
///
/// <para>The first cut of this pin asserted a store-wide "all naive timestamps are UTC" rule and would have
/// forbidden the CPU collector's intentional local clock. Reviewing the CONSUMERS is what corrected it,
/// which is why each fact here names the read path it protects rather than the function it matches.</para>
/// </summary>
public sealed class CollectorTimestampFrameTests
{
    /// <summary>
    /// Local and UTC clock functions. <c>IgnoreCase</c> because T-SQL function names are case-insensitive:
    /// <c>getdate()</c> runs identically to <c>GETDATE()</c> and would otherwise walk straight past a guard
    /// that only knows the canonical casing.
    /// </summary>
    private static readonly Regex s_localClock = new(
        @"\b(?:GETDATE|SYSDATETIME)\s*\(\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex s_utcClock = new(
        @"\b(?:GETUTCDATE|SYSUTCDATETIME)\s*\(\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    [Fact]
    public void MemoryPressureEvents_StampsSampleTimeInUtc()
    {
        var sql = QueryTextOf("MemoryPressureEventsCollector.cs");

        Assert.True(
            s_utcClock.IsMatch(sql),
            "MemoryPressureEventsCollector must derive its STORED sample_time from SYSUTCDATETIME(). Both "
            + "readers assume naive UTC - Darling passes the column to ViewerTimeHelper.ForDisplay, Lite "
            + "windows it with the UTC-based GetTimeRange and adds UtcOffsetMinutes when plotting - so a "
            + "local clock here puts the whole memory-pressure series one UTC offset away from every other "
            + "lane. Measured at 4h on the us-east-2 fleet.");

        Assert.False(
            s_localClock.IsMatch(sql),
            "MemoryPressureEventsCollector's query still contains a LOCAL clock function; its stored "
            + "sample_time must be UTC (see above).");
    }

    [Fact]
    public void CpuUtilization_KeepsSampleTimeInTheServersLocalClock()
    {
        var sql = QueryTextOf("CpuUtilizationCollector.cs");

        Assert.True(
            s_localClock.IsMatch(sql),
            "CpuUtilizationCollector's ring-buffer arm must keep SYSDATETIME(). Its server-local "
            + "sample_time is an intentional documented convention: #1262 de-skews it in SQL per collection "
            + "batch, and Lite windows it through GetTimeRangeServerLocal and shifts it by "
            + "ServerTimeHelper.UtcOffsetMinutes. Converting it to UTC breaks Lite's CPU chart on both the "
            + "window and the plotted position, while Darling stays green because the per-batch de-skew "
            + "self-calibrates to zero - a regression visible on one app only.");
    }

    /// <summary>
    /// <c>default_trace_events.event_time</c> is the monitored server's LOCAL wall clock — the .trc files
    /// store local time and <c>ft.StartTime</c> is shipped verbatim — so every bound this collector compares
    /// against it has to come from the server's clock too.
    ///
    /// <para>Only the archival-empty fallback was ever at risk, and it is the branch nobody exercises: the
    /// steady-state bound is the watermark, which IS a previously-stored <c>event_time</c> and therefore
    /// already server-local, and the true-first-run bound is a 1900 sentinel that no clock can misread. The
    /// third branch fires only when the hot store has been emptied by retention/archival on a server that HAS
    /// collected before, which is why a host-UTC bound there survived review: it is correct on a UTC server,
    /// and every store anyone develops against is UTC.</para>
    ///
    /// <para>Pinned against the SOURCE rather than a rendered query because the point is the absence of the
    /// alternative: this collector must carry a LOCAL clock and no UTC clock at all, which is the exact
    /// inverse of <see cref="MemoryPressureEvents_StampsSampleTimeInUtc"/>. Both facts are per-TABLE. There is
    /// no store-wide rule to appeal to, and no name-keyed one either — <c>system_health.event_time</c> is UTC
    /// under the same column name, which is why <c>StoreSqlClockDisciplineTests</c> cannot judge either.</para>
    /// </summary>
    [Fact]
    public void DefaultTraceEvents_DerivesEveryCutoffFromTheServersClock()
    {
        var sql = QueryTextOf("DefaultTraceEventsCollector.cs");

        Assert.True(
            s_localClock.IsMatch(sql),
            "DefaultTraceEventsCollector's archival-empty fallback must derive its cutoff from SYSDATETIME(). "
            + "ft.StartTime is the server's LOCAL wall clock, so a host-supplied UTC bound delivers the wrong "
            + "window by the server's offset: 17 hours at UTC-7 (a hole in trace history nothing refills) and "
            + "34 at UTC+10 (re-ingesting events already in parquet, which v_default_trace_events UNIONs "
            + "without dedup — the double-count the bound exists to prevent). Both are silent.");

        Assert.False(
            s_utcClock.IsMatch(sql),
            "DefaultTraceEventsCollector's query contains a UTC clock function; every bound it compares "
            + "against the server-local ft.StartTime must be in the server's clock (see above).");
    }

    /// <summary>
    /// The collector's query constants, with COMMENT spans removed and string literals kept — the inverse
    /// of <c>CSharpSourceWalker.StripCommentsAndStrings</c>, because the SQL under test IS a verbatim
    /// literal. Both C# and embedded T-SQL comment forms go, since this codebase's SQL carries its
    /// reasoning in <c>/* ... */</c> blocks that name the very functions matched here.
    /// </summary>
    private static string QueryTextOf(string file)
    {
        var path = Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors", file);

        Assert.True(File.Exists(path), $"collector not found: {path}");

        var text = File.ReadAllText(path);
        var withoutBlocks = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        var withoutSlashes = Regex.Replace(withoutBlocks, @"//[^\r\n]*", " ");

        return Regex.Replace(withoutSlashes, @"--[^\r\n]*", " ");
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
