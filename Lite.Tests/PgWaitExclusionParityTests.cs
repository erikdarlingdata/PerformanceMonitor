/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2630: the two PostgreSQL wait collectors must agree about what counts as a wait.
///
/// <para>
/// They read different sources for the same measurement — <c>pg_wait_stats</c> takes Aurora's own
/// instrumentation, <c>pg_wait_sampling</c> takes a sampling profiler — and #2625 now tells a
/// stock-PostgreSQL operator, in as many words, to read the second one INSTEAD of the first. Two
/// exclusion lists means one server answers "what is it waiting on" differently depending on its flavor,
/// which is worse than either answer alone.
/// </para>
///
/// <para>
/// They did disagree. The sampler excluded <c>Activity</c> and nothing else, and on the first target with
/// real client connections <c>ClientRead</c> was <b>2,717,290 of 2,717,989 samples — 100.0%</b>, with every
/// real event rounded to zero. The unfiltered profile on that target held 17,864,575 samples of
/// <c>Activity</c>, <c>Client</c> and <c>Timeout</c> against 2,150 samples of everything else.
/// </para>
///
/// <para>
/// <c>Activity</c> was excluded from the start because it is visibly absurd — background sleep loops
/// accumulating a second per second of uptime. <c>Client</c> and <c>Timeout</c> need a CLIENT to be idle
/// before they dominate, which no container and no CI run provides. That is the whole reason this went
/// unnoticed, and the reason the guard is a parity assertion rather than a list: whichever list is edited
/// next, the other has to follow.
/// </para>
/// </summary>
public class PgWaitExclusionParityTests
{
    [Fact]
    public void TheAuroraCollectorExcludesTheThreeTypesThatAreNotWork()
    {
        Assert.Equal(
            new[] { "Activity", "Client", "Timeout" },
            PgWaitStatsCollector.IgnoredWaitTypes.OrderBy(t => t, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The sampler splices the SAME set into its SQL. Asserted against the set rather than against three
    /// string literals, so adding a fourth type to the shared definition carries the sampler with it.
    /// </summary>
    [Fact]
    public void TheSamplerExcludesEveryTypeTheAuroraCollectorDoes()
    {
        var sql = PgWaitSamplingCollector.Instance.BuildQuery(Context()).Text;

        foreach (var type in PgWaitStatsCollector.IgnoredWaitTypes)
        {
            Assert.Contains($"'{type}'", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The one type that must NOT be filtered, and the reason the predicate coalesces before it compares.
    ///
    /// <para>A backend on CPU arrives with a NULL <c>event_type</c>. Comparing NULL against a list yields
    /// NULL, which a <c>WHERE</c> discards — so a filter written the obvious way would silently drop this
    /// collector's distinctive signal, the one that lets its share column answer "waiting or working?"
    /// before it answers "waiting on what?". Measured after the fix: CPU 1,914 samples, IO 204, LWLock 9,
    /// IPC 2 — the CPU row survived and leads.</para>
    /// </summary>
    [Fact]
    public void TheCpuRowSurvivesTheFilter_BecauseTheTypeIsCoalescedBeforeItIsCompared()
    {
        var sql = PgWaitSamplingCollector.Instance.BuildQuery(Context()).Text;

        Assert.Contains("coalesce(p.event_type, 'CPU') NOT IN", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("'CPU'", string.Join(",", PgWaitStatsCollector.IgnoredWaitTypes), StringComparison.Ordinal);
    }

    /// <summary>
    /// And the filter is applied at COLLECTION, not at read. These are cumulative counters: leaving the
    /// excluded types in would spend store on millions of samples of a server doing nothing, and every
    /// reader would have to remember to filter them again.
    /// </summary>
    [Fact]
    public void TheExclusionIsInTheCollectorsQuery_NotLeftToTheReader()
    {
        var sql = PgWaitSamplingCollector.Instance.BuildQuery(Context()).Text;

        var whereIndex = sql.IndexOf("WHERE", StringComparison.Ordinal);
        var groupIndex = sql.IndexOf("GROUP BY", StringComparison.Ordinal);

        Assert.True(whereIndex >= 0 && groupIndex > whereIndex, "The collector query has no WHERE ahead of its GROUP BY.");
        Assert.Contains("Client", sql[whereIndex..groupIndex], StringComparison.Ordinal);
    }

    private static CollectorContext Context()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc),
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170000,
            },
        };
}
