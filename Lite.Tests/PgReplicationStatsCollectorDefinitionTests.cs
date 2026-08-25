/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2544, the replication slice. The assertions are about the two things measurement showed matter: that
/// BOTH the byte distance and the time lag are captured, and that every distance is measured from the
/// primary's current WAL position rather than from what was sent.
/// </summary>
public class PgReplicationStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgReplicationStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
            },
            ExcludedDatabases = Array.Empty<string>(),
        }).Text;

    /// <summary>
    /// Both measures, because they disagree in the case that matters. Measured against a standby holding
    /// <c>pg_wal_replay_pause()</c>: 33.7 MB behind while the time lag read 2.8 seconds. Storing the lag
    /// alone would make a stalled replica look survivable.
    /// </summary>
    [Fact]
    public void BothTheByteDistanceAndTheTimeLag_AreCollected()
    {
        var names = PgReplicationStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("replay_bytes_behind", names);
        Assert.Contains("replay_lag_ms", names);
    }

    /// <summary>
    /// Four distances, not one. In the stalled run <c>sent</c>, <c>write</c> and <c>flush</c> were all ZERO
    /// while <c>replay</c> was 33.7 MB behind — so the fault was purely apply, which no single column shows.
    /// </summary>
    [Fact]
    public void AllFourStages_AreMeasuredSeparately()
    {
        var names = PgReplicationStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        foreach (var stage in new[] { "sent_bytes_behind", "write_bytes_behind", "flush_bytes_behind", "replay_bytes_behind" })
        {
            Assert.Contains(stage, names);
        }
    }

    /// <summary>
    /// Distance is measured from the primary's CURRENT WAL position, never from <c>sent_lsn</c>. Using what
    /// was sent as the baseline hides a sender that has itself fallen behind — the distance would read zero
    /// while the standby was arbitrarily far from the truth.
    /// </summary>
    [Fact]
    public void DistanceIsMeasuredFromCurrentWalLsn_NotFromSentLsn()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        /* Every pg_wal_lsn_diff must take pg_current_wal_lsn() as its FIRST argument. */
        var diffs = Regex.Matches(sql, @"pg_wal_lsn_diff\(\s*([^,]+),").Cast<Match>().ToArray();

        Assert.NotEmpty(diffs);
        Assert.All(diffs, m => Assert.Contains("pg_current_wal_lsn()", m.Groups[1].Value, StringComparison.Ordinal));
    }

    /// <summary>
    /// The lag columns are PostgreSQL <c>interval</c>s and must be converted at the source. Storing an
    /// interval would force every consumer to parse it.
    /// </summary>
    [Fact]
    public void LagIntervals_AreConvertedToMilliseconds()
        => Assert.Equal(3, Regex.Matches(Sql(), @"EXTRACT\(EPOCH FROM r\.\w+_lag\) \* 1000").Count);

    /// <summary>
    /// <c>sync_state</c> is collected because a SYNC standby falling behind blocks commits on the primary.
    /// Identical lag, entirely different severity from an async one's.
    /// </summary>
    [Fact]
    public void SyncStateIsCollected_BecauseItChangesTheSeverity()
        => Assert.Contains("sync_state", PgReplicationStatsCollector.Instance.PayloadColumns.Select(c => c.Name));

    /// <summary>
    /// Applies to every target INCLUDING standbys: a cascading replica's downstream is as worth watching as
    /// a primary's, and recovery state changes on failover without a dispatch gate noticing. A standby with
    /// no downstream returns zero rows, which is correct rather than an error.
    /// </summary>
    [Fact]
    public void AppliesTo_EveryPostgresTarget_IncludingStandbys()
    {
        foreach (var major in new[] { 13, 14, 16, 17, 18 })
        {
            Assert.True(PgReplicationStatsCollector.Instance.AppliesTo(
                new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));
        }
    }

    /// <summary>
    /// <c>backend_start</c> is converted with <c>AT TIME ZONE 'UTC'</c> and never a bare cast — the cast
    /// renders in the SESSION's TimeZone, so a store session east of UTC would record a stamp hours from the
    /// one the server meant.
    /// </summary>
    [Fact]
    public void BackendStart_IsConvertedToUtc_NotBareCast()
    {
        var sql = Sql();

        Assert.Contains("AT TIME ZONE 'UTC'", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"backend_start\s*::\s*timestamp"), sql);
    }

    /// <summary>
    /// Catalog reads are schema-qualified: <c>pg_catalog</c> is searched implicitly but not necessarily
    /// FIRST, so an unqualified read can resolve to an object a user created in a schema earlier in the
    /// monitoring login's search_path.
    /// </summary>
    [Fact]
    public void EveryCatalogRead_IsSchemaQualified()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        foreach (var name in new[] { "pg_stat_replication", "pg_wal_lsn_diff", "pg_current_wal_lsn" })
        {
            foreach (Match match in Regex.Matches(sql, $@"(\S*)\b{Regex.Escape(name)}\b"))
            {
                /* ENDS WITH, not equals. These calls NEST — pg_catalog.pg_wal_lsn_diff(pg_catalog.
                   pg_current_wal_lsn(), ...) — so the greedy \S* captures the enclosing call as well as the
                   qualification. Asserting equality fails on correctly-qualified SQL, which is what the
                   first draft of this test did. */
                Assert.EndsWith("pg_catalog.", match.Groups[1].Value, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>One SELECT alias per payload column, in order — a mismatch is a silently shifted binary
    /// COPY, which writes every value into the wrong column rather than failing.</summary>
    [Fact]
    public void SelectAliases_MatchThePayloadOrder()
    {
        var expected = PgReplicationStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        var selected = Sql()
            .Split('\n')
            .Where(line => !line.TrimStart().StartsWith("FROM", StringComparison.Ordinal))
            .Select(line => Regex.Match(line, @"\bAS\s+([a-z_]+),?\s*$"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value)
            .ToArray();

        Assert.Equal(expected, selected);
    }
}
