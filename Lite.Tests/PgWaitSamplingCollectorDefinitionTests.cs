/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// Pins the wait-sampling collector on the three things measurement decided, each of which produces
/// correct-looking output when it is wrong.
/// </summary>
public class PgWaitSamplingCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170000,
            },
        };

    private static string Sql => PgWaitSamplingCollector.Instance.BuildQuery(MakeContext()).Text;

    [Fact]
    public void Identity_IsTheTableAndEngineTheStoreExpects()
    {
        Assert.Equal("pg_wait_sampling", PgWaitSamplingCollector.Instance.Name);
        Assert.Equal("pg_wait_sampling", PgWaitSamplingCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgWaitSamplingCollector.Instance.TargetEngine);
    }

    /// <summary>
    /// <c>Activity</c> must never be stored. Measured on an idle PostgreSQL 17, the entire top of the raw
    /// profile was background processes waiting for work — <c>AutovacuumMain</c>, <c>LogicalLauncherMain</c>,
    /// <c>WalWriterMain</c>, <c>CheckpointerMain</c> — and they accumulate forever precisely BECAUSE the
    /// server is quiet. Rank that and every healthy server reports autovacuum's idle loop as its top wait.
    /// </summary>
    [Fact]
    public void IdleBackgroundWaits_AreExcludedAtTheSource()
    {
        Assert.Contains("event_type IS DISTINCT FROM 'Activity'", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>IS DISTINCT FROM</c>, never <c>&lt;&gt;</c>. A NULL event type means the backend was not waiting,
    /// and <c>NULL &lt;&gt; 'Activity'</c> is NULL rather than true — so a plain inequality would silently
    /// discard every on-CPU sample, which is real signal and is deliberately kept and labelled.
    /// </summary>
    [Fact]
    public void TheActivityFilter_IsNullSafe()
    {
        Assert.DoesNotMatch(new Regex(@"event_type\s*<>\s*'Activity'"), Sql);
        Assert.Contains("coalesce(p.event_type, 'CPU')", Sql, StringComparison.Ordinal);
        Assert.Contains("coalesce(p.event, 'Running')", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The profile period travels with the counts. A sample tally is uninterpretable without the period it
    /// was gathered at, and storing a derived millisecond figure would bake today's period into history.
    /// </summary>
    [Fact]
    public void TheProfilePeriod_IsCollectedAlongsideTheCounts()
    {
        Assert.Contains("pg_wait_sampling.profile_period", Sql, StringComparison.Ordinal);
        Assert.Contains("profile_period_ms", Sql, StringComparison.Ordinal);

        var columns = PgWaitSamplingCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.Contains("profile_period_ms", columns);
        Assert.Contains("sample_count", columns);

        /* The name must not promise a duration it is not. */
        Assert.DoesNotContain("wait_ms", columns);
        Assert.DoesNotContain("wait_time_ms", columns);
    }

    /// <summary>
    /// Read with the MISSING_OK form, like every other GUC this codebase reads: a renamed or absent setting
    /// must degrade one column rather than fail the whole collection.
    /// </summary>
    [Fact]
    public void TheGucRead_ToleratesTheSettingBeingAbsent()
    {
        foreach (Match call in Regex.Matches(Sql, @"current_setting\(([^)]*)\)"))
        {
            Assert.Contains(", true", call.Groups[1].Value, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Cluster-wide, so no <c>database_name</c> — the profile spans every backend and version 1.1 exposes no
    /// database column. Inventing that attribution is precisely the scope error #2599 removed from three
    /// other collectors.
    /// </summary>
    [Fact]
    public void ItDoesNotClaimPerDatabaseAttribution()
    {
        Assert.False(PgWaitSamplingCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));

        Assert.DoesNotContain(
            "database_name",
            PgWaitSamplingCollector.Instance.PayloadColumns.Select(c => c.Name));
    }

    /// <summary>
    /// <c>queryid</c> is a 64-bit hash — measured at nineteen digits on the live rig — so the column has to
    /// be a bigint, and unattributed waits are kept rather than filtered so the stored profile still agrees
    /// with the server's own totals.
    /// </summary>
    [Fact]
    public void QueryId_IsBigIntAndUnattributedWaitsSurvive()
    {
        var queryId = PgWaitSamplingCollector.Instance.PayloadColumns.Single(c => c.Name == "query_id");
        Assert.Equal(CollectorColumnType.BigInt, queryId.Type);

        Assert.DoesNotMatch(new Regex(@"queryid\s*(<>|!=)\s*0"), Sql);
    }
}
