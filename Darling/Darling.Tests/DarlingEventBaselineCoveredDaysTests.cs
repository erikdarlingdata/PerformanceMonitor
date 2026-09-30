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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731, source pins: the blocking and deadlock baselines count the hours collection covered, as Lite's do. A slot is
/// one local (date, hour). It is covered when the event's OWN collector logged a <c>SUCCESS</c> run in it, or when it
/// holds events; a bucket's mean is its events over its covered days, and <c>sample_count</c> = <c>distinct_days</c>
/// is that same number of days. The logged slots count only on a server whose event source holds at least one event in
/// the window: a successful run proves the collector ran, not that its source could see events. Two layers here: the
/// shape of each Darling arm, and the twin relation to Lite's
/// <c>BaselineProvider.EventBaselineSql</c>, so the two products cannot drift apart on the log source, the success
/// filter, the collector names or the covered-day divisor. The behaviour itself is proved live in
/// <see cref="DarlingEventBaselineCoveredDaysLiveTests"/> (and in Lite's <c>EventBaselineCoveredDaysTests</c> over DuckDB).
/// </summary>
public sealed class DarlingEventBaselineCoveredDaysTests
{
    /// <summary>The compiled arm text with its line endings normalized, so a multi-line anchor matches on any checkout.</summary>
    private static string ArmSql(string metric) =>
        PgBaselineProvider.GetBaselineQuery(metric)!.Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>One CTE's text: from its opening <c>name AS (</c> to the next CTE's (or the end of the text).</summary>
    private static string Cte(string sql, string name, string? next)
    {
        var start = sql.IndexOf(name + " AS (", StringComparison.Ordinal);
        Assert.True(start >= 0, $"the arm lost its {name} CTE:\n{sql}");
        var end = next is null ? sql.Length : sql.IndexOf(next + " AS (", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the arm's {name} CTE is not followed by {next}:\n{sql}");
        return sql[start..end];
    }

    /// <summary>
    /// Both arms: hours are counted from the collection log's <c>SUCCESS</c> runs of the event's own collector plus the
    /// slots that hold events, and the divisor is the number of DAYS those slots cover. The collector names come from
    /// the shared collectors' own <c>Name</c> and the success literal from the classification the collection-log
    /// writer uses, so neither can be renamed under the SQL. Every CTE extracts hour, dow and date from
    /// <see cref="PgBaselineProvider.LocalCollectionTime"/>, never from a bare <c>collection_time</c>.
    /// </summary>
    [Theory]
    [InlineData(MetricNames.Blocking, "blocked_process_reports", TimescaleSupport.BlockedProcessBaselineView)]
    [InlineData(MetricNames.Deadlock, "deadlocks", TimescaleSupport.DeadlockBaselineView)]
    public void TheArm_CountsTheHoursItsOwnCollectorCovered_AndDividesByCoveredDays(string metric, string eventTable, string aggregate)
    {
        var sql = ArmSql(metric);
        var collector = CollectorCatalog.All.Single(c => c.TargetTable == eventTable).Name;
        var success = EnumeratedCollectorDriver.ClassifyReturnedRun(abandoned: false);
        var local = PgBaselineProvider.LocalCollectionTime;

        var logged = Cte(sql, "logged", "events");
        var events = Cte(sql, "events", "slots");
        var slots = Cte(sql, "slots", null);

        /* The covered hours: this collector's own successful runs in the collection log, inside the window. */
        Assert.Equal("SUCCESS", success);
        Assert.Contains("FROM collection_log\n", logged, StringComparison.Ordinal);
        Assert.Contains($"AND   collector_name = '{collector}'\n", logged, StringComparison.Ordinal);
        Assert.Contains($"AND   status = '{success}'\n", logged, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3\n", logged, StringComparison.Ordinal);
        Assert.DoesNotContain(aggregate, logged, StringComparison.Ordinal);

        /* The events: the baseline aggregate, every collection, summed per slot. No collector or status filter here. */
        Assert.Contains($"FROM {aggregate}\n", events, StringComparison.Ordinal);
        Assert.Contains("SUM(event_count) AS n", events, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3\n", events, StringComparison.Ordinal);
        Assert.DoesNotContain("collector_name", events, StringComparison.Ordinal);
        Assert.DoesNotContain("status", events, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_log", events, StringComparison.Ordinal);

        /* A covered slot with no events arrives with a zero count, and only when the server's source holds at least one
           event in the window (a SUCCESS run proves the collector ran, not that its source could see events: a blocked
           process threshold of 0 logs SUCCESS every hour over an empty ring buffer); a slot with events arrives with
           its count. */
        Assert.Contains(
            "SELECT hh, dw, d, 0 AS n FROM logged\n    WHERE EXISTS (SELECT 1 FROM events)\n    UNION ALL\n    SELECT hh, dw, d, n FROM events",
            slots,
            StringComparison.Ordinal);

        /* The local clock, in EVERY CTE: hour, dow AND date, each from the shifted time and never a bare collection_time. */
        foreach (var cte in new[] { logged, events })
        {
            Assert.Contains($"EXTRACT(HOUR FROM {local})::INT AS hh", cte, StringComparison.Ordinal);
            Assert.Contains($"EXTRACT(DOW FROM {local})::INT AS dw", cte, StringComparison.Ordinal);
            Assert.Contains($"{local}::DATE AS d", cte, StringComparison.Ordinal);
            Assert.Equal(3, Regex.Matches(cte, Regex.Escape(local)).Count);
            Assert.DoesNotMatch(new Regex(@"EXTRACT\s*\(\s*(HOUR|DOW)\s+FROM\s+collection_time\s*\)", RegexOptions.IgnoreCase), cte);
            Assert.DoesNotMatch(new Regex(@"(?<![\w.])collection_time::DATE"), cte);
            Assert.Contains("GROUP BY hh, dw, d", cte, StringComparison.Ordinal);
        }

        /* The covered-day divisor: the distinct DATES of the slots (log rows and event rows together), for the mean and
           for both counts. The old divisor was the distinct dates of the EVENT rows alone, which is what made an hour of
           a quiet month a missing row and an hour with events on two of five covered days a mean over two. */
        Assert.Contains("SUM(n)::DOUBLE PRECISION / COUNT(DISTINCT d) AS mean_val", sql, StringComparison.Ordinal);
        Assert.Contains("0::DOUBLE PRECISION AS stddev_val", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT d) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT d) AS distinct_days", sql, StringComparison.Ordinal);
        Assert.Contains("FROM slots\nGROUP BY hh, dw", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GREATEST(", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"COUNT\s*\(\s*DISTINCT\s+" + Regex.Escape(local)), sql);

        /* The rest of the contract the reader and the census rely on: six columns, the four bound parameters. */
        Assert.Equal(6, Regex.Matches(sql, @"\bAS (hour_of_day|day_of_week|mean_val|stddev_val|sample_count|distinct_days)\b").Count);
        Assert.Equal(new[] { 1, 2, 3, 4, 5, 6 }, Regex.Matches(sql, @"\$(\d+)").Select(m => int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture)).Distinct().OrderBy(n => n).ToArray());
    }

    /// <summary>
    /// The Blocking and Deadlock arms are the same helper called with their own collector, log, event source and
    /// event-count expression: the text differs by exactly those four arguments and nothing else.
    /// </summary>
    [Fact]
    public void TheTwoArms_AreOneShape_DifferingOnlyInTheirFourArguments()
    {
        var blocking = ArmSql(MetricNames.Blocking);
        var deadlock = ArmSql(MetricNames.Deadlock);

        Assert.Equal(
            blocking.Replace("'blocked_process_report'", "'deadlocks'", StringComparison.Ordinal)
                    .Replace("FROM blocked_process_baseline", "FROM deadlock_baseline", StringComparison.Ordinal),
            deadlock);
    }

    /// <summary>The text of <c>EventBaselineSql</c> in a provider's source: its signature line through the close of its SQL.</summary>
    private static string EventBaselineBody(string source, string product)
    {
        var start = source.IndexOf("internal static string EventBaselineSql(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{product} lost EventBaselineSql");
        const string End = "GROUP BY hh, dw\";";
        var end = source.IndexOf(End, start, StringComparison.Ordinal);
        Assert.True(end > start, $"{product}'s EventBaselineSql lost its closing GROUP BY");
        return source[start..(end + End.Length)];
    }

    /// <summary>One provider's call of <c>EventBaselineSql</c> from a metric's arm: collector, log, event source, count.</summary>
    private static (string Collector, string Log, string Events, string Count) ArmCall(string source, string metric, string product)
    {
        var match = Regex.Match(
            source,
            @"MetricNames\." + metric + @" => EventBaselineSql\(""([^""]+)"", ""([^""]+)"", ""([^""]+)"", ""([^""]+)""\)");
        Assert.True(match.Success, $"{product}'s {metric} arm no longer calls EventBaselineSql with four literal arguments");
        return (match.Groups[1].Value, match.Groups[2].Value, match.Groups[3].Value, match.Groups[4].Value);
    }

    /// <summary>
    /// The twin relation with Lite's <c>BaselineProvider.EventBaselineSql</c>, source against source (Darling.Tests
    /// cannot reference the Lite assembly): the whole helper, signature and SQL, is BYTE-IDENTICAL in the two files, so
    /// the log filter, the success literal, the covered-day divisor and the local-clock extraction cannot drift in one
    /// product. What may differ is the four arguments, and those are pinned pairwise: the same collector name in both,
    /// each product's log source the twin of the other's (<c>v_collection_log</c> is Lite's view over the table),
    /// Lite's event source the view over the collector's own table and Darling's the collector's baseline aggregate.
    /// </summary>
    [Fact]
    public void TheTwoProducts_CarryTheSameEventBaselineBody_AndTheSameCollectors()
    {
        var lite = RepoFile.ReadRepoFileLf("Lite", "Analysis", "BaselineProvider.cs");
        var darling = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");

        var liteBody = EventBaselineBody(lite, "Lite");
        Assert.Equal(liteBody, EventBaselineBody(darling, "Darling"));

        /* The body is the covered-day shape (not two identical empty stubs): the pieces the arms above rely on. */
        Assert.Contains("collector_name = '\" + collector + @\"'", liteBody, StringComparison.Ordinal);
        Assert.Contains("AND   status = 'SUCCESS'", liteBody, StringComparison.Ordinal);
        Assert.Contains("SUM(n)::DOUBLE PRECISION / COUNT(DISTINCT d) AS mean_val", liteBody, StringComparison.Ordinal);
        Assert.Contains("SELECT hh, dw, d, 0 AS n FROM logged\n    WHERE EXISTS (SELECT 1 FROM events)\n    UNION ALL", liteBody, StringComparison.Ordinal);

        foreach (var (metric, eventTable, aggregate, liteView) in new[]
        {
            ("Blocking", "blocked_process_reports", TimescaleSupport.BlockedProcessBaselineView, "v_blocked_process_reports"),
            ("Deadlock", "deadlocks", TimescaleSupport.DeadlockBaselineView, "v_deadlocks"),
        })
        {
            var liteCall = ArmCall(lite, metric, "Lite");
            var darlingCall = ArmCall(darling, metric, "Darling");
            var collector = CollectorCatalog.All.Single(c => c.TargetTable == eventTable).Name;

            Assert.Equal(collector, liteCall.Collector);
            Assert.Equal(liteCall.Collector, darlingCall.Collector);
            Assert.Equal("v_" + darlingCall.Log, liteCall.Log);
            Assert.Equal("collection_log", darlingCall.Log);
            Assert.Equal(liteView, liteCall.Events);
            Assert.Equal(aggregate, darlingCall.Events);
            Assert.Equal("COUNT(*)", liteCall.Count);
            Assert.Equal("SUM(event_count)", darlingCall.Count);
        }
    }
}

/// <summary>
/// #4731, the live proof for <see cref="DarlingEventBaselineCoveredDaysTests"/>, in the <c>live-postgres</c>
/// collection so the shared store is established before it runs and the residue check runs after it (#1862, #1873).
/// Both families, the real provider over the real baseline supply (the plain fallback views when the store has no
/// continuous aggregate), seeded with the collection log's own rows.
///
/// <para>The window is [Feb 2 14:00, Mar 4 14:00) for the analysis time Wed Mar 4 14:00 (Feb 2 is a Monday) and the
/// server has no clock row, so buckets key on UTC. The bucket under test is Tuesday 14:00: Feb 3, 10, 17, 24 and Mar 3
/// make five covered days.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingEventBaselineCoveredDaysLiveTests
{
    private const int Tuesday = (int)DayOfWeek.Tuesday;
    private const int Hour = 14;
    private const string ServerName = "event-baseline-covered-days";

    /// <summary>Well below any id the generator hands out, so a seeded log row can never collide with a real one.</summary>
    private const long LogIdBase = -4731_000_000_000L;

    private static readonly DateTime AnalysisTime = new(2026, 3, 4, 14, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime[] Tuesdays =
    [
        new(2026, 2, 3, 14, 0, 0, DateTimeKind.Unspecified), new(2026, 2, 10, 14, 0, 0, DateTimeKind.Unspecified),
        new(2026, 2, 17, 14, 0, 0, DateTimeKind.Unspecified), new(2026, 2, 24, 14, 0, 0, DateTimeKind.Unspecified),
        new(2026, 3, 3, 14, 0, 0, DateTimeKind.Unspecified),
    ];

    /// <summary>The one event that lets a quiet server's covered hours count (#4731): inside the window, on Sunday
    /// Feb 8 at 09:10, which is neither hour 14 nor the bucket under test (Tuesday 14:00).</summary>
    private static readonly DateTime ElsewhereEvent = new(2026, 2, 8, 9, 10, 0, DateTimeKind.Unspecified);

    private static (string Metric, string Collector, int ServerId) Family(string name) => name == "blocking"
        ? (MetricNames.Blocking, "blocked_process_report", -4731_21)
        : (MetricNames.Deadlock, "deadlocks", -4731_22);

    /// <summary>
    /// (a) A quiet hour on a server whose source has captured events: five weeks of <c>SUCCESS</c> runs every 15
    /// minutes and one event, on a Sunday at 09:10 (neither hour 14 nor Tuesday). Before #4731 the arm grouped the
    /// event rows that exist, so it returned NO row for any other hour; now every hour of the week is a row and the
    /// Tuesday 14:00 bucket is a measured zero over the five days that covered it.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task Live_AQuietHour_OnAServerThatHasCapturedEvents_IsARowWithMeanZeroOverFiveCoveredDays_NotAMissingRow(string family)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live event-baseline coverage test.");

        var ct = TestContext.Current.CancellationToken;
        var (metric, collector, serverId) = Family(family);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedQuietMonthAsync(connection, serverId, collector, ct);
            await SeedEventAsync(connection, family, serverId, ElsewhereEvent, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var map = await new PgBaselineProvider(postgres).GetBucketMapAsync(serverId, metric, AnalysisTime, AnalysisTime.AddHours(1), ct);

            Assert.Equal(24 * 7, map.Buckets.Count);
            var tuesday = map.Buckets[(Hour, Tuesday)];
            Assert.Equal(0.0, tuesday.Mean);
            Assert.Equal(0.0, tuesday.StdDev);
            Assert.Equal(5L, tuesday.SampleCount);
            Assert.Equal(5L, tuesday.DistinctDays);
            Assert.Equal(4L, map.Buckets[(Hour, (int)DayOfWeek.Wednesday)].SampleCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => TearDownAsync(cleanup, serverId, cleanupCt));
        }
    }

    /// <summary>
    /// (a2) A server whose collector logged <c>SUCCESS</c> every hour for five weeks and whose source holds no event:
    /// a threshold of 0 (RDS, Azure, a login that cannot set it) reads an empty ring buffer and succeeds. A successful
    /// run proves the collector ran, not that its source could see events, so the arm returns no bucket at all, as it
    /// did before covered days, and not a trusted mean of zero.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task Live_ACollectorThatLoggedSuccessEveryHour_ForAServerWhoseSourceHoldsNoEvent_HasNoBaseline(string family)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live event-baseline coverage test.");

        var ct = TestContext.Current.CancellationToken;
        var (metric, collector, serverId) = Family(family);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedQuietMonthAsync(connection, serverId, collector, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var map = await new PgBaselineProvider(postgres).GetBucketMapAsync(serverId, metric, AnalysisTime, AnalysisTime.AddHours(1), ct);

            Assert.Empty(map.Buckets);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => TearDownAsync(cleanup, serverId, cleanupCt));
        }
    }

    /// <summary>
    /// (c) Events on two of the five covered days: the mean is the events over FIVE days, not over the two that had
    /// events, and the day counts are five. Six events over five days is 1.2 a day; the old divisor gave 3.
    /// </summary>
    [Theory]
    [InlineData("blocking")]
    [InlineData("deadlock")]
    public async Task Live_EventsOnTwoOfFiveCoveredDays_DivideByFive_NotByTwo(string family)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live event-baseline coverage test.");

        var ct = TestContext.Current.CancellationToken;
        var (metric, collector, serverId) = Family(family);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedQuietMonthAsync(connection, serverId, collector, ct);
            foreach (var day in new[] { Tuesdays[0], Tuesdays[3] })
            {
                for (var i = 0; i < 3; i++)
                {
                    await SeedEventAsync(connection, family, serverId, day.AddMinutes(10 + i), ct);
                }
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var map = await new PgBaselineProvider(postgres).GetBucketMapAsync(serverId, metric, AnalysisTime, AnalysisTime.AddHours(1), ct);

            var tuesday = map.Buckets[(Hour, Tuesday)];
            Assert.Equal(6.0 / 5.0, tuesday.Mean, 9);
            Assert.Equal(5L, tuesday.SampleCount);
            Assert.Equal(5L, tuesday.DistinctDays);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => TearDownAsync(cleanup, serverId, cleanupCt));
        }
    }

    /// <summary>Five weeks of <c>SUCCESS</c> runs every 15 minutes, ending just before the analysis hour: every slot of the window is covered.</summary>
    private static Task SeedQuietMonthAsync(NpgsqlConnection connection, int serverId, string collector, CancellationToken ct) =>
        ExecAsync(connection, ct,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
              SELECT $1 - row_number() OVER (), $2, $3, $4, t, 'SUCCESS', 0
              FROM generate_series($5::timestamp, $6::timestamp, INTERVAL '15 minutes') AS g(t)",
            LogIdBase + serverId * 1_000_000L, serverId, ServerName, collector, AnalysisTime.AddDays(-35), AnalysisTime.AddMinutes(-15));

    private static Task SeedEventAsync(NpgsqlConnection connection, string family, int serverId, DateTime at, CancellationToken ct) =>
        family == "blocking"
            ? ExecAsync(connection, ct,
                "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
                CollectionIdGenerator.Next(), at, serverId, ServerName, at)
            : ExecAsync(connection, ct,
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ($1, $2, $3, $4, $5)",
                CollectionIdGenerator.Next(), at, serverId, ServerName, at);

    /// <summary>The teardown, run on the cleanup's own connection: the seeded rows, then the plain fallback views this test made.</summary>
    private static async Task TearDownAsync(NpgsqlConnection cleanup, int serverId, CancellationToken ct)
    {
        await CleanupAsync(cleanup, serverId, ct);
        foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
        {
            using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), cleanup);
            await drop.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {serverId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {serverId}; " +
            $"DELETE FROM deadlocks WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
