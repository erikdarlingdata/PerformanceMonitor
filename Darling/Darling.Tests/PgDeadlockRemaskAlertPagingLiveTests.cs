/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5625: the two <c>config_alert_log</c> stages of the hourly re-mask (deadlock alerts, finding alerts) walk the log
/// one server (and, for finding alerts, one metric) at a time through <c>idx_config_alert_log_time (server_id,
/// metric_name, alert_time)</c>, so a page reads a range bounded by its size whatever the log holds. They used to
/// filter on the metric alone, which that index cannot serve: every page read every deadlock alert (a Bitmap Heap Scan
/// and a Sort) or the whole log (a Seq Scan), which timed out on a large store. Seeded server-side at about 500,000
/// alert rows over 50 servers; no assertion reads the clock. The plans hold on PostgreSQL 16, 17 and 18 because each
/// read is an equality on the index's leading columns and a range on its last (and the steps between servers and
/// metrics are a leading-column descent and a row comparison on the first two columns): nothing here relies on PG 18's
/// skip scan.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], like PgDeadlockRemaskTests. The test reaches
   DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it: the
   walks read the whole log, so they must not meet another class's alerts. Leave it out; this comment is here so the
   next sweep does not "fix" it. */
public sealed class PgDeadlockRemaskAlertPagingLiveTests
{
    private const int Servers = 50;
    private const int FillerAlerts = 500_000;
    private const int RawDeadlockAlerts = 1_600;
    private const int CurrentDeadlockAlerts = 2_500;
    private const int FindingDecoys = 40_000;
    private static readonly string[] s_rawDeadlockServers = ["2", "9", "30", "49"];

    private static readonly PgLogHashKey s_key = new(Enumerable.Range(1, PgLogHashKey.KeyLength).Select(i => (byte)i).ToArray());

    /// <summary>The deadlock walk: each statement is a range of the index bounded by the page, a page with nothing to
    /// read ends on the next server, and the walk reaches and rewrites every raw alert once, ties included.</summary>
    [Fact]
    public async Task DeadlockAlerts_ArePagedPerServerThroughTheIndex_AndTheWalkRewritesEveryRowOnce()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        /* A server's slice: an index range, one search, no row filtered after its read, and a fixed few buffers
           whatever the log holds (the old page read every deadlock row of every server). */
        var slice = await ExplainAsync(connection, PgDeadlockRemask.AlertPageSql,
        [
            Param(9, NpgsqlDbType.Integer), Param(null, NpgsqlDbType.Timestamp), Param((long)PgDeadlockRemask.MaxAlertRowsPerPass, NpgsqlDbType.Bigint),
        ], ct);
        AssertIndexRange(slice, "the deadlock slice");
        AssertEquality(slice, "the deadlock slice", "server_id", "metric_name");
        Assert.True(
            slice.RowsRead <= PgDeadlockRemask.MaxAlertRowsPerPass + 20,
            $"a slice read {slice.RowsRead} rows from the log; its page is {PgDeadlockRemask.MaxAlertRowsPerPass} (the old page read every deadlock alert: {RawDeadlockAlerts + CurrentDeadlockAlerts})");
        Assert.True(
            slice.Buffers <= 2L * PgDeadlockRemask.MaxAlertRowsPerPass + 200,
            $"a slice read {slice.Buffers} buffers; its page is {PgDeadlockRemask.MaxAlertRowsPerPass} rows");

        /* A server with no alert at all: nothing to read, and the same few buffers. */
        var none = await ExplainAsync(connection, PgDeadlockRemask.AlertPageSql,
        [
            Param(Servers + 1, NpgsqlDbType.Integer), Param(null, NpgsqlDbType.Timestamp), Param((long)PgDeadlockRemask.MaxAlertRowsPerPass, NpgsqlDbType.Bigint),
        ], ct);
        AssertIndexRange(none, "the empty deadlock slice");
        Assert.True(none.Buffers <= 40, $"an empty slice read {none.Buffers} buffers");

        /* The step to the next server is one descent of the index's leading column. */
        var probe = await ExplainAsync(connection, PgDeadlockRemask.AlertServerProbeSql, [Param(6L, NpgsqlDbType.Bigint)], ct);
        AssertIndexRange(probe, "the server probe");
        Assert.True(probe.Buffers <= 40, $"a server probe read {probe.Buffers} buffers");

        /* The walk. Every page moves the cursor; a page examines at most its size plus the rows tied with its last. */
        PgDeadlockRemask.AlertLogCursor? cursor = null;
        int pages = 0, examined = 0, rewritten = 0, raced = 0;
        do
        {
            var (next, e, w, r) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, cursor, s_key, true, null, ct);
            Assert.True(next is null || next != cursor, "the cursor must move on every page");
            Assert.True(e <= PgDeadlockRemask.MaxAlertRowsPerPass + 20, $"a page examined {e} rows");
            pages++;
            examined += e;
            rewritten += w;
            raced += r;
            cursor = next;
        }
        while (cursor is not null);

        Assert.Equal(RawDeadlockAlerts + CurrentDeadlockAlerts, examined);
        Assert.Equal(RawDeadlockAlerts, rewritten);
        Assert.Equal(0, raced);
        Assert.InRange(pages, (RawDeadlockAlerts + CurrentDeadlockAlerts) / PgDeadlockRemask.MaxAlertRowsPerPass, (RawDeadlockAlerts + CurrentDeadlockAlerts) / PgDeadlockRemask.MaxAlertRowsPerPass + Servers);
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config_alert_log WHERE context_json LIKE '%Leak5625%'", ct));

        /* A second walk finds nothing left to rewrite. */
        cursor = null;
        var again = 0;
        do
        {
            var (next, _, w, _) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, cursor, s_key, false, null, ct);
            again += w;
            cursor = next;
        }
        while (cursor is not null);

        Assert.Equal(0, again);
    }

    /// <summary>The finding-alert walk: each statement is a range of the index for one server and metric, found by a
    /// loose scan of the metric names, so no range on <c>metric_name</c> (and no collation question) is needed; the
    /// cursor moves on a page that rewrites nothing; older-than-the-section rows are never read; and every raw alert
    /// is rewritten once, ties included.</summary>
    [Fact]
    public async Task FindingAlerts_ArePagedPerServerAndMetricThroughTheIndex_AndTheWalkRewritesEveryRowOnce()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var (metric, rawContext) = PgDeadlockRemaskTests.LegacyFindingAlert(drillDown, story, "5625abcd00000000");
        var (olderMetric, olderContext) = PgDeadlockRemaskTests.LegacyFindingAlert(drillDown, story, "5625e0e000000000");
        var plantedAt = new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Unspecified);
        var olderAt = new DateTime(2026, 9, 5, 12, 0, 0, DateTimeKind.Unspecified);
        var planted = 0;
        foreach (var (server, offset) in new[] { (1, 0), (17, 0), (17, 0), (17, 60), (33, 0), (50, 0), (50, 120) })
        {
            await PlantAsync(connection, server, metric, plantedAt.AddSeconds(offset), rawContext, ct);
            planted++;
        }

        await PlantAsync(connection, 17, olderMetric, olderAt, olderContext, ct);
        await PlantAsync(connection, 17, metric, olderAt, rawContext, ct);
        await ExecuteAsync(connection, "ANALYZE config_alert_log", ct);

        /* A (server, metric) slice: one search of the index, the time bound an index condition (an older row is never
           read), no row filtered after its read, and a few buffers. */
        var decoyMetric = await TextAsync(connection, "SELECT metric_name FROM config_alert_log WHERE server_id = 9 AND metric_name LIKE 'Analysis: decoy%' ORDER BY metric_name LIMIT 1", ct);
        var slice = await ExplainAsync(connection, PgDeadlockRemask.FindingAlertPageSql,
        [
            Param(9, NpgsqlDbType.Integer), Param(decoyMetric, NpgsqlDbType.Text), Param(null, NpgsqlDbType.Timestamp), Param((long)PgDeadlockRemask.MaxFindingAlertRowsPerPass, NpgsqlDbType.Bigint),
        ], ct);
        AssertIndexRange(slice, "the finding alert slice");
        AssertEquality(slice, "the finding alert slice", "server_id", "metric_name");
        Assert.Contains(slice.IndexConditions, condition => condition.Contains("alert_time >=", StringComparison.Ordinal));
        Assert.True(slice.RowsRead <= PgDeadlockRemask.MaxFindingAlertRowsPerPass + 20, $"a slice read {slice.RowsRead} rows from the log; its page is {PgDeadlockRemask.MaxFindingAlertRowsPerPass}");
        Assert.True(slice.Buffers <= 2L * PgDeadlockRemask.MaxFindingAlertRowsPerPass + 200, $"a slice read {slice.Buffers} buffers");

        /* The loose scan between metrics: a row comparison on the index's first two columns. */
        var step = await ExplainAsync(connection, PgDeadlockRemask.AlertPairNextSql,
            [Param(9, NpgsqlDbType.Integer), Param(decoyMetric, NpgsqlDbType.Text)], ct);
        AssertIndexRange(step, "the metric step");
        Assert.True(step.Buffers <= 40, $"a metric step read {step.Buffers} buffers");
        var first = await ExplainAsync(connection, PgDeadlockRemask.AlertPairFirstSql, [], ct);
        AssertIndexRange(first, "the first metric");
        Assert.True(first.Buffers <= 40, $"the first metric read {first.Buffers} buffers");

        /* A page that examines decoys needing nothing (server 5 has no planted alert): the cursor moves anyway. */
        var start = new PgDeadlockRemask.AlertLogCursor(5, "Analysis: ", null);
        var (afterFirst, firstExamined, firstRewritten, _) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, start, null, ct);
        Assert.NotNull(afterFirst);
        Assert.NotEqual(start, afterFirst);
        Assert.InRange(firstExamined, PgDeadlockRemask.MaxFindingAlertRowsPerPass, PgDeadlockRemask.MaxFindingAlertRowsPerPass + 20);
        Assert.Equal(0, firstRewritten);

        /* The whole walk. */
        PgDeadlockRemask.AlertLogCursor? cursor = null;
        int pages = 0, examined = 0, rewritten = 0, raced = 0;
        do
        {
            var (next, e, w, r) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, cursor, null, ct);
            Assert.True(next is null || next != cursor, "the cursor must move on every page");
            Assert.True(e <= PgDeadlockRemask.MaxFindingAlertRowsPerPass + 20, $"a page examined {e} alerts");
            pages++;
            examined += e;
            rewritten += w;
            raced += r;
            cursor = next;
        }
        while (cursor is not null);

        Assert.Equal(planted, rewritten);
        Assert.Equal(0, raced);
        Assert.Equal(
            await ScalarAsync(connection, "SELECT count(*) FROM config_alert_log WHERE metric_name LIKE 'Analysis: %' AND alert_time >= '2026-09-18'", ct),
            examined);
        Assert.InRange(pages, examined / (PgDeadlockRemask.MaxFindingAlertRowsPerPass + 20), examined / PgDeadlockRemask.MaxFindingAlertRowsPerPass + 2);

        /* Every planted row changed once; the two older than the section are still as they were (never read). */
        Assert.Equal(
            (long)planted,
            await CountAsync(connection, "SELECT count(*) FROM config_alert_log WHERE metric_name = $1 AND alert_time >= '2026-09-18' AND context_json <> $2", metric, rawContext, ct));
        Assert.Equal(
            1L,
            await CountAsync(connection, "SELECT count(*) FROM config_alert_log WHERE metric_name = $1 AND alert_time < '2026-09-18' AND context_json = $2", metric, rawContext, ct));
        Assert.Equal(
            1L,
            await CountAsync(connection, "SELECT count(*) FROM config_alert_log WHERE metric_name = $1 AND alert_time < '2026-09-18' AND context_json = $2", olderMetric, olderContext, ct));

        /* A second walk rewrites nothing. */
        cursor = null;
        var again = 0;
        do
        {
            var (next, _, w, _) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, cursor, null, ct);
            again += w;
            cursor = next;
        }
        while (cursor is not null);

        Assert.Equal(0, again);
    }

    /// <summary>An empty log ends both walks at once, and a cursor past the log's last server does too.</summary>
    [Fact]
    public async Task AnEmptyLog_AndACursorPastTheEnd_EndBothWalks()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal(((PgDeadlockRemask.AlertLogCursor?)null, 0, 0, 0), await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, null, s_key, true, null, ct));
        Assert.Equal(((PgDeadlockRemask.AlertLogCursor?)null, 0, 0, 0), await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, null, null, ct));

        await PlantAsync(connection, 3, AlertEngine.DeadlockWatermarkMetric, new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Unspecified), "{}", ct);
        var pastTheEnd = new PgDeadlockRemask.AlertLogCursor(int.MaxValue, AlertEngine.DeadlockWatermarkMetric, new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Unspecified));
        Assert.Equal(((PgDeadlockRemask.AlertLogCursor?)null, 0, 0, 0), await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, pastTheEnd, s_key, true, null, ct));
        Assert.Equal(((PgDeadlockRemask.AlertLogCursor?)null, 0, 0, 0), await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, pastTheEnd, null, ct));
    }

    /// <summary>The slice also looks up the stored reports its incidents name (the <c>reports</c> CTE, by server and
    /// hash). With 100,000 stored reports over 50 servers and a slice of 450 alerts whose keys all match stored
    /// reports, that lookup goes through <c>idx_pg_deadlocks_identity (server_id, deadlock_hash)</c>, never a scan of the
    /// table, and reads only the keys' own rows. No migration: the index is V104's.</summary>
    [Fact]
    public async Task TheReportLookupOfASlice_UsesTheIdentityIndex_AndReadsOnlyTheSlicesKeys()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string dummyKey = "00000000000000000000000000000000";
        var template = AlertContextSerializer.Serialize(new AlertContext { Incidents = [new AlertIncident(dummyKey, ["UPDATE t SET c = 1"])] });
        Assert.Contains(dummyKey, template, StringComparison.Ordinal);

        /* 100,000 stored reports over 50 servers, each its own hash. */
        await ExecuteAsync(connection, @"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, graph_text)
SELECT g, timestamp '2026-10-01' + g * interval '20 seconds', 1 + g % 50, 'example-pg-' || (1 + g % 50),
       timestamp '2026-10-01' + g * interval '20 seconds' - interval '5 seconds', 1000 + g % 5000, 2,
       upper(md5(g::text)), 'Process 1 waits for ShareLock on transaction ' || g
FROM generate_series(1, 100000) AS g", ct);

        /* 450 deadlock alerts on server 9, each naming the hash of one of that server's stored reports (g = 8 + 50 i). */
        await using (var alerts = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
SELECT timestamp '2026-10-04' + i * interval '1 second', 9, 'example-pg-9', $2, 1, 1, true, 'webhook', false,
       replace($1, $3, upper(md5((8 + 50 * i)::text)))
FROM generate_series(1, 450) AS i", connection))
        {
            alerts.Parameters.Add(Param(template, NpgsqlDbType.Text));
            alerts.Parameters.Add(Param(AlertEngine.DeadlockWatermarkMetric, NpgsqlDbType.Text));
            alerts.Parameters.Add(Param(dummyKey, NpgsqlDbType.Text));
            await alerts.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE pg_deadlocks", ct);
        await ExecuteAsync(connection, "ANALYZE config_alert_log", ct);

        var plan = await ExplainScansAsync(connection, PgDeadlockRemask.AlertPageSql,
        [
            Param(9, NpgsqlDbType.Integer), Param(null, NpgsqlDbType.Timestamp), Param((long)PgDeadlockRemask.MaxAlertRowsPerPass, NpgsqlDbType.Bigint),
        ], ct);

        var reports = plan.Where(scan => !scan.Relation.Equals("a", StringComparison.Ordinal)).ToList();
        var summary = string.Join("; ", plan.Select(scan => $"{scan.Relation}/{scan.NodeType}/{scan.IndexName}/{scan.RowsRead}"));
        Assert.True(reports.Count > 0, $"no scan of the stored reports in the plan: {summary}");
        Assert.All(reports, scan => Assert.True(
            scan.NodeType != "Seq Scan" && scan.IndexName == "idx_pg_deadlocks_identity",
            $"the report lookup scanned {scan.Relation} with {scan.NodeType} {scan.IndexName}: {summary}"));
        var read = reports.Sum(scan => scan.RowsRead);
        Assert.True(read <= 450 + 50, $"the report lookup read {read} stored reports for 450 keys (the table holds 100,000): {summary}");
        Assert.True(read >= 450, $"the report lookup read {read} stored reports; each of the 450 keys has one: {summary}");

        /* And the slice's reports come back: a walk over it finds each alert's report. */
        var (_, examined, _, raced) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, null, s_key, true, null, ct);
        Assert.Equal(450, examined);
        Assert.Equal(0, raced);
    }

    /* ───────────────────────── seeding ───────────────────────── */

    /// <summary>About 500,000 alert rows over 50 servers in one INSERT ... SELECT each: mostly other metrics; 40,000
    /// <c>Analysis: </c> alerts that need nothing (97 metric names on every server, a tenth of the rows older than the
    /// finding-alert section); current deadlock alerts; and raw deadlock alerts on four servers, twelve at a time tied
    /// on one instant per server.</summary>
    /// <summary>The step caps, server ids below one, the cancel cursors and a server with thousands of alerts, one
    /// scenario after another in one database (each empties the log first): they are small, and a database for each
    /// would cost the class more than the checks do.</summary>
    [Fact]
    public async Task TheGuardsOfTheTwoAlertStages_HoldOnTheirOwnData()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert paging test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await FindingAlerts_TheStepCap_CountsEveryProbe_AndAnOrdinaryMetricCursorReadsNoRows(connection, ct);
        await TheStepCap_EndsADeadlockPage_AndServerIdsBelowOneAreWalkedByBothStages(connection, ct);
        await ACancelMidPage_AcrossTwoServersOrTwoMetrics_LosesNoRowAndRepeatsNone(connection, ct);
        await APageOnAServerWithThousandsOfAlerts_StopsAtThePageSize(connection, ct);
    }

    /// <summary>The finding-alert stage counts every probe against <see cref="PgDeadlockRemask.MaxAlertStepsPerPage"/>,
    /// whatever a metric is called (#5625). 150 servers hold four ordinary metrics each (600 pairs, one alert apiece) and
    /// the only <c>Analysis: </c> alerts are two at the last server: a page that counted only the steps onto an
    /// Analysis metric would run all 600 probes in one statement chain. Each page here ends at its cap with a cursor
    /// that moved, the cursor may rest on an ordinary metric (whose rows are not read: its name says it is no
    /// Analysis one), and the walk still reaches the two alerts and ends.</summary>
    private static async Task FindingAlerts_TheStepCap_CountsEveryProbe_AndAnOrdinaryMetricCursorReadsNoRows(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection, "TRUNCATE config_alert_log", ct);

        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var (metric, rawContext) = PgDeadlockRemaskTests.LegacyFindingAlert(drillDown, story, "5625abcd00000001");
        await ExecuteAsync(connection, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
SELECT timestamp '2026-10-05' + g * interval '1 second', 1 + g % 150, 'example-pg-' || (1 + g % 150),
       (ARRAY['Blocking', 'High CPU', 'Low Memory', 'Poor Wait Stats'])[1 + (g / 150) % 4], 1, 1, true, 'webhook', false, '{""Details"":[]}'
FROM generate_series(0, 599) AS g", ct);
        await PlantAsync(connection, 150, metric, new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Unspecified), rawContext, ct);
        await PlantAsync(connection, 150, metric, new DateTime(2026, 10, 6, 12, 1, 0, DateTimeKind.Unspecified), rawContext, ct);
        await ExecuteAsync(connection, "ANALYZE config_alert_log", ct);

        PgDeadlockRemask.AlertLogCursor? cursor = null;
        int pages = 0, examined = 0, rewritten = 0;
        var cursorsOnOrdinaryMetrics = 0;
        do
        {
            var (next, e, w, _) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, cursor, null, ct);
            Assert.True(next is null || next != cursor, "the cursor must move on every page");
            pages++;
            examined += e;
            rewritten += w;
            cursorsOnOrdinaryMetrics += next is { } at && !at.MetricName.StartsWith("Analysis: ", StringComparison.Ordinal) ? 1 : 0;
            cursor = next;
            Assert.True(pages < 50, "the walk must end");
        }
        while (cursor is not null);

        /* 600 ordinary pairs at 100 probes a page. */
        Assert.InRange(pages, 6, 8);
        Assert.True(cursorsOnOrdinaryMetrics > 0, "a page that ends on an ordinary metric hands the next page a cursor on it");
        Assert.Equal(2, examined);
        Assert.Equal(2, rewritten);
    }

    /// <summary>The deadlock stage's step cap ends a page after <see cref="PgDeadlockRemask.MaxAlertStepsPerPage"/>
    /// servers with nothing to read (200 servers between two that hold deadlock alerts), and a server id of -1 or 0 is
    /// walked like any other: the walk starts below every id, not at 0 (#5625). The finding-alert stage meets the same
    /// ids.</summary>
    private static async Task TheStepCap_EndsADeadlockPage_AndServerIdsBelowOneAreWalkedByBothStages(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection, "TRUNCATE config_alert_log", ct);

        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var (metric, rawContext) = PgDeadlockRemaskTests.LegacyFindingAlert(drillDown, story, "5625abcd00000002");
        var at = new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Unspecified);
        var gone = GoneContext();

        /* Servers -1 and 0 through 250 hold one ordinary alert each; raw deadlock alerts sit at -1, 0, 1 and 201, so
           200 servers lie between the last two. */
        await ExecuteAsync(connection, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted)
SELECT timestamp '2026-10-05' + g * interval '1 second', g, 'example-pg-' || g, 'High CPU', 1, 1, true, 'webhook', false
FROM generate_series(-1, 250) AS g", ct);
        foreach (var server in new[] { -1, 0, 1, 201 })
        {
            await PlantAsync(connection, server, AlertEngine.DeadlockWatermarkMetric, at, gone, ct);
        }

        await PlantAsync(connection, -1, metric, at, rawContext, ct);
        await ExecuteAsync(connection, "ANALYZE config_alert_log", ct);

        PgDeadlockRemask.AlertLogCursor? cursor = null;
        int pages = 0, rewritten = 0;
        do
        {
            var (next, _, w, _) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, cursor, s_key, true, null, ct);
            Assert.True(next is null || next != cursor, "the cursor must move on every page");
            pages++;
            rewritten += w;
            cursor = next;
            Assert.True(pages < 50, "the walk must end");
        }
        while (cursor is not null);

        /* 252 servers at 100 steps a page. */
        Assert.InRange(pages, 3, 4);
        Assert.Equal(4, rewritten);
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config_alert_log WHERE context_json LIKE '%Leak5625%'", ct));

        /* The finding-alert stage: its one alert is at server -1. */
        cursor = null;
        var findingRewritten = 0;
        pages = 0;
        do
        {
            var (next, _, w, _) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, cursor, null, ct);
            Assert.True(next is null || next != cursor, "the cursor must move on every page");
            findingRewritten += w;
            cursor = next;
            Assert.True(++pages < 50, "the walk must end");
        }
        while (cursor is not null);

        Assert.Equal(1, findingRewritten);
        Assert.Equal(0L, await CountAsync(connection, "SELECT count(*) FROM config_alert_log WHERE metric_name = $1 AND context_json = $2", metric, rawContext, ct));
    }

    /// <summary>A cancel in the middle of a page leaves the cursor at the last instant finished, and that instant is
    /// told from the next row's by its server (deadlock alerts) or its server and metric (finding alerts), not by its
    /// time alone (#5625): rows of two servers, or two metrics, can carry the same <c>alert_time</c>. The row after the
    /// cancel is read again by the resumed page, and no row before it is.</summary>
    private static async Task ACancelMidPage_AcrossTwoServersOrTwoMetrics_LosesNoRowAndRepeatsNone(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection, "TRUNCATE config_alert_log", ct);

        var at = new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Unspecified);

        /* Deadlock alerts: server 1 at t, server 2 at t and at t + 1 s (the same instant on two servers). */
        var gone = GoneContext();
        await PlantAsync(connection, 1, AlertEngine.DeadlockWatermarkMetric, at, gone, ct);
        await PlantAsync(connection, 2, AlertEngine.DeadlockWatermarkMetric, at, gone, ct);
        await PlantAsync(connection, 2, AlertEngine.DeadlockWatermarkMetric, at.AddSeconds(1), gone, ct);

        using (var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            PgDeadlockRemask.AlertLogCursor? last = null;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, null, s_key, true, (row, _) =>
                {
                    last = row ?? last;
                    cancel.Cancel();
                }, cancel.Token));

            Assert.Equal(new PgDeadlockRemask.AlertLogCursor(1, AlertEngine.DeadlockWatermarkMetric, at), last);
            var (next, examined, rewritten, _) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, last, s_key, true, null, ct);
            Assert.Null(next);
            Assert.Equal(2, examined);
            Assert.Equal(2, rewritten);
        }

        Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config_alert_log WHERE context_json LIKE '%Leak5625%'", ct));

        /* Finding alerts: server 1, one metric at t, another at t and at t + 1 s. */
        var (drillDown, story) = PgDeadlockRemaskTests.LegacyFinding();
        var (metric, rawContext) = PgDeadlockRemaskTests.LegacyFindingAlert(drillDown, story, "5625abcd00000003");
        var otherMetric = metric + "~";
        await PlantAsync(connection, 1, metric, at, rawContext, ct);
        await PlantAsync(connection, 1, otherMetric, at, rawContext, ct);
        await PlantAsync(connection, 1, otherMetric, at.AddSeconds(1), rawContext, ct);

        using (var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            PgDeadlockRemask.AlertLogCursor? last = null;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, null, (row, _) =>
                {
                    last = row ?? last;
                    cancel.Cancel();
                }, cancel.Token));

            Assert.Equal(new PgDeadlockRemask.AlertLogCursor(1, metric, at), last);
            var (next, examined, rewritten, _) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, last, null, ct);
            Assert.Null(next);
            Assert.Equal(2, examined);
            Assert.Equal(2, rewritten);
        }

        Assert.Equal(0L, await CountAsync(connection, "SELECT count(*) FROM config_alert_log WHERE metric_name LIKE $1 AND context_json = $2", metric + "%", rawContext, ct));
    }

    /// <summary>A server (and metric) with thousands of alerts: a page reads the page size and the rows tied with its
    /// last, an ordered range of the index that stops there, not the server's whole set (#5625). Both stages, and the
    /// plans, on 6,000 alerts of one server.</summary>
    private static async Task APageOnAServerWithThousandsOfAlerts_StopsAtThePageSize(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection, "TRUNCATE config_alert_log", ct);

        const int Alerts = 6_000;
        await using (var seed = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
SELECT timestamp '2026-10-05' + g * interval '1 second', 7, 'example-pg-7', m.name, 1, 1, true, 'webhook', false, '{""Incidents"":[]}'
FROM generate_series(1, $1) AS g
CROSS JOIN (VALUES ($2), ('Analysis: big [00000001]')) AS m(name)", connection))
        {
            seed.Parameters.Add(Param(Alerts, NpgsqlDbType.Integer));
            seed.Parameters.Add(Param(AlertEngine.DeadlockWatermarkMetric, NpgsqlDbType.Text));
            await seed.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE config_alert_log", ct);

        var deadlock = await ExplainAsync(connection, PgDeadlockRemask.AlertPageSql,
        [
            Param(7, NpgsqlDbType.Integer), Param(null, NpgsqlDbType.Timestamp), Param((long)PgDeadlockRemask.MaxAlertRowsPerPass, NpgsqlDbType.Bigint),
        ], ct);
        AssertIndexRange(deadlock, "the deadlock slice of a server with many alerts");
        Assert.True(
            deadlock.RowsRead <= PgDeadlockRemask.MaxAlertRowsPerPass + 20,
            $"a slice read {deadlock.RowsRead} rows from the log; its page is {PgDeadlockRemask.MaxAlertRowsPerPass} of the server's {Alerts}");

        var findings = await ExplainAsync(connection, PgDeadlockRemask.FindingAlertPageSql,
        [
            Param(7, NpgsqlDbType.Integer), Param("Analysis: big [00000001]", NpgsqlDbType.Text), Param(null, NpgsqlDbType.Timestamp), Param((long)PgDeadlockRemask.MaxFindingAlertRowsPerPass, NpgsqlDbType.Bigint),
        ], ct);
        AssertIndexRange(findings, "the finding alert slice of a pair with many alerts");
        Assert.True(
            findings.RowsRead <= PgDeadlockRemask.MaxFindingAlertRowsPerPass + 20,
            $"a slice read {findings.RowsRead} rows from the log; its page is {PgDeadlockRemask.MaxFindingAlertRowsPerPass} of the pair's {Alerts}");

        /* The pages themselves: the page size, and a cursor inside the server. */
        var (next, examined, _, _) = await PgDeadlockRemask.RemaskStoredAlertsAsync(connection, null, s_key, true, null, ct);
        Assert.InRange(examined, PgDeadlockRemask.MaxAlertRowsPerPass, PgDeadlockRemask.MaxAlertRowsPerPass + 20);
        Assert.Equal(7, next?.ServerId);
        Assert.NotNull(next?.AlertTime);

        var (findingNext, findingExamined, _, _) = await PgDeadlockRemask.RemaskStoredFindingAlertsAsync(connection, null, null, ct);
        Assert.InRange(findingExamined, PgDeadlockRemask.MaxFindingAlertRowsPerPass, PgDeadlockRemask.MaxFindingAlertRowsPerPass + 20);
        Assert.Equal("Analysis: big [00000001]", findingNext?.MetricName);
        Assert.NotNull(findingNext?.AlertTime);
    }

    /// <summary>A deadlock alert's context as an incident whose report retention has dropped: raw, and re-masked by the
    /// walk (it names a statement literal that must leave the store).</summary>
    private static string GoneContext() => AlertContextSerializer.Serialize(new AlertContext
    {
        Incidents = [new AlertIncident(PgDeadlockLogParser.HashOf("a report retention dropped"), ["UPDATE creds SET pw = 'Leak5625' WHERE id = 7"])],
    });

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var filler = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted)
SELECT timestamp '2026-10-01' + g * interval '3 seconds', 1 + g % $2, 'example-pg-' || (1 + g % $2),
       (ARRAY['High CPU', 'Blocking', 'Long Running Query', 'Poor Wait Stats', 'TempDB Space', 'Data File Space', 'Log File Space', 'Low Memory'])[1 + g % 8],
       1, 1, true, 'webhook', false
FROM generate_series(1, $1) AS g", connection))
        {
            filler.Parameters.Add(Param(FillerAlerts, NpgsqlDbType.Integer));
            filler.Parameters.Add(Param(Servers, NpgsqlDbType.Integer));
            await filler.ExecuteNonQueryAsync(ct);
        }

        await using (var decoys = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
SELECT CASE WHEN (g / 97) % 10 = 0 THEN timestamp '2026-09-01' ELSE timestamp '2026-10-02' END + g * interval '2 seconds',
       1 + g % $2, 'example-pg-' || (1 + g % $2),
       'Analysis: decoy ' || (g % 97) || ' [' || lpad(to_hex(g % 97), 8, '0') || ']',
       1, 1, true, 'webhook', false, '{""Details"":[]}'
FROM generate_series(1, $1) AS g", connection))
        {
            decoys.Parameters.Add(Param(FindingDecoys, NpgsqlDbType.Integer));
            decoys.Parameters.Add(Param(Servers, NpgsqlDbType.Integer));
            await decoys.ExecuteNonQueryAsync(ct);
        }

        var goneJson = AlertContextSerializer.Serialize(new AlertContext
        {
            Incidents = [new AlertIncident(PgDeadlockLogParser.HashOf("a report retention dropped"), ["UPDATE creds SET pw = 'Leak5625' WHERE id = 7"])],
        });
        await using (var current = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
SELECT timestamp '2026-10-03' + g * interval '5 seconds', 1 + g % $2, 'example-pg-' || (1 + g % $2), $3, 1, 1, true, 'webhook', false, '{""Incidents"":[]}'
FROM generate_series(1, $1) AS g", connection))
        {
            current.Parameters.Add(Param(CurrentDeadlockAlerts, NpgsqlDbType.Integer));
            current.Parameters.Add(Param(Servers, NpgsqlDbType.Integer));
            current.Parameters.Add(Param(AlertEngine.DeadlockWatermarkMetric, NpgsqlDbType.Text));
            await current.ExecuteNonQueryAsync(ct);
        }

        await using (var raw = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
SELECT timestamp '2026-10-04' + (g / 12) * interval '1 second', ($4::int[])[1 + g % 4], 'example-pg-' || ($4::int[])[1 + g % 4], $3, 1, 1, true, 'webhook', false, $2
FROM generate_series(1, $1) AS g", connection))
        {
            raw.Parameters.Add(Param(RawDeadlockAlerts, NpgsqlDbType.Integer));
            raw.Parameters.Add(Param(goneJson, NpgsqlDbType.Text));
            raw.Parameters.Add(Param(AlertEngine.DeadlockWatermarkMetric, NpgsqlDbType.Text));
            raw.Parameters.Add(Param(s_rawDeadlockServers.Select(s => int.Parse(s, CultureInfo.InvariantCulture)).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Integer));
            await raw.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE config_alert_log", ct);
    }

    private static async Task PlantAsync(NpgsqlConnection connection, int server, string metric, DateTime at, string context, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json)
VALUES ($1, $2, 'example-pg-' || $2, $3, 1, 1, true, 'webhook', false, $4)", connection);
        command.Parameters.Add(Param(at, NpgsqlDbType.Timestamp));
        command.Parameters.Add(Param(server, NpgsqlDbType.Integer));
        command.Parameters.Add(Param(metric, NpgsqlDbType.Text));
        command.Parameters.Add(Param(context, NpgsqlDbType.Text));
        await command.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── plans ───────────────────────── */

    private sealed record PlanFacts(
        IReadOnlyList<string> ScanTypes, IReadOnlyList<string> Indexes, IReadOnlyList<string> IndexConditions, long Buffers,
        long RowsRemovedByFilter, long RowsRead, IReadOnlyList<long> IndexSearches);

    /// <summary>The statement's plan, run with the same typed parameters the service binds (an unnamed statement, so
    /// planned with their values): the scan nodes over <c>config_alert_log</c>, their index conditions, the buffers they
    /// touched, the rows they filtered out after reading, and, on PostgreSQL 18, their index searches.</summary>
    private static async Task<PlanFacts> ExplainAsync(NpgsqlConnection connection, string sql, NpgsqlParameter[] parameters, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var scans = new List<string>();
        var indexes = new List<string>();
        var conditions = new List<string>();
        var searches = new List<long>();
        long buffers = 0, removed = 0, read = 0;
        void Walk(JsonElement node)
        {
            var type = node.GetProperty("Node Type").GetString()!;
            var onLog = node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "config_alert_log";
            if (onLog && type is "Seq Scan" or "Index Scan" or "Index Only Scan" or "Bitmap Heap Scan" or "Tid Range Scan")
            {
                scans.Add(type);
                buffers += node.GetProperty("Shared Hit Blocks").GetInt64() + node.GetProperty("Shared Read Blocks").GetInt64();
                removed += node.TryGetProperty("Rows Removed by Filter", out var filtered) ? filtered.GetInt64() : 0;
                read += (long)(node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble());
            }

            /* An index node over the log: Index Scan and Index Only Scan carry their relation, a Bitmap Index Scan
               (the child of a Bitmap Heap Scan) only its index. */
            if ((onLog || type == "Bitmap Index Scan") && node.TryGetProperty("Index Name", out var indexName))
            {
                indexes.Add(indexName.GetString()!);
                if (node.TryGetProperty("Index Cond", out var condition))
                {
                    conditions.Add(condition.GetString()!);
                }

                if (node.TryGetProperty("Index Searches", out var count))
                {
                    searches.Add(count.GetInt64());
                }
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }

        Walk(document.RootElement[0].GetProperty("Plan"));
        return new PlanFacts(scans, indexes, conditions, buffers, removed, read, searches);
    }

    /// <summary>One index range on <c>idx_config_alert_log_time</c>: the log is read only through that index (a Bitmap Heap
    /// Scan over it is the same range, which the planner picks when the page holds most of the range), never by a Seq
    /// Scan; nothing is removed by a filter after its read; and, where the server reports it (PostgreSQL 18), each index
    /// node searches once.</summary>
    private static void AssertIndexRange(PlanFacts plan, string what)
    {
        Assert.True(plan.Indexes.Count > 0, $"{what}: no index scan of config_alert_log in the plan");
        Assert.DoesNotContain(plan.ScanTypes, scan => scan is "Seq Scan" or "Tid Range Scan");
        Assert.All(plan.Indexes, index => Assert.Equal("idx_config_alert_log_time", index));
        Assert.True(plan.RowsRemovedByFilter == 0, $"{what}: {plan.RowsRemovedByFilter} rows were read and filtered out");
        Assert.All(plan.IndexSearches, count => Assert.Equal(1, count));
    }

    /// <summary>The range starts at one server (and metric): every index condition names them as equalities, so the
    /// read cannot stray into another server's rows.</summary>
    private static void AssertEquality(PlanFacts plan, string what, params string[] columns)
    {
        Assert.NotEmpty(plan.IndexConditions);
        foreach (var condition in plan.IndexConditions)
        {
            foreach (var column in columns)
            {
                Assert.True(condition.Contains(column + " = ", StringComparison.Ordinal), $"{what}: index condition '{condition}' has no equality on {column}");
            }
        }
    }

    private sealed record ScanFacts(string Relation, string NodeType, string? IndexName, long RowsRead);

    /// <summary>Every scan of the statement's plan with its alias, its index and the rows it returned in all its loops.
    /// A Bitmap Heap Scan takes the index of its Bitmap Index Scan child. The alert-log scan is the alias <c>a</c>.</summary>
    private static async Task<List<ScanFacts>> ExplainScansAsync(NpgsqlConnection connection, string sql, NpgsqlParameter[] parameters, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var scans = new List<ScanFacts>();
        void Walk(JsonElement node)
        {
            var type = node.GetProperty("Node Type").GetString()!;
            if (node.TryGetProperty("Relation Name", out var relation) && type.EndsWith("Scan", StringComparison.Ordinal))
            {
                var index = node.TryGetProperty("Index Name", out var name) ? name.GetString() : null;
                if (type == "Bitmap Heap Scan" && node.TryGetProperty("Plans", out var kids))
                {
                    index = kids.EnumerateArray().Select(kid => kid.TryGetProperty("Index Name", out var kidIndex) ? kidIndex.GetString() : null).FirstOrDefault(kidIndex => kidIndex is not null);
                }

                scans.Add(new ScanFacts(
                    node.TryGetProperty("Alias", out var alias) ? alias.GetString()! : relation.GetString()!,
                    type, index, (long)(node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble())));
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }

        Walk(document.RootElement[0].GetProperty("Plan"));
        return scans;
    }

    private static NpgsqlParameter Param(object? value, NpgsqlDbType type) =>
        new() { Value = value ?? DBNull.Value, NpgsqlDbType = type };

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, string first, string second, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(Param(first, NpgsqlDbType.Text));
        command.Parameters.Add(Param(second, NpgsqlDbType.Text));
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }
}
