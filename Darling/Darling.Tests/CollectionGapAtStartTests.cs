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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5450: at start, one "Collection Gap At Start" alert when the newest pre-start collection is 15 minutes or
/// more before the start. The evaluator holds the gate; the live test reads the newest time with the real SQL.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
[Collection("gap-cache-serial")]
public sealed class CollectionGapAtStartTests
{
    private static readonly CancellationToken Ct = TestContext.Current.CancellationToken;
    private static readonly DateTime Start = new(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingSelfAlertEvaluator.CollectionGapReport Report(int minutesBeforeStart) =>
        new(Start.AddMinutes(-minutesBeforeStart), Start);

    [Fact]
    public async Task ThirtyMinuteGap_FiresOnce_WithTheMessageAndTheValues()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.EvaluateCollectionGapAtStartAsync(Report(30), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Collection Gap At Start", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("collectiongapatstart", fired.ServerKey);
        Assert.Equal(
            "Darling was not collecting from 2026-10-06 11:30 to 2026-10-06 12:00 UTC (30 minutes). " +
            "Nothing was collected for any server in that window. " +
            "If this was not a planned stop, check why the service or its host was down.",
            fired.DetailText);
        Assert.Equal(30, fired.NumericCurrentValue);
        Assert.Equal(15, fired.NumericThresholdValue);
    }

    [Fact]
    public async Task ExactlyFifteenMinutes_Fires_AndFourteen_DoesNot()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.EvaluateCollectionGapAtStartAsync(Report(14), Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        await e.EvaluateCollectionGapAtStartAsync(Report(15), Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task FiveMinuteGap_FiresNothing()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.EvaluateCollectionGapAtStartAsync(Report(5), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task EmptyStore_FiresNothing()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        await e.EvaluateCollectionGapAtStartAsync(null, Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AlertsDisabled_FiresNothing()
    {
        var h = new DarlingSelfAlertTests.Harness();
        h.Settings.AlertsEnabled = false;
        var e = h.Build();

        await e.EvaluateCollectionGapAtStartAsync(Report(30), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public void TheMetric_IsSelfMonitorFamily_Warning_AndNumeric()
    {
        Assert.Equal("Collection Gap At Start", DarlingSelfAlertEvaluator.CollectionGapAtStartMetric);
        Assert.Equal(AlertFamily.SelfMonitor, AlertFamily.Of(DarlingSelfAlertEvaluator.CollectionGapAtStartMetric));
        Assert.Equal("WARNING", AlertSeverity.ForMetric(DarlingSelfAlertEvaluator.CollectionGapAtStartMetric).BadgeText);
        /* A measurement (minutes), so never the state-only dash. */
        Assert.False(PerformanceMonitor.Common.AlertMetricClassifier.IsStateOnly(DarlingSelfAlertEvaluator.CollectionGapAtStartMetric));
        Assert.Equal(15, DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold.TotalMinutes);
    }

    [Fact]
    public async Task GapIsMeasuredToTheProcessStart_NotToTheEndOfALongMigration()
    {
        /* The report's StartUtc is the process start. A migration that then ran for 20 minutes does not
           enter the measure: 10 minutes from the last collection to the process start fires nothing. */
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var processStart = Start;
        var report = new DarlingSelfAlertEvaluator.CollectionGapReport(processStart.AddMinutes(-10), processStart);
        _ = processStart.AddMinutes(20); /* migrations finished here; the old code measured to this */
        await e.EvaluateCollectionGapAtStartAsync(report, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* A negative gap (a row newer than the start) fires nothing either. */
        await e.EvaluateCollectionGapAtStartAsync(new DarlingSelfAlertEvaluator.CollectionGapReport(processStart.AddMinutes(5), processStart), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenStoreAsync(string baseCs, bool hypertable)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseCs, Ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        if (!hypertable)
        {
            /* The migration makes a hypertable when the cluster has TimescaleDB. Swap in an ordinary table with
               the one shipped index, so the plan is the one a store without TimescaleDB gets. */
            await ExecAsync(connection, @"
DROP TABLE collect.collection_log CASCADE;
CREATE TABLE collect.collection_log (log_id bigint NOT NULL, server_id integer NOT NULL, server_name text,
    collector_name text, collection_time timestamp NOT NULL, status text);
CREATE INDEX idx_collection_log_time ON collect.collection_log(server_id, collection_time);");
        }

        if (hypertable)
        {
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, Ct), "TimescaleDB must be enabled on the test cluster");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, Ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, Ct));
        }

        return (scratch, connection);
    }

    private static async Task AddServerAsync(NpgsqlConnection connection, int id, bool enabled) =>
        await ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (@id, @n, 'h', @e)",
            new NpgsqlParameter("id", id), new NpgsqlParameter("n", "gap-" + id), new NpgsqlParameter("e", enabled));

    private static async Task SeedAsync(NpgsqlConnection connection, int serverId, DateTime newest, int days)
    {
        var u = DateTime.SpecifyKind(newest, DateTimeKind.Unspecified);
        await ExecAsync(connection, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT (@sid::bigint * 1000000) + row_number() OVER (ORDER BY t), @sid, 'gap-host', 'cpu', t, 'SUCCESS'
FROM generate_series(@newest - make_interval(days => @days), @newest, INTERVAL '1 hour') AS t",
            new NpgsqlParameter("sid", serverId),
            new NpgsqlParameter("days", days),
            new NpgsqlParameter("newest", NpgsqlDbType.Timestamp) { Value = u });
    }

    private static async Task<string> ExplainJsonAsync(NpgsqlConnection connection)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + DarlingSelfAlertEvaluator.NewestCollectionTimeSql, connection);
        return Convert.ToString(await explain.ExecuteScalarAsync(Ct), CultureInfo.InvariantCulture)!;
    }

    private static void CollectScans(System.Text.Json.JsonElement e, List<string> scans)
    {
        if (e.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            if (e.TryGetProperty("Node Type", out var t) && t.GetString()!.EndsWith("Scan", StringComparison.Ordinal))
            {
                scans.Add(t.GetString() + ": " + (e.TryGetProperty("Relation Name", out var r) ? r.GetString() : ""));
            }

            foreach (var p in e.EnumerateObject())
            {
                CollectScans(p.Value, scans);
            }
        }
        else if (e.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            foreach (var x in e.EnumerateArray())
            {
                CollectScans(x, scans);
            }
        }
    }

    private static string? Env() => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task NoEnabledServer_ReadsNull_SoNothingFires()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Env()), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var (scratch, connection) = await OpenStoreAsync(Env()!, hypertable: false);
        try
        {
            /* A store with no servers at all (first install) reads null. */
            Assert.Null(await ScalarAsync(connection, DarlingSelfAlertEvaluator.NewestCollectionTimeSql));
            await AddServerAsync(connection, 7, enabled: false);
            await AddServerAsync(connection, 8, enabled: false);
            await SeedAsync(connection, 7, DateTime.UtcNow.AddMinutes(-40), 2);
            Assert.Null(await ScalarAsync(connection, DarlingSelfAlertEvaluator.NewestCollectionTimeSql));
            ok = true;
        }
        finally
        {
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }

    [Fact]
    public async Task DisabledServersNewerRows_AndTheFleetSentinel_DoNotHideTheGap()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Env()), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var (scratch, connection) = await OpenStoreAsync(Env()!, hypertable: false);
        try
        {
            var now = DateTime.UtcNow;
            var newest = DateTime.SpecifyKind(now.AddMinutes(-40), DateTimeKind.Unspecified);
            await AddServerAsync(connection, 7, enabled: true);
            await AddServerAsync(connection, 8, enabled: false);
            await SeedAsync(connection, 7, now.AddMinutes(-40), 1);
            await SeedAsync(connection, 8, now.AddMinutes(-1), 1);
            await SeedAsync(connection, 0, now.AddMinutes(-2), 1);
            var read = Assert.IsType<DateTime>(await ScalarAsync(connection, DarlingSelfAlertEvaluator.NewestCollectionTimeSql));
            Assert.Equal(newest, read);
            ok = true;
        }
        finally
        {
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NewestCollectionTimeSql_IsOneBackwardIndexProbePerServer_NeverASeqScan(bool hypertable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Env()), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var (scratch, connection) = await OpenStoreAsync(Env()!, hypertable);
        try
        {
            var now = DateTime.UtcNow;
            var newest = DateTime.SpecifyKind(now.AddMinutes(-40), DateTimeKind.Unspecified);
            foreach (var id in new[] { 7, 8, 9 })
            {
                await AddServerAsync(connection, id, enabled: true);
                await SeedAsync(connection, id, now.AddMinutes(-40 - id), 35);
            }

            await SeedAsync(connection, 7, now.AddMinutes(-40), 0);
            await ExecAsync(connection, "ANALYZE collect.collection_log");
            Assert.Equal(newest, Assert.IsType<DateTime>(await ScalarAsync(connection, DarlingSelfAlertEvaluator.NewestCollectionTimeSql)));

            var chunks = Convert.ToInt32(await ScalarAsync(connection,
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'collection_log'"), CultureInfo.InvariantCulture);
            var isHypertable = Convert.ToInt32(await ScalarAsync(connection,
                "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'collection_log'"), CultureInfo.InvariantCulture);
            Assert.Equal(hypertable ? 1 : 0, isHypertable);
            if (hypertable)
            {
                Assert.True(chunks >= 4, $"seed should span several chunks, saw {chunks}");
            }

            var plan = await ExplainJsonAsync(connection);
            var scansSeen = new List<string>();
            Console.WriteLine("PLAN5450 hypertable=" + hypertable + " chunks=" + chunks + "\n" + plan);
            /* The three-row server list may be seq-scanned; collection_log (or any chunk of it) never. */
            using (var doc = System.Text.Json.JsonDocument.Parse(plan))
            {
                CollectScans(doc.RootElement, scansSeen);
                Assert.DoesNotContain(scansSeen, x => x.StartsWith("Seq Scan:", StringComparison.Ordinal) && !x.Contains("config_monitored_servers", StringComparison.Ordinal));
            }

            Assert.Contains(scansSeen, x => x.StartsWith("Index Only Scan:", StringComparison.Ordinal) || x.StartsWith("Index Scan:", StringComparison.Ordinal));
            Assert.Contains("Backward", plan, StringComparison.Ordinal);
            if (!hypertable)
            {
                Assert.Contains("idx_collection_log_time", plan, StringComparison.Ordinal);
            }

            ok = true;
        }
        finally
        {
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(Ct);
        return value is DBNull ? null : value;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, params NpgsqlParameter[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        await command.ExecuteNonQueryAsync(Ct);
    }
}
