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
    public async Task NewestCollectionTimeSql_ReadsTheNewestNonFleetRow_AndStaysBounded()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5450 live test.");

        var bodySucceeded = false;
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, Ct);
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(Ct);
            await PgMigrations.MigrateAsync(connection, Ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, Ct), "TimescaleDB must be enabled on the test cluster");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, Ct);

            /* An empty store: a first install reads null and so fires nothing. */
            Assert.Null(await ScalarAsync(connection, DarlingSelfAlertEvaluator.NewestCollectionTimeSql));

            var start = DateTime.UtcNow;
            var newest = DateTime.SpecifyKind(start.AddMinutes(-40), DateTimeKind.Unspecified);
            /* Five chunks' worth: one row every 6 hours for 35 days, ending exactly 40 minutes before "start". */
            await ExecAsync(connection, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT row_number() OVER (ORDER BY t), 7, 'gap-host', 'cpu', t, 'SUCCESS'
FROM generate_series(@newest - INTERVAL '35 days', @newest, INTERVAL '6 hours') AS t",
                new NpgsqlParameter("newest", NpgsqlDbType.Timestamp) { Value = newest });
            /* The fleet sentinel's own newer row (the purge run record) must not hide the gap. */
            await ExecAsync(connection, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES (999999, 0, 'fleet', 'retention', @t, 'SUCCESS')",
                new NpgsqlParameter("t", NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(start.AddMinutes(-2), DateTimeKind.Unspecified) });

            var chunks = Convert.ToInt32(await ScalarAsync(connection,
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'collection_log'"), CultureInfo.InvariantCulture);
            Assert.True(chunks >= 4, $"seed should span several chunks, saw {chunks}");

            var read = await ScalarAsync(connection, DarlingSelfAlertEvaluator.NewestCollectionTimeSql);
            Assert.Equal(newest, Assert.IsType<DateTime>(read));

            /* Bounded: the plan reads the time index backward and stops at the first row; it never seq-scans. */
            var plan = new List<string>();
            await using (var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) " + DarlingSelfAlertEvaluator.NewestCollectionTimeSql, connection))
            await using (var reader = await explain.ExecuteReaderAsync(Ct))
            {
                while (await reader.ReadAsync(Ct))
                {
                    plan.Add(reader.GetString(0));
                }
            }

            var text = string.Join("\n", plan);
            Assert.DoesNotContain("Seq Scan", text, StringComparison.Ordinal);
            Assert.Contains("Index", text, StringComparison.Ordinal);
            Assert.Contains("Limit", text, StringComparison.Ordinal);
            Console.WriteLine("PLAN5450 chunks=" + chunks + "\n" + text);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () => await scratch.DisposeAsync());
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
