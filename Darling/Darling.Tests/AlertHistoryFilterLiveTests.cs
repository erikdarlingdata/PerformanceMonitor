/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6074 (gated, DARLING_TEST_PG): <c>get_alert_history</c> takes <c>metric_name</c> and <c>family</c>, applied in the SQL
/// ahead of the row cap. The seed puts a busier stream of one metric newer than a single row of the wanted metric, and asks for
/// a page smaller than the stream: a filter applied after the cap would return nothing.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertHistoryFilterLiveTests
{
    private const int ServerId = -607401;
    private const string ServerName = "alert-filter-6074";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert-history filter live tests.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await DeleteAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name, is_enabled = TRUE",
            ServerId, ServerName, DarlingMcpTestData.Naive(now));

        (int MinutesAgo, string Metric)[] rows =
        {
            (1, "Blocking Detected"), (2, "Blocking Detected"), (3, "Blocking Detected"), (4, "Custom:abc"),
            (20, "Failed Agent Job"), (25, "Long-Running Job"), (30, "High CPU"), (40, "Collection Stopped"),
        };
        foreach (var (minutesAgo, metric) in rows)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL)",
                DarlingMcpTestData.Naive(now.AddMinutes(-minutesAgo)), ServerId, ServerName, metric);
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }

    private static string[] Metrics(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).ToArray();

    [Fact]
    public async Task MetricNameAndFamily_ApplyBeforeTheRowCap_AndAnUnknownFilterIsAnEmptyAnswerThatNamesIt()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            await using var postgres = OpenDataSource(cs);

            var byMetric = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 2, 2, metric_name: "high cpu", cancellationToken: ct);
            Assert.Equal(new[] { "High CPU" }, Metrics(byMetric));
            Assert.False(JsonDocument.Parse(byMetric).RootElement.GetProperty("truncated").GetBoolean());

            var byFamily = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 2, 2, family: "Agent-Jobs", cancellationToken: ct);
            Assert.Equal(new[] { "Failed Agent Job", "Long-Running Job" }, Metrics(byFamily));

            var self = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 2, 5, family: "self-monitor", cancellationToken: ct);
            Assert.Equal(new[] { "Collection Stopped" }, Metrics(self));

            /* The fall-through family also owns what the census does not list (a custom alert), and the page is the family's newest rows. */
            var performance = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 2, 5, family: "performance", cancellationToken: ct);
            Assert.Equal(new[] { "Blocking Detected", "Blocking Detected", "Blocking Detected", "Custom:abc", "High CPU" }, Metrics(performance));

            var badFamily = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 2, 5, family: "nonsense", cancellationToken: ct)).RootElement;
            Assert.Equal("empty", badFamily.GetProperty("status").GetString());
            Assert.Contains("family 'nonsense'", badFamily.GetProperty("message").GetString(), StringComparison.Ordinal);

            var badMetric = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName, 2, 5, metric_name: "No Such Alert", cancellationToken: ct)).RootElement;
            Assert.Equal("empty", badMetric.GetProperty("status").GetString());
            Assert.Contains("metric_name 'No Such Alert'", badMetric.GetProperty("message").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task TheFamilyPredicate_AgreesWithAlertFamilyOf_ForEveryCensusNameAndPrefixShape()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = OpenDataSource(cs);
        var names = AlertFamily.MetricFamilies.Keys.Concat(AlertFamily.RecoveryPairs.Keys)
            .Concat(new[] { "Custom:1", "External:src:key", "Analysis: cpu_pressure [ab12cd34]", "Some Uncensused Alert" })
            .Distinct().ToList();

        foreach (var family in AlertFamily.All)
        {
            var predicate = AlertFamily.SqlFamilyPredicate("n", family);
            await using var command = postgres.CreateCommand(
                $"SELECT n FROM (VALUES {string.Join(", ", names.Select(n => "('" + n.Replace("'", "''") + "')"))}) t(n) WHERE {predicate}");
            command.CommandTimeout = 30;
            var matched = new System.Collections.Generic.List<string>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct)) matched.Add(reader.GetString(0));
            }

            Assert.Equal(names.Where(n => AlertFamily.Of(n) == family).OrderBy(n => n, StringComparer.Ordinal),
                matched.OrderBy(n => n, StringComparer.Ordinal));
        }
    }
}
