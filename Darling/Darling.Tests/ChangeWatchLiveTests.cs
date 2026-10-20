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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6234 on a live store: a recorded change opens a watch, and when its window ends the pass compares it with the hour before and the
/// same hour a week earlier. Seeded wait rows stand in for the collected history; the changes are recorded through the store.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchLiveTests
{
    internal const int ServerId = 7;
    internal const string ServerName = "sql-seven";

    internal sealed record Harness(
        NpgsqlConnection Connection, NpgsqlDataSource Source, ChangeWatchStore Store, DateTime Now, Func<ChangeWatchPass> NewPass,
        Func<Func<DateTime>, ChangeWatchPass> NewPassAt);

    private static DateTime Minute(DateTime t) => new(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0, DateTimeKind.Utc);

    internal static async Task RunAsync(bool agentRoute, Func<Harness, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the change watch live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var source = NpgsqlDataSource.Create(storeString);
        await using var investigationSource = NpgsqlDataSource.Create(InvestigationDispatcher.WithLockTimeout(storeString));
        var now = Minute(DateTime.UtcNow);
        var config = new DarlingConfig();
        config.NotificationRoutes = agentRoute
            ? new[] { new NotificationRoute(1, InvestigationTriggerKinds.ChangeWatchMetric, "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelInvestigate } }
            : Array.Empty<NotificationRoute>();
        var settings = new DarlingAlertSettings(config);
        var store = new ChangeWatchStore(investigationSource, 30, () => now);
        var requests = new InvestigationRequestStore(investigationSource, 30, () => now);
        var dispatcher = new InvestigationDispatcher(requests, settings, NullLogger.Instance);

        /* A new pass over the same store and the same data source holds no state, as a restarted service would. */
        ChangeWatchPass NewPass() => new(source, store, dispatcher.Core, NullLogger.Instance, 30, () => now);
        ChangeWatchPass NewPassAt(Func<DateTime> clock) => new(source, store, dispatcher.Core, NullLogger.Instance, 30, clock);
        await body(new Harness(connection, source, store, now, NewPass, NewPassAt), ct);
    }

    internal static async Task SeedAsync(NpgsqlConnection connection, string table, IReadOnlyDictionary<string, object> values, CancellationToken ct)
    {
        /* Every NOT NULL column without a default that the test does not name gets a neutral value, so the seed survives a collector adding a column. */
        var filled = new Dictionary<string, object>(values);
        await using (var columns = new NpgsqlCommand(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 AND is_nullable = 'NO' AND column_default IS NULL AND is_identity = 'NO' AND is_generated = 'NEVER'", connection))
        {
            columns.Parameters.AddWithValue(table);
            await using var reader = await columns.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                if (filled.ContainsKey(name))
                {
                    continue;
                }

                filled[name] = reader.GetString(1) switch
                {
                    "text" or "character varying" => "x",
                    "boolean" => false,
                    "bytea" => new byte[] { 0 },
                    var t when t.StartsWith("timestamp", StringComparison.Ordinal) => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                    _ => 0,
                };
            }
        }

        var names = filled.Keys.ToList();
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO collect.{table} ({string.Join(", ", names)}) VALUES ({string.Join(", ", names.Select((_, i) => "$" + (i + 1).ToString(CultureInfo.InvariantCulture)))})", connection);
        foreach (var name in names)
        {
            insert.Parameters.Add(new NpgsqlParameter { Value = filled[name] });
        }

        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One wait row a minute over [start, start + 60 minutes], each holding <paramref name="msPerSecond"/> of wait time per second.</summary>
    private static async Task SeedWaitsAsync(NpgsqlConnection connection, DateTime start, double msPerSecond, CancellationToken ct)
    {
        for (var minute = 1; minute <= 60; minute++)
        {
            await SeedAsync(connection, "wait_stats", new Dictionary<string, object>
            {
                ["server_id"] = ServerId,
                ["server_name"] = ServerName,
                ["collection_time"] = DateTime.SpecifyKind(start.AddMinutes(minute), DateTimeKind.Unspecified),
                ["wait_type"] = "PAGEIOLATCH_SH",
                ["delta_wait_time_ms"] = (long)(msPerSecond * 60),
            }, ct);
        }
    }

    private static Task<ChangeWatchChange[]> OneChange(DateTime at, string what) =>
        Task.FromResult(new[] { new ChangeWatchChange($"server_config:{what}:{at:O}", "server_config", at, what) });

    internal static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    internal static async Task<string?> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AChangeFollowedByARegression_OpensOneRequest_WithTheChangeInItsBrief()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism: Configured value changed from 4 to 1"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            var detail = await TextAsync(h.Connection, "SELECT detail_text FROM config.investigation_request_alerts", ct);
            Assert.Contains("max degree of parallelism", detail, StringComparison.Ordinal);
            Assert.Contains("total_wait_ms_per_second before 100 after 1000", detail, StringComparison.Ordinal);
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task AChangeWithFlatNumbers_OpensNoRequest_AndClosesTheWatchQuietly()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 120, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "cost threshold for parallelism"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            Assert.Equal("closed", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task TwoChangesInsideOneWindow_GiveOneWatch_AndOneRequestNamingBoth()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var first = h.Now.AddMinutes(-100);
            var second = h.Now.AddMinutes(-85);
            await SeedWaitsAsync(h.Connection, first.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, first, 1000, ct);
            await SeedWaitsAsync(h.Connection, first.AddMinutes(60), 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(first, "trace flag 8048 enabled"), ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(second, "database Sales compatibility level"), ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            var detail = await TextAsync(h.Connection, "SELECT detail_text FROM config.investigation_request_alerts", ct);
            Assert.Contains("trace flag 8048", detail, StringComparison.Ordinal);
            Assert.Contains("compatibility level", detail, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AWeekEarlierHourAsBadAsTheAfterWindow_OpensNoRequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await SeedWaitsAsync(h.Connection, change.AddDays(-7), 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max server memory"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task AWatchWhoseWindowEndedWhileTheServiceWasDown_IsEvaluatedByTheNextPass()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddHours(-6);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "recovery model"), ct);

            /* The pass that recorded the change is gone; a new one over the same store finds the open watch whose window ended hours ago. */
            Assert.Equal("open", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal("closed", await TextAsync(h.Connection, "SELECT state FROM config.change_watches", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task WithInvestigationsOff_NothingIsOpened_AndTheWatchSaysSo()
    {
        await RunAsync(false, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("moved_no_request", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ARecordedChange_IsRecordedOnce_AndIsNotDueBeforeItsWindowEnds()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-30);
            var changes = await OneChange(change, "trace flag 1117");
            await h.Store.RecordChangesAsync(ServerId, ServerName, changes, ct);
            Assert.Null(await h.Store.RecordChangesAsync(ServerId, ServerName, changes, ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Empty(await h.Store.DueAsync(ct));
        });
    }

    /// <summary>Wait rows for the minutes first..last after <paramref name="start"/>, each carrying its 60 second interval, at <paramref name="msPerSecond"/>.</summary>
    private static async Task SeedWaitMinutesAsync(NpgsqlConnection connection, DateTime start, int firstMinute, int lastMinute, double msPerSecond, CancellationToken ct)
    {
        for (var minute = firstMinute; minute <= lastMinute; minute++)
        {
            await SeedAsync(connection, "wait_stats", new Dictionary<string, object>
            {
                ["server_id"] = ServerId,
                ["server_name"] = ServerName,
                ["collection_time"] = DateTime.SpecifyKind(start.AddMinutes(minute), DateTimeKind.Unspecified),
                ["wait_type"] = "PAGEIOLATCH_SH",
                ["delta_wait_time_ms"] = (long)(msPerSecond * 60),
                ["sample_interval_seconds"] = 60,
            }, ct);
        }
    }

    [Fact]
    public async Task ABeforeWindowCollectedForAThirdOfItsLength_IsNotCompared_AndIsListedWithItsCoverage()
    {
        await RunAsync(true, async (h, ct) =>
        {
            /* The service was down for 40 of the 60 minutes before the change; the load itself never changed (1000 ms/s throughout). */
            var change = h.Now.AddMinutes(-90);
            await SeedWaitMinutesAsync(h.Connection, change.AddMinutes(-60), 1, 20, 1000, ct);
            await SeedWaitMinutesAsync(h.Connection, change, 1, 60, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            var json = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!;
            var entry = json["not_measured"]!.AsArray().Single(n => n!["measure"]!.GetValue<string>() == "total_wait_ms_per_second")!;
            Assert.Contains("before window was collected for 33%", entry["reason"]!.GetValue<string>(), StringComparison.Ordinal);
            Assert.Empty(json["measures"]!.AsArray());
        });
    }

    [Fact]
    public async Task AWeekEarlierWindowOfTwoRows_DoesNotCount_AndTheMeasureSaysItWasNotCompared()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitMinutesAsync(h.Connection, change.AddMinutes(-60), 1, 60, 1000, ct);
            await SeedWaitMinutesAsync(h.Connection, change, 1, 60, 2000, ct);
            await SeedWaitMinutesAsync(h.Connection, change.AddDays(-7), 1, 2, 1, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var json = JsonNode.Parse((await TextAsync(h.Connection, "SELECT result_json FROM config.change_watches", ct))!)!;
            var measure = json["measures"]!.AsArray().Single(m => m!["measure"]!.GetValue<string>() == "total_wait_ms_per_second")!;
            Assert.Null(measure["week_earlier"]);
            Assert.Contains("week-earlier window was collected for", json["notes"]!.AsArray().Single(n => n!.GetValue<string>().Contains("week-earlier", StringComparison.Ordinal))!.GetValue<string>(), StringComparison.Ordinal);
        });
    }
}
