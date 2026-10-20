/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #6234, the PostgreSQL pass, on a live store: a PostgreSQL target's setting, per-database override and extension changes are
/// detected through the marks, and its watch is evaluated from the wait, CPU and statement collectors. Seeded rows stand in for the
/// collected history.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchPostgresLiveTests
{
    private const int PgServerId = 21;
    private const string PgServerName = "pg-twentyone";

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static async Task AddPgServerAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) " +
            "VALUES ($1, $2, $2, true, 'postgres', now(), now())", c);
        command.Parameters.AddWithValue(PgServerId);
        command.Parameters.AddWithValue(PgServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task ConfigAsync(NpgsqlConnection c, DateTime at, string name, string value, string? database, CancellationToken ct) =>
        SeedAsync(c, "pg_server_config", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(at), ["name"] = name,
            ["setting"] = value, ["source"] = database is null ? "configuration file" : "database",
            ["database_name"] = (object?)database ?? DBNull.Value,
        }, ct);

    private static Task ExtensionAsync(NpgsqlConnection c, DateTime at, string extension, string? version, CancellationToken ct) =>
        SeedAsync(c, "pg_extension_availability", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(at), ["database_name"] = "app",
            ["extension_name"] = extension, ["state"] = version is null ? "available" : "installed",
            ["installed_version"] = (object?)version ?? DBNull.Value,
        }, ct);

    /// <summary>One wait row a minute over (start, start + 60 minutes], each holding <paramref name="msPerSecond"/> of wait time per second.</summary>
    private static async Task WaitsAsync(NpgsqlConnection c, DateTime start, double msPerSecond, CancellationToken ct)
    {
        for (var minute = 1; minute <= 60; minute++)
        {
            await SeedAsync(c, "pg_wait_stats", new Dictionary<string, object>
            {
                ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(start.AddMinutes(minute)),
                ["wait_type"] = "IO", ["wait_event"] = "DataFileRead", ["delta_waits"] = 100L, ["delta_wait_time_us"] = (long)(msPerSecond * 60 * 1000), ["sample_interval_seconds"] = 60,
            }, ct);
        }
    }

    /// <summary>One CPU sample a minute, as Performance Insights supplies them: the evaluator needs at least half of a window covered.</summary>
    private static async Task CpuAsync(NpgsqlConnection c, DateTime start, double percent, CancellationToken ct)
    {
        for (var minute = 1; minute <= 60; minute++)
        {
            await SeedAsync(c, "pg_cpu_utilization", new Dictionary<string, object>
            {
                ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(start.AddMinutes(minute)),
                ["sample_time"] = Naive(start.AddMinutes(minute)), ["cpu_percent"] = percent,
            }, ct);
        }
    }

    private static Task StatementAsync(NpgsqlConnection c, DateTime start, long calls, long totalMs, CancellationToken ct) =>
        SeedAsync(c, "pg_statement_stats", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(start.AddMinutes(30)),
            ["queryid"] = 42L, ["database_id"] = 5L, ["user_id"] = 10L, ["toplevel"] = true,
            ["delta_calls"] = calls, ["delta_total_exec_time_ms"] = totalMs,
        }, ct);

    private static Task<ChangeWatchChange[]> OneChange(DateTime at, string what) =>
        Task.FromResult(new[] { new ChangeWatchChange($"pg_config:{what}:{at:O}", "pg_config", at, what) });

    private static async Task<JsonNode> ResultAsync(NpgsqlConnection c, CancellationToken ct) =>
        JsonNode.Parse((await TextAsync(c, "SELECT result_json FROM config.change_watches", ct))!)!;

    private static string[] Keys(IEnumerable<ChangeWatchChange> changes) => changes.Select(c => c.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task ASettingChangeFollowedByARegression_OpensOneRequest_AfterDetectionRecordedIt()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var start = h.Now.AddHours(-3);
            await ConfigAsync(h.Connection, start.AddMinutes(-50), "work_mem", "4096", null, ct);
            await ConfigAsync(h.Connection, start.AddMinutes(-10), "work_mem", "4096", null, ct);

            /* The first pass on a store with history sets the marks and opens nothing. */
            await h.NewPassAt(() => start).DetectAsync(ct);
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(ChangeWatchStore.PostgresMarkSources.OrderBy(s => s), (await h.Store.GetMarksAsync(PgServerId, ct)).Keys.OrderBy(s => s));

            /* A later capture shows work_mem moved: the pass records it, and the watch is due when its hour ends. */
            var change = start.AddMinutes(5);
            await ConfigAsync(h.Connection, change, "work_mem", "65536", null, ct);
            await h.NewPassAt(() => start.AddMinutes(10)).DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));

            await WaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await WaitsAsync(h.Connection, change, 1000, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'change_watch'", ct));
            var detail = await TextAsync(h.Connection, "SELECT detail_text FROM config.investigation_request_alerts", ct);
            Assert.Contains("work_mem", detail, StringComparison.Ordinal);
            Assert.Contains("total_wait_ms_per_second before 100 after 1000", detail, StringComparison.Ordinal);
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task FlatNumbers_OpenNothing_AndTheWatchClosesFlat()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var change = h.Now.AddMinutes(-90);
            await WaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await WaitsAsync(h.Connection, change, 120, ct);
            await CpuAsync(h.Connection, change.AddMinutes(-60), 20, ct);
            await CpuAsync(h.Connection, change, 25, ct);
            await StatementAsync(h.Connection, change.AddMinutes(-60), 50, 500, ct);
            await StatementAsync(h.Connection, change, 50, 600, ct);
            await h.Store.RecordChangesAsync(PgServerId, PgServerName, await OneChange(change, "shared_buffers"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            var measures = (await ResultAsync(h.Connection, ct))["measures"]!.AsArray();
            Assert.Equal(2, measures.Count(m => (bool)m!["moved"]! == false));
            Assert.Empty((await ResultAsync(h.Connection, ct))["not_measured"]!.AsArray());
        });
    }

    [Fact]
    public async Task AWeekEarlierHourAsBadAsTheAfterWindow_OpensNothing()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var change = h.Now.AddMinutes(-90);
            await WaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await WaitsAsync(h.Connection, change, 1000, ct);
            /* The same hour a week earlier was just as bad: the daily curve, not the change. */
            await WaitsAsync(h.Connection, change.AddDays(-7), 1000, ct);
            await h.Store.RecordChangesAsync(PgServerId, PgServerName, await OneChange(change, "work_mem"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ATargetWithNoCpuSource_ReportsCpuAsNotMeasured_NeverAsFlat_AndTheOtherMeasuresStillOpenTheRequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var change = h.Now.AddMinutes(-90);
            await WaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await WaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(PgServerId, PgServerName, await OneChange(change, "work_mem"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var result = await ResultAsync(h.Connection, ct);
            Assert.DoesNotContain(result["measures"]!.AsArray(), m => (string)m!["measure"]! == "avg_cpu_percent");
            var cpu = Assert.Single(result["not_measured"]!.AsArray(), n => (string)n!["measure"]! == "avg_cpu_percent");
            Assert.Contains("no CPU source", (string)cpu!["reason"]!, StringComparison.Ordinal);
            Assert.Contains(result["not_measured"]!.AsArray(), n => (string)n!["measure"]! == "query_avg_duration_ms");
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task CpuThatRose_AndAStatementThatDoubled_EachOpenTheRequest_AndTheTenCallFloorHoldsStatementsBack()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var change = h.Now.AddMinutes(-90);
            await WaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await WaitsAsync(h.Connection, change, 100, ct);
            await CpuAsync(h.Connection, change.AddMinutes(-60), 20, ct);
            await CpuAsync(h.Connection, change, 60, ct);
            await StatementAsync(h.Connection, change.AddMinutes(-60), 5, 50, ct);
            await StatementAsync(h.Connection, change, 5, 250, ct);
            await h.Store.RecordChangesAsync(PgServerId, PgServerName, await OneChange(change, "random_page_cost"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            /* Five calls in each window: the statement is under the floor, so only CPU moved. */
            var result = await ResultAsync(h.Connection, ct);
            var moved = result["measures"]!.AsArray().Where(m => (bool)m!["moved"]!).Select(m => (string)m!["measure"]!).ToArray();
            Assert.Equal(["avg_cpu_percent"], moved);
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });

        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var change = h.Now.AddMinutes(-90);
            await WaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await WaitsAsync(h.Connection, change, 100, ct);
            await StatementAsync(h.Connection, change.AddMinutes(-60), 50, 500, ct);
            await StatementAsync(h.Connection, change, 50, 2500, ct);
            await h.Store.RecordChangesAsync(PgServerId, PgServerName, await OneChange(change, "random_page_cost"), ct);

            await h.NewPass().EvaluateDueAsync(ct);

            var detail = await TextAsync(h.Connection, "SELECT detail_text FROM config.investigation_request_alerts", ct);
            Assert.Contains("query_avg_duration_ms:5:42 before 10 after 50", detail, StringComparison.Ordinal);
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ReadPostgresChanges_FindsExactlyTheChangesAfterEachSourcesMark_AndNothingAtOrBeforeIt()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var mark = h.Now.AddMinutes(-30);
            var s0 = mark.AddMinutes(-20);
            var s1 = mark.AddMinutes(-10);
            var s2 = mark.AddMinutes(10);
            var s3 = mark.AddMinutes(20);
            foreach (var (at, server, scoped) in new[] { (s0, "4096", "1024"), (s1, "8192", "2048"), (s2, "16384", "4096"), (s3, "16384", "4096") })
            {
                await ConfigAsync(h.Connection, at, "work_mem", server, null, ct);
                await ConfigAsync(h.Connection, at, "work_mem", scoped, "app", ct);
            }

            foreach (var (at, version) in new[] { (mark.AddHours(-2), (string?)null), (mark.AddHours(-1), "1.0"), (s2, "1.1") })
            {
                await ExtensionAsync(h.Connection, at, "pg_stat_statements", version, ct);
            }

            Dictionary<string, DateTime> Marks(DateTime at) => ChangeWatchStore.PostgresMarkSources.ToDictionary(s => s, _ => at, StringComparer.Ordinal);
            var pass = h.NewPass();

            var found = await pass.ReadPostgresChangesAsync(PgServerId, Marks(mark), h.Now, ct);
            Assert.Equal(3, found.Count);
            Assert.Single(found, c => c.Kind == "pg_config" && c.ChangeTimeUtc == s2 && c.Description.Contains("from 8192 to 16384", StringComparison.Ordinal));
            Assert.Single(found, c => c.Kind == "pg_scoped_config" && c.ChangeTimeUtc == s2 && c.DatabaseName == "app");
            Assert.Single(found, c => c.Kind == "pg_extension" && c.ChangeTimeUtc == s2 && c.Description.Contains("version changed from 1.0 to 1.1", StringComparison.Ordinal));

            /* A mark at the change itself excludes it: a change is seen once. */
            Assert.Empty(await pass.ReadPostgresChangesAsync(PgServerId, Marks(s2), h.Now, ct));

            /* An earlier mark finds the older changes too, so the mark is what left them out above. */
            var earlierMarks = Marks(s0.AddMinutes(-5));
            earlierMarks["pg_extensions"] = mark.AddMinutes(-90);
            var earlier = await pass.ReadPostgresChangesAsync(PgServerId, earlierMarks, h.Now, ct);
            Assert.Contains(earlier, c => c.ChangeTimeUtc == s1 && c.Kind == "pg_config");
            Assert.Contains(earlier, c => c.ChangeTimeUtc == s1 && c.Kind == "pg_scoped_config");
            Assert.Contains(earlier, c => c.Kind == "pg_extension" && c.Description.Contains("installed", StringComparison.Ordinal));
            Assert.True(earlier.Count > found.Count);

            /* Each source is bounded by its own mark: moving only the extension mark leaves the settings found. */
            var mixed = Marks(mark);
            mixed["pg_extensions"] = s3;
            Assert.Equal(2, (await pass.ReadPostgresChangesAsync(PgServerId, mixed, h.Now, ct)).Count);
            Assert.Equal(found.Count, Keys(found).Distinct().Count());
        });
    }

    /// <summary>
    /// The cumulative pg_wait_sampling profile for one wait event, one collection every 5 minutes over (start, start + 60 minutes], the profile
    /// growing by <paramref name="msPerSecond"/> of waiting per second at a 10 ms period.
    /// </summary>
    private static async Task SamplingAsync(NpgsqlConnection c, DateTime start, string eventType, string waitEvent, double msPerSecond, long baseSamples, CancellationToken ct)
    {
        var perStep = (long)(msPerSecond * 300 / 10);
        for (var step = 1; step <= 12; step++)
        {
            await SeedAsync(c, "pg_wait_sampling", new Dictionary<string, object>
            {
                ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(start.AddMinutes(step * 5)),
                ["event_type"] = eventType, ["event"] = waitEvent, ["query_id"] = 0L,
                ["sample_count"] = baseSamples + step * perStep, ["profile_period_ms"] = 10, ["backend_count"] = 1,
            }, ct);
        }
    }

    [Fact]
    public async Task AStockPostgresTargetWithPgWaitSampling_HasItsWaitsMeasured_AndAWaitRegressionOpensTheRequest()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var start = h.Now.AddHours(-3);
            await ConfigAsync(h.Connection, start.AddMinutes(-50), "work_mem", "4096", null, ct);
            await ConfigAsync(h.Connection, start.AddMinutes(-10), "work_mem", "4096", null, ct);
            await h.NewPassAt(() => start).DetectAsync(ct);
            var change = start.AddMinutes(5);
            await ConfigAsync(h.Connection, change, "work_mem", "65536", null, ct);
            await h.NewPassAt(() => start.AddMinutes(10)).DetectAsync(ct);

            /* Waits 100 ms/s before and 1000 after; a CPU-event series at 3000 ms/s in both windows is time on the CPU, not waiting. */
            await SamplingAsync(h.Connection, change.AddMinutes(-60), "IO", "DataFileRead", 100, 1_000, ct);
            await SamplingAsync(h.Connection, change, "IO", "DataFileRead", 1000, 1_000 + 12 * 3_000, ct);
            await SamplingAsync(h.Connection, change.AddMinutes(-60), "CPU", "CPU", 3000, 0, ct);
            await SamplingAsync(h.Connection, change, "CPU", "CPU", 3000, 12 * 90_000, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            var json = await ResultAsync(h.Connection, ct);
            var wait = json["measures"]!.AsArray().Single(m => m!["measure"]!.GetValue<string>() == "total_wait_ms_per_second")!;
            Assert.InRange(wait["before"]!.GetValue<double>(), 95, 105);
            Assert.InRange(wait["after"]!.GetValue<double>(), 950, 1050);
            Assert.True(wait["moved"]!.GetValue<bool>());
            Assert.DoesNotContain(json["not_measured"]!.AsArray(), n => n!["measure"]!.GetValue<string>() == "total_wait_ms_per_second");
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ATargetWithNeitherWaitSource_NamesBothMissingPieces_AndAStatementAbsentBeforeIsListed()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            var start = h.Now.AddHours(-3);
            await ConfigAsync(h.Connection, start.AddMinutes(-50), "work_mem", "4096", null, ct);
            await ConfigAsync(h.Connection, start.AddMinutes(-10), "work_mem", "4096", null, ct);
            await h.NewPassAt(() => start).DetectAsync(ct);
            var change = start.AddMinutes(5);
            await ConfigAsync(h.Connection, change, "work_mem", "65536", null, ct);
            await h.NewPassAt(() => start.AddMinutes(10)).DetectAsync(ct);

            /* Statement rows exist in both windows, but the one after-query is not in the before window (a statistics reset or a new query). */
            await SeedAsync(h.Connection, "pg_statement_stats", new Dictionary<string, object>
            {
                ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(change.AddMinutes(-30)),
                ["queryid"] = 41L, ["database_id"] = 5L, ["user_id"] = 10L, ["toplevel"] = true, ["delta_calls"] = 50L, ["delta_total_exec_time_ms"] = 500L,
            }, ct);
            await StatementAsync(h.Connection, change, 50, 5_000, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            var notMeasured = (await ResultAsync(h.Connection, ct))["not_measured"]!.AsArray();
            var wait = notMeasured.Single(n => n!["measure"]!.GetValue<string>() == "total_wait_ms_per_second")!["reason"]!.GetValue<string>();
            Assert.Contains("pg_wait_stats", wait, StringComparison.Ordinal);
            Assert.Contains("pg_wait_sampling", wait, StringComparison.Ordinal);
            var statement = notMeasured.Single(n => n!["measure"]!.GetValue<string>() == "query_avg_duration_ms")!["reason"]!.GetValue<string>();
            Assert.Contains("no pg_stat_statements rows in the before window", statement, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheEmptyReadNamesTheChangeKindsOfTheTargetsOwnEngine()
    {
        await RunAsync(false, async (h, ct) =>
        {
            await AddPgServerAsync(h.Connection, ct);
            await using (var insert = new NpgsqlCommand(
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) " +
                "VALUES (22, 'sql-twentytwo', 'sql-twentytwo', true, 'sqlserver', now(), now())", h.Connection))
            {
                await insert.ExecuteNonQueryAsync(ct);
            }

            var postgres = await DarlingMcpChangeWatchTools.GetChangeWatches(h.Source, PgServerName, cancellationToken: ct);
            Assert.Contains("extension", postgres, StringComparison.Ordinal);
            Assert.DoesNotContain("trace flag", postgres, StringComparison.Ordinal);

            var sqlServer = await DarlingMcpChangeWatchTools.GetChangeWatches(h.Source, "sql-twentytwo", cancellationToken: ct);
            Assert.Contains("trace flag", sqlServer, StringComparison.Ordinal);
            Assert.DoesNotContain("extension", sqlServer, StringComparison.Ordinal);
        });
    }
}
