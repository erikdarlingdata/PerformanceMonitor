/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: each test mints a scratch TimescaleDB database, because it materialises continuous aggregates. */

/// <summary>
/// <c>get_resource_use_by_tag</c> (#5983) on a TimescaleDB store, where the plan-cache and procedures sections route by retention
/// tier: what each says about how far back it reached, and the two hourly statements read by column name.
/// </summary>
public sealed class QueryTagRollupRoutingLiveTests
{
    private const int ServerId = -598301;
    private const string ServerName = "a5983-tag-routing";

    /* A window far in the past: Hourly tier by age, whatever the clock says. Aligned to the hour so no partial-hour edge muddies the pin. */
    private static readonly DateTime WindowEnd = new(2026, 1, 5, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime At = WindowEnd.AddHours(-10).AddMinutes(30);

    private static Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object?[] args) =>
        DarlingMcpTestData.ExecAsync(connection, ct, sql, args);

    private static async Task TagAsync(NpgsqlConnection connection, CancellationToken ct, string name, string kind, string key)
    {
        await ExecAsync(connection, ct, "INSERT INTO config.query_tags (name) VALUES ($1)", name);
        await ExecAsync(connection, ct,
            "INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) SELECT $1, $2, $3, id, 'hand_hash' FROM config.query_tags WHERE name = $4",
            ServerId, kind, key, name);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, CancellationToken ct, string view, DateTime from, DateTime to)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static JsonElement Section(JsonDocument doc, string name)
    {
        foreach (var s in doc.RootElement.GetProperty("sections").EnumerateArray())
        {
            if (s.GetProperty("section").GetString() == name)
            {
                return s;
            }
        }

        throw new InvalidOperationException("no section " + name);
    }

    /// <summary>A 168-hour ask on a store whose raw tier keeps four days and whose io rollup holds nothing: both stats sections fall to raw
    /// over a window raw does not hold, and each says how far it reached (the raw floor), not the 7 days that were asked.</summary>
    [Fact]
    public Task ASevenDayAsk_FallingToRaw_NamesTheRawFloor_PerSection() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            var floor = DarlingMcpTestData.TruncateToSeconds(now.AddDays(-4));
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, floor, "QFLOOR", 1_000, 1_000, 7, 1);
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, now.AddHours(-2), "QNEW", 1_000, 1_000, 7, 1);
            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, floor, "PFLOOR", 1_000, 1_000, 7, 1);
            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, now.AddHours(-2), "PNEW", 1_000, 1_000, 7, 1);
            /* The interval-honest hourly view is refreshed so the retention tier for a 7-day window is Hourly; the io rollup is left empty,
               so the io route is unavailable and the section reads raw. */
            await RefreshAsync(connection, ct, TimescaleSupport.QueryStatsIntervalHourlyView, now.AddDays(-5), now.AddHours(1));
            await RefreshAsync(connection, ct, TimescaleSupport.ProcedureStatsIntervalHourlyView, now.AddDays(-5), now.AddHours(1));

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var doc = JsonDocument.Parse(
                await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(source, ServerName, 168, null, 25, null, null, ct));

            foreach (var name in new[] { "plan_cache", "procedures" })
            {
                var section = Section(doc, name);
                Assert.Equal("raw", section.GetProperty("tier").GetString());
                Assert.True(section.GetProperty("window_truncated").GetBoolean(), name + " reached back 4 days of the 7 asked");
                Assert.Equal(
                    McpHelpers.FormatEffectiveStart(floor), section.GetProperty("effective_start").GetString());
                Assert.False(string.IsNullOrEmpty(section.GetProperty("truncation_note").GetString()));
            }
        });

    /// <summary>A window the hourly rollup serves: the section's window_end is the served end (the materialisation ceiling), not the end asked.</summary>
    [Fact]
    public Task AnHourlyServedWindow_ReportsTheMaterialisationCeiling_AsItsWindowEnd() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, At, "QHOUR", 4_000_000, 6_000_000, 11, 3);
            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, At, "PHOUR", 4_000_000, 6_000_000, 11, 3);
            /* Materialised only to three hours before the end asked, so the ceiling is an edge. */
            var through = WindowEnd.AddHours(-3);
            foreach (var view in new[]
                     {
                         TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIoHourlyView,
                         TimescaleSupport.ProcedureStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView,
                     })
            {
                await RefreshAsync(connection, ct, view, WindowEnd.AddHours(-30), through);
            }

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
            var planCeiling = coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsIoHourlyView, WindowEnd.AddHours(-24));
            var procCeiling = coverage.HourlyEndCeiling(TimescaleSupport.ProcedureStatsIoHourlyView, WindowEnd.AddHours(-24));
            Assert.True(planCeiling is not null && planCeiling < WindowEnd, "the seed must leave the ceiling before the end asked");

            await using var fresh = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var doc = JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(
                fresh, ServerName, 24, null, 25, WindowEnd.ToString("o", CultureInfo.InvariantCulture), null, ct));

            var plan = Section(doc, "plan_cache");
            var procedures = Section(doc, "procedures");
            Assert.Equal("hourly", plan.GetProperty("tier").GetString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(planCeiling!.Value), plan.GetProperty("window_end").GetString());
            Assert.Equal("hourly", procedures.GetProperty("tier").GetString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(procCeiling!.Value), procedures.GetProperty("window_end").GetString());
            Assert.Contains("materialized only to", plan.GetProperty("window_edges_note").GetString(), StringComparison.Ordinal);
        });

    /// <summary>#6794: a 24-hour window is inside raw retention, so a one-server read takes raw. A fleet read (no server) takes the hourly
    /// io rollup there, because raw would be every server's raw rows for the whole window, and says so with the hourly route's own words:
    /// tier hourly, the served window_end and the edges note. The figures are the rollup's, which equal raw's over the whole hours.</summary>
    [Fact]
    public Task AFleetRead_InsideRawRetention_TakesTheHourlyRollup_AndSaysSo_WhileOneServerKeepsRaw() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            var now = DateTime.UtcNow;
            /* One row before the window, so the io rollup's floor reaches the window's start, and one inside it, in a whole hour. */
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, now.AddHours(-30), "QOLD", 1_000, 1_000, 7, 1);
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, now.AddHours(-3), "QNEW", 4_000_000, 6_000_000, 11, 3);
            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, now.AddHours(-30), "POLD", 1_000, 1_000, 7, 1);
            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, now.AddHours(-3), "PNEW", 4_000_000, 6_000_000, 11, 3);
            foreach (var view in new[] { TimescaleSupport.QueryStatsIoHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView })
            {
                await RefreshAsync(connection, ct, view, now.AddHours(-32), now.AddHours(-1));
            }

            await using var fleetSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var fleet = JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(fleetSource, null, 24, null, 25, null, null, ct));
            foreach (var name in new[] { "plan_cache", "procedures" })
            {
                var section = Section(fleet, name);
                Assert.Equal("hourly", section.GetProperty("tier").GetString());
                Assert.Contains("hourly rollup", section.GetProperty("source").GetString(), StringComparison.Ordinal);
                Assert.Equal(JsonValueKind.String, section.GetProperty("window_end").ValueKind);
                Assert.False(string.IsNullOrEmpty(section.GetProperty("window_edges_note").GetString()));
                Assert.Equal(4_000, section.GetProperty("fleet_total").GetProperty("cpu_ms").GetDouble());
            }

            await using var oneSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var one = JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(oneSource, ServerName, 24, null, 25, null, null, ct));
            foreach (var name in new[] { "plan_cache", "procedures" })
            {
                var section = Section(one, name);
                Assert.Equal("raw", section.GetProperty("tier").GetString());
                Assert.Equal(4_000, section.GetProperty("total").GetProperty("cpu_ms").GetDouble());
            }
        });

    /// <summary>The io hourly statement for the plan cache reads its measures by name: five different figures land on five different measures.</summary>
    [Fact]
    public Task ThePlanCacheHourlyStatement_ReadsEachMeasureByName() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await TagAsync(connection, ct, "A", "hash", TopRankingLiveTests.QueryHash("QHOUR"));
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, At, "QHOUR", 4_000_000, 6_000_000, 11, 3);
            await RefreshAsync(connection, ct, TimescaleSupport.QueryStatsIoHourlyView, WindowEnd.AddHours(-30), WindowEnd);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var facts = await DarlingQueryTagRollupReader.ReadFactsAsync(
                source, QueryTagRollupSql.PlanCacheHourlySql, QueryTagRollupSection.PlanCache, [ServerId], WindowEnd.AddHours(-24), WindowEnd, null, ct);
            var fact = Assert.Single(facts);
            Assert.NotNull(fact.TagId);
            Assert.Equal(new double[] { 4_000_000, 6_000_000, 11, 0, 3 }, fact.Values);
        });

    /// <summary>The io hourly statement for procedures, as above.</summary>
    [Fact]
    public Task TheProceduresHourlyStatement_ReadsEachMeasureByName() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await TagAsync(connection, ct, "A", "object", TopRankingLiveTests.Db + ".dbo." + TopRankingLiveTests.ProcName("PHOUR"));
            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, At, "PHOUR", 4_000_000, 6_000_000, 11, 3);
            await RefreshAsync(connection, ct, TimescaleSupport.ProcedureStatsIoHourlyView, WindowEnd.AddHours(-30), WindowEnd);

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var facts = await DarlingQueryTagRollupReader.ReadFactsAsync(
                source, QueryTagRollupSql.ProceduresHourlySql, QueryTagRollupSection.Procedures, [ServerId], WindowEnd.AddHours(-24), WindowEnd, null, ct);
            var fact = Assert.Single(facts);
            Assert.NotNull(fact.TagId);
            Assert.Equal(new double[] { 4_000_000, 6_000_000, 11, 0, 3 }, fact.Values);
        });

    private static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live query-tag routing tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live query-tag routing tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt), CultureInfo.InvariantCulture);
                Assert.Equal(0L, schedulers);
            });
        }
    }
}
