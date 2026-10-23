/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
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
/// #6234, round 3: the edges of a watch's windows and what the watch list says. A collection whose interval began before the window is
/// counted only for the part inside it; a PostgreSQL CPU sample taken inside the window counts even when it was collected after the window
/// ended; a watch taken over after its request opened stores its measures; a fallback to the top queries is noted once; and
/// <c>get_change_watches</c> filters on and describes every state and result a watch can have. Each fact runs on its own scratch database.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchWindowEdgeLiveTests
{
    private const int PgServerId = 31;
    private const string PgServerName = "pg-thirtyone";

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, c);
        foreach (var value in values)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task AddServerAsync(NpgsqlConnection c, int id, string name, string engine, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) VALUES ($1, $2, $2, true, $3, now(), now())",
            ct, id, name, engine);

    private static JsonNode Measure(JsonNode result, string name) =>
        result["measures"]!.AsArray().Single(m => (string)m!["measure"]! == name)!;

    private static async Task<JsonNode> ResultAsync(NpgsqlConnection c, CancellationToken ct) =>
        JsonNode.Parse((await TextAsync(c, "SELECT result_json FROM config.change_watches", ct))!)!;

    /// <summary>One wait collection of a SQL Server target: <paramref name="deltaMs"/> over <paramref name="intervalSeconds"/>, stored at <paramref name="at"/>.</summary>
    private static Task WaitRowAsync(NpgsqlConnection c, DateTime at, long deltaMs, int intervalSeconds, CancellationToken ct) =>
        SeedAsync(c, "wait_stats", new Dictionary<string, object>
        {
            ["server_id"] = ServerId, ["server_name"] = ServerName, ["collection_time"] = Naive(at), ["wait_type"] = "PAGEIOLATCH_SH",
            ["delta_wait_time_ms"] = deltaMs, ["sample_interval_seconds"] = intervalSeconds,
        }, ct);

    /// <summary>One wait collection of an Aurora target, in microseconds as the collector stores it.</summary>
    private static Task PgWaitRowAsync(NpgsqlConnection c, DateTime at, long deltaMs, int intervalSeconds, CancellationToken ct) =>
        SeedAsync(c, "pg_wait_stats", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(at),
            ["wait_type"] = "IO", ["wait_event"] = "DataFileRead", ["delta_waits"] = 100L, ["delta_wait_time_us"] = deltaMs * 1000, ["sample_interval_seconds"] = intervalSeconds,
        }, ct);

    private static async Task PgWaitsAsync(NpgsqlConnection c, DateTime start, double msPerSecond, CancellationToken ct)
    {
        for (var minute = 1; minute <= 60; minute++)
        {
            await PgWaitRowAsync(c, start.AddMinutes(minute), (long)(msPerSecond * 60), 60, ct);
        }
    }

    /// <summary>One CPU sample a minute; each is collected <paramref name="collectedLate"/> after the instant it was taken (a sample is never collected before it was taken).</summary>
    private static Task PgCpuSampleAsync(NpgsqlConnection c, DateTime sampleTime, double percent, TimeSpan collectedLate, CancellationToken ct) =>
        SeedAsync(c, "pg_cpu_utilization", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(sampleTime + collectedLate),
            ["sample_time"] = Naive(sampleTime), ["cpu_percent"] = percent,
        }, ct);

    [Fact]
    public async Task AWaitCollectionWhoseIntervalBeganBeforeTheWindow_IsCountedOnlyForTheTimeInsideIt()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);

            /* After a long gap the first collection carries an hour of waits, nearly all of them from before the change (2000 ms/s over 3600 s);
               the rest of the window is as quiet as the hour before it. Counted whole it reads about 1000 ms/s and opens a request. */
            await WaitRowAsync(h.Connection, change.AddMinutes(1), 3600L * 2000, 3600, ct);
            for (var minute = 2; minute <= 60; minute++)
            {
                await WaitRowAsync(h.Connection, change.AddMinutes(minute), 100L * 60, 60, ct);
            }

            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);
            await h.NewPass().EvaluateDueAsync(ct);

            var after = (double)Measure(await ResultAsync(h.Connection, ct), "total_wait_ms_per_second")["after"]!;
            Assert.InRange(after, 120, 145);
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task AnAuroraWaitCollectionWhoseIntervalBeganBeforeTheWindow_IsCountedOnlyForTheTimeInsideIt()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, PgServerId, PgServerName, "postgres", ct);
            var change = h.Now.AddMinutes(-90);
            await PgWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await PgWaitRowAsync(h.Connection, change.AddMinutes(1), 3600L * 2000, 3600, ct);
            for (var minute = 2; minute <= 60; minute++)
            {
                await PgWaitRowAsync(h.Connection, change.AddMinutes(minute), 100L * 60, 60, ct);
            }

            await h.Store.RecordChangesAsync(PgServerId, PgServerName,
                new[] { new ChangeWatchChange("pg_config:work_mem", "pg_config", change, "work_mem") }, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            var after = (double)Measure(await ResultAsync(h.Connection, ct), "total_wait_ms_per_second")["after"]!;
            Assert.InRange(after, 120, 145);
            Assert.Equal("flat", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task APostgresCpuSampleTakenInTheLastMinutesOfTheWindow_CountsEvenCollectedAfterTheWindowEnded()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, PgServerId, PgServerName, "postgres", ct);
            var change = h.Now.AddMinutes(-90);
            await PgWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await PgWaitsAsync(h.Connection, change, 100, ct);
            for (var minute = 1; minute <= 60; minute++)
            {
                await PgCpuSampleAsync(h.Connection, change.AddMinutes(-60 + minute), 20, TimeSpan.Zero, ct);

                /* The sample is collected 15 minutes after it was taken: the last 15 of the window are collected after it ended. */
                await PgCpuSampleAsync(h.Connection, change.AddMinutes(minute), minute <= 45 ? 20 : 90, TimeSpan.FromMinutes(15), ct);
            }

            await h.Store.RecordChangesAsync(PgServerId, PgServerName,
                new[] { new ChangeWatchChange("pg_config:random_page_cost", "pg_config", change, "random_page_cost") }, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            var cpu = Measure(await ResultAsync(h.Connection, ct), "avg_cpu_percent");
            Assert.Equal(37.5, (double)cpu["after"]!, 1);
            Assert.True((bool)cpu["moved"]!);
        });
    }

    [Fact]
    public async Task AWatchTakenOverAfterItsRequestOpened_StoresTheMeasures_NotAnEmptyResult()
    {
        await RunAsync(true, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await SeedWaitsAsync(h.Connection, change.AddMinutes(-60), 100, ct);
            await SeedWaitsAsync(h.Connection, change, 1000, ct);
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(change, "max degree of parallelism"), ct);
            await h.NewPass().EvaluateDueAsync(ct);
            var requestId = await TextAsync(h.Connection, "SELECT request_id::text FROM config.change_watches", ct);

            /* The claimer opened the request and died before it closed the watch, and nothing of its evaluation was kept. */
            await ExecAsync(h.Connection,
                "UPDATE config.change_watches SET state = 'evaluating', claimed_at = $1, closed_at = NULL, result = NULL, request_id = NULL, result_json = NULL", ct, h.Now.AddMinutes(-30));
            await h.NewPass().EvaluateDueAsync(ct);

            Assert.Equal(requestId, await TextAsync(h.Connection, "SELECT request_id::text FROM config.change_watches", ct));
            Assert.Equal("request_opened", await TextAsync(h.Connection, "SELECT result FROM config.change_watches", ct));
            var wait = Measure(await ResultAsync(h.Connection, ct), "total_wait_ms_per_second");
            Assert.True((bool)wait["moved"]!);
            Assert.Equal(1000, (double)wait["after"]!, 0);
        });
    }

    [Fact]
    public async Task APlanWatchThatFallsBackToTheTopQueries_SaysWhyOnceInItsNotes()
    {
        await RunAsync(false, async (h, ct) =>
        {
            var change = h.Now.AddMinutes(-90);
            await h.Store.RecordChangesAsync(ServerId, ServerName,
                new[] { new ChangeWatchChange("plan_force:snapshot:Sales:77:5", "plan_force", change, "plan 5 of query 77 in Sales: forced", "Sales", 77) }, ct);
            await h.NewPass().EvaluateDueAsync(ct);

            var result = await ResultAsync(h.Connection, ct);
            var notes = result["notes"]!.AsArray().Select(n => (string)n!).ToArray();
            Assert.Single(notes, n => n.Contains("no interval for query 77", StringComparison.Ordinal));
            Assert.Single(notes, n => n.Contains("measured instead", StringComparison.Ordinal));
            Assert.Single(result["not_measured"]!.AsArray(), n => ((string)n!["measure"]!).StartsWith("query_avg_duration_ms:Sales:77", StringComparison.Ordinal));
        });
    }

    private static async Task InsertWatchAsync(NpgsqlConnection c, DateTime firstChange, string state, string? result, CancellationToken ct) =>
        await ExecAsync(c,
            "INSERT INTO config.change_watches (watch_id, server_id, server_name, opened_at, first_change_at, last_change_at, window_ends_at, state, closed_at, result, claimed_at) " +
            "VALUES (gen_random_uuid(), $1, $2, $3, $3, $3, $4, $5, CASE WHEN $5 = 'closed' THEN $4 END, $6, CASE WHEN $5 = 'evaluating' THEN $3 END)",
            ct, PgServerId, PgServerName, firstChange, firstChange.AddMinutes(60), state, (object?)result ?? DBNull.Value);

    private static async Task<string[]> ListedAsync(NpgsqlDataSource source, string? state, CancellationToken ct)
    {
        var json = await DarlingMcpChangeWatchTools.GetChangeWatches(source, PgServerName, state, 50, ct);
        var parsed = JsonNode.Parse(json)!;
        Assert.Null(parsed["refusal"]);
        return parsed["watches"] is JsonArray watches
            ? watches.Select(w => (string)w!["state"]! + "/" + (string?)w["result"]).OrderBy(s => s, StringComparer.Ordinal).ToArray()
            : [];
    }

    [Fact]
    public async Task GetChangeWatches_FiltersOnEveryStateAndResult_AndDescribesEachOfThem()
    {
        await RunAsync(false, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, PgServerId, PgServerName, "postgres", ct);
            var t = h.Now.AddDays(-2);
            await InsertWatchAsync(h.Connection, t.AddHours(1), "open", null, ct);
            await InsertWatchAsync(h.Connection, t.AddHours(2), "evaluating", null, ct);
            await InsertWatchAsync(h.Connection, t.AddHours(3), "closed", ChangeWatchStore.ResultFlat, ct);
            await InsertWatchAsync(h.Connection, t.AddHours(4), "closed", ChangeWatchStore.ResultAgentChange, ct);
            await InsertWatchAsync(h.Connection, t.AddHours(5), "closed", ChangeWatchStore.ResultRequestOpened, ct);

            Assert.Equal(["evaluating/"], await ListedAsync(h.Source, "evaluating", ct));
            Assert.Equal(["open/"], await ListedAsync(h.Source, "open", ct));
            Assert.Equal(["closed/flat", "closed/moved_agent_change", "closed/request_opened"], await ListedAsync(h.Source, "closed", ct));
            Assert.Equal(["closed/moved_agent_change"], await ListedAsync(h.Source, ChangeWatchStore.ResultAgentChange, ct));
            Assert.Equal(5, (await ListedAsync(h.Source, null, ct)).Length);
            /* #6561: the answer echoes "all" for no filter, so "all" (any case) is accepted and lists the same watches as omitting state. */
            Assert.Equal(await ListedAsync(h.Source, null, ct), await ListedAsync(h.Source, "all", ct));
            Assert.Equal(5, (await ListedAsync(h.Source, "ALL", ct)).Length);

            /* A watch that failed the way the worker fails one: claimed and failed MaxAttempts times, then closed by FailAttemptAsync (state and result both 'failed'). */
            await InsertWatchAsync(h.Connection, t.AddHours(6), "evaluating", null, ct);
            var failing = Guid.Parse((await TextAsync(h.Connection, "SELECT watch_id::text FROM config.change_watches ORDER BY first_change_at DESC LIMIT 1", ct))!);
            await ExecAsync(h.Connection, "UPDATE config.change_watches SET attempts = $2 WHERE watch_id = $1", ct, failing, (short)ChangeWatchStore.MaxAttempts);
            Assert.True(await h.Store.FailAttemptAsync(failing, "the read timed out", ct));

            Assert.Equal(["failed/failed"], await ListedAsync(h.Source, "failed", ct));
            Assert.Equal(["evaluating/"], await ListedAsync(h.Source, "evaluating", ct));
            Assert.Equal(["closed/flat", "closed/moved_agent_change", "closed/request_opened"], await ListedAsync(h.Source, "closed", ct));
            Assert.Equal(6, (await ListedAsync(h.Source, null, ct)).Length);
            Assert.Contains("the read timed out",
                await DarlingMcpChangeWatchTools.GetChangeWatches(h.Source, PgServerName, ChangeWatchStore.StateFailed, 50, ct), StringComparison.Ordinal);
            Assert.Contains("Invalid state", await DarlingMcpChangeWatchTools.GetChangeWatches(h.Source, PgServerName, "stuck", 50, ct), StringComparison.Ordinal);
            Assert.Contains("Invalid state", await DarlingMcpChangeWatchTools.GetChangeWatches(h.Source, PgServerName, "everything", 50, ct), StringComparison.Ordinal);
        });

        /* Every value the filter accepts is explained in what the tool tells a caller. */
        var method = typeof(DarlingMcpChangeWatchTools).GetMethod(nameof(DarlingMcpChangeWatchTools.GetChangeWatches))!;
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
        var state = method.GetParameters().Single(p => p.Name == "state").GetCustomAttribute<DescriptionAttribute>()!.Description;
        foreach (var value in DarlingMcpChangeWatchTools.StateFilters)
        {
            Assert.Contains(value, description, StringComparison.Ordinal);
            Assert.Contains(value, state, StringComparison.Ordinal);
        }
    }
}
