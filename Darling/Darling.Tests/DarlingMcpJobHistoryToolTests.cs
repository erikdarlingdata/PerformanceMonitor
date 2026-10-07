/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <c>get_job_history</c>: the tool surface, its refusals (which run before any store read), and, on a seeded
/// store with fixed anchors, that the tool, the Storage reader, the WPF viewer's read and <c>/api/read</c> agree.
/// </summary>
public sealed class DarlingMcpJobHistoryToolSurfaceTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpJobTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IncludesGetJobHistory_StaticAndStringReturning()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_job_history");
        Assert.True(method.IsStatic);
        Assert.Equal(typeof(Task<string>), method.ReturnType);
    }

    [Fact]
    public void ParamContract_ServerOptional_FilterAndWindowArguments()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_job_history");
        var described = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .ToArray();

        Assert.Equal(
            new[] { "server_name", "hours_back", "job_name", "status", "category", "limit", "as_of" },
            described.Select(p => p.Name).ToArray());
        Assert.All(described, p => Assert.True(p.HasDefaultValue, $"{p.Name} must be optional"));
    }

    [Fact]
    public void TheAgentStateIsJudgedAgainstTheAlertsLiveStalenessSetting_NotTheShippedDefault()
    {
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingJobReader.cs").ReplaceLineEndings("\n");
        var start = reader.IndexOf("internal static AgentState ResolveAgentState(", StringComparison.Ordinal);
        Assert.True(start > 0, "ResolveAgentState is gone");
        var body = reader[start..reader.IndexOf("\n    }\n", start, StringComparison.Ordinal)];
        Assert.Contains("TimeSpan staleWindow", body, StringComparison.Ordinal);
        Assert.Contains(">= staleWindow", body, StringComparison.Ordinal);
        Assert.DoesNotContain("StaleWindow", body.Replace("staleWindow", ""), StringComparison.Ordinal);
        Assert.DoesNotContain("FromMinutes", body, StringComparison.Ordinal);
        Assert.DoesNotContain("AddMinutes", body, StringComparison.Ordinal);

        /* The window is the setting the engine reads (DarlingAlertSettings.CollectionStaleMinutes), with its clamp. */
        Assert.Equal("SELECT collection_stale_minutes FROM config_alert_settings WHERE id = 1", DarlingJobReader.CollectionStaleMinutesSql);
        var read = reader[reader.IndexOf("ReadLatestAgentStatesAsync(", StringComparison.Ordinal)..];
        Assert.Contains("var staleWindow = await ReadStaleWindowAsync(postgres, cancellationToken);", read, StringComparison.Ordinal);
        var settings = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs").ReplaceLineEndings("\n");
        Assert.Contains("CollectionStaleMinutes => Math.Clamp(_config.Alerts.CollectionStaleMinutes, 5, 1440);", settings, StringComparison.Ordinal);
        Assert.Equal(5, DarlingJobReader.StaleMinutesMin);
        Assert.Equal(1440, DarlingJobReader.StaleMinutesMax);
    }

    [Fact]
    public void TheStaleWindowIsTheStoredSettingClamped_AndTheShippedDefaultWhenNoneIsStored()
    {
        Assert.Equal(TimeSpan.FromMinutes(60), DarlingJobReader.StaleWindowFor(60));
        Assert.Equal(TimeSpan.FromMinutes(5), DarlingJobReader.StaleWindowFor(0));
        Assert.Equal(TimeSpan.FromMinutes(1440), DarlingJobReader.StaleWindowFor(99999));
        Assert.Equal(DarlingSelfAlertEvaluator.StaleWindow, DarlingJobReader.StaleWindowFor(null));
    }

    [Fact]
    public void AStaleSnapshotReadsAsUnknown_AndAFreshOneKeepsItsState()
    {
        var now = new DateTime(2026, 3, 11, 12, 0, 0, DateTimeKind.Utc);
        var window = TimeSpan.FromMinutes(60);
        var next = now.AddHours(1);

        var fresh = DarlingJobReader.ResolveAgentState("s", false, "Stopped", next, now - window + TimeSpan.FromSeconds(1), now, window);
        Assert.False(fresh.AgentRunning);
        Assert.Equal("Stopped", fresh.AgentStatusDesc);
        Assert.Equal(next, fresh.NextRunUtc);

        var atTheWindow = DarlingJobReader.ResolveAgentState("s", true, "Running", next, now - window, now, window);
        Assert.Null(atTheWindow.AgentRunning);
        Assert.Equal(DarlingJobReader.AgentUnknownDescription, atTheWindow.AgentStatusDesc);
        Assert.Null(atTheWindow.NextRunUtc);

        var stoppedAndStale = DarlingJobReader.ResolveAgentState("s", false, "Stopped", null, now - window - TimeSpan.FromHours(1), now, window);
        Assert.Null(stoppedAndStale.AgentRunning);

        /* The same 45-minute-old stopped row: inside a 60-minute window it is stopped, inside the shipped 30 it is unknown. */
        var age = now - TimeSpan.FromMinutes(45);
        Assert.False(DarlingJobReader.ResolveAgentState("s", false, "Stopped", null, age, now, TimeSpan.FromMinutes(60)).AgentRunning);
        Assert.Null(DarlingJobReader.ResolveAgentState("s", false, "Stopped", null, age, now, DarlingSelfAlertEvaluator.StaleWindow).AgentRunning);
    }

    [Fact]
    public void AFreshRowWithNoAgentServiceIsNotStopped_ButAStoppedOneIs()
    {
        var now = new DateTime(2026, 3, 11, 12, 0, 0, DateTimeKind.Utc);
        var window = TimeSpan.FromMinutes(30);
        var captured = now - TimeSpan.FromMinutes(1);

        /* The collector stores agent_running = 0 with NULL descriptions when sys.dm_server_services has no Agent row. */
        var none = DarlingJobReader.ResolveAgentState("s", false, null, null, captured, now, window);
        Assert.Null(none.AgentRunning);
        Assert.Equal("no SQL Agent service found", none.AgentStatusDesc);
        Assert.Equal("no SQL Agent service found", DarlingJobReader.NoAgentServiceDescription);

        var stopped = DarlingJobReader.ResolveAgentState("s", false, "Stopped", null, captured, now, window);
        Assert.False(stopped.AgentRunning);
        Assert.Equal("Stopped", stopped.AgentStatusDesc);

        /* A stale no-service row is just stale. */
        Assert.Equal(DarlingJobReader.AgentUnknownDescription,
            DarlingJobReader.ResolveAgentState("s", false, null, null, now - window, now, window).AgentStatusDesc);
    }

    [Fact]
    public async Task AnUnknownStatus_IsRefusedByName_BeforeAnyStoreRead()
    {
        var body = await DarlingMcpJobTools.GetJobHistory(null!, status: "Exploded");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(body));
        using var doc = JsonDocument.Parse(body);
        var message = doc.RootElement.GetProperty("message").GetString()!;
        Assert.Contains("Invalid status value 'Exploded'", message, StringComparison.Ordinal);
        Assert.Contains("Failed, Succeeded, Retry, Canceled", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABadWindow_IsRefused_BeforeAnyStoreRead()
    {
        var hours = await DarlingMcpJobTools.GetJobHistory(null!, hours_back: 0);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(hours));
        var asOf = await DarlingMcpJobTools.GetJobHistory(null!, as_of: "not-a-date");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(asOf));
        var limit = await DarlingMcpJobTools.GetJobHistory(null!, limit: 0);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(limit));
    }
}

[Collection("live-postgres")]
public sealed class DarlingMcpJobHistoryToolLiveTests
{
    private const string NameA = "jobhist-alpha";
    private const string NameB = "jobhist-bravo";
    private const string NameC = "jobhist-charlie";
    private static readonly int ServerA = ServerIdHelper.GetDeterministicHashCode(NameA);
    private static readonly int ServerB = ServerIdHelper.GetDeterministicHashCode(NameB);
    private static readonly int ServerC = ServerIdHelper.GetDeterministicHashCode(NameC);

    /* Fixed anchors: the window is 2026-03-10 00:00Z to 2026-03-11 00:00Z, whatever today is. */
    private static readonly DateTime AsOf = new(2026, 3, 11, 0, 0, 0, DateTimeKind.Utc);
    private const string AsOfText = "2026-03-11T00:00:00Z";
    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private async Task<(NpgsqlConnection Connection, NpgsqlDataSource Postgres)> OpenAsync(CancellationToken ct)
    {
        var connection = new NpgsqlConnection(Cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);
        return (connection, NpgsqlDataSource.Create(Cs!));
    }

    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, ServerA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerB, NameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerC, NameC, ct);
        /* Alpha keeps local time at UTC-5; the others have no offset row and read as UTC. */
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES ($1,$2,$3,$4,$5)",
            CollectionIdGenerator.Next(), new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Unspecified), ServerA, NameA, -300);

        /* Alpha: local 08:00 is 13:00 UTC. */
        await InsertAsync(c, ct, 1, ServerA, NameA, "Nightly ETL", "Data Maintenance", 0, 1, 600, new DateTime(2026, 3, 10, 8, 0, 0), null);
        await InsertAsync(c, ct, 2, ServerA, NameA, "Nightly ETL", "Data Maintenance", 1, 0, 30, new DateTime(2026, 3, 10, 9, 0, 0), "step blew up");
        await InsertAsync(c, ct, 3, ServerA, NameA, "Nightly ETL", "Data Maintenance", 0, 0, 90, new DateTime(2026, 3, 10, 9, 0, 0), "Job failed");
        await InsertAsync(c, ct, 4, ServerA, NameA, "Index Rebuild", "Maintenance", 0, 1, 45, new DateTime(2026, 3, 10, 10, 0, 0), null);
        await InsertAsync(c, ct, 5, ServerB, NameB, "Log Backup", "Backup", 0, 1, 12, new DateTime(2026, 3, 10, 14, 0, 0), null);
        await InsertAsync(c, ct, 6, ServerB, NameB, "Nightly ETL", "Data Maintenance", 0, 1, 70, new DateTime(2026, 3, 10, 15, 0, 0), null);
        /* Before the window: must not appear. */
        await InsertAsync(c, ct, 7, ServerA, NameA, "Old Job", "Maintenance", 0, 1, 5, new DateTime(2026, 3, 9, 1, 0, 0), null);
    }

    private static Task InsertAsync(NpgsqlConnection c, CancellationToken ct, long id, int serverId, string serverName, string job,
        string category, int stepId, int status, long seconds, DateTime localRun, string? message) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                                       category_name, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds,
                                       retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,$14,0,$15)",
            4_843_100_000L + id, new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Unspecified), serverId, serverName, 4_843_100_000L + id,
            "job-" + job.Replace(' ', '-'), job, category, stepId, stepId == 0 ? "(Job outcome)" : "Step " + stepId.ToString(CultureInfo.InvariantCulture),
            status, status == 1 ? "The job succeeded." : "The job failed.", localRun, seconds, message);

    /* The alert's staleness setting as found before a test moved it (null: no settings row existed); restored by CleanupAsync. */
    private static (bool Recorded, int? Original) s_staleSetting;

    private static async Task SetStaleMinutesAsync(NpgsqlConnection c, CancellationToken ct, int minutes)
    {
        if (!s_staleSetting.Recorded)
        {
            await using var read = new NpgsqlCommand("SELECT collection_stale_minutes FROM config_alert_settings WHERE id = 1", c);
            var value = await read.ExecuteScalarAsync(ct);
            s_staleSetting = (true, value is null or DBNull ? null : Convert.ToInt32(value, CultureInfo.InvariantCulture));
        }

        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE config_alert_settings SET collection_stale_minutes = $1 WHERE id = 1", minutes);
    }

    private static async Task CleanupAsync(NpgsqlConnection c, CancellationToken ct)
    {
        if (s_staleSetting.Recorded)
        {
            if (s_staleSetting.Original is { } original)
                await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE config_alert_settings SET collection_stale_minutes = $1 WHERE id = 1", original);
            else
                await DarlingMcpTestData.ExecAsync(c, ct, "DELETE FROM config_alert_settings WHERE id = 1");
            s_staleSetting = default;
        }

        var ids = $"{ServerA}, {ServerB}, {ServerC}";
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM job_history WHERE server_id IN ({ids})");
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM agent_status WHERE server_id IN ({ids})");
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM server_properties WHERE server_id IN ({ids})");
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM servers WHERE server_id IN ({ids})");
    }

    private static async Task<(int Status, string Body)> WebGetAsync(NpgsqlDataSource postgres, string pathAndQuery, CancellationToken ct)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        await using var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, postgres, new CollectorRuntimeState(), new CapturingTestLogger());
        await app.StartAsync(ct);
        using var server = app.GetTestServer();
        var path = pathAndQuery.Split('?', 2);
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = path[0];
            request.Request.QueryString = new QueryString("?" + path[1]);
            request.Request.Headers.Host = "localhost";
        });
        using var reader = new StreamReader(context.Response.Body);
        return (context.Response.StatusCode, await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task ViewerRows_EqualTheStorageReaderRows_MappedThroughFrom()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            var since = AsOf.AddHours(-24);

            await using var viewer = new ViewerDataService(Cs!);
            var viewerRows = await viewer.GetJobHistoryAsync(since, null, 2000, ct);
            var storage = await DarlingJobHistoryReader.GetAsync(postgres, since, null, 2000, 60, cancellationToken: ct);

            var expected = storage.Select(ViewerJobHistoryRow.From).ToList();
            Assert.Equal(6 + 0, viewerRows.Count(r => r.ServerId == ServerA || r.ServerId == ServerB));
            Assert.Equal(JsonSerializer.Serialize(expected), JsonSerializer.Serialize(viewerRows));
            /* Alpha's local 08:00 is 13:00 UTC: the conversion survived the move. */
            Assert.Contains(viewerRows, r => r.ServerId == ServerA && r.RunDateTimeUtc == new DateTime(2026, 3, 10, 13, 0, 0));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task Tool_RowsEqualTheStorageReader_WithTheWpfColumnsInUtc()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            var body = await DarlingMcpJobTools.GetJobHistory(postgres, NameA, 24, as_of: AsOfText, cancellationToken: ct);
            var storage = await DarlingJobHistoryReader.GetAsync(postgres, AsOf.AddHours(-24), ServerA, 101, 60, cancellationToken: ct);

            using var doc = JsonDocument.Parse(body);
            var runs = doc.RootElement.GetProperty("runs").EnumerateArray().ToList();
            Assert.Equal(storage.Count, runs.Count);
            Assert.Equal(4, runs.Count);
            Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
            Assert.Equal(NameA, doc.RootElement.GetProperty("server").GetString());
            for (var i = 0; i < runs.Count; i++)
            {
                Assert.Equal(DateTime.SpecifyKind(storage[i].RunDateTimeUtc!.Value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
                    runs[i].GetProperty("run_time").GetString());
                Assert.Equal(storage[i].JobName, runs[i].GetProperty("job_name").GetString());
                Assert.Equal(storage[i].CategoryName, runs[i].GetProperty("category").GetString());
                Assert.Equal(storage[i].RunDurationSeconds, runs[i].GetProperty("duration_seconds").GetInt64());
                Assert.Equal(storage[i].ServerName, runs[i].GetProperty("server").GetString());
            }

            Assert.EndsWith("Z", runs[0].GetProperty("run_time").GetString(), StringComparison.Ordinal);
            Assert.Equal("2026-03-10T15:00:00.0000000Z", runs[0].GetProperty("run_time").GetString());
            Assert.Equal("Failed", runs.First(r => r.GetProperty("job_name").GetString() == "Nightly ETL" && r.GetProperty("step").GetString() == "1: Step 1").GetProperty("status").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task APastAsOf_ReadsTheRunsBeforeIt_WhateverIsStoredAfterIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            /* More runs after as_of than any fetch limit: 2100 one-second successful outcomes of the same job, local
               15:01 onward on alpha (UTC-5), so 20:01Z onward - all inside the hour after as_of 20:00Z, where a widened
               end bound would fill the whole fetch with them. A successful run exactly AT as_of (local 15:00) is seeded too. */
            await InsertAsync(c, ct, 8, ServerA, NameA, "Nightly ETL", "Data Maintenance", 0, 1, 600, new DateTime(2026, 3, 10, 15, 0, 0), null);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                                           category_name, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds,
                                           retries_attempted, message)
                  SELECT 4843200000 + g, $1, $2, $3, 4843200000 + g, 'job-Nightly-ETL', 'Nightly ETL', true,
                         'Data Maintenance', 0, '(Job outcome)', 1, 'The job succeeded.', $4 + g * interval '1 minute', 1, 0, NULL
                  FROM generate_series(1, 2100) AS g",
                new DateTime(2026, 3, 10, 12, 0, 0, DateTimeKind.Unspecified), ServerA, NameA, new DateTime(2026, 3, 10, 15, 0, 0, DateTimeKind.Unspecified));

            const string pastAsOf = "2026-03-10T20:00:00Z";
            var limited = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, 24, limit: 3, as_of: pastAsOf, cancellationToken: ct)).RootElement;
            Assert.Equal(3, limited.GetProperty("shown").GetInt32());
            Assert.True(limited.GetProperty("truncated").GetBoolean());
            Assert.All(limited.GetProperty("runs").EnumerateArray(),
                r => Assert.True(DateTime.Parse(r.GetProperty("run_time").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal) <= new DateTime(2026, 3, 10, 20, 0, 0, DateTimeKind.Utc)));

            var all = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, 24, limit: 10, as_of: pastAsOf, cancellationToken: ct)).RootElement;
            Assert.Equal(5, all.GetProperty("shown").GetInt32());
            Assert.False(all.GetProperty("truncated").GetBoolean());
            foreach (var run in all.GetProperty("runs").EnumerateArray().Where(r => r.GetProperty("job_name").GetString() == "Nightly ETL"))
            {
                /* The success stats stop at as_of too, inclusive: the success exactly at 20:00Z is the last one. */
                Assert.Equal(new DateTime(2026, 3, 10, 20, 0, 0, DateTimeKind.Utc), DateTime.Parse(run.GetProperty("last_success").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal));
                Assert.False(run.GetProperty("is_long_running").GetBoolean());
            }

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task AnAzureSqlDatabaseServerWithNoRuns_AnswersNotCollected_NotEmpty()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET sql_engine_edition = 5 WHERE server_id = $1", ServerC);
            var body = await DarlingMcpJobTools.GetJobHistory(postgres, NameC, as_of: AsOfText, cancellationToken: ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(body));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task Filters_AndFleetMode_AndLimit()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);

            /* No server_name: every server, and the job filter keeps both servers' ETL runs. */
            var fleet = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, job_name: "nightly etl", as_of: AsOfText, cancellationToken: ct));
            Assert.Equal(4, fleet.RootElement.GetProperty("runs").GetArrayLength());
            Assert.Equal(
                new[] { NameA, NameB },
                fleet.RootElement.GetProperty("runs").EnumerateArray().Select(r => r.GetProperty("server").GetString()!).Distinct().OrderBy(s => s, StringComparer.Ordinal).ToArray());

            var failed = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, status: "failed", as_of: AsOfText, cancellationToken: ct));
            Assert.Equal(2, failed.RootElement.GetProperty("runs").GetArrayLength());

            var category = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, category: "backup", as_of: AsOfText, cancellationToken: ct));
            Assert.Equal("Log Backup", Assert.Single(category.RootElement.GetProperty("runs").EnumerateArray()).GetProperty("job_name").GetString());

            var limited = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, limit: 2, as_of: AsOfText, cancellationToken: ct));
            Assert.Equal(2, limited.RootElement.GetProperty("runs").GetArrayLength());
            Assert.True(limited.RootElement.GetProperty("truncated").GetBoolean());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task UnknownServer_Refused_AndAServerWithNoRuns_IsEmpty()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            var unknown = await DarlingMcpJobTools.GetJobHistory(postgres, "no-such-server", as_of: AsOfText, cancellationToken: ct);
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(unknown));
            Assert.Contains("Could not resolve", unknown, StringComparison.Ordinal);
            Assert.DoesNotContain("\"runs\"", unknown, StringComparison.Ordinal);

            var empty = await DarlingMcpJobTools.GetJobHistory(postgres, NameC, as_of: AsOfText, cancellationToken: ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(empty));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    /* The snapshot's age is its age at the anchor these tests read (AsOf), not at the clock: the tool reads the Agent as of the
       window's end (#5242), so a seed hung off the clock would sit after a past as_of and never be read. */
    private static Task SeedAgentAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, bool? running, string? desc, TimeSpan age, DateTime? nextLocal = null) =>
        SeedAgentAtAsync(c, ct, serverId, serverName, running, desc, AsOf - age, nextLocal);

    /* The same insert at an absolute instant (naive UTC, as the collector stores it). */
    private static Task SeedAgentAtAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, bool? running, string? desc, DateTime capturedUtc, DateTime? nextLocal = null) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO agent_status (collection_id, collection_time, server_id, server_name, agent_running, agent_status_desc, agent_startup_desc, next_scheduled_run)
              VALUES ($1,$2,$3,$4,$5,$6,'Automatic',$7)",
            CollectionIdGenerator.Next(), DateTime.SpecifyKind(capturedUtc, DateTimeKind.Unspecified), serverId, serverName,
            (object?)running ?? DBNull.Value, (object?)desc ?? DBNull.Value, (object?)nextLocal ?? DBNull.Value);

    [Fact]
    public async Task ANamedServer_CarriesItsAgentStateFromTheNewestSnapshot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            /* Alpha (UTC-5): an older running row, then the newest says stopped, with a next run at local 21:00 (02:00 UTC the next day).
               Bravo: running, with a next run at local 21:00 (UTC, so unconverted). */
            await SeedAgentAsync(c, ct, ServerA, NameA, true, "Running", TimeSpan.FromMinutes(12));
            await SeedAgentAsync(c, ct, ServerA, NameA, false, "Stopped", TimeSpan.FromMinutes(2), new DateTime(2026, 3, 11, 21, 0, 0));
            await SeedAgentAsync(c, ct, ServerB, NameB, true, "Running", TimeSpan.FromMinutes(1), new DateTime(2026, 3, 11, 21, 0, 0));

            var stopped = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.False(stopped.GetProperty("agent_running").GetBoolean());
            Assert.Equal("Stopped", stopped.GetProperty("agent_status_desc").GetString());
            Assert.Equal("2026-03-12T02:00:00.0000000Z", stopped.GetProperty("next_run").GetString());
            Assert.EndsWith("Z", stopped.GetProperty("captured_at").GetString(), StringComparison.Ordinal);
            Assert.Equal(4, stopped.GetProperty("runs").GetArrayLength());

            var running = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameB, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.True(running.GetProperty("agent_running").GetBoolean());
            Assert.Equal("Running", running.GetProperty("agent_status_desc").GetString());
            Assert.Equal("2026-03-11T21:00:00.0000000Z", running.GetProperty("next_run").GetString());
            Assert.Equal(2, running.GetProperty("runs").GetArrayLength());
            /* A named server's answer carries the flat fields, never the fleet's counts. */
            Assert.False(running.TryGetProperty("agents_total", out var absent));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task ASnapshotOlderThanTheStalenessWindow_ReadsUnknown_NeverItsLastValue()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SetStaleMinutesAsync(c, ct, 30);
            var stale = DarlingSelfAlertEvaluator.StaleWindow + TimeSpan.FromMinutes(5);
            await SeedAgentAsync(c, ct, ServerA, NameA, false, "Stopped", stale, AsOf.AddHours(1));
            await SeedAgentAsync(c, ct, ServerB, NameB, true, "Running", stale);

            foreach (var name in new[] { NameA, NameB })
            {
                var body = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, name, as_of: AsOfText, cancellationToken: ct)).RootElement;
                Assert.Equal(JsonValueKind.Null, body.GetProperty("agent_running").ValueKind);
                Assert.Equal("unknown (no recent status)", body.GetProperty("agent_status_desc").GetString());
                Assert.Equal(JsonValueKind.Null, body.GetProperty("next_run").ValueKind);
            }

            /* A server with no snapshot at all is unknown too, with no capture time (its answer is empty, so the state is under hints). */
            var none = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameC, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", none.GetProperty("status").GetString());
            var hints = none.GetProperty("hints");
            Assert.Equal(JsonValueKind.Null, hints.GetProperty("agent_running").ValueKind);
            Assert.Equal("unknown (no recent status)", hints.GetProperty("agent_status_desc").GetString());
            Assert.Equal(JsonValueKind.Null, hints.GetProperty("captured_at").ValueKind);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task TheFleetAnswerListsEachServersAgent_AndAnEmptyAnswerCarriesItUnderHints()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SetStaleMinutesAsync(c, ct, 30);
            await SeedAgentAsync(c, ct, ServerA, NameA, false, "Stopped", TimeSpan.FromMinutes(3));
            await SeedAgentAsync(c, ct, ServerB, NameB, true, "Running", TimeSpan.FromMinutes(3));
            await SeedAgentAsync(c, ct, ServerC, NameC, true, "Running", DarlingSelfAlertEvaluator.StaleWindow + TimeSpan.FromMinutes(1));

            var fleet = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.False(fleet.TryGetProperty("agent_running", out var flat));
            /* Counts cover every enabled server with a snapshot (other tests' servers may be present); the detail names this test's. */
            Assert.True(fleet.GetProperty("agents_total").GetInt32() >= 3);
            Assert.True(fleet.GetProperty("agents_running").GetInt32() >= 1);
            var notRunning = fleet.GetProperty("agents_not_running").EnumerateArray()
                .Where(a => a.GetProperty("server").GetString()!.StartsWith("jobhist-", StringComparison.Ordinal)).ToList();
            Assert.Equal(new[] { NameA, NameC }, notRunning.Select(a => a.GetProperty("server").GetString()!).ToArray());
            Assert.Equal(JsonValueKind.False, notRunning[0].GetProperty("agent_running").ValueKind);
            Assert.Equal("Stopped", notRunning[0].GetProperty("agent_status_desc").GetString());
            Assert.Equal(JsonValueKind.Null, notRunning[1].GetProperty("agent_running").ValueKind);
            Assert.Equal("unknown (no recent status)", notRunning[1].GetProperty("agent_status_desc").GetString());
            Assert.DoesNotContain(NameB, fleet.GetProperty("agents_not_running").ToString(), StringComparison.Ordinal);
            Assert.Equal(6, fleet.GetProperty("runs").GetArrayLength());

            /* An empty window still says what the Agent is doing: that is when "no jobs ran" needs the reason. */
            var empty = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, job_name: "no such job", as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", empty.GetProperty("status").GetString());
            var hints = empty.GetProperty("hints");
            Assert.False(hints.GetProperty("agent_running").GetBoolean());
            Assert.Equal("Stopped", hints.GetProperty("agent_status_desc").GetString());
            Assert.True(hints.TryGetProperty("window_truncated", out var windowTruncatedHint));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task TheAgentIsJudgedAgainstTheAlertsLiveStalenessSetting()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SeedAgentAsync(c, ct, ServerA, NameA, false, "Stopped", TimeSpan.FromMinutes(45), AsOf.AddHours(1));

            /* The shipped 30 minutes: a 45-minute-old row says nothing about the Agent at the anchor. */
            await SetStaleMinutesAsync(c, ct, 30);
            var thirty = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, thirty.GetProperty("agent_running").ValueKind);
            Assert.Equal("unknown (no recent status)", thirty.GetProperty("agent_status_desc").GetString());

            /* At 60 minutes the Agent Not Running alert fires on that same row, so the tool reports it stopped. */
            await SetStaleMinutesAsync(c, ct, 60);
            var sixty = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.False(sixty.GetProperty("agent_running").GetBoolean());
            Assert.Equal("Stopped", sixty.GetProperty("agent_status_desc").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task AServerWithNoAgentService_IsNotStopped_NamedOrInTheFleet()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SetStaleMinutesAsync(c, ct, 30);
            /* Bravo has no Agent service: the collector stores agent_running = false with NULL descriptions. Alpha's is really stopped. */
            await SeedAgentAsync(c, ct, ServerA, NameA, false, "Stopped", TimeSpan.FromMinutes(2));
            await SeedAgentAsync(c, ct, ServerB, NameB, false, null, TimeSpan.FromMinutes(2));

            var named = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameB, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, named.GetProperty("agent_running").ValueKind);
            Assert.Equal("no SQL Agent service found", named.GetProperty("agent_status_desc").GetString());

            var fleet = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, as_of: AsOfText, cancellationToken: ct)).RootElement;
            var listed = fleet.GetProperty("agents_not_running").EnumerateArray()
                .Where(a => a.GetProperty("server").GetString()!.StartsWith("jobhist-", StringComparison.Ordinal)).ToList();
            Assert.Equal(new[] { NameA, NameB }, listed.Select(a => a.GetProperty("server").GetString()!).ToArray());
            Assert.Equal(JsonValueKind.False, listed[0].GetProperty("agent_running").ValueKind);
            Assert.Equal(JsonValueKind.Null, listed[1].GetProperty("agent_running").ValueKind);
            Assert.Equal("no SQL Agent service found", listed[1].GetProperty("agent_status_desc").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task AFailedAgentRead_StillServesTheRuns_WithoutTheAgentFields()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SeedAgentAsync(c, ct, ServerA, NameA, false, "Stopped", TimeSpan.FromMinutes(2));
            DarlingJobReader.AgentReadFaultForTests.Value = new TimeoutException("the Agent read timed out");

            var named = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal(4, named.GetProperty("runs").GetArrayLength());
            Assert.False(named.TryGetProperty("agent_running", out var absent1));
            Assert.False(named.TryGetProperty("agent_status_desc", out var absent2));

            var fleet = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal(6, fleet.GetProperty("runs").GetArrayLength());
            Assert.False(fleet.TryGetProperty("agents_total", out var absent3));

            /* An empty answer keeps its own hints, without the Agent fields. */
            var empty = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, job_name: "no such job", as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", empty.GetProperty("status").GetString());
            Assert.True(empty.GetProperty("hints").TryGetProperty("window_truncated", out var absent4));
            Assert.False(empty.GetProperty("hints").TryGetProperty("agent_running", out var absent5));
            ok = true;
        }
        finally
        {
            DarlingJobReader.AgentReadFaultForTests.Value = null;
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    /* #5242: the Agent state follows the window's end. The row read is the newest snapshot at or before the anchor, judged
       against the anchor, so a past as_of answers "what was the Agent doing then" and leaves out what was collected after it.
       These cases hang their times off one instant each test fixes (whole seconds, so the stored microseconds round-trip
       exactly) rather than off the clock at the moment of the call. */
    private static DateTime FixedNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);
    }

    private static string AsOfOf(DateTime utc) => utc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private static string CapturedOf(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);

    /* An answer with no run in its window carries the Agent state under hints (the seeded runs are all in March, so a
       window that ends near now is empty). */
    private static JsonElement HintsOf(string body)
    {
        var root = JsonDocument.Parse(body).RootElement;
        Assert.Equal("empty", root.GetProperty("status").GetString());
        return root.GetProperty("hints");
    }

    [Fact]
    public async Task ANamedServer_ReadsItsAgentAsOfTheAnchor_NotItsNewestSnapshot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SetStaleMinutesAsync(c, ct, 30);
            /* Alpha's Agent was stopped 20 minutes ago and has been running again since 2 minutes ago. */
            var now = FixedNow();
            var stoppedAt = now.AddMinutes(-20);
            var runningAt = now.AddMinutes(-2);
            await SeedAgentAtAsync(c, ct, ServerA, NameA, false, "Stopped", stoppedAt, now.AddHours(1));
            await SeedAgentAtAsync(c, ct, ServerA, NameA, true, "Running", runningAt, now.AddHours(2));

            /* An anchor between the two reads the older snapshot: stopped, stamped with the older capture time. */
            var then = HintsOf(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, as_of: AsOfOf(now.AddMinutes(-10)), cancellationToken: ct));
            Assert.False(then.GetProperty("agent_running").GetBoolean());
            Assert.Equal("Stopped", then.GetProperty("agent_status_desc").GetString());
            Assert.Equal(CapturedOf(stoppedAt), then.GetProperty("captured_at").GetString());

            /* No as_of is the window ending now: the newer snapshot, running. */
            var current = HintsOf(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, cancellationToken: ct));
            Assert.True(current.GetProperty("agent_running").GetBoolean());
            Assert.Equal("Running", current.GetProperty("agent_status_desc").GetString());
            Assert.Equal(CapturedOf(runningAt), current.GetProperty("captured_at").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task AnAnchorBeforeEverySnapshot_ReadsUnknown_AndTheFleetLeavesTheServerOut()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SetStaleMinutesAsync(c, ct, 30);
            /* Both servers' only snapshots are 2 minutes old, so an as_of in March has nothing at or before it. */
            var now = FixedNow();
            await SeedAgentAtAsync(c, ct, ServerA, NameA, false, "Stopped", now.AddMinutes(-2), now.AddHours(1));
            await SeedAgentAtAsync(c, ct, ServerB, NameB, true, "Running", now.AddMinutes(-2));

            /* A named server reads unknown, with no capture time: there is no snapshot to name. */
            var named = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, NameA, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.Equal(4, named.GetProperty("runs").GetArrayLength());
            Assert.Equal(JsonValueKind.Null, named.GetProperty("agent_running").ValueKind);
            Assert.Equal("unknown (no recent status)", named.GetProperty("agent_status_desc").GetString());
            Assert.Equal(JsonValueKind.Null, named.GetProperty("next_run").ValueKind);
            Assert.Equal(JsonValueKind.Null, named.GetProperty("captured_at").ValueKind);

            /* The fleet counts servers that have a snapshot at or before the anchor: these two are not listed, as unknown or at all. */
            var fleetThen = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(postgres, as_of: AsOfText, cancellationToken: ct)).RootElement;
            Assert.DoesNotContain("jobhist-", fleetThen.GetProperty("agents_not_running").ToString(), StringComparison.Ordinal);
            var fleetNow = HintsOf(await DarlingMcpJobTools.GetJobHistory(postgres, cancellationToken: ct));
            Assert.True(fleetNow.GetProperty("agents_total").GetInt32() - fleetThen.GetProperty("agents_total").GetInt32() >= 2,
                "the two seeded servers are counted now and not at the March anchor");
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task TheFleetCountsAServerByItsAgentAsOfTheAnchor()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            await SetStaleMinutesAsync(c, ct, 30);
            /* Alpha was stopped 20 minutes ago and has been running since 2 minutes ago. Bravo's only snapshot is 2 minutes old. */
            var now = FixedNow();
            var stoppedAt = now.AddMinutes(-20);
            await SeedAgentAtAsync(c, ct, ServerA, NameA, false, "Stopped", stoppedAt);
            await SeedAgentAtAsync(c, ct, ServerA, NameA, true, "Running", now.AddMinutes(-2));
            await SeedAgentAtAsync(c, ct, ServerB, NameB, true, "Running", now.AddMinutes(-2));

            /* An anchor between Alpha's two snapshots counts Alpha as not running, with the older capture time. Bravo has no
               snapshot yet at the anchor, so it is left out of the counts rather than listed as unknown. */
            var then = HintsOf(await DarlingMcpJobTools.GetJobHistory(postgres, as_of: AsOfOf(now.AddMinutes(-10)), cancellationToken: ct));
            var notRunningThen = then.GetProperty("agents_not_running").EnumerateArray()
                .Where(a => a.GetProperty("server").GetString()!.StartsWith("jobhist-", StringComparison.Ordinal)).ToList();
            Assert.Equal(new[] { NameA }, notRunningThen.Select(a => a.GetProperty("server").GetString()!).ToArray());
            Assert.Equal(JsonValueKind.False, notRunningThen[0].GetProperty("agent_running").ValueKind);
            Assert.Equal("Stopped", notRunningThen[0].GetProperty("agent_status_desc").GetString());
            Assert.Equal(CapturedOf(stoppedAt), notRunningThen[0].GetProperty("captured_at").GetString());

            /* Now both are running: neither is listed, and the two running Agents are counted. */
            var current = HintsOf(await DarlingMcpJobTools.GetJobHistory(postgres, cancellationToken: ct));
            Assert.DoesNotContain("jobhist-", current.GetProperty("agents_not_running").ToString(), StringComparison.Ordinal);
            Assert.True(current.GetProperty("agents_running").GetInt32() - then.GetProperty("agents_running").GetInt32() >= 2,
                "Alpha and Bravo are running now; at the anchor Alpha was stopped and Bravo had no snapshot");
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    [Fact]
    public async Task ApiRead_ServesTheToolsBody()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;
        var (c, postgres) = await OpenAsync(ct);
        await using var _ = postgres;
        using var __ = c;
        var ok = false;
        try
        {
            await SeedAsync(c, ct);
            var tool = await DarlingMcpJobTools.GetJobHistory(postgres, NameA, 24, "Nightly ETL", "Failed", null, 10, AsOfText, ct);
            var (status, body) = await WebGetAsync(postgres,
                $"/api/read/get_job_history?server={NameA}&hours=24&job_name=Nightly%20ETL&status=Failed&limit=10&as_of={AsOfText}", ct);

            Assert.Equal(StatusCodes.Status200OK, status);
            using var expected = JsonDocument.Parse(tool);
            using var actual = JsonDocument.Parse(body);
            Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
            Assert.Equal(2, actual.RootElement.GetProperty("runs").GetArrayLength());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }
}
