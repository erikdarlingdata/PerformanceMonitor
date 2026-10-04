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

    private static async Task CleanupAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var ids = $"{ServerA}, {ServerB}, {ServerC}";
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM job_history WHERE server_id IN ({ids})");
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
