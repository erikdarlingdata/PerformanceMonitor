/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for the slow-read record (#5097): the acceptance path (a slowed <c>get_query_store_top</c> call is read back
/// with <c>get_slow_reads</c>, arguments and statements intact), the writer's retention and row cap, and the read tool's
/// empty answer. Each fact mints its own scratch database.
/// </summary>
[Collection("live-postgres")]
public sealed class SlowReadsLiveTests
{
    private const string Token = "slow-read-pin-token";
    private const int ServerId = 5097001;
    private const string ServerName = "slow-read-pin";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = ServerName });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static SlowReadRecord Record(DateTime time, string route) => new(
        time, "mcp", route, "timeout", 9_000, null, null, null, "{}", false, null, null, "[]", 0, false, "57014");

    private static async Task<TestServer> BuildServerAsync(NpgsqlDataSource postgres, SlowReadLog slowReads)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        var sharedBaselines = new BaselineCache();
        builder.Services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(
            postgres, planFetcher: null, logger: NullLogger.Instance, baselineCache: sharedBaselines));
        builder.Services.AddSingleton<Microsoft.Extensions.Logging.ILogger>(NullLogger.Instance);

        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(), NullLogger.Instance, slowReads);

        var app = builder.Build();
        var host = new DarlingMcpHostService(NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
        host.ConfigurePipeline(app, networkMode: false, networkListenIp: null, allowedCidr: System.Net.IPNetwork.Parse("127.0.0.1/32"), bearerToken: Token);
        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<JsonObject> CallToolAsync(TestServer server, string tool, JsonObject arguments)
    {
        var request = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["id"] = 1,
            ["method"] = "tools/call",
            ["params"] = new JsonObject { ["name"] = tool, ["arguments"] = arguments },
        }.ToJsonString();

        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(request);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        using var reader = new System.IO.StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        var data = body.Split('\n').Select(l => l.TrimEnd('\r')).FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        var envelope = JsonNode.Parse(data is null ? body : data["data:".Length..].Trim())!.AsObject();
        var text = (string)envelope["result"]!["content"]![0]!["text"]!;
        return JsonNode.Parse(text)!.AsObject();
    }

    [Fact]
    public async Task ASlowedQueryStoreTopCall_IsRecordedWithItsArguments_SourceAndStatements_AndReadBackByGetSlowReads()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5097 slow-read live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await RegisterServerAsync(connection, ct);

        var bodySucceeded = false;
        var priorThreshold = SlowReadLog.ThresholdMs;
        var log = new SlowReadLog();
        using var writerStop = new CancellationTokenSource();
        Task? writer = null;
        TestServer? server = null;
        try
        {
            /* Program.cs registers the process listener; a test host does not. */
            ReadStatementCapture.Register();
            SlowReadLog.ThresholdMs = 0;
            writer = log.RunAsync(postgres, null, writerStop.Token);
            server = await BuildServerAsync(postgres, log);

            await CallToolAsync(server, "get_query_store_top", new JsonObject { ["server_name"] = ServerName, ["hours_back"] = 48, ["top"] = 20 });

            JsonObject? answer = null;
            for (var attempt = 0; attempt < 50; attempt++)
            {
                answer = await CallToolAsync(server, "get_slow_reads", new JsonObject { ["route"] = "get_query_store_top" });
                if (answer["reads"] is JsonArray { Count: > 0 })
                {
                    break;
                }

                await Task.Delay(100, ct);
            }

            var read = Assert.Single(answer!["reads"]!.AsArray())!.AsObject();
            Assert.Equal("mcp", (string)read["surface"]!);
            Assert.Equal(48, (int)read["arguments"]!["hours_back"]!);
            Assert.Equal(20, (int)read["arguments"]!["top"]!);
            Assert.Equal(ServerName, (string)read["server_name"]!);
            Assert.Equal(ServerId, (int)read["arguments"]!["server_id"]!);
            Assert.False(read["arguments"]!.AsObject().ContainsKey("server_name"));
            Assert.Contains((string)read["source"]!, new[] { "raw", "interval_table" });
            var statements = read["statements"]!.AsArray();
            Assert.NotEmpty(statements);
            Assert.All(statements, s => Assert.True((double)s!["ms"]! >= 0));
            Assert.True((int)read["statement_count"]! >= statements.Count);
            Assert.EndsWith("Z", (string)read["read_time"]!, StringComparison.Ordinal);
            Assert.NotNull(read["window_start"]);

            bodySucceeded = true;
        }
        finally
        {
            SlowReadLog.ThresholdMs = priorThreshold;
            server?.Dispose();
            writerStop.Cancel();
            if (writer is not null)
            {
                await writer;
            }

            await LiveStoreCleanupFinishAsync(scratch, bodySucceeded);
        }
    }

    private static Task LiveStoreCleanupFinishAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);

    [Fact]
    public async Task TheWriter_KeepsTheNewestTenThousandRows_AndPurgesRowsOlderThanThirtyDays()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5097 slow-read live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            var now = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
            await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.slow_reads (read_time, surface, route, outcome, total_ms, arguments, arguments_truncated, statements, statement_count, statements_truncated)
SELECT TIMESTAMP '2026-10-01 11:00:00' - make_interval(secs => g), 'mcp', 'seed', 'timeout', 9000, '{}'::jsonb, FALSE, '[]'::jsonb, 0, FALSE
FROM generate_series(1, 10000) AS g;
INSERT INTO collect.slow_reads (read_time, surface, route, outcome, total_ms, arguments, arguments_truncated, statements, statement_count, statements_truncated)
VALUES (TIMESTAMP '2026-08-01 00:00:00', 'mcp', 'ancient', 'timeout', 9000, '{}'::jsonb, FALSE, '[]'::jsonb, 0, FALSE);", connection))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            await SlowReadLog.StoreAsync(postgres, Record(now, "newest"), null);

            await using var count = new NpgsqlCommand("SELECT count(*) FROM collect.slow_reads", connection);
            Assert.Equal((long)SlowReadLog.RowCap, await count.ExecuteScalarAsync(ct));
            await using var ancient = new NpgsqlCommand("SELECT count(*) FROM collect.slow_reads WHERE route = 'ancient'", connection);
            Assert.Equal(0L, await ancient.ExecuteScalarAsync(ct));
            await using var newest = new NpgsqlCommand("SELECT count(*) FROM collect.slow_reads WHERE route = 'newest'", connection);
            Assert.Equal(1L, await newest.ExecuteScalarAsync(ct));
            await using var oldest = new NpgsqlCommand("SELECT min(read_time) FROM collect.slow_reads", connection);
            Assert.Equal(new DateTime(2026, 10, 1, 11, 0, 0).AddSeconds(-9_999), (DateTime)(await oldest.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanupFinishAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task GetSlowReads_OnAnEmptyStore_AnswersEmpty_AndRefusesABadSurface()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5097 slow-read live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            var empty = await DarlingMcpSlowReadTools.GetSlowReads(postgres, cancellationToken: ct);
            Assert.Equal("empty", (string)JsonNode.Parse(empty)!["status"]!);

            var refused = await DarlingMcpSlowReadTools.GetSlowReads(postgres, surface: "nope", cancellationToken: ct);
            Assert.Contains("surface", refused, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanupFinishAsync(scratch, bodySucceeded);
        }
    }
}
