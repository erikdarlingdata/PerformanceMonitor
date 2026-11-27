/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. It's in the timing collection (#5602) because it judges how long a read waited. */

/// <summary>
/// #9514: the Threshold Overrides read and the fleet read, served together by the web host on a small pooled connection source with a
/// PostgreSQL target registered. The overrides list read used to dispose its first reader by hand and again at the end of the method.
/// That reader object belongs to the pooled connection, and a connection the first dispose released can be leased by the next request
/// before the second dispose runs, which then closed that request's reader and released its connection while it was still reading. The
/// fleet read failed with "a command is already in progress" or an index out of range, and the overrides read waited for a command timeout.
/// </summary>
[Collection("timing")]
public sealed class WebOverridesBesideFleetLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task OverridesAndFleetReadsServedTogether_OnPooledConnections_NeitherFailsNorWaits()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the overrides-beside-fleet live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            /* A small pool shared by both routes, the way the web host's one data source is: a released connection is leased again at once. */
            var pooled = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = true,
                MinPoolSize = 0,
                MaxPoolSize = 4,
                Timeout = 30,
            }.ConnectionString;
            await using var source = NpgsqlDataSource.Create(pooled);

            await ExecAsync(source, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7'), (8, 'pg-8', 'h8')", ct);
            await ExecAsync(source, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
                + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, NULL, now(), now()), "
                + "(8, 'pg-8', 'pg-8', TRUE, 15, '" + MonitoredEngineKind.AuroraPostgres + "', now(), now())", ct);
            await ExecAsync(source, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(source, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (8, 'cpu.threshold_percent', 77, 'seed')", ct);

            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingWebEndpoints.MapAll(app, source, new CollectorRuntimeState(), new CapturingTestLogger());
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<int> Get(string path)
            {
                var context = await server.SendAsync(request =>
                {
                    request.Request.Method = "GET";
                    request.Request.Path = path;
                    request.Request.Headers.Host = "localhost";
                });
                using var reader = new StreamReader(context.Response.Body);
                await reader.ReadToEndAsync(ct);
                return context.Response.StatusCode;
            }

            var failures = new ConcurrentBag<string>();
            var slowest = 0L;
            var stop = Stopwatch.StartNew();

            async Task Worker(string path, int rounds)
            {
                for (var i = 0; i < rounds && stop.Elapsed < TimeSpan.FromSeconds(60) && failures.IsEmpty; i++)
                {
                    var clock = Stopwatch.StartNew();
                    try
                    {
                        var status = await Get(path);
                        if (status != 200)
                        {
                            failures.Add($"{path} answered {status}");
                        }
                    }
                    catch (Exception ex)
                    {
                        failures.Add($"{path} threw {ex.GetType().Name}: {ex.Message}");
                    }

                    var took = clock.ElapsedMilliseconds;
                    long seen;
                    while ((seen = Interlocked.Read(ref slowest)) < took && Interlocked.CompareExchange(ref slowest, took, seen) != seen)
                    {
                    }
                }
            }

            var workers = new List<Task>();
            for (var w = 0; w < 4; w++)
            {
                workers.Add(Task.Run(() => Worker("/api/threshold-overrides", 100), ct));
                workers.Add(Task.Run(() => Worker("/api/fleet", 100), ct));
            }

            var finished = Task.WhenAll(workers);
            if (await Task.WhenAny(finished, Task.Delay(TimeSpan.FromSeconds(90), ct)) != finished)
            {
                failures.Add("reads were still waiting 90 s after they started");
            }

            Assert.True(failures.IsEmpty, "A read failed while the other route ran: " + string.Join(" | ", failures.Take(3)));
            Assert.True(Interlocked.Read(ref slowest) < 10_000, $"A read waited {slowest} ms beside the other route");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (connection, token) =>
            {
                NpgsqlConnection.ClearAllPools();
                return Task.CompletedTask;
            });
        }
    }
}
