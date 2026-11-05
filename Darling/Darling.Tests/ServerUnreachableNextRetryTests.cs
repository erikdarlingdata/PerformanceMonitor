/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7590 part B: while the service cannot connect to a server, the registry also holds when it will try again, Collection Health
/// (the <c>get_collection_health</c> reply, the web tab and the Viewer text) says collection is waiting on the connection with that time,
/// and "Run all collectors now" is off on the web with the offline tip.
/// </summary>
[Collection("live-postgres")]
public sealed class ServerUnreachableNextRetryTests
{
    private static readonly DateTime Since = new(2026, 11, 4, 10, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void The_waiting_text_names_the_since_time_the_error_and_the_next_retry()
    {
        var now = Since.AddMinutes(5);

        var future = ServerHealthClassifier.WaitingOnConnectionText(Since, "no route", now.AddSeconds(90), now);
        Assert.Contains("waiting on the connection", future);
        Assert.Contains("Server unreachable since 2026-11-04 10:00 UTC: no route", future);
        Assert.Contains("Next retry at 2026-11-04 10:06:30 UTC.", future);

        /* A retry time already past means the attempt is under way or about to be. */
        Assert.Contains("Next retry is due now.", ServerHealthClassifier.WaitingOnConnectionText(Since, "no route", now.AddSeconds(-5), now));

        /* No stored time says nothing about the retry (a failure written by a service that had not decided one). */
        Assert.DoesNotContain("Next retry", ServerHealthClassifier.WaitingOnConnectionText(Since, "no route", null, now));
    }

    [Fact]
    public async Task A_failed_connect_stores_the_next_attempt_get_collection_health_names_it_and_a_good_connect_clears_it()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live connect-state test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        static async Task UpsertAsync(NpgsqlDataSource source, CancellationToken token)
        {
            await using var command = source.CreateCommand(DarlingObservability.UpsertServerSql);
            command.Parameters.AddWithValue(77);
            command.Parameters.AddWithValue("sql-live");
            command.Parameters.AddWithValue("sql-live");
            command.Parameters.AddWithValue(3);
            command.Parameters.AddWithValue(16);
            command.Parameters.AddWithValue("sqlserver");
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(0m);
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
            await command.ExecuteNonQueryAsync(token);
        }

        static async Task<DateTime?> StoredNextAttemptAsync(NpgsqlDataSource source, CancellationToken token)
        {
            await using var command = source.CreateCommand("SELECT next_connect_attempt FROM servers WHERE server_id = 77");
            var value = await command.ExecuteScalarAsync(token);
            return value is DateTime t ? t : null;
        }

        static async Task<JsonElement> HealthAsync(NpgsqlDataSource source, CancellationToken token)
        {
            using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealthUncut(source, "sql-live", token));
            return doc.RootElement.Clone();
        }

        await UpsertAsync(postgres, ct);

        /* One collector run, so the tool has a collector row to answer with (it answers "unavailable" for a server with none). */
        await using (var seed = postgres.CreateCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, 77, 'sql-live', 'wait_stats', $2, 'SUCCESS')"))
        {
            seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
            seed.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-2), DateTimeKind.Unspecified));
            await seed.ExecuteNonQueryAsync(ct);
        }

        /* A reachable server: nothing stored, and the reply has no connection block. */
        Assert.Null(await StoredNextAttemptAsync(postgres, ct));
        var reachable = await HealthAsync(postgres, ct);
        Assert.True(reachable.TryGetProperty("collectors", out _), "the reply is the collector answer: " + reachable.GetRawText()[..Math.Min(200, reachable.GetRawText().Length)]);
        Assert.False(reachable.TryGetProperty("server_connection", out var none) && none.ValueKind != JsonValueKind.Null,
            "a reachable server's get_collection_health reply carries no server_connection");

        /* The real connect call site, with a failed attempt: the worker writes the retry time it decided. */
        var worker = new DarlingWorker(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = postgres,
            SelfAlertsForTests = new DarlingSelfAlertEvaluator(
                new DarlingSelfAlertTests.FakeSettings(), new DarlingSelfAlertTests.RecordingDeliverer(),
                new DarlingSelfAlertTests.FakeHistoryStore(), _ => false),
        };
        var config = new MonitoredServer { Name = "sql-live", Host = "sql-live", StoredServerId = 77 };
        var state = new DarlingWorker.ServerLoopState { Config = config };
        var tryConnect = typeof(DarlingWorker).GetMethod("TryConnectAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var before = DateTime.UtcNow;
        await (Task)tryConnect.Invoke(worker, [state, null, new DarlingConfig(), new ConnectAttempt(config, null, new InvalidOperationException("connection refused")), ct])!;

        var stored = await StoredNextAttemptAsync(postgres, ct);
        Assert.NotNull(stored);
        Assert.True(Math.Abs((stored!.Value - state.NextConnectAttempt).TotalMilliseconds) < 1, "the stored retry time is the one the backoff decided");
        Assert.InRange(stored.Value, before, before.AddMinutes(10));

        var down = await HealthAsync(postgres, ct);
        Assert.True(down.TryGetProperty("server_connection", out var block), down.GetRawText()[..Math.Min(600, down.GetRawText().Length)]);
        Assert.Equal("waiting_on_connection", block.GetProperty("status").GetString());
        Assert.NotNull(block.GetProperty("next_connect_attempt").GetString());
        Assert.Contains("Next retry", block.GetProperty("message").GetString());
        Assert.Contains("connection refused", block.GetProperty("last_connect_error").GetString());

        /* A good connect clears it with the other two columns. */
        await UpsertAsync(postgres, ct);
        Assert.Null(await StoredNextAttemptAsync(postgres, ct));
        var cleared = await HealthAsync(postgres, ct);
        Assert.False(cleared.TryGetProperty("server_connection", out var gone) && gone.ValueKind != JsonValueKind.Null);
    }

    private static JsonElement RunButtonHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-collector-run-offline-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the collector run offline harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the collector run offline harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void Run_all_collectors_now_is_off_with_the_offline_tip_for_an_unreachable_server_and_on_for_a_reachable_one()
    {
        var r = RunButtonHarness();

        foreach (var server in new[] { "srv-down", "srv-stored" })
        {
            Assert.True(r.GetProperty(server).GetProperty("disabled").GetBoolean(), server + " is unreachable, so the button is off");
            Assert.StartsWith("This server is offline: the service has no connection to it", r.GetProperty(server).GetProperty("title").GetString());
        }

        var up = r.GetProperty("srv-up");
        Assert.False(up.GetProperty("disabled").GetBoolean());
        Assert.DoesNotContain("offline", up.GetProperty("title").GetString());
    }
}
