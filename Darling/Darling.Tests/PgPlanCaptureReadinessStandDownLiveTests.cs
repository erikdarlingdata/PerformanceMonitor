/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8982: <c>pg_plan_capture</c> on Aurora and RDS reads the newest <c>library_loaded</c> readiness row before it downloads the
/// RDS log. FALSE stands the run down under the extension collectors' named skip, with the readiness remedy, and never reaches
/// the plan ingestor. No row, a TRUE row, a NULL row (unknown, not false) and a FALSE row older than the lookback all run as
/// before. The target connection string points at a closed local port, so no run here reaches a server or the AWS API: the
/// ingestor is entered (its resume load counts it) and then finds a host it does not treat as RDS.
/// </summary>
[Collection("live-postgres")]
public sealed class PgPlanCaptureReadinessStandDownLiveTests
{
    /* #1776 own-store: every row is keyed by this distinctive fake server id and removed in the cleanup. */
    private const int ServerId = -898201;
    private const string ServerName = "plan-capture-standdown";
    private const string Collector = "pg_plan_capture";
    private const string SeededDetail = "Seeded remedy 8982: add auto_explain to the cluster parameter group, then reboot the writer.";
    private const string SeededObserved = "pg_stat_statements,pg_hint_plan";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Seed(int MinutesAgo, bool? Satisfied);

    public static TheoryData<string> RunsAsBeforeCases => new()
    {
        "no row",
        "newest true",
        "newest null",
        "false past the lookback",
    };

    private static Seed[] SeedsFor(string name) => name switch
    {
        "no row" => [],
        "newest true" => [new(120, false), new(30, true)],
        /* NULL-safe: the newest row is unknown, so the older FALSE behind it must not decide the run. */
        "newest null" => [new(120, false), new(30, null)],
        "false past the lookback" => [new((int)TimeSpan.FromDays(3).TotalMinutes, false)],
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    [Theory]
    [MemberData(nameof(RunsAsBeforeCases))]
    public async Task ReadinessThatIsNotANewestFalse_RunsTheIngestAsBefore(string name)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-capture stand-down test.");

        await WithStoreAsync(cs!, SeedsFor(name), async (postgres, connection, ct) =>
        {
            var (runner, entered) = BuildRunner(postgres);

            var result = await runner.IngestRdsPlansAsync(Runtime(), ct);

            Assert.NotNull(result);
            Assert.Equal(1, entered.Count);
        });
    }

    [Fact]
    public async Task ANewestFalse_StandsDown_WithTheReadinessRemedy_AndNoIngest()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-capture stand-down test.");

        await WithStoreAsync(cs!, [new(120, true), new(30, false)], async (postgres, connection, ct) =>
        {
            var (runner, entered) = BuildRunner(postgres);

            var stood = await Assert.ThrowsAsync<PgPlanCapturePreconditionException>(() => runner.IngestRdsPlansAsync(Runtime(), ct));

            Assert.Contains(SeededDetail, stood.Message, StringComparison.Ordinal);
            Assert.Contains(SeededObserved, stood.Message, StringComparison.Ordinal);
            Assert.Equal(0, entered.Count);
        });
    }

    /// <summary>
    /// The whole run through the worker: the stand-down lands in <c>collection_log</c> as the extension collectors' named skip,
    /// with the readiness remedy as its message, and <c>get_pg_plans</c> still answers with the readiness facets rather than the
    /// generic missing-extension text.
    /// </summary>
    [Fact]
    public async Task TheWorkerRecordsTheStandDownAsANamedSkip_AndGetPgPlansNamesTheReadinessFacets()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-capture stand-down test.");

        await WithStoreAsync(cs!, [new(30, false)], async (postgres, connection, ct) =>
        {
            var (runner, entered) = BuildRunner(postgres);
            var runtime = Runtime();
            var server = new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };

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
            };

            /* The worker's own dispatch picks the RDS route by target facts; the override sends this run down it directly. */
            worker.DispatchOverrideForTests = (_, overrideRuntime, overrideToken) => runner.IngestRdsPlansAsync(overrideRuntime, overrideToken);

            var effective = StoreConfigProvider.ResolveSchedule(Collector, ServerId, worker.ScheduleOverridesForTest);
            /* A stand-down is not a SUCCESS, so the run reports itself as not succeeded; the collection_log row below is the record. */
            await worker.RunOnLoadAsync(server, runner, Collector, ServerId, effective, ct);

            var (status, message) = await ReadRunAsync(connection, ct);
            Assert.Equal(CollectorRuntimePrecondition.ExtensionMissingStatus, status);
            Assert.True(CollectorRuntimePrecondition.IsNamedSkip(status));
            Assert.Contains(SeededDetail, message, StringComparison.Ordinal);
            Assert.Equal(0, entered.Count);

            var plans = JsonDocument.Parse(await DarlingMcpPgPlanTools.GetPgPlans(postgres, ServerName, 24, 10, cancellationToken: ct)).RootElement;
            Assert.Equal(CollectorRuntimePrecondition.StatusWord, plans.GetProperty("status").GetString());
            Assert.Equal("plan_capture_not_ready", plans.GetProperty("hints").GetProperty("reason").GetString());
        });
    }

    private static ServerRuntime Runtime()
    {
        var config = new MonitoredServer { Name = ServerName, Host = "127.0.0.1" };
        return new ServerRuntime
        {
            Config = config,
            /* A closed local port: the log_destination read fails at once and falls back, and the host is not an RDS one. */
            ConnectionString = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=3",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            StorageName = ServerName,
            ServerId = ServerId,
        };
    }

    private sealed class Counter
    {
        public int Count;
    }

    private static (DarlingCollectorRunner Runner, Counter Entered) BuildRunner(NpgsqlDataSource postgres)
    {
        var runner = new DarlingCollectorRunner(
            postgres,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => "0a1b2c3d");

        var entered = new Counter();
        var resume = new RdsResumeStore(
            Collector,
            (_, _, _) =>
            {
                Interlocked.Increment(ref entered.Count);
                return Task.FromResult(new Dictionary<string, string>());
            },
            (_, _, _, _) => Task.CompletedTask);

        var logs = new RdsLogSource(_ => throw new InvalidOperationException("this test never reaches the AWS log API"));
        runner.UseRdsPlanIngestorForTests(new RdsPlanIngestor(postgres, logs, resume: resume));
        return (runner, entered);
    }

    private static async Task WithStoreAsync(
        string cs, IReadOnlyList<Seed> seeds, Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            foreach (var seed in seeds)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_plan_capture_readiness (collection_id, collection_time, server_id, server_name, facet, is_satisfied, observed, detail)
VALUES ($1, $2, $3, $4, 'library_loaded', $5::boolean, $6, $7)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-seed.MinutesAgo)), ServerId, ServerName,
                    seed.Satisfied, SeededObserved, SeededDetail);
            }

            await body(postgres, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task<(string Status, string Message)> ReadRunAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT status, error_message FROM collection_log WHERE server_id = $1 AND collector_name = $2", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(Collector);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the run wrote no collection_log row");
        var run = (reader.GetString(0), reader.IsDBNull(1) ? string.Empty : reader.GetString(1));
        Assert.False(await reader.ReadAsync(ct), "the run wrote more than one collection_log row");
        return run;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_plan_capture_readiness WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collector_state WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
