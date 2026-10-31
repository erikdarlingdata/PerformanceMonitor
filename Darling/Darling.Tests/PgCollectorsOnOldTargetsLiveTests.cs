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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6985: every PostgreSQL collector, run end to end against a REAL old server. #6206 (six collectors failing on every run
/// on PostgreSQL 10 and 12) and #6836 (<c>Column 'wal_records' is null</c> on the same servers) both reached a release
/// because no test ever used an old server as the monitored target: the other live classes monitor the bundled
/// PostgreSQL 18, <c>PgOldVersionReadPathLiveTests</c> claims an old major against it, and #6210 only ran the query text by
/// hand.
/// <para>For each target variable (<c>DARLING_TEST_PG_TARGET_&lt;major&gt;</c>, started by
/// <c>start-test-pg.ps1 -Mode StartTargets</c>) the test connects the way the service does
/// (<see cref="DarlingServerConnector.ConnectPostgresAsync(MonitoredServer, string, Microsoft.Extensions.Logging.ILogger?, CancellationToken)"/>,
/// so the target carries the real <c>server_version_num</c> and extension version), then walks
/// <see cref="CollectorCatalog.All"/> AT RUN TIME: every PostgreSQL definition that applies to that target goes through
/// <see cref="DarlingCollectorRunner.RunAsync{TRow}"/> twice (the query, <c>ReadAsync</c>, the delta calculator, then
/// <c>WritePayload</c> through the store's COPY into the migrated throwaway store, which is <c>DARLING_TEST_PG</c>). A new
/// collector therefore runs by default and stays out only with a row in <see cref="Excluded"/>. The second pass follows a
/// write to the target, so the delta path sees rows.</para>
/// <para>The class FAILS rather than skips when <c>CI</c> is set and an expected target variable is missing: a skipped live
/// class reads as a pass.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgCollectorsOnOldTargetsLiveTests(ITestOutputHelper output)
{
    /// <summary>The majors CI starts as targets. A major added to <c>start-test-pg.ps1 -Majors</c> is added here too.</summary>
    public static IEnumerable<TheoryDataRow<int>> ExpectedMajors() => new[] { 10 }.Select(major => new TheoryDataRow<int>(major));

    /// <summary>
    /// Collectors that do not run against a target started by <c>start-test-pg.ps1</c>, each with the reason. A collector
    /// missing from here AND from the catalog's engine gate runs; one listed here must still name a catalog definition
    /// (<see cref="ExclusionsNameCatalogDefinitions"/>).
    /// </summary>
    private static readonly Dictionary<string, string> Excluded = new(StringComparer.Ordinal)
    {
        ["pg_log_events"] = "reads the server log with pg_read_file; the target has no logging_collector. The log readers are covered live on the 5542-5550 clusters.",
        ["pg_deadlocks"] = "reads the server log with pg_read_file; the target has no logging_collector. Covered live on the 5542-5550 clusters.",
        ["pg_plan_capture"] = "reads auto_explain log events from the server log; the target has no logging_collector and the EDB zip's auto_explain is not preloaded. Covered live on 5542/5544.",
        ["pg_cpu_utilization"] = "reads RDS Performance Insights through the AWS API, not SQL on the target; a self-hosted host has no such source.",
    };

    private static bool InCi => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI"));

    [Fact]
    public void ExclusionsNameCatalogDefinitions()
    {
        foreach (var name in Excluded.Keys)
        {
            var definition = CollectorCatalog.Find(name);
            Assert.True(definition is { TargetEngine: CollectorTargetEngine.PostgreSql },
                $"Excluded names '{name}', which is not a PostgreSQL definition in CollectorCatalog.All; remove the row or fix the name.");
        }
    }

    [Theory]
    [MemberData(nameof(ExpectedMajors))]
    public async Task EveryApplicableCollector_ReadsAndStoresRowsFromARealOldTarget(int major)
    {
        var variable = $"DARLING_TEST_PG_TARGET_{major}";
        var targetConnectionString = Environment.GetEnvironmentVariable(variable);
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(targetConnectionString) && InCi)
        {
            Assert.Fail($"{variable} is not set in CI: the old-target clusters were not started, so no collector ran against PostgreSQL {major}. Run start-test-pg.ps1 -Mode StartTargets -Majors {major}.");
        }

        Assert.SkipWhen(string.IsNullOrEmpty(targetConnectionString), $"Set {variable} (start-test-pg.ps1 -Mode StartTargets) to run the collectors against a real PostgreSQL {major}.");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the old-target collector test.");

        var ct = TestContext.Current.CancellationToken;
        var storeBuilder = new NpgsqlConnectionStringBuilder(storeConnectionString!) { SearchPath = PgSchemaGenerator.SearchPath };
        await using (var migrate = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(storeBuilder.ConnectionString) { Pooling = false }.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var target = new NpgsqlConnectionStringBuilder(targetConnectionString!);
        var serverId = -Random.Shared.Next(1_000_000, int.MaxValue);
        var config = new MonitoredServer
        {
            Name = $"pm6985-pg{major}-" + Guid.NewGuid().ToString("N")[..8],
            Host = target.Host ?? "127.0.0.1",
            Port = target.Port,
            Database = target.Database ?? "postgres",
            Engine = "postgres",
            Auth = "sql",
            Username = target.Username ?? "darling",
            EncryptMode = "Optional",
            StoredServerId = serverId,
        };

        var bodySucceeded = false;
        await using var store = NpgsqlDataSource.Create(storeBuilder.ConnectionString);
        try
        {
            var runtime = await DarlingServerConnector.ConnectPostgresAsync(
                config, MonitoredServerConnection.BuildConnectionString(config, "unused"), NullLogger.Instance, ct);
            Assert.Equal(major, runtime.Target.PostgresMajorVersion);
            Assert.Equal(major, runtime.Target.PostgresVersionNum / 10000);

            var runner = new DarlingCollectorRunner(store, new CollectorDeltaCalculator(), NullLogger<DarlingCollectorRunner>.Instance);
            var definitions = CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql).ToList();
            var ran = new List<string>();
            var gatedOff = new List<string>();
            var failures = new List<string>();

            foreach (var pass in new[] { 1, 2 })
            {
                if (pass == 2)
                {
                    // A change on the target between the passes, so the delta calculator has two real snapshots to difference.
                    await using var work = NpgsqlDataSource.Create(target.ConnectionString);
                    await using var command = work.CreateCommand("UPDATE target_workload SET payload = payload || 'y' WHERE id <= 50; SELECT count(*) FROM target_workload");
                    await command.ExecuteNonQueryAsync(ct);
                }

                foreach (var definition in definitions)
                {
                    if (Excluded.ContainsKey(definition.Name))
                    {
                        continue;
                    }

                    if (!CollectorCatalog.AppliesTo(definition, runtime.Target))
                    {
                        if (pass == 1)
                        {
                            gatedOff.Add(definition.Name);
                        }

                        continue;
                    }

                    try
                    {
                        await RunOneAsync(runner, definition, runtime, ct);
                        if (pass == 1)
                        {
                            ran.Add(definition.Name);
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        failures.Add($"pass {pass}: {definition.Name}: {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }

            output.WriteLine($"PostgreSQL {major} (server_version_num {runtime.Target.PostgresVersionNum}): ran {ran.Count} collectors: {string.Join(", ", ran)}");
            output.WriteLine($"  the catalog's own gate turned off {gatedOff.Count}: {string.Join(", ", gatedOff)}");
            output.WriteLine($"  excluded {Excluded.Count}: {string.Join(", ", Excluded.Keys)}");

            Assert.True(failures.Count == 0, $"{failures.Count} collector run(s) threw against PostgreSQL {major}:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
            Assert.True(ran.Count > 0, $"no PostgreSQL collector applied to the PostgreSQL {major} target, so nothing was tested");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(storeBuilder.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var table in CollectorCatalog.All
                    .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql).Select(d => d.TargetTable).Distinct())
                {
                    await using var delete = new NpgsqlCommand($"DELETE FROM collect.{table} WHERE server_id = {serverId}", cleanup);
                    await delete.ExecuteNonQueryAsync(cleanupCt);
                }
            });
        }
    }

    /// <summary>The runner's generic entry point, reached from a non-generic catalog entry through the definition's row type.</summary>
    private static async Task RunOneAsync(DarlingCollectorRunner runner, ICollectorSchemaInfo definition, ServerRuntime runtime, CancellationToken ct)
    {
        var rowType = definition.GetType().GetInterfaces()
            .Single(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollectorDefinition<>))
            .GetGenericArguments()[0];
        var run = typeof(DarlingCollectorRunner).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Single(m => m.Name == nameof(DarlingCollectorRunner.RunAsync) && m.IsGenericMethodDefinition)
            .MakeGenericMethod(rowType);
        try
        {
            await (Task)run.Invoke(runner, new object[] { definition, runtime, ct })!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
        }
    }
}
