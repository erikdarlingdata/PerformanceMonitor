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
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9416: each background pass and collector names its store connection under the service's prefix, and every reader that
/// recognises the service's own sessions by that prefix still does.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class StoreSessionNamesTests
{
    private static List<string> EveryStepName() =>
        typeof(StoreSessionNames)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name != nameof(StoreSessionNames.ServicePrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();

    [Fact]
    public void ServicePrefix_IsTheNameTheServiceConnectsWith()
    {
        Assert.Equal(DarlingManagedPostgres.ServiceApplicationName, StoreSessionNames.ServicePrefix);
    }

    [Fact]
    public void EveryStepName_IsOneTheMonitorSessionFiltersStillMatch_AndFitsWhatTheStoreKeeps()
    {
        var names = EveryStepName();
        names.Add(StoreSessionNames.ForCollector("wait_stats"));
        names.Add(StoreSessionNames.ForCollector("a_collector_with_a_very_long_name_that_will_not_fit_in_the_store"));

        Assert.True(names.Count > 30);
        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
        foreach (var name in names)
        {
            Assert.StartsWith(StoreSessionNames.ServicePrefix + "/", name, StringComparison.Ordinal);
            Assert.InRange(name.Length, 1, StoreSessionNames.MaxLength);
            Assert.True(name.All(c => c is >= ' ' and <= '~'), name);
            Assert.True(MonitorApplicationNames.MatchesSqlProgramNamePattern(name), name);
            Assert.True(MonitorApplicationNames.IsMonitorApplication(name), name);
            Assert.True(PostgresRemediationExecutor.IsDarlingProgram(name), name);
        }
    }

    [Fact]
    public void ForCollector_NamesTheCollector_AndFallsBackWhenThereIsNone()
    {
        Assert.Equal(StoreSessionNames.ServicePrefix + "/collect-query_stats", StoreSessionNames.ForCollector("query_stats"));
        Assert.Equal(StoreSessionNames.CollectorState, StoreSessionNames.ForCollector(""));
        Assert.Equal(StoreSessionNames.CollectorState, StoreSessionNames.ForCollector("  "));

        var cut = StoreSessionNames.ForCollector(new string('x', 200));
        Assert.Equal(StoreSessionNames.MaxLength, cut.Length);
    }

    /// <summary>
    /// Opens through the helper on a one-connection pool, so a second lease is the same backend: the name is on the backend while
    /// it is leased (read from another connection), the monitor-session predicate the store reads use drops it, the next plain
    /// lease gets the name the pool started with back, and a source that names itself is left alone.
    /// </summary>
    [Fact]
    public async Task OpenConnectionAsync_NamesTheLeasedBackend_AndTheNextLeaseStartsFromThePoolsName()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #9416 live pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        try
        {
            var serviceBuilder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                ApplicationName = StoreSessionNames.ServicePrefix,
                MaxPoolSize = 1,
            };
            await using var service = NpgsqlDataSource.Create(serviceBuilder.ConnectionString);
            await using var watcher = NpgsqlDataSource.Create(scratch.ConnectionString);

            int pid;
            await using (var leased = await service.OpenConnectionAsync(StoreSessionNames.PlanRegression, ct))
            {
                pid = leased.ProcessID;
                Assert.Equal(StoreSessionNames.PlanRegression, await WatchedNameAsync(watcher, pid, ct));

                await using var watching = await watcher.OpenConnectionAsync(ct);
                await using var program = new NpgsqlCommand(
                    "SELECT count(*) FROM (SELECT application_name AS program_name FROM pg_stat_activity WHERE pid = $1) t WHERE "
                    + MonitorApplicationNames.StoreNotMonitorProgramNamePredicate, watching);
                program.Parameters.Add(new NpgsqlParameter<int> { TypedValue = pid });
                Assert.Equal(0L, (long)(await program.ExecuteScalarAsync(ct))!);
            }

            await using (var again = await service.OpenConnectionAsync(StoreSessionNames.ForCollector("wait_stats"), ct))
            {
                Assert.Equal(pid, again.ProcessID);
                Assert.Equal(StoreSessionNames.ForCollector("wait_stats"), await WatchedNameAsync(watcher, pid, ct));
            }

            await using (var plain = await service.OpenConnectionAsync(ct))
            {
                Assert.Equal(pid, plain.ProcessID);
                await using var ping = new NpgsqlCommand("SELECT 1", plain);
                await ping.ExecuteScalarAsync(ct);
                Assert.Equal(StoreSessionNames.ServicePrefix, await WatchedNameAsync(watcher, pid, ct));
            }

            var operatorBuilder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { ApplicationName = "my-own-label", MaxPoolSize = 1 };
            await using var operatorSource = NpgsqlDataSource.Create(operatorBuilder.ConnectionString);
            await using (var theirs = await operatorSource.OpenConnectionAsync(StoreSessionNames.PlanRegression, ct))
            {
                Assert.Equal("my-own-label", await WatchedNameAsync(watcher, theirs.ProcessID, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<string?> WatchedNameAsync(NpgsqlDataSource watcher, int pid, CancellationToken ct)
    {
        await using var connection = await watcher.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand("SELECT application_name FROM pg_stat_activity WHERE pid = $1", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = pid });
        return (string?)await command.ExecuteScalarAsync(ct);
    }
}
