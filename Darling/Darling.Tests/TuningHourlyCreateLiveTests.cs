/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The hourly tuning pass runs beside live collection, so a converged store must issue no CREATE INDEX at all
/// (<c>IF NOT EXISTS</c> takes its table locks before it looks), a missing index must still be built, and a
/// CREATE that is issued must be bounded by a lock_timeout. Own scratch database per test.
/// </summary>
public sealed class TuningHourlyCreateLiveTests
{
    private static readonly string[] TunedIndexes =
    {
        "idx_query_stats_server_hash_time",
        "idx_query_store_stats_server_db_query_plan_time",
        PgTableTuning.ForcePlanFailuresIndexName,
        "idx_store_metrics_kind_name_time",
    };

    /// <summary>
    /// Deterministic method: a second connection holds ROW EXCLUSIVE (what a writer holds) on each tuned table,
    /// which conflicts with the ShareLock a CREATE INDEX takes, even an <c>IF NOT EXISTS</c> one that would
    /// no-op, but not with the lock the pass's ALTER TABLE statements take. The pass's session caps every wait
    /// at 2 s, so any CREATE that reaches a locked table fails with 55P03 and is not counted as applied: a
    /// pass that issues none counts every statement.
    /// </summary>
    [Fact]
    public async Task AConvergedStore_IssuesNoCreateIndex_EvenWhileWritersHoldTheTables()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the hourly tuning live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);
        foreach (var name in TunedIndexes)
        {
            Assert.True(await IndexExistsAsync(body, name, ct), name + " should exist after the first pass");
        }

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await Exec(holder, "BEGIN", ct);
        try
        {
            await Exec(holder, "LOCK TABLE collect.query_stats, collect.query_store_stats, collect.store_metrics IN ROW EXCLUSIVE MODE", ct);
            await Exec(body, "SET lock_timeout = '2s'", ct);

            var applied = await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);

            Assert.Equal(PgTableTuning.Statements.Count, applied);
        }
        finally
        {
            await Exec(holder, "ROLLBACK", CancellationToken.None);
        }
    }

    [Fact]
    public async Task AMissingIndex_IsStillBuilt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the hourly tuning live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);

        await Exec(body, "DROP INDEX collect.idx_store_metrics_kind_name_time", ct);
        Assert.False(await IndexExistsAsync(body, "idx_store_metrics_kind_name_time", ct));

        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);
        Assert.True(await IndexExistsAsync(body, "idx_store_metrics_kind_name_time", ct));
    }

    [Fact]
    public void ACreateThatIsStillIssued_RunsUnderASetLocalLockTimeout()
    {
        var src = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Storage", "PgTableTuning.cs");
        Assert.Contains("GuardedCreate(statement)", src, StringComparison.Ordinal);
        Assert.Contains("\"BEGIN; SET LOCAL lock_timeout = '\" + CreateLockTimeoutSeconds + \"'; \" + statement + \"; COMMIT;\"", src, StringComparison.Ordinal);
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", c);
        cmd.Parameters.AddWithValue(name);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }
}
