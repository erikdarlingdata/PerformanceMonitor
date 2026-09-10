/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3267 end-to-end against a real store: <c>get_fleet_overview</c>'s own read really does return a
/// PostgreSQL target's instance CPU, and the four source arms are distinguishable in ONE fleet call.
///
/// <para><b>Why this and not only the shape pins.</b> <c>FleetCardPostgresCpuTests</c> asserts the SQL's
/// TEXT and the reduction's arithmetic; neither can see whether the query runs. The bare
/// <c>pg_cpu_utilization</c> name has to resolve through the store's <c>search_path</c> to
/// <c>collect.pg_cpu_utilization</c>, the naive-UTC bind has to compare against a naive <c>timestamp</c>
/// column without Npgsql inferring <c>timestamptz</c> and zone-shifting it, and <c>DISTINCT ON</c> has to
/// pick the row this reasoning says it picks. #2213's lesson was a feature whose SQL had been live-verified
/// for weeks and which could not collect a row, because every defect was in the wiring.</para>
///
/// <para><b>The fixture is deliberately not one situation per store.</b> Four servers share the sample —
/// each landing on a different <see cref="FleetCpuSource"/> arm — so a read that smeared one server's value
/// across the fleet, or an <c>INNER JOIN</c>-shaped mistake that dropped the servers with no CPU row, fails
/// here rather than in production. The names are chosen so that neither alphabetical order nor insertion
/// order lines up with the expected values.</para>
///
/// <para><b>Three rows for the Aurora target, two of them traps.</b> The newest row by
/// <c>sample_time</c> carries a NULL value, which is a real state (Performance Insights returns a data point
/// with no value for a period it has no sample for, and the ingestor stores it) — so a read without
/// <c>cpu_percent IS NOT NULL</c> returns "no CPU" for a server that has some. And a row outside the
/// freshness bound carries a value in a DIFFERENT band, so a read whose bound does not work bands the card
/// Healthy instead of Warning rather than merely reporting a stale number.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FleetCardPostgresCpuLivePostgresTests
{
    /* NEGATIVE, per this family's convention: teardown deletes from `servers` by id and a real store assigns
       ids from a sequence, so a positive sentinel is one collision away from removing an operator's row. */
    private const int AuroraServerId = -993_2671;
    private const int SelfHostedServerId = -993_2672;
    private const int AuroraSilentServerId = -993_2673;
    private const int SqlServerServerId = -993_2674;

    private const string AuroraName = "zz-fleet-pg-cpu-aurora";
    private const string SelfHostedName = "aa-fleet-pg-cpu-selfhosted";
    private const string AuroraSilentName = "mm-fleet-pg-cpu-aurora-silent";
    private const string SqlServerName = "kk-fleet-pg-cpu-sqlserver";

    private static readonly int[] SentinelIds =
        [AuroraServerId, SelfHostedServerId, AuroraSilentServerId, SqlServerServerId];

    [Fact]
    public async Task TheFleetCardsCarryEachEnginesCpu_AndNameWhichCollectorAnswered()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet PostgreSQL-CPU test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;

            await InsertServerAsync(connection, AuroraServerId, AuroraName, MonitoredEngineKind.AuroraPostgres, ct);
            await InsertServerAsync(connection, SelfHostedServerId, SelfHostedName, MonitoredEngineKind.Postgres, ct);
            await InsertServerAsync(connection, AuroraSilentServerId, AuroraSilentName, MonitoredEngineKind.AuroraPostgres, ct);
            await InsertServerAsync(connection, SqlServerServerId, SqlServerName, MonitoredEngineKind.SqlServer, ct);

            /* Every server collected 30 seconds ago, so freshness is Fresh for all four and the CPU row is
               the only thing that varies. Without this they would all band on collection state, which is
               the reading the issue is about replacing. */
            foreach (var (id, name) in new[]
            {
                (AuroraServerId, AuroraName), (SelfHostedServerId, SelfHostedName),
                (AuroraSilentServerId, AuroraSilentName), (SqlServerServerId, SqlServerName),
            })
            {
                await InsertCollectionLogAsync(connection, id, name, now.AddSeconds(-30), ct);
            }

            /* Trap 1: the newest sample has no value. */
            await InsertPgCpuAsync(connection, AuroraServerId, AuroraName, now.AddMinutes(-2), now.AddMinutes(-2), null, ct);
            /* The answer. */
            await InsertPgCpuAsync(connection, AuroraServerId, AuroraName, now.AddMinutes(-2), now.AddMinutes(-3), 87.0, ct);
            /* Trap 2: outside DarlingPgCpuUtilizationReader.Freshness, and in the Healthy band. */
            await InsertPgCpuAsync(connection, AuroraServerId, AuroraName, now.AddMinutes(-90), now.AddMinutes(-90), 12.0, ct);

            /* Trap 3: a value on ANOTHER server, so a read that lost its per-server key would hand the
               Aurora card 3% and the assertion below would fail rather than pass on a smear. */
            await InsertPgCpuAsync(connection, AuroraSilentServerId, AuroraSilentName, now.AddMinutes(-90), now.AddMinutes(-90), 3.0, ct);

            /* The SQL Server arm, so "additive only" is asserted against a live read rather than argued. */
            await InsertSqlServerCpuAsync(connection, SqlServerServerId, SqlServerName, now.AddMinutes(-1), 30, 4, ct);

            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-1), now, now, cancellationToken: ct);

            /* Accounting first: every seeded server is still in the answer. A join-shaped mistake in the new
               read drops the three with no current PostgreSQL CPU row, and every per-card assertion below
               would then fail with "sequence contains no matching element" — a confusing way to learn that
               the fix removed servers from the fleet. */
            var seeded = result.Cards.Where(c => SentinelIds.Contains(c.ServerId)).ToList();
            Assert.Equal(
                SentinelIds.OrderBy(i => i).ToArray(),
                seeded.Select(c => c.ServerId).OrderBy(i => i).ToArray());

            // ── the field case: an Aurora target reports its instance CPU and bands on it ────────────
            var aurora = seeded.Single(c => c.ServerId == AuroraServerId);
            Assert.Equal(87.0, aurora.InstanceCpuPercent);
            Assert.Equal(87.0, aurora.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Warning, aurora.CpuSeverity);
            Assert.Equal(FleetCpuSource.PerformanceInsights, aurora.CpuSource);
            Assert.Equal(FleetHealthBand.Warning, aurora.Band);
            /* No per-process attribution is claimed - Performance Insights publishes none. */
            Assert.Null(aurora.CpuPercent);
            Assert.Null(aurora.OtherProcessCpuPercent);
            /* And no PostgreSQL source exists for these, so they stay null rather than reading as zero. */
            Assert.Null(aurora.MemoryMb);
            Assert.Null(aurora.BufferPoolMb);
            Assert.Null(aurora.TotalThreads);
            Assert.Equal(HealthSeverity.Unknown, aurora.ThreadsSeverity);

            // ── a PostgreSQL target this build collects no instance CPU for ─────────────────────────
            var selfHosted = seeded.Single(c => c.ServerId == SelfHostedServerId);
            Assert.Null(selfHosted.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Unknown, selfHosted.CpuSeverity);
            Assert.Equal(FleetCpuSource.NoSourceForEngine, selfHosted.CpuSource);

            // ── an Aurora target whose ingest has produced nothing CURRENT ──────────────────────────
            /* Its only row is 90 minutes old, so the bound excludes it. The arm has to be NotCollected and
               not NoSourceForEngine: this one is a gap someone can go and fix. */
            var silent = seeded.Single(c => c.ServerId == AuroraSilentServerId);
            Assert.Null(silent.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Unknown, silent.CpuSeverity);
            Assert.Equal(FleetCpuSource.NotCollected, silent.CpuSource);

            // ── the SQL Server path, unchanged ──────────────────────────────────────────────────────
            var sqlServer = seeded.Single(c => c.ServerId == SqlServerServerId);
            Assert.Equal(30.0, sqlServer.CpuPercent);
            Assert.Equal(4.0, sqlServer.OtherProcessCpuPercent);
            Assert.Equal(34.0, sqlServer.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Healthy, sqlServer.CpuSeverity);
            Assert.Equal(FleetCpuSource.RingBuffer, sqlServer.CpuSource);
            Assert.Null(sqlServer.InstanceCpuPercent);

            /* All four arms present in one call, which is what makes the read's per-server keying and the
               classification jointly observable rather than one-at-a-time. */
            Assert.Equal(4, seeded.Select(c => c.CpuSource).Distinct().Count());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    private static async Task InsertServerAsync(
        NpgsqlConnection connection, int serverId, string name, string engineKind, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES ($1, $2, $2, TRUE, 0, $3)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(engineKind);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, 'pg_cpu_utilization', $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertPgCpuAsync(
        NpgsqlConnection connection, int serverId, string name,
        DateTime collectionTime, DateTime sampleTime, double? cpuPercent, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_cpu_utilization (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(cpuPercent ?? (object)DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSqlServerCpuAsync(
        NpgsqlConnection connection, int serverId, string name,
        DateTime collectionTime, int sqlCpu, int otherCpu, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.AddWithValue(otherCpu);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", SentinelIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));

        foreach (var table in new[] { "pg_cpu_utilization", "cpu_utilization_stats", "collection_log", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
