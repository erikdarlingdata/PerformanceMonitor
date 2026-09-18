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
///
/// <para><b>It also carries #3281's invariant end to end.</b> The answer row holds the shape the fleet
/// really produces — <c>cpu_percent</c> 100.0 with 87% of the CONFIGURED ACU ceiling in use — so a card
/// banded off the raw reading reads Critical and one banded off the ceiling reads Warning. A fifth server
/// has a CURRENT reading and NO capacity sample and must band Unknown: that is the round trip of three new
/// nullable columns, where a coalesce anywhere in the read path would turn SQL NULL into a measured 0 and
/// claim headroom nobody measured. Neither is visible to an in-memory fixture, which is the whole reason
/// this file exists beside the shape pins.</para>
///
/// <para><b>And #3539's deadlock arm, on the same fixture.</b> The Aurora target carries a
/// <c>pg_database_stats</c> counter series shaped to defeat every wrong read at once: two databases, one
/// of which steps its lifetime counter 40 → 42 → 42 → 45 (five new deadlocks across one flat interval) and
/// the other of which is RESET mid-window (7 → 7 → 0 → 1: one new deadlock after the reset, and a −7 a
/// naive difference would subtract), plus an out-of-window row far below the series and a second server's
/// series under the same database name. <c>SUM(deadlocks)</c> over the window would answer 184;
/// last-minus-first per database would answer 5 − 6 = −1; an unbounded read would add 37; a read that lost
/// its server partition would fold the other server's 50 in. The right answer is 6, and the card must band
/// it Warning (6/hr is past the shipped 5/hr bar) with the rate published and <c>deadlock_source</c>
/// reading the counter arm because the <c>pg_database_stats</c> collector has a HEALTHY row in the health
/// window. The self-hosted target has the same collector row and a FLAT series (two samples, one
/// difference of zero), so its card is the measured, earned Healthy zero; the silent Aurora target has
/// stats rows and no collector row, so its count is read (50) and its source is <c>CollectorSilent</c> — the
/// engine no longer answers on its own. A PostgreSQL target with NO rows in the window would band Unknown:
/// a difference of nothing is not a zero, which is what keeps #3539 A6's never-collected card measuring
/// nothing, and the unit matrix pins that arm.</para>
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
    private const int AuroraNoCapacityServerId = -993_2675;

    private const string AuroraName = "zz-fleet-pg-cpu-aurora";
    private const string SelfHostedName = "aa-fleet-pg-cpu-selfhosted";
    private const string AuroraSilentName = "mm-fleet-pg-cpu-aurora-silent";
    private const string SqlServerName = "kk-fleet-pg-cpu-sqlserver";
    private const string AuroraNoCapacityName = "dd-fleet-pg-cpu-aurora-nocapacity";

    private static readonly int[] SentinelIds =
    [
        AuroraServerId, SelfHostedServerId, AuroraSilentServerId, SqlServerServerId,
        AuroraNoCapacityServerId,
    ];

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
            await InsertServerAsync(connection, AuroraNoCapacityServerId, AuroraNoCapacityName, MonitoredEngineKind.AuroraPostgres, ct);

            /* Every server collected 30 seconds ago, so freshness is Fresh for all four and the CPU row is
               the only thing that varies. Without this they would all band on collection state, which is
               the reading the issue is about replacing. */
            foreach (var (id, name) in new[]
            {
                (AuroraServerId, AuroraName), (SelfHostedServerId, SelfHostedName),
                (AuroraSilentServerId, AuroraSilentName), (SqlServerServerId, SqlServerName),
                (AuroraNoCapacityServerId, AuroraNoCapacityName),
            })
            {
                await InsertCollectionLogAsync(connection, id, name, now.AddSeconds(-30), ct);
            }

            /* Trap 1: the newest sample has no value. */
            await InsertPgCpuAsync(connection, AuroraServerId, AuroraName,
                now.AddMinutes(-2), now.AddMinutes(-2), null, null, null, null, ct);
            /* The answer, in the shape the fleet actually produces (#3281): the raw reading is pinned at
               100% of the capacity CURRENTLY ALLOCATED while 87% of the CONFIGURED ceiling is in use. A
               card banded off the raw reading reads Critical here; banded off the ceiling it reads
               Warning, so the two are not merely different numbers. */
            await InsertPgCpuAsync(connection, AuroraServerId, AuroraName,
                now.AddMinutes(-2), now.AddMinutes(-3), 100.0, 87.0, 10.44, 12.0, ct);
            /* Trap 2: outside DarlingPgCpuUtilizationReader.Freshness, and in the Healthy band on BOTH
               figures, so neither a bound that fails on the CPU column nor one that fails on the capacity
               column can look like the answer. */
            await InsertPgCpuAsync(connection, AuroraServerId, AuroraName,
                now.AddMinutes(-90), now.AddMinutes(-90), 12.0, 9.0, 1.08, 12.0, ct);

            /* Trap 3: a value on ANOTHER server, so a read that lost its per-server key would hand the
               Aurora card 3% and the assertion below would fail rather than pass on a smear. */
            await InsertPgCpuAsync(connection, AuroraSilentServerId, AuroraSilentName,
                now.AddMinutes(-90), now.AddMinutes(-90), 3.0, 2.0, 0.24, 12.0, ct);

            /* The #3281 invariant, end to end through the store: a CURRENT Performance Insights CPU
               reading with NO capacity sample beside it. The raw 100.0 is the reading that used to fire
               the alert, so a band that fell back to it would read Critical here; the honest answer is
               Unknown, because what is unknown is not the CPU but what the CPU is a fraction of. Live
               rather than only in the unit matrix because this is the round trip of three new nullable
               columns: the read has to bring back SQL NULL as C# null without a coalesce anywhere in the
               path turning it into a measured 0. */
            await InsertPgCpuAsync(connection, AuroraNoCapacityServerId, AuroraNoCapacityName,
                now.AddMinutes(-2), now.AddMinutes(-3), 100.0, null, null, null, ct);

            /* The SQL Server arm, so "additive only" is asserted against a live read rather than argued. */
            await InsertSqlServerCpuAsync(connection, SqlServerServerId, SqlServerName, now.AddMinutes(-1), 30, 4, ct);

            /* #3539: the PostgreSQL deadlock arm. The pg_database_stats collector's health row makes the
               two targets COVERED; the counter series below is what the count is differenced from. */
            await InsertCollectionLogAsync(connection, AuroraServerId, AuroraName, now.AddSeconds(-30), ct, PgDatabaseStatsCollector.Instance.Name);
            await InsertCollectionLogAsync(connection, SelfHostedServerId, SelfHostedName, now.AddSeconds(-30), ct, PgDatabaseStatsCollector.Instance.Name);
            /* Database "orders": 40 -> 42 -> 42 -> 45 = 2 + 0 + 3 = 5 new deadlocks. */
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "orders", now.AddMinutes(-40), 40, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "orders", now.AddMinutes(-30), 42, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "orders", now.AddMinutes(-20), 42, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "orders", now.AddMinutes(-10), 45, ct);
            /* Database "billing": 7 -> 7 -> 0 (reset) -> 1 = 0 + clamp(-7) + 1 = 1 new deadlock. */
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "billing", now.AddMinutes(-40), 7, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "billing", now.AddMinutes(-30), 7, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "billing", now.AddMinutes(-20), 0, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "billing", now.AddMinutes(-10), 1, ct);
            /* The covered, MEASURED zero: two samples, one flat difference. Without the second sample the
               card would honestly read Unknown - a difference needs two samples - so the fixture supplies
               it, and asserts Healthy is EARNED rather than defaulted. */
            await InsertPgDatabaseStatsAsync(connection, SelfHostedServerId, SelfHostedName, "app", now.AddMinutes(-20), 12, ct);
            await InsertPgDatabaseStatsAsync(connection, SelfHostedServerId, SelfHostedName, "app", now.AddMinutes(-10), 12, ct);
            /* Trap: a row OUTSIDE the window with a much lower counter. A read that did not bound its
               window on the start would see 3 -> 40 and add 37. */
            await InsertPgDatabaseStatsAsync(connection, AuroraServerId, AuroraName, "orders", now.AddMinutes(-90), 3, ct);
            /* Trap: a series on ANOTHER server, so a read that lost its per-server partition would fold
               these into the Aurora card. */
            await InsertPgDatabaseStatsAsync(connection, AuroraSilentServerId, AuroraSilentName, "orders", now.AddMinutes(-30), 100, ct);
            await InsertPgDatabaseStatsAsync(connection, AuroraSilentServerId, AuroraSilentName, "orders", now.AddMinutes(-20), 150, ct);

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
            Assert.Equal(100.0, aurora.InstanceCpuPercent);
            Assert.Equal(100.0, aurora.TotalCpuPercent);
            /* The capacity figures round-trip from the SAME row as the CPU reading (#3281), which is what
               makes the band's input and the number beside it describe one minute. */
            Assert.Equal(87.0, aurora.AcuUtilizationPercent);
            Assert.Equal(12.0, aurora.MaxConfiguredAcu);
            /* Banded on the ceiling, so Warning and not the Critical the raw 100.0 would earn. */
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

            /* #3272, end to end: the two DMV-sourced bands claim nothing for this engine. Worth a live
               arm and not only the unit matrix, because the engine gate reads `servers.engine_kind` out of
               the store — a column this test wrote and the reader round-tripped, which is the one part of
               the decision no in-memory fixture exercises. */
            Assert.Equal(HealthSeverity.Unknown, aurora.MemorySeverity);
            Assert.Equal(HealthSeverity.Unknown, aurora.BlockingSeverity);
            Assert.Equal(0, aurora.BlockingCount);

            /* #3539, end to end: the deadlock count is the per-database counter DIFFERENCE, clamped across
               the reset, summed - 5 + 1 = 6 - and not the 184 a SUM(deadlocks) gives, the -1 a per-database
               last-minus-first gives, the 43 an unbounded read gives, or the extra 50 a read that lost its
               server partition folds in. Banded through the shared tiers over the one-hour window: 6/hr is
               past the shipped 5/hr Warning bar. */
            Assert.Equal(6, aurora.DeadlockCount);
            Assert.True(aurora.DeadlockMeasured);
            Assert.Equal(6.0, aurora.DeadlockRatePerHour);
            Assert.Equal(HealthSeverity.Warning, aurora.DeadlockSeverity);
            /* The sample that first showed the newest step - both series stepped at -10 min. */
            Assert.Equal(DateTime.SpecifyKind(now.AddMinutes(-10), DateTimeKind.Unspecified).Ticks / TimeSpan.TicksPerSecond,
                aurora.DeadlockLastSeen!.Value.Ticks / TimeSpan.TicksPerSecond);
            /* Covered through the counter arm, because pg_database_stats has a health row. */
            Assert.Equal(FleetDeadlockSource.PostgresTarget, aurora.DeadlockSource);
            Assert.Equal(CollectorHealthClassifier.Healthy, aurora.DeadlockCollectorBand);

            // ── a PostgreSQL target this build collects no instance CPU for ─────────────────────────
            var selfHosted = seeded.Single(c => c.ServerId == SelfHostedServerId);
            Assert.Null(selfHosted.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Unknown, selfHosted.CpuSeverity);
            Assert.Equal(FleetCpuSource.NoSourceForEngine, selfHosted.CpuSource);
            /* #3539: the covered zero - a running pg_database_stats collector and a flat counter across the
               window is a measured Healthy, exactly as a quiet SQL Server's is. */
            Assert.Equal(0, selfHosted.DeadlockCount);
            Assert.True(selfHosted.DeadlockMeasured);
            Assert.Equal(0.0, selfHosted.DeadlockRatePerHour);
            Assert.Equal(HealthSeverity.Healthy, selfHosted.DeadlockSeverity);
            Assert.Equal(FleetDeadlockSource.PostgresTarget, selfHosted.DeadlockSource);

            // ── an Aurora target whose ingest has produced nothing CURRENT ──────────────────────────
            /* Its only row is 90 minutes old, so the bound excludes it. The arm has to be NotCollected and
               not NoSourceForEngine: this one is a gap someone can go and fix. */
            var silent = seeded.Single(c => c.ServerId == AuroraSilentServerId);
            Assert.Null(silent.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Unknown, silent.CpuSeverity);
            Assert.Equal(FleetCpuSource.NotCollected, silent.CpuSource);
            /* #3539: this target has stats rows (the partition trap, 100 -> 150) but NO pg_database_stats
               health row, so the count is read - 50 - and the coverage says its collector is silent
               rather than the pre-#3539 "PostgreSQL, cannot count". Both facts on one card: the count
               is what the store holds, the source is what the health window knows. */
            Assert.Equal(50, silent.DeadlockCount);
            Assert.True(silent.DeadlockMeasured);
            Assert.Equal(HealthSeverity.Critical, silent.DeadlockSeverity);
            Assert.Equal(FleetDeadlockSource.CollectorSilent, silent.DeadlockSource);

            // ── an Aurora target with a CURRENT reading and no capacity sample (#3281) ──────────────
            /* Unknown, never Healthy, and never the Critical the raw reading alone would earn. The source
               arm still says PerformanceInsights, because a reading WAS collected: the card discloses the
               gap as "we have CPU and no capacity", not as "we have nothing". */
            var noCapacity = seeded.Single(c => c.ServerId == AuroraNoCapacityServerId);
            Assert.Equal(100.0, noCapacity.InstanceCpuPercent);
            Assert.Equal(100.0, noCapacity.TotalCpuPercent);
            Assert.Null(noCapacity.AcuUtilizationPercent);
            Assert.Null(noCapacity.MaxConfiguredAcu);
            Assert.Equal(HealthSeverity.Unknown, noCapacity.CpuSeverity);
            Assert.NotEqual(HealthSeverity.Healthy, noCapacity.CpuSeverity);
            Assert.NotEqual(HealthSeverity.Critical, noCapacity.CpuSeverity);
            Assert.Equal(FleetCpuSource.PerformanceInsights, noCapacity.CpuSource);
            /* Unknown does not escalate, so an unmeasured capacity cannot reorder the fleet. */
            Assert.Equal(FleetHealthBand.Healthy, noCapacity.Band);

            // ── the SQL Server path, unchanged ──────────────────────────────────────────────────────
            var sqlServer = seeded.Single(c => c.ServerId == SqlServerServerId);
            Assert.Equal(30.0, sqlServer.CpuPercent);
            Assert.Equal(4.0, sqlServer.OtherProcessCpuPercent);
            Assert.Equal(34.0, sqlServer.TotalCpuPercent);
            Assert.Equal(HealthSeverity.Healthy, sqlServer.CpuSeverity);
            Assert.Equal(FleetCpuSource.RingBuffer, sqlServer.CpuSource);
            Assert.Null(sqlServer.InstanceCpuPercent);
            /* #3272's other half, live: the same engine gate that made the Aurora card claim nothing must
               leave a SQL Server card MEASURED. Healthy here is earned — the views exist for this engine
               and hold no pressure, no blocking and no deadlock for this sentinel. The unit matrix covers
               the Warning and Critical arms; what this adds is that the gate reads the real column. */
            Assert.Equal(HealthSeverity.Healthy, sqlServer.MemorySeverity);
            Assert.Equal(HealthSeverity.Healthy, sqlServer.BlockingSeverity);
            Assert.Equal(HealthSeverity.Healthy, sqlServer.DeadlockSeverity);
            Assert.Equal(FleetDeadlockSource.CollectorSilent, sqlServer.DeadlockSource);

            /* All four arms present in one call, which is what makes the read's per-server keying and the
               classification jointly observable rather than one-at-a-time. Five servers, four arms: the
               no-capacity card shares the PerformanceInsights arm with the answer card on purpose, because
               provenance and bandability are separate facts and this fixture holds a case where they
               disagree. */
            Assert.Equal(4, seeded.Select(c => c.CpuSource).Distinct().Count());
            Assert.Equal(5, seeded.Count);

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
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, CancellationToken ct,
        string collectorName = "pg_cpu_utilization")
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, $5, $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(collectorName);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Seeds one <c>collect.pg_database_stats</c> row carrying only the deadlock counter (#3539) —
    /// the other counters are left NULL, which the read must tolerate (it differences one column).</summary>
    private static async Task InsertPgDatabaseStatsAsync(
        NpgsqlConnection connection, int serverId, string name, string databaseName,
        DateTime collectionTime, long deadlocks, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name, deadlocks)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(deadlocks);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Seeds one <c>collect.pg_cpu_utilization</c> row. The three capacity values are separate
    /// parameters with no defaults, so every call site states whether that minute had a capacity sample —
    /// which is the fact the band turns on (#3281), and a defaulted null would make "no capacity" the
    /// quiet outcome of forgetting rather than a decision the fixture made.</summary>
    private static async Task InsertPgCpuAsync(
        NpgsqlConnection connection, int serverId, string name,
        DateTime collectionTime, DateTime sampleTime, double? cpuPercent,
        double? acuUtilizationPercent, double? serverlessCapacityAcu, double? maxConfiguredAcu,
        CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_cpu_utilization
    (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent,
     acu_utilization_percent, serverless_capacity_acu, max_configured_acu)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(cpuPercent ?? (object)DBNull.Value);
        command.Parameters.AddWithValue(acuUtilizationPercent ?? (object)DBNull.Value);
        command.Parameters.AddWithValue(serverlessCapacityAcu ?? (object)DBNull.Value);
        command.Parameters.AddWithValue(maxConfiguredAcu ?? (object)DBNull.Value);
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

        foreach (var table in new[] { "pg_database_stats", "pg_cpu_utilization", "cpu_utilization_stats", "collection_log", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
