/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the optional-TimescaleDB contract. Ungated: the hypertable scope is EXACTLY the shared
/// collector catalog (the registry/config/analysis tables can never sneak in), every
/// create_hypertable partitions by_range on the definition's own prefix time column
/// (collection_time almost everywhere; the config snapshots' capture_time) with if_not_exists +
/// migrate_data, compression segments by server_id, and the policy is the hardcoded 1-day
/// if_not_exists shape. Gated on DARLING_TEST_PG (the dev fixture has the extension): detect →
/// convert (idempotent) → a 40-day-old wait_stats row and a 70-day-old collection_log row are removed
/// by the drop_chunks-based purge (collection_log is a hypertable since V23) while a fresh row holds →
/// the compression policy applies idempotently and lands in timescaledb_information.jobs.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes/chunk drops) cannot race another class. */
[Collection("live-postgres")]
public sealed class TimescaleSupportTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -717171;

    [Fact]
    public void HypertableScope_IsExactlyTheCollectorCatalog()
    {
        /* Scope = the catalog, table-for-table: 26 collector tables, nothing else. */
        Assert.Equal(
            CollectorCatalog.All.Select(s => s.TargetTable).ToArray(),
            TimescaleSupport.HypertableTables.Select(s => s.TargetTable).ToArray());

        /* The registry/config/analysis tables stay plain: registries keep their PRIMARY KEYs
           (which hypertables reject unless they include the partition column), and
           analysis_findings — designed keyless so it COULD convert later — is a deliberate
           not-yet. Widening the scope must consciously break this pin. */
        var hypertables = TimescaleSupport.HypertableTables.Select(s => s.TargetTable).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var excluded in new[]
        {
            "servers",
            "config_alert_log", "config_edge_trigger_watermarks", "config_mute_rules",
            "analysis_findings", "analysis_muted", "darling_schema_version",
        })
        {
            Assert.False(hypertables.Contains(excluded), $"'{excluded}' must never be converted to a hypertable");
        }

        /* collection_log IS a hypertable (since V23) but is deliberately NOT in the catalog: it is converted +
           compressed DIRECTLY — authoritatively by EnsureCollectionLogHypertableAsync at runtime, plus a
           best-effort V23-migration fast-path — and purged directly by DarlingRetention, so the catalog-driven
           runtime loops (ConvertToHypertables / ApplyCompressionPolicy) must never touch it. Its +1 IS reflected
           in the worker-sizing count, though (HypertableCount). */
        Assert.False(hypertables.Contains("collection_log"),
            "collection_log must stay OUT of the collector catalog — it is converted directly, not via the catalog loop");
        Assert.Equal(TimescaleSupport.HypertableTables.Count + 1, TimescaleSupport.HypertableCount);
    }

    [Fact]
    public void CreateHypertableSql_PartitionsByEachDefinitionsOwnTimeColumn()
    {
        var byName = CollectorCatalog.All.ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);

        Assert.Equal(
            "SELECT create_hypertable('wait_stats', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)",
            TimescaleSupport.CreateHypertableSql(byName["wait_stats"]));

        /* The config snapshots partition on their capture_time, not collection_time. */
        Assert.Equal(
            "SELECT create_hypertable('server_config', by_range('capture_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)",
            TimescaleSupport.CreateHypertableSql(byName["server_config"]));
        Assert.Equal(
            "SELECT create_hypertable('trace_flags', by_range('capture_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)",
            TimescaleSupport.CreateHypertableSql(byName["trace_flags"]));

        /* Every table: its own prefix time column, 1-day chunk interval, idempotent, and existing
           plain-PG data migrates into chunks. */
        foreach (var schema in CollectorCatalog.All)
        {
            var sql = TimescaleSupport.CreateHypertableSql(schema);
            Assert.Contains($"create_hypertable('{schema.TargetTable}', by_range('{schema.PrefixTimeColumnName}', INTERVAL '1 days')", sql, StringComparison.Ordinal);
            Assert.Contains("if_not_exists => true", sql, StringComparison.Ordinal);
            Assert.Contains("migrate_data => true", sql, StringComparison.Ordinal);
        }

        /* collection_log's runtime conversion (the raw-name overload, since it has no ICollectorSchemaInfo) —
           the AUTHORITATIVE path EnsureCollectionLogHypertableAsync runs, identical shape to the collectors. */
        Assert.Equal(
            "SELECT create_hypertable('collection_log', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)",
            TimescaleSupport.CreateHypertableSql(TimescaleSupport.CollectionLogTable, TimescaleSupport.CollectionLogTimeColumn));
    }

    [Fact]
    public void CompressionSql_SegmentsByServerId_OneDayPolicy_IfNotExists()
    {
        var byName = CollectorCatalog.All.ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);

        Assert.Equal(
            "ALTER TABLE wait_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')",
            TimescaleSupport.EnableCompressionSql(byName["wait_stats"]));
        Assert.Equal(
            "SELECT add_compression_policy('wait_stats', compress_after => INTERVAL '1 days', schedule_interval => INTERVAL '1 hour', if_not_exists => true)",
            TimescaleSupport.AddCompressionPolicySql(byName["wait_stats"]));

        /* 1 day matches the 1-day chunk interval so chunks become compressible quickly, keeping the
           managed store compact (#1458). */
        Assert.Equal(1, TimescaleSupport.CompressAfterDays);

        /* #1778: schedule_interval is passed EXPLICITLY on every table. Omitting it does not mean "some
           sensible default" — TimescaleDB computes 12 hours for a 1-day chunk interval, which is the
           twice-daily tick the field reported, so an omitted argument here is a silent half-day of latency. */
        Assert.Equal("1 hour", TimescaleSupport.CompressScheduleInterval);

        foreach (var schema in CollectorCatalog.All)
        {
            Assert.Contains("timescaledb.compress_segmentby = 'server_id'",
                TimescaleSupport.EnableCompressionSql(schema), StringComparison.Ordinal);
            Assert.Contains("if_not_exists => true",
                TimescaleSupport.AddCompressionPolicySql(schema), StringComparison.Ordinal);
            Assert.Contains($"schedule_interval => INTERVAL '{TimescaleSupport.CompressScheduleInterval}'",
                TimescaleSupport.AddCompressionPolicySql(schema), StringComparison.Ordinal);
        }

        /* collection_log gets the identical compression via the raw-name overloads (the runtime path). */
        Assert.Equal(
            "ALTER TABLE collection_log SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')",
            TimescaleSupport.EnableCompressionSql(TimescaleSupport.CollectionLogTable));
        Assert.Equal(
            "SELECT add_compression_policy('collection_log', compress_after => INTERVAL '1 days', schedule_interval => INTERVAL '1 hour', if_not_exists => true)",
            TimescaleSupport.AddCompressionPolicySql(TimescaleSupport.CollectionLogTable));
    }

    /* ---------------- compression-job self-heal (#1581) — pure predicate ---------------- */

    private static readonly DateTime s_now = new(2026, 7, 19, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void IsCompressionJobStuck_NextStartNegativeInfinity_IsStuck()
    {
        /* The dominant failure mode: next_start = -infinity on a job that is NOT running — the scheduler
           abandoned it and never re-fires it. */
        Assert.True(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: true, jobStatus: "Scheduled", lastRunStartedAtUtc: null,
            scheduleInterval: TimeSpan.FromHours(12), nowUtc: s_now, out var reason));
        Assert.Contains("-infinity", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void IsCompressionJobStuck_NegativeInfinityWhileRunning_IsTheMidRunMarker_NotStuck()
    {
        /* Measured live on TimescaleDB 2.x: from scheduler pickup to run completion, next_start reads
           -infinity WITH job_status = 'Running' — the engine only computes the real next start when the
           run finishes. An unconditioned -infinity arm flagged every healthy job caught mid-run (the
           field's transient stuck→self-healed alert noise, and the CI flake where the live test caught
           its own re-arm-triggered run). Mid-run belongs to the elapsed-bound arm: */
        Assert.False(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: true, jobStatus: "Running", lastRunStartedAtUtc: s_now.AddMinutes(-3),
            scheduleInterval: TimeSpan.FromHours(12), nowUtc: s_now, out _));

        /* ...which still catches a genuinely HUNG run that carries the mid-run marker. */
        Assert.True(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: true, jobStatus: "Running", lastRunStartedAtUtc: s_now.AddHours(-30),
            scheduleInterval: TimeSpan.FromHours(12), nowUtc: s_now, out var reason));
        Assert.Contains("Running", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void IsCompressionJobStuck_HealthyScheduled_IsNotStuck()
    {
        /* A normally scheduled job (finite next_start, not running) is healthy. */
        Assert.False(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Scheduled", lastRunStartedAtUtc: s_now.AddMinutes(-5),
            scheduleInterval: TimeSpan.FromHours(12), nowUtc: s_now, out var reason));
        Assert.Equal("", reason);
    }

    [Fact]
    public void IsCompressionJobStuck_RunningPastBound_IsStuck()
    {
        /* Running since well past max(2x interval, floor). At the #1778 tick the FLOOR dominates (2x 1h = 2h,
           floor 6h), so it takes more than six hours to call a run hung — 8h elapsed does. */
        Assert.True(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Running", lastRunStartedAtUtc: s_now.AddHours(-8),
            scheduleInterval: TimeSpan.FromHours(1), nowUtc: s_now, out var reason));
        Assert.Contains("Running", reason, StringComparison.Ordinal);

        /* And the run the field actually measured (1h33m) is NOT hung at that same tick — the regression #1778
           would otherwise have introduced by shortening the interval without raising the floor. */
        Assert.False(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Running", lastRunStartedAtUtc: s_now.AddMinutes(-93),
            scheduleInterval: TimeSpan.FromHours(1), nowUtc: s_now, out _));
    }

    [Fact]
    public void IsCompressionJobStuck_RunningWithinBound_IsNotStuck()
    {
        /* Running for 10 minutes with a 12h interval (bound = 24h) — legitimately in progress, not stuck. */
        Assert.False(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Running", lastRunStartedAtUtc: s_now.AddMinutes(-10),
            scheduleInterval: TimeSpan.FromHours(12), nowUtc: s_now, out _));
    }

    [Fact]
    public void IsCompressionJobStuck_RunningButNoStartTime_IsNotStuck()
    {
        /* Running with an unknown last_run_started_at cannot be judged as hung — do not false-flag. */
        Assert.False(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Running", lastRunStartedAtUtc: null,
            scheduleInterval: TimeSpan.FromHours(1), nowUtc: s_now, out _));
    }

    [Fact]
    public void IsCompressionJobStuck_RunningWithNeverRanSentinel_IsNotStuck()
    {
        /* #1760: TimescaleDB's never-ran sentinel is -infinity, which Npgsql maps to DateTime.MinValue. Read
           literally that is a run "started" in year 1 — an elapsed of ~739,000 days that clears every bound —
           so a healthy job got flagged as stuck for the whole of its FIRST run. StuckCompressionJobsSql NULLIFs
           the sentinel; this is the second line of defence, so a future caller reading the column un-guarded
           cannot resurrect the false positive. */
        Assert.False(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Running", lastRunStartedAtUtc: DateTime.MinValue,
            scheduleInterval: TimeSpan.FromHours(12), nowUtc: s_now, out _));
    }

    [Fact]
    public void StuckCompressionJobsSql_GuardsTheNeverRanSentinel()
    {
        /* The guard lives in the ONE query the detector and the live test both run. Containment, not shape:
           the point is that last_run_started_at is never read raw. */
        Assert.Contains("NULLIF(js.last_run_started_at, '-infinity'::timestamptz)",
            TimescaleSupport.StuckCompressionJobsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void StuckRunningBound_UsesMaxOfTwiceIntervalAndFloor()
    {
        /* 2x a large interval wins over the floor. */
        Assert.Equal(TimeSpan.FromHours(24), TimescaleSupport.StuckRunningBound(TimeSpan.FromHours(12)));
        /* The floor wins over 2x a tiny interval — and since #1778 shortened the tick to an hour, the floor is
           what holds this bound up in production rather than a term that never bound anything. */
        Assert.Equal(TimeSpan.FromHours(6), TimescaleSupport.StuckRunningBound(TimeSpan.FromMinutes(1)));
        /* A missing/zero interval falls back to the floor. */
        Assert.Equal(TimeSpan.FromHours(6), TimescaleSupport.StuckRunningBound(null));
        Assert.Equal(TimeSpan.FromHours(6), TimescaleSupport.StuckRunningBound(TimeSpan.Zero));
    }

    [Fact]
    public void RearmJobSql_IsParameterized_NotInterpolated()
    {
        /* The job_id is ALWAYS bound as $1, never interpolated; next_start is SQL now(), not a value. */
        Assert.Equal("SELECT alter_job($1::integer, next_start => now())", TimescaleSupport.RearmJobSql);
        Assert.Contains("$1", TimescaleSupport.RearmJobSql, StringComparison.Ordinal);
        Assert.Contains("next_start => now()", TimescaleSupport.RearmJobSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadStuckCompressionJobs_StoreUnavailable_ReturnsEmpty_DoesNotThrow()
    {
        /* Failure-isolated: when the timescaledb_information views are absent (a plain-PostgreSQL store — the
           belt-and-suspenders behind the worker's _timescaleAvailable gate) or the store is unreachable, the
           read returns an empty list and logs at Debug, NEVER throwing into the sweep loop. An unopened
           connection exercises that catch deterministically without a live store. */
        using var connection = new NpgsqlConnection("Host=localhost;Port=1;Database=darling-does-not-exist");
        var result = await TimescaleSupport.ReadStuckCompressionJobsAsync(
            connection, DateTime.UtcNow, logger: null, TestContext.Current.CancellationToken);
        Assert.Empty(result);
    }

    [Fact]
    public async Task TryRearmJob_StoreUnavailable_ReturnsFalse_DoesNotThrow()
    {
        /* An alter_job failure (permission denied on a least-privilege BYO store, or a store hiccup) degrades to
           false + a single log line — never a crash. An unopened connection stands in for any such failure. */
        using var connection = new NpgsqlConnection("Host=localhost;Port=1;Database=darling-does-not-exist");
        Assert.False(await TimescaleSupport.TryRearmJobAsync(
            connection, jobId: 1234, logger: null, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EndToEnd_DetectConvertAndDropChunksPurge_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live Timescale test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The dev fixture has the extension (validated live on 2.28.1): enable must succeed and
           detection must agree. */
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        Assert.True(await TimescaleSupport.DetectAsync(connection, ct));

        /* Conversion covers every collector table and is idempotent (if_not_exists no-ops). */
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));

        /* wait_stats really is a hypertable now — so the purge below genuinely exercises
           drop_chunks, not the per-table DELETE fallback. */
        using (var isHypertable = new NpgsqlCommand(
            "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'wait_stats'", connection))
        {
            Assert.Equal(1L, await isHypertable.ExecuteScalarAsync(ct));
        }

        /* collection_log is ALSO a hypertable now — but NOT via ConvertToHypertablesAsync (it is outside the
           collector catalog). The V23 migration converts it only on an upgrade where the extension already
           exists; on a store whose migrations ran BEFORE CREATE EXTENSION (this shared test database, and any
           fresh managed store) V23's guard skips and the AUTHORITATIVE runtime path is
           EnsureCollectionLogHypertableAsync — the same call the service makes right after TryEnableAsync on
           every start. Exercise it exactly like the service does, then the purge below genuinely hits
           drop_chunks, not the DELETE fallback. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct),
            "EnsureCollectionLogHypertableAsync is expected to convert (or no-op on) collection_log once the extension is enabled");

        using (var logIsHypertable = new NpgsqlCommand(
            "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'collection_log'", connection))
        {
            Assert.Equal(1L, await logIsHypertable.ExecuteScalarAsync(ct));
        }

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* All timestamps Kind-Unspecified — naive-UTC storage, see PgCollectorRowWriter. */
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            /* wait_stats retention is 30 days. The old row is 40 days back so its WHOLE chunk
               (7-day default width → spanning at most now-43d..now-36d) is past the horizon —
               drop_chunks only drops fully-expired chunks. The fresh row lives in the current
               chunk, which can never be fully expired. */
            using (var insert = new NpgsqlCommand(
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", connection))
            {
                insert.Parameters.AddWithValue(1L);
                insert.Parameters.AddWithValue(utcNow.AddDays(-40));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("timescale-e2e");
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var insert = new NpgsqlCommand(
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", connection))
            {
                insert.Parameters.AddWithValue(2L);
                insert.Parameters.AddWithValue(utcNow.AddHours(-1));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("timescale-e2e");
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* collection_log is a hypertable since V23, so in Timescale mode it purges via drop_chunks too.
               drop_chunks only drops WHOLE expired chunks, so this row must be past collection_log's own 2x
               horizon (60 days) for its 1-day chunk to be fully expired: 70 days back. (A row inside the 60-day
               window would survive — exercised on the plain-PG DELETE path in DarlingRetentionTests.) */
            using (var insert = new NpgsqlCommand(
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)", connection))
            {
                insert.Parameters.AddWithValue(1L);
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("timescale-e2e");
                insert.Parameters.AddWithValue("wait_stats");
                insert.Parameters.AddWithValue(utcNow.AddDays(-70));
                insert.Parameters.AddWithValue("SUCCESS");
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* The Timescale purge. Deliberately NO assertion on the returned global activity count
               (#1564): chunk drops are per-table + time-window across the WHOLE shared store, so sibling
               collection classes' rows make the global number order-dependent. The contract is the
               OWN-SCOPED evidence below: this server's fresh row survives, its old rows are gone — plus
               the is-hypertable assertions above proving the drop_chunks branch was in play. If
               drop_chunks transiently fails (e.g. a lock clash with the shared fixture's compression
               policy jobs, which run mid-suite), the time-sliced DELETE fallback now clears the rows even
               inside a compressed chunk — the capturing logger surfaces any such fallback in the failure
               text instead of silencing it (a silent skip was #1564's whole failure mode). */
            var purgeLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, purgeLog, ct);

            using (var read = new NpgsqlCommand(
                "SELECT collection_time FROM wait_stats WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), $"the fresh wait_stats row did not survive the drop_chunks purge; {purgeLog.Joined}");
                var survivor = reader.GetDateTime(0);
                Assert.True(survivor > utcNow.AddDays(-1), $"the surviving row should be the 1-hour one, got {survivor:O}; {purgeLog.Joined}");
                Assert.False(await reader.ReadAsync(ct), $"the 40-day wait_stats row survived the drop_chunks purge; {purgeLog.Joined}");
            }

            /* The 70-day collection_log row went — via drop_chunks (past the 60-day horizon), or via the
               DELETE fallback if drop_chunks transiently failed. */
            using (var read = new NpgsqlCommand(
                "SELECT COUNT(*) FROM collection_log WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                var remaining = (long)(await read.ExecuteScalarAsync(ct))!;
                Assert.True(remaining == 0L, $"the 70-day collection_log row survived the purge ({remaining} row(s)); {purgeLog.Joined}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task EndToEnd_CompressionPolicyApplies_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compression test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        /* Compression needs hypertables first — idempotent, so safe regardless of test order. */
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));

        /* Applies cleanly and idempotently (the second pass re-runs ALTER SET and the policy
           no-ops on if_not_exists). */
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct));
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct));

        /* The background job really exists. proc_name is 'policy_compression' on the long-stable
           API; the LIKE also tolerates the 2.18+ columnstore rebrand's naming. */
        using (var job = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM timescaledb_information.jobs
WHERE hypertable_name = 'wait_stats'
  AND (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection))
        {
            var jobs = (long)(await job.ExecuteScalarAsync(ct))!;
            Assert.True(jobs >= 1, "expected a compression policy job on wait_stats in timescaledb_information.jobs");
        }

        /* Deliberately NO policy removal on cleanup: the applied policies are the service's
           real end state on this fixture, and if_not_exists keeps every rerun a no-op. */
    }

    /* ---------------- the compression TICK (#1778) ---------------- */

    /// <summary>
    /// The converge statements, pinned ungated. Scope is the thing that matters: a statement that retunes
    /// "policy jobs" instead of COMPRESSION policy jobs would silently re-cadence the retention policies, whose
    /// armed/paused state is #1680's whole guarantee.
    /// </summary>
    [Fact]
    public void CompressionScheduleConverge_ScopesToCompressionJobs_AndCastsJobIdToInteger()
    {
        var probe = TimescaleSupport.StaleCompressionScheduleSql;

        /* The same tolerant proc_name scoping the stuck-job reader uses — 'policy_compression' plus the 2.18+
           columnstore rebrand — and NOTHING else. */
        Assert.Contains("proc_name LIKE '%compression%'", probe, StringComparison.Ordinal);
        Assert.Contains("proc_name LIKE '%columnstore%'", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("policy_retention", probe, StringComparison.Ordinal);

        /* Compared as a typed INTERVAL by PostgreSQL, not as text in C#: '01:00:00' and '1 hour' are the same
           interval and must not be seen as a difference to converge on every single start. */
        Assert.Contains($"IS DISTINCT FROM INTERVAL '{TimescaleSupport.CompressScheduleInterval}'", probe, StringComparison.Ordinal);

        /* #1586: alter_job takes job_id INTEGER and PostgreSQL will not down-cast a bigint bind during function
           resolution — an un-cast parameter fails 42883 at runtime while every string pin still passes. */
        Assert.Equal(
            "SELECT alter_job($1::integer, schedule_interval => INTERVAL '1 hour')",
            TimescaleSupport.SetCompressionScheduleSql);
    }

    /// <summary>
    /// THE COUPLED CONSTANT (#1778). <see cref="TimescaleSupport.StuckRunningBound"/> is
    /// <c>max(2x schedule_interval, floor)</c>, so shortening the tick shortens the bound. While the interval was
    /// TimescaleDB's 12-hour default the first term dominated at 24h; at a 1-hour tick the FLOOR is the only
    /// thing left holding it up. If the floor is not raised to compensate, the #1581 self-heal starts re-arming
    /// compressions that are legitimately running — the field measured a query_stats compression still going at
    /// 1h33m and characterized them as hours-long.
    /// </summary>
    [Fact]
    public void StuckRunningBound_StaysAboveTheFieldsLongestLegitimateRun_UnderTheShorterTick()
    {
        var tick = TimeSpan.FromHours(1);
        Assert.Equal("1 hour", TimescaleSupport.CompressScheduleInterval);

        /* The longest legitimately-running compression #1778 recorded on the field box. The bound must clear it
           with headroom, or the self-heal fights a healthy store. */
        var longestObservedFieldRun = TimeSpan.FromMinutes(93);
        var bound = TimescaleSupport.StuckRunningBound(tick);

        Assert.True(bound > longestObservedFieldRun,
            $"the stuck-Running bound at a {tick} tick is {bound}, which would flag the field's own {longestObservedFieldRun} compression as hung");

        /* Specifically: the floor now dominates, and 2x the tick does not. */
        Assert.Equal(TimeSpan.FromHours(6), bound);
        Assert.Equal(TimeSpan.FromHours(6), TimescaleSupport.StuckRunningBound(null));

        /* Still derived from the interval when the interval is the larger term — the shape is unchanged. */
        Assert.Equal(TimeSpan.FromHours(24), TimescaleSupport.StuckRunningBound(TimeSpan.FromHours(12)));
    }

    /// <summary>The activity record's pure logic: a running job reports its elapsed time; a finished one reports
    /// nothing to watch; store/service clock skew can never produce a negative elapsed.</summary>
    [Fact]
    public void CompressionActivity_ReportsElapsedForRunningJobsOnly_AndClampsClockSkew()
    {
        var now = new DateTime(2026, 7, 27, 12, 0, 0, DateTimeKind.Utc);

        var running = new CompressionActivity("query_stats", "Running", now.AddMinutes(-93), null, 2);
        Assert.True(running.IsRunning);
        Assert.Equal(TimeSpan.FromMinutes(93), running.RunningFor(now));

        var scheduled = new CompressionActivity("wait_stats", "Scheduled", now.AddMinutes(-5), TimeSpan.FromSeconds(12), 0);
        Assert.False(scheduled.IsRunning);
        Assert.Null(scheduled.RunningFor(now));

        /* A store clock slightly ahead of the service must read as "just started", never as negative. */
        var skewed = new CompressionActivity("procedure_stats", "Running", now.AddMinutes(3), null, 0);
        Assert.Equal(TimeSpan.Zero, skewed.RunningFor(now));

        /* A running job that never recorded a start has nothing to report rather than a bogus elapsed. */
        Assert.Null(new CompressionActivity("deadlocks", "Running", null, null, 0).RunningFor(now));

        /* THE SEAM WITH #1760. TimescaleDB's never-ran sentinel is -infinity, which Npgsql maps to
           DateTime.MinValue, and job_status comes from pg_stat_activity INDEPENDENTLY of the start time — so a
           policy's very first run genuinely reads Running with a MinValue start. That must read as "no elapsed
           to report", never as ~739,000 days. Note the zero-clamp cannot catch this: the sentinel yields a huge
           POSITIVE elapsed, so it needs its own guard. */
        var neverRan = new CompressionActivity("query_stats", "Running", DateTime.MinValue, null, 1);
        Assert.True(neverRan.IsRunning);
        Assert.Null(neverRan.RunningFor(now));
    }

    /// <summary>
    /// The #1760 guard belongs on BOTH compression queries, and this pins that it is on the one #1778 added.
    /// The two were written in parallel branches and each was green alone; only the seam between them is where
    /// the never-ran sentinel leaks back in.
    /// </summary>
    [Fact]
    public void CompressionActivitySql_GuardsTheNeverRanSentinel_LikeTheStuckJobQuery()
    {
        /* EVERY read of the column is guarded, not merely the first one. Asserting Contains would pass while an
           unguarded read sat further down the statement — which is exactly the state this query was in when the
           SELECT list had the NULLIF and the duration CASE two lines below still read the column raw. Counting
           makes the pin cover what it looks like it covers. */
        foreach (var (name, sql) in new[]
        {
            ("CompressionActivitySql", TimescaleSupport.CompressionActivitySql),
            ("StuckCompressionJobsSql", TimescaleSupport.StuckCompressionJobsSql),
        })
        {
            var reads = CountOccurrences(sql, "js.last_run_started_at");
            var guarded = CountOccurrences(sql, "NULLIF(js.last_run_started_at, '-infinity'::timestamptz)");

            Assert.True(reads > 0, $"{name} is expected to read last_run_started_at at all");
            Assert.True(reads == guarded,
                $"{name} reads js.last_run_started_at {reads} time(s) but guards it {guarded} time(s) — an unguarded read lets TimescaleDB's -infinity never-ran sentinel through as DateTime.MinValue");
        }
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal); at >= 0;
             at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// THE LATENCY PIN (#1778), live. Two halves, and the second is the one that reaches real deployments:
    /// <list type="number">
    /// <item>A policy created by the product carries a 1-hour <c>schedule_interval</c> — not TimescaleDB's
    /// computed default, which this test first MEASURES as 12 hours on a 1-day-chunk hypertable so the pin
    /// rests on the shipped extension's behavior rather than on documentation.</item>
    /// <item>A store that already has a 12-hour policy is CONVERGED to 1 hour. <c>add_compression_policy</c>
    /// with <c>if_not_exists => true</c> returns -1 and skips against an existing policy, so without the
    /// converge every store that ever ran an older build — including the one #1778 was reported from — keeps
    /// the twice-daily tick forever.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task EndToEnd_CompressionTick_IsHourly_AndConvergesAnExistingTwelveHourPolicy_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compression-tick test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        const string Legacy = "tick1778_legacy";
        const string Fresh = "tick1778_fresh";

        var bodySucceeded = false;
        try
        {
            await DropTickTableAsync(connection, Legacy, ct);
            await DropTickTableAsync(connection, Fresh, ct);

            /* (1) A store as an OLDER BUILD left it: the policy created with no schedule_interval at all. */
            await CreateTickTableAsync(connection, Legacy, ct);
            await ExecAsync(connection,
                $"SELECT add_compression_policy('collect.{Legacy}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days', if_not_exists => true)", ct);

            /* MEASURED, not assumed: this is the twice-daily tick the field reported, and it is TimescaleDB's
               default for a 1-day chunk interval rather than anything the product chose. */
            Assert.Equal(TimeSpan.FromHours(12), await ScheduleIntervalAsync(connection, Legacy, ct));

            /* The create statement ALONE cannot fix it — if_not_exists skips an existing policy outright. */
            await ExecAsync(connection, TimescaleSupport.AddCompressionPolicySql($"collect.{Legacy}"), ct);
            Assert.Equal(TimeSpan.FromHours(12), await ScheduleIntervalAsync(connection, Legacy, ct));

            /* (2) A fresh policy the product creates carries the tick from the start. */
            await CreateTickTableAsync(connection, Fresh, ct);
            await ExecAsync(connection, TimescaleSupport.AddCompressionPolicySql($"collect.{Fresh}"), ct);
            Assert.Equal(TimeSpan.FromHours(1), await ScheduleIntervalAsync(connection, Fresh, ct));

            /* THE ASSERTION THE WHOLE ISSUE COMES DOWN TO: the legacy store converges. Logged through a
               capturing logger because that line IS the operator's evidence the cadence changed on their box,
               and a structured-logging placeholder/argument mismatch would render it wrong with no error
               anywhere — the one defect that cannot be caught by asserting on the return value. */
            var convergeLog = new CapturingTestLogger();
            var converged = await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, convergeLog, ct);
            Assert.True(converged >= 1, $"expected the 12-hour legacy policy to be retuned; {convergeLog.Joined}");
            Assert.Equal(TimeSpan.FromHours(1), await ScheduleIntervalAsync(connection, Legacy, ct));

            /* The rendered line names the table, the cadence it LEFT, and the cadence it is on now — all three,
               or an operator cannot tell a converged store from an untouched one. */
            Assert.Contains($"retuned {Legacy}'s compression policy from a 12:00:00 tick to 1 hour",
                convergeLog.Joined, StringComparison.Ordinal);

            /* Idempotent: a converged store finds nothing on the next start, so this never churns alter_job. */
            Assert.Equal(0, await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, null, ct));

            /* Scope: retention policies keep their own cadence. Retuning those would re-cadence the armed/paused
               machinery #1680 depends on.

               The test plants its OWN retention policy rather than asserting over whatever the shared fixture
               happens to hold: "no policy_retention job has a 1-hour cadence" is trivially satisfied on a store
               with no retention jobs at all, so without this the scope check could pass while proving nothing. */
            await ExecAsync(connection,
                $"SELECT add_retention_policy('collect.{Fresh}', drop_after => INTERVAL '30 days', if_not_exists => true)", ct);

            var retentionBefore = await RetentionScheduleIntervalAsync(connection, Fresh, ct);
            Assert.NotNull(retentionBefore);
            Assert.NotEqual(TimeSpan.FromHours(1), retentionBefore);

            /* Converging again with a retention policy present must leave it exactly as it was. */
            await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, null, ct);
            Assert.Equal(retentionBefore, await RetentionScheduleIntervalAsync(connection, Fresh, ct));

            using var retention = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention'
AND   schedule_interval = INTERVAL '1 hour'", connection);
            Assert.Equal(0L, (long)(await retention.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DropTickTableAsync(cleanup, Legacy, cleanupCt);
                await DropTickTableAsync(cleanup, Fresh, cleanupCt);
            });
        }
    }

    /// <summary>
    /// The CHECK logic the tick drives (#1778): when a compression policy fires it compresses the chunks that
    /// are closed AND past the eligibility delay, and leaves a chunk that is still too young alone. Runs the
    /// real policy body via <c>run_job</c> rather than waiting out a schedule — the cadence is pinned separately
    /// by the latency test, and this pins what a firing actually does.
    ///
    /// <para>This is also the guard on the boundary between the two levers: #1768's eligibility DELAY decides
    /// WHAT is eligible and this change only decides WHEN eligible chunks get taken. A change that compressed
    /// the young chunk would be reaching across that line.</para>
    /// </summary>
    [Fact]
    public async Task EndToEnd_CompressionRun_TakesEligibleChunksAndLeavesYoungOnes_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compression-run test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        const string Table = "tick1778_eligibility";

        var bodySucceeded = false;
        try
        {
            await DropTickTableAsync(connection, Table, ct);
            await CreateTickTableAsync(connection, Table, ct);

            /* Policy first, parked, while there is nothing to compress — see AddCompressionPolicyParkedAsync.
               Adding it after the inserts hands the scheduler two eligible chunks and a head start on the
               assertions below. */
            await AddCompressionPolicyParkedAsync(connection, Table, ct);

            /* One chunk five days back (closed, well past the 1-day delay) and one from midday TODAY (still
               open, and young either way).

               MIDDAY, not now() (#1972). These seeds span 200 seconds, and the chunks are day-aligned
               (ChunkIntervalDays), so a now()-relative span straddles a chunk boundary whenever the suite runs
               within ~200 seconds of midnight: the one intended chunk becomes two and the counts below fail
               deterministically. Both directions were real — a young seed run in 00:00:00-00:03:20 split into
               yesterday's tail plus today's and failed the uncompressed count (caught live at 00:01:38 UTC),
               and an old seed run in 23:56:40-23:59:59 split into two closed, both-eligible chunks and failed
               the compressed count. Anchoring to midday puts 200 seconds of slack against 12 hours of margin
               on either side, so chunk placement no longer depends on what time the suite runs. */
            await ExecAsync(connection,
                $"INSERT INTO collect.{Table} SELECT date_trunc('day', now()::timestamp) - INTERVAL '5 days' + INTERVAL '12 hours' + (g || ' seconds')::interval, {TestServerId}, g FROM generate_series(1, 200) g", ct);
            await ExecAsync(connection,
                $"INSERT INTO collect.{Table} SELECT date_trunc('day', now()::timestamp) + INTERVAL '12 hours' + (g || ' seconds')::interval, {TestServerId}, g FROM generate_series(1, 200) g", ct);

            await RunPolicyAsync(connection, Table, ct);

            Assert.Equal(1, await ChunkCountAsync(connection, Table, compressed: true, ct));
            Assert.Equal(1, await ChunkCountAsync(connection, Table, compressed: false, ct));

            /* And the one left uncompressed is the YOUNG one, not an arbitrary survivor. The midday anchor
               keeps this true with room to spare: midday today sits in today's chunk, whose range_end is
               tomorrow's midnight, so range_end > now() - CompressAfterDays holds at every hour of the day. */
            using var young = new NpgsqlCommand($@"
SELECT COUNT(*)
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect'
AND   hypertable_name = '{Table}'
AND   NOT is_compressed
AND   range_end > now() - INTERVAL '{TimescaleSupport.CompressAfterDays} days'", connection);
            Assert.Equal(1L, (long)(await young.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropTickTableAsync(cleanup, Table, cleanupCt));
        }
    }

    /// <summary>
    /// FAILURE ISOLATION at the chunk level (#1778, the #1775 shape): one chunk that cannot be compressed must
    /// not stop the policy from compressing the rest. Made deterministic by holding ACCESS EXCLUSIVE on the
    /// middle chunk from a second connection while the policy runs under a short <c>lock_timeout</c> — the
    /// compression of that chunk fails for real, exactly as a chunk locked by live collection would.
    /// </summary>
    [Fact]
    public async Task EndToEnd_CompressionRun_OneUncompressibleChunkDoesNotBlockTheRest_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compression-isolation test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        const string Table = "tick1778_isolation";

        var bodySucceeded = false;
        try
        {
            await DropTickTableAsync(connection, Table, ct);
            await CreateTickTableAsync(connection, Table, ct);

            /* Policy first, parked, while there is nothing to compress. This test is the one the unparked
               scheduler actually broke on CI, in both directions — see AddCompressionPolicyParkedAsync. */
            await AddCompressionPolicyParkedAsync(connection, Table, ct);

            /* Three chunks, all eligible. Midday-anchored, not now()-relative (#1972): run in the last ~200
               seconds before midnight, a now()-relative seed crosses the day boundary N days back and lands
               FOUR chunks here instead of three, failing the counts below. */
            foreach (var daysBack in new[] { 7, 5, 3 })
            {
                await ExecAsync(connection,
                    $"INSERT INTO collect.{Table} SELECT date_trunc('day', now()::timestamp) - INTERVAL '{daysBack} days' + INTERVAL '12 hours' + (g || ' seconds')::interval, {TestServerId}, g FROM generate_series(1, 200) g", ct);
            }

            string middleChunk;
            using (var probe = new NpgsqlCommand($@"
SELECT chunk_schema || '.' || chunk_name
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}'
ORDER BY range_start
OFFSET 1 LIMIT 1", connection))
            {
                middleChunk = (string)(await probe.ExecuteScalarAsync(ct))!;
            }

            /* A second connection holds the middle chunk; its transaction is rolled back in the finally. */
            using var blocker = new NpgsqlConnection(connectionString);
            await blocker.OpenAsync(ct);
            await using var blocking = await blocker.BeginTransactionAsync(ct);
            using (var take = new NpgsqlCommand($"LOCK TABLE {middleChunk} IN ACCESS EXCLUSIVE MODE", blocker, blocking))
            {
                await take.ExecuteNonQueryAsync(ct);
            }

            await ExecAsync(connection, "SET lock_timeout = '3s'", ct);
            var policyRan = false;
            try
            {
                await RunPolicyAsync(connection, Table, ct);
                policyRan = true;
            }
            finally
            {
                /* RunOwnedAsync, not RunAsync (#1896): both statements MUST run on the connections this test
                   already holds. lock_timeout is a session setting, so resetting it on a fresh connection would
                   leave the real session at 3s; and the lock can only be released by the transaction holding
                   it. What is borrowed is the masking rule — if RunPolicyAsync threw, that is the failure worth
                   reporting, and a broken session throwing again here must not stand in front of it. */
                await LiveStoreCleanup.RunOwnedAsync(policyRan, async () =>
                {
                    await ExecAsync(connection, "SET lock_timeout = 0", ct);
                    await blocking.RollbackAsync(ct);
                });
            }

            /* THE PIN: the two reachable chunks compressed anyway. A run that aborted on the first failure
               would leave one or zero. */
            Assert.Equal(2, await ChunkCountAsync(connection, Table, compressed: true, ct));
            Assert.Equal(1, await ChunkCountAsync(connection, Table, compressed: false, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropTickTableAsync(cleanup, Table, cleanupCt));
        }
    }

    /// <summary>
    /// FAILURE ISOLATION in the converge loop itself (#1778): one policy whose <c>alter_job</c> fails — the
    /// bring-your-own-store case where the login does not own that job — must leave the remaining policies
    /// converged rather than abandoning the sweep partway. Made deterministic by row-locking one job's
    /// <c>bgw_job</c> tuple from a second connection, which is what an <c>alter_job</c> failure looks like from
    /// the caller's side.
    /// </summary>
    [Fact]
    public async Task EndToEnd_ConvergeCompressionSchedule_OneFailingJobDoesNotBlockTheRest_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live converge-isolation test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        const string Blocked = "tick1778_blocked";
        const string Reachable = "tick1778_reachable";

        var bodySucceeded = false;
        try
        {
            await DropTickTableAsync(connection, Blocked, ct);
            await DropTickTableAsync(connection, Reachable, ct);

            /* Two stores-worth of legacy policies: both created the OLD way, so both are stale. */
            foreach (var table in new[] { Blocked, Reachable })
            {
                await CreateTickTableAsync(connection, table, ct);
                await ExecAsync(connection,
                    $"SELECT add_compression_policy('collect.{table}', compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days', if_not_exists => true)", ct);
                Assert.Equal(TimeSpan.FromHours(12), await ScheduleIntervalAsync(connection, table, ct));
            }

            int blockedJobId;
            using (var probe = new NpgsqlCommand($@"
SELECT job_id
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{Blocked}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection))
            {
                blockedJobId = Convert.ToInt32((await probe.ExecuteScalarAsync(ct))!, CultureInfo.InvariantCulture);
            }

            using var blocker = new NpgsqlConnection(connectionString);
            await blocker.OpenAsync(ct);
            await using var blocking = await blocker.BeginTransactionAsync(ct);
            using (var take = new NpgsqlCommand("SELECT id FROM _timescaledb_config.bgw_job WHERE id = $1::integer FOR UPDATE", blocker, blocking))
            {
                take.Parameters.AddWithValue(blockedJobId);
                await take.ExecuteNonQueryAsync(ct);
            }

            await ExecAsync(connection, "SET lock_timeout = '3s'", ct);
            int converged;
            var convergeRan = false;
            try
            {
                converged = await TimescaleSupport.ConvergeCompressionScheduleAsync(connection, null, ct);
                convergeRan = true;
            }
            finally
            {
                /* RunOwnedAsync for the same reason as the isolation test above: a session setting and a
                   transaction's own rollback cannot be moved to a fresh connection. See #1896. */
                await LiveStoreCleanup.RunOwnedAsync(convergeRan, async () =>
                {
                    await ExecAsync(connection, "SET lock_timeout = 0", ct);
                    await blocking.RollbackAsync(ct);
                });
            }

            /* THE PIN: the reachable policy converged even though the other one threw. A sweep that let the
               first failure escape would leave this at 12 hours. */
            Assert.Equal(TimeSpan.FromHours(1), await ScheduleIntervalAsync(connection, Reachable, ct));
            Assert.True(converged >= 1, "expected the reachable policy to be counted as converged");

            /* And the failure is reported honestly rather than counted as success: the blocked one is untouched
               and still stale, so the next start retries it. */
            Assert.Equal(TimeSpan.FromHours(12), await ScheduleIntervalAsync(connection, Blocked, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DropTickTableAsync(cleanup, Blocked, cleanupCt);
                await DropTickTableAsync(cleanup, Reachable, cleanupCt);
            });
        }
    }

    /// <summary>Live activity read (#1778): the observability surface returns a row per compression policy and
    /// reports the eligible-but-uncompressed backlog that the tick exists to keep at zero.</summary>
    [Fact]
    public async Task EndToEnd_CompressionActivity_ReportsBacklogAndClearsAfterARun_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compression-activity test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        const string Table = "tick1778_activity";

        var bodySucceeded = false;
        try
        {
            await DropTickTableAsync(connection, Table, ct);
            await CreateTickTableAsync(connection, Table, ct);

            /* Policy first, parked, while there is nothing to compress. The backlog assertion below is the most
               exposed of the three: it reads EligibleUncompressedChunks BEFORE running the job, so a background
               run that got there first would report a settled zero and fail the test for being correct. */
            await AddCompressionPolicyParkedAsync(connection, Table, ct);

            /* Midday-anchored, not now()-relative (#1972): in the last ~200 seconds before midnight a
               now()-relative seed crosses the day boundary five days back and puts TWO eligible chunks in the
               backlog, failing the count below. */
            await ExecAsync(connection,
                $"INSERT INTO collect.{Table} SELECT date_trunc('day', now()::timestamp) - INTERVAL '5 days' + INTERVAL '12 hours' + (g || ' seconds')::interval, {TestServerId}, g FROM generate_series(1, 200) g", ct);

            var before = await TimescaleSupport.ReadCompressionActivityAsync(connection, null, ct);
            var waiting = Assert.Single(before, a => string.Equals(a.HypertableName, Table, StringComparison.Ordinal));
            Assert.Equal(1, waiting.EligibleUncompressedChunks);

            await RunPolicyAsync(connection, Table, ct);

            var after = await TimescaleSupport.ReadCompressionActivityAsync(connection, null, ct);
            var settled = Assert.Single(after, a => string.Equals(a.HypertableName, Table, StringComparison.Ordinal));
            Assert.Equal(0, settled.EligibleUncompressedChunks);

            /* Logging must survive every shape the store can hand back, including a null logger. */
            TimescaleSupport.LogCompressionActivity(after, DateTime.UtcNow, null);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropTickTableAsync(cleanup, Table, cleanupCt));
        }
    }

    /// <summary>
    /// THE REFRESH-WINDOW PIN (#3012), live — the half that reaches already-deployed stores, which is the
    /// only population the incident was reported from.
    ///
    /// <para><b>What it would mean for this test not to exist.</b>
    /// <c>add_continuous_aggregate_policy(if_not_exists =&gt; true)</c> returns -1 against a policy the store
    /// already has and changes nothing about it, so a store still on the 3-day window keeps re-materializing
    /// three days every hour and the fix is INERT while the whole suite stays green. That failure direction is
    /// worse than a red one, not milder: an inert fix with a passing suite is a fix nobody looks at again.
    /// String-shape assertions on the SQL cannot see it, because they never run the statement.</para>
    ///
    /// <para><b>The four assumptions only a live store can settle</b>, all named as unverified when the change
    /// was written: that <c>alter_job</c> takes <c>fixed_schedule</c> and <c>initial_start</c> together on this
    /// runtime; that <c>jsonb_set</c> produces a <c>start_offset</c> TimescaleDB then honours; that the
    /// <c>jobs</c> → <c>continuous_aggregates</c> join on materialization-hypertable schema/name recovers
    /// <c>view_name</c> for a refresh policy; and that a policy CREATED with <c>initial_start</c> comes back
    /// <c>fixed_schedule = true</c> — on which the converge's own no-op-ness rests, because a fresh store whose
    /// jobs read <c>false</c> would be re-altered on every start forever.</para>
    ///
    /// <para><b>The fixed schedule is not a refinement.</b> Measured on the production store roughly two hours
    /// after the offsets were applied by hand: every job read <c>fixed_schedule = f</c> and only one of six
    /// hourly refreshes was still on the minute it was set to. TimescaleDB computes the next start from the
    /// previous FINISH when <c>initial_start</c> is absent, so a hand-applied stagger decays back into
    /// coincidence within hours — correct at apply time and gone by the afternoon. A stagger that drifts is not
    /// a stagger, which is why <c>fixed_schedule</c> is asserted here rather than only the minute.</para>
    /// </summary>
    [Fact]
    public async Task EndToEnd_HourlyRefreshWindow_ConvergesAThreeDayFinishToStartPolicy_AndLeavesTheDailyTierAlone_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live refresh-window converge test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Two REAL views, because the converge is scoped by membership of HourlyRefreshPhaseOrder and a
           throwaway name would be skipped — which is a property worth having, and is asserted at the end. */
        const string Hourly = TimescaleSupport.QueryStatsHourlyView;
        const string Daily = TimescaleSupport.QueryStatsDailyView;

        /* This test MUTATES the shared fixture's shape (creating the hourly/daily CAGGs changes compose's tier
           routing), so it restores it: snapshot what already exists and drop only what it creates. */
        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var createLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, createLog, ct);

            /* ---- (1) A policy as an OLDER BUILD left it: 3-day window, no initial_start. ---- */
            await ExecAsync(connection, $"SELECT remove_continuous_aggregate_policy('collect.{Hourly}', if_exists => true)", ct);
            await ExecAsync(connection,
                $"SELECT add_continuous_aggregate_policy('collect.{Hourly}', start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', if_not_exists => true)", ct);

            var legacy = await RefreshPolicyStateAsync(connection, Hourly, ct);
            Assert.NotNull(legacy);
            Assert.Equal(TimeSpan.FromDays(3).TotalSeconds, legacy!.StartOffsetSeconds);

            /* MEASURED, not assumed: omitting initial_start is what leaves a job on finish-to-start
               scheduling, which is the mechanism by which the production store's hand-set offsets drifted. */
            Assert.False(legacy.FixedSchedule,
                "a policy created without initial_start is expected to be finish-to-start on this runtime");

            /* ---- (2) The CREATE statement ALONE cannot fix it — if_not_exists skips outright. ---- */
            await ExecAsync(connection, TimescaleSupport.AddHourlyRefreshPolicySql(Hourly), ct);
            var afterCreate = await RefreshPolicyStateAsync(connection, Hourly, ct);
            Assert.Equal(TimeSpan.FromDays(3).TotalSeconds, afterCreate!.StartOffsetSeconds);
            Assert.False(afterCreate.FixedSchedule);

            /* ---- (3) THE ASSERTION THE WHOLE CHANGE COMES DOWN TO: the deployed store converges. ---- */
            var convergeLog = new CapturingTestLogger();
            var converged = await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, convergeLog, ct);
            Assert.True(converged >= 1,
                $"expected the 3-day finish-to-start policy on {Hourly} to be moved; {convergeLog.Joined}");

            var moved = await RefreshPolicyStateAsync(connection, Hourly, ct);
            Assert.NotNull(moved);
            Assert.Equal(TimescaleSupport.HourlyRefreshStartSpan.TotalSeconds, moved!.StartOffsetSeconds);
            Assert.True(moved.FixedSchedule,
                $"the converge must pin the schedule, or the phase drifts back to coincidence within hours; {convergeLog.Joined}");
            Assert.Equal(TimescaleSupport.RefreshPhaseMinutesFor(Hourly), moved.PhaseMinutes);

            /* end_offset must survive untouched — the converge writes start_offset with jsonb_set against the
               job's OWN config, so losing a sibling key here would mean it replaced the config wholesale. */
            Assert.Equal(TimeSpan.FromHours(1), moved.EndOffset);

            /* The rendered line names the view and the window it is on now, because that line IS the
               operator's evidence the store changed. A structured-logging placeholder/argument mismatch would
               render it wrong with no error anywhere — the one defect asserting on the return value cannot see. */
            Assert.Contains($"moved {Hourly}'s refresh policy to a {TimescaleSupport.HourlyRefreshStartOffset} window",
                convergeLog.Joined, StringComparison.Ordinal);

            /* ---- (4) Idempotent: a converged store finds nothing, so this never churns alter_job. ---- */
            Assert.Equal(0, await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, null, ct));

            /* ---- (5) The CREATE path on a store with no policy yet: both halves from the start. ---- */
            await ExecAsync(connection, $"SELECT remove_continuous_aggregate_policy('collect.{Hourly}', if_exists => true)", ct);
            await ExecAsync(connection, TimescaleSupport.AddHourlyRefreshPolicySql(Hourly), ct);

            var fresh = await RefreshPolicyStateAsync(connection, Hourly, ct);
            Assert.NotNull(fresh);
            Assert.Equal(TimescaleSupport.HourlyRefreshStartSpan.TotalSeconds, fresh!.StartOffsetSeconds);
            Assert.True(fresh.FixedSchedule,
                "passing initial_start is expected to put the job on a fixed schedule; if it does not, the converge re-alters every hourly policy on every start");
            Assert.Equal(TimescaleSupport.RefreshPhaseMinutesFor(Hourly), fresh.PhaseMinutes);

            /* And therefore the converge is a no-op against a FRESH store too, not only a settled one. */
            Assert.Equal(0, await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, null, ct));

            /* ---- (6) SCOPE: the DAILY tier keeps its 3-day window and its finish-to-start schedule. ---- */
            var daily = await RefreshPolicyStateAsync(connection, Daily, ct);
            Assert.NotNull(daily);
            Assert.Equal(TimeSpan.FromDays(3).TotalSeconds, daily!.StartOffsetSeconds);
            Assert.False(daily.FixedSchedule, "the daily tier is deliberately left on finish-to-start scheduling");

            /* Deliberately AFTER a converge has run and reported 0: "the daily policy is still on 3 days" is
               trivially true if the converge never looked at anything, so the check has to follow a pass that
               did look at the hourly policies and chose not to touch this one. */
            await ExecAsync(connection,
                $@"SELECT alter_job(j.job_id, config => jsonb_set(j.config, '{{start_offset}}', to_jsonb('3 days'::text)))
FROM timescaledb_information.jobs AS j
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  ca.materialization_hypertable_schema = j.hypertable_schema
  AND ca.materialization_hypertable_name = j.hypertable_name
WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
AND   ca.view_schema = 'collect'
AND   ca.view_name = '{Daily}'", ct);

            Assert.Equal(TimeSpan.FromDays(3).TotalSeconds,
                (await RefreshPolicyStateAsync(connection, Daily, ct))!.StartOffsetSeconds);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropContinuousAggregatesAsync(
                    (await ExistingCaggsAsync(cleanup, cleanupCt)).Except(preexistingCaggs, StringComparer.Ordinal), cleanupCt));
        }
    }

    /// <summary>One continuous aggregate's live refresh-policy state, read the same way
    /// <see cref="TimescaleSupport.ContinuousAggregateRefreshStateSql"/> reads it — by VIEW NAME, recovered
    /// through the materialization-hypertable join, never by job id.</summary>
    private sealed record RefreshPolicyState(double StartOffsetSeconds, bool FixedSchedule, int? PhaseMinutes, TimeSpan? EndOffset);

    private static async Task<RefreshPolicyState?> RefreshPolicyStateAsync(
        NpgsqlConnection connection, string view, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
SELECT
    EXTRACT(EPOCH FROM (j.config->>'start_offset')::interval)::double precision,
    j.fixed_schedule,
    CASE
        WHEN j.initial_start IS NULL THEN NULL
        ELSE EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int
    END,
    (j.config->>'end_offset')::interval
FROM timescaledb_information.jobs AS j
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  ca.materialization_hypertable_schema = j.hypertable_schema
  AND ca.materialization_hypertable_name = j.hypertable_name
WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
AND   ca.view_schema = 'collect'
AND   ca.view_name = '{view}'", connection);

        using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return new RefreshPolicyState(
            reader.GetDouble(0),
            !reader.IsDBNull(1) && reader.GetBoolean(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetFieldValue<TimeSpan>(3));
    }

    /* ---- #1778 live-test helpers: throwaway hypertables shaped like a collector table ---- */

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task CreateTickTableAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        await ExecAsync(connection,
            $"CREATE TABLE collect.{table} (collection_time timestamp NOT NULL, server_id integer NOT NULL, value bigint)", ct);
        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql($"collect.{table}", "collection_time"), ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{table}"), ct);
    }

    /* DROP TABLE takes the hypertable, its chunks and its compression policy with it — and #1873 makes the
       removal verified, so a leftover tick table cannot masquerade as a clean teardown. */
    private static async Task DropTickTableAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
        => await new LiveCleanupBatch(connection).DropTableAsync(table, ct);

    private static async Task<TimeSpan?> ScheduleIntervalAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
SELECT schedule_interval
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{table}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection);
        return await command.ExecuteScalarAsync(ct) is TimeSpan interval ? interval : null;
    }

    /* The RETENTION policy's cadence on a table — the scope control for the compression converge. */
    private static async Task<TimeSpan?> RetentionScheduleIntervalAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
SELECT schedule_interval
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{table}'
AND   proc_name = 'policy_retention'", connection);
        return await command.ExecuteScalarAsync(ct) is TimeSpan interval ? interval : null;
    }

    /* Runs the policy body NOW instead of waiting out its schedule. job_id is INTEGER (#1586). */
    /// <summary>
    /// Adds the PRODUCT's compression policy and immediately PARKS the job it creates, so the only thing that
    /// ever compresses these chunks is the test's own <see cref="RunPolicyAsync"/> call.
    ///
    /// <para><b>MUST be called before the table has any eligible chunk.</b> That is not tidiness, it is the
    /// half of this that closes the race. <c>add_compression_policy</c> creates the job SCHEDULED with no
    /// <c>initial_start</c>, and TimescaleDB launches it within a second or two — measured on 2.28.1: against a
    /// hypertable holding three eligible chunks, adding the policy and then doing nothing at all compressed all
    /// three inside six seconds, with <c>run_job</c> never called. Parking a job that has already launched does
    /// not recall the run in flight; adding the policy while there is nothing to compress does, because that
    /// run finds an empty chunk list and the park stops every run after it.</para>
    ///
    /// <para><b>What that background run did to these tests.</b> It has no <c>lock_timeout</c> (the default is
    /// wait-forever), so in the isolation test it queued behind the ACCESS EXCLUSIVE lock the test takes on the
    /// middle chunk, and compressed that chunk the instant the test rolled the blocker back — landing directly
    /// on the assertions. CI caught it twice, in both of its arms: <c>Expected 2 / Actual 3</c> when the
    /// background run beat the first assertion, and <c>Expected 1 / Actual 0</c> when the chunk flipped BETWEEN
    /// the two reads, so neither count saw it. Same cause, two unrecognizably different failures, on a test
    /// whose subject was never involved.</para>
    ///
    /// <para><b>Why it hid so well.</b> Whether the job launches at all depends on a free background-worker
    /// slot, and this repo's <c>pg-runtime</c> sets <c>timescaledb.max_background_workers = 16</c> against
    /// PostgreSQL's <c>max_worker_processes = 8</c> — so launches routinely fail outright ("failed to start a
    /// background worker" in the server log) and the race simply does not happen. Twenty consecutive local runs
    /// passed for that reason alone; raising <c>max_worker_processes</c> on the same rig made the background
    /// run fire every time. The suite was not immune, it was under-resourced.</para>
    ///
    /// <para>The idiom is already in this file — the #1760 stuck-sentinel probe parks its job "so it cannot run
    /// mid-assertion" — and is the same lever <c>PayloadDimensionLiveTests.EnsureAggregatesWithoutPoliciesAsync</c>
    /// pulls against the identical #1788 behaviour on the aggregate side. <c>run_job</c> still executes a parked
    /// job, verified live, so the deterministic foreground path these tests are built on is unaffected.</para>
    /// </summary>
    private static async Task AddCompressionPolicyParkedAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        /* Create and park in ONE transaction (#1888), the same lever the product pulls for retention
           policies (#1705, EnsureRetentionPoliciesAsync): "the only way to never expose an armed job is to
           keep the bgw_job row invisible until it already reads scheduled = false — the scheduler is a
           separate backend and cannot see an uncommitted row."

           As two autocommit statements this left a real window. add_compression_policy creates the job
           SCHEDULED and TimescaleDB launches it within a second or two (#1788), so the scheduler could take
           it between the create and the park — and parking a job that has ALREADY launched does not recall
           the run in flight (#1874). The launched run then evaluates its body when it gets a worker, which
           under full-suite load is late enough that the test's rows have landed, so it compresses chunks the
           test is about to count and the deterministic run_job below is no longer the only thing that
           compressed. That is why these tests passed alone and in their own class but failed in the full
           suite once #1888 gave the scheduler enough workers to launch reliably: more parallel load widens
           the launch-to-execute gap, and more slots make the launch itself certain. */
        await using var tx = await connection.BeginTransactionAsync(ct);

        using (var create = new NpgsqlCommand(TimescaleSupport.AddCompressionPolicySql($"collect.{table}"), connection, tx))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        /* Same job-lookup predicate as RunPolicyAsync, including the 2.18+ columnstore rename. The
           uncommitted job row is visible to THIS session, so the park lands on it before anyone else can
           see it armed. */
        using (var park = new NpgsqlCommand($@"
SELECT alter_job(job_id, scheduled => false, next_start => 'infinity'::timestamptz)
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{table}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection, tx))
        {
            await park.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    private static async Task RunPolicyAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        int jobId;
        using (var probe = new NpgsqlCommand($@"
SELECT job_id
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{table}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection))
        {
            jobId = Convert.ToInt32((await probe.ExecuteScalarAsync(ct))!, CultureInfo.InvariantCulture);
        }

        using var run = new NpgsqlCommand("CALL run_job($1::integer)", connection);
        run.Parameters.AddWithValue(jobId);
        await run.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> ChunkCountAsync(NpgsqlConnection connection, string table, bool compressed, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
SELECT COUNT(*)
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect'
AND   hypertable_name = '{table}'
AND   is_compressed = {(compressed ? "true" : "false")}", connection);
        return Convert.ToInt32((await command.ExecuteScalarAsync(ct))!, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// #1705: EXECUTES the retention policies instead of string-matching the SQL. The bug this replaces shipped
    /// precisely because the only pin asserted the generated string contained <c>scheduled =&gt; false</c> — an
    /// argument <c>add_retention_policy</c> has never had — so the pin passed while the statement failed 42883 on
    /// every store and the per-policy catch downgraded it to a warning. Nothing that reads a string can catch
    /// that; only running it can. Asserts every policy is created (not swallowed), lands in
    /// <c>timescaledb_information.jobs</c>, and is created PAUSED so #1680's guarantee still holds.
    /// </summary>
    [Fact]
    public async Task EndToEnd_RetentionPoliciesActuallyApply_AndAreCreatedPaused_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live retention test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));

        /* This test MUTATES the shared fixture's shape, so it restores it. Creating the hourly CAGGs changes
           compose's tier routing (RunComposedPanel_OldWindow_AgainstPlainPostgres_RunsCleanOnRaw asserts a
           10-day window lands on RAW, which only holds while no rollup exists), and leaving an ARMED raw
           retention policy behind could drop chunks another live test planted. Snapshot what already exists,
           and drop only what this test creates. */
        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* Retention targets the hourly CAGGs as well as the raw tables, so the aggregates must exist first —
               the same ordering EnsureRetentionPoliciesAsync documents. */
            /* #2818: capture, don't discard. Every failure inside EnsureRetentionPoliciesAsync lands in a
               per-relation catch that logs a warning and moves on — the right behaviour for the service, but
               with a null logger the only surviving evidence is the count, and "expected 17, got 0" cannot be
               triaged (that exact failure was written off as a flake once already). Same repair as #1564's
               purge E2Es: pass the capturing logger and fold Joined into every count assertion, so the next
               failure names the actual Postgres error in the CI log. */
            var retentionLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, retentionLog, ct);

            /* THE assertion: every policy applied. A 42883 would be caught per-policy and counted as 0. */
            var applied = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, retentionLog, ct);
            Assert.True(applied == RetentionPolicyCount,
                $"expected all {RetentionPolicyCount} retention policies to apply, got {applied} — a swallowed error means the policy SQL is invalid on this TimescaleDB; {retentionLog.Joined}");

            /* Idempotent: the second pass hits if_not_exists (job_id -1) and must not throw on alter_job(-1).
               Fresh logger — CapturingTestLogger has no reset, and EnsureRetentionPoliciesAsync always logs an
               Information summary even on success, so reusing retentionLog would bury this pass's own evidence
               under the first pass's already-explained noise (review finding on #2887). */
            var reapplyLog = new CapturingTestLogger();
            var reapplied = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, reapplyLog, ct);
            Assert.True(reapplied == RetentionPolicyCount,
                $"the idempotent second pass should count all {RetentionPolicyCount} policies, got {reapplied}; {reapplyLog.Joined}");

            using var job = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention'
AND   hypertable_schema = 'collect'", connection);
            var jobs = (long)(await job.ExecuteScalarAsync(ct))!;
            Assert.True(jobs >= RetentionPolicyCount,
                $"expected at least {RetentionPolicyCount} policy_retention jobs on collect.*, found {jobs}");

            /* Created PAUSED (#1680). The invariant that holds whether or not the safety check armed a policy:
               none may be ARMED while its source still holds rows its coverage tier does not cover. An
               un-paused creation is exactly what would violate it. */
            using var unsafeArmed = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.scheduled
AND   j.hypertable_name = 'query_stats'
AND   (SELECT min(collection_time) FROM collect.query_stats) IS NOT NULL
AND   ((SELECT min(bucket) FROM collect.query_stats_hourly) IS NULL
       OR (SELECT min(bucket) FROM collect.query_stats_hourly) > (SELECT min(collection_time) FROM collect.query_stats))", connection);
            var bad = (long)(await unsafeArmed.ExecuteScalarAsync(ct))!;
            Assert.True(bad == 0, "a retention policy is ARMED while its coverage tier does not cover everything the source holds — creation was not paused");

            bodySucceeded = true;
        }
        finally
        {
            /* Retention policies first (they reference the relations), then only the CAGGs this test created —
               DROP ... CASCADE takes each aggregate's own policy with it.

               Every removal is VERIFIED (#1873), and this is the site with the most to verify: unlike its
               sibling in PayloadDimensionLiveTests, this test calls EnsureContinuousAggregatesAsync DIRECTLY,
               so the refresh policies it attaches are still armed and still firing immediately (#1788) while
               the drops below run. DropContinuousAggregatesAsync takes each policy off before dropping its
               aggregate, which bounds the collision to the one refresh already executing; the retry then
               outlasts that. Left as it was, this finally raced an active scheduler and reported success
               either way.

               On a FRESH connection since #1896: this is also the heaviest teardown in the class — sixteen
               retention policies and up to fourteen aggregates — so it is the one with the most to lose from
               running on a session the body's failure may have closed, where every statement after the first
               would be abandoned. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);

                foreach (var relation in RetentionRelations)
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }

                await batch.DropContinuousAggregatesAsync(
                    (await ExistingCaggsAsync(cleanup, cleanupCt)).Except(preexistingCaggs, StringComparer.Ordinal), cleanupCt);
            });
        }
    }

    /// <summary>
    /// #1937's upgrade half, measured rather than assumed: <c>add_retention_policy(if_not_exists)</c> leaves an
    /// existing policy's <c>drop_after</c> alone, so a horizon change would reach fresh installs only — the
    /// sweep must CONVERGE existing policies onto the constants, and the convergence must not touch anything
    /// else about the job. Both scheduled states are proven here: a policy demoted to the old 21-day horizon
    /// while HELD comes back at 90 still held, and one demoted while ARMED comes back at 90 still armed with
    /// its <c>next_start</c> unmoved — converging a horizon must never trigger an immediate purge (#1680's
    /// never-expose-an-armed-window discipline, held through an update rather than only at creation).
    ///
    /// <para>The <c>next_start</c> half is asserted through <see cref="AssertConvergenceLeftNextStartAlone"/>
    /// rather than as raw equality, because the policy under test is deliberately ARMED and an armed
    /// TimescaleDB job is one its background scheduler may run at any moment — see that helper for the two
    /// events being separated and for what the separation cannot see (#2937).</para>
    ///
    /// <para>The settled third sweep additionally asserts the convergence reported moving NOTHING, because
    /// that is the only observable difference between the no-op the <c>IS DISTINCT FROM</c> guard promises and
    /// a redundant re-apply of every horizon - see <see cref="HorizonMovesReported"/>.</para>
    /// </summary>
    [Fact]
    public async Task EnsureRetentionPolicies_ConvergesAnOldHorizon_PreservingScheduledStateAndNextStart_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live convergence test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));

        var preexistingCaggs = await ExistingCaggsAsync(connection, ct);

        const string HeldRelation = "query_stats";
        const string ArmedRelation = "procedure_stats_hourly";

        var bodySucceeded = false;
        try
        {
            /* One query_stats row 30 days back keeps that relation's coverage SHORT for the whole test: it is
               outside the hourly CAGG's 3-day refresh window, so even the policies' immediate first fire
               (#1788) cannot materialize it, and the #1909 gate keeps HOLDING the policy on every sweep. That
               makes held-ness the gate's own genuine verdict rather than test-forced state — on an empty
               relation the gate legitimately ARMS (nothing to protect), which is exactly what the
               procedure_stats side demonstrates. */
            using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
VALUES (1, $1, 9137, 'converge-1937', 'TestDb', decode(md5('converge'), 'hex'), decode(md5('h'), 'hex'), 1, 1, 1)", connection))
            {
                seed.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-30), DateTimeKind.Unspecified));
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* #2818: the nightly's one "expected 17, got 0" on this exact line was untriageable because every
               per-relation warning went to a null logger. Capture instead, and carry the evidence in the
               assertion message — see the EndToEnd test above for the full reasoning. A FRESH logger per pass
               below: this fixture keeps query_stats HELD for the whole test (see the comment above), so every
               pass logs a benign "... HELD PAUSED ..." warning for it plus EnsureRetentionPoliciesAsync's own
               Information summary — sharing one CapturingTestLogger (which has no reset) would bury whichever
               pass actually fails under the earlier passes' expected noise (review finding on #2887). */
            var createLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, createLog, ct);
            var created17 = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, createLog, ct);
            Assert.True(created17 == RetentionPolicyCount,
                $"the creation pass should apply all {RetentionPolicyCount} retention policies, got {created17}; {createLog.Joined}");

            /* The gate's own verdicts, asserted as preconditions: short coverage holds, empty coverage arms. */
            var created = new
            {
                Held = await PolicyStateAsync(connection, HeldRelation, ct),
                Armed = await PolicyStateAsync(connection, ArmedRelation, ct),
            };
            Assert.False(created.Held.Scheduled, "short coverage must HOLD the policy at creation");
            Assert.True(created.Armed.Scheduled, "empty coverage must ARM the policy at creation");

            /* Demote both to the pre-#1937 horizon, exactly as an upgraded store presents them. The armed one
               gets a far-future next_start, so it cannot fire a real purge while the test runs — and that
               pushed-out next_start is precisely what must survive the convergence unmoved. */
            await DemoteHorizonAsync(connection, HeldRelation, "21 days", ct);
            await DemoteHorizonAsync(connection, ArmedRelation, "21 days", ct);
            using (var push = new NpgsqlCommand(@"
SELECT alter_job(j.job_id, next_start => now() + interval '1 hour')
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '" + ArmedRelation + "'", connection))
            {
                await push.ExecuteNonQueryAsync(ct);
            }

            var before = new
            {
                Held = await PolicyStateAsync(connection, HeldRelation, ct),
                Armed = await PolicyStateAsync(connection, ArmedRelation, ct),
            };
            Assert.Equal(("21 days", false), (before.Held.DropAfter, before.Held.Scheduled));
            Assert.Equal(("21 days", true), (before.Armed.DropAfter, before.Armed.Scheduled));

            /* THE measured claim: the sweep converges both onto the constant, preserving everything else.
               Fresh logger — see the fixture-level #2818 comment above. */
            var convergeLog = new CapturingTestLogger();
            var converged = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, convergeLog, ct);
            Assert.True(converged == RetentionPolicyCount,
                $"the convergence pass should count all {RetentionPolicyCount} policies, got {converged}; {convergeLog.Joined}");

            /* ... and it reports moving exactly the two horizons that drifted, read off the LOG rather than
               the catalog. This is the POSITIVE CONTROL for the settled sweep's zero further down: a reading
               that matched nothing would make that zero a claim about nothing, so the same reading has to fire
               here, on the pass that really does move something. */
            var convergeMoves = HorizonMovesReported(convergeLog);
            Assert.True(convergeMoves == 2,
                $"the convergence pass should report moving exactly the two demoted horizons, got {convergeMoves}; {convergeLog.Joined}");
            Assert.Contains($"Retention policy for {HeldRelation} moved to a 4 days horizon", convergeLog.Joined, StringComparison.Ordinal);
            Assert.Contains($"Retention policy for {ArmedRelation} moved to a 90 days horizon", convergeLog.Joined, StringComparison.Ordinal);

            var after = new
            {
                Held = await PolicyStateAsync(connection, HeldRelation, ct),
                Armed = await PolicyStateAsync(connection, ArmedRelation, ct),
            };
            /* Each relation converges to ITS OWN constant: the raw table to 4 days, the hourly CAGG to
               #1937's 90 - the sweep reads the horizon per policy from RetentionPolicies, not one number. */
            Assert.Equal("4 days", after.Held.DropAfter);
            Assert.False(after.Held.Scheduled, "a HELD policy must stay held across a horizon convergence");
            Assert.Equal("90 days", after.Armed.DropAfter);
            Assert.True(after.Armed.Scheduled, "an ARMED policy must stay armed across a horizon convergence");
            AssertConvergenceLeftNextStartAlone(before.Armed, after.Armed, "the convergence sweep");

            /* Idempotence: a third sweep finds nothing distinct from the constants and moves nothing.
               Fresh logger — see the fixture-level #2818 comment above. */
            var settledLog = new CapturingTestLogger();
            var settled17 = await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, settledLog, ct);
            Assert.True(settled17 == RetentionPolicyCount,
                $"the idempotent third sweep should count all {RetentionPolicyCount} policies, got {settled17}; {settledLog.Joined}");

            /* THE guard's claim, and the one thing the state reads below cannot see. Every value they compare
               is identical whether this sweep was a no-op or re-applied all 17 horizons on top of themselves,
               because a returned row is the convergence's ONLY effect beyond the horizon itself - it is what
               increments the sweep's converged count and logs the per-relation line. So idempotence has to be
               asserted as "moved nothing", not as "ended up the same".

               Measured with the IS DISTINCT FROM guard deleted from ConvergeRetentionHorizonSql, on
               TimescaleDB 2.29.2 / PostgreSQL 17.11: the statement then returns a row for all 17 relations on
               EVERY start, a fresh store's first one included, and this is the only assertion in the test that
               notices - state, counts and next_start all still match. An operator would be told 17 tiers had
               just been migrated off an earlier default when none had, on every restart, forever. #1958's
               defect class exactly: a log line that did not survive being checked. */
            var settledMoves = HorizonMovesReported(settledLog);
            Assert.True(settledMoves == 0,
                $"the settled sweep must report moving NO horizon - the IS DISTINCT FROM guard makes the convergence a no-op once every policy is on its constant - got {settledMoves}; {settledLog.Joined}");
            var settled = await PolicyStateAsync(connection, ArmedRelation, ct);
            Assert.Equal(("90 days", true), (settled.DropAfter, settled.Scheduled));
            AssertConvergenceLeftNextStartAlone(after.Armed, settled, "the idempotent third sweep");
            Assert.Equal("4 days", (await PolicyStateAsync(connection, HeldRelation, ct)).DropAfter);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);

                foreach (var relation in RetentionRelations)
                {
                    await batch.RemoveRetentionPolicyAsync(relation, cleanupCt);
                }

                await batch.DropContinuousAggregatesAsync(
                    (await ExistingCaggsAsync(cleanup, cleanupCt)).Except(preexistingCaggs, StringComparer.Ordinal), cleanupCt);

                using var unseed = new NpgsqlCommand(
                    "DELETE FROM collect.query_stats WHERE server_name = 'converge-1937'", cleanup);
                await unseed.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// How many relations a sweep REPORTED moving onto a new horizon, counted off the captured log rather than
    /// out of the catalog. <c>ConvergeRetentionHorizonSql</c> ends in an <c>IS DISTINCT FROM</c> comparison
    /// whose entire purpose is to return NO row for a policy already sitting on its constant, and a returned
    /// row is the only thing that statement does beyond the horizon itself: it is what increments
    /// <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/>'s converged count and logs one line per
    /// relation. The resulting job state therefore cannot tell a no-op from a redundant re-apply of the value
    /// already there - the log is the only place that difference exists at all.
    ///
    /// <para>Counted by phrase, and the phrase is deliberately a fragment: the sweep's own end-of-run summary
    /// says "moved ONTO a new horizon", so it cannot be miscounted here. The fragment is pinned in BOTH
    /// directions by the two callers - the convergence pass requires a positive count AND the exact
    /// per-relation text, so a reword that stopped matching fails loudly there rather than quietly turning the
    /// settled pass's expected zero into a tautology.</para>
    /// </summary>
    private static int HorizonMovesReported(CapturingTestLogger log)
    {
        const string Moved = "moved to a";

        var captured = log.Joined;
        var reported = 0;
        for (var at = captured.IndexOf(Moved, StringComparison.Ordinal); at >= 0;
             at = captured.IndexOf(Moved, at + Moved.Length, StringComparison.Ordinal))
        {
            reported++;
        }

        return reported;
    }

    /// <summary>Sets a retention policy's <c>drop_after</c> directly, standing in for a store created under an
    /// older default. Named <c>config</c> only, like the convergence itself, so the demotion cannot arm.</summary>
    private static async Task DemoteHorizonAsync(NpgsqlConnection connection, string relation, string horizon, System.Threading.CancellationToken ct)
    {
        using var demote = new NpgsqlCommand(@"
SELECT alter_job(j.job_id, config => jsonb_set(j.config, '{drop_after}', to_jsonb($1::text)))
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '" + relation + "'", connection);
        demote.Parameters.AddWithValue(horizon);
        await demote.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One policy's observable state: the horizon it would drop at, whether it is armed, when it
    /// would next run, and — since #2937 — the scheduler's own run bookkeeping for the same job, which is what
    /// makes a <c>next_start</c> that moved attributable. See
    /// <see cref="AssertConvergenceLeftNextStartAlone"/>.</summary>
    private sealed record RetentionPolicyState(
        string DropAfter,
        bool Scheduled,
        DateTime? NextStart,
        long Runs,
        DateTime? LastRunStartedAt,
        string? JobStatus);

    private static async Task<RetentionPolicyState> PolicyStateAsync(
        NpgsqlConnection connection, string relation, System.Threading.CancellationToken ct)
    {
        /* Joined on job_id, NEVER on the hypertable name: timescaledb_information.jobs resolves a continuous
           aggregate's policy to the USER VIEW (collect.procedure_stats_hourly, via
           COALESCE(ca.user_view_schema, ...)), while job_stats reports the same job against the internal
           materialization hypertable (_timescaledb_internal._materialized_hypertable_N). Measured on 2.29.2 -
           filtering job_stats by this relation name matches nothing, which would read as "never run" and make
           the gate below permanently blind. */
        using var read = new NpgsqlCommand(@"
SELECT j.config->>'drop_after', j.scheduled, j.next_start,
       coalesce(s.total_runs, 0), s.last_run_started_at, s.job_status
FROM timescaledb_information.jobs AS j
LEFT JOIN timescaledb_information.job_stats AS s
  ON s.job_id = j.job_id
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '" + relation + "'", connection) { CommandTimeout = PolicyReadTimeoutSeconds };
        using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"no policy_retention job found for collect.{relation}");

        /* next_start is NULL for a HELD job - a paused policy has no next run (the same sentinel family as
           job_stats' -infinity). */
        return new RetentionPolicyState(
            reader.GetString(0),
            reader.GetBoolean(1),
            await reader.IsDBNullAsync(2, ct) ? null : reader.GetDateTime(2),
            reader.GetInt64(3),
            await reader.IsDBNullAsync(4, ct) ? null : reader.GetDateTime(4),
            await reader.IsDBNullAsync(5, ct) ? null : reader.GetString(5));
    }

    /// <summary>Catalog read against the live fixture, explicit rather than on Npgsql's undocumented 30-second
    /// default (#2874).</summary>
    private const int PolicyReadTimeoutSeconds = 30;

    /// <summary>
    /// The <c>next_start</c>-unmoved claim, stated so that TimescaleDB's background scheduler cannot decide it
    /// either way (#2937). The policy under test is deliberately ARMED — that is the state #1680 cares about —
    /// and an armed job is one the scheduler may run at any moment, after which <c>next_start</c> LEGITIMATELY
    /// advances. Raw equality conflated that with the defect this test exists for, so it went red on healthy
    /// behaviour at whatever rate the sweep overlapped the job's schedule; three earlier flakes in this family
    /// (#1889, #2143, #2818) were all a live test racing the same scheduler.
    ///
    /// <para><b>The two events, and what separates them.</b> Measured on TimescaleDB 2.29.2 / PostgreSQL 17:
    /// none of the sweep's own statements touch <c>next_start</c> — not <c>alter_job(config =&gt; ...)</c> (the
    /// convergence), not <c>alter_job(scheduled =&gt; true)</c> on an already-armed job, not a hold followed by
    /// a re-arm. A scheduler RUN is the only thing that moves it, and a run is independently visible in the
    /// same catalog row: <c>total_runs</c> increments when the run STARTS, leaving <c>next_start</c> at the
    /// <c>-infinity</c> in-progress sentinel with <c>job_status = 'Running'</c>, and completion sets
    /// <c>next_start = last_successful_finish + schedule_interval</c> — ~24 hours out for a retention policy,
    /// which is the exact value the CI failure reported.</para>
    ///
    /// <para>So <b>no run and <c>next_start</c> moved is a defect</b>, and stays a hard failure asserted as
    /// the same equality it always was: that is the branch a convergence which re-arms or re-times the policy
    /// lands in. A run HAVING happened is the scheduler's own entitlement, and what still has to hold there is
    /// that the resulting value is one a run produces — the in-progress sentinel, or a next run strictly later
    /// than the run that explains it — rather than an immediate-purge window.</para>
    ///
    /// <para><b>What this cannot see</b>, named rather than papered over: a sweep that both re-timed the policy
    /// AND had a scheduler run land in the same window takes the second branch and passes, because the pre-run
    /// value is gone by then and nothing in the catalog attributes a run to a cause. The strict branch is where
    /// the claim lives; the tolerant branch only declines to blame the sweep for what the scheduler is
    /// documented to do.</para>
    /// </summary>
    private static void AssertConvergenceLeftNextStartAlone(
        RetentionPolicyState before, RetentionPolicyState after, string sweep)
    {
        /* The evidence travels WITH the assertion - a bare "values differ" on this line was untriageable, and
           #2818 was filed about exactly that on this test. */
        var evidence =
            $"next_start {Stamp(before.NextStart)} -> {Stamp(after.NextStart)}, "
            + $"total_runs {before.Runs} -> {after.Runs}, "
            + $"last_run_started_at {Stamp(before.LastRunStartedAt)} -> {Stamp(after.LastRunStartedAt)}, "
            + $"job_status {before.JobStatus ?? "(null)"} -> {after.JobStatus ?? "(null)"}";

        if (after.Runs == before.Runs && !RunInFlight(before))
        {
            /* No run intervened, so the sweep is the ONLY thing that could have moved next_start. */
            Assert.True(after.NextStart == before.NextStart,
                $"{sweep} moved an ARMED policy's next_start with no scheduler run to account for it - "
                + $"converging a horizon must never re-time or re-arm the job (#1680); {evidence}");
            return;
        }

        /* Either a new run started (total_runs increments at run START, not at completion) or one that was
           ALREADY in flight at the earlier observation has since finished - and a completion moves next_start
           with the run count static, so it has to reach this branch too or it would read as a sweep. */
        Assert.True(after.Scheduled, $"a scheduler run must not leave the policy held; {evidence}");

        if (RunInFlight(after))
        {
            /* Still executing. next_start is the -infinity sentinel, which Npgsql surfaces as
               DateTime.MinValue; pin that rather than accepting whatever is there. */
            Assert.True(after.NextStart == DateTime.MinValue,
                $"a run in progress must leave next_start at the -infinity in-progress sentinel; {evidence}");
            return;
        }

        Assert.True(after.LastRunStartedAt is not null && after.NextStart > after.LastRunStartedAt,
            $"{sweep} left an ARMED policy's next run at or before the run that supposedly explains it, "
            + $"which is an immediate-purge window and not a scheduled one (#1680); {evidence}");

        static string Stamp(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture) ?? "(null)";

        /* A run TimescaleDB has started and not finished. job_status is the view's own word for it; the
           -infinity next_start is the same state read off the row, kept as a second tell because it is the
           value the assertions above actually compare and Npgsql surfaces it as DateTime.MinValue. */
        static bool RunInFlight(RetentionPolicyState state)
            => string.Equals(state.JobStatus, "Running", StringComparison.Ordinal)
            || state.NextStart == DateTime.MinValue;
    }

    /// <summary>The continuous aggregates present in <c>collect</c> right now, so the retention test can drop
    /// exactly the ones it created and leave a pre-existing store's shape alone.</summary>
    private static async Task<string[]> ExistingCaggsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect'", connection);
        using var reader = await command.ExecuteReaderAsync(ct);
        var names = new System.Collections.Generic.List<string>();
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }


    /// <summary>
    /// The relations EnsureRetentionPoliciesAsync attaches policies to, for teardown: the three raw tables,
    /// the four hourly CAGGs, and the seven baseline aggregates (#1757; nine until #2007). The last group is DERIVED from the
    /// product's own list rather than restated, so adding a baseline aggregate cannot leave an armed retention
    /// policy behind on this shared fixture.
    /// </summary>
    private static readonly string[] RetentionRelations = new[]
    {
        "query_stats", "procedure_stats", "query_store_stats",
        "query_stats_hourly", "procedure_stats_hourly", "query_store_stats_hourly", "query_stats_db_hourly",
        /* The corrected Query Store tier (#1849): the interval-grain dedup layer on its own short horizon,
           and the corrected hourly on the standard 21-day one. The corrected DAILY is kept indefinitely like
           every other daily, so it carries no policy and must not appear here — and neither does #1869's
           day-grain daily beside it, for the same reason. Its interval-grain DAILY source does, on its own
           slightly longer horizon: it has to outlive the hourly dedup layer whose purge waits on it. */
        TimescaleSupport.QueryStoreStatsIntervalHourlyView,
        TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
        TimescaleSupport.QueryStoreStatsIntervalDailyView,
    }
    .Concat(TimescaleSupport.BaselineAggregates.Select(a => a.View))
    .ToArray();

    /// <summary>The policy set EnsureRetentionPoliciesAsync attaches, derived so the two cannot drift.</summary>
    private static readonly int RetentionPolicyCount = RetentionRelations.Length;

    [Fact]
    public async Task CompressionJobSelfHeal_DetectionQueryValid_AndRearmSucceeds_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live compression self-heal test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));
        Assert.Equal(CollectorCatalog.All.Count, await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct));

        /* Pick one real compression policy job (on wait_stats). */
        long jobId;
        using (var find = new NpgsqlCommand(@"
SELECT j.job_id
FROM timescaledb_information.jobs AS j
WHERE j.hypertable_name = 'wait_stats'
  AND (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')
ORDER BY j.job_id
LIMIT 1", connection))
        {
            var result = await find.ExecuteScalarAsync(ct);
            Assert.NotNull(result);
            jobId = Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
        }

        /* (1) The detection query is valid SQL against the REAL timescaledb_information job_stats/jobs views
           (including the `next_start = '-infinity'::timestamptz` comparison), and a healthy compression job is
           NOT flagged — no false alarm. This is the full ReadStuckCompressionJobsAsync path against the live
           schema.

           A just-added compression policy job can momentarily read next_start = '-infinity' in job_stats
           BEFORE TimescaleDB's background scheduler assigns its first real next run — and the detector is
           CORRECT to flag -infinity — so asserting immediately after ApplyCompressionPolicy raced that window
           and intermittently false-failed on a slow CI runner. Deterministically settle the job into the
           healthy state the assertion is actually about: give it a real FUTURE next_start (via the same
           alter_job the self-heal uses), then wait for the detector itself to report it healthy.

           The wait's RESULT is what the assertion below reads, and that is the whole point rather than a
           convenience: a wait that only proves "healthy at some instant", followed by a fresh read, asserts
           against a DIFFERENT observation than the one it validated, and the job can leave the healthy state
           in the gap between them (the scheduler picking it up reads next_start = -infinity with status
           Running mid-run — see leg 3). That gap is this test's third flake in the same class, after #1760
           polled a copy of one detector arm and after the wait was introduced to close it; it survived
           because the helper's guarantee stops at the moment it returns. Consuming the returned snapshot
           makes settled-according-to-the-wait and settled-according-to-the-assertion the same observation
           by construction, which is what the helper's contract claimed all along. */
        using (var arm = new NpgsqlCommand("SELECT alter_job($1::integer, next_start => now() + interval '1 hour')", connection))
        {
            arm.Parameters.Add(new NpgsqlParameter { Value = jobId });
            await arm.ExecuteNonQueryAsync(ct);
        }
        var healthy = await WaitUntilDetectorReportsHealthyAsync(connection, jobId, ct);

        /* The SQL really is valid against the live catalog, and this job really is in its result set.
           ReadStuckCompressionJobsAsync is failure-isolated (a broken query is swallowed and returns an EMPTY
           list), so DoesNotContain ALONE would pass just as happily against SQL that never compiled — the one
           thing this leg claims to prove. Run the production const directly, where a syntax or column error
           throws, and require the job to be present: only then does "not flagged" mean the detector looked at
           this job and judged it healthy.

           This one keeps its OWN read, which is safe where the health assertion is not: StuckCompressionJobsSql
           filters on proc_name alone, so it returns every compression job whatever state it is in, and "this job
           is in the result set" cannot race. Flagging is the C# predicate applied on top of those rows, and that
           is the only part that moves. */
        var observed = await ReadObservedJobIdsAsync(connection, ct);
        Assert.Contains(jobId, observed);

        /* Deliberately tautological, and kept for what it documents rather than what it can catch: `healthy` is
           the snapshot the wait already found clean, so this cannot fail today. It states the property leg (1)
           exists to assert, at the place a reader looks for it, and it fails loudly if the helper is ever
           changed to return something other than the satisfying poll's own result. The load-bearing check is
           the wait's bounded loop, which fails carrying the detector's reason string. */
        Assert.DoesNotContain(healthy, s => s.JobId == jobId);

        /* (2) The #1586 REGRESSION GUARD: the production re-arm runs the real alter_job against TimescaleDB and
           SUCCEEDS. The job_id MUST be sent as `integer`, not `bigint` — an un-cast bound long fails with
           `42883: function alter_job(bigint, ...) does not exist`, which shipped in #1585 and made every
           self-heal re-arm silently throw. This drives the exact production path (TryRearmJobAsync ->
           RearmJobSql `alter_job($1::integer, next_start => now())`), which the unit tests — using a fake re-arm
           delegate — cannot reach.

           We deliberately do NOT simulate the stuck state by forcing next_start to -infinity: TimescaleDB
           REJECTS `alter_job(..., next_start => '-infinity')` with `22023: cannot set next start to -infinity`
           (the dead-scheduler -infinity arises from TimescaleDB's own background scheduler on a failed run, not
           from a user call, so it cannot be injected through the public API). The -infinity / Running-past-bound
           DETECTION logic is covered by the pure IsCompressionJobStuck unit tests. */
        Assert.True(await TimescaleSupport.TryRearmJobAsync(connection, jobId, null, ct));

        /* (3) After a real re-arm (next_start => now()) the job settles healthy. SETTLES, not "reads healthy
           on one snapshot": next_start => now() makes the job immediately due, the scheduler picks it up, and
           from pickup to completion job_stats reads next_start = -infinity with status Running — the mid-run
           marker (measured live; the detector now defers that state to its elapsed-bound arm). A single
           un-settled read raced the very run the re-arm triggered, which was this test's own flake. */
        await WaitUntilDetectorReportsHealthyAsync(connection, jobId, ct);
    }

    /// <summary>
    /// Wait until <see cref="TimescaleSupport.ReadStuckCompressionJobsAsync"/> — the DETECTION QUERY ITSELF,
    /// the thing under test — stops flagging this job. #1760: the predecessor polled
    /// <c>next_start &lt;&gt; '-infinity'</c> directly, which is only ONE of the two arms the detector
    /// evaluates, so "settled" and "the assertion will pass" were different statements and the gap between
    /// them was the flake. Worse, the value it polled was the one the caller's own
    /// <c>alter_job(next_start =&gt; …)</c> had just written, so it was satisfied on its first poll and waited
    /// for nothing at all — no timeout increase could ever have helped.
    ///
    /// <para>Polling the detector closes that by construction: there is one predicate, not two copies that
    /// drift, so settled-according-to-the-wait IS settled-according-to-the-assertion. Bounded, and it fails
    /// loudly carrying the detector's OWN reason string — a job that never settles is genuinely stuck and must
    /// not silently pass, and the reason names which arm held it rather than assuming <c>next_start</c>.</para>
    ///
    /// <para>RETURNS the flagged list from the poll that satisfied it, and callers must assert against THAT
    /// rather than issuing a fresh read. The guarantee above holds only at the instant this returns: the
    /// scheduler is free to pick the job up immediately afterward, and mid-run it reads
    /// <c>next_start = -infinity</c> with status Running, which the detector flags and is right to flag. A
    /// caller that re-queries is therefore asserting on an observation this method never validated — which is
    /// exactly how the race came back after being closed once.</para>
    /// </summary>
    private static async Task<IReadOnlyList<StuckCompressionJob>> WaitUntilDetectorReportsHealthyAsync(
        NpgsqlConnection connection, long jobId, System.Threading.CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            var flagged = await TimescaleSupport.ReadStuckCompressionJobsAsync(connection, DateTime.UtcNow, null, ct);
            var mine = flagged.FirstOrDefault(s => s.JobId == jobId);
            if (mine is null)
            {
                return flagged;
            }

            Assert.True(DateTime.UtcNow < deadline,
                $"compression job {jobId} was still flagged as stuck after 30s: {mine.Reason}");
            await Task.Delay(250, ct);
        }
    }

    /// <summary>
    /// Every job_id the production detection query OBSERVES — not just the ones it flags — by running
    /// <see cref="TimescaleSupport.StuckCompressionJobsSql"/> itself. Sharing the const is the whole point: a
    /// paraphrase here could compile happily while the real query did not.
    /// </summary>
    private static async Task<List<long>> ReadObservedJobIdsAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var ids = new List<long>();
        using var command = new NpgsqlCommand(TimescaleSupport.StuckCompressionJobsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(Convert.ToInt64(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture));
        }

        return ids;
    }

    /// <summary>
    /// #1760 root cause, pinned deterministically: TimescaleDB's never-ran sentinel in
    /// <c>last_run_started_at</c> is <b>-infinity</b>, not NULL, and Npgsql maps that to
    /// <see cref="DateTime.MinValue"/>. Un-guarded, that turned "never ran" into "started in year 1" — an
    /// elapsed of ~739,000 days that clears every <see cref="TimescaleSupport.StuckRunningBound"/> — so a
    /// healthy job was flagged stuck for the whole of its FIRST run, whenever the detector's read landed while
    /// <c>job_status</c> already said <c>Running</c>. Those two fields come from independent sources in
    /// TimescaleDB's own view (<c>pg_stat_activity.state</c> vs <c>bgw_job_stat.last_start</c>), so that window
    /// is structural, not hypothetical.
    ///
    /// <para>Deterministic, not timing-dependent: a freshly added policy has no <c>bgw_job_stat</c> row at all
    /// (every column NULL), and it is <c>alter_job</c> that materialises the row carrying the -infinity
    /// sentinel — so arming it an hour out both creates the row and guarantees the scheduler cannot run the job
    /// out from under the assertion. Asserts the raw column really does carry the sentinel (otherwise the
    /// NULLIF guard would be dead code that passes for the wrong reason) AND that the production query hands
    /// back NULL for it.</para>
    /// </summary>
    [Fact]
    public async Task StuckCompressionJobsSql_NeverRunJob_ReadsNullLastRunStartedAt_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the never-ran sentinel pin.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        /* Migrate first, as every other gated test in this class does. It is not the schema this test wants —
           it is the search_path: the migration sets the database to collect, config, public, and WITHOUT public
           on the path TimescaleDB's own by_range is unresolvable, so create_hypertable fails to resolve its
           argument and the whole statement dies as 42883 by_range(unknown, interval) does not exist. Skipping
           this line is what made the test pass on a rig whose search_path a previous run had already set, and
           fail on CI's throwaway cluster. */
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        const string table = "stuck_sentinel_probe_1760";
        var bodySucceeded = false;
        try
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table} CASCADE", ct);
            await ExecuteAsync(connection,
                $"CREATE TABLE {table} (collection_time timestamptz NOT NULL, server_id integer NOT NULL)", ct);
            /* The product's own SQL builders, not hand-rolled equivalents: a one-argument by_range('col') is
               accepted by TimescaleDB 2.28 but not by the older version CI's fixture carries, and the point of
               this test is the catalog's behaviour rather than a second dialect of the same DDL. */
            await ExecuteAsync(connection, TimescaleSupport.CreateHypertableSql(table, "collection_time"), ct);
            await ExecuteAsync(connection, TimescaleSupport.EnableCompressionSql(table), ct);

            /* PARKED, in one transaction — the same lever #1889 pulled for the three compression tests, and
               this one needs it MORE than they do. Their assertions are about which chunks a run compressed;
               this one asserts the job has NEVER RUN (last_run_started_at = '-infinity', the #1760 sentinel),
               which a single background launch destroys outright and no amount of re-reading recovers. An
               unparked add_compression_policy creates the job SCHEDULED and TimescaleDB launches it within a
               second or two (#1788), so the window between creating it and parking it below was the whole
               defect: observed failing as Expected: True / Actual: False on a full-suite run, twice. */
            await AddCompressionPolicyParkedAsync(connection, table, ct);

            long jobId;
            using (var find = new NpgsqlCommand(
                $"SELECT job_id FROM timescaledb_information.jobs WHERE hypertable_name = '{table}' "
                + "AND (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%') ORDER BY job_id LIMIT 1",
                connection))
            {
                var found = await find.ExecuteScalarAsync(ct);
                Assert.NotNull(found);
                jobId = Convert.ToInt64(found, System.Globalization.CultureInfo.InvariantCulture);
            }

            /* Materialise the stat row and park the job an hour out so it cannot run mid-assertion. */
            using (var arm = new NpgsqlCommand(
                "SELECT alter_job($1::integer, next_start => now() + interval '1 hour')", connection))
            {
                arm.Parameters.Add(new NpgsqlParameter { Value = jobId });
                await arm.ExecuteNonQueryAsync(ct);
            }

            /* The sentinel is really there — the guard is not dead code. */
            using (var raw = new NpgsqlCommand(
                "SELECT last_run_started_at = '-infinity'::timestamptz FROM timescaledb_information.job_stats WHERE job_id = $1::integer",
                connection))
            {
                raw.Parameters.Add(new NpgsqlParameter { Value = jobId });
                Assert.Equal(true, await raw.ExecuteScalarAsync(ct));
            }

            /* ...and the production query neutralises it, so the stuck-Running arm cannot fire on a job that
               has never run. Without the NULLIF this reads DateTime.MinValue. */
            using (var guarded = new NpgsqlCommand(TimescaleSupport.StuckCompressionJobsSql, connection))
            {
                await using var reader = await guarded.ExecuteReaderAsync(ct);
                var sawJob = false;
                while (await reader.ReadAsync(ct))
                {
                    if (Convert.ToInt64(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture) != jobId)
                    {
                        continue;
                    }

                    sawJob = true;
                    Assert.True(reader.IsDBNull(3),
                        "a never-run compression job must read a NULL last_run_started_at through the production "
                        + "query; a non-null value here is the year-1 elapsed that flagged healthy jobs as stuck.");
                }

                Assert.True(sawJob, $"the production query did not observe compression job {jobId}.");
            }

            /* The pure predicate agrees end to end: never-ran + Running is NOT stuck. */
            Assert.False(
                TimescaleSupport.IsCompressionJobStuck(false, "Running", null, TimeSpan.FromHours(12), DateTime.UtcNow, out _));

            bodySucceeded = true;
        }
        finally
        {
            /* Through LiveCleanupBatch rather than a bare DROP (#1873's verification, #1896's connection): the
               probe table is a hypertable carrying a compression policy, and its removal has a postcondition
               worth checking rather than assuming. The bare name resolves through the cleanup connection's
               search_path to collect, which is where it was created. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropTableAsync(table, cleanupCt));
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Takes its cancellation token rather than reaching for <c>TestContext.Current.CancellationToken</c>
    /// (#1896). As teardown this runs under <see cref="LiveStoreCleanup"/>, which passes
    /// <see cref="System.Threading.CancellationToken.None"/> on purpose so a CANCELLED run still restores the
    /// shared store — and a helper that fetched the test's own token would have re-signalled itself on exactly
    /// that path, skipping the delete it exists to perform.
    /// </summary>
    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {TestServerId}; DELETE FROM collection_log WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
