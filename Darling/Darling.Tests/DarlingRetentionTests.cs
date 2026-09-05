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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the retention contract. Ungated: every collector in the shared catalog has a
/// <see cref="CollectorScheduleDefaults"/> entry with a positive RetentionDays (the purge can
/// never skip a table silently or compute a nonsense cutoff), the generated DELETE targets
/// each definition's own prefix time column (collection_time almost everywhere; the config
/// snapshots' capture_time), and the Timescale branch's drop_chunks statement carries each
/// table's own shared horizon (the drop_chunks purge end-to-end lives in
/// TimescaleSupportTests). Gated on DARLING_TEST_PG: the DELETE-path purge end-to-end against
/// a dev Postgres — expired wait_stats and collection_log rows go, a fresh row survives.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingRetentionTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -616161;

    [Fact]
    public void PurgeSummary_TotalPurged_SumsDeletedRowsAndDroppedChunks()
    {
        /* The single headline count the daily log + the purge_now result_json ("rowsPurged") report. */
        var summary = new PurgeSummary(TablesPurged: 31, RowsDeleted: 1200, ChunksDropped: 42);
        Assert.Equal(1242, summary.TotalPurged);
        Assert.Equal(0, new PurgeSummary(0, 0, 0).TotalPurged);
    }

    [Fact]
    public void EveryCatalogCollector_HasAPositiveSharedRetention()
    {
        foreach (var definition in CollectorCatalog.All)
        {
            Assert.True(CollectorScheduleDefaults.All.TryGetValue(definition.Name, out var schedule),
                $"collector '{definition.Name}' has no CollectorScheduleDefaults entry — its table '{definition.TargetTable}' would never be purged");
            Assert.True(schedule!.RetentionDays > 0,
                $"collector '{definition.Name}' has RetentionDays {schedule.RetentionDays} — the purge cutoff would be nonsensical");
        }
    }

    [Fact]
    public void DeleteSql_BatchesOnEachDefinitionsOwnTimeColumn()
    {
        var byName = CollectorCatalog.All.ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);

        /* Batched by TIME SLICE, not by ctid row cap (#1564): reading ctid through TimescaleDB's
           transparent decompression is unsupported ("transparent decompression only supports tableoid
           system column"), so the old ctid IN (SELECT ... LIMIT 10000) shape ERRORED whenever any in-range
           chunk was compressed and the table silently kept its expired rows. Each execution clears the
           oldest one day of expired rows — one chunk's worth on a hypertable, one day's arrival volume on a
           plain table — and a slice that deletes nothing terminates the drain. $1 is bound once and
           referenced by all three positions. */
        Assert.Equal(
            "DELETE FROM wait_stats WHERE collection_time < $1 AND collection_time >= (SELECT min(collection_time) FROM wait_stats WHERE collection_time < $1) AND collection_time < (SELECT min(collection_time) FROM wait_stats WHERE collection_time < $1) + INTERVAL '1 days'",
            DarlingRetention.DeleteSqlFor(byName["wait_stats"]));

        /* The config snapshots batch on their capture_time, not collection_time. */
        Assert.Equal(
            "DELETE FROM trace_flags WHERE capture_time < $1 AND capture_time >= (SELECT min(capture_time) FROM trace_flags WHERE capture_time < $1) AND capture_time < (SELECT min(capture_time) FROM trace_flags WHERE capture_time < $1) + INTERVAL '1 days'",
            DarlingRetention.DeleteSqlFor(byName["trace_flags"]));

        /* collection_log's PLAIN-PostgreSQL / conversion-failed FALLBACK still runs through the same batched
           builder (since V23 it is a hypertable, so on Timescale it purges via drop_chunks — pinned below). */
        Assert.Equal(
            "DELETE FROM collection_log WHERE collection_time < $1 AND collection_time >= (SELECT min(collection_time) FROM collection_log WHERE collection_time < $1) AND collection_time < (SELECT min(collection_time) FROM collection_log WHERE collection_time < $1) + INTERVAL '1 days'",
            DarlingRetention.TimeSlicedDeleteSql("collection_log", "collection_time"));

        /* The slice width IS the hypertable chunk width — the fallback's unit of work stays one chunk. */
        Assert.Equal(1, TimescaleSupport.ChunkIntervalDays);
    }

    [Fact]
    public void CollectionLogRetention_IsTwiceTheBaseWindow()
    {
        /* collection_log is kept 2x the base data-retention window (the Dashboard's retention_date x2) so a
           run-record outlives the metric rows it explains: 60 days vs the dominant 30-day collector horizon. */
        Assert.Equal(30, DarlingRetention.DataRetentionBaseDays);
        Assert.Equal(60, DarlingRetention.CollectionLogRetentionDays);
        Assert.Equal(DarlingRetention.DataRetentionBaseDays * 2, DarlingRetention.CollectionLogRetentionDays);

        /* The base mirrors the dominant collector horizon it is derived from. */
        Assert.Equal(DarlingRetention.DataRetentionBaseDays, CollectorScheduleDefaults.All["wait_stats"].RetentionDays);
    }

    [Fact]
    public void AlertHistoryRetention_IsNinetyDays_AndBatchesOnAlertTime()
    {
        /* config_alert_log (the fired-alert history) is a plain config-schema registry table — not a collector
           (no CollectorScheduleDefaults horizon) and not a hypertable — so it purges through the SAME batched
           DELETE builder as collection_log, on its own alert_time column. Kept 90 days: a bounded but generous
           audit-trail horizon (there is no operator setting governing it today, so the constant is the source
           of truth). */
        Assert.Equal(90, DarlingRetention.AlertHistoryRetentionDays);
        Assert.Equal(
            "DELETE FROM config_alert_log WHERE alert_time < $1 AND alert_time >= (SELECT min(alert_time) FROM config_alert_log WHERE alert_time < $1) AND alert_time < (SELECT min(alert_time) FROM config_alert_log WHERE alert_time < $1) + INTERVAL '1 days'",
            DarlingRetention.TimeSlicedDeleteSql("config_alert_log", "alert_time"));
    }

    [Fact]
    public void CommandHistoryRetention_IsTheBaseWindow_AndPurgesOnlyTerminalRows()
    {
        /* config.config_command is not an audit surface anyone reads (a caller polls its OWN command_id and
           moves on) and it is higher-volume than alert history — every viewer live-plan / actual-plan /
           active-queries fetch enqueues a row — so it gets the base data window, not the alert log's 90 days. */
        Assert.Equal(30, DarlingRetention.CommandHistoryRetentionDays);
        Assert.Equal(DarlingRetention.DataRetentionBaseDays, DarlingRetention.CommandHistoryRetentionDays);

        /* SCHEMA-QUALIFIED: unlike collection_log / config_alert_log (created bare, so they land in `collect`
           under search_path = collect, config, public), this table really is in `config`. A bare name would
           resolve to a nonexistent collect.config_command and the purge would fail every night into a warning
           nobody reads. The terminal-status filter appears in the DELETE **and in both min() subqueries** —
           anchoring a slice on an ineligible row would delete nothing and terminate the drain early. */
        Assert.Equal(
            "DELETE FROM config.config_command WHERE created_at < $1 AND status IN ('succeeded', 'failed')"
            + " AND created_at >= (SELECT min(created_at) FROM config.config_command WHERE created_at < $1 AND status IN ('succeeded', 'failed'))"
            + " AND created_at < (SELECT min(created_at) FROM config.config_command WHERE created_at < $1 AND status IN ('succeeded', 'failed')) + INTERVAL '1 days'",
            DarlingRetention.TimeSlicedDeleteSql("config.config_command", "created_at", DarlingRetention.TerminalCommandStatuses));

        /* Only the two states the executor ever writes terminally. A pending / in_progress row is never
           purged: deleting a live command strands the caller polling it. */
        Assert.Equal("status IN ('succeeded', 'failed')", DarlingRetention.TerminalCommandStatuses);
    }

    [Fact]
    public void PlanForceLedgerRetention_IsAYear_AndIsTheLongestHorizonInTheStore()
    {
        /* collect.plan_force_actions (the force-plan bot's decision journal) is not a collector and not a
           hypertable, so it purges through the same batched DELETE builder as config_alert_log, on its own
           action_time column. A year: it is the audit trail of a bot WRITING to production servers, so it
           has to outlive the metrics that motivated each decision by enough that "why did this plan change"
           is still answerable releases later. */
        Assert.Equal(365, DarlingRetention.PlanForceLedgerRetentionDays);

        /* The LONGEST horizon in the store, deliberately — strictly greater than every sibling, including
           the alert log's already-generous 90 days. This is the assertion that catches a copy-paste of a
           shorter sibling's constant into the purge call. */
        Assert.True(
            DarlingRetention.PlanForceLedgerRetentionDays > DarlingRetention.AlertHistoryRetentionDays,
            "the plan-force ledger must outlive alert history");
        Assert.True(
            DarlingRetention.PlanForceLedgerRetentionDays > DarlingRetention.CollectionLogRetentionDays,
            "the plan-force ledger must outlive the collection log");
        Assert.True(
            DarlingRetention.PlanForceLedgerRetentionDays > DarlingRetention.CommandHistoryRetentionDays,
            "the plan-force ledger must outlive command history");

        /* SCHEMA-QUALIFIED to match the V107 DDL and PgPlanForceActionStore, which both name
           collect.plan_force_actions explicitly. No extra predicate: unlike config.config_command every row
           here is eligible once it is past the horizon — the journal is append-only, so there is no live-row
           state a purge could strand. */
        Assert.Equal(
            "DELETE FROM collect.plan_force_actions WHERE action_time < $1"
            + " AND action_time >= (SELECT min(action_time) FROM collect.plan_force_actions WHERE action_time < $1)"
            + " AND action_time < (SELECT min(action_time) FROM collect.plan_force_actions WHERE action_time < $1) + INTERVAL '1 days'",
            DarlingRetention.TimeSlicedDeleteSql("collect.plan_force_actions", "action_time"));
    }

    [Fact]
    public void ThePlanForceLedger_IsNotACollectorTable_SoTheCatalogLoopCannotReachIt()
    {
        /* Why the purge needs its own explicit block rather than an entry in the shared catalog: the journal
           is written by the service's post-analysis bot pass, not by a collector, so nothing in
           CollectorCatalog.All names it and the catalog-driven loop skips it entirely. If it ever DOES gain a
           catalog entry, this test fails and the explicit block becomes a double-purge to delete. */
        Assert.DoesNotContain(
            CollectorCatalog.All,
            d => d.TargetTable.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase));

        /* Positive control for that negative: the same predicate DOES find a real collector table, so the
           assertion above is a fact about the catalog rather than a mis-spelled probe that can never match. */
        Assert.Contains(
            CollectorCatalog.All,
            d => d.TargetTable.Equals("file_io_stats", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void TimeSlicedDelete_WithoutAnExtraPredicate_IsUnchanged()
    {
        /* The optional predicate must not perturb the existing callers' statements by so much as a space. */
        Assert.Equal(
            DarlingRetention.TimeSlicedDeleteSql("collection_log", "collection_time"),
            DarlingRetention.TimeSlicedDeleteSql("collection_log", "collection_time", extraPredicate: null));
    }

    /* ---------------- batched-drain loop (pure, injected executor) ---------------- */

    [Fact]
    public async Task DrainBatches_LoopsUntilBatchBelowCap_ThenStops()
    {
        /* [cap, cap, partial] -> 3 executions, summed; the partial batch (< cap) terminates the drain. */
        var batches = new Queue<int>(new[] { 10000, 10000, 3 });
        var calls = 0;
        var total = await DarlingRetention.DrainBatchesAsync(
            _ => { calls++; return Task.FromResult(batches.Dequeue()); }, batchSize: 10000, CancellationToken.None);

        Assert.Equal(20003, total);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task DrainBatches_SingleUnderCapBatch_StopsAfterOne()
    {
        var calls = 0;
        var total = await DarlingRetention.DrainBatchesAsync(
            _ => { calls++; return Task.FromResult(5); }, batchSize: 10000, CancellationToken.None);

        Assert.Equal(5, total);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task DrainBatches_ExactMultipleOfCap_TerminatesOnTheEmptyBatch()
    {
        /* Exactly cap, then 0: the full-cap batch forces another round; the empty batch terminates it. */
        var batches = new Queue<int>(new[] { 10000, 0 });
        var calls = 0;
        var total = await DarlingRetention.DrainBatchesAsync(
            _ => { calls++; return Task.FromResult(batches.Dequeue()); }, batchSize: 10000, CancellationToken.None);

        Assert.Equal(10000, total);
        Assert.Equal(2, calls);
    }

    /// <summary>
    /// The row-capped drain has to SAY what it did, because rows-deleted alone cannot tell a drain from a
    /// peel — and that ambiguity is #2386's failure mode, where a purge that cleared one bounded slice and
    /// a purge that cleared a real backlog wrote the same log line. It also has to print the resolved
    /// cutoff rather than leaving a reader to apply the retention knob, which is a day off: the cutoff
    /// carries a one-day margin, so counting rows older than the knob overstates the eligible set by a full
    /// day of ingest.
    ///
    /// <para>Source-level rather than a log-capture harness: what is being pinned is that the call site
    /// passes those two values at all. A logger fake would assert the same thing through more machinery,
    /// and the failure this guards against is someone simplifying the message back to a row count.</para>
    /// </summary>
    [Fact]
    public void TheRowCappedDrain_LogsItsBatchCountAndResolvedCutoff()
    {
        var source = ReadRetentionSource();
        var at = source.IndexOf("await DrainBatchesAsync(", StringComparison.Ordinal);
        Assert.True(at >= 0, "the drain call site moved (#2386)");
        var body = source[Math.Max(0, at - 2000)..Math.Min(source.Length, at + 2500)];

        Assert.Contains("batches++", body, StringComparison.Ordinal);
        Assert.Contains("{Batches} batch(es)", body, StringComparison.Ordinal);

        /* The cutoff the DELETE actually bound, not the knob it was derived from. */
        Assert.Contains("cutoff {Cutoff:", body, StringComparison.Ordinal);

        /* Only the row-capped path: the time-sliced statement passes batchSize 1, where "batches" is
           always 1 and would say nothing. */
        Assert.Contains("if (batchSize > 1)", body, StringComparison.Ordinal);
    }

    /* ---------------- run-record status/message (pure) ---------------- */

    [Fact]
    public void BuildRunRecordSummary_AllTablesClean_IsSuccess()
    {
        var (status, message) = DarlingRetention.BuildRunRecordSummary(
            tablesPurged: 33, totalRowsDeleted: 1200, totalChunksDropped: 42, tablesFailed: 0);

        Assert.Equal("SUCCESS", status);
        Assert.Contains("33 table(s)", message);
        Assert.Contains("1200 row(s) deleted", message);
        Assert.Contains("42 chunk(s) dropped", message);
        Assert.DoesNotContain("failed", message);
    }

    [Fact]
    public void BuildRunRecordSummary_SomeTablesFailed_IsWarning()
    {
        var (status, message) = DarlingRetention.BuildRunRecordSummary(
            tablesPurged: 30, totalRowsDeleted: 500, totalChunksDropped: 0, tablesFailed: 3);

        Assert.Equal("WARNING", status);
        Assert.Contains("3 failed", message);
    }

    [Fact]
    public async Task Purge_UnexpectedThrowInSweep_DoesNotPropagate_ReturnsPartialSummary()
    {
        /* The daily caller does NOT wrap PurgeAsync, so an unexpected throw inside the sweep would kill the
           collection loop. Here a caller resolver throws on the first collector, driving the outer catch (the
           ERROR path): PurgeAsync must swallow it and return a partial summary. The null data source is never
           dereferenced before the throw, and the ERROR run-record write is itself failure-isolated, so this
           stays a DB-free test. */
        var summary = await DarlingRetention.PurgeAsync(
            postgres: null!, timescaleAvailable: false, logger: null, TestContext.Current.CancellationToken,
            retentionDaysFor: _ => throw new InvalidOperationException("resolver boom"));

        Assert.Equal(0, summary.TablesPurged);
        Assert.Equal(0, summary.TotalPurged);
    }

    /// <summary>
    /// The Timescale branch: drop_chunks per table with the table's OWN shared horizon flowing
    /// into make_interval (no time column appears — the partition column is implicit in the
    /// hypertable dimension, so capture_time tables get the identical shape). The
    /// DELETE-vs-drop_chunks branch itself is exercised end-to-end in TimescaleSupportTests.
    /// </summary>
    [Fact]
    public void DropChunksSql_CarriesEachTablesOwnRetentionDays()
    {
        var byName = CollectorCatalog.All.ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);

        Assert.Equal("SELECT drop_chunks('wait_stats', older_than => make_interval(days => 30))",
            DarlingRetention.DropChunksSqlFor(byName["wait_stats"], CollectorScheduleDefaults.All["wait_stats"].RetentionDays));
        Assert.Equal("SELECT drop_chunks('trace_flags', older_than => make_interval(days => 30))",
            DarlingRetention.DropChunksSqlFor(byName["trace_flags"], CollectorScheduleDefaults.All["trace_flags"].RetentionDays));
        Assert.Equal("SELECT drop_chunks('index_object_stats', older_than => make_interval(days => 90))",
            DarlingRetention.DropChunksSqlFor(byName["index_object_stats"], CollectorScheduleDefaults.All["index_object_stats"].RetentionDays));
        Assert.Equal("SELECT drop_chunks('server_properties', older_than => make_interval(days => 365))",
            DarlingRetention.DropChunksSqlFor(byName["server_properties"], CollectorScheduleDefaults.All["server_properties"].RetentionDays));

        /* collection_log is a hypertable since V23 (converted directly by the V23 migration, outside the
           collector catalog), so with Timescale it purges via drop_chunks at its own 2x horizon — the raw-table
           overload, since it has no ICollectorSchemaInfo. NOT the batched DELETE (that is the plain-PG fallback). */
        Assert.Equal(
            "SELECT drop_chunks('collection_log', older_than => make_interval(days => 60))",
            DarlingRetention.DropChunksSqlFor("collection_log", DarlingRetention.CollectionLogRetentionDays));
    }

    [Fact]
    public async Task EndToEnd_PurgeDeletesExpiredRowsAndKeepsFreshOnes_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live retention test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* All timestamps Kind-Unspecified — naive-UTC storage, see PgCollectorRowWriter. */
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            /* wait_stats retention is 30 days: one row well past it, one fresh. Payload columns
               are nullable, so the standard prefix is enough. */
            using (var insert = new NpgsqlCommand(
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", connection))
            {
                insert.Parameters.AddWithValue(1L);
                insert.Parameters.AddWithValue(utcNow.AddDays(-40));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var insert = new NpgsqlCommand(
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", connection))
            {
                insert.Parameters.AddWithValue(2L);
                insert.Parameters.AddWithValue(utcNow.AddHours(-1));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* collection_log purges on its own 2x horizon (60 days) so a run-record outlives the metric rows
               it explains: a 70-day row is past the horizon and goes, a 45-day row is inside the 60-day
               window (but past the 30-day data window) and SURVIVES — proving the 2x extension. */
            using (var insert = new NpgsqlCommand(
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)", connection))
            {
                insert.Parameters.AddWithValue(1L);
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                insert.Parameters.AddWithValue("wait_stats");
                insert.Parameters.AddWithValue(utcNow.AddDays(-70));
                insert.Parameters.AddWithValue("SUCCESS");
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var insert = new NpgsqlCommand(
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)", connection))
            {
                insert.Parameters.AddWithValue(2L);
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                insert.Parameters.AddWithValue("wait_stats");
                insert.Parameters.AddWithValue(utcNow.AddDays(-45));
                insert.Parameters.AddWithValue("SUCCESS");
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* config_alert_log (fired-alert history) purges on its own 90-day horizon: a 100-day row is past
               it and goes, a 1-hour row survives. Only alert_time / server_id / server_name / metric_name +
               the two NOT NULL value columns are required (the rest of the V3 columns default). */
            using (var insert = new NpgsqlCommand(
                "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES ($1, $2, $3, $4, $5, $6)", connection))
            {
                insert.Parameters.AddWithValue(utcNow.AddDays(-100));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                insert.Parameters.AddWithValue("cpu");
                insert.Parameters.AddWithValue(99.0);
                insert.Parameters.AddWithValue(80.0);
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var insert = new NpgsqlCommand(
                "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES ($1, $2, $3, $4, $5, $6)", connection))
            {
                insert.Parameters.AddWithValue(utcNow.AddHours(-1));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                insert.Parameters.AddWithValue("cpu");
                insert.Parameters.AddWithValue(99.0);
                insert.Parameters.AddWithValue(80.0);
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* config.config_command purges TERMINAL rows past 30 days. Four rows prove the whole contract:
               a 40-day succeeded and a 40-day failed row GO, a 40-day PENDING row SURVIVES (retention must
               never delete a live command out from under the caller polling it, however old), and a fresh
               terminal row survives the horizon. command_id is GENERATED ALWAYS AS IDENTITY, so it is never
               supplied; target_server_id carries TestServerId so the cleanup can scope to our rows. */
            foreach (var (status, ageDays) in new[]
                     {
                         ("succeeded", 40), ("failed", 40), ("pending", 40), ("succeeded", 0),
                     })
            {
                using var insert = new NpgsqlCommand(
                    "INSERT INTO config.config_command (created_at, requested_by, command_type, target_server_id, status) VALUES ($1, $2, $3, $4, $5)", connection);
                insert.Parameters.AddWithValue(ageDays == 0 ? utcNow.AddHours(-1) : utcNow.AddDays(-ageDays));
                insert.Parameters.AddWithValue("retention-e2e");
                insert.Parameters.AddWithValue("snapshot_now");
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue(status);
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* collect.plan_force_actions (the force-plan bot's decision journal) purges on its own 365-day
               horizon. The two ages are chosen to DISCRIMINATE that horizon rather than merely exercise it: a
               400-day row is past it and goes, and a 100-day row SURVIVES even though it is past every other
               horizon in the store (90-day alert history, 60-day collection_log, 30-day base). So this fails
               if the purge is wired to a shorter sibling's constant by copy-paste, and it fails if the table
               is not purged at all. Only the NOT NULL columns without defaults are supplied; action_id is
               GENERATED ALWAYS AS IDENTITY, so it is never written. */
            foreach (var (ageDays, decision) in new[] { (400, "would_force"), (100, "blocked") })
            {
                using var insert = new NpgsqlCommand(
                    "INSERT INTO collect.plan_force_actions"
                    + " (action_time, server_id, server_name, database_name, query_id, plan_id, action, mode, decision, outcome)"
                    + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
                insert.Parameters.AddWithValue(utcNow.AddDays(-ageDays));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("retention-e2e");
                insert.Parameters.AddWithValue("retention_e2e_db");
                insert.Parameters.AddWithValue(1L);
                insert.Parameters.AddWithValue(2L);
                insert.Parameters.AddWithValue("force");
                insert.Parameters.AddWithValue("dry_run");
                insert.Parameters.AddWithValue(decision);
                insert.Parameters.AddWithValue("journaled");
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* At least our three expired rows go (40-day wait_stats, 70-day collection_log, 100-day
               config_alert_log); a shared dev store may shed more. The extension-free DELETE path on purpose
               (timescaleAvailable: false) — it must keep working even on a store whose tables ARE hypertables,
               INCLUDING ones with compressed chunks (the shared fixture's compression policies run mid-suite;
               #1564's time-sliced DELETE rides DML decompression); the drop_chunks branch is
               TimescaleSupportTests' job. */
            /* Deliberately NO assertion on the returned global activity count (#1564): the shared dev
               store's contents at purge time depend on sibling-class order, making the global number
               flaky ("expected 3, got 2" on one dispatch). The contract is the OWN-SCOPED evidence
               below, keyed on TestServerId per table, plus the fleet-sentinel audit record. The capturing
               logger folds any per-table purge warning into the failure text (a silent skip was #1564's
               whole failure mode). */
            var purgeLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, purgeLog, ct);

            using (var read = new NpgsqlCommand(
                "SELECT collection_time FROM wait_stats WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), $"the fresh wait_stats row did not survive the purge; {purgeLog.Joined}");
                var survivor = reader.GetDateTime(0);
                Assert.True(survivor > utcNow.AddDays(-1), $"the surviving row should be the 1-hour one, got {survivor:O}; {purgeLog.Joined}");
                Assert.False(await reader.ReadAsync(ct), $"the 40-day wait_stats row survived the purge; {purgeLog.Joined}");
            }

            using (var read = new NpgsqlCommand(
                "SELECT collection_time FROM collection_log WHERE server_id = $1 ORDER BY collection_time DESC", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), $"the 45-day collection_log row (inside the 60-day 2x horizon) did not survive the purge; {purgeLog.Joined}");
                var survivor = reader.GetDateTime(0);
                Assert.True(survivor < utcNow.AddDays(-44) && survivor > utcNow.AddDays(-46),
                    $"the surviving log row should be the 45-day one, got {survivor:O}; {purgeLog.Joined}");
                Assert.False(await reader.ReadAsync(ct), $"the 70-day collection_log row survived past the 60-day horizon; {purgeLog.Joined}");
            }

            using (var read = new NpgsqlCommand(
                "SELECT alert_time FROM config_alert_log WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), $"the fresh config_alert_log row did not survive the purge; {purgeLog.Joined}");
                var survivor = reader.GetDateTime(0);
                Assert.True(survivor > utcNow.AddDays(-1), $"the surviving alert-history row should be the 1-hour one, got {survivor:O}; {purgeLog.Joined}");
                Assert.False(await reader.ReadAsync(ct), $"the 100-day config_alert_log row survived past the 90-day horizon; {purgeLog.Joined}");
            }

            using (var read = new NpgsqlCommand(
                "SELECT status, created_at FROM config.config_command WHERE target_server_id = $1 ORDER BY created_at", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);

                var survivors = new List<(string Status, DateTime CreatedAt)>();
                while (await reader.ReadAsync(ct))
                {
                    survivors.Add((reader.GetString(0), reader.GetDateTime(1)));
                }

                /* The 40-day pending row (never purged, whatever its age) and the 1-hour terminal row. */
                Assert.Equal(2, survivors.Count);
                Assert.Contains(survivors, s => s.Status == "pending" && s.CreatedAt < utcNow.AddDays(-39));
                Assert.Contains(survivors, s => s.Status == "succeeded" && s.CreatedAt > utcNow.AddDays(-1));
                Assert.DoesNotContain(survivors, s => s.Status == "failed");
            }

            using (var read = new NpgsqlCommand(
                "SELECT action_time FROM collect.plan_force_actions WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct),
                    $"the 100-day plan_force_actions row (inside the 365-day horizon) did not survive the purge; {purgeLog.Joined}");
                var survivor = reader.GetDateTime(0);
                Assert.True(survivor < utcNow.AddDays(-99) && survivor > utcNow.AddDays(-101),
                    $"the surviving journal row should be the 100-day one, got {survivor:O}; {purgeLog.Joined}");
                Assert.False(await reader.ReadAsync(ct),
                    $"the 400-day plan_force_actions row survived past the 365-day horizon; {purgeLog.Joined}");
            }

            /* The purge writes ONE auditable run-record under the fleet sentinel server_id — SUCCESS here
               (every table purged cleanly on this store). Never attributed to a real monitored server. */
            using (var read = new NpgsqlCommand(
                "SELECT status FROM collection_log WHERE server_id = $1 AND collector_name = 'data_retention' ORDER BY collection_time DESC LIMIT 1", connection))
            {
                read.Parameters.AddWithValue(DarlingObservability.FleetServerId);
                var status = await read.ExecuteScalarAsync(ct) as string;
                Assert.True(status == "SUCCESS", $"expected a SUCCESS run-record, got '{status}'; {purgeLog.Joined}");
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
    public async Task EndToEnd_DeletePathPurgesExpiredRows_InsideACompressedChunk_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live compressed-chunk purge test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.SkipUnless(await TimescaleSupport.DetectAsync(connection, ct),
            "TimescaleDB is not installed in the DARLING_TEST_PG store — the compressed-chunk purge shape needs it.");

        /* #1564's deterministic regression: the DELETE purge path must clear expired rows even when their
           chunk is COMPRESSED. The old ctid-batched DELETE errored on any compressed in-range chunk
           ("transparent decompression only supports tableoid system column"), silently skipped the table,
           and the expired rows survived — in CI the compression E2E's leftover policies compressed chunks
           mid-suite (policy jobs run immediately on creation), so this fired as an order/timing flake; in
           production it broke the drop_chunks-failed fallback exactly when compressed chunks made it
           matter. Here the compression is applied SYNCHRONOUSLY (no background-job race), so the shape is
           exercised on every run. */
        Assert.True(await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct) > 0);
        await ExecAsync(connection, "ALTER TABLE wait_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

            using (var insert = new NpgsqlCommand(
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", connection))
            {
                insert.Parameters.AddWithValue(1L);
                insert.Parameters.AddWithValue(utcNow.AddDays(-40));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("compressed-purge-e2e");
                await insert.ExecuteNonQueryAsync(ct);
            }

            using (var insert = new NpgsqlCommand(
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", connection))
            {
                insert.Parameters.AddWithValue(2L);
                insert.Parameters.AddWithValue(utcNow.AddHours(-1));
                insert.Parameters.AddWithValue(TestServerId);
                insert.Parameters.AddWithValue("compressed-purge-e2e");
                await insert.ExecuteNonQueryAsync(ct);
            }

            /* Compress the -40d row's chunk (if_not_compressed tolerates the shared fixture's policy having
               beaten us to it), then PROVE the fixture is exercising the compressed shape. */
            await ExecAsync(connection,
                "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('wait_stats', older_than => INTERVAL '35 days') c", ct);
            using (var check = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'wait_stats'
  AND is_compressed
  AND range_end < now() - INTERVAL '35 days'", connection))
            {
                Assert.True((long)(await check.ExecuteScalarAsync(ct))! >= 1,
                    "expected the -40d wait_stats chunk to be compressed — the fixture is not exercising the compressed-chunk shape");
            }

            /* timescaleAvailable: false forces the DELETE path — the exact statement shape that used to
               error — against the compressed chunk. */
            var purgeLog = new CapturingTestLogger();
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, purgeLog, ct);

            using (var read = new NpgsqlCommand(
                "SELECT collection_time FROM wait_stats WHERE server_id = $1", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), $"the fresh wait_stats row did not survive the compressed-chunk purge; {purgeLog.Joined}");
                var survivor = reader.GetDateTime(0);
                Assert.True(survivor > utcNow.AddDays(-1), $"the surviving row should be the 1-hour one, got {survivor:O}; {purgeLog.Joined}");
                Assert.False(await reader.ReadAsync(ct), $"the 40-day row inside the compressed chunk survived the purge; {purgeLog.Joined}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task EndToEnd_PurgeResolverThrows_WritesErrorRunRecord_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live ERROR run-record test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            /* A resolver that throws drives PurgeAsync into its outer catch, which writes an ERROR run-record
               to the store under the fleet sentinel — the failure is auditable, not a crashed loop. */
            var summary = await DarlingRetention.PurgeAsync(
                postgres, timescaleAvailable: false, null, ct,
                retentionDaysFor: _ => throw new InvalidOperationException("resolver boom"));
            Assert.Equal(0, summary.TablesPurged);

            using var read = new NpgsqlCommand(
                "SELECT status FROM collection_log WHERE server_id = $1 AND collector_name = 'data_retention' ORDER BY collection_time DESC LIMIT 1", connection);
            read.Parameters.AddWithValue(DarlingObservability.FleetServerId);
            Assert.Equal("ERROR", await read.ExecuteScalarAsync(ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Also clears the fleet-sentinel data_retention run-record the purge writes, so the shared dev store
           doesn't accumulate them and the run-record assertion always reads THIS run's row. */
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {TestServerId}; " +
            $"DELETE FROM collection_log WHERE server_id = {TestServerId}; " +
            $"DELETE FROM config_alert_log WHERE server_id = {TestServerId}; " +
            $"DELETE FROM config.config_command WHERE target_server_id = {TestServerId}; " +
            $"DELETE FROM collect.plan_force_actions WHERE server_id = {TestServerId}; " +
            $"DELETE FROM collection_log WHERE server_id = {DarlingObservability.FleetServerId} AND collector_name = 'data_retention';",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---------------- #2143 drop_chunks deadlock retry ---------------- */

    private static PostgresException Deadlock() =>
        new("deadlock detected", "ERROR", "ERROR", PostgresErrorCodes.DeadlockDetected);

    [Fact]
    public async Task DropChunksRetry_OneDeadlock_RetriesOnce_AndSucceeds()
    {
        /* The field case (#2143, caught by the nightly's purge e2e): the first attempt loses a deadlock
           to a background job whose locks clear in milliseconds — the retry completes the purge instead
           of wasting the cycle on the DELETE fallback. */
        var calls = 0;
        var result = await DarlingRetention.ExecuteDropChunksWithDeadlockRetryAsync(
            () => ++calls == 1 ? throw Deadlock() : Task.FromResult(3),
            "collection_log", logger: null);

        Assert.Equal(2, calls);
        Assert.Equal(3, result);
    }

    [Fact]
    public async Task DropChunksRetry_TwoDeadlocks_GivesUpToTheDeleteFallback()
    {
        /* A second deadlock in a row is STANDING contention — camping a retry loop on a lock queue is
           worse than the DELETE fallback + next cycle. Exactly two attempts, then null. */
        var calls = 0;
        var result = await DarlingRetention.ExecuteDropChunksWithDeadlockRetryAsync(
            () => { calls++; throw Deadlock(); },
            "collection_log", logger: null);

        Assert.Equal(2, calls);
        Assert.Null(result);
    }

    [Fact]
    public async Task DropChunksRetry_NonDeadlockFailure_DoesNotRetry()
    {
        /* Only 40P01 earns a retry — any other failure keeps the original single-shot posture (a missing
           relation or a permission error does not get better by asking again). */
        var calls = 0;
        var result = await DarlingRetention.ExecuteDropChunksWithDeadlockRetryAsync(
            () => { calls++; throw new InvalidOperationException("not a deadlock"); },
            "collection_log", logger: null);

        Assert.Equal(1, calls);
        Assert.Null(result);
    }

    [Fact]
    public async Task DropChunksRetry_CleanRun_IsSingleShot()
    {
        var calls = 0;
        var result = await DarlingRetention.ExecuteDropChunksWithDeadlockRetryAsync(
            () => Task.FromResult(++calls == 1 ? 7 : -1),
            "collection_log", logger: null);

        Assert.Equal(1, calls);
        Assert.Equal(7, result);
    }

    /* ---------------- #2386: the plan dim is capped by ROWS, not by a time slice ---------------- */

    /// <summary>
    /// The statement, and the two properties that make it work where the time slice does not.
    ///
    /// <para><b>Measured on the live use2 store</b> (133 GB / 12.4 M rows): deletes run at ~1,000 rows/sec,
    /// linear from 10 k to 50 k, worst observed 834 rows/sec. A one-DAY slice there is ~755 k rows of
    /// ~9.5 KB gzipped plan XML — ~7 GB of TOAST in one statement, needing ~755 s against a 300 s command
    /// timeout. It timed out, rolled back, deleted nothing, and every later sweep retried the same doomed
    /// slice, so retention on the store's largest table stopped permanently while the table kept
    /// growing.</para>
    /// </summary>
    [Fact]
    public void RowCappedDelete_IsOldestFirst_AndBoundedByRows()
    {
        var sql = DarlingRetention.RowCappedDeleteSql("query_plan_dim", "last_seen", 50_000);

        Assert.Equal(
            "DELETE FROM query_plan_dim WHERE ctid IN ("
            + "SELECT ctid FROM query_plan_dim WHERE last_seen < $1 "
            + "ORDER BY last_seen LIMIT 50000)",
            sql);

        /* Oldest-first is not cosmetic: progress has to be monotonic. An unordered cap nibbles arbitrary
           rows and leaves MIN(last_seen) where it was, so the floor never advances and the next sweep
           faces the same work. */
        Assert.Contains("ORDER BY last_seen LIMIT", sql, StringComparison.Ordinal);

        /* Bounded by rows, never by a time span — that is the whole point. */
        Assert.DoesNotContain("INTERVAL", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The cap is a measurement, not a round number. 50 k costs ~50 s at the measured rate, leaving ~5x
    /// margin under <c>DeleteTimeoutSeconds</c> — margin that exists because a loaded box is slower than
    /// the idle one this was measured on. 100 k would run ~97 s nominal and ~291 s under a 3x slowdown,
    /// i.e. back against the wall this fix exists to move away from.
    /// </summary>
    [Fact]
    public void ThePlanDimRowCap_LeavesRoomUnderTheCommandTimeout()
    {
        Assert.Equal(50_000, DarlingRetention.PlanDimDeleteRowCap);

        /* At the WORST measured throughput (834 rows/sec), the cap must still finish comfortably inside
           the command timeout. Pinned as arithmetic so raising the cap without re-measuring fails here. */
        const int worstObservedRowsPerSecond = 834;
        var worstCaseSeconds = DarlingRetention.PlanDimDeleteRowCap / (double)worstObservedRowsPerSecond;

        Assert.True(
            worstCaseSeconds < 120,
            $"cap would take {worstCaseSeconds:F0}s at the worst measured rate; the timeout is 300s and "
            + "the margin is deliberate");
    }

    /// <summary>
    /// Only the plan dimension is capped. <c>query_text_dim</c> is ~40 MB in total and drains in a single
    /// slice, and the fact tables need the compressed-chunk-safe shape that the <c>ctid</c> idiom cannot
    /// provide (#1564) — so the row cap is scoped to the one table whose rows are large enough to matter.
    /// </summary>
    [Fact]
    public void OnlyThePlanDim_TakesTheRowCap()
    {
        var source = ReadRetentionSource();

        var at = source.IndexOf("foreach (var dimTable in PayloadDimensions.DimTables)", StringComparison.Ordinal);
        Assert.True(at >= 0, "the dim purge loop moved (#2386)");

        var body = source[at..Math.Min(source.Length, at + 2200)];

        /* The plan dim is selected by name, and BOTH the statement and the drain batch size follow it. */
        Assert.Contains("PayloadDimensions.QueryPlanDimTable", body, StringComparison.Ordinal);
        Assert.Contains("RowCappedDeleteSql(", body, StringComparison.Ordinal);
        Assert.Contains("TimeSlicedDeleteSql(", body, StringComparison.Ordinal);
        Assert.Contains("batchSize:", body, StringComparison.Ordinal);

        /* A row-capped statement MUST carry its cap as the batch size, or the drain loop reverts to
           "stop when a batch clears nothing" and does one batch per sweep instead of draining. */
        Assert.Contains("PlanDimDeleteRowCap", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// A timed-out purge reports what it actually removed. Each statement autocommits, so a failure on the
    /// fifth batch does not undo the first four — but the old catch returned null and discarded the running
    /// total, so the sweep summary said "0 row(s) deleted, 1 failed" for a purge that had deleted 755 k
    /// rows. That is the line an operator reads, and it turned "slower than the timeout" into "completely
    /// stuck", which is a different problem with a different fix.
    /// </summary>
    [Fact]
    public void AFailedPurge_ReportsTheRowsItDidRemove()
    {
        var source = ReadRetentionSource();

        var at = source.IndexOf("private static async Task<int?> PurgeOneAsync(", StringComparison.Ordinal);
        Assert.True(at >= 0, "PurgeOneAsync moved (#2386)");

        /* Brace-matched rather than a fixed character window. The window was 4000, the method is now 4651
           chars, and the catch this test exists to check sits at 4521 — so #2401's comment additions pushed
           the assertion's target out of the slice and the test failed while the code it guards was correct.
           A guard whose reach depends on how much prose the method carries is a guard that goes off at the
           wrong times. */
        var body = MethodBodyFrom(source, at);

        Assert.Contains("after removing {Rows} row(s)", body, StringComparison.Ordinal);

        /* The accumulator has to live outside the try, or the catch cannot see it. Anchored on the CATCH
           rather than on "try": the word appears in the explanatory comment above the block, so matching
           it finds prose instead of code and the assertion passes for the wrong reason. */
        Assert.True(
            body.IndexOf("var deleted = 0;", StringComparison.Ordinal)
                < body.IndexOf("catch (Exception ex)", StringComparison.Ordinal),
            "the row accumulator must be declared before the catch can read it (#2386)");
    }

    /// <summary>
    /// The full body of the member starting at <paramref name="declarationAt"/>, by brace matching. Replaces
    /// the fixed-size slices these source guards used to take: those silently shrink their own coverage as a
    /// method grows, so an assertion can stop reaching its target without anything failing to compile.
    /// </summary>
    private static string MethodBodyFrom(string source, int declarationAt)
    {
        var open = source.IndexOf('{', declarationAt);
        Assert.True(open >= 0, "no body found at the declaration offset");

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{') depth++;
            else if (source[i] == '}' && --depth == 0) return source[declarationAt..i];
        }

        throw new InvalidOperationException("unbalanced braces scanning the method body");
    }

    private static string ReadRetentionSource(
        [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var relative = System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        for (var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!);
             dir is not null; dir = dir.Parent)
        {
            var candidate = System.IO.Path.Combine(dir.FullName, relative);
            if (System.IO.File.Exists(candidate))
            {
                return System.IO.File.ReadAllText(candidate);
            }
        }

        throw new System.IO.FileNotFoundException($"Could not locate {relative}");
    }
}
