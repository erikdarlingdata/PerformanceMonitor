/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V47 <c>pvs_stats</c> store surface (#1951): the migration itself, the viewer schema gate that
/// a StorageVersion bump obligates, and — against a real Postgres — that a row written through the
/// generated schema comes back out of the viewer read with its VALUES intact.
///
/// <para>The read-back is a value assertion, deliberately. Npgsql does not throw when a
/// <c>DateTimeKind.Utc</c> value is written to a bare <c>timestamp</c> column; it infers timestamptz and
/// PostgreSQL casts into the server's zone, so the stored value silently shifts by the server's UTC offset
/// while <c>Kind</c> round-trips as <c>Unspecified</c> either way. A Kind assertion is therefore vacuous
/// and only comparing the value catches the class — which matters here because four of this collector's
/// columns are cleaner timestamps whose exact values are the diagnostic.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PvsStatsStoreTests
{
    private const int TestServerId = 991951;

    [Fact]
    public void V47_MigrationIdentity_AndStorageVersionTracksTheNewestRung()
    {
        var v47 = PgMigrations.Scripts.Single(m => m.Version == 47);

        Assert.Equal("pvs-stats", v47.Name);
        /* V48 (#1984, the PVS-pressure alert knobs), then V49 (#1986, the database-state alert), then V50
           (#2008 2a, the server-tag colour), then V51 (#2012 stage 2, the query-stats host object), then V52
           (#2060, the persisted finding drill-down), then V53 (#2068, the store self-metrics table) followed
           this migration — the newest-rung pins track the newest, the V47 identity pins below are
           unchanged. */
        /* The invariant the test name states, with no literal to go stale: the build's schema version IS
           the newest registered rung. Three in-flight branches bumping versions made the literal form a
           recurring multi-test failure (#2210 round, again here at V62). */
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);

        /* collect.-qualified like V44 and V34, and idempotent so a re-run is a no-op. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.pvs_stats (", v47.Sql, StringComparison.Ordinal);
        Assert.Contains(
            "CREATE INDEX IF NOT EXISTS idx_pvs_stats_time ON collect.pvs_stats(server_id, collection_time);",
            v47.Sql,
            StringComparison.Ordinal);

        /* The v_* passthrough is what lets Darling's FinOps read be byte-identical to Lite's, whose own
           v_pvs_stats UNIONs the hot table with the parquet archive. Drop it and the two front ends
           diverge on their first line.

           The statement is UNQUALIFIED on purpose (it resolves through the migrate session's search_path,
           like V10-V13): DarlingObservabilityTests scans every migration for exactly this bare form, so a
           collect.-qualified view would be invisible to the guard that exists to stop a collector view
           being added without its V14 refresh. Registering the collector in PostV8ViewCollectors is the
           other half — the guard asserts the two sets are equal. */
        Assert.Contains(
            "CREATE OR REPLACE VIEW v_pvs_stats AS SELECT * FROM pvs_stats;",
            v47.Sql,
            StringComparison.Ordinal);
        Assert.Contains("v_pvs_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("FROM v_pvs_stats", ViewerDataService.PvsStatsLatestSql, StringComparison.Ordinal);

        /* The migration must carry EVERY payload column, or an upgraded store's binary COPY fails on the
           first cycle with a column the generated fresh schema has and this one does not. */
        foreach (var column in PvsStatsCollector.Instance.PayloadColumns)
        {
            Assert.Contains($"    {column.Name} ", v47.Sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ViewerSchemaGate_KnowsV47_SoAFullyMigratedStoreIsNotRefused()
    {
        /* The trap a StorageVersion bump sets: the viewer's connect-time gate probes the store and
           compares the result against RequiredStoreSchemaVersion. A probe that cannot SEE the newest
           migration reports every healthy store as skewed and refuses to open it — permanently.
           (53 since #2068's store self-metrics table; the full-sentinel pin lives in
           ViewerDataServiceTests.) Invariant form, no literal to go stale: the gate always
           requires exactly the build's schema version. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
        Assert.Contains("table_name = 'pvs_stats'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        /* The V47 arm: pvs_stats present (and nothing newer) maps to exactly 47. */
        Assert.Equal(47, ViewerDataService.MapProbedSchemaVersion(
            hasConfigControlPlane: true, hasAlertDeliveryOverride: true, hasAnalysisState: true,
            hasAlertTuningKnobs: true, hasDefaultTraceEvents: true, hasIndexObjectStatsLatestIndex: true,
            hasCollectionLogHypertableOrPlainPg: true, hasJobHistory: true, hasAgentStatus: true,
            hasGenericWebhook: true, hasDeadlocksDatabaseName: true, hasQueryStoreReplicaRole: true,
            hasLongQueryCompletions: true, hasWebDashboardConfig: true, hasCustomViews: true,
            hasServerTags: true, hasConnectionRefireKnobs: true, hasAgCollectors: true,
            hasAgAlertKnobs: true, hasAgLatencyColumns: true, hasAgDisconnectRefire: true,
            hasPayloadDimensions: true, hasDimFloorIndexes: true, hasBlockingWaitThreshold: true,
            hasQueryStoreIntervalIdentity: true, hasPagerDutyWebhook: true, hasPagerDutyProxy: true,
            hasCollectorState: true, hasPlanCorrection: true, hasPvsStats: true));

        /* A V46 store — everything but pvs_stats — must still map to 46, not 47. */
        Assert.Equal(46, ViewerDataService.MapProbedSchemaVersion(
            hasConfigControlPlane: true, hasAlertDeliveryOverride: true, hasAnalysisState: true,
            hasAlertTuningKnobs: true, hasDefaultTraceEvents: true, hasIndexObjectStatsLatestIndex: true,
            hasCollectionLogHypertableOrPlainPg: true, hasJobHistory: true, hasAgentStatus: true,
            hasGenericWebhook: true, hasDeadlocksDatabaseName: true, hasQueryStoreReplicaRole: true,
            hasLongQueryCompletions: true, hasWebDashboardConfig: true, hasCustomViews: true,
            hasServerTags: true, hasConnectionRefireKnobs: true, hasAgCollectors: true,
            hasAgAlertKnobs: true, hasAgLatencyColumns: true, hasAgDisconnectRefire: true,
            hasPayloadDimensions: true, hasDimFloorIndexes: true, hasBlockingWaitThreshold: true,
            hasQueryStoreIntervalIdentity: true, hasPagerDutyWebhook: true, hasPagerDutyProxy: true,
            hasCollectorState: true, hasPlanCorrection: true, hasPvsStats: false));
    }

    [Fact]
    public async Task PvsStats_RoundTripsThroughTheStore_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PVS store read.");

        var collectionTime = new DateTime(2026, 8, 1, 4, 6, 37, DateTimeKind.Unspecified);
        var cleanerStart = new DateTime(2026, 8, 1, 4, 6, 30, DateTimeKind.Unspecified);
        var cleanerEnd = new DateTime(2026, 8, 1, 4, 6, 35, DateTimeKind.Unspecified);

        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            /* Idempotent: brings an older store forward to V47, no-ops on a current one. */
            await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
            await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);

            /* Values mirror the live SQL Server 2025 reading this collector was verified against:
               912.82 MB of off-row PVS inside 1280.00 MB of data files, cleanup idle. */
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.pvs_stats
(
    collection_id, collection_time, server_id, server_name,
    database_name, database_id, is_accelerated_database_recovery_on, pvs_filegroup_id,
    persistent_version_store_size_mb, online_index_version_store_size_mb, database_data_size_mb,
    current_aborted_transaction_count, oldest_active_transaction_id, oldest_aborted_transaction_id,
    min_transaction_timestamp, online_index_min_transaction_timestamp, secondary_low_water_mark,
    offrow_version_cleaner_start_time, offrow_version_cleaner_end_time,
    aborted_version_cleaner_start_time, aborted_version_cleaner_end_time,
    pvs_off_row_page_skipped_low_water_mark, pvs_off_row_page_skipped_transaction_not_cleaned,
    pvs_off_row_page_skipped_oldest_active_xdesid, pvs_off_row_page_skipped_min_useful_xts,
    pvs_off_row_page_skipped_oldest_snapshot, pvs_off_row_page_skipped_oldest_aborted_xdesid
)
VALUES
(
    $1, $2, $3, $4,
    $5, $6, $7, $8,
    $9, $10, $11,
    $12, $13, $14,
    $15, $16, $17,
    $18, $19,
    $20, $21,
    $22, $23,
    $24, $25,
    $26, $27
)", connection);

            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 1 });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = collectionTime });
            insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestServerId });
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "pvs-test-server" });
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "PvsScratch1951" });
            insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 15 });
            insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = true });
            insert.Parameters.Add(new NpgsqlParameter<short> { TypedValue = 1 });
            insert.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = 912.82m });
            insert.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = 0.00m });
            insert.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = 1280.00m });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 3 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 97388 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 1244 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 63157 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 63157 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 0 });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cleanerStart });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cleanerEnd });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cleanerStart });
            insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cleanerEnd });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 10 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 20 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 30 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 40 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 50 });
            insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 60 });

            await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var rows = await viewer.GetPvsStatsLatestAsync(TestServerId, TestContext.Current.CancellationToken);

            var row = Assert.Single(rows);
            Assert.Equal("PvsScratch1951", row.DatabaseName);
            Assert.True(row.IsAdrOn);

            /* Sizes survive the numeric(19,2) round trip exactly — a KB/MB mix-up or a lost CONVERT
               would show up here as a 1024x value, not as an error. */
            Assert.Equal(912.82m, row.PvsSizeMb);
            Assert.Equal(0.00m, row.OnlineIndexVersionStoreMb);
            Assert.Equal(1280.00m, row.DatabaseDataSizeMb);

            /* The derived ratio MS's guide reads first: 912.82 / 1280.00 = 71.3%. */
            Assert.Equal(71.3m, row.PvsPercentOfDatabase);

            Assert.Equal(3L, row.AbortedTransactionCount);
            Assert.Equal(97388L, row.OldestActiveTransactionId);
            Assert.Equal(1244L, row.OldestAbortedTransactionId);

            /* The gap MS's read is about, surfaced as a number rather than a verdict: 97388 - 1244. */
            Assert.Equal(96144L, row.AbortedTransactionLag);

            /* VALUE assertions, not Kind: the whole point of this test. A silent zone shift on write
               would land here as an offset, and Kind would read Unspecified either way. */
            Assert.Equal(cleanerStart, row.OffrowCleanerStartTime);
            Assert.Equal(cleanerEnd, row.OffrowCleanerEndTime);
            Assert.Equal(cleanerStart, row.AbortedCleanerStartTime);
            Assert.Equal(cleanerEnd, row.AbortedCleanerEndTime);

            /* Both cleaners have an end time -> idle, not stuck. */
            Assert.Equal("Idle", row.CleanupState);
            Assert.Equal(cleanerEnd, row.LastCleanupEnd);

            Assert.Equal(10L, row.SkippedLowWaterMark);
            Assert.Equal(40L, row.SkippedMinUsefulXts);
            Assert.Equal(60L, row.SkippedOldestAborted);

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: teardown goes through LiveStoreCleanup, never a hand-rolled finally. A cleanup that
               throws on its own connection would REPLACE the body's real failure with a connection error. */
            await LiveStoreCleanup.RunAsync(
                connectionString!,
                bodySucceeded,
                (connection, cancellationToken) => DeleteTestRowsAsync(connection, cancellationToken));
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken cancellationToken)
    {
        await using var delete = new NpgsqlCommand(
            "DELETE FROM collect.pvs_stats WHERE server_id = $1", connection);
        delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestServerId });
        await delete.ExecuteNonQueryAsync(cancellationToken);
    }
}
