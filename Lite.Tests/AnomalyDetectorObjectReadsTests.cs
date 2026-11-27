/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the anomaly detector's two object reads (growth and contention) find the newest two snapshot times first and bind
/// them as plain equalities, so the Parquet scan can push the time down. The answer must not change, wherever the two
/// snapshots sit: wholly hot, one hot and one archived, wholly archived, a server that stopped reporting long ago, and a
/// pair whose newer snapshot is in the archive. Each case runs the detector and compares its two facts with what the
/// statements that read through a subquery (the text before #9195, kept below as the reference) answer on the same store.
/// </summary>
public sealed class AnomalyDetectorObjectReadsTests : IDisposable
{
    private const int ServerId = 9;
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private long _nextId = 1;
    private int _archiveFiles;

    private static readonly DateTime End = new(2026, 6, 15, 12, 0, 0, DateTimeKind.Utc);

    public AnomalyDetectorObjectReadsTests()
    {
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>The reads before #9195: the two newest snapshots found in the statement through a subquery. The growth text keeps
    /// its cutoff as $4 and the contention text as $3, as it was.</summary>
    private const string ReferenceGrowthSql = @"
WITH snaps AS (SELECT DISTINCT collection_time FROM v_index_object_stats WHERE server_id = $1 AND collection_time < $4 ORDER BY collection_time DESC LIMIT 2),
latest AS (SELECT MAX(collection_time) t FROM snaps),
prior AS (SELECT MIN(collection_time) t FROM snaps),
cur AS (SELECT database_name, object_id, MAX(schema_name) schema_name, MAX(table_name) table_name, SUM(reserved_mb) mb
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM latest) GROUP BY database_name, object_id),
prv AS (SELECT database_name, object_id, SUM(reserved_mb) mb
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM prior) GROUP BY database_name, object_id)
SELECT cur.database_name, cur.schema_name, cur.table_name, prv.mb AS prior_mb, cur.mb AS current_mb,
       cur.mb - prv.mb AS growth_mb,
       (cur.mb - prv.mb) * 100.0 / NULLIF(prv.mb, 0) AS growth_pct
FROM cur JOIN prv ON cur.database_name = prv.database_name AND cur.object_id = prv.object_id
WHERE (SELECT t FROM latest) <> (SELECT t FROM prior)
AND   cur.mb - prv.mb >= $2
AND   (prv.mb <= 0 OR (cur.mb - prv.mb) * 100.0 / NULLIF(prv.mb, 0) >= $3)
ORDER BY growth_mb DESC LIMIT 1";

    private const string ReferenceContentionSql = @"
WITH snaps AS (SELECT DISTINCT collection_time FROM v_index_object_stats WHERE server_id = $1 AND collection_time < $3 ORDER BY collection_time DESC LIMIT 2),
latest AS (SELECT MAX(collection_time) t FROM snaps),
prior AS (SELECT MIN(collection_time) t FROM snaps),
cur AS (SELECT database_name, object_id, index_id, schema_name, table_name, index_name,
               COALESCE(row_lock_wait_in_ms,0) ms, COALESCE(index_lock_promotion_count,0) esc
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM latest)),
prv AS (SELECT database_name, object_id, index_id, COALESCE(row_lock_wait_in_ms,0) ms, COALESCE(index_lock_promotion_count,0) esc
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM prior))
SELECT cur.database_name, cur.schema_name, cur.table_name, cur.index_name,
       cur.ms - prv.ms AS ms_delta, cur.esc - prv.esc AS esc_delta
FROM cur JOIN prv ON cur.database_name = prv.database_name AND cur.object_id = prv.object_id AND cur.index_id = prv.index_id
WHERE (SELECT t FROM latest) <> (SELECT t FROM prior)
AND   cur.ms >= prv.ms
AND   cur.ms - prv.ms >= $2
ORDER BY ms_delta DESC LIMIT 1";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One snapshot at <paramref name="at"/>: table Orders (two indexes, so the growth read has a group to add up)
    /// and table Events, both growing from snapshot to snapshot by <paramref name="step"/>, with lock waits that climb by
    /// 70 s a snapshot on Orders' nonclustered index and by 70 s on Events' too (a tie in the contention read's top value).</summary>
    private async Task SeedSnapshotAsync(DuckDBConnection connection, DateTime at, int step, int serverId = ServerId)
    {
        foreach (var (objectId, table, index, indexId, mb, lockWait) in new[]
        {
            (100, "Orders", "PK_Orders", 1, 200 + step * 150, 1000L + step * 5000),
            (100, "Orders", "IX_Orders", 2, 100 + step * 100, 1000L + step * 70000),
            (200, "Events", "PK_Events", 1, 300 + step * 300, 2000L + step * 70000),
        })
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"INSERT INTO index_object_stats
                (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
                 schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                 user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
                VALUES ($1, $2, $3, 'TestServer', $4, 'AppDb', 7, 'dbo', $5, $6, $7, $8, 'NONCLUSTERED', $9, $9, 1000,
                        0, 0, 0, 0, $10, $11)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId++); P(at); P(serverId); P(End.AddDays(-90)); P(objectId); P(table); P(indexId); P(index);
            P((decimal)mb); P(lockWait); P(step);
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    /// <summary>A CPU row inside the baseline gate's 30 days, so the detector runs at all.</summary>
    private async Task SeedBaselineGateAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = End.AddDays(-2) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Moves the rows stamped at <paramref name="times"/> out of the hot table into their own Parquet file in the
    /// archive folder, as the archive move does.</summary>
    private async Task ArchiveAsync(DuckDBConnection connection, params DateTime[] times)
    {
        Directory.CreateDirectory(_archiveDir);
        var list = string.Join(", ", times.Select(t => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'"));
        var file = Path.Combine(_archiveDir, $"2026{++_archiveFiles:0000}_0000_index_object_stats.parquet").Replace("\\", "/");
        await ExecuteAsync(connection, $"COPY (SELECT * FROM index_object_stats WHERE collection_time IN ({list})) TO '{file}' (FORMAT PARQUET)");
        await ExecuteAsync(connection, $"DELETE FROM index_object_stats WHERE collection_time IN ({list})");
    }

    private sealed record Snapshot(DateTime At, int Step, bool Archived);

    /// <summary>Seeds the snapshots (archived ones moved to Parquet), runs the detector with the window ending at
    /// <see cref="End"/>, and compares its object facts with the reference statements on the same store.</summary>
    private async Task RunAsync(Snapshot[] snapshots, bool expectFacts)
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, "BEGIN TRANSACTION");
            await SeedBaselineGateAsync(connection);
            foreach (var s in snapshots)
                await SeedSnapshotAsync(connection, s.At, s.Step);
            /* Another server's snapshots, newer than any of this one's: the read is for one server only. */
            await SeedSnapshotAsync(connection, End.AddMinutes(-5), 9, serverId: ServerId + 1);
            await SeedSnapshotAsync(connection, End.AddMinutes(-6), 1, serverId: ServerId + 1);

            await ExecuteAsync(connection, "COMMIT");
            foreach (var s in snapshots.Where(x => x.Archived))
                await ArchiveAsync(connection, s.At);
        }

        await initializer.CreateArchiveViewsAsync();

        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = End.AddHours(-4),
            TimeRangeEnd = End
        };
        var facts = await new AnomalyDetector(initializer, new BaselineProvider(initializer)).DetectAnomaliesAsync(context);
        var growth = facts.SingleOrDefault(f => f.Key == "ANOMALY_OBJECT_GROWTH");
        var contention = facts.SingleOrDefault(f => f.Key == "ANOMALY_OBJECT_CONTENTION");

        using var read = initializer.CreateConnection();
        await read.OpenAsync(TestContext.Current.CancellationToken);

        using (var cmd = read.CreateCommand())
        {
            cmd.CommandText = ReferenceGrowthSql;
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = AnomalyThresholdsForTests.GrowthMb });
            cmd.Parameters.Add(new DuckDBParameter { Value = AnomalyThresholdsForTests.GrowthPct });
            cmd.Parameters.Add(new DuckDBParameter { Value = End });
            using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            if (!await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                Assert.Null(growth);
                Assert.False(expectFacts, "the scenario should have produced a growth fact");
            }
            else
            {
                Assert.NotNull(growth);
                Assert.True(expectFacts);
                Assert.Equal(reader.GetString(0), growth!.DatabaseName);
                Assert.Equal($"{reader.GetString(1)}.{reader.GetString(2)}", growth.ObjectName);
                Assert.Equal(Convert.ToDouble(reader.GetValue(3)), growth.Metadata["prior_mb"]);
                Assert.Equal(Convert.ToDouble(reader.GetValue(4)), growth.Metadata["current_mb"]);
                Assert.Equal(Convert.ToDouble(reader.GetValue(5)), growth.Metadata["growth_mb"]);
                Assert.Equal(Convert.ToDouble(reader.GetValue(6)), growth.Metadata["growth_pct"], 6);
            }
        }

        using (var cmd = read.CreateCommand())
        {
            cmd.CommandText = ReferenceContentionSql;
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = AnomalyThresholdsForTests.LockWaitMs });
            cmd.Parameters.Add(new DuckDBParameter { Value = End });
            using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            if (!await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                Assert.Null(contention);
                Assert.False(expectFacts, "the scenario should have produced a contention fact");
            }
            else
            {
                Assert.NotNull(contention);
                Assert.True(expectFacts);
                Assert.Equal(reader.GetString(0), contention!.DatabaseName);
                /* Two indexes tie on the lock-wait delta, so the read's pick between them is not part of the answer: the
                   value, the escalation delta and the database are. */
                Assert.Equal(Convert.ToDouble(reader.GetValue(4)), contention.Metadata["lock_wait_ms_delta"]);
                Assert.Equal(Convert.ToDouble(reader.GetValue(5)), contention.Metadata["escalation_delta"]);
            }
        }
    }

    private static class AnomalyThresholdsForTests
    {
        public static decimal GrowthMb => PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.ObjectGrowthMbThreshold;
        public static double GrowthPct => PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.ObjectGrowthPctThreshold;
        public static long LockWaitMs => PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.ObjectLockWaitMsDeltaThreshold;
    }

    private static Snapshot Hot(int daysBack, int step) => new(End.AddDays(-daysBack), step, false);
    private static Snapshot Archived(int daysBack, int step) => new(End.AddDays(-daysBack), step, true);

    [Fact]
    public Task AllSnapshotsHot_AnswersAsBefore() =>
        RunAsync([Hot(3, 0), Hot(2, 1), Hot(1, 2), Hot(0, 3) with { At = End.AddHours(-1) }], expectFacts: true);

    [Fact]
    public Task NewerHotOlderArchived_AnswersAsBefore() =>
        RunAsync([Archived(3, 0), Archived(2, 1), Hot(1, 2), Hot(0, 3) with { At = End.AddHours(-1) }], expectFacts: true);

    [Fact]
    public Task PairStraddlesTheStores_AnswersAsBefore() =>
        RunAsync([Archived(3, 0), Archived(2, 1), Archived(1, 2), Hot(0, 3) with { At = End.AddHours(-1) }], expectFacts: true);

    [Fact]
    public Task EverySnapshotArchived_AnswersAsBefore() =>
        RunAsync([Archived(3, 0), Archived(2, 1), Archived(1, 2), Archived(0, 3) with { At = End.AddHours(-1) }], expectFacts: true);

    /// <summary>The archive holds the newer snapshot and the hot table the older ones: the hot table alone would answer with the
    /// wrong pair.</summary>
    [Fact]
    public Task NewestSnapshotInTheArchive_AnswersAsBefore() =>
        RunAsync([Hot(3, 0), Hot(2, 1), Archived(0, 3) with { At = End.AddHours(-1) }], expectFacts: true);

    /// <summary>A server that stopped reporting: its last two snapshots are weeks old and a bounded look-back would find nothing.</summary>
    [Fact]
    public Task StaleServerHot_StillComparesItsLastTwoSnapshots() =>
        RunAsync([Hot(60, 0), Hot(59, 1)], expectFacts: true);

    [Fact]
    public Task StaleServerArchived_StillComparesItsLastTwoSnapshots() =>
        RunAsync([Archived(60, 0), Archived(59, 1)], expectFacts: true);

    [Fact]
    public Task StaleServerSplitAcrossTheStores_StillComparesItsLastTwoSnapshots() =>
        RunAsync([Archived(60, 0), Hot(59, 1)], expectFacts: true);

    /// <summary>A snapshot stamped on the window's end or after it belongs to a later window and is not compared.</summary>
    [Fact]
    public Task SnapshotsAtAndAfterTheEnd_AreNotCompared() =>
        RunAsync([Archived(2, 0), Hot(1, 1), Hot(0, 1) with { At = End }, Hot(0, 1) with { At = End.AddHours(1) }], expectFacts: true);

    [Fact]
    public Task OneSnapshotHot_RaisesNothing() => RunAsync([Hot(1, 0)], expectFacts: false);

    [Fact]
    public Task OneSnapshotArchived_RaisesNothing() => RunAsync([Archived(1, 0)], expectFacts: false);

    [Fact]
    public Task NoSnapshot_RaisesNothing() => RunAsync([], expectFacts: false);

    /// <summary>Two snapshots with the same sizes and lock waits: the pair is found and nothing grew or waited.</summary>
    [Fact]
    public Task TwoSnapshotsBothHot_NoGrowthPastTheThreshold_RaisesNothing() =>
        RunAsync([Hot(2, 0), Hot(1, 0)], expectFacts: false);
}
