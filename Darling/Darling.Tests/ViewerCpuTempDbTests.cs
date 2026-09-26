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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the CPU-tab and tempdb-tab reads (W1a viewer copy-parity) against the Darling store contract,
/// no live Postgres. The CPU tab (and, since W1d, the Overview's CPU lane) plots the de-skewed
/// per-sample series bucketed to <see cref="TrendBudget.Chart"/>'s width (#4234): a bucket wider than
/// one sample averages the two gauge columns, and a bucket holding exactly one physical sample is
/// stamped at that sample's own raw time by the C# reader rather than the bucket grid (ruling item 3).
/// The tempdb reads mirror Lite's view-based queries (v_tempdb_stats / v_file_io_stats): the numeric(18,2)
/// MB columns are CAST to double precision for the typed reader, total_sessions_using_tempdb stays bigint
/// (GetInt64). The usage read (TempDbTrendSql) is unaffected by #4234 — still one row per collection. The
/// file-I/O read filters to tempdb, averages stall/op per file, and — since #4234 — is bucketed the same
/// way as the File I/O tab's own per-file reads (Lite's twin bucketed first, #4340).
/// </summary>
public sealed class ViewerCpuTempDbSqlTests
{
    [Fact]
    public void CpuUtilizationSql_SelectsFromRawCte_WindowedOnCollectionTime_OrderedByBucketStart()
    {
        Assert.Contains("FROM cpu_utilization_stats", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("sample_time", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("sqlserver_cpu_utilization", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("other_process_cpu_utilization", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        /* The outer bucketed query groups/orders by the bucket_start ordinal, not the raw column name
           (#4234 — the pre-bucketing read ordered by the bare sample_time column). */
        Assert.Contains("GROUP BY 1", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY 1", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
    }

    [Fact]
    public void CpuUtilizationSql_IsBucketedWithGaugeAveragesAndNullAsZero()
    {
        /* #4234: both CPU columns are gauges (ruling item 2), averaged per bucket. A NULL
           other_process_cpu_utilization (SQL on Linux, #1048) is COALESCEd to 0 BEFORE the AVG, matching
           the pre-bucketing reader's per-row "NULL reads as 0" rule exactly rather than letting
           Postgres's NULL-skipping AVG silently change a mixed-OS bucket's value. */
        Assert.Contains("AVG(COALESCE(sqlserver_cpu_utilization, 0))", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
        Assert.Contains("AVG(COALESCE(other_process_cpu_utilization, 0))", ViewerDataService.CpuUtilizationSql, StringComparison.Ordinal);
    }

    /// <summary>#4234: the source check ruling item 6 asks for — every bucketed WPF trend statement carries
    /// a width parameter and buckets via the shared origin, so the width the C# picks (<c>TrendBuckets.AutoMinutes</c>)
    /// is the width the SQL actually bins on.</summary>
    [Fact]
    public void CpuUtilizationSql_CarriesABucketWidth_AndTheSingletonColumnsForRawPassthrough()
    {
        var sql = ViewerDataService.CpuUtilizationSql;
        Assert.Contains("date_bin(CAST($3 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(sample_time) AS first_sample_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS sample_count", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CpuUtilizationSql_DeSkewsSampleTime_PerBatchOffsetRoundedTo15Minutes()
    {
        /* #1262: sample_time is the monitored server's LOCAL wall clock, not naive UTC. The read
           de-skews it to naive UTC by subtracting the per-batch UTC offset
           = round(MAX(sample_time) over the batch - collection_time) to the nearest 15 minutes, so the
           naive-UTC-assuming ToLocalTime aligns the CPU series with every collection_time-based lane. */
        var sql = ViewerDataService.CpuUtilizationSql;

        /* Window MAX over the collection batch — collection_time is the batch key (in the Darling PG
           store collection_id is a plain non-unique bigint, NOT a batch id), so the partition is
           (server_id, collection_time). One value per row: no roll-up of the raw samples. */
        Assert.Contains("MAX(sample_time) OVER (PARTITION BY server_id, collection_time)", sql, StringComparison.Ordinal);

        /* Offset = anchor - collection_time in epoch seconds, rounded to the nearest 900s (15 min),
           then subtracted as an interval. */
        Assert.Contains("EXTRACT(EPOCH FROM (", sql, StringComparison.Ordinal);
        Assert.Contains("- collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("/ 900.0", sql, StringComparison.Ordinal);
        Assert.Contains("ROUND(", sql, StringComparison.Ordinal);
        Assert.Contains("INTERVAL '15 minutes'", sql, StringComparison.Ordinal);

        /* The de-skewed value is still projected as sample_time (both consumers read column 0). */
        Assert.Contains("AS sample_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CpuUtilizationSql_ReadsColumnsThatExistInTheGeneratedCpuTable()
    {
        Assert.Equal("cpu_utilization_stats", CpuUtilizationCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(CpuUtilizationCollector.Instance);
        foreach (var column in new[] { "sample_time", "sqlserver_cpu_utilization", "other_process_cpu_utilization" })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TempDbTrendSql_SelectsUsageColumns_CastsMbToDouble_OverTheWindow()
    {
        Assert.Contains("FROM v_tempdb_stats", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);

        /* Every numeric(18,2) MB column is CAST to double precision for the typed GetDouble reader. */
        foreach (var col in new[]
        {
            "user_object_reserved_mb", "internal_object_reserved_mb", "version_store_reserved_mb",
            "total_reserved_mb", "unallocated_mb", "top_session_tempdb_mb",
        })
        {
            Assert.Contains($"CAST({col} AS double precision)", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
        }

        /* bigint session count + integer session id are selected raw (read with their own getters). */
        Assert.Contains("total_sessions_using_tempdb", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
        Assert.Contains("top_session_id", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
        /* The bigint session count must NOT be cast to double (it's read via GetInt64). */
        Assert.DoesNotContain("CAST(total_sessions_using_tempdb", ViewerDataService.TempDbTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TempDbTrendSql_ReadsColumnsThatExistInTheGeneratedTempDbTable()
    {
        Assert.Equal("tempdb_stats", TempDbStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(TempDbStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "user_object_reserved_mb", "internal_object_reserved_mb",
            "version_store_reserved_mb", "total_reserved_mb", "unallocated_mb",
            "total_sessions_using_tempdb", "top_session_id", "top_session_tempdb_mb",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TempDbFileIoTrendSql_FiltersToTempDb_GroupsPerFileAndBucket_CastsStallToDouble()
    {
        Assert.Contains("FROM v_file_io_stats", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("database_name = 'tempdb'", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        /* #4234: grouped on file_name plus the bucket_start ordinal, not the raw collection_time column
           (the pre-bucketing read grouped/ordered on the bare column). */
        Assert.Contains("GROUP BY file_name, 2", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY file_name, 2", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("CAST(delta_stall_read_ms AS double precision)", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains("CAST(delta_stall_write_ms AS double precision)", ViewerDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
    }

    /// <summary>#4234 ruling item 6 (source check): the tempdb file-I/O read carries a bucket width and
    /// projects the singleton-detection columns, mirroring <c>FileIoLatencyTrendSql</c>. Proven once by
    /// hand against the pre-#4234 text: no <c>date_bin</c> anywhere (a per-collection read has no bucket
    /// width), so this assert fails there.</summary>
    [Fact]
    public void TempDbFileIoTrendSql_CarriesABucketWidth_AndProjectsSingletonDetectionColumns()
    {
        var sql = ViewerDataService.TempDbFileIoTrendSql;
        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING COUNT(rated_reads) > 0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TempDbFileIoTrendSql_ReadsColumnsThatExistInTheGeneratedFileIoTable()
    {
        Assert.Equal("file_io_stats", FileIoStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(FileIoStatsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "database_name", "file_name",
            "delta_reads", "delta_writes", "delta_stall_read_ms", "delta_stall_write_ms",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("cpu")]
    [InlineData("tempdb")]
    [InlineData("fileio")]
    public void NewViewerReads_PgDialect_PositionalParams_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "cpu" => ViewerDataService.CpuUtilizationSql,
            "tempdb" => ViewerDataService.TempDbTrendSql,
            _ => ViewerDataService.TempDbFileIoTrendSql,
        };

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the W1a reads plus the #1262 CPU de-skew: raw CPU
/// samples (order + NULL-other-as-0); the CPU sample_time de-skew (a server-local batch shifted back to
/// naive UTC by the per-batch offset, and a UTC batch left untouched, both landing on collection_time);
/// the tempdb usage read (MB-as-double and — the load-bearing check — the bigint
/// total_sessions_using_tempdb read via GetInt64, which GetInt32 would throw on); and the tempdb
/// file-I/O read (tempdb-only filtering, per-file average-latency computation, and — since #4234 — the
/// same per-file bucket cap and singleton pass-through as the File I/O tab's own reads). Shares the
/// serialized "live-postgres" collection so the row churn can't race another class; uses negative
/// sentinel server_ids and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerCpuTempDbLivePostgresTests
{
    private const int CpuServerId = -949494;
    private const string CpuServerName = "viewer-cpu-tab-e2e";

    private const int SkewServerId = -979797;
    private const string SkewServerName = "viewer-cpu-skew-e2e";

    private const int BudgetServerId = -989898;
    private const string BudgetServerName = "viewer-cpu-budget-e2e";

    private const int TempDbServerId = -959595;
    private const string TempDbServerName = "viewer-tempdb-tab-e2e";

    private const int FileIoServerId = -969696;
    private const string FileIoServerName = "viewer-tempdb-fileio-e2e";

    private const int FileIoBudgetServerId = -969697;

    [Fact]
    public async Task Cpu_ReadsRawSamplesInOrder_NullOtherAsZero_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU-tab test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "cpu_utilization_stats", CpuServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var collUtc = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var s1 = collUtc;
            var s2 = collUtc.AddMinutes(1);
            var s3 = collUtc.AddMinutes(2);

            /* Three raw samples in one collection; the middle one has NULL other-process CPU (SQL on
               Linux). Inserted out of sample_time order to prove the read's ORDER BY sample_time. This
               batch is a UTC server (sample_time == UTC), so the #1262 de-skew is a no-op here (the
               newest sample is +2min from collection_time, which rounds to a 0 offset). */
            await InsertCpuAsync(connection, CpuServerId, CpuServerName, collUtc, s3, sqlCpu: 30, otherCpu: 15);
            await InsertCpuAsync(connection, CpuServerId, CpuServerName, collUtc, s1, sqlCpu: 10, otherCpu: 5);
            await InsertCpuAsync(connection, CpuServerId, CpuServerName, collUtc, s2, sqlCpu: 20, otherCpu: null);

            var samples = await viewer.GetCpuUtilizationAsync(CpuServerId, collUtc.AddMinutes(-1));

            Assert.Equal(3, samples.Count);
            /* Raw (not averaged), ordered by sample_time. */
            Assert.Equal(new[] { s1.Ticks, s2.Ticks, s3.Ticks }, samples.Select(s => s.SampleTime.Ticks));
            Assert.Equal(new[] { 10.0, 20.0, 30.0 }, samples.Select(s => s.SqlServerCpu));
            /* Middle sample's NULL other-process CPU reads as 0. */
            Assert.Equal(new[] { 5.0, 0.0, 15.0 }, samples.Select(s => s.OtherProcessCpu));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "cpu_utilization_stats", CpuServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task Cpu_DeSkewsServerLocalSampleTimeToUtc_AlignsWithCollectionTime_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU de-skew test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "cpu_utilization_stats", SkewServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* #1262 round-trip. cpu_utilization_stats stores sample_time as the monitored server's LOCAL
               wall clock while collection_time is naive UTC. GetCpuUtilizationAsync must de-skew each
               sample back to true naive UTC by the per-batch offset (round(MAX(sample_time) over the
               batch - collection_time) to 15 min), so every batch below lands back on its collection_time
               axis regardless of the server's timezone. In each batch the newest sample is 30s before the
               collection instant (the anchor the de-skew keys on) and an older sample sits 5 min back;
               each batch has its own collection_time (an hour apart) so the (server_id, collection_time)
               partitions stay separate. Four zones exercise the derivation:
                 - Pacific PDT (UTC-7): Erik's fleet in summer.
                 - Pacific PST (UTC-8): the same fleet across its DST boundary.
                 - India IST (UTC+5:30): a POSITIVE, HALF-hour offset — proves the 15-minute rounding
                   generalizes past whole hours (14:00 - 30s -> round(+5:30 -30s) = +5:30 exactly).
                 - UTC (offset 0): the de-skew must be a no-op (stored local already equals true UTC). */
            var pacificSummer = TimeSpan.FromHours(-7);
            var pacificWinter = TimeSpan.FromHours(-8);
            var indiaStandard = new TimeSpan(5, 30, 0);
            var utc = TimeSpan.Zero;

            var collPdt = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Unspecified);
            var collPst = new DateTime(2026, 6, 1, 13, 0, 0, DateTimeKind.Unspecified);
            var collInd = new DateTime(2026, 6, 1, 14, 0, 0, DateTimeKind.Unspecified);
            var collUtc = new DateTime(2026, 6, 1, 15, 0, 0, DateTimeKind.Unspecified);

            /* trueX = the intended true naive-UTC sample time; the row stores trueX + zone offset (the
               server-local wall clock the collector would record). */
            var truePdtOld = collPdt.AddMinutes(-5); var truePdtNew = collPdt.AddSeconds(-30);
            var truePstOld = collPst.AddMinutes(-5); var truePstNew = collPst.AddSeconds(-30);
            var trueIndOld = collInd.AddMinutes(-5); var trueIndNew = collInd.AddSeconds(-30);
            var trueUtcOld = collUtc.AddMinutes(-5); var trueUtcNew = collUtc.AddSeconds(-30);

            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collPdt, truePdtOld + pacificSummer, sqlCpu: 11, otherCpu: 3);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collPdt, truePdtNew + pacificSummer, sqlCpu: 12, otherCpu: 4);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collPst, truePstOld + pacificWinter, sqlCpu: 21, otherCpu: 5);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collPst, truePstNew + pacificWinter, sqlCpu: 22, otherCpu: 6);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collInd, trueIndOld + indiaStandard, sqlCpu: 31, otherCpu: 7);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collInd, trueIndNew + indiaStandard, sqlCpu: 32, otherCpu: 8);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collUtc, trueUtcOld + utc, sqlCpu: 41, otherCpu: 9);
            await InsertCpuAsync(connection, SkewServerId, SkewServerName, collUtc, trueUtcNew + utc, sqlCpu: 42, otherCpu: 10);

            /* #4234: an explicit endUtc close to the fixture's own fixed 2026-06-01 window — the default
               (the wall clock) would otherwise size the bucket width off a multi-month gap and merge these
               hour-apart batches together. */
            var samples = await viewer.GetCpuUtilizationAsync(SkewServerId, collPdt.AddMinutes(-10), collUtc.AddMinutes(10));

            Assert.Equal(8, samples.Count);

            /* Every sample_time comes back as its intended true naive UTC — each skewed batch shifted by
               the negative of its zone offset, the UTC batch untouched — ordered by that de-skewed time.
               Paired with its (unique) CPU value so the assertion pins which row de-skewed to which
               instant. */
            Assert.Equal(
                new (long, double)[]
                {
                    (truePdtOld.Ticks, 11),
                    (truePdtNew.Ticks, 12),
                    (truePstOld.Ticks, 21),
                    (truePstNew.Ticks, 22),
                    (trueIndOld.Ticks, 31),
                    (trueIndNew.Ticks, 32),
                    (trueUtcOld.Ticks, 41),
                    (trueUtcNew.Ticks, 42),
                },
                samples.Select(s => (s.SampleTime.Ticks, s.SqlServerCpu)));

            /* The load-bearing outcome: each batch's newest de-skewed sample now sits within the 15-min
               rounding tolerance of its own collection_time, on every zone — i.e. the CPU series is back
               on the collection_time axis every other lane plots on. */
            Assert.True((samples[1].SampleTime - collPdt).Duration() <= TimeSpan.FromMinutes(15),
                "The Pacific PDT (UTC-7) batch's newest sample should align with its collection_time after de-skew.");
            Assert.True((samples[3].SampleTime - collPst).Duration() <= TimeSpan.FromMinutes(15),
                "The Pacific PST (UTC-8) batch's newest sample should align with its collection_time after de-skew.");
            Assert.True((samples[5].SampleTime - collInd).Duration() <= TimeSpan.FromMinutes(15),
                "The India IST (UTC+5:30) batch's newest sample should align with its collection_time after de-skew.");
            Assert.True((samples[7].SampleTime - collUtc).Duration() <= TimeSpan.FromMinutes(15),
                "The UTC batch's newest sample should align with its collection_time (unchanged).");

            /* Each skewed batch was genuinely shifted (stored local != returned UTC); the UTC batch was
               left untouched (stored local == returned UTC). */
            Assert.NotEqual((truePdtNew + pacificSummer).Ticks, samples[1].SampleTime.Ticks);
            Assert.NotEqual((truePstNew + pacificWinter).Ticks, samples[3].SampleTime.Ticks);
            Assert.NotEqual((trueIndNew + indiaStandard).Ticks, samples[5].SampleTime.Ticks);
            Assert.Equal(trueUtcNew.Ticks, samples[7].SampleTime.Ticks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "cpu_utilization_stats", SkewServerId, cleanupCt));
        }
    }

    /// <summary>#4234 ruling item 6: a 7-day window (one sample per minute, the ring buffer's real cadence)
    /// must not hand back all 10,080 raw rows — the bucketed read caps at <see cref="TrendBudget.Chart"/>'s
    /// single-series budget.</summary>
    [Fact]
    public async Task Cpu_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "cpu_utilization_stats", BudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);

            await BulkSeedCpuAsync(connection, BudgetServerId, BudgetServerName, start, end);

            var samples = await viewer.GetCpuUtilizationAsync(BudgetServerId, start, end);

            var budget = TrendBudget.Chart.AutoPoints;
            Assert.True(samples.Count > 0 && samples.Count <= budget, $"{samples.Count} rows over a budget of {budget}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "cpu_utilization_stats", BudgetServerId, cleanupCt));
        }
    }

    /// <summary>#4234 ruling item 3: when the window's budget covers every collection (three samples, five
    /// minutes apart, each its own bucket), every point is stamped at its own raw sample_time — seeded off
    /// the minute grid (:37 seconds) so a fix that floors to the date_bin grid line instead would lose the
    /// seconds and fail this.</summary>
    [Fact]
    public async Task Cpu_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU singleton-passthrough test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "cpu_utilization_stats", BudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            /* collection_time == sample_time (a UTC batch): the #1262 de-skew is a no-op, so the raw
               sample_time survives unchanged if every bucket the read returns is a true singleton. */
            await InsertCpuAsync(connection, BudgetServerId, BudgetServerName, t1, t1, sqlCpu: 10, otherCpu: 5);
            await InsertCpuAsync(connection, BudgetServerId, BudgetServerName, t2, t2, sqlCpu: 20, otherCpu: 6);
            await InsertCpuAsync(connection, BudgetServerId, BudgetServerName, t3, t3, sqlCpu: 30, otherCpu: 7);

            /* Explicit endUtc close to the fixed fixture window — the default (the wall clock) would
               otherwise size the bucket width off a multi-month gap and merge these 5-minute-apart
               collections into one bucket. */
            var samples = await viewer.GetCpuUtilizationAsync(BudgetServerId, t1.AddMinutes(-1), t3.AddMinutes(1));

            Assert.Equal(new[] { t1.Ticks, t2.Ticks, t3.Ticks }, samples.Select(s => s.SampleTime.Ticks));
            Assert.Equal(new[] { 10.0, 20.0, 30.0 }, samples.Select(s => s.SqlServerCpu));
            Assert.Equal(new[] { 5.0, 6.0, 7.0 }, samples.Select(s => s.OtherProcessCpu));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "cpu_utilization_stats", BudgetServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task TempDb_ReadsMbAsDouble_AndBigintSessionCount_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb-usage test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "tempdb_stats", TempDbServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* total_sessions deliberately exceeds int.MaxValue so a GetInt32 reader would throw —
               this is the read's load-bearing bigint check. */
            const long bigSessionCount = 5_000_000_000L;

            await InsertTempDbAsync(connection, t1,
                userMb: 100.25m, internalMb: 50.50m, versionMb: 10.75m,
                totalMb: 161.50m, unallocMb: 38.50m, totalSessions: 3, topSessionId: 55, topSessionMb: 12.00m);
            await InsertTempDbAsync(connection, t2,
                userMb: 200.00m, internalMb: 60.00m, versionMb: 20.00m,
                totalMb: 280.00m, unallocMb: 120.00m, totalSessions: bigSessionCount, topSessionId: 77, topSessionMb: 25.50m);

            var samples = await viewer.GetTempDbTrendAsync(TempDbServerId, t1.AddMinutes(-1));

            Assert.Equal(2, samples.Count);

            /* Ordered by collection_time; MB columns read as double. */
            Assert.Equal(t1.Ticks, samples[0].CollectionTime.Ticks);
            Assert.Equal(100.25, samples[0].UserObjectReservedMb, precision: 3);
            Assert.Equal(50.50, samples[0].InternalObjectReservedMb, precision: 3);
            Assert.Equal(10.75, samples[0].VersionStoreReservedMb, precision: 3);
            Assert.Equal(161.50, samples[0].TotalReservedMb, precision: 3);
            Assert.Equal(38.50, samples[0].UnallocatedMb, precision: 3);
            Assert.Equal(3L, samples[0].TotalSessionsUsingTempDb);

            /* The bigint session count round-trips through GetInt64. */
            Assert.Equal(t2.Ticks, samples[1].CollectionTime.Ticks);
            Assert.Equal(bigSessionCount, samples[1].TotalSessionsUsingTempDb);
            Assert.Equal(77, samples[1].TopSessionId);
            Assert.Equal(25.50, samples[1].TopSessionTempDbMb, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "tempdb_stats", TempDbServerId, cleanupCt));
        }
    }

    /// <summary>#4234 ruling item 3: at a short window the budget covers every collection (one per file
    /// here), so both reads return the raw points UNCHANGED, stamped at their own raw collection time
    /// rather than the date_bin grid — proven by the exact-tick equality below, which the always-floors
    /// pre-#4234 read also happened to satisfy only because it never bucketed at all.</summary>
    [Fact]
    public async Task TempDbFileIo_TempDbOnly_PerFileAverageLatency_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb-file-I/O test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "file_io_stats", FileIoServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));

            /* Two tempdb files plus a user-database file that must be excluded by the WHERE clause. */
            await InsertFileIoAsync(connection, t1, "tempdb", "tempdb_data",
                deltaReads: 10, deltaWrites: 4, deltaStallReadMs: 100, deltaStallWriteMs: 20);
            await InsertFileIoAsync(connection, t1, "tempdb", "tempdb_log",
                deltaReads: 0, deltaWrites: 8, deltaStallReadMs: 0, deltaStallWriteMs: 80);
            await InsertFileIoAsync(connection, t1, "StackOverflow2010", "so_data",
                deltaReads: 1000, deltaWrites: 1000, deltaStallReadMs: 999999, deltaStallWriteMs: 999999);

            var rows = await viewer.GetTempDbFileIoTrendAsync(FileIoServerId, t1.AddMinutes(-1), t1.AddMinutes(1));

            /* Only the two tempdb files survive the database_name = 'tempdb' filter. */
            Assert.Equal(2, rows.Count);
            /* #4234: one collection per file over the whole window — the budget covers it, so every
               point is stamped at its own raw collection_time, not a bucket_start grid line. */
            Assert.All(rows, r => Assert.Equal(t1.Ticks, r.CollectionTime.Ticks));
            /* Ordered by file_name. */
            Assert.Equal(new[] { "tempdb_data", "tempdb_log" }, rows.Select(r => r.FileName));

            /* tempdb_data: 100 stall / 10 reads = 10 ms read; 20 stall / 4 writes = 5 ms write. */
            Assert.Equal(10.0, rows[0].AvgReadLatencyMs, precision: 3);
            Assert.Equal(5.0, rows[0].AvgWriteLatencyMs, precision: 3);
            /* tempdb_log: 0 reads → CASE ELSE 0 read latency; 80 stall / 8 writes = 10 ms write. */
            Assert.Equal(0.0, rows[1].AvgReadLatencyMs, precision: 3);
            Assert.Equal(10.0, rows[1].AvgWriteLatencyMs, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "file_io_stats", FileIoServerId, cleanupCt));
        }
    }

    /// <summary>#4234 ruling items 2/6: a 7-day window at the real 1-minute file_io_stats cadence used to
    /// return every collection per file — now capped to <see cref="TrendBudget.Chart"/>'s point budget PER
    /// FILE, exactly like the File I/O tab's own per-file reads. Bulk-seeded server-side (generate_series)
    /// rather than one round trip per minute.</summary>
    [Fact]
    public async Task TempDbFileIo_SevenDayWindow_ReturnsAtMostBudgetTimesFileCount_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb-file-I/O budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "file_io_stats", FileIoBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            var files = new[] { "tempdb_data", "tempdb_data2", "tempdb_log", "tempdb_data3" };
            var baseId = CollectionIdGenerator.Next() * 1_000_000L;
            foreach (var file in files)
            {
                await BulkSeedTempDbFileIoAsync(connection, TestContext.Current.CancellationToken, baseId, FileIoBudgetServerId, start, end, file);
                baseId += 20_000;
            }

            var rows = await viewer.GetTempDbFileIoTrendAsync(FileIoBudgetServerId, start, end);
            var budget = TrendBudget.Chart.AutoPoints * files.Length;

            Assert.True(rows.Count > 0 && rows.Count <= budget, $"tempdb file I/O: {rows.Count} rows over a {files.Length}-file budget of {budget}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "file_io_stats", FileIoBudgetServerId, cleanupCt));
        }
    }

    private static async Task InsertCpuAsync(
        NpgsqlConnection connection, int serverId, string serverName,
        DateTime collectionTimeUtc, DateTime sampleTimeLocal, int sqlCpu, int? otherCpu)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        /* collection_id is a plain non-unique bigint in the Darling PG store (PgSchemaGenerator drops
           DuckDB's PK), so every row of a batch can share it — collection_time is the real batch key. */
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        /* sample_time is the monitored server's LOCAL wall clock as the collector stores it. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTimeLocal, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)otherCpu ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One collection (one ring-buffer sample) per minute from <paramref name="start"/> to
    /// <paramref name="end"/> inclusive, generated server-side — the real cadence a 7-day CPU window would
    /// see — without a per-row round trip. A UTC batch (sample_time == collection_time), so the #1262
    /// de-skew is a no-op throughout.</summary>
    private static async Task BulkSeedCpuAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT 1, g, $1, $2, g, 50, 10
FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertTempDbAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc,
        decimal userMb, decimal internalMb, decimal versionMb, decimal totalMb, decimal unallocMb,
        long totalSessions, int topSessionId, decimal topSessionMb)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name,
     user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
     total_reserved_mb, unallocated_mb, total_sessions_using_tempdb,
     top_session_id, top_session_tempdb_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TempDbServerId);
        command.Parameters.AddWithValue(TempDbServerName);
        command.Parameters.AddWithValue(userMb);
        command.Parameters.AddWithValue(internalMb);
        command.Parameters.AddWithValue(versionMb);
        command.Parameters.AddWithValue(totalMb);
        command.Parameters.AddWithValue(unallocMb);
        command.Parameters.AddWithValue(totalSessions);
        command.Parameters.AddWithValue(topSessionId);
        command.Parameters.AddWithValue(topSessionMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertFileIoAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string databaseName, string fileName,
        long deltaReads, long deltaWrites, long deltaStallReadMs, long deltaStallWriteMs)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, delta_reads, delta_writes,
     delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(FileIoServerId);
        command.Parameters.AddWithValue(FileIoServerName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(fileName);
        command.Parameters.AddWithValue(deltaReads);
        command.Parameters.AddWithValue(deltaWrites);
        command.Parameters.AddWithValue(deltaStallReadMs);
        command.Parameters.AddWithValue(deltaStallWriteMs);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One tempdb file_io_stats collection per minute from <paramref name="start"/> to
    /// <paramref name="end"/> inclusive, generated server-side — the real cadence a 7-day tempdb-file-I/O
    /// window would see — without a per-row round trip. Mirrors ViewerFileIoBlockingTests's
    /// BulkSeedFileIoAsync, database_name fixed to 'tempdb'.</summary>
    private static async Task BulkSeedTempDbFileIoAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long baseId, int serverId,
        DateTime start, DateTime end, string fileName)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, delta_reads, delta_writes,
     delta_stall_read_ms, delta_stall_write_ms)
SELECT $1 + row_number() OVER (), g, $2, $3, 'tempdb', $4, 10, 4, 100, 20
FROM generate_series($5::timestamp, $6::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(baseId);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(FileIoServerName);
        command.Parameters.AddWithValue(fileName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
