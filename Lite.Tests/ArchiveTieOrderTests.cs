/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5381: an archive file written in 2,048-row row groups is read by several threads, so a
/// <c>ROW_NUMBER() ... ORDER BY collection_time DESC</c> that ties (same statement, different sql_handle, one
/// collection_time) no longer picks the same row each run. The analysis reads break the tie with collection_id.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class ArchiveTieOrderTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveTieOrderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    private const int Keys = 2000;
    private const int RowsPerKey = 6;
    private const int OffenderKeys = 10;

    /// <summary>
    /// 2,000 statements with six rows each, every row at ONE collection_time (12,000 rows, about six row groups).
    /// For ten of the statements only the row with the highest collection_id has a wide min/max worker-time spread;
    /// the other five rows of that statement, and every row of every other statement, are flat. So which row
    /// "rn = 1" picks decides whether the statement is a parameter-sensitivity offender.
    /// </summary>
    private async Task<DuckDbInitializer> SeedArchivedTiesAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeAsync();

        var at = TestDataSeeder.TestPeriodStart.AddMinutes(30);
        var created = TestDataSeeder.TestPeriodStart.AddDays(-3);
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var seed = connection.CreateCommand();
            seed.CommandText = $@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
SELECT
    (r * {Keys} + k + 1)::BIGINT,
    TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}',
    {TestDataSeeder.TestServerId}, 'TIE', 'TieDb', 'QH' || k, 'PH' || k,
    TIMESTAMP '{created:yyyy-MM-dd HH:mm:ss}', 100,
    CASE WHEN r = {RowsPerKey - 1} AND k < {OffenderKeys} THEN 10000 ELSE 300000 END,
    CASE WHEN r = {RowsPerKey - 1} AND k < {OffenderKeys} THEN 1000000 ELSE 300000 END,
    1024, 1024, 0, 0, 'SELECT 1', 1
FROM range({RowsPerKey}) AS a(r), range({Keys}) AS b(k)";
            await seed.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await new ArchiveService(initializer, _archiveDir).ArchiveAllAndResetAsync();
        return initializer;
    }

    private static string FormatFact(Fact fact)
    {
        var offenders = fact.Metadata["offender_count"];
        var ratio = fact.Metadata["worst_ratio"];
        return $"offenders={offenders} ratio={ratio}";
    }

    [Fact]
    public async Task ParameterSensitivity_PicksTheSameRowAcrossRuns_ForTiedCollectionTimesInAMultiGroupArchiveFile()
    {
        var initializer = await SeedArchivedTiesAsync();

        var file = Directory.GetFiles(_archiveDir, "*_query_stats.parquet").Single().Replace('\\', '/');
        using (var connection = new DuckDBConnection("Data Source=:memory:"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{file}')";
            var groups = Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            Assert.True(groups > 1, $"the archive file is {groups} row group(s); the tie order only varies across several");
        }

        var collector = new DuckDbFactCollector(initializer);
        var context = TestDataSeeder.CreateTestContext();
        var seen = new HashSet<string>();
        for (var run = 0; run < 20; run++)
        {
            var facts = await collector.CollectFactsAsync(context);
            var fact = facts.SingleOrDefault(f => f.Key == "PARAMETER_SENSITIVITY");
            seen.Add(fact is null ? "none" : FormatFact(fact));
        }

        /* collection_id DESC makes the newest-inserted row of a tie the one rn = 1 picks, and that row is the wide
           one for each of the ten statements. */
        Assert.Equal([$"offenders={OffenderKeys} ratio=100"], seen.ToArray());
    }
}
