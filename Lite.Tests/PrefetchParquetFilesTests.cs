/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9362: the main connection reads local Parquet files with <c>prefetch_all_parquet_files</c> on. DuckDB's default fetches one
/// column chunk at a time, which over the archive's 2,048-row row groups (#5381) is tens of thousands of small serial reads for one
/// wide scan, and that is where the 168 hour regressions and top queries reads spent their time. The first test proves DuckDB.NET
/// reads true through the main connection, which sets it right after the open (#9553: the win-arm64 library rejects it in the connection string). The second keeps the #5381 memory budget honest with the setting on: it reads an
/// archive whose plan XML column is as wide as real plans, through the main connection at its own memory limit.
/// </summary>
[Collection(QueryStoreMemoryPinCollection.Name)]
public sealed class PrefetchParquetFilesTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "LiteTests_9362_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _duckDb;

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task MainConnection_OpenedLikeLite_PrefetchesParquetFiles()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeAsync();

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT current_setting('prefetch_all_parquet_files')";
        Assert.True(Convert.ToBoolean(await cmd.ExecuteScalarAsync()));

        /* DuckDB's own default is off, so the assertion above reads the SET the main connection ran and not a changed default. */
        using var plain = new DuckDBConnection("DataSource=:memory:");
        await plain.OpenAsync();
        using var plainCmd = plain.CreateCommand();
        plainCmd.CommandText = "SELECT current_setting('prefetch_all_parquet_files')";
        Assert.False(Convert.ToBoolean(await plainCmd.ExecuteScalarAsync()));
    }

    /// <summary>The connection InitializeAsync itself holds (the sentinel) takes the setting, not only the ones CreateConnection hands out.</summary>
    [Fact]
    public async Task InitializeAsync_SentinelConnection_PrefetchesParquetFiles()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeAsync();

        Assert.Equal("true", _duckDb.ReadSentinelSettingForTests("prefetch_all_parquet_files"), ignoreCase: true);
    }

    /// <summary>The sentinel a test adopts from a copied file (the reopen path) takes the setting too.</summary>
    [Fact]
    public async Task AdoptedSentinelConnection_PrefetchesParquetFiles()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();

        Assert.Equal("true", _duckDb.ReadSentinelSettingForTests("prefetch_all_parquet_files"), ignoreCase: true);
    }

    /// <summary>
    /// #9553: the SET is a speed-up only. When DuckDB refuses it, the open still succeeds and one Warning line is logged, however
    /// many connections hit it.
    /// </summary>
    [Fact]
    public async Task SetFailure_DoesNotFailTheOpen_AndWarnsOnce()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeAsync();

        using var connection = new DuckDBConnection("DataSource=:memory:");
        await connection.OpenAsync();
        AppLogger.DrainBufferedLines();

        _duckDb.ApplyPrefetchSetting(connection, "SET prefetch_all_parquet_files_no_such_option = true");
        _duckDb.ApplyPrefetchSetting(connection, "SET prefetch_all_parquet_files_no_such_option = true");

        var lines = AppLogger.DrainBufferedLines().FindAll(l => l.Contains("[DuckDbInitializer]") && l.Contains("prefetch_all_parquet_files"));
        var line = Assert.Single(lines);
        Assert.Contains("[WARN ]", line);
        Assert.Contains("DuckDBException", line);

        using var check = connection.CreateCommand();
        check.CommandText = "SELECT 1";
        Assert.Equal(1, Convert.ToInt32(await check.ExecuteScalarAsync()));
    }

    private const int ArchiveDays = 3;
    private const int RowsPerDay = 2_048;
    private const int PlanChars = 80_000;

    /// <summary>
    /// An archive of query_stats rows carrying plan XML of a realistic size (80 KB a plan, one 2,048-row row group a day), read by
    /// the top queries read through the main connection at its resting memory limit and thread count, with the setting on. It must
    /// return the plans of the winners, not run out of memory.
    /// </summary>
    [Fact]
    public async Task TopQueriesByCpu_OverAnArchiveOfWidePlanXml_FitsTheMainConnectionsMemoryLimit()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();
        Directory.CreateDirectory(_duckDb.ArchivePath);

        var firstDay = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-ArchiveDays - 1), DateTimeKind.Unspecified);
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            for (var f = 0; f < ArchiveDays; f++)
            {
                var day = firstDay.AddDays(f);
                var dayText = day.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                var path = Path.Combine(_duckDb.ArchivePath, $"{day:yyyyMMdd}_2300_query_stats.parquet").Replace('\\', '/');
                /* A plan is repeated md5 text plus the row's own digest, so it does not collapse to one dictionary entry. */
                await ExecAsync(connection, $@"
COPY (
    SELECT ({(f + 1) * 10_000_000L}::BIGINT + i) AS collection_id,
           TIMESTAMP '{dayText}' + INTERVAL ((i % 20)) HOUR + INTERVAL ((i % 50)) MINUTE AS collection_time,
           9362::INTEGER AS server_id,
           'TestSrv' AS server_name,
           'db_' || (i % 5)::VARCHAR AS database_name,
           md5((i % 400)::VARCHAR) AS query_hash,
           '0x' || md5('h' || (i % 400)::VARCHAR) AS sql_handle,
           TIMESTAMP '{dayText}' + INTERVAL ((i % 20)) HOUR + INTERVAL ((i % 50)) MINUTE AS last_execution_time,
           100::BIGINT AS delta_execution_count,
           (1000 + i)::BIGINT AS delta_worker_time,
           (1000 + i)::BIGINT AS delta_elapsed_time,
           'SELECT ' || (i % 400)::VARCHAR || ' FROM dbo.t' AS query_text,
           '<ShowPlanXML>' || repeat(md5(i::VARCHAR || ':{dayText}'), {PlanChars / 32}) || '</ShowPlanXML>' AS query_plan_xml
    FROM range(0, {RowsPerDay}) t(i)
) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048)");
            }
        }

        await _duckDb.CreateArchiveViewsAsync();

        var from = firstDay;
        var to = firstDay.AddDays(ArchiveDays);
        var rows = await new LocalDataService(_duckDb).GetTopQueriesByCpuAsync(9362, hoursBack: 24 * ArchiveDays, top: 50, fromDate: from, toDate: to);

        Assert.Equal(50, rows.Count);
        Assert.All(rows, r => Assert.True((r.QueryPlan?.Length ?? 0) >= PlanChars, "each winner carries its full plan XML"));
    }
}
