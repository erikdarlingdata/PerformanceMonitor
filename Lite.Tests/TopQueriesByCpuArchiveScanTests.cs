using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6096: Top Queries by CPU named its module (the procedure a statement belongs to) with a window over EVERY
/// procedure_stats row the archive holds for the server, so the read grew with the archive and not with the window: a
/// user on 344 databases saw it take 120 s. The ranking now hands the module lookup only the sql_handles of the ranked
/// groups. Each test archives its rows through the real reset, so every row is read from Parquet.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class TopQueriesByCpuArchiveScanTests : IDisposable
{
    private const int ServerId = 1;
    private const int UnrelatedProcedures = 400;
    private const int SnapshotsEach = 3;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public TopQueriesByCpuArchiveScanTests()
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

    private async Task<LocalDataService> SeedAndArchiveAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            var now = DateTime.UtcNow;
            var ts = now.AddHours(-2).ToString("yyyy-MM-dd HH:mm:ss");
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, last_execution_time,
    delta_execution_count, delta_worker_time, delta_elapsed_time, query_text, sql_handle, sample_interval_seconds)
VALUES (1, TIMESTAMP '{ts}', {ServerId}, 'SRV', 'Db1', '0xAAAA', '0xBBBB', TIMESTAMP '{ts}', 5, 900000, 900000, 'SELECT 1', '0xMATCH', 60);

-- The procedure the ranked statement belongs to, and a few hundred the page never asks about.
INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, sample_interval_seconds)
SELECT 1000 + p * {SnapshotsEach} + s, TIMESTAMP '{ts}' - s * INTERVAL 1 MINUTE, {ServerId}, 'SRV', 'Db1', 'dbo', 'usp_Unrelated_' || p, '0xOTHER' || p, 60
FROM range({UnrelatedProcedures}) a(p), range({SnapshotsEach}) b(s);

INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, sample_interval_seconds)
SELECT 900000 + s, TIMESTAMP '{ts}' - s * INTERVAL 1 MINUTE, {ServerId}, 'SRV', 'Db1', 'dbo', 'usp_Match', '0xMATCH', 60
FROM range({SnapshotsEach}) b(s);";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        await new ArchiveService(initializer, _archiveDir).ArchiveAllAndResetAsync();
        return new LocalDataService(initializer);
    }

    [Fact]
    public async Task TheModuleStillComesFromTheArchivedProcedureRow()
    {
        var dataService = await SeedAndArchiveAsync();

        var rows = await dataService.GetTopQueriesByCpuAsync(ServerId, 24, 50);

        var row = Assert.Single(rows);
        Assert.Equal("usp_Match", row.ModuleObjectName);
        Assert.Equal("dbo", row.ModuleSchemaName);
    }

    [Fact]
    public async Task TheModuleLookupDoesNotRankEveryArchivedProcedureRow()
    {
        var dataService = await SeedAndArchiveAsync();
        var profilePath = Path.Combine(_tempDir, "profile.json");

        using (var connection = await dataService.OpenConnectionAsync())
        {
            await ExecuteAsync(connection, "PRAGMA enable_profiling = 'json'");
            await ExecuteAsync(connection, $"PRAGMA profiling_output = '{profilePath.Replace('\\', '/')}'");

            using var command = connection.CreateCommand();
            command.CommandText = LocalDataService.TopQueriesByCpuSql("", 200);
            var now = DateTime.UtcNow;
            command.Parameters.Add(new DuckDBParameter { Value = ServerId });
            command.Parameters.Add(new DuckDBParameter { Value = now.AddHours(-24) });
            command.Parameters.Add(new DuckDBParameter { Value = now });
            command.Parameters.Add(new DuckDBParameter { Value = 50 });
            command.Parameters.Add(new DuckDBParameter { Value = 0 });
            command.Parameters.Add(new DuckDBParameter { Value = 0 });
            using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
            {
                while (await reader.ReadAsync(TestContext.Current.CancellationToken))
                {
                }
            }

            await ExecuteAsync(connection, "PRAGMA disable_profiling");
        }

        using var profile = JsonDocument.Parse(await File.ReadAllTextAsync(profilePath, TestContext.Current.CancellationToken));
        var windowRows = new List<long>();
        CollectWindowCardinalities(profile.RootElement, windowRows);

        Assert.NotEmpty(windowRows);
        /* The module window used to see every archived procedure_stats row for the server (400 handles x 3 snapshots + the
           matching 3); it now sees the matching handle's rows only. */
        Assert.True(windowRows.Max() < UnrelatedProcedures,
            $"a window operator ranked {windowRows.Max()} rows, so the read still ranks the archive and not the page's handles");
    }

    private static async Task ExecuteAsync(LockedConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static void CollectWindowCardinalities(JsonElement node, List<long> into)
    {
        if (node.ValueKind == JsonValueKind.Object)
        {
            if (node.TryGetProperty("operator_type", out var type)
                && type.GetString() is { } name
                && name.Contains("WINDOW", StringComparison.OrdinalIgnoreCase)
                && node.TryGetProperty("operator_cardinality", out var cardinality))
            {
                into.Add(cardinality.GetInt64());
            }

            foreach (var property in node.EnumerateObject())
                CollectWindowCardinalities(property.Value, into);
        }
        else if (node.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in node.EnumerateArray())
                CollectWindowCardinalities(item, into);
        }
    }
}
