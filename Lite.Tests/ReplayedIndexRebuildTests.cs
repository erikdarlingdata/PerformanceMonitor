/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// duckdb#26106: rows that WAL replay restores when a file opens after an unclean close are lost from the ART indexes
/// on their table by the next automatic or shutdown checkpoint, and a later DELETE over them fails with a FATAL error
/// that invalidates the database. Lite checkpoints right after every open and then rebuilds its explicit indexes.
///
/// <para>Every store here is real. The unclean close is DuckDB's own: the rows are committed to the WAL and the last
/// connection closes with the checkpoint on shutdown turned off, which leaves the file as a killed process would.</para>
/// </summary>
public class ReplayedIndexRebuildTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public ReplayedIndexRebuildTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
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

    /* 500 collection_log rows for one server: collection_log carries the explicit (server_id, collection_time)
       index the archive delete runs over. */
    private const string InsertRows = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 7, 'pin', 'wait_stats', TIMESTAMP '2026-09-01' + to_seconds(i), 'SUCCESS'
FROM range(500) AS r(i)";

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static long Count(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    /// <summary>A Lite store whose last rows are committed to the WAL only, closed the way a crash closes it.</summary>
    private async System.Threading.Tasks.Task<string> StoreClosedWithRowsOnlyInTheWalAsync()
    {
        var lite = new DuckDbInitializer(_dbPath);
        await lite.InitializeAsync();
        var connectionString = lite.ConnectionString;
        lite.Dispose();

        using (var connection = new DuckDBConnection(connectionString))
        {
            connection.Open();
            Execute(connection, "CHECKPOINT");
            Execute(connection, InsertRows);
            Execute(connection, "PRAGMA disable_checkpoint_on_shutdown");
        }

        Assert.True(new FileInfo(_dbPath + ".wal").Length > 0, "the rows must be in the WAL, or the open replays nothing");
        return connectionString;
    }

    [Fact]
    public async System.Threading.Tasks.Task RowsReplayedFromTheWal_StayInTheirIndex_ThroughLitesOpenAndAShutdown()
    {
        var connectionString = await StoreClosedWithRowsOnlyInTheWalAsync();

        /* Lite opens the file, which replays the WAL, and closes it normally: the shutdown checkpoint is the one
           that loses the replayed rows' index entries. */
        var lite = new DuckDbInitializer(_dbPath);
        await lite.InitializeAsync();
        lite.Dispose();

        using var connection = new DuckDBConnection(connectionString);
        connection.Open();
        Assert.Equal(1, Count(connection,
            "SELECT count(*) FROM collection_log WHERE server_id = 7 AND collection_time = TIMESTAMP '2026-09-01 00:00:07'"));

        /* The archive delete's shape. Over rows the index has lost, DuckDB fails it with a FATAL error. */
        Execute(connection, "DELETE FROM collection_log WHERE server_id = 7");
        Assert.Equal(0, Count(connection, "SELECT count(*) FROM collection_log WHERE server_id = 7"));
    }

    [Fact]
    public async System.Threading.Tasks.Task AnIndexAnEarlierShutdownDamaged_IsRepairedWhenLiteOpens()
    {
        var connectionString = await StoreClosedWithRowsOnlyInTheWalAsync();

        /* Something other than this build opens the file and closes it: the replay and the shutdown checkpoint
           happen before Lite sees the file, so its index has already lost the rows, as on a store an older build
           opened after a crash. */
        using (var other = new DuckDBConnection(connectionString))
        {
            other.Open();
        }

        var lite = new DuckDbInitializer(_dbPath);
        await lite.InitializeAsync();
        try
        {
            using var connection = lite.CreateConnection();
            connection.Open();
            Execute(connection, "DELETE FROM collection_log WHERE server_id = 7");
            Assert.Equal(0, Count(connection, "SELECT count(*) FROM collection_log WHERE server_id = 7"));
        }
        finally
        {
            lite.Dispose();
        }
    }

    [Fact]
    public async System.Threading.Tasks.Task TheRebuild_RunsAtEveryOpen_AndLeavesEveryDefinitionAsItWas()
    {
        var lite = new DuckDbInitializer(_dbPath);
        await lite.InitializeAsync();
        var connectionString = lite.ConnectionString;
        lite.Dispose();

        var before = Definitions(connectionString);
        Assert.True(before.Count >= 10, $"only {before.Count} explicit indexes: the schema did not build");

        for (var open = 1; open <= 2; open++)
        {
            var log = new CapturingLogger();
            var reopened = new DuckDbInitializer(_dbPath, log);
            await reopened.InitializeAsync();
            reopened.Dispose();

            Assert.Contains(log.Entries, e => e.Level == LogLevel.Information
                && e.Message.StartsWith($"Rebuilt {before.Count} indexes on ", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Error);
            Assert.Equal(before, Definitions(connectionString));
        }
    }

    [Fact]
    public void TheCheckpoint_GatesTheRebuild_AndNoIndexIsDroppedAndCreatedInOneTransaction()
    {
        /* Two rules no behaviour pin can show without killing the test process. A failed CHECKPOINT leaves every
           index in place. And a DROP and its CREATE never share a transaction: for an index loaded from the file,
           with Lite's checkpoint_threshold, COMMIT then ends the process with an access violation. */
        var repair = Source("Lite", "Database", "DuckDbInitializer.ReplayedIndexes.cs");
        var body = repair[repair.IndexOf("private async Task CheckpointAndRebuildIndexesAsync(", StringComparison.Ordinal)..];
        var checkpoint = body.IndexOf("await ExecuteNonQueryAsync(connection, \"CHECKPOINT\");", StringComparison.Ordinal);
        var skip = body.IndexOf("return;", StringComparison.Ordinal);
        var drop = body.IndexOf("DROP INDEX", StringComparison.Ordinal);
        Assert.True(checkpoint > 0, "the repair no longer runs an explicit CHECKPOINT");
        Assert.True(skip > checkpoint && drop > skip,
            "the rebuild must come after the CHECKPOINT, and a failed CHECKPOINT must return before it");
        Assert.False(body.Contains("BeginTransaction", StringComparison.Ordinal),
            "an index dropped and created in one transaction ends the process at COMMIT on a normal open");

        var initializer = Source("Lite", "Database", "DuckDbInitializer.cs");
        var core = initializer[initializer.IndexOf("private async Task InitializeCoreAsync()", StringComparison.Ordinal)..];
        var call = core.IndexOf("await CheckpointAndRebuildIndexesAsync(connection);", StringComparison.Ordinal);
        Assert.True(call > 0, "InitializeCoreAsync no longer repairs the indexes at open");
        Assert.True(call < core.IndexOf("CREATE TABLE IF NOT EXISTS schema_version", StringComparison.Ordinal)
            && call < core.IndexOf("RunMigrationsAsync(", StringComparison.Ordinal),
            "the repair must run before anything at open can delete or update an indexed row");
        Assert.True(call < core.IndexOf("Schema.GetAllIndexStatements()", StringComparison.Ordinal),
            "the schema's index statements must run after the repair, to put back an index it dropped and could not create");
    }

    private static List<string> Definitions(string connectionString)
    {
        var definitions = new List<string>();
        using var connection = new DuckDBConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT table_name, index_name, sql
FROM duckdb_indexes()
WHERE NOT is_primary
AND   sql IS NOT NULL
ORDER BY table_name, index_name";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            definitions.Add($"{reader.GetString(0)} | {reader.GetString(1)} | {reader.GetString(2)}");
        }

        return definitions;
    }

    private static string Source(params string[] parts)
    {
        var path = Path.Combine(RepoRoot(), Path.Combine(parts));
        return File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new InvalidOperationException("Could not find the repository root above " + thisFile);
    }

    private sealed class CapturingLogger : ILogger<DuckDbInitializer>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Entries)
            {
                Entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
