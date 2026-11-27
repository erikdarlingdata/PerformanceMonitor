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
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9065: the MCP <c>database_name</c> lookup (<see cref="LocalDataService.DatabaseNameCandidatesAsync"/>) reads the hot
/// <c>database_states</c> table first and the Parquet archive only for what the hot table does not hold. The archive-and-reset
/// moves every hot row to Parquet, so right after it the newest list and every earlier spelling are in the archive alone, and
/// once a new list is collected the newest list is hot again while the earlier spellings stay in the archive.
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both process-wide, so this class
   joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class McpDatabaseNameAfterArchiveTests : IDisposable
{
    private const int ServerId = 4891;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly DateTime _now = DateTime.UtcNow;
    private long _nextId = 1;

    public McpDatabaseNameAfterArchiveTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        CollectionResetGate.ResetForTests();
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch (IOException)
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>One row of the database list taken <paramref name="age"/> ago.</summary>
    private string Listed(TimeSpan age, string database) =>
        "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) "
        + $"VALUES ({_nextId++}, TIMESTAMP '{_now - age:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', 5, 'ONLINE', false)";

    private async Task ExecAsync(params string[] statements)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task<long> CountAsync(string source)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {source} WHERE server_id = {ServerId}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private async Task<string[]> CandidatesAsync(string databaseName) =>
        (await new LocalDataService(_duckDb).DatabaseNameCandidatesAsync(ServerId, databaseName, _now, TestContext.Current.CancellationToken))!.Names
        .OrderBy(name => name, StringComparer.Ordinal)
        .ToArray();

    /// <summary>When the lookup says the newest list was taken.</summary>
    private async Task<DateTime?> ListTakenAsync(string databaseName) =>
        (await new LocalDataService(_duckDb).DatabaseNameCandidatesAsync(ServerId, databaseName, _now, TestContext.Current.CancellationToken))!.ListTakenUtc;

    /// <summary>The time <see cref="Listed"/> stores for a list taken <paramref name="age"/> ago: whole seconds.</summary>
    private DateTime Stamped(TimeSpan age)
    {
        var at = _now - age;
        return new DateTime(at.Year, at.Month, at.Day, at.Hour, at.Minute, at.Second);
    }

    [Fact]
    public async Task AfterArchiveReset_TheListAndTheEarlierSpellingComeFromTheArchive_AndANewHotListIsTheNewestOne()
    {
        /* sales three hours ago, renamed to Sales by an hour ago. */
        await _duckDb.InitializeFromTemplateAsync();
        await ExecAsync(
            Listed(TimeSpan.FromHours(3), "master"), Listed(TimeSpan.FromHours(3), "sales"),
            Listed(TimeSpan.FromHours(1), "master"), Listed(TimeSpan.FromHours(1), "Sales"));
        await new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

        Assert.Equal(0, await CountAsync("database_states"));
        Assert.Equal(4, await CountAsync("v_database_states"));

        /* The hot table is empty: the newest list and the earlier spelling are both read from the archive. */
        Assert.Equal(new[] { "Sales" }, await CandidatesAsync("SALES"));
        Assert.Equal(new[] { "Sales" }, await CandidatesAsync("Sales"));
        Assert.Equal(new[] { "Sales", "sales" }, await CandidatesAsync("sales"));
        Assert.Empty(await CandidatesAsync("Nowhere"));
        Assert.Equal(Stamped(TimeSpan.FromHours(1)), await ListTakenAsync("SALES"));

        /* The next collection writes a hot list, renamed once more: it is the newest list, and both earlier spellings are still
           found in the archive. */
        await ExecAsync(Listed(TimeSpan.FromMinutes(10), "master"), Listed(TimeSpan.FromMinutes(10), "SALES"));

        Assert.Equal(new[] { "SALES" }, await CandidatesAsync("SALES"));
        Assert.Equal(new[] { "SALES" }, await CandidatesAsync("sAlEs"));
        Assert.Equal(new[] { "SALES", "Sales" }, await CandidatesAsync("Sales"));
        Assert.Equal(new[] { "SALES", "sales" }, await CandidatesAsync("sales"));
        Assert.Equal(Stamped(TimeSpan.FromMinutes(10)), await ListTakenAsync("sAlEs"));
    }
}
