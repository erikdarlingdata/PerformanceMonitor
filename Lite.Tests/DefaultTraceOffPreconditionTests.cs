/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9107: a SQL Server whose default trace is turned off (<c>default trace enabled</c> = 0) used to read default_trace_events as failing
/// with no permissions. These tests hold the collector's half (what its read records from the batch's tail) and the readers' half (what
/// the MCP read and the health row say about the newest run).
/// </summary>
public sealed class DefaultTraceOffCollectorTests
{
    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    private static CollectorContext NewContext() => new()
    {
        ServerId = 1,
        ServerName = "s",
        CollectionTime = DateTime.UtcNow,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
    };

    /// <summary>The batch's two result sets: an empty payload, then the tail the query returns (path, start, and the setting when given).</summary>
    private static System.Data.Common.DbDataReader Batch(string? path, object? setting, bool settingColumn = true)
    {
        var set = new DataSet();
        var payload = new DataTable("payload");
        var types = new[]
        {
            typeof(DateTime), typeof(string), typeof(int), typeof(int), typeof(string), typeof(int),
            typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(long),
            typeof(long), typeof(string), typeof(string), typeof(int), typeof(int), typeof(int),
            typeof(long), typeof(long), typeof(DateTime),
        };
        for (var i = 0; i < types.Length; i++)
        {
            payload.Columns.Add("c" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), types[i]);
        }

        var tail = new DataTable("tail");
        tail.Columns.Add("current_trace_path", typeof(string));
        tail.Columns.Add("current_trace_start", typeof(DateTime));
        if (settingColumn)
        {
            tail.Columns.Add("default_trace_enabled", typeof(int));
            tail.Rows.Add(
                (object?)path ?? DBNull.Value,
                path is null ? DBNull.Value : new DateTime(2026, 11, 1, 5, 0, 0),
                setting ?? DBNull.Value);
        }
        else
        {
            tail.Rows.Add((object?)path ?? DBNull.Value, path is null ? DBNull.Value : new DateTime(2026, 11, 1, 5, 0, 0));
        }

        set.Tables.Add(payload);
        set.Tables.Add(tail);
        return set.CreateDataReader();
    }

    private static async Task<string?> RunNoteAsync(System.Data.Common.DbDataReader reader)
    {
        var context = NewContext();
        await DefaultTraceEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        return CollectorMeasurementNote.Render(context.Measurements);
    }

    [Fact]
    public async Task NoRunningTrace_AndTheSettingIsOff_RecordsTheCount()
    {
        var note = await RunNoteAsync(Batch(path: null, setting: 0));

        Assert.True(DefaultTraceOffNote.TryParse(note));
    }

    [Fact]
    public async Task ARunningTrace_RecordsNothingEvenIfTheSettingColumnSaysOff()
    {
        /* The running trace decides: a row means there is a trace to read, whatever else the batch says. */
        Assert.Null(await RunNoteAsync(Batch(path: @"S:\MSSQL\Log\log_5.trc", setting: 1)));
        Assert.Null(await RunNoteAsync(Batch(path: @"S:\MSSQL\Log\log_5.trc", setting: 0)));
    }

    [Fact]
    public async Task NoRunningTrace_WithTheSettingOn_KeepsItsOldAnswer()
    {
        /* A login that truly cannot see the trace (the setting is 1, there is still no readable row) is not told to turn anything on. */
        Assert.Null(await RunNoteAsync(Batch(path: null, setting: 1)));
    }

    [Fact]
    public async Task NoRunningTrace_WithTheSettingUnknown_SaysNothing()
    {
        /* NULL (no such configuration row) and an older tail without the column are unknown, and unknown never reads as off. */
        Assert.Null(await RunNoteAsync(Batch(path: null, setting: null)));
        Assert.Null(await RunNoteAsync(Batch(path: null, setting: null, settingColumn: false)));
    }

    [Fact]
    public void ATraceOffRun_IsNotATrueNegative_ForTheHealthyEmptyAnswer()
    {
        var now = DateTime.UtcNow;

        /* A run whose note is only counts reads as a true negative ("its source returned no rows"); a trace that is off is not one. */
        Assert.Null(CollectorRuntimePrecondition.HealthyEmptyMessage(
            "srv", "default_trace_events", "default trace events", "SUCCESS", DefaultTraceOffNote.Label + "=1", now));
        Assert.NotNull(CollectorRuntimePrecondition.HealthyEmptyMessage(
            "srv", "default_trace_events", "default trace events", "SUCCESS", "plans_rendered=0", now));
    }

    [Fact]
    public void TheOutcomeMessage_NamesTheSettingAndTheStatement_OnlyForATraceOffSuccess()
    {
        var now = DateTime.UtcNow;

        var off = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "srv", "default_trace_events", "SUCCESS", DefaultTraceOffNote.Label + "=1", now);
        Assert.NotNull(off);
        Assert.Contains(DefaultTraceOffNote.SettingName, off, StringComparison.Ordinal);
        Assert.Contains(DefaultTraceOffNote.EnableStatement, off, StringComparison.Ordinal);

        /* A run that found the trace on (no note) is not a precondition, and a refused grant keeps its own answer, which never
           tells a login that was refused to turn the trace on. */
        Assert.Null(CollectorRuntimePrecondition.CollectionOutcomeMessage("srv", "default_trace_events", "SUCCESS", null, now));
        var denied = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "srv", "default_trace_events", "PERMISSIONS", "SQL Error #8189: You do not have permission to run 'SYS.TRACES'.", now);
        Assert.NotNull(denied);
        Assert.DoesNotContain(DefaultTraceOffNote.EnableStatement, denied, StringComparison.Ordinal);
    }
}

public sealed class DefaultTraceOffReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "LiteTraceOffSrv";
    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public DefaultTraceOffReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-traceoff-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>The JSON text with the encoder's escaped apostrophes put back, so a statement that holds quotes can be looked for in it.</summary>
    private static string Unescaped(string json) => json.Replace("\\u0027", "'", StringComparison.Ordinal);

    private static string StatusOf(string json) => JsonDocument.Parse(json).RootElement.GetProperty("status").GetString()!;

    private async Task SeedRunAsync(DateTime whenUtc, string status, string? note)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, 'default_trace_events', $4, 50, $5, $6, 0, 40, 10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(whenUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)note ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task AServerWithTheTraceOff_AnswersPrecondition_WithTheSetting_NotEmpty()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 6; i++)
        {
            await SeedRunAsync(now.AddMinutes(-i * 15 - 1), "SUCCESS", DefaultTraceOffNote.Label + "=1");
        }

        var answer = await McpDefaultTraceTools.GetDefaultTraceEvents(new LocalDataService(_duckDb), _serverManager, ServerName);

        Assert.Equal(CollectorRuntimePrecondition.StatusWord, StatusOf(answer));
        Assert.Contains(DefaultTraceOffNote.SettingName, Unescaped(answer), StringComparison.Ordinal);
        Assert.Contains(DefaultTraceOffNote.EnableStatement, Unescaped(answer), StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnceTheTraceIsBackOn_TheNextRunClearsTheAnswer()
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync(now.AddMinutes(-31), "SUCCESS", DefaultTraceOffNote.Label + "=1");
        await SeedRunAsync(now.AddMinutes(-1), "SUCCESS", null);

        var answer = await McpDefaultTraceTools.GetDefaultTraceEvents(new LocalDataService(_duckDb), _serverManager, ServerName);

        Assert.Equal("empty", StatusOf(answer));
    }

    [Fact]
    public async Task ADeniedLogin_KeepsItsPermissionAnswer_AndIsNeverToldToTurnTheTraceOn()
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync(now.AddMinutes(-1), "PERMISSIONS", "SQL Error #8189: You do not have permission to run 'SYS.TRACES'.");

        var answer = await McpDefaultTraceTools.GetDefaultTraceEvents(new LocalDataService(_duckDb), _serverManager, ServerName);

        Assert.Equal(CollectorRuntimePrecondition.StatusWord, StatusOf(answer));
        Assert.DoesNotContain(DefaultTraceOffNote.EnableStatement, Unescaped(answer), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheHealthRow_ForATraceOffServer_IsNotFailingAndNotNoPermissions_AndSaysWhatToDo()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 8; i++)
        {
            await SeedRunAsync(now.AddMinutes(-i * 30 - 2), "SUCCESS", DefaultTraceOffNote.Label + "=1");
        }

        var json = await McpHealthTools.GetCollectionHealth(new LocalDataService(_duckDb), _serverManager, ServerName);
        var row = JsonDocument.Parse(json).RootElement.GetProperty("collectors").EnumerateArray()
            .Single(c => c.GetProperty("collector").GetString() == "default_trace_events");

        var status = row.GetProperty("status").GetString();
        Assert.NotEqual(CollectorHealthClassifier.Failing, status);
        Assert.NotEqual(CollectorHealthClassifier.NoPermissions, status);

        /* The full row's note cell (the one every surface composes) says what to do. */
        var cell = CollectorHealthClassifier.FormatCollectionNote(
            DefaultTraceOffNote.Label + "=1", noteCount: 8, totalRuns: 8, collectorName: "default_trace_events");
        Assert.Contains(DefaultTraceOffNote.EnableStatement, cell, StringComparison.Ordinal);
    }
}
