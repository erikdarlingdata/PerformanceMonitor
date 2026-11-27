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
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Long-Running Query alert's DuckDB read over rows with NULL columns (#9081). A request that is running and not waiting is stored
/// with a NULL wait_type, and a session can carry no program name, login, database or statement text. No exclusion treats a NULL as a
/// match, so those rows stay in the read.
/// </summary>
public sealed class LongRunningQueryNullWaitReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 9081_1;
    private readonly DuckDbInitializer _duckDb;

    public LongRunningQueryNullWaitReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>One request row 15 minutes into its run. A null argument is stored as NULL.</summary>
    private async Task InsertRequestAsync(
        DateTime collected, int sessionId, string? waitType,
        string? queryText = "SELECT COUNT(*) FROM Posts", string? database = "StackOverflow",
        string? program = "HammerDB", string? login = "app_user")
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, total_elapsed_time_ms, cpu_time_ms, reads, writes, wait_type, program_name, login_name)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6, 'running', $7, 5, 5, 5, $8, $9, $10)";
        foreach (var value in new object?[] { (long)sessionId, collected, ServerId, sessionId, database, queryText, 900_000L + sessionId, waitType, program, login })
        {
            command.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime OneMinuteAgo() => DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);

    /// <summary>
    /// Each wait-type opt-out drops the waits it names and keeps the request with no current wait, so that request is returned with
    /// every opt-out on and with each one on alone. The rows in a named wait, and the sp_server_diagnostics statement with no wait,
    /// show each opt-out still drops what it names.
    /// </summary>
    [Fact]
    public async Task TheRead_KeepsARequestWithNoCurrentWait_UnderEveryWaitTypeOptOut()
    {
        var collected = OneMinuteAgo();
        await InsertRequestAsync(collected, 91, waitType: null);
        await InsertRequestAsync(collected, 92, "SP_SERVER_DIAGNOSTICS_SLEEP");
        await InsertRequestAsync(collected, 93, "WAITFOR");
        await InsertRequestAsync(collected, 94, "BACKUPTHREAD");
        await InsertRequestAsync(collected, 95, "XE_LIVE_TARGET_TVF");
        await InsertRequestAsync(collected, 96, waitType: null, queryText: "EXEC sys.sp_server_diagnostics 5");

        var service = new LocalDataService(_duckDb);

        async Task<int[]> ReadAsync(bool diagnostics, bool waitFor, bool backups, bool misc)
        {
            var read = await service.GetLongRunningQueriesAsync(
                ServerId, thresholdMinutes: 5, maxResults: 10,
                excludeSpServerDiagnostics: diagnostics, excludeWaitFor: waitFor, excludeBackups: backups,
                excludeMiscWaits: misc, excludeCdc: true);
            return read.Sessions.Select(q => q.SessionId).OrderBy(id => id).ToArray();
        }

        Assert.Equal(new[] { 91 }, await ReadAsync(diagnostics: true, waitFor: true, backups: true, misc: true));
        Assert.Equal(new[] { 91, 93, 94, 95 }, await ReadAsync(diagnostics: true, waitFor: false, backups: false, misc: false));
        Assert.Equal(new[] { 91, 92, 94, 95, 96 }, await ReadAsync(diagnostics: false, waitFor: true, backups: false, misc: false));
        Assert.Equal(new[] { 91, 92, 93, 95, 96 }, await ReadAsync(diagnostics: false, waitFor: false, backups: true, misc: false));
        Assert.Equal(new[] { 91, 92, 93, 94, 96 }, await ReadAsync(diagnostics: false, waitFor: false, backups: false, misc: true));
    }

    /// <summary>
    /// The program-prefix, login and database exclusions, the CDC opt-out, the monitor-session filter and the statement-text half of
    /// the sp_server_diagnostics opt-out each keep a row whose column is NULL. The three named rows show each exclusion is on and counted.
    /// </summary>
    [Fact]
    public async Task TheRead_KeepsARowWithNoProgramLoginDatabaseOrText_UnderEveryExclusion()
    {
        var collected = OneMinuteAgo();
        await InsertRequestAsync(collected, 97, "CXPACKET", queryText: null, database: null, program: null, login: null);
        await InsertRequestAsync(collected, 98, "CXPACKET", database: "ExcludedDb");
        await InsertRequestAsync(collected, 99, "CXPACKET", database: "Other", program: "SQLAgent - TSQL JobStep (Job 0x01 : Step 1)");
        await InsertRequestAsync(collected, 100, "CXPACKET", database: "Other", login: @"NT AUTHORITY\SYSTEM");

        var read = await new LocalDataService(_duckDb).GetLongRunningQueriesAsync(
            ServerId, thresholdMinutes: 5, maxResults: 10,
            exclusions: LongRunningQueryExclusions.Defaults, excludedDatabases: new[] { "excludeddb" });

        Assert.Equal(97, Assert.Single(read.Sessions).SessionId);
        Assert.Equal(1, read.ExcludedByProgramPrefix);
        Assert.Equal(1, read.ExcludedByLogin);
        Assert.Equal(1, read.ExcludedByDatabase);
    }
}
