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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7900: Lite's get_long_query_completions read picks its events by when they HAPPENED (<c>event_time</c>), not by when the
/// collector stored them (<c>collection_time</c>), as its blocked-process and deadlock reads do and as Darling's twin does. The
/// collector stores an event minutes after it happens, so on a narrow window placed on an incident a completion that happened
/// inside the window but was stored after its end is in the window, and one that happened before the window but was stored
/// inside it is not.
/// </summary>
public sealed class LongQueryEventTimeWindowTests : IDisposable
{
    private const int ServerId = 7900;
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    public LongQueryEventTimeWindowTests() => Directory.CreateDirectory(_tempDir);

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

    private static string Ts(DateTime value) => $"TIMESTAMP '{value:yyyy-MM-dd HH:mm:ss.ffffff}'";

    [Fact]
    public async Task TheMcpRead_WindowsOnTheEventsOwnTime_ALateStoredEventLists_AnEarlierEventStoredInsideDoesNot()
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        /* A one-hour window that ended two hours ago, so a row stored after its end is still in the past. */
        var now = DateTime.UtcNow;
        var end = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc).AddHours(-2);
        var start = end.AddHours(-1);

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            /* Happened five minutes before the window's end, stored three minutes after it: in the window. */
            await SeedAsync(connection, 1, eventTime: end.AddMinutes(-5), stored: end.AddMinutes(3), seconds: 9, text: "late_stored");
            /* Happened five minutes before the window began, stored two minutes after it began: not in the window. */
            await SeedAsync(connection, 2, eventTime: start.AddMinutes(-5), stored: start.AddMinutes(2), seconds: 8, text: "early_event");
            /* Happened and stored inside the window. */
            await SeedAsync(connection, 3, eventTime: start.AddMinutes(30), stored: start.AddMinutes(31), seconds: 7, text: "inside");
        }

        var service = new LocalDataService(initializer);
        var rows = await service.GetSlowestLongQueryCompletionsAsync(ServerId, 1, end, 10);

        Assert.Equal(new[] { "late_stored", "inside" }, rows.Select(r => r.StatementText).ToArray());
    }

    private static async Task SeedAsync(DuckDBConnection connection, int id, DateTime eventTime, DateTime stored, int seconds, string text)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, "
            + "event_type, database_name, duration_microseconds, statement_text) "
            + $"VALUES ({id}, {Ts(stored)}, {ServerId}, 'S', {Ts(eventTime)}, 'rpc_completed', 'db1', {seconds * 1_000_000L}, '{text}')";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
