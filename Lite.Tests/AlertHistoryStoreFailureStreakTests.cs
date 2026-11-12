/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The alert history store's catch arms write the exception once per streak of failures, per (server, operation): on
/// the first failure, and again on the first failure after the operation succeeded. The failures in between write the
/// message only. <c>AppLogger.Error</c> given an exception writes the stack and every inner exception after the line,
/// and the engine runs these reads and writes for every server on every sweep, so a damaged table would otherwise write
/// every server's stack on every sweep for as long as the fault lasts.
///
/// <para>The fault is the table the read needs being gone: it is renamed away, and renamed back for the recovery. The
/// read is the alert persistence gate's, which the High CPU and blocking wait checks load on every sweep. The method logs and
/// rethrows, and the engine's seed step catches it, so the failing reads here are awaited for their throw.</para>
///
/// <para>The line that carries the exception is the one AppLogger renders as <c>message | Type: Message</c>, written
/// in the same branch as the stack, so that suffix on the line naming this test's metric is the marker.</para>
///
/// <para>Collection <c>app-logger-statics</c>: the log assertions drain the process-wide buffer, which the other tests
/// that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class AlertHistoryStoreFailureStreakTests : IClassFixture<SharedDuckDbFixture>
{
    private const string Table = "config_alert_persistence_state";
    private const string HiddenTable = Table + "_hidden";
    private const int ServerId = 4242;

    private readonly DuckDbInitializer _duckDb;

    public AlertHistoryStoreFailureStreakTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ARepeatedReadFailure_WritesTheExceptionOnce_AndAgainAfterAReadSucceeds()
    {
        var store = new DuckDbAlertHistoryStore(_duckDb);
        var metric = "streak-" + Guid.NewGuid().ToString("N")[..8];

        AppLogger.DrainBufferedLines();
        await RenameAsync(Table, HiddenTable);
        try
        {
            /* Two failures in a row, each rethrown: two lines name the metric, and one of them carries the exception. */
            await Assert.ThrowsAnyAsync<Exception>(() => store.LoadAlertPersistenceAsync(ServerId, metric));
            await Assert.ThrowsAnyAsync<Exception>(() => store.LoadAlertPersistenceAsync(ServerId, metric));

            var firstStreak = FailureLines(AppLogger.DrainBufferedLines(), metric);
            Assert.Equal(2, firstStreak.Count);
            Assert.Single(firstStreak, CarriesTheException);

            /* The table is back: a read that finds no row is a read that succeeded, and writes no failure line. */
            await RenameAsync(HiddenTable, Table);
            Assert.Null(await store.LoadAlertPersistenceAsync(ServerId, metric));
            Assert.Empty(FailureLines(AppLogger.DrainBufferedLines(), metric));

            /* The first failure after that carries the exception again. */
            await RenameAsync(Table, HiddenTable);
            await Assert.ThrowsAnyAsync<Exception>(() => store.LoadAlertPersistenceAsync(ServerId, metric));

            var line = Assert.Single(FailureLines(AppLogger.DrainBufferedLines(), metric));
            Assert.True(CarriesTheException(line), line);
        }
        finally
        {
            await RenameAsync(HiddenTable, Table);
        }
    }

    private async Task RenameAsync(string from, string to)
    {
        using var writeLock = _duckDb.AcquireWriteLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"ALTER TABLE IF EXISTS {from} RENAME TO {to}";
        await command.ExecuteNonQueryAsync();
    }

    /* Only the arm's lines for this test's metric: the buffer is process-wide. */
    private static List<string> FailureLines(IEnumerable<string> log, string metric) =>
        log.Where(line => line.Contains("[Alerts]", StringComparison.Ordinal)
            && line.Contains($"Could not load the alert persistence gate ({metric}):", StringComparison.Ordinal)).ToList();

    private static bool CarriesTheException(string line) =>
        Regex.IsMatch(line, @" \| \w*Exception: ");
}
