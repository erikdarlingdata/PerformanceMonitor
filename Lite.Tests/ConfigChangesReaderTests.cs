/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for the Configuration Changes tab's readers
/// (LocalDataService.ConfigChanges). There is NO change table — the pure diff + the change/snapshot records
/// live once in the Common <see cref="ConfigChangeDiff"/> (already unit-tested there and in the Darling
/// config-history tests); these prove the LITE glue end-to-end against a freshly initialized store: seed the
/// v_ snapshot views' base tables with two captures that encode a KNOWN change, read back through the reader,
/// and assert the diff surfaces the expected change rows with the derived columns, the pre-window baseline is
/// kept (upper-bound-only read), the window excludes out-of-window changes, the #1319 database filter, and the
/// row VM's server-time render. The scenarios mirror the Darling config-history tests exactly.
/// </summary>
/* Renders through ServerTimeHelper.FormatServerTime, which reads the process-wide offset + display mode —
   see the collection note on SystemEventsReaderTests, which MUTATES both. */
[Collection("server-time-helper")]
public sealed class ConfigChangesReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8888;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ConfigChangesReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    // ── Server config ──

    [Fact]
    public async Task ServerConfigChanges_DetectsValueMove_NewestFirst_WithDerivedColumns()
    {
        var service = new LocalDataService(_duckDb);

        var older = Truncate(DateTime.UtcNow.AddHours(-3));
        var newer = Truncate(DateTime.UtcNow.AddHours(-2));

        /* max degree of parallelism 0 -> 4 (both configured and in-use move together). An unchanged setting is
           also planted to prove only the moved one surfaces. */
        await SeedServerConfigAsync(older, "max degree of parallelism", 0, 0, isDynamic: true, isAdvanced: true);
        await SeedServerConfigAsync(newer, "max degree of parallelism", 4, 4, isDynamic: true, isAdvanced: true);
        await SeedServerConfigAsync(older, "cost threshold for parallelism", 5, 5, isDynamic: true, isAdvanced: true);
        await SeedServerConfigAsync(newer, "cost threshold for parallelism", 5, 5, isDynamic: true, isAdvanced: true);

        var change = Assert.Single(await service.GetServerConfigChangesAsync(ServerId));
        Assert.Equal("max degree of parallelism", change.ConfigurationName);
        Assert.Equal(0L, change.OldValueConfigured);
        Assert.Equal(4L, change.NewValueConfigured);
        Assert.Equal("Configured value changed from 0 to 4", change.ChangeDescription);
    }

    [Fact]
    public async Task ServerConfigChanges_RequiresRestart_DerivedFromDynamicAndValueMismatch()
    {
        var service = new LocalDataService(_duckDb);

        var older = Truncate(DateTime.UtcNow.AddHours(-3));
        var newer = Truncate(DateTime.UtcNow.AddHours(-2));

        /* Non-dynamic, new configured (16000) != new in-use (8000) => requires restart. */
        await SeedServerConfigAsync(older, "max server memory (MB)", 8000, 8000, isDynamic: false, isAdvanced: false);
        await SeedServerConfigAsync(newer, "max server memory (MB)", 16000, 8000, isDynamic: false, isAdvanced: false);

        var change = Assert.Single(await service.GetServerConfigChangesAsync(ServerId));
        Assert.Equal("Yes", change.RequiresRestartDisplay);
        Assert.Equal("No", change.DynamicDisplay);
        Assert.Equal("Configured value changed from 8000 to 16000", change.ChangeDescription);
    }

    [Fact]
    public async Task ServerConfigChanges_SingleSnapshot_YieldsNothing()
    {
        var service = new LocalDataService(_duckDb);

        await SeedServerConfigAsync(Truncate(DateTime.UtcNow.AddHours(-2)), "max server memory (MB)", 8000, 8000, true, true);

        Assert.Empty(await service.GetServerConfigChangesAsync(ServerId));
    }

    [Fact]
    public async Task ServerConfigChanges_PreWindowBaselineKept_ChangeOnFirstInWindowSnapshotIsDetected()
    {
        var service = new LocalDataService(_duckDb);

        /* Baseline BEFORE the 24h window (30h ago); the value moves to the in-window capture (1h ago). Because
           the read upper-bounds only, the pre-window baseline is kept and the change is still detected. */
        await SeedServerConfigAsync(Truncate(DateTime.UtcNow.AddHours(-30)), "max degree of parallelism", 0, 0, true, true);
        await SeedServerConfigAsync(Truncate(DateTime.UtcNow.AddHours(-1)), "max degree of parallelism", 8, 8, true, true);

        var change = Assert.Single(await service.GetServerConfigChangesAsync(ServerId, hoursBack: 24));
        Assert.Equal(0L, change.OldValueConfigured);
        Assert.Equal(8L, change.NewValueConfigured);
    }

    [Fact]
    public async Task ServerConfigChanges_WindowExcludesChangeOlderThanTheWindow()
    {
        var service = new LocalDataService(_duckDb);

        /* Both captures are OLDER than a 24h window (50h and 48h ago), so the 48h-ago change is excluded. */
        await SeedServerConfigAsync(Truncate(DateTime.UtcNow.AddHours(-50)), "max degree of parallelism", 0, 0, true, true);
        await SeedServerConfigAsync(Truncate(DateTime.UtcNow.AddHours(-48)), "max degree of parallelism", 8, 8, true, true);

        Assert.Empty(await service.GetServerConfigChangesAsync(ServerId, hoursBack: 24));
    }

    // ── Database config (wide-row unpivot) ──

    [Fact]
    public async Task DatabaseConfigChanges_UnpivotsWideRow_NamesTheChangedColumn_WithDescription()
    {
        var service = new LocalDataService(_duckDb);

        var older = Truncate(DateTime.UtcNow.AddHours(-3));
        var newer = Truncate(DateTime.UtcNow.AddHours(-2));

        await SeedDatabaseConfigRecoveryModelAsync(older, "StackOverflow", "FULL");
        await SeedDatabaseConfigRecoveryModelAsync(newer, "StackOverflow", "SIMPLE");

        var change = Assert.Single(await service.GetDatabaseConfigChangesAsync(ServerId));
        Assert.Equal("StackOverflow", change.DatabaseName);
        Assert.Equal("recovery_model", change.SettingName);
        Assert.Equal("FULL", change.OldValue);
        Assert.Equal("SIMPLE", change.NewValue);
        Assert.Equal("Changed from FULL to SIMPLE", change.ChangeDescription);
    }

    [Fact]
    public async Task DatabaseConfigChanges_DatabaseFilter_1319_FiltersInSql()
    {
        var service = new LocalDataService(_duckDb);

        var older = Truncate(DateTime.UtcNow.AddHours(-3));
        var newer = Truncate(DateTime.UtcNow.AddHours(-2));

        await SeedDatabaseConfigRecoveryModelAsync(older, "DbA", "FULL");
        await SeedDatabaseConfigRecoveryModelAsync(newer, "DbA", "SIMPLE");
        await SeedDatabaseConfigRecoveryModelAsync(older, "DbB", "FULL");
        await SeedDatabaseConfigRecoveryModelAsync(newer, "DbB", "SIMPLE");

        Assert.Equal(2, (await service.GetDatabaseConfigChangesAsync(ServerId)).Count);
        var filtered = await service.GetDatabaseConfigChangesAsync(ServerId, databaseNames: new[] { "DbA" });
        var change = Assert.Single(filtered);
        Assert.Equal("DbA", change.DatabaseName);
    }

    // ── Trace flags (set-diff) ──

    [Fact]
    public async Task TraceFlagChanges_DetectsEnableDisableModify()
    {
        var service = new LocalDataService(_duckDb);

        var t0 = Truncate(DateTime.UtcNow.AddHours(-4));
        var t1 = Truncate(DateTime.UtcNow.AddHours(-3));
        var t2 = Truncate(DateTime.UtcNow.AddHours(-2));

        /* T0: 3226 global on. */
        await SeedTraceFlagAsync(t0, 3226, status: true, isGlobal: true, isSession: false);
        /* T1: 3226 still on, 1117 appears (enabled). */
        await SeedTraceFlagAsync(t1, 3226, status: true, isGlobal: true, isSession: false);
        await SeedTraceFlagAsync(t1, 1117, status: true, isGlobal: true, isSession: false);
        /* T2: 1117 disappears (disabled), 3226 becomes session-scoped (modified). */
        await SeedTraceFlagAsync(t2, 3226, status: true, isGlobal: false, isSession: true);

        var changes = await service.GetTraceFlagChangesAsync(ServerId);
        Assert.Equal(3, changes.Count);

        var enabled = Assert.Single(changes, c => c.TraceFlag == 1117 && c.ChangeDescription == "Trace flag 1117 ENABLED");
        Assert.Equal("", enabled.PreviousStatusDisplay);
        Assert.Equal("ON", enabled.NewStatusDisplay);
        Assert.Equal("GLOBAL", enabled.Scope);

        Assert.Single(changes, c => c.TraceFlag == 1117 && c.ChangeDescription == "Trace flag 1117 DISABLED");

        var modified = Assert.Single(changes, c => c.TraceFlag == 3226 && c.ChangeDescription == "Trace flag 3226 scope changed");
        Assert.Equal("SESSION", modified.Scope);
        Assert.Equal("Yes", modified.SessionDisplay);
        Assert.Equal("No", modified.GlobalDisplay);
    }

    /* #5633 item 19: a trace_flags run that finds no flag stores no rows, so the last flag being turned off was invisible. A
       SUCCESSFUL run with rows_collected = 0 is now an empty snapshot; a failed run, or one whose count was not recorded, is not. */

    [Fact]
    public async Task TraceFlagChanges_FlagOnThenSuccessfulEmptyRun_ReportsTheFlagTurnedOff()
    {
        var service = new LocalDataService(_duckDb);
        var on = Truncate(DateTime.UtcNow.AddHours(-5));
        var empty = Truncate(DateTime.UtcNow.AddHours(-2));

        await SeedTraceFlagAsync(on, 3226, status: true, isGlobal: true, isSession: false);
        await SeedTraceFlagRunAsync(on, "SUCCESS", 1);
        await SeedTraceFlagRunAsync(empty, "SUCCESS", 0);

        var change = Assert.Single(await service.GetTraceFlagChangesAsync(ServerId));
        Assert.Equal(3226, change.TraceFlag);
        Assert.Equal("Trace flag 3226 DISABLED", change.ChangeDescription);
        Assert.Equal(empty, change.ChangeTime);
    }

    [Fact]
    public async Task TraceFlagChanges_FlagOnThenFailedOrUnrecordedRun_ReportsNoChange()
    {
        var service = new LocalDataService(_duckDb);
        var on = Truncate(DateTime.UtcNow.AddHours(-5));

        await SeedTraceFlagAsync(on, 3226, status: true, isGlobal: true, isSession: false);
        await SeedTraceFlagRunAsync(on, "SUCCESS", 1);
        await SeedTraceFlagRunAsync(Truncate(DateTime.UtcNow.AddHours(-3)), "ERROR", 0);
        await SeedTraceFlagRunAsync(Truncate(DateTime.UtcNow.AddHours(-2)), "PERMISSIONS", 0);
        await SeedTraceFlagRunAsync(Truncate(DateTime.UtcNow.AddHours(-1)), "SUCCESS", null);

        Assert.Empty(await service.GetTraceFlagChangesAsync(ServerId));
    }

    [Fact]
    public async Task TraceFlagChanges_SuccessfulEmptyRunThenFlagOn_ReportsTheFlagTurnedOn_EvenWhenTheEmptyRunPrecedesTheWindow()
    {
        var service = new LocalDataService(_duckDb);
        var empty = Truncate(DateTime.UtcNow.AddHours(-30));
        var on = Truncate(DateTime.UtcNow.AddHours(-3));

        await SeedTraceFlagRunAsync(empty, "SUCCESS", 0);
        await SeedTraceFlagAsync(on, 1117, status: true, isGlobal: true, isSession: false);
        await SeedTraceFlagRunAsync(on, "SUCCESS", 1);

        /* The default 24 h window starts after the empty run: the newest empty run BEFORE the window is the baseline. */
        var change = Assert.Single(await service.GetTraceFlagChangesAsync(ServerId));
        Assert.Equal("Trace flag 1117 ENABLED", change.ChangeDescription);
        Assert.Equal(on, change.ChangeTime);
    }

    // ── Row VM projection (server-time render, shared with the other grids) ──

    [Fact]
    public void ServerConfigChangeRow_ChangeTimeLocal_RendersThroughServerTimeHelper()
    {
        var utc = new DateTime(2026, 7, 12, 9, 30, 0, DateTimeKind.Utc);
        var expected = ServerTimeHelper.FormatServerTime(utc, "yyyy-MM-dd HH:mm:ss");

        var row = new ServerConfigChangeRow(new ConfigChangeDiff.ServerConfigChange(
            utc, "max degree of parallelism", 0, 4, 0, 4, true, true));

        Assert.Equal(expected, row.ChangeTimeLocal);
    }

    // ── Seeding helpers (raw INSERT via the shared initializer's connection) ──

    private async Task SeedServerConfigAsync(
        DateTime captureTimeUtc, string configurationName, long valueConfigured, long valueInUse, bool isDynamic, bool isAdvanced)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = captureTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = configurationName });
        cmd.Parameters.Add(new DuckDBParameter { Value = valueConfigured });
        cmd.Parameters.Add(new DuckDBParameter { Value = valueInUse });
        cmd.Parameters.Add(new DuckDBParameter { Value = isDynamic });
        cmd.Parameters.Add(new DuckDBParameter { Value = isAdvanced });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDatabaseConfigRecoveryModelAsync(DateTime captureTimeUtc, string databaseName, string recoveryModel)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        /* Only the prefix + database_name + recovery_model are set; the other 26 setting columns stay NULL in
           both captures, so the wide-row diff surfaces ONLY the recovery_model move. */
        cmd.CommandText = @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name, recovery_model)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = captureTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = recoveryModel });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedTraceFlagAsync(DateTime captureTimeUtc, int traceFlag, bool status, bool isGlobal, bool isSession)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = captureTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = traceFlag });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = isGlobal });
        cmd.Parameters.Add(new DuckDBParameter { Value = isSession });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedTraceFlagRunAsync(DateTime collectionTimeUtc, string status, long? rowsCollected)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES ($1, $2, $3, 'trace_flags', $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = rowsCollected is null ? DBNull.Value : rowsCollected.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
