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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7550: Lite's Configuration grids and Perfmon Counters list names in the order DuckDB sorts them, which is by byte,
/// so every capitalized name came before every lower-case one. Each read seeds names whose byte order and case-blind
/// order differ and asserts the case-blind one, with the instance as the next key where rows share a counter.
/// </summary>
public sealed class NameOrderWithoutCaseTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -975501;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private DuckDBConnection? _seedConn;
    private long _nextId = -975000;

    public NameOrderWithoutCaseTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task ConfigurationReads_OrderNamesWithoutCase()
    {
        var at = DateTime.UtcNow.AddMinutes(-5);
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

        foreach (var name in new[] { "ADR Cleaner", "access check cache", "Ad Hoc Distributed Queries" })
        {
            await ExecAsync(
                @"INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", _nextId--, at, ServerId, "NameOrderServer", name, 1L, 1L, true, false);
        }

        foreach (var name in new[] { "Zeta", "alpha", "Beta" })
        {
            await ExecAsync(
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level)
VALUES ($1, $2, $3, $4, $5, $6, $7)", _nextId--, at, ServerId, "NameOrderServer", name, "ONLINE", 160);
            await ExecAsync(
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state)
VALUES ($1, $2, $3, $4, $5, $6, $7)", _nextId--, at, ServerId, "NameOrderServer", name, "READ_WRITE", "READ_WRITE");
        }

        foreach (var (db, setting) in new[]
                 {
                     ("Zeta", "MAXDOP"), ("alpha", "Legacy_Cardinality_Estimation"), ("alpha", "ACCELERATED_PLAN_FORCING"),
                     ("alpha", "batch_mode_adaptive_joins"), ("Beta", "MAXDOP"),
                 })
        {
            await ExecAsync(
                @"INSERT INTO database_scoped_config (config_id, capture_time, server_id, server_name, database_name, configuration_name, value, value_for_secondary)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", _nextId--, at, ServerId, "NameOrderServer", db, setting, "0", "0");
        }

        string[] databasesInOrder = ["alpha", "Beta", "Zeta"];
        Assert.Equal(["access check cache", "Ad Hoc Distributed Queries", "ADR Cleaner"],
            (await _dataService.GetLatestServerConfigAsync(ServerId)).Select(r => r.ConfigurationName));
        Assert.Equal(databasesInOrder, (await _dataService.GetLatestDatabaseConfigAsync(ServerId)).Select(r => r.DatabaseName));
        Assert.Equal(databasesInOrder, (await _dataService.GetLatestQueryStoreHealthAsync(ServerId)).Select(r => r.DatabaseName));
        Assert.Equal(
            [
                "alpha/ACCELERATED_PLAN_FORCING", "alpha/batch_mode_adaptive_joins", "alpha/Legacy_Cardinality_Estimation",
                "Beta/MAXDOP", "Zeta/MAXDOP",
            ],
            (await _dataService.GetLatestDatabaseScopedConfigAsync(ServerId)).Select(r => r.DatabaseName + "/" + r.ConfigurationName));
    }

    /// <summary>#7689: the Databases picker ended in a plain ORDER BY, so Alpha, Beta and Gamma came before delta and epsilon.</summary>
    [Fact]
    public async Task DatabasesPicker_OrdersNamesWithoutCase()
    {
        var at = DateTime.UtcNow.AddMinutes(-5);
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

        foreach (var name in new[] { "Gamma", "delta", "Alpha", "epsilon", "Beta" })
        {
            await ExecAsync(
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level)
VALUES ($1, $2, $3, $4, $5, $6, $7)", _nextId--, at, ServerId, "NameOrderServer", name, "ONLINE", 160);
        }

        Assert.Equal(["Alpha", "Beta", "delta", "epsilon", "Gamma"], await _dataService.GetCollectedDatabaseNamesAsync(ServerId));
    }

    [Fact]
    public async Task PerfmonCounters_OrderNamesWithoutCase_ThenByInstance()
    {
        var at = DateTime.UtcNow.AddMinutes(-5);
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

        /* "Active requests" is stored internal first: the instance is the second key, so default still leads. */
        foreach (var (counter, instance) in new[]
                 {
                     ("Active parallel threads", "default"), ("Active requests", "internal"), ("Active Temp Tables", "default"),
                     ("Active requests", "default"),
                 })
        {
            await ExecAsync(
                @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", _nextId--, at, ServerId, "NameOrderServer", "SQLServer:Workload Group Stats", counter, instance, 1L, 0L, 60, 65792);
        }

        Assert.Equal(
            ["Active parallel threads/default", "Active requests/default", "Active requests/internal", "Active Temp Tables/default"],
            (await _dataService.GetLatestPerfmonStatsAsync(ServerId)).Select(r => r.CounterName + "/" + r.InstanceName));
    }

    [Fact]
    public async Task PerfmonCounterPicker_ListsNamesWithoutCase()
    {
        var at = DateTime.UtcNow.AddMinutes(-5);
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

        /* Byte order would put Alpha, Beta, Delta, alpha, beta, gamma. */
        foreach (var counter in new[] { "gamma", "Delta", "beta", "alpha", "Alpha", "Beta" })
        {
            await ExecAsync(
                @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", _nextId--, at, ServerId, "NameOrderServer", "SQLServer:Workload Group Stats", counter, "default", 1L, 0L, 60, 65792);
        }

        Assert.Equal(
            ["Alpha", "alpha", "Beta", "beta", "Delta", "gamma"],
            await _dataService.GetDistinctPerfmonCountersAsync(ServerId, hoursBack: 1));
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
