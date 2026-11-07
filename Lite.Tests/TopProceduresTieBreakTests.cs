/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7908: Lite's procedures ranking (<see cref="LocalDataService.GetTopProceduresByCpuAsync"/>, the read behind
/// get_top_procedures_by_cpu and the Top Procedures grid) ends on the whole group key after the CPU sum, as the
/// queries twin and Darling's procedures read do, so procedures tied at the cut come back in one order every time.
/// Zero CPU is the common tie on a quiet window.
/// </summary>
public sealed class TopProceduresTieBreakTests : IDisposable
{
    private const int ServerId = 7908;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public TopProceduresTieBreakTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    [Fact]
    public async Task ProceduresTiedAtTheCut_ComeBackInNameOrder_OnEveryRead()
    {
        var at = DateTime.UtcNow.AddMinutes(-10);
        /* Eight procedures, all at zero CPU with executions, seeded out of name order. */
        foreach (var name in new[] { "usp_Zeta", "usp_Mu", "usp_Alpha", "usp_Theta", "usp_Beta", "usp_Kappa", "usp_Gamma", "usp_Rho" })
        {
            await ProcedureAsync(at, name, cpuUs: 0);
        }

        var service = new LocalDataService(_duckDb);
        var expected = new[] { "usp_Alpha", "usp_Beta", "usp_Gamma", "usp_Kappa" };
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var rows = await service.GetTopProceduresByCpuAsync(ServerId, hoursBack: 1, top: 4, asOfUtc: DateTime.UtcNow);
            Assert.Equal(expected, rows.Select(r => r.ObjectName).ToArray());
        }
    }

    [Fact]
    public async Task TheCpuOrderStillWins_AndTheNameOrderOnlyBreaksTies()
    {
        var at = DateTime.UtcNow.AddMinutes(-10);
        await ProcedureAsync(at, "usp_Alpha", cpuUs: 0);
        await ProcedureAsync(at, "usp_Zeta", cpuUs: 5_000);
        await ProcedureAsync(at, "usp_Beta", cpuUs: 5_000);
        await ProcedureAsync(at, "usp_Mu", cpuUs: 9_000);

        var service = new LocalDataService(_duckDb);
        var rows = await service.GetTopProceduresByCpuAsync(ServerId, hoursBack: 1, top: 10, asOfUtc: DateTime.UtcNow);

        Assert.Equal(new[] { "usp_Mu", "usp_Beta", "usp_Zeta", "usp_Alpha" }, rows.Select(r => r.ObjectName).ToArray());
    }

    private async Task ProcedureAsync(DateTime at, string objectName, long cpuUs)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time) VALUES ({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', 'AppDb', 'dbo', '{objectName}', 'SQL_STORED_PROCEDURE', TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', 10, {cpuUs}, 5000)";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
