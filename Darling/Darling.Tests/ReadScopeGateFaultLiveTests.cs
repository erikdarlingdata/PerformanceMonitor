/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test mints its own database through
   ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #5097: the source decision itself faulting on a connection that opens and accepts the read-only step. The
/// scratch database is migrated and then loses the wide-table coverage table, so the gate's own input read
/// faults. Both readers must note <c>GateFailed</c> and answer from raw, and the gate's single Warning is the
/// only line logged.
/// </summary>
public sealed class ReadScopeGateFaultLiveTests
{
    private const int ServerId = -5097001;
    private const string ServerName = "gate-fault-live";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<ScratchPostgres> FaultedStoreAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        foreach (var sql in new[]
        {
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                + $"VALUES ({ServerId}, '{ServerName}', '{ServerName}', TRUE, 16, now(), now()) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE",
            "DROP TABLE collect.query_store_interval_wide_coverage CASCADE",
        })
        {
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync(ct);
        }

        return scratch;
    }

    [Fact]
    public async Task TheMcpTopRead_WhenTheGateInputReadFaults_NotesGateFailed_AndReadsRaw()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5097 gate-fault live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FaultedStoreAsync(ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var end = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);

        var logger = new CapturingTestLogger();
        using var scope = ReadScope.Open(logger);
        var result = await DarlingDataReader.GetQueryStoreTopWithReachAsync(postgres, ServerId, end.AddDays(-3), end, 10, null, null, null, ct);

        Assert.Null(result.Table);
        Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task TheComposeWideGate_WhenTheGateInputReadFaults_NotesGateFailed_AndReadsRaw()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5097 gate-fault live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FaultedStoreAsync(ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var end = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);

        var logger = new CapturingTestLogger();
        using var scope = ReadScope.Open(logger);
        var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(postgres, null, end.AddDays(-3), end, end, ct);

        Assert.False(resolution.Eligible);
        Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }
}
