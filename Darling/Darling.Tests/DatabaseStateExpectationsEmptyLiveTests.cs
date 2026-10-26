/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Gated (DARLING_TEST_PG): the three different empty answers of <c>get_database_state_expectations</c>. A server whose engine never
/// writes a database list is <c>not_collected</c>; a SQL Server target with no snapshot yet is <c>unavailable</c>; a database name
/// missing from a real snapshot is <c>empty</c>.
/// </summary>
public sealed class DatabaseStateExpectationsEmptyLiveTests
{
    [Fact]
    public async Task GetDatabaseStateExpectations_WithNoSnapshot_SplitsNotCollectedFromUnavailable_AndAMissingNameStaysEmpty()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the database state empty-answer test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (azure, box) = (ServerIdHelper.GetDeterministicHashCode("dse-azure"), ServerIdHelper.GetDeterministicHashCode("dse-box"));
        await DarlingMcpTestData.RegisterServerAsync(connection, azure, "dse-azure", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, box, "dse-box", ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = 5 WHERE server_id = $1", azure);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = 3 WHERE server_id = $1", box);

        /* An Azure SQL Database never writes a database list: permanent, and the filter makes no difference. */
        var azureNone = JsonDocument.Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(dataSource, "dse-azure", "Sales", ct)).RootElement;
        Assert.Equal("not_collected", azureNone.GetProperty("status").GetString());

        /* A SQL Server target with no snapshot yet could have one: unavailable, with or without a name filter. */
        var boxNone = JsonDocument.Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(dataSource, "dse-box", cancellationToken: ct)).RootElement;
        Assert.Equal("unavailable", boxNone.GetProperty("status").GetString());
        var boxNoneFiltered = JsonDocument.Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(dataSource, "dse-box", "Sales", ct)).RootElement;
        Assert.Equal("unavailable", boxNoneFiltered.GetProperty("status").GetString());

        /* Once a snapshot exists, a name it does not hold is a true negative, and a name it holds is listed. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,'Sales',5,'ONLINE',FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow - TimeSpan.FromMinutes(5)), box, "dse-box");
        var missing = JsonDocument.Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(dataSource, "dse-box", "NoSuchDb", ct)).RootElement;
        Assert.Equal("empty", missing.GetProperty("status").GetString());
        Assert.Contains("NoSuchDb", missing.GetProperty("message").GetString(), StringComparison.Ordinal);
        var held = JsonDocument.Parse(await DarlingMcpDatabaseStateTools.GetDatabaseStateExpectations(dataSource, "dse-box", "Sales", ct)).RootElement;
        Assert.False(held.TryGetProperty("status", out _));
        Assert.Equal(1, held.GetProperty("databases").GetArrayLength());
    }
}
