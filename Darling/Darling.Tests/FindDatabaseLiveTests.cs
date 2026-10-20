/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #6074 part 2 (gated, DARLING_TEST_PG): <c>find_database</c> names each monitored server whose NEWEST database list holds a database,
/// exact and case-insensitive, with the newest list's time and whether the server is still monitored. A database only an OLDER list
/// names (dropped since) is not found; a server with no recent list is not searched.
/// </summary>
public sealed class FindDatabaseLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the find_database live test.";

    private static async Task SnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, TimeSpan age, System.Threading.CancellationToken ct, params string[] databases)
    {
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow - age);
        foreach (var database in databases)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',FALSE)",
                CollectionIdGenerator.Next(), at, serverId, serverName, database);
        }
    }

    [Fact]
    public async Task FindDatabase_NamesEachServerWhoseNewestListHoldsIt_AndOnlyThose()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (a, b, c, d) = (ServerIdHelper.GetDeterministicHashCode("fd-a"), ServerIdHelper.GetDeterministicHashCode("fd-b"),
            ServerIdHelper.GetDeterministicHashCode("fd-c"), ServerIdHelper.GetDeterministicHashCode("fd-d"));
        foreach (var (id, name) in new[] { (a, "fd-a"), (b, "fd-b"), (c, "fd-c"), (d, "fd-d") })
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        /* a: holds Tenant42 in its newest list; b: only an OLDER list held it (dropped since); c: holds it, but the server is disabled;
           d: its only list is older than the look-back, so it is not searched. */
        await SnapshotAsync(connection, a, "fd-a", TimeSpan.FromMinutes(10), ct, "master", "TENANT42");
        await SnapshotAsync(connection, b, "fd-b", TimeSpan.FromHours(3), ct, "Tenant42", "other");
        await SnapshotAsync(connection, b, "fd-b", TimeSpan.FromMinutes(20), ct, "other");
        await SnapshotAsync(connection, c, "fd-c", TimeSpan.FromHours(9), ct, "tenant42");
        await SnapshotAsync(connection, d, "fd-d", TimeSpan.FromDays(10), ct, "Tenant42");
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET is_enabled = FALSE WHERE server_id = $1", c);

        var found = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, " tenant42 ", ct)).RootElement;
        var matches = found.GetProperty("matches").EnumerateArray().ToList();
        Assert.Equal(new[] { "fd-a", "fd-c" }, matches.Select(m => m.GetProperty("server_name").GetString()).ToArray());
        Assert.Equal("TENANT42", matches[0].GetProperty("database_name").GetString());
        Assert.True(matches[0].GetProperty("is_monitored").GetBoolean());
        Assert.True(matches[0].GetProperty("snapshot_current").GetBoolean());
        Assert.False(matches[1].GetProperty("is_monitored").GetBoolean());
        Assert.False(matches[1].GetProperty("snapshot_current").GetBoolean());
        Assert.Equal(3, found.GetProperty("servers_searched").GetInt32());

        var none = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, "NoSuchDb", ct)).RootElement;
        Assert.Equal("empty", none.GetProperty("status").GetString());
        Assert.Contains("NoSuchDb", none.GetProperty("message").GetString(), StringComparison.Ordinal);

        var blank = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, "  ", ct)).RootElement;
        Assert.Equal("invalid", blank.GetProperty("status").GetString());
    }

    /// <summary>
    /// #6178: with no database list anywhere, the answer is not_collected when every registered server is an Azure SQL Database
    /// (that engine never writes the list), and stays empty when at least one server could have had it.
    /// </summary>
    [Fact]
    public async Task FindDatabase_WithNoList_AnswersNotCollectedOnlyWhenEveryServerIsAzureSqlDb()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (azure, box) = (ServerIdHelper.GetDeterministicHashCode("fd-azure"), ServerIdHelper.GetDeterministicHashCode("fd-box"));
        await DarlingMcpTestData.RegisterServerAsync(connection, azure, "fd-azure", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, box, "fd-box", ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = 5 WHERE server_id = $1", azure);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = 3 WHERE server_id = $1", box);

        var mixed = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, "Tenant1", ct)).RootElement;
        Assert.Equal("empty", mixed.GetProperty("status").GetString());

        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", box);
        var azureOnly = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, "Tenant1", ct)).RootElement;
        Assert.Equal("not_collected", azureOnly.GetProperty("status").GetString());
        var message = azureOnly.GetProperty("message").GetString();
        Assert.Contains("Azure SQL Database", message, StringComparison.Ordinal);
        Assert.Contains("database_states", message, StringComparison.Ordinal);
    }
}


/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #6178 (gated, DARLING_TEST_PG): <c>get_job_history</c> with no server named answers not_collected when every registered server is an
/// Azure SQL Database (no SQL Agent), and stays empty when at least one server could have jobs. The stored Hyperscale read of
/// <c>get_database_sizes</c> comes from the newest server_properties row.
/// </summary>
public sealed class AzureEngineWordingLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the Azure wording live tests.";

    [Fact]
    public async Task JobHistory_OfEveryServer_AnswersNotCollectedOnlyWhenEveryServerIsAzureSqlDb()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (azure, azure2, box) = (ServerIdHelper.GetDeterministicHashCode("jh-azure"), ServerIdHelper.GetDeterministicHashCode("jh-azure2"),
            ServerIdHelper.GetDeterministicHashCode("jh-box"));
        await DarlingMcpTestData.RegisterServerAsync(connection, azure, "jh-azure", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, azure2, "jh-azure2", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, box, "jh-box", ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = 5 WHERE server_id = $1 OR server_id = $2", azure, azure2);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET sql_engine_edition = 3 WHERE server_id = $1", box);

        var mixed = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(dataSource, cancellationToken: ct)).RootElement;
        Assert.Equal("empty", mixed.GetProperty("status").GetString());

        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", box);
        var azureOnly = JsonDocument.Parse(await DarlingMcpJobTools.GetJobHistory(dataSource, cancellationToken: ct)).RootElement;
        Assert.Equal("not_collected", azureOnly.GetProperty("status").GetString());
        var message = azureOnly.GetProperty("message").GetString();
        Assert.Contains("Azure SQL Database", message, StringComparison.Ordinal);
        Assert.Contains("job_history", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task IsHyperscale_ReadsTheNewestStoredEditionOrServiceObjective()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var id = ServerIdHelper.GetDeterministicHashCode("hs-server");
        await DarlingMcpTestData.RegisterServerAsync(connection, id, "hs-server", ct);
        Assert.False(await DarlingEngineCapability.IsHyperscaleAsync(dataSource, id, ct));

        async Task StoreAsync(TimeSpan age, string edition, string objective) =>
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, service_objective, engine_edition) VALUES ($1,$2,$3,$4,$5,$6,5)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow - age), id, "hs-server", edition, objective);

        await StoreAsync(TimeSpan.FromHours(2), "Azure SQL Database (General Purpose)", "GP_Gen5_4");
        Assert.False(await DarlingEngineCapability.IsHyperscaleAsync(dataSource, id, ct));
        await StoreAsync(TimeSpan.FromMinutes(5), "Azure SQL Database (Hyperscale)", "HS_Gen5_4");
        Assert.True(await DarlingEngineCapability.IsHyperscaleAsync(dataSource, id, ct));
    }
}
