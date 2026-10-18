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
/// #6074 part 3 (gated, DARLING_TEST_PG): a Query Store read that finds nothing says so, and names <c>get_query_store_health</c>,
/// when the database's newest health row is not READ_WRITE. The tag and execution-type empties already end in their own
/// sentences; the hint rides on them too, because a read-only Query Store looks like a quiet database whichever filter was on.
/// A READ_WRITE database keeps its answer exactly as it was.
/// </summary>
public sealed class QueryStoreReadOnlyHintLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the read-only Query Store hint live test.";

    [Fact]
    public async Task AnEmptyQueryStoreTop_OnAReadOnlyDatabase_NamesGetQueryStoreHealth_AndAReadWriteOneDoesNot()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        const string serverName = "qs-readonly-hint-6074";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        foreach (var (database, state) in new[] { ("RoDb", "READ_ONLY"), ("RwDb", "READ_WRITE") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
                  VALUES ($1,$2,$3,$4,$5,$6,'READ_WRITE',2,10,8192,'AUTO',21,200,60,'AUTO')",
                CollectionIdGenerator.Next(), now.AddHours(-1), serverId, serverName, database, state);
        }

        /* An execution_type filter matching nothing is the "measured zero" branch only when the unfiltered read has rows; here there are none,
           so both calls take the not-collected/unavailable ladder. The READ_ONLY database is the one the hint is about. */
        var readOnly = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(dataSource, serverName, 24, 20, "RoDb", cancellationToken: ct)).RootElement;
        var message = readOnly.GetProperty("message").GetString()!;
        Assert.Contains("get_query_store_health", message, StringComparison.Ordinal);
        Assert.Contains("RoDb", message, StringComparison.Ordinal);
        Assert.Contains("READ_ONLY", message, StringComparison.Ordinal);

        var readWrite = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(dataSource, serverName, 24, 20, "RwDb", cancellationToken: ct)).RootElement;
        Assert.DoesNotContain("get_query_store_health", readWrite.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* The scoped RoDb answer above is already named by the older all-or-nothing precondition, which only fires when NO database in
           scope is READ_WRITE. The hint's own case is a server read that spans both: one READ_WRITE database silences that precondition,
           so only the hint says that RoDb stopped recording. This is the assertion that fails without the #6074 hint. */
        var wholeServer = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(dataSource, serverName, 24, 20, cancellationToken: ct)).RootElement;
        Assert.Contains("is not READ_WRITE on RoDb (READ_ONLY)", wholeServer.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.Contains("get_query_store_health", wholeServer.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }
}
