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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4378: the collector-freshness arm (<see cref="AlertNotebookEndpoint.ResolveStatusAsync"/>'s arms 3/4)
/// must count only <c>SUCCESS</c> collection_log rows as evidence the instrument is up. An <c>ERROR</c> row
/// after the anchor is proof the collector tried and failed, not proof it is fresh — the bug this fixes was
/// counting ANY status, so a run that failed every time still read "No resolution recorded" instead of the
/// honest "Unknown (not collected since T)". A <c>SKIPPED</c> row does not count either: it can be written
/// without the run ever contacting the target (see <c>Lite/Services/RemoteCollectorService.cs</c>'s
/// user-cancelled-MFA catch), and Darling's own per-run classifier never writes it.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookCollectorFreshnessLiveTests
{
    private const int ServerId = -437800;
    private const string ServerName = "alert-notebook-freshness-4378";
    private const string Collector = "wait_stats";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OnlyErrorRunsAfterAnchor_ReadsUnknown_NotCollectedSince()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-freshness test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));

            /* An ERROR row after the anchor. Old code counted ANY status here, so it read this as fresh. */
            await SeedAsync(connection, ct, anchor.AddMinutes(5), "ERROR");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var status = await AlertNotebookEndpoint.ResolveStatusAsync(
                postgres, ServerId, fleetLevelStore: false, "High CPU", anchor, DateTime.UtcNow,
                new System.Collections.Generic.List<PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow>(),
                matchedRow: null, NullLogger.Instance, ct);

            Assert.StartsWith("Unknown (not collected since ", status, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task OnlySkippedRunsAfterAnchor_ReadsUnknown_NotCollectedSince()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-freshness test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));

            /* A SKIPPED row after the anchor. SKIPPED can be written without the run ever contacting the
               target (a user-cancelled MFA sign-in), so it must not read as fresh. */
            await SeedAsync(connection, ct, anchor.AddMinutes(5), "SKIPPED");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var status = await AlertNotebookEndpoint.ResolveStatusAsync(
                postgres, ServerId, fleetLevelStore: false, "High CPU", anchor, DateTime.UtcNow,
                new System.Collections.Generic.List<PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow>(),
                matchedRow: null, NullLogger.Instance, ct);

            Assert.StartsWith("Unknown (not collected since ", status, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ASuccessRunAfterAnchor_ReadsNoResolutionRecorded()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-freshness test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));

            await SeedAsync(connection, ct, anchor.AddMinutes(5), "SUCCESS");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var status = await AlertNotebookEndpoint.ResolveStatusAsync(
                postgres, ServerId, fleetLevelStore: false, "High CPU", anchor, DateTime.UtcNow,
                new System.Collections.Generic.List<PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow>(),
                matchedRow: null, NullLogger.Instance, ct);

            Assert.Equal("No resolution recorded", status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task OnlyRowsBeforeAnchor_ReadsUnknown_NotCollectedSince()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-freshness test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));

            await SeedAsync(connection, ct, anchor.AddMinutes(-5), "SUCCESS");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var status = await AlertNotebookEndpoint.ResolveStatusAsync(
                postgres, ServerId, fleetLevelStore: false, "High CPU", anchor, DateTime.UtcNow,
                new System.Collections.Generic.List<PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader.AlertHistoryReadRow>(),
                matchedRow: null, NullLogger.Instance, ct);

            Assert.StartsWith("Unknown (not collected since ", status, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ── helpers ── */

    private static Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, $4, $5, $6)",
            CollectionIdGenerator.Next(), ServerId, ServerName, Collector, DarlingMcpTestData.Naive(collectionTimeUtc), status);

    private static Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
}
