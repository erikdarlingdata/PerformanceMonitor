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
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: the long-running-query alert read returns the WHOLE statement, the statement filter judges it, and the cut to
/// the alert's 300 characters comes after. The read used to cut in SQL (<c>SUBSTRING(r.query_text, 1, 300)</c>), so a
/// value that sat early in a batch with the text that names it past the 300th character read clean and the alert
/// carried the start of the secret. #1776 own-store: this class registers one server of its own and removes its rows.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementAlertReadCutLiveTests
{
    private const string ServerName = "statement-alert-cut-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>A plain statement of 500 characters: kept as the first 300 and nothing more.</summary>
    private static readonly string Plain = "SELECT plain_ssf " + new string('x', 483);

    [Fact]
    public async Task LongRunningQueries_JudgeTheWholeStatement_ThenCutAtThreeHundred()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live alert cut test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var collected = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(-1);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, total_elapsed_time_ms, cpu_time_ms, reads, writes, query_hash, wait_type)
VALUES ($1, $2, $3, $4, 71, 'UriDb', $5, 900000, 1, 1, 0, '0x01', 'CXPACKET'), ($6, $2, $3, $4, 72, 'UriDb', $7, 800000, 1, 1, 0, '0x02', 'CXPACKET')",
                CollectionIdGenerator.Next(), collected, ServerId, ServerName, StatementScrubCanary.UriStatement(300),
                CollectionIdGenerator.Next(), Plain);

            var adapter = new DarlingAlertReadAdapter(postgres);
            var read = await adapter.GetLongRunningQueriesAsync(
                ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), thresholdMinutes: 5, maxResults: 5,
                excludeSpServerDiagnostics: true, excludeWaitFor: true, excludeBackups: true, excludeMiscWaits: true,
                excludeCdc: true, excludedDatabases: Array.Empty<string>(), LongRunningQueryExclusions.None, ct);

            var named = read.Sessions.Single(q => q.SessionId == 71).QueryText;
            Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, named, StringComparison.Ordinal);
            Assert.Contains(SensitiveStatements.PlaceholderText, named, StringComparison.Ordinal);

            var plain = read.Sessions.Single(q => q.SessionId == 72).QueryText;
            Assert.Equal(Plain[..300], plain);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_snapshots WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
