/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5711: the long-query trace's <c>query_hash</c> action is 0 on every batch-level completion (measured on SQL Server
/// 2016, 2017 and 2019: normal and aborted alike, and on the attention event). The Darling readers must treat 0 (and
/// empty or NULL) as unknown, and key a row by a fingerprint of its scrubbed statement text instead, so rows of different
/// statements do not collapse into one query. Lite's twin is <c>LongQueryUnknownHashTests</c> in Lite.Tests.
/// </summary>
[Collection("live-postgres")]
public sealed class LongQueryUnknownHashTests
{
    private static readonly string[] Statements =
    {
        "WAITFOR DELAY '00:00:09'; SELECT 1",
        "SELECT COUNT_BIG(*) FROM sys.all_columns a CROSS JOIN sys.all_columns b",
        "SELECT TOP (5) name FROM sys.columns ORDER BY name",
    };

    /* Rows with query_hash 0 from three different statements: three identities, none of them the raw 0. A reader that
       keyed by the stored value would give one. */
    [Fact]
    public void TheMcpRow_KeysZeroHashRowsOfDifferentStatements_ByTheirOwnText()
    {
        var rows = Statements.Select(s => new DarlingLongQueryReader.LongQueryReadRow { QueryHash = "0", StatementText = s }).ToList();

        Assert.Equal(3, rows.Select(r => r.QueryIdentity).Distinct().Count());
        Assert.All(rows, r => Assert.StartsWith(QueryHashIdentity.FingerprintPrefix, r.QueryIdentity));
    }

    [Fact]
    public void TheViewerRow_KeysZeroHashRowsOfDifferentStatements_ByTheirOwnText()
    {
        var rows = Statements.Select(s => new ViewerLongQueryRow { QueryHash = "0", StatementText = s }).ToList();

        Assert.Equal(3, rows.Select(r => r.QueryIdentity).Distinct().Count());
        Assert.All(rows, r => Assert.StartsWith(QueryHashIdentity.FingerprintPrefix, r.QueryIdentity));
    }

    [Fact]
    public void ARealHashIsTheIdentity_AndARowWithNoHashAndNoTextHasNone()
    {
        Assert.Equal("0xabc", new DarlingLongQueryReader.LongQueryReadRow { QueryHash = "0xABC", StatementText = "SELECT 1" }.QueryIdentity);
        Assert.Null(new DarlingLongQueryReader.LongQueryReadRow { QueryHash = "0" }.QueryIdentity);
        Assert.Equal("", new ViewerLongQueryRow { QueryHash = "", StatementText = "" }.QueryIdentity);
    }

    /* The pins that the readers apply the rule: the stored value goes through KnownHash on the way in, the MCP payload
       carries query_identity, and the grids bind and filter on the identity, not the raw hash. Putting any of them back
       to the raw value fails here. */
    [Fact]
    public void TheReaders_PassTheStoredHashThroughKnownHash_AndTheGridsBindTheIdentity()
    {
        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingLongQueryReader.cs");
        Assert.Contains("QueryHash = reader.IsDBNull(8) ? null : QueryHashIdentity.KnownHash(reader.GetString(8))", reader, StringComparison.Ordinal);

        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpLongQueryTools.cs");
        Assert.Contains("query_identity = r.QueryIdentity", tool, StringComparison.Ordinal);

        var viewer = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.LongQueries.cs");
        Assert.Contains("QueryHashIdentity.KnownHash(reader.GetString(9))", viewer, StringComparison.Ordinal);

        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var grid = xaml[xaml.IndexOf("x:Name=\"LongQueryCompletionsGrid\"", StringComparison.Ordinal)..];
        var column = grid[..grid.IndexOf("</DataGridTextColumn>", grid.IndexOf("{Binding QueryIdentity}", StringComparison.Ordinal), StringComparison.Ordinal)];
        Assert.Contains("{Binding QueryIdentity}", column, StringComparison.Ordinal);
        Assert.Contains("Tag=\"QueryIdentity\"", column, StringComparison.Ordinal);
    }

    private const string ServerName = "darling-longquery-unknown-hash-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* The MCP reader against the store: rows stored with query_hash '0' read back with a NULL hash and one identity per
       statement. Skips without DARLING_TEST_PG, like its siblings. */
    [Fact]
    public async Task TheStoreRead_ReturnsNoHashForZeroHashRows_AndOneIdentityPerStatement()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live unknown-hash test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            DateTime end;
            using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await DeleteRowsAsync(connection, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
                end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
                var hashes = new[] { "0", "0", "0x0000000000000000", "0x1A2B3C4D5E6F7A8B" };
                var texts = Statements.Append("EXEC dbo.RealHash").ToArray();
                for (var i = 0; i < hashes.Length; i++)
                {
                    var at = DarlingMcpTestData.Naive(end.AddMinutes(-10 - i));
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text, query_hash)
VALUES ($1,$2,$3,$4,$5,'sql_batch_completed','Db',$6,$7,$8)",
                        CollectionIdGenerator.Next(), at, ServerId, ServerName, at, 5_000_000L - i, texts[i], hashes[i]);
                }
            }

            var rows = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, end.AddHours(-1), end, 100, cancellationToken: ct);

            Assert.Equal(4, rows.Count);
            var unknown = rows.Where(r => r.StatementText != "EXEC dbo.RealHash").ToList();
            Assert.Equal(3, unknown.Count);
            Assert.All(unknown, r => Assert.Null(r.QueryHash));
            Assert.Equal(3, unknown.Select(r => r.QueryIdentity).Distinct().Count());
            var real = rows.Single(r => r.StatementText == "EXEC dbo.RealHash");
            Assert.Equal("0x1A2B3C4D5E6F7A8B", real.QueryHash);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM long_query_completions WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
