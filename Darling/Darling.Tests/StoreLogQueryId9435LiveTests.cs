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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_store_log with a <c>query_id</c> (#9435): the slow plan of ONE store statement is one call away. The id lives
/// only in the plan row's message (<c>queryid=&lt;id&gt;</c>), so these tests read a real stored capture and check that
/// the filter returns the matching plan and nothing else, including when one id is a prefix or suffix of another.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreLogQueryId9435LiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Prefix = "2026-11-20 14:03:02.551 UTC [5288] ";

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9435 store-log tests.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: the scratch store is this class's own database, dropped on dispose. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(postgres, ct);
    }

    /// <summary>An auto_explain JSON plan entry. Each id gets its own table, so each is its own plan shape and its own row.</summary>
    private static string PlanEntry(long queryId, string table) =>
        Prefix + "LOG:  duration: 12000.5 ms  plan:\n"
        + "\t{\n"
        + "\t  \"Plan\": {\n"
        + "\t    \"Node Type\": \"Seq Scan\",\n"
        + "\t    \"Relation Name\": \"" + table + "\",\n"
        + "\t    \"Alias\": \"" + table + "\"\n"
        + "\t  },\n"
        + "\t  \"Query Identifier\": " + queryId + "\n"
        + "\t}\n";

    private static async Task<string> ReadAsync(NpgsqlDataSource postgres, string? queryId, CancellationToken ct, int limit = 50) =>
        await DarlingMcpStoreLogTools.GetStoreLog(postgres, limit: limit, query_id: queryId, cancellationToken: ct);

    private static async Task SeedAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var slab = PlanEntry(12, "ledger_twelve")
            + PlanEntry(123, "ledger_onetwothree")
            + PlanEntry(-8172272423916980386, "ledger_negative")
            + Prefix + "ERROR:  reconciliation of journal_unrelated stopped\n";
        var retained = StoreLogClassifier.Classify(slab).Groups.Count(g => g.MessageText is not null);
        Assert.Equal(4, retained);
        await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct);
    }

    [Fact]
    public async Task QueryId_ReturnsOnlyThatStatementsPlan_AndAnIdThatIsAPrefixOfAnotherDoesNotMatchIt()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            await SeedAsync(postgres, ct);

            var all = JsonDocument.Parse(await ReadAsync(postgres, null, ct)).RootElement;
            Assert.Equal(4, all.GetProperty("retained_events_returned").GetInt32());

            foreach (var (id, table) in new[] { ("12", "ledger_twelve"), ("123", "ledger_onetwothree"), ("-8172272423916980386", "ledger_negative") })
            {
                var json = await ReadAsync(postgres, id, ct);
                Assert.False(McpHelpers.IsErrorEnvelope(json) || McpHelpers.IsRefusalEnvelope(json), json[..Math.Min(json.Length, 400)]);
                var root = JsonDocument.Parse(json).RootElement;
                var row = Assert.Single(root.GetProperty("retained_events").EnumerateArray());
                Assert.Equal("slow_plan", row.GetProperty("event_class").GetString());
                Assert.EndsWith(" queryid=" + id, row.GetProperty("message_text").GetString(), StringComparison.Ordinal);
                Assert.Contains(table, row.GetProperty("sample_line").GetString(), StringComparison.Ordinal);
                Assert.Equal(1, root.GetProperty("retained_events_total").GetInt32());
            }
        });
    }

    [Fact]
    public async Task QueryId_WithNoPlanInTheWindow_IsEmpty_AndAnIdThatIsASuffixOfAnotherDoesNotMatchIt()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            await SeedAsync(postgres, ct);

            /* 23 ends 123's message, 2 starts it, 2272423916980386 ends the negative id's: none is a statement that ran. */
            foreach (var missing in new[] { "23", "2", "2272423916980386", "8172272423916980386", "999" })
            {
                var json = await ReadAsync(postgres, missing, ct);
                Assert.False(McpHelpers.IsErrorEnvelope(json) || McpHelpers.IsRefusalEnvelope(json), json[..Math.Min(json.Length, 400)]);
                Assert.Equal("empty", JsonDocument.Parse(json).RootElement.GetProperty("status").GetString());
            }
        });
    }

    [Fact]
    public async Task QueryId_ThatIsNotAnInteger_IsRefused_AndOmittingItReadsEveryRow()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            await SeedAsync(postgres, ct);

            Assert.True(McpHelpers.IsRefusalEnvelope(await ReadAsync(postgres, "12; DROP", ct)));

            var blank = JsonDocument.Parse(await ReadAsync(postgres, "  ", ct)).RootElement;
            Assert.Equal(4, blank.GetProperty("retained_events_returned").GetInt32());
        });
    }
}
