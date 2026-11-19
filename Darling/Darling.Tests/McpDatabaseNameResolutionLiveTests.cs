/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test and the class is
   deliberately NOT [Collection("live-postgres")]. */
/// <summary>
/// #9065 (gated, DARLING_TEST_PG): a caller's <c>database_name</c> resolved against the server's newest database list, through the
/// REAL MCP host (<see cref="StatementFilterCensus.BuildLiveHostAsync"/>: the host's own tool and filter registration) over a
/// migrated store. A differently cased name reads the same rows as the stored spelling and the answer names the spelling it used;
/// two stored names that differ only by case are refused with both listed; a name the list does not hold reads as before and says
/// so; a server with no list reads as before and says nothing. A PostgreSQL target is resolved against its own list.
/// </summary>
public sealed class McpDatabaseNameResolutionLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the database_name resolution live tests.";
    private const string Listed = "dbn-listed";
    private const string Unlisted = "dbn-unlisted";
    private const string PgTarget = "dbn-pg";

    private static async Task WithHostAsync(Func<TestServer, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = DateTime.UtcNow;
        foreach (var server in new[] { Listed, Unlisted })
        {
            var serverId = ServerIdHelper.GetDeterministicHashCode(server);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, server, ct);

            /* Two long queries in SalesDB, one in each of the two names that differ only by case and one in a database no list
               names (dropped since), on both servers. */
            foreach (var (database, seconds) in new[] { ("SalesDB", 9), ("SalesDB", 7), ("Reports", 5), ("REPORTS", 3), ("DroppedDb", 2) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
                      VALUES ($1, $2, $3, $4, $2, 'rpc_completed', $5, $6, $7)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30)), serverId, server, database, seconds * 1_000_000L, "EXEC p" + seconds);
            }
        }

        /* Only the listed server has database lists. Its older list names a database the newest one does not. */
        var listedId = ServerIdHelper.GetDeterministicHashCode(Listed);
        foreach (var (age, databases) in new[]
                 {
                     (TimeSpan.FromHours(2), new[] { "master", "salesdb_old" }),
                     (TimeSpan.FromMinutes(10), new[] { "master", "SalesDB", "Reports", "REPORTS" }),
                 })
        {
            foreach (var database in databases)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',FALSE)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now - age), listedId, Listed, database);
            }
        }

        /* A PostgreSQL target keeps its list in pg_database_stats, whose newest snapshot also carries the nameless
           shared-relations row. Orders and orders are two databases there. */
        var pgId = ServerIdHelper.GetDeterministicHashCode(PgTarget);
        await DarlingMcpTestData.RegisterServerAsync(connection, pgId, PgTarget, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = 'postgres' WHERE server_id = $1", pgId);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-10)), pgId, PgTarget);
        foreach (var (age, databases) in new[]
                 {
                     (TimeSpan.FromHours(2), new[] { "postgres", "appdb_old" }),
                     (TimeSpan.FromMinutes(10), new[] { "postgres", "AppDb", "Orders", "orders" }),
                 })
        {
            foreach (var database in databases)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now - age), pgId, PgTarget, database);
            }
        }

        foreach (var (database, extension) in new[] { ("AppDb", "plpgsql"), ("AppDb", "pg_stat_statements"), ("Orders", "plpgsql"), ("orders", "pg_trgm") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO pg_extension_availability (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, is_monitoring_relevant)
                  VALUES ($1, $2, $3, $4, $5, $6, 'installed', TRUE)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30)), pgId, PgTarget, database, extension);
        }

        await using var postgres = NpgsqlDataSource.Create(storeString);
        using var host = await StatementFilterCensus.BuildLiveHostAsync(storeString, McpDataDetail.Full);
        try
        {
            await body(host, postgres, ct);
        }
        finally
        {
            NpgsqlConnection.ClearAllPools();
        }
    }

    private static async Task<(JsonElement Json, bool IsError)> LongQueriesAsync(TestServer host, string server, string? database)
    {
        var arguments = new JsonObject { ["server_name"] = server };
        if (database is not null)
        {
            arguments["database_name"] = database;
        }

        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            host, "get_long_query_completions", arguments, bearerToken: StatementFilterCensus.LiveHostToken);
        return (JsonDocument.Parse(text).RootElement.Clone(), isError);
    }

    private static async Task<(JsonElement Json, bool IsError)> ExtensionsAsync(TestServer host, string database)
    {
        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            host, "get_pg_extensions", new JsonObject { ["server_name"] = PgTarget, ["database_name"] = database },
            bearerToken: StatementFilterCensus.LiveHostToken);
        return (JsonDocument.Parse(text).RootElement.Clone(), isError);
    }

    private static bool Has(JsonElement answer, string field) => answer.TryGetProperty(field, out _);

    [Fact]
    public Task ADifferentlyCasedName_ReadsTheSameRowsAsTheStoredSpelling_AndNamesTheSpellingItUsed() =>
        WithHostAsync(async (host, _, _) =>
        {
            var (exact, exactError) = await LongQueriesAsync(host, Listed, "SalesDB");
            Assert.False(exactError);
            Assert.Equal(2, exact.GetProperty("completions").GetArrayLength());
            Assert.False(Has(exact, McpDatabaseNameResolver.ResolvedField));
            Assert.False(Has(exact, McpDatabaseNameResolver.NoteField));

            foreach (var given in new[] { "salesdb", "SALESDB", "sAlEsDb" })
            {
                var (folded, foldedError) = await LongQueriesAsync(host, Listed, given);
                Assert.False(foldedError);
                Assert.Equal(exact.GetProperty("completions").GetRawText(), folded.GetProperty("completions").GetRawText());
                Assert.Equal("SalesDB", folded.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
                Assert.False(Has(folded, McpDatabaseNameResolver.NoteField));
            }
        });

    [Fact]
    public Task TwoStoredNamesThatDifferOnlyByCase_AreRefusedWithBothListed_AndEachStillReadsWhenSpelledExactly() =>
        WithHostAsync(async (host, _, _) =>
        {
            var (refused, isError) = await LongQueriesAsync(host, Listed, "reports");
            Assert.True(isError);
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Equal("database_name", refused.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.Contains("'REPORTS', 'Reports'", refused.GetProperty("message").GetString(), StringComparison.Ordinal);

            foreach (var (given, milliseconds) in new[] { ("Reports", 5000.0), ("REPORTS", 3000.0) })
            {
                var (answer, exactError) = await LongQueriesAsync(host, Listed, given);
                Assert.False(exactError);
                var row = Assert.Single(answer.GetProperty("completions").EnumerateArray());
                Assert.Equal(milliseconds, row.GetProperty("duration_ms").GetDouble());
                Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField));
            }
        });

    [Theory]
    [InlineData("NoSuchDb")]
    [InlineData(" salesdb")]
    [InlineData("salesdb_old")]
    public Task ANameTheNewestListDoesNotHold_ReadsAsGiven_AndTheEmptyAnswerSaysItIsNotOnTheList(string given) =>
        WithHostAsync(async (host, postgres, ct) =>
        {
            var (answer, isError) = await LongQueriesAsync(host, Listed, given);

            Assert.False(isError);
            Assert.Equal("empty", answer.GetProperty("status").GetString());
            Assert.Equal(McpDatabaseNameResolver.NotSeenMessage(given), answer.GetProperty(McpDatabaseNameResolver.NoteField).GetString());
            Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField));

            /* The tool's own empty answer is still there, word for word: the note is the only thing added. */
            var direct = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, Listed, 24, 10, null, DatabaseFilter.One(given), null, ct)).RootElement;
            Assert.Equal(direct.GetProperty("message").GetString(), answer.GetProperty("message").GetString());
        });

    /// <summary>A database dropped before the newest list was taken still has history: its rows read as before, beside the note.</summary>
    [Fact]
    public Task ADroppedDatabasesHistory_StillReadsUnderItsExactName_BesideTheNote() =>
        WithHostAsync(async (host, _, _) =>
        {
            var (answer, isError) = await LongQueriesAsync(host, Listed, "DroppedDb");

            Assert.False(isError);
            var row = Assert.Single(answer.GetProperty("completions").EnumerateArray());
            Assert.Equal(2000.0, row.GetProperty("duration_ms").GetDouble());
            Assert.Equal(McpDatabaseNameResolver.NotSeenMessage("DroppedDb"), answer.GetProperty(McpDatabaseNameResolver.NoteField).GetString());

            /* Another letter case of it is not resolved: the list is the only thing a name is resolved against. */
            var (folded, _) = await LongQueriesAsync(host, Listed, "droppeddb");
            Assert.Equal("empty", folded.GetProperty("status").GetString());
            Assert.False(Has(folded, McpDatabaseNameResolver.ResolvedField));
        });

    /// <summary>
    /// A PostgreSQL target's list is its newest <c>pg_database_stats</c> snapshot, and names that differ only by case are ordinary
    /// there: each reads its own rows when spelled as stored, another letter case of the pair is refused, and a unique one resolves.
    /// </summary>
    [Fact]
    public Task APostgreSqlTargetsName_IsResolvedAgainstItsOwnDatabaseList() =>
        WithHostAsync(async (host, _, _) =>
        {
            var (exact, exactError) = await ExtensionsAsync(host, "AppDb");
            Assert.False(exactError);
            Assert.Equal(2, exact.GetProperty("extensions").GetArrayLength());
            Assert.False(Has(exact, McpDatabaseNameResolver.ResolvedField));

            var (folded, foldedError) = await ExtensionsAsync(host, "APPDB");
            Assert.False(foldedError);
            Assert.Equal(exact.GetProperty("extensions").GetRawText(), folded.GetProperty("extensions").GetRawText());
            Assert.Equal("AppDb", folded.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());

            foreach (var (given, extension) in new[] { ("Orders", "plpgsql"), ("orders", "pg_trgm") })
            {
                var (answer, _) = await ExtensionsAsync(host, given);
                var row = Assert.Single(answer.GetProperty("extensions").EnumerateArray());
                Assert.Equal(extension, row.GetProperty("extension_name").GetString());
                Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField));
            }

            var (refused, isError) = await ExtensionsAsync(host, "ORDERS");
            Assert.True(isError);
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Contains("'Orders', 'orders'", refused.GetProperty("message").GetString(), StringComparison.Ordinal);

            var (unseen, _) = await ExtensionsAsync(host, "appdb_old");
            Assert.Equal(McpDatabaseNameResolver.NotSeenMessage("appdb_old"), unseen.GetProperty(McpDatabaseNameResolver.NoteField).GetString());
        });

    [Fact]
    public Task AServerWithNoDatabaseList_ReadsAsBefore_AndSaysNothingAboutTheName() =>
        WithHostAsync(async (host, _, _) =>
        {
            var (folded, _) = await LongQueriesAsync(host, Unlisted, "salesdb");
            Assert.Equal("empty", folded.GetProperty("status").GetString());
            Assert.False(Has(folded, McpDatabaseNameResolver.ResolvedField));
            Assert.False(Has(folded, McpDatabaseNameResolver.NoteField));

            var (exact, _) = await LongQueriesAsync(host, Unlisted, "SalesDB");
            Assert.Equal(2, exact.GetProperty("completions").GetArrayLength());

            /* No name is every database, on either server, with nothing added. */
            var (all, _) = await LongQueriesAsync(host, Listed, null);
            Assert.Equal(5, all.GetProperty("completions").GetArrayLength());
            Assert.False(Has(all, McpDatabaseNameResolver.ResolvedField));
            Assert.False(Has(all, McpDatabaseNameResolver.NoteField));
        });

    [Fact]
    public Task TheCandidatesRead_TellsNoListFromAListWithoutTheName_AndReturnsOnlyTheMatchingSpellings() =>
        WithHostAsync(async (_, postgres, ct) =>
        {
            var listedId = ServerIdHelper.GetDeterministicHashCode(Listed);
            var now = DateTime.UtcNow;

            Assert.Equal(new[] { "SalesDB" }, await DarlingMcpDatabaseNames.CandidatesAsync(postgres, listedId, "SALESDB", now, ct));
            Assert.Equal(
                new[] { "REPORTS", "Reports" },
                (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, listedId, "reports", now, ct))!.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Empty((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, listedId, "salesdb_old", now, ct))!);
            Assert.Null(await DarlingMcpDatabaseNames.CandidatesAsync(postgres, ServerIdHelper.GetDeterministicHashCode(Unlisted), "SalesDB", now, ct));

            /* A PostgreSQL target's list is read when the server has no SQL Server one. */
            var pgId = ServerIdHelper.GetDeterministicHashCode(PgTarget);
            Assert.Equal(new[] { "AppDb" }, await DarlingMcpDatabaseNames.CandidatesAsync(postgres, pgId, "appdb", now, ct));
            Assert.Equal(
                new[] { "Orders", "orders" },
                (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, pgId, "ORDERS", now, ct))!.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Empty((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, pgId, "appdb_old", now, ct))!);

            /* A list older than the look-back is no list. */
            Assert.Null(await DarlingMcpDatabaseNames.CandidatesAsync(
                postgres, listedId, "SalesDB", now + DarlingMcpFindDatabaseTools.LookBack + TimeSpan.FromHours(1), ct));
        });

    /// <summary>
    /// Every tool the live host serves that takes <c>database_name</c> and <c>server_name</c>, called with a differently cased name:
    /// each answer names the stored spelling. The required arguments a tool has beyond those two are filled from its own schema, so
    /// the call reaches the tool; what the tool then answers (rows, empty, a refusal of the filler) does not matter here.
    /// </summary>
    [Fact]
    public Task EveryServedToolThatTakesADatabaseName_NamesTheStoredSpellingItResolved() =>
        WithHostAsync(async (host, _, _) =>
        {
            var tools = (await StatementFilterCensus.ListToolSchemasAsync(host, bearerToken: StatementFilterCensus.LiveHostToken))
                .Where(tool => tool.InputSchema.TryGetProperty("properties", out var properties)
                               && properties.TryGetProperty("database_name", out _)
                               && properties.TryGetProperty("server_name", out _))
                .OrderBy(tool => tool.Name, StringComparer.Ordinal)
                .ToList();

            var problems = new List<string>();
            foreach (var (name, schema) in tools)
            {
                var arguments = new JsonObject { ["server_name"] = Listed, ["database_name"] = "salesdb" };
                if (schema.TryGetProperty("required", out var required))
                {
                    foreach (var parameter in required.EnumerateArray().Select(r => r.GetString()!).Where(r => !arguments.ContainsKey(r)))
                    {
                        arguments[parameter] = Filler(schema.GetProperty("properties").GetProperty(parameter));
                    }
                }

                var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
                    host, name, arguments, bearerToken: StatementFilterCensus.LiveHostToken);
                var resolved = !isError && text.TrimStart().StartsWith('{')
                               && JsonDocument.Parse(text).RootElement.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var field)
                    ? field.GetString()
                    : null;
                if (resolved != "SalesDB")
                {
                    problems.Add($"{name}: {McpDatabaseNameResolver.ResolvedField} '{resolved}' (error {isError}) in: {(text.Length > 300 ? text[..300] : text)}");
                }
            }

            Assert.True(problems.Count == 0, string.Join("\n", problems));
            Assert.True(tools.Count >= 40, $"only {tools.Count} served tools take database_name and server_name.");
        });

    /// <summary>A value the binder reads into a required parameter of the schema's type.</summary>
    private static JsonNode Filler(JsonElement parameter)
    {
        if (parameter.TryGetProperty("enum", out var values) && values.GetArrayLength() > 0)
        {
            return JsonNode.Parse(values[0].GetRawText())!;
        }

        return (parameter.TryGetProperty("type", out var type) ? type.GetString() : "string") switch
        {
            "integer" or "number" => JsonValue.Create(1),
            "boolean" => JsonValue.Create(false),
            "array" => new JsonArray(),
            _ => JsonValue.Create("0x0000000000000001"),
        };
    }
}
