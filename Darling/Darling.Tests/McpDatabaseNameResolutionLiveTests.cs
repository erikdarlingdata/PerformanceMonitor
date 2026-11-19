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
    private const string Twin = "dbn-twin";
    private const string PgTwin = "dbn-pg-twin";
    private const string Old = "dbn-old";
    private const string PgOld = "dbn-pg-old";
    private const string Clash = "dbn-clash";
    private const string ClashOther = "dbn-clash-other";

    /// <summary>When the only lists of <see cref="Old"/> and <see cref="PgOld"/> were taken: days ago, inside the look-back, on a whole second.</summary>
    private static readonly DateTime OldListTaken = DateTime.UtcNow.Date.AddDays(-3).AddHours(4).AddMinutes(5).AddSeconds(6);

    private static async Task WithHostAsync(Func<TestServer, NpgsqlDataSource, CancellationToken, Task> body, bool allowWrites = false)
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

        /* Two servers with case twins over time. Three hours ago each had one database in lower case and one in mixed case. Since
           then the first was renamed to mixed case (the list an hour ago), and the second was dropped (absent an hour ago) and
           recreated in lower case (the newest list). Rows stored under each spelling date from when that spelling was the name. */
        var twinId = ServerIdHelper.GetDeterministicHashCode(Twin);
        await DarlingMcpTestData.RegisterServerAsync(connection, twinId, Twin, ct);
        foreach (var (age, databases) in new[]
                 {
                     (TimeSpan.FromHours(3), new[] { "master", "sales", "Archive" }),
                     (TimeSpan.FromHours(1), new[] { "master", "Sales" }),
                     (TimeSpan.FromMinutes(10), new[] { "master", "Sales", "archive" }),
                 })
        {
            foreach (var database in databases)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',FALSE)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now - age), twinId, Twin, database);
            }
        }

        foreach (var (database, seconds, minutesAgo) in new[] { ("sales", 11, 150), ("Sales", 4, 20), ("Archive", 6, 150), ("archive", 1, 5) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
                  VALUES ($1, $2, $3, $4, $2, 'rpc_completed', $5, $6, $7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-minutesAgo)), twinId, Twin, database, seconds * 1_000_000L, "EXEC p" + seconds);
        }

        var pgTwinId = ServerIdHelper.GetDeterministicHashCode(PgTwin);
        await DarlingMcpTestData.RegisterServerAsync(connection, pgTwinId, PgTwin, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = 'postgres' WHERE server_id = $1", pgTwinId);
        foreach (var (age, databases) in new[]
                 {
                     (TimeSpan.FromHours(3), new[] { "postgres", "appsales", "Ledger" }),
                     (TimeSpan.FromHours(1), new[] { "postgres", "AppSales" }),
                     (TimeSpan.FromMinutes(10), new[] { "postgres", "AppSales", "ledger" }),
                 })
        {
            foreach (var database in databases)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now - age), pgTwinId, PgTwin, database);
            }
        }

        /* The extension inventory is a snapshot: the one from before the rename and the drop, and the newest one. */
        foreach (var (database, extension, minutesAgo) in new[]
                 {
                     ("appsales", "plpgsql", 150), ("Ledger", "pg_trgm", 150),
                     ("AppSales", "pg_stat_statements", 5), ("ledger", "plpgsql", 5),
                 })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO pg_extension_availability (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, is_monitoring_relevant)
                  VALUES ($1, $2, $3, $4, $5, $6, 'installed', TRUE)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-minutesAgo)), pgTwinId, PgTwin, database, extension);
        }

        /* A server whose only list is days old, with one long query since, and a PostgreSQL target whose only list is as old. */
        var oldId = ServerIdHelper.GetDeterministicHashCode(Old);
        await DarlingMcpTestData.RegisterServerAsync(connection, oldId, Old, ct);
        foreach (var database in new[] { "master", "SalesDB", "Reports", "REPORTS" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',FALSE)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(OldListTaken), oldId, Old, database);
        }

        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
              VALUES ($1, $2, $3, $4, $2, 'rpc_completed', $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30)), oldId, Old, "SalesDB", 8_000_000L, "EXEC p8");

        var pgOldId = ServerIdHelper.GetDeterministicHashCode(PgOld);
        await DarlingMcpTestData.RegisterServerAsync(connection, pgOldId, PgOld, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = 'postgres' WHERE server_id = $1", pgOldId);
        foreach (var database in new[] { "postgres", "AppDb" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(OldListTaken), pgOldId, PgOld, database);
        }

        /* Two servers one name fits: it is the first one's stored name and the second one's display name. Both list SalesDB. */
        var clashId = ServerIdHelper.GetDeterministicHashCode(Clash);
        var clashOtherId = ServerIdHelper.GetDeterministicHashCode(ClashOther);
        await DarlingMcpTestData.RegisterServerAsync(connection, clashId, Clash, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, clashOtherId, ClashOther, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = $1 WHERE server_id = $2", "dbn-first-of-two", clashId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = $1 WHERE server_id = $2", Clash, clashOtherId);
        foreach (var (id, name) in new[] { (clashId, Clash), (clashOtherId, ClashOther) })
        {
            foreach (var database in new[] { "master", "SalesDB" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',FALSE)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-10)), id, name, database);
            }
        }

        await using var postgres = NpgsqlDataSource.Create(storeString);
        using var host = await StatementFilterCensus.BuildLiveHostAsync(storeString, McpDataDetail.Full, allowWrites: allowWrites);
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

    /// <summary>The PostgreSQL twin's extension inventory for one database, as of now or as of <paramref name="asOf"/>.</summary>
    private static async Task<(JsonElement Json, bool IsError)> TwinExtensionsAsync(TestServer host, string database, DateTime? asOf = null)
    {
        var arguments = new JsonObject { ["server_name"] = PgTwin, ["database_name"] = database };
        if (asOf is { } end)
        {
            arguments["as_of"] = end.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);
        }

        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(host, "get_pg_extensions", arguments, bearerToken: StatementFilterCensus.LiveHostToken);
        return (JsonDocument.Parse(text).RootElement.Clone(), isError);
    }

    private static async Task<(JsonElement Json, bool IsError)> SetExpectedAsync(TestServer host, string server, string database)
    {
        var (text, isError) = await StatementFilterCensus.CallRealToolAsync(
            host, "set_database_state_expected",
            new JsonObject { ["server_name"] = server, ["database_name"] = database, ["expected_state"] = "OFFLINE" },
            bearerToken: StatementFilterCensus.LiveHostToken);
        return (JsonDocument.Parse(text).RootElement.Clone(), isError);
    }

    private static bool Has(JsonElement answer, string field) => answer.TryGetProperty(field, out _);

    private static string[] Sorted(IReadOnlyList<string>? names) => names!.OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The database names of the long queries an answer lists.</summary>
    private static string[] DatabasesOf(JsonElement answer) =>
        answer.GetProperty("completions").EnumerateArray().Select(row => row.GetProperty("database_name").GetString()!).ToArray();

    [Fact]
    public Task ASqlServerDatabaseRenamedOrRecreatedInAnotherCase_KeepsItsEarlierRowsReachableUnderTheEarlierSpelling() =>
        WithHostAsync(async (host, _, _) =>
        {
            /* Each spelling the server has had, given exactly, reads its own rows and is never rewritten. */
            foreach (var spelling in new[] { "sales", "Sales", "Archive", "archive" })
            {
                var (answer, isError) = await LongQueriesAsync(host, Twin, spelling);
                Assert.False(isError);
                Assert.Equal(new[] { spelling }, DatabasesOf(answer));
                Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField), spelling);
                Assert.False(Has(answer, McpDatabaseNameResolver.NoteField), spelling);
            }

            /* A spelling no list has ever held resolves to the newest list's. */
            foreach (var (given, stored) in new[] { ("SALES", "Sales"), ("ARCHIVE", "archive") })
            {
                var (answer, isError) = await LongQueriesAsync(host, Twin, given);
                Assert.False(isError);
                Assert.Equal(new[] { stored }, DatabasesOf(answer));
                Assert.Equal(stored, answer.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
            }
        });

    [Fact]
    public Task APostgreSqlDatabaseRenamedOrRecreatedInAnotherCase_KeepsItsEarlierRowsReachableUnderTheEarlierSpelling() =>
        WithHostAsync(async (host, _, _) =>
        {
            var beforeTheChange = DateTime.UtcNow.AddHours(-2);
            foreach (var (spelling, extension, asOf) in new (string, string, DateTime?)[]
                     {
                         ("appsales", "plpgsql", beforeTheChange), ("Ledger", "pg_trgm", beforeTheChange),
                         ("AppSales", "pg_stat_statements", null), ("ledger", "plpgsql", null),
                     })
            {
                var (answer, isError) = await TwinExtensionsAsync(host, spelling, asOf);
                Assert.False(isError);
                var row = Assert.Single(answer.GetProperty("extensions").EnumerateArray());
                Assert.Equal((spelling, extension), (row.GetProperty("database_name").GetString(), row.GetProperty("extension_name").GetString()));
                Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField), spelling);
                Assert.False(Has(answer, McpDatabaseNameResolver.NoteField), spelling);
            }

            foreach (var (given, stored) in new[] { ("APPSALES", "AppSales"), ("LEDGER", "ledger") })
            {
                var (answer, isError) = await TwinExtensionsAsync(host, given);
                Assert.False(isError);
                Assert.Equal(stored, Assert.Single(answer.GetProperty("extensions").EnumerateArray()).GetProperty("database_name").GetString());
                Assert.Equal(stored, answer.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
            }
        });

    [Fact]
    public Task TheCandidatesRead_AddsTheGivenSpelling_OnlyWhenAnEarlierListInTheLookBackHoldsIt() =>
        WithHostAsync(async (_, postgres, ct) =>
        {
            var now = DateTime.UtcNow;
            foreach (var (server, earlier, newest, recreated) in new[] { (Twin, "sales", "Sales", "archive"), (PgTwin, "appsales", "AppSales", "ledger") })
            {
                var serverId = ServerIdHelper.GetDeterministicHashCode(server);
                var dropped = char.ToUpperInvariant(recreated[0]) + recreated[1..];

                Assert.Equal(new[] { newest, earlier }, Sorted((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, serverId, earlier, now, ct))?.Names));
                Assert.Equal(new[] { dropped, recreated }, Sorted((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, serverId, dropped, now, ct))?.Names));
                Assert.Equal(new[] { newest }, (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, serverId, newest, now, ct))?.Names);
                Assert.Equal(new[] { newest }, (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, serverId, newest.ToUpperInvariant(), now, ct))?.Names);

                /* With only the newest list inside the look-back, the earlier spelling is on no list. */
                var onlyTheNewestList = now + DarlingMcpFindDatabaseTools.LookBack - TimeSpan.FromMinutes(30);
                Assert.Equal(new[] { newest }, (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, serverId, earlier, onlyTheNewestList, ct))?.Names);
            }
        });

    [Fact]
    public Task AToolThatChangesState_IsRefusedForACaseOnlyMismatch_AndRunsWithTheExactSpelling() =>
        WithHostAsync(async (host, postgres, ct) =>
        {
            var listedId = ServerIdHelper.GetDeterministicHashCode(Listed);

            async Task<List<DatabaseStateExpectedStore.Row>> ExpectedAsync() =>
                (await DatabaseStateExpectedStore.ReadAsync(postgres, listedId, 30, ct)).Where(row => row.ExpectedState is not null).ToList();

            var (refused, isError) = await SetExpectedAsync(host, Listed, "salesdb");
            Assert.True(isError);
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Equal("database_name", refused.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.StartsWith(McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB"), refused.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Contains("pass 'SalesDB' exactly", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Empty(await ExpectedAsync());

            /* Two names differ only by case: still the list, as for a read. */
            var (ambiguous, ambiguousError) = await SetExpectedAsync(host, Listed, "reports");
            Assert.True(ambiguousError);
            Assert.Contains("'REPORTS', 'Reports'", ambiguous.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Empty(await ExpectedAsync());

            var (set, setError) = await SetExpectedAsync(host, Listed, "SalesDB");
            Assert.False(setError);
            Assert.Equal("set", set.GetProperty("status").GetString());
            Assert.False(Has(set, McpDatabaseNameResolver.ResolvedField));
            var row = Assert.Single(await ExpectedAsync());
            Assert.Equal(("SalesDB", "OFFLINE", true), (row.DatabaseName, row.ExpectedState, row.IsUserOverride));

            /* An earlier spelling is exact too, so it reaches the tool, which answers for itself. */
            var (earlier, _) = await SetExpectedAsync(host, Twin, "sales");
            Assert.Contains("is not in the latest collected snapshot", earlier.GetProperty("message").GetString(), StringComparison.Ordinal);
        }, allowWrites: true);

    [Fact]
    public Task AServerNameThatFitsTwoServers_IsRefusedByAToolThatChangesState_AndNothingIsWritten() =>
        WithHostAsync(async (host, postgres, ct) =>
        {
            var clashId = ServerIdHelper.GetDeterministicHashCode(Clash);
            var clashOtherId = ServerIdHelper.GetDeterministicHashCode(ClashOther);

            async Task<int> ExpectationsAsync()
            {
                var rows = 0;
                foreach (var id in new[] { clashId, clashOtherId })
                {
                    rows += (await DatabaseStateExpectedStore.ReadAsync(postgres, id, 30, ct)).Count(row => row.ExpectedState is not null);
                }

                return rows;
            }

            Task<(string Text, bool IsError)> SetAsync(string database) => StatementFilterCensus.CallRealToolAsync(
                host, "set_database_state_expected",
                new JsonObject { ["server_name"] = Clash, ["database_name"] = database, ["expected_state"] = "OFFLINE" },
                bearerToken: StatementFilterCensus.LiveHostToken);

            /* The name is one server's stored name and the other's display name: a read takes the stored name, a write refuses. */
            var (stored, readError) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, Clash, ct);
            Assert.Null(readError);
            Assert.Equal(clashId, stored.ServerId);
            var (_, clash) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, Clash, ct, NameClash.Refuse);
            Assert.NotNull(clash);
            var clashMessage = JsonDocument.Parse(clash!).RootElement.GetProperty("message").GetString();

            var (refused, _) = await SetAsync("SalesDB");
            Assert.Equal(clashMessage, JsonDocument.Parse(refused).RootElement.GetProperty("message").GetString());
            Assert.Equal(0, await ExpectationsAsync());

            /* Another case of the name is corrected against neither server's list: the lookup stands aside and the tool refuses the server. */
            var (otherCase, _) = await SetAsync("salesdb");
            Assert.Equal(clashMessage, JsonDocument.Parse(otherCase).RootElement.GetProperty("message").GetString());
            Assert.Equal(0, await ExpectationsAsync());

            /* A name that fits one server still writes. */
            var (set, setError) = await SetExpectedAsync(host, ClashOther, "SalesDB");
            Assert.False(setError);
            Assert.Equal("set", set.GetProperty("status").GetString());
            Assert.Equal(1, await ExpectationsAsync());
        }, allowWrites: true);

    [Fact]
    public Task AListTakenDaysAgo_SaysWhenItWasTaken_BesideTheResolvedName_InTheNote_AndInTheRefusalOfAWrite() =>
        WithHostAsync(async (host, _, _) =>
        {
            var asOf = McpDatabaseNameResolver.AsOf(OldListTaken);
            Assert.EndsWith("T04:05:06.0000000Z", asOf, StringComparison.Ordinal);
            Assert.True(DateTime.UtcNow - OldListTaken > TimeSpan.FromDays(2));

            var (resolved, _) = await LongQueriesAsync(host, Old, "salesdb");
            Assert.Equal("SalesDB", resolved.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
            Assert.Equal(asOf, resolved.GetProperty(McpDatabaseNameResolver.ResolvedAsOfField).GetString());

            var (unseen, _) = await LongQueriesAsync(host, Old, "Nowhere");
            var note = unseen.GetProperty(McpDatabaseNameResolver.NoteField).GetString()!;
            Assert.Equal(McpDatabaseNameResolver.NotSeenMessage("Nowhere", OldListTaken), note);
            Assert.EndsWith("was taken at " + asOf + ".", note, StringComparison.Ordinal);

            var (refused, refusedError) = await SetExpectedAsync(host, Old, "salesdb");
            Assert.True(refusedError);
            var refusal = refused.GetProperty("message").GetString()!;
            Assert.Equal(McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB", OldListTaken), refusal);
            Assert.EndsWith("was taken at " + asOf + ".", refusal, StringComparison.Ordinal);

            var (ambiguous, ambiguousError) = await LongQueriesAsync(host, Old, "reports");
            Assert.True(ambiguousError);
            Assert.EndsWith("was taken at " + asOf + ".", ambiguous.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* An exact name says nothing about the list, so it carries no time. */
            var (exact, _) = await LongQueriesAsync(host, Old, "SalesDB");
            Assert.False(Has(exact, McpDatabaseNameResolver.ResolvedAsOfField));
            Assert.False(Has(exact, McpDatabaseNameResolver.NoteField));

            /* A PostgreSQL target's list is dated the same way. */
            var (pgText, _) = await StatementFilterCensus.CallRealToolAsync(
                host, "get_pg_extensions", new JsonObject { ["server_name"] = PgOld, ["database_name"] = "appdb" },
                bearerToken: StatementFilterCensus.LiveHostToken);
            var pg = JsonDocument.Parse(pgText).RootElement;
            Assert.Equal("AppDb", pg.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
            Assert.Equal(asOf, pg.GetProperty(McpDatabaseNameResolver.ResolvedAsOfField).GetString());
        }, allowWrites: true);

    [Fact]
    public Task TheServedDescriptions_SayExactOnlyOnEveryToolThatChangesState_AndCaseFoldingOnEveryRead() =>
        WithHostAsync(async (host, _, _) =>
        {
            var tools = (await StatementFilterCensus.ListToolSchemasAsync(host, bearerToken: StatementFilterCensus.LiveHostToken))
                .Where(tool => tool.InputSchema.TryGetProperty("properties", out var properties)
                               && properties.TryGetProperty("database_name", out _)
                               && properties.TryGetProperty("server_name", out _))
                .ToList();

            var problems = new List<string>();
            var changingState = new List<string>();
            foreach (var (name, schema) in tools)
            {
                var described = schema.GetProperty("properties").GetProperty("database_name").GetProperty("description").GetString()!;
                var write = DarlingMcpWritePolicy.IsWrite(name);
                if (write)
                {
                    changingState.Add(name);
                }

                if (!described.EndsWith(write ? McpDatabaseNameResolver.ExactOnlySentence : McpDatabaseNameResolver.MatchingSentence, StringComparison.Ordinal)
                    || described.Contains(write ? McpDatabaseNameResolver.MatchingSentence : McpDatabaseNameResolver.ExactOnlySentence, StringComparison.Ordinal))
                {
                    problems.Add($"{name} (changes state: {write}): {described}");
                }
            }

            Assert.True(problems.Count == 0, string.Join("\n", problems));
            Assert.Contains("kill_session", changingState);
            Assert.Contains("set_database_state_expected", changingState);
            Assert.True(changingState.Count >= 8, $"only {changingState.Count} served tools that change state take database_name and server_name.");
            Assert.True(tools.Count - changingState.Count >= 40, $"only {tools.Count - changingState.Count} served reads take database_name and server_name.");
        }, allowWrites: true);

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
            Assert.StartsWith(McpDatabaseNameResolver.NotSeenMessage(given), answer.GetProperty(McpDatabaseNameResolver.NoteField).GetString(), StringComparison.Ordinal);
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
            Assert.StartsWith(McpDatabaseNameResolver.NotSeenMessage("DroppedDb"), answer.GetProperty(McpDatabaseNameResolver.NoteField).GetString(), StringComparison.Ordinal);

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
            Assert.StartsWith(McpDatabaseNameResolver.NotSeenMessage("appdb_old"), unseen.GetProperty(McpDatabaseNameResolver.NoteField).GetString(), StringComparison.Ordinal);
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

            Assert.Equal(new[] { "SalesDB" }, (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, listedId, "SALESDB", now, ct))?.Names);
            Assert.Equal(
                new[] { "REPORTS", "Reports" },
                ((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, listedId, "reports", now, ct))?.Names)!.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Empty(((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, listedId, "salesdb_old", now, ct))?.Names)!);
            Assert.Null((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, ServerIdHelper.GetDeterministicHashCode(Unlisted), "SalesDB", now, ct))?.Names);

            /* A PostgreSQL target's list is read when the server has no SQL Server one. */
            var pgId = ServerIdHelper.GetDeterministicHashCode(PgTarget);
            Assert.Equal(new[] { "AppDb" }, (await DarlingMcpDatabaseNames.CandidatesAsync(postgres, pgId, "appdb", now, ct))?.Names);
            Assert.Equal(
                new[] { "Orders", "orders" },
                ((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, pgId, "ORDERS", now, ct))?.Names)!.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Empty(((await DarlingMcpDatabaseNames.CandidatesAsync(postgres, pgId, "appdb_old", now, ct))?.Names)!);

            /* A list older than the look-back is no list. */
            Assert.Null((await DarlingMcpDatabaseNames.CandidatesAsync(
                postgres, listedId, "SalesDB", now + DarlingMcpFindDatabaseTools.LookBack + TimeSpan.FromHours(1), ct))?.Names);
        });

    /// <summary>
    /// Every tool the live host serves that takes <c>database_name</c> and <c>server_name</c>, called with a differently cased name:
    /// each read's answer names the stored spelling, and each tool that changes state (the server-action tools are served on
    /// every host and gate themselves) is refused with it. The required arguments a tool has beyond those two are filled from its
    /// own schema, so the call reaches the tool; what a read then answers (rows, empty, a refusal of the filler) does not matter
    /// here.
    /// </summary>
    [Fact]
    public Task EveryServedToolThatTakesADatabaseName_NamesTheStoredSpellingItResolved_OrIsRefusedWithItWhenItChangesState() =>
        WithHostAsync(async (host, _, _) =>
        {
            var tools = (await StatementFilterCensus.ListToolSchemasAsync(host, bearerToken: StatementFilterCensus.LiveHostToken))
                .Where(tool => tool.InputSchema.TryGetProperty("properties", out var properties)
                               && properties.TryGetProperty("database_name", out _)
                               && properties.TryGetProperty("server_name", out _))
                .OrderBy(tool => tool.Name, StringComparer.Ordinal)
                .ToList();

            var problems = new List<string>();
            var refused = 0;
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
                if (DarlingMcpWritePolicy.IsWrite(name))
                {
                    refused++;
                    if (!isError || !JsonDocument.Parse(text).RootElement.GetProperty("message").GetString()!.StartsWith(McpDatabaseNameResolver.ExactRequiredMessage("salesdb", "SalesDB"), StringComparison.Ordinal))
                    {
                        problems.Add($"{name}: changes state and was not refused with the stored spelling (error {isError}) in: {(text.Length > 300 ? text[..300] : text)}");
                    }

                    continue;
                }

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
            Assert.True(tools.Count - refused >= 40, $"only {tools.Count - refused} served reads take database_name and server_name.");
            Assert.True(refused >= 7, $"only {refused} served tools that change state take database_name and server_name.");
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
