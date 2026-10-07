/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5245: <c>GET /api/server-databases</c> through a real host over a live store. The list is the user databases the
/// store has collected for ONE server, from BOTH views the desktop picker reads (<c>v_database_config</c> and
/// <c>v_database_size_stats</c>), de-duplicated, with the system databases removed, and a name that is markup travels as
/// JSON data. Skipped without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
/* #1776: seeds and reads registry + database rows on the SHARED store, so it serializes with the other live classes
   that do. */
[Collection("live-postgres")]
public sealed class ServerDatabasesEndpointLiveTests
{
    private const int ServerId = 5_245_001;
    private const string ServerName = "srv5245-endpoint-live";
    private const int OtherServerId = 5_245_002;
    private const string OtherServerName = "srv5245-endpoint-other";

    /// <summary>A name that is markup: it must come back as a string value, never be interpreted.</summary>
    private const string MarkupName = "<img src=x onerror=alert(1)>";

    [Fact]
    public async Task TheRoute_AnswersTheUserDatabasesOfOneServer_FromBothViews_WithSystemDatabasesRemoved_AndMarkupAsData()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5245 server-databases route test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, ServerId, ServerName, ct);
            await InsertServerAsync(connection, OtherServerId, OtherServerName, ct);
            var now = DateTime.UtcNow;

            /* v_database_config only, v_database_size_stats only, and both (AppB); two system databases. */
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, "AppA", now, ct);
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, "AppB", now, ct);
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, "master", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, "AppB", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, "AppC", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, "tempdb", now, ct);
            await InsertAsync(connection, "database_size_stats", "collection_id", "collection_time", ServerId, ServerName, MarkupName, now, ct);
            /* Another server's database must not leak into this server's list. */
            await InsertAsync(connection, "database_config", "config_id", "capture_time", OtherServerId, OtherServerName, "OtherOnly", now, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();

            using var response = await client.GetAsync("/api/server-databases?server=" + Uri.EscapeDataString(ServerName), ct);
            var text = await response.Content.ReadAsStringAsync(ct);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

            using var doc = JsonDocument.Parse(text);
            Assert.Equal(ServerName, doc.RootElement.GetProperty("server").GetString());
            Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
            var names = doc.RootElement.GetProperty("databases").EnumerateArray().Select(e => e.GetString()!).ToArray();

            /* Both views, de-duplicated, system databases gone, the other server's database absent. The ORDER BY is
               the database collation's, so the set is compared in ordinal order. */
            Assert.Equal(
                new[] { "AppA", "AppB", "AppC", MarkupName }.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
                names.OrderBy(n => n, StringComparer.Ordinal).ToArray());

            /* Markup is data: the angle brackets are escaped in the body, so the raw text holds no tag at all. */
            Assert.DoesNotContain("<img", text, StringComparison.Ordinal);
            Assert.DoesNotContain("<", text, StringComparison.Ordinal);

            /* Only GET is mapped, and an unknown server is the shared 400 refusal, not an empty list. */
            using var post = await client.PostAsync("/api/server-databases", new System.Net.Http.StringContent("{}"), ct);
            Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
            using var unknown = await client.GetAsync("/api/server-databases?server=" + Uri.EscapeDataString("no-such-server-5245"), ct);
            Assert.Equal(HttpStatusCode.BadRequest, unknown.StatusCode);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheRoute_StopsAtItsCap_AndSaysTruncated_OnlyWhenOneMoreDatabaseExists()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5245 server-databases cap test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, ServerId, ServerName, ct);
            var cap = DarlingServerDatabasesEndpoint.MaxDatabases;
            Assert.Equal(5000, cap);

            /* Exactly cap databases: every name comes back and nothing is cut. */
            await InsertManyAsync(connection, ServerId, ServerName, 1, cap, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();
            var url = "/api/server-databases?server=" + Uri.EscapeDataString(ServerName);

            using (var exact = await client.GetAsync(url, ct))
            {
                Assert.Equal(HttpStatusCode.OK, exact.StatusCode);
                using var doc = JsonDocument.Parse(await exact.Content.ReadAsStringAsync(ct));
                Assert.Equal(cap, doc.RootElement.GetProperty("databases").GetArrayLength());
                Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
            }

            /* One more (cap + 1): the cap's worth of names, in name order, and truncated. The extra name is the last in
               name order, so it is the one left out. */
            await InsertManyAsync(connection, ServerId, ServerName, cap + 1, cap + 1, ct);
            using (var cut = await client.GetAsync(url, ct))
            {
                Assert.Equal(HttpStatusCode.OK, cut.StatusCode);
                using var doc = JsonDocument.Parse(await cut.Content.ReadAsStringAsync(ct));
                var names = doc.RootElement.GetProperty("databases").EnumerateArray().Select(e => e.GetString()!).ToArray();
                Assert.Equal(cap, names.Length);
                Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
                Assert.DoesNotContain(CapName(cap + 1), names);
                Assert.Contains(CapName(1), names);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#5314: with more databases than the route's cap, a database past the cap is absent from the plain list
    /// (<c>truncated</c> is true), and the same route with <c>search</c> finds it. Seeded at the real cap (5,001 numbered names
    /// plus one name that sorts after all of them), so the whole path runs at its real size.</summary>
    [Fact]
    public async Task TheRoute_Search_FindsADatabasePastTheCap_ThePlainListLeavesOut()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5314 server-databases search test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, ServerId, ServerName, ct);
            var cap = DarlingServerDatabasesEndpoint.MaxDatabases;
            await InsertManyAsync(connection, ServerId, ServerName, 1, cap + 1, ct);
            const string Past = "zz_past_the_cap_5314";
            await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, Past, DateTime.UtcNow, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();
            var url = "/api/server-databases?server=" + Uri.EscapeDataString(ServerName);

            var (plainNames, plainCut) = await GetAsync(client, url, ct);
            Assert.Equal(cap, plainNames.Length);
            Assert.True(plainCut);
            Assert.DoesNotContain(Past, plainNames);

            // A blank search reads as no search: the same cut list.
            var (blankNames, blankCut) = await GetAsync(client, url + "&search=%20%20", ct);
            Assert.Equal(cap, blankNames.Length);
            Assert.True(blankCut);

            // The search reaches past the cap, case-insensitively, and the answer is whole (nothing more to say).
            var (found, foundCut) = await GetAsync(client, url + "&search=" + Uri.EscapeDataString("PAST_THE"), ct);
            Assert.Equal(new[] { Past }, found);
            Assert.False(foundCut);

            // A search that matches more than the cap is cut at the cap, with the flag.
            var (many, manyCut) = await GetAsync(client, url + "&search=cap5245_", ct);
            Assert.Equal(cap, many.Length);
            Assert.True(manyCut);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#5314: <c>%</c>, <c>_</c> and the escape character in a search match only themselves.</summary>
    [Fact]
    public async Task TheRoute_Search_MatchesPercentUnderscoreAndBackslashOnlyAsThemselves()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5314 server-databases wildcard test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, ServerId, ServerName, ct);
            var now = DateTime.UtcNow;
            foreach (var name in new[] { "pct%name", "pctXname", "und_name", "undXname", "back\\slash", "backXslash", "Plain" })
            {
                await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, name, now, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();
            var url = "/api/server-databases?server=" + Uri.EscapeDataString(ServerName) + "&search=";

            Assert.Equal(new[] { "pct%name" }, (await GetAsync(client, url + Uri.EscapeDataString("pct%"), ct)).Names);
            Assert.Equal(new[] { "und_name" }, (await GetAsync(client, url + Uri.EscapeDataString("und_"), ct)).Names);
            Assert.Equal(new[] { "back\\slash" }, (await GetAsync(client, url + Uri.EscapeDataString("back\\"), ct)).Names);
            // A lone wildcard is a character to find, not "everything": no name holds a bare percent sign before "X".
            Assert.Empty((await GetAsync(client, url + Uri.EscapeDataString("%X"), ct)).Names);
            Assert.Equal(new[] { "und_name" }, (await GetAsync(client, url + Uri.EscapeDataString("UND_NAME"), ct)).Names);
            // Without a search every name comes back.
            Assert.Equal(7, (await GetAsync(client, "/api/server-databases?server=" + Uri.EscapeDataString(ServerName), ct)).Names.Length);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#5314: the route's row counts and its truncated flag, asserted over a handful of rows through a small cap,
    /// so they fail on their own and not only through the constant (an off-by-one in the extra-row probe, or a search that
    /// limits after instead of before, changes a count here).</summary>
    [Fact]
    public async Task TheRoute_RowCountsAndTruncatedFlag_HoldAtASmallCap_WithAndWithoutSearch()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5314 server-databases small-cap test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, ServerId, ServerName, ct);
            var now = DateTime.UtcNow;
            // Five names: three "alpha" ones, then "beta1" and "beta2".
            foreach (var name in new[] { "alpha1", "alpha2", "alpha3", "beta1", "beta2" })
            {
                await InsertAsync(connection, "database_config", "config_id", "capture_time", ServerId, ServerName, name, now, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger, 3);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();
            var url = "/api/server-databases?server=" + Uri.EscapeDataString(ServerName);

            // Five names, a cap of three: the first three by name, cut.
            var plain = await GetAsync(client, url, ct);
            Assert.Equal(new[] { "alpha1", "alpha2", "alpha3" }, plain.Names);
            Assert.True(plain.Truncated);

            // The search narrows BEFORE the limit: both "beta" names come back (they are past the cap in the plain list), not cut.
            var beta = await GetAsync(client, url + "&search=BETA", ct);
            Assert.Equal(new[] { "beta1", "beta2" }, beta.Names);
            Assert.False(beta.Truncated);

            // Exactly the cap's worth of matches is not cut; one more is.
            var alpha = await GetAsync(client, url + "&search=alpha", ct);
            Assert.Equal(new[] { "alpha1", "alpha2", "alpha3" }, alpha.Names);
            Assert.False(alpha.Truncated);
            var a = await GetAsync(client, url + "&search=a", ct);
            Assert.Equal(new[] { "alpha1", "alpha2", "alpha3" }, a.Names);
            Assert.True(a.Truncated);

            // No match is an empty list, not an error.
            var none = await GetAsync(client, url + "&search=nothing-matches", ct);
            Assert.Empty(none.Names);
            Assert.False(none.Truncated);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#5314: a NUL, a repeated <c>search</c> key and a value over 128 characters are refused with the route's
    /// invalid envelope (a 400), before the store is read, and the body never holds the value sent.</summary>
    [Fact]
    public async Task TheRoute_Search_RefusesANul_ARepeatedKey_AndAnOverLongValue_WithoutEchoingThem()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5314 server-databases refusal test.");

        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var app = builder.Build();
        DarlingServerDatabasesEndpoint.Map(app, postgres, app.Logger);
        await app.StartAsync(ct);
        using var client = app.GetTestClient();
        var url = "/api/server-databases?server=" + Uri.EscapeDataString(ServerName) + "&search=";

        foreach (var (query, echoed) in new[]
        {
            ("ab%00cd", "cd"),
            ("first&search=second", "second"),
            (new string('x', DarlingServerDatabasesEndpoint.MaxSearchLength + 1), "xxxxxxxx"),
        })
        {
            using var response = await client.GetAsync(url + query, ct);
            var text = await response.Content.ReadAsStringAsync(ct);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            using var doc = JsonDocument.Parse(text);
            Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
            Assert.Equal("search", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.DoesNotContain(echoed, text, StringComparison.Ordinal);
        }
    }

    private static async Task<(string[] Names, bool Truncated)> GetAsync(
        System.Net.Http.HttpClient client, string url, CancellationToken ct)
    {
        using var response = await client.GetAsync(url, ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return (
            doc.RootElement.GetProperty("databases").EnumerateArray().Select(e => e.GetString()!).ToArray(),
            doc.RootElement.GetProperty("truncated").GetBoolean());
    }

    /// <summary>The seeded name for number <paramref name="n"/>; the zero padding keeps name order equal to number order
    /// under any collation.</summary>
    private static string CapName(int n) => "cap5245_" + n.ToString("D5", System.Globalization.CultureInfo.InvariantCulture);

    private static async Task InsertManyAsync(
        NpgsqlConnection connection, int serverId, string serverName, int first, int last, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name) " +
            "SELECT $1 + g, $2, $3, $4, 'cap5245_' || lpad(g::text, 5, '0') FROM generate_series($5::int, $6::int) AS g",
            connection);
        command.Parameters.AddWithValue(5_245_000_000_000L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(first);
        command.Parameters.AddWithValue(last);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled) VALUES ($1, $2, $3, TRUE)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, string table, string idColumn, string timeColumn,
        int serverId, string serverName, string databaseName, DateTime whenUtc, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            $"INSERT INTO {table} ({idColumn}, {timeColumn}, server_id, server_name, database_name) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(whenUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(databaseName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "database_config", "database_size_stats", "servers" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = ANY($1)", connection);
            command.Parameters.AddWithValue(new[] { ServerId, OtherServerId });
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}

/// <summary>
/// #5245 L7: the inventory SQL deduplicates inside EACH arm. Over 500 databases, hourly for 90 days (2.2 million
/// <c>v_database_size_stats</c> rows) the bare <c>UNION</c> shape sorted every row and took a 4.4 s median; hashing each
/// arm down first took 0.26 s with the same rows. A pin, because the shape is invisible in the result.
/// </summary>
public sealed class CollectedDatabasesSqlTests
{
    [Fact]
    public void NamesSql_DeduplicatesInsideEachArm_SoTheUnionSortsNamesNotRows()
    {
        var sql = string.Join(" ", CollectedDatabases.NamesSql.Split(['\r', '\n', ' '], StringSplitOptions.RemoveEmptyEntries));

        Assert.Contains("SELECT DISTINCT database_name FROM v_database_config WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT database_name FROM v_database_size_stats WHERE server_id = $1", sql, StringComparison.Ordinal);
    }

    /// <summary>#5314: the search read is the shared list narrowed by one <c>ILIKE</c> against $3 with a backslash escape, ahead of
    /// the ORDER BY and the LIMIT, so the limit counts matches; the pattern builder escapes the three characters that mean
    /// something to LIKE.</summary>
    [Fact]
    public void NamesSearchLimitedSql_NarrowsBeforeTheLimit_WithOneEscapedPatternParameter()
    {
        var sql = CollectedDatabases.NamesSearchLimitedSql;
        var like = sql.IndexOf("database_name ILIKE $3 ESCAPE '\\'", StringComparison.Ordinal);
        var order = sql.IndexOf("ORDER BY database_name", StringComparison.Ordinal);
        var limit = sql.IndexOf("LIMIT $2", StringComparison.Ordinal);
        Assert.True(like > 0 && like < order && order < limit, sql);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);
        Assert.StartsWith(CollectedDatabases.NamesSql[..CollectedDatabases.NamesSql.IndexOf("\nORDER BY", StringComparison.Ordinal)], sql, StringComparison.Ordinal);

        Assert.Equal("%a\\%b\\_c\\\\d%", CollectedDatabases.SearchPattern("a%b_c\\d"));
        Assert.Equal("%plain%", CollectedDatabases.SearchPattern("plain"));
    }

    /// <summary>#5314: the search value's binding rules, pure. Blank is no search; a NUL, a second key and a value over 128
    /// characters are refused, and the refusal sentence never carries what was sent.</summary>
    [Fact]
    public void TryBindSearch_ReadsBlankAsNoSearch_AndRefusesNulRepeatsAndOverLongValues()
    {
        static (bool Ok, string? Search, string? Refusal) Bind(string query)
        {
            var ok = DarlingServerDatabasesEndpoint.TryBindSearch(
                new Microsoft.AspNetCore.Http.QueryCollection(Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(query)),
                out var search, out var refusal);
            return (ok, search, refusal);
        }

        Assert.Equal((true, null, null), Bind(""));
        Assert.Equal((true, null, null), Bind("?search="));
        Assert.Equal((true, null, null), Bind("?search=%20%09"));
        Assert.Equal((true, " ab ", null), Bind("?search=%20ab%20"));
        Assert.Equal((true, new string('x', 128), null), Bind("?search=" + new string('x', 128)));

        foreach (var bad in new[] { "?search=a%00b", "?search=one&search=two", "?search=" + new string('y', 129) })
        {
            var (ok, search, refusal) = Bind(bad);
            Assert.False(ok);
            Assert.Null(search);
            Assert.Contains("\"status\":\"invalid\"", refusal, StringComparison.Ordinal);
            Assert.DoesNotContain("two", refusal, StringComparison.Ordinal);
            Assert.DoesNotContain("yyyy", refusal, StringComparison.Ordinal);
        }
    }

    /// <summary>The web route reads <see cref="CollectedDatabases.NamesLimitedSql"/>: the shared list plus one trailing LIMIT.
    /// The desktop Excluded Databases picker keeps the unlimited <see cref="CollectedDatabases.NamesSql"/>, so its list
    /// is every collected name as before.</summary>
    [Fact]
    public void NamesLimitedSql_IsTheSharedListPlusALimit_AndTheDesktopPickerStaysUnlimited()
    {
        Assert.Equal(CollectedDatabases.NamesSql + "\nLIMIT $2", CollectedDatabases.NamesLimitedSql);
        Assert.DoesNotContain("LIMIT", CollectedDatabases.NamesSql, StringComparison.Ordinal);

        var desktop = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.ExcludedDatabases.cs");
        Assert.Contains("CollectedDatabases.NamesSql", desktop, StringComparison.Ordinal);
        Assert.DoesNotContain("NamesLimitedSql", desktop, StringComparison.Ordinal);

        var route = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingServerDatabasesEndpoint.cs");
        Assert.Contains("CollectedDatabases.NamesLimitedSql", route, StringComparison.Ordinal);
        Assert.Contains("CollectedDatabases.NamesSearchLimitedSql", route, StringComparison.Ordinal);
    }
}