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
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres and never touches another
   database; cleanup goes through LiveStoreCleanup. */

/// <summary>
/// #6214, V204: shared folders, personal views and an owner on the custom views. Behaviour against a real store: the migration
/// (fresh, as an upgrade over V203 rows, run again), each uniqueness scope, the web routes' identity rules (a personal view
/// needs a session with an owner key, only the owner changes or deletes it, the list hides other people's personal views while GET
/// by id does not, a personal view moves to a shared folder), ownership by owner key and never by display name, the folder-name
/// refusals, and the MCP tools' fields and refusals (MCP may delete a personal view and may not update one).
/// </summary>
public sealed class DarlingCustomViewFoldersLiveTests
{
    private const string Dashboard = "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"span\":1}]}";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static NpgsqlDataSource DataSourceFor(string connectionString) => NpgsqlDataSource.Create(
        new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = "collect,config,public" }.ConnectionString);

    private static async Task<ScratchPostgres> MigratedScratchAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return scratch;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    // ---- migration ----

    [Fact]
    public async Task V204_FreshStore_AndAnUpgradeOverV203Views_KeepEveryView_AsSharedTopLevel_AndRunsAgainUnchanged()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 migration facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Fresh: the new shape is in place and V31's store-wide UNIQUE (name) is gone, whatever its name was. */
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal((long)StorageVersion.SchemaVersion, await ScalarAsync(connection, "SELECT max(version)::bigint FROM darling_schema_version", ct));
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config.custom_views'::regclass AND contype = 'u'", ct));
            Assert.Equal(2L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_indexes WHERE schemaname = 'config' AND tablename = 'custom_views' AND indexname IN ('ux_custom_views_shared_name', 'ux_custom_views_personal_name')", ct));

            /* Upgrade: a store at V203 holds views under the old store-wide unique name, and one carries an updated_by. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 203, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 204", ct);
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config.custom_views'::regclass AND contype = 'u'", ct));
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'custom_views' AND column_name = 'created_by'", ct));
            await ExecAsync(connection,
                "INSERT INTO config.custom_views (name, definition, updated_by) VALUES ('old-one', '{\"panels\":[]}', 'someone'), ('old-two', '{\"panels\":[]}', NULL)", ct);

            await PgMigrations.MigrateAsync(connection, ct);
            await AssertUpgradedRowsAsync(connection, ct);

            /* Run again, the migration's own SQL included: nothing changes and nothing fails. */
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 204", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await AssertUpgradedRowsAsync(connection, ct);

            /* The upgraded rows now follow the new scopes: the same name may be taken in a folder. */
            await ExecAsync(connection, "INSERT INTO config.custom_views (name, definition, folder) VALUES ('old-one', '{\"panels\":[]}', 'DBA')", ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task AssertUpgradedRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT name, folder, personal, created_by, updated_by FROM config.custom_views WHERE name IN ('old-one', 'old-two') AND folder IS NULL ORDER BY name", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal("old-one", reader.GetString(0));
        Assert.True(reader.IsDBNull(1));
        Assert.False(reader.GetBoolean(2));
        Assert.True(reader.IsDBNull(3));
        Assert.Equal("someone", reader.GetString(4));
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal("old-two", reader.GetString(0));
        Assert.True(reader.IsDBNull(3));
        Assert.False(await reader.ReadAsync(ct));
    }

    [Fact]
    public async Task TheStore_RefusesABadFolderAndAPersonalViewWithNoOwner_EvenFromARawInsert()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 constraint facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            foreach (var bad in new[]
            {
                "INSERT INTO config.custom_views (name, definition, folder) VALUES ('a', '{}', ' padded')",
                "INSERT INTO config.custom_views (name, definition, folder) VALUES ('a', '{}', '')",
                "INSERT INTO config.custom_views (name, definition, folder) VALUES ('a', '{}', repeat('x', 101))",
                "INSERT INTO config.custom_views (name, definition, folder) VALUES ('a', '{}', E'tab\\there')",
                "INSERT INTO config.custom_views (name, definition, personal) VALUES ('a', '{}', true)",
                "INSERT INTO config.custom_views (name, definition, personal, created_by) VALUES ('a', '{}', true, 'named-but-keyless')",
            })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, bad, ct));
                Assert.Equal("23514", ex.SqlState);
            }

            await ExecAsync(connection, "INSERT INTO config.custom_views (name, definition, folder) VALUES ('ok', '{}', repeat('x', 100))", ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    // ---- the store: each uniqueness scope, folder normalization ----

    [Fact]
    public async Task Names_AreUniquePerScope_SharedPerFolder_PersonalPerOwner_AndTheConflictNamesItsScope()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 uniqueness facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        var store = new CustomViewStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
        var bodySucceeded = false;
        try
        {
            static CustomView Made(CustomViewResult result) => Assert.IsType<CustomViewResult.Ok>(result).View!;

            /* Shared, top level: a second one conflicts and says where. */
            Made(await store.CreateAsync("Waits", null, Dashboard, "web", cancellationToken: ct));
            var topDup = Assert.IsType<CustomViewResult.Conflict>(await store.CreateAsync("Waits", null, Dashboard, "web", cancellationToken: ct));
            Assert.Equal("A shared view named 'Waits' already exists at the top level.", topDup.Message);

            /* The same name in a folder is a different view; a second in that folder conflicts and names the folder. A padded
               folder name is the same folder. Names compare exactly: a different case is a different name. */
            Made(await store.CreateAsync("Waits", null, Dashboard, "web", folder: "DBA", cancellationToken: ct));
            var folderDup = Assert.IsType<CustomViewResult.Conflict>(
                await store.CreateAsync("Waits", null, Dashboard, "web", folder: "  DBA  ", cancellationToken: ct));
            Assert.Equal("A shared view named 'Waits' already exists in folder 'DBA'.", folderDup.Message);
            Made(await store.CreateAsync("waits", null, Dashboard, "web", folder: "DBA", cancellationToken: ct));

            /* A blank folder is the top level, not a folder named ''. */
            Assert.IsType<CustomViewResult.Conflict>(await store.CreateAsync("Waits", null, Dashboard, "web", folder: "   ", cancellationToken: ct));

            /* Personal: unique per owner; two people may each have the same name, and a personal view may share a shared view's name. */
            var alices = Made(await store.CreateAsync("Waits", null, Dashboard, "alice", personal: true, ownerKey: "key-alice", cancellationToken: ct));
            Made(await store.CreateAsync("Waits", null, Dashboard, "bob", personal: true, ownerKey: "key-bob", cancellationToken: ct));
            var personalDup = Assert.IsType<CustomViewResult.Conflict>(
                await store.CreateAsync("Waits", null, Dashboard, "alice", personal: true, ownerKey: "key-alice", cancellationToken: ct));
            Assert.Equal("You already have a personal view named 'Waits'.", personalDup.Message);
            Assert.True(alices.Personal);
            Assert.Equal("alice", alices.CreatedBy);

            /* A rename onto a taken name in the same scope conflicts with that scope's sentence too. */
            var other = Made(await store.CreateAsync("Other", null, Dashboard, "alice", personal: true, ownerKey: "key-alice", cancellationToken: ct));
            var renameDup = Assert.IsType<CustomViewResult.Conflict>(
                await store.UpdateAsync(other.Id, "Waits", null, Dashboard, 1, "alice", null, true, "key-alice", ct));
            Assert.Equal("You already have a personal view named 'Waits'.", renameDup.Message);

            /* A personal view with no owner key cannot be made, whatever name the session gives. */
            Assert.IsType<CustomViewResult.Invalid>(await store.CreateAsync("Nobody", null, Dashboard, null, personal: true, cancellationToken: ct));
            Assert.IsType<CustomViewResult.Invalid>(await store.CreateAsync("Keyless", null, Dashboard, "alice", personal: true, cancellationToken: ct));

            /* The folder rules the store holds for every writer. */
            Assert.IsType<CustomViewResult.Invalid>(await store.CreateAsync("Long", null, Dashboard, "web", folder: new string('x', 101), cancellationToken: ct));
            Assert.IsType<CustomViewResult.Invalid>(await store.CreateAsync("Ctl", null, Dashboard, "web", folder: "a\u0007b", cancellationToken: ct));

            /* Invisible formatting characters (a right-to-left override, a zero-width space, a tag character beyond the first plane)
               and the reserved name "My views" in any case are refused; a name that merely contains it is fine. */
            foreach (var bad in new[] { "a\u202Eb", "dev\u200Bs", "x\U000E0041y", "My views", "my VIEWS", "  MY views  ",
                /* the same name with a no-break space, an ideographic space, a variation selector or a hangul filler: it would
                   render as "My views" (or a lookalike of any folder), so it is refused too */
                "My\u00A0views", "my\u3000 VIEWS", "My\uFE0Fviews", "Dev\u3164s", "Dev\u034Fs" })
            {
                Assert.IsType<CustomViewResult.Invalid>(await store.CreateAsync("Bad", null, Dashboard, "web", folder: bad, cancellationToken: ct));
            }

            Assert.Equal("My views 2", Made(await store.CreateAsync("Fine", null, Dashboard, "web", folder: "My views 2", cancellationToken: ct)).Folder);
            Assert.Equal(new string('x', 100), Made(await store.CreateAsync("Max", null, Dashboard, "web", folder: new string('x', 100), cancellationToken: ct)).Folder);

            /* Whitespace of every kind is stored as plain single spaces with the ends trimmed, so two spellings of one name are one folder. */
            Assert.Equal("Ops Team", Made(await store.CreateAsync("Spaced", null, Dashboard, "web", folder: "  Ops \u00A0\t  Team\u3000 ", cancellationToken: ct)).Folder);
            Assert.Equal("My views 2", Made(await store.CreateAsync("Nbsp", null, Dashboard, "web", folder: "My\u00A0views\u00A0\u00A02", cancellationToken: ct)).Folder);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    // ---- the web routes ----

    private sealed class WebFixture : IAsyncDisposable
    {
        private readonly WebApplication _app;
        public HttpClient Client { get; }

        private WebFixture(WebApplication app, HttpClient client)
        {
            _app = app;
            Client = client;
        }

        /// <summary>Builds the real custom-view routes behind a stand-in for the auth middleware: the request's seat comes from the
        /// <c>X-Test-Subject</c> header (none = the shared-token seat, stored here) and the owner key from <c>X-Test-Key</c> (none = a seat with a
        /// name and no key, like a cookie from before owner keys existed).</summary>
        public static async Task<WebFixture> StartAsync(NpgsqlDataSource dataSource, CancellationToken ct)
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            var app = builder.Build();
            app.Use(async (context, next) =>
            {
                if (context.Request.Headers.TryGetValue("X-Test-Subject", out var subject) && subject.Count > 0)
                {
                    var key = context.Request.Headers.TryGetValue("X-Test-Key", out var k) && k.Count > 0 ? k[0] : null;
                    context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat(subject[0], CanEdit: true, key);
                }
                else
                {
                    /* A request with no seat reads as reads-only, so the shared-token seat is stored the way the real gate stores it. */
                    context.Items[DarlingWebSeat.HttpContextItemKey] = DarlingWebSeat.SharedToken;
                }

                await next();
            });
            DarlingWebEndpoints.MapCustomViews(app, dataSource, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
                new ReadLatencyRecorder(null, null), null);
            await app.StartAsync(ct);
            return new WebFixture(app, app.GetTestClient());
        }

        /// <summary>Sends as <paramref name="subject"/> (null = the shared token). A named seat gets the owner key
        /// <c>key-{subject}</c> unless <paramref name="key"/> names another or <paramref name="noKey"/> is set.</summary>
        public async Task<(HttpStatusCode Status, JsonElement Body)> SendAsync(
            HttpMethod method, string path, string? subject, object? body, CancellationToken ct, string? key = null, bool noKey = false)
        {
            using var request = new HttpRequestMessage(method, path);
            if (subject is not null)
            {
                request.Headers.Add("X-Test-Subject", subject);
                if (!noKey)
                {
                    request.Headers.Add("X-Test-Key", key ?? "key-" + subject);
                }
            }

            if (body is not null)
            {
                request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
            }
            else if (method != HttpMethod.Get)
            {
                request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
            }

            using var response = await Client.SendAsync(request, ct);
            var text = await response.Content.ReadAsStringAsync(ct);
            return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await _app.DisposeAsync();
        }
    }

    private static object ViewBody(string name, string? folder = null, bool? personal = null, int? version = null, bool folderGiven = false)
    {
        var body = new System.Collections.Generic.Dictionary<string, object?>
        {
            ["name"] = name,
            ["definition"] = JsonDocument.Parse(Dashboard).RootElement,
        };
        if (folder is not null || folderGiven)
        {
            body["folder"] = folder;
        }

        if (personal is { } p)
        {
            body["personal"] = p;
        }

        if (version is { } v)
        {
            body["version"] = v;
        }

        return body;
    }

    [Fact]
    public async Task Web_APersonalView_NeedsAnIdentity_IsHiddenFromTheListOfOthers_ButNotFromGetById_AndOnlyItsOwnerChangesOrDeletesIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 web facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        await using var web = await WebFixture.StartAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* The shared-token session has no identity: a personal view is a 400 with the plain sentence, and nothing is stored. */
            var (noIdentity, noIdentityBody) = await web.SendAsync(HttpMethod.Post, "/api/views", null, ViewBody("Mine", personal: true), ct);
            Assert.Equal(HttpStatusCode.BadRequest, noIdentity);
            Assert.Equal("A personal view needs a signed-in identity; this session has none.", noIdentityBody.GetProperty("error").GetString());
            var (_, emptyList) = await web.SendAsync(HttpMethod.Get, "/api/views", null, null, ct);
            Assert.Equal(0, emptyList.GetArrayLength());

            /* Alice makes a personal view and a shared one in a folder. The wire carries folder, personal and created_by. */
            var (created, mine) = await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("Mine", personal: true), ct);
            Assert.Equal(HttpStatusCode.Created, created);
            Assert.True(mine.GetProperty("personal").GetBoolean());
            Assert.Equal("alice", mine.GetProperty("created_by").GetString());
            Assert.True(mine.GetProperty("mine").GetBoolean());
            Assert.False(mine.TryGetProperty("owner_key", out _));
            Assert.Equal("alice", mine.GetProperty("updated_by").GetString());
            Assert.Equal(JsonValueKind.Null, mine.GetProperty("folder").ValueKind);
            var mineId = mine.GetProperty("id").GetInt64();
            var (sharedStatus, shared) = await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("Team", folder: " Devs "), ct);
            Assert.Equal(HttpStatusCode.Created, sharedStatus);
            Assert.Equal("Devs", shared.GetProperty("folder").GetString());
            Assert.False(shared.GetProperty("personal").GetBoolean());
            var sharedId = shared.GetProperty("id").GetInt64();

            /* The list: every shared view plus the caller's own personal ones. */
            static string[] Names(JsonElement list) => list.EnumerateArray().Select(v => v.GetProperty("name").GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToArray();
            Assert.Equal(new[] { "Mine", "Team" }, Names((await web.SendAsync(HttpMethod.Get, "/api/views", "alice", null, ct)).Body));
            Assert.Equal(new[] { "Team" }, Names((await web.SendAsync(HttpMethod.Get, "/api/views", "bob", null, ct)).Body));
            Assert.Equal(new[] { "Team" }, Names((await web.SendAsync(HttpMethod.Get, "/api/views", null, null, ct)).Body));
            var teamRow = (await web.SendAsync(HttpMethod.Get, "/api/views", "bob", null, ct)).Body.EnumerateArray().Single();
            Assert.Equal("Devs", teamRow.GetProperty("folder").GetString());
            Assert.False(teamRow.GetProperty("personal").GetBoolean());
            Assert.Equal("alice", teamRow.GetProperty("created_by").GetString());
            Assert.False(teamRow.GetProperty("mine").GetBoolean());
            Assert.False(teamRow.TryGetProperty("owner_key", out _));
            var aliceRows = (await web.SendAsync(HttpMethod.Get, "/api/views", "alice", null, ct)).Body.EnumerateArray().ToDictionary(v => v.GetProperty("name").GetString()!);
            Assert.True(aliceRows["Mine"].GetProperty("mine").GetBoolean());
            Assert.True(aliceRows["Team"].GetProperty("mine").GetBoolean());

            /* The page decides "My views", read-only and the personal checkbox from each view's `mine` and the session's `can_own`,
               never by comparing names. The session returns neither the subject nor the key. */
            var (_, aliceSession) = await web.SendAsync(HttpMethod.Get, "/api/session", "alice", null, ct);
            Assert.True(aliceSession.GetProperty("can_own").GetBoolean());
            Assert.False(aliceSession.TryGetProperty("subject", out _));
            Assert.False(aliceSession.TryGetProperty("owner_key", out _));
            var (_, tokenSession) = await web.SendAsync(HttpMethod.Get, "/api/session", null, null, ct);
            Assert.False(tokenSession.GetProperty("can_own").GetBoolean());
            Assert.True(tokenSession.GetProperty("can_edit").GetBoolean());

            /* GET by id still returns another person's personal view, so a link keeps working. */
            var (getStatus, got) = await web.SendAsync(HttpMethod.Get, $"/api/views/{mineId}", "bob", null, ct);
            Assert.Equal(HttpStatusCode.OK, getStatus);
            Assert.True(got.GetProperty("personal").GetBoolean());
            Assert.Equal("alice", got.GetProperty("created_by").GetString());
            Assert.False(got.GetProperty("mine").GetBoolean());
            Assert.False(got.TryGetProperty("owner_key", out _));

            /* Another person, and the shared-token session, can neither update nor delete it. */
            foreach (var intruder in new string?[] { "bob", null })
            {
                var (put, putBody) = await web.SendAsync(HttpMethod.Put, $"/api/views/{mineId}", intruder, ViewBody("Hijacked", version: 1), ct);
                Assert.Equal(HttpStatusCode.Forbidden, put);
                Assert.Equal("This is a personal view. Only its owner can change or delete it.", putBody.GetProperty("error").GetString());
                var (del, _) = await web.SendAsync(HttpMethod.Delete, $"/api/views/{mineId}", intruder, null, ct);
                Assert.Equal(HttpStatusCode.Forbidden, del);
            }

            Assert.Equal("Mine", (await web.SendAsync(HttpMethod.Get, $"/api/views/{mineId}", "alice", null, ct)).Body.GetProperty("name").GetString());

            /* A shared view keeps today's edit rules: anyone may change it. Omitted folder and personal keep the stored values. */
            var (sharedPut, sharedAfter) = await web.SendAsync(HttpMethod.Put, $"/api/views/{sharedId}", "bob", ViewBody("Team renamed", version: 1), ct);
            Assert.Equal(HttpStatusCode.OK, sharedPut);
            Assert.Equal("Devs", sharedAfter.GetProperty("folder").GetString());
            Assert.False(sharedAfter.GetProperty("personal").GetBoolean());
            Assert.Equal("alice", sharedAfter.GetProperty("created_by").GetString());
            Assert.Equal("bob", sharedAfter.GetProperty("updated_by").GetString());

            /* A folder given as null moves it to the top level. */
            var (toTop, topBody) = await web.SendAsync(HttpMethod.Put, $"/api/views/{sharedId}", "bob", ViewBody("Team renamed", version: 2, folderGiven: true), ct);
            Assert.Equal(HttpStatusCode.OK, toTop);
            Assert.Equal(JsonValueKind.Null, topBody.GetProperty("folder").ValueKind);

            /* Somebody else's shared view cannot be turned personal by another person. Its creator can. */
            var (stolen, stolenBody) = await web.SendAsync(HttpMethod.Put, $"/api/views/{sharedId}", "bob", ViewBody("Team renamed", personal: true, version: 3), ct);
            Assert.Equal(HttpStatusCode.Forbidden, stolen);
            Assert.Equal("Only the person who created a view can make it personal.", stolenBody.GetProperty("error").GetString());

            /* A bad folder or flag type is a 400. */
            Assert.Equal(HttpStatusCode.BadRequest, (await web.SendAsync(HttpMethod.Post, "/api/views", "alice",
                new { name = "x", definition = JsonDocument.Parse(Dashboard).RootElement, folder = 5 }, ct)).Status);
            Assert.Equal(HttpStatusCode.BadRequest, (await web.SendAsync(HttpMethod.Post, "/api/views", "alice",
                new { name = "x", definition = JsonDocument.Parse(Dashboard).RootElement, personal = "yes" }, ct)).Status);
            Assert.Equal(HttpStatusCode.BadRequest, (await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("x", folder: new string('f', 101)), ct)).Status);

            /* The owner deletes it. */
            Assert.Equal(HttpStatusCode.NoContent, (await web.SendAsync(HttpMethod.Delete, $"/api/views/{mineId}", "alice", null, ct)).Status);
            Assert.Equal(HttpStatusCode.NotFound, (await web.SendAsync(HttpMethod.Delete, $"/api/views/{mineId}", "alice", null, ct)).Status);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Web_APersonalView_MovesToAFolder_WithAnUpdate_AndTheNameMustBeFreeThere_AndASharedViewWithNoCreatorCanNeverBecomePersonal()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 move facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        await using var web = await WebFixture.StartAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            await web.SendAsync(HttpMethod.Post, "/api/views", "bob", ViewBody("Top queries", folder: "Devs"), ct);
            var (_, mine) = await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("Top queries", personal: true), ct);
            var mineId = mine.GetProperty("id").GetInt64();

            /* The same name is taken in Devs: the move is a 409 naming the folder, and the view stays personal. */
            var (clash, clashBody) = await web.SendAsync(HttpMethod.Put, $"/api/views/{mineId}", "alice", ViewBody("Top queries", folder: "Devs", personal: false, version: 1), ct);
            Assert.Equal(HttpStatusCode.Conflict, clash);
            Assert.Equal("A shared view named 'Top queries' already exists in folder 'Devs'.", clashBody.GetProperty("error").GetString());
            Assert.True((await web.SendAsync(HttpMethod.Get, $"/api/views/{mineId}", "alice", null, ct)).Body.GetProperty("personal").GetBoolean());

            /* Into a free folder it moves, keeps its creator, and is then listed for everyone. */
            var (moved, movedBody) = await web.SendAsync(HttpMethod.Put, $"/api/views/{mineId}", "alice", ViewBody("Top queries", folder: "DBA", personal: false, version: 1), ct);
            Assert.Equal(HttpStatusCode.OK, moved);
            Assert.False(movedBody.GetProperty("personal").GetBoolean());
            Assert.Equal("DBA", movedBody.GetProperty("folder").GetString());
            Assert.Equal("alice", movedBody.GetProperty("created_by").GetString());
            var (_, bobsList) = await web.SendAsync(HttpMethod.Get, "/api/views", "bob", null, ct);
            Assert.Equal(2, bobsList.GetArrayLength());

            /* Now shared, anyone may change it. */
            Assert.Equal(HttpStatusCode.OK, (await web.SendAsync(HttpMethod.Put, $"/api/views/{mineId}", "bob", ViewBody("Top queries 2", version: 2), ct)).Status);

            /* A view with no recorded creator (made before V204) can be made personal by nobody: that would let one person take a team's
               view out of everyone else's list. The refusal is a plain 403 and the view stays shared and listed for everyone. */
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                await ExecAsync(owner, "INSERT INTO config.custom_views (name, definition) VALUES ('legacy', '{\"panels\":[]}')", ct);
            }

            var legacy = (await web.SendAsync(HttpMethod.Get, "/api/views", "carol", null, ct)).Body.EnumerateArray().Single(v => v.GetProperty("name").GetString() == "legacy");
            var legacyId = legacy.GetProperty("id").GetInt64();
            Assert.Equal(JsonValueKind.Null, legacy.GetProperty("created_by").ValueKind);
            foreach (var caller in new[] { "carol", "bob" })
            {
                var (claimed, claimedBody) = await web.SendAsync(HttpMethod.Put, $"/api/views/{legacyId}", caller, ViewBody("legacy", personal: true, version: 1), ct);
                Assert.Equal(HttpStatusCode.Forbidden, claimed);
                Assert.Equal("This shared view has no recorded creator, so it can't be made personal. Save a copy as a personal view instead.", claimedBody.GetProperty("error").GetString());
            }

            var (stillShared, stillSharedBody) = await web.SendAsync(HttpMethod.Get, $"/api/views/{legacyId}", "bob", null, ct);
            Assert.Equal(HttpStatusCode.OK, stillShared);
            Assert.False(stillSharedBody.GetProperty("personal").GetBoolean());
            Assert.Equal(JsonValueKind.Null, stillSharedBody.GetProperty("created_by").ValueKind);
            Assert.Equal(1, stillSharedBody.GetProperty("version").GetInt32());
            Assert.Contains((await web.SendAsync(HttpMethod.Get, "/api/views", "bob", null, ct)).Body.EnumerateArray(), v => v.GetProperty("name").GetString() == "legacy");

            /* A legacy view is still an ordinary shared view otherwise, and a personal copy of it is a new view its saver owns. */
            Assert.Equal(HttpStatusCode.OK, (await web.SendAsync(HttpMethod.Put, $"/api/views/{legacyId}", "carol", ViewBody("legacy", version: 1), ct)).Status);
            var (copyStatus, copy) = await web.SendAsync(HttpMethod.Post, "/api/views", "carol", ViewBody("legacy", personal: true), ct);
            Assert.Equal(HttpStatusCode.Created, copyStatus);
            Assert.Equal("carol", copy.GetProperty("created_by").GetString());
            Assert.True(copy.GetProperty("mine").GetBoolean());
            Assert.Equal(HttpStatusCode.Forbidden, (await web.SendAsync(HttpMethod.Delete, $"/api/views/{copy.GetProperty("id").GetInt64()}", "bob", null, ct)).Status);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Web_TwoSessionsWithTheSameDisplayName_ButDifferentOwnerKeys_NeverSeeOrChangeEachOthersPersonalViews()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 owner-key facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        await using var web = await WebFixture.StartAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* Same display subject, two people (an identity provider reissued the name): each owns only what their own key made. */
            var (_, first) = await web.SendAsync(HttpMethod.Post, "/api/views", "sam@example.invalid", ViewBody("Private", personal: true), ct, key: "key-first");
            var firstId = first.GetProperty("id").GetInt64();
            var (_, firstShared) = await web.SendAsync(HttpMethod.Post, "/api/views", "sam@example.invalid", ViewBody("Team"), ct, key: "key-first");
            var firstSharedId = firstShared.GetProperty("id").GetInt64();

            /* The second session does not see the first one's personal view under `mine`, or at all in its list. */
            var (_, secondList) = await web.SendAsync(HttpMethod.Get, "/api/views", "sam@example.invalid", null, ct, key: "key-second");
            Assert.Equal(new[] { "Team" }, secondList.EnumerateArray().Select(v => v.GetProperty("name").GetString()!).ToArray());
            Assert.False(secondList.EnumerateArray().Single().GetProperty("mine").GetBoolean());
            var (_, firstList) = await web.SendAsync(HttpMethod.Get, "/api/views", "sam@example.invalid", null, ct, key: "key-first");
            Assert.All(firstList.EnumerateArray(), v => Assert.True(v.GetProperty("mine").GetBoolean()));
            Assert.Equal(2, firstList.GetArrayLength());
            Assert.False((await web.SendAsync(HttpMethod.Get, $"/api/views/{firstId}", "sam@example.invalid", null, ct, key: "key-second")).Body.GetProperty("mine").GetBoolean());

            /* The second session can neither change nor delete it, though the display names are identical. */
            Assert.Equal(HttpStatusCode.Forbidden, (await web.SendAsync(
                HttpMethod.Put, $"/api/views/{firstId}", "sam@example.invalid", ViewBody("Taken", version: 1), ct, key: "key-second")).Status);
            Assert.Equal(HttpStatusCode.Forbidden, (await web.SendAsync(
                HttpMethod.Delete, $"/api/views/{firstId}", "sam@example.invalid", null, ct, key: "key-second")).Status);

            /* Nor can it make the first one's shared view personal, which would take it out of the first person's list. */
            var (stolen, stolenBody) = await web.SendAsync(
                HttpMethod.Put, $"/api/views/{firstSharedId}", "sam@example.invalid", ViewBody("Team", personal: true, version: 1), ct, key: "key-second");
            Assert.Equal(HttpStatusCode.Forbidden, stolen);
            Assert.Equal("Only the person who created a view can make it personal.", stolenBody.GetProperty("error").GetString());

            /* Personal names are unique per owner key: the second person may use the same name. */
            Assert.Equal(HttpStatusCode.Created, (await web.SendAsync(
                HttpMethod.Post, "/api/views", "sam@example.invalid", ViewBody("Private", personal: true), ct, key: "key-second")).Status);

            /* The first person's own key still works after a rename of their display name. */
            Assert.Equal(HttpStatusCode.OK, (await web.SendAsync(
                HttpMethod.Put, $"/api/views/{firstId}", "sam.renamed@example.invalid", ViewBody("Private v2", version: 1), ct, key: "key-first")).Status);
            Assert.Equal(HttpStatusCode.OK, (await web.SendAsync(
                HttpMethod.Put, $"/api/views/{firstSharedId}", "sam.renamed@example.invalid", ViewBody("Team", personal: true, version: 1), ct, key: "key-first")).Status);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Web_ASessionNamedWebOrMcp_CannotMakeAViewTheTokenOrMcpMadePersonal_AndASessionWithNoOwnerKeyIsToldToSignInAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 sentinel facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        await using var web = await WebFixture.StartAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* One view from the shared token (created_by "web") and one from MCP (created_by "mcp"): neither carries an owner key. */
            var (_, tokenMade) = await web.SendAsync(HttpMethod.Post, "/api/views", null, ViewBody("Token made"), ct);
            Assert.Equal("web", tokenMade.GetProperty("created_by").GetString());
            var mcpMade = JsonDocument.Parse(await DarlingMcpCustomViewTools.CreateCustomView(dataSource, "Mcp made", Dashboard)).RootElement;
            Assert.Equal("mcp", mcpMade.GetProperty("created_by").GetString());

            /* A person whose display name is the same word still does not own them. */
            foreach (var (name, id, expected) in new[] { ("web", tokenMade.GetProperty("id").GetInt64(), "Token made"), ("mcp", mcpMade.GetProperty("id").GetInt64(), "Mcp made") })
            {
                var (status, body) = await web.SendAsync(HttpMethod.Put, $"/api/views/{id}", name, ViewBody(expected, personal: true, version: 1), ct, key: "key-" + name);
                Assert.Equal(HttpStatusCode.Forbidden, status);
                Assert.Equal("This shared view has no recorded creator, so it can't be made personal. Save a copy as a personal view instead.", body.GetProperty("error").GetString());
                var row = (await web.SendAsync(HttpMethod.Get, "/api/views", name, null, ct, key: "key-" + name)).Body.EnumerateArray().Single(v => v.GetProperty("name").GetString() == expected);
                Assert.False(row.GetProperty("mine").GetBoolean());
                Assert.False(row.GetProperty("personal").GetBoolean());
            }

            /* A session that names a person but has no key (a cookie from before owner keys existed) is told to sign in again,
               on create and on turning a view personal; saving a shared view still works. A session with no person says so. */
            var (oldCreate, oldCreateBody) = await web.SendAsync(HttpMethod.Post, "/api/views", "olga", ViewBody("Olga's", personal: true), ct, noKey: true);
            Assert.Equal(HttpStatusCode.BadRequest, oldCreate);
            Assert.Equal("Your sign-in is from an older version. Open /auth/logout, then sign in again, to save personal views.", oldCreateBody.GetProperty("error").GetString());
            var (oldShared, oldSharedBody) = await web.SendAsync(HttpMethod.Post, "/api/views", "olga", ViewBody("Olga's team"), ct, noKey: true);
            Assert.Equal(HttpStatusCode.Created, oldShared);
            Assert.False(oldSharedBody.GetProperty("mine").GetBoolean());
            var oldSharedId = oldSharedBody.GetProperty("id").GetInt64();
            var (oldPersonalize, oldPersonalizeBody) = await web.SendAsync(
                HttpMethod.Put, $"/api/views/{oldSharedId}", "olga", ViewBody("Olga's team", personal: true, version: 1), ct, noKey: true);
            Assert.Equal(HttpStatusCode.BadRequest, oldPersonalize);
            Assert.Equal("Your sign-in is from an older version. Open /auth/logout, then sign in again, to save personal views.", oldPersonalizeBody.GetProperty("error").GetString());
            Assert.False((await web.SendAsync(HttpMethod.Get, "/api/session", "olga", null, ct, noKey: true)).Body.GetProperty("can_own").GetBoolean());
            var (tokenStatus, tokenBody) = await web.SendAsync(HttpMethod.Post, "/api/views", null, ViewBody("Nobody's", personal: true), ct);
            Assert.Equal(HttpStatusCode.BadRequest, tokenStatus);
            Assert.Equal("A personal view needs a signed-in identity; this session has none.", tokenBody.GetProperty("error").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Web_AFolderNameWithInvisibleOrControlCharacters_OrTheReservedMyViews_IsARefused400_OnCreateAndOnUpdate()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 folder-name facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        await using var web = await WebFixture.StartAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            foreach (var bad in new[] { "Dev\u202Es", "Dev\u200Bs", "Dev\u0007s", "My views", "MY VIEWS", " my views " })
            {
                var (status, body) = await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("Folder test", folder: bad), ct);
                Assert.Equal(HttpStatusCode.BadRequest, status);
                Assert.False(string.IsNullOrWhiteSpace(body.GetProperty("error").GetString()));
            }

            var (_, ok) = await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("Folder test", folder: "Devs"), ct);
            var id = ok.GetProperty("id").GetInt64();
            foreach (var bad in new[] { "Dev\u202Es", "my Views" })
            {
                Assert.Equal(HttpStatusCode.BadRequest, (await web.SendAsync(
                    HttpMethod.Put, $"/api/views/{id}", "alice", ViewBody("Folder test", folder: bad, version: 1), ct)).Status);
            }

            Assert.Equal("Devs", (await web.SendAsync(HttpMethod.Get, $"/api/views/{id}", "alice", null, ct)).Body.GetProperty("folder").GetString());

            /* A name with odd spaces and runs of spaces is stored as plain single spaces. */
            var (spacedStatus, spaced) = await web.SendAsync(HttpMethod.Post, "/api/views", "alice", ViewBody("Spaced folder", folder: "Dev\u00A0\u00A0 Ops\t"), ct);
            Assert.Equal(HttpStatusCode.Created, spacedStatus);
            Assert.Equal("Dev Ops", spaced.GetProperty("folder").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    // ---- MCP ----

    [Fact]
    public async Task Mcp_ListsEveryViewWithItsFolderAndOwner_FiltersByFolder_PutsViewsInFolders_AndRefusesAPersonalView()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V204 MCP facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await MigratedScratchAsync(ct);
        await using var dataSource = DataSourceFor(scratch.ConnectionString);
        var store = new CustomViewStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
        var bodySucceeded = false;
        try
        {
            var personal = Assert.IsType<CustomViewResult.Ok>(
                await store.CreateAsync("Alice scratch", null, Dashboard, "alice", personal: true, ownerKey: "key-alice", cancellationToken: ct)).View!;

            /* create_custom_view takes a folder and stamps MCP as creator; the view is shared. */
            var created = JsonDocument.Parse(await DarlingMcpCustomViewTools.CreateCustomView(dataSource, "Fleet", Dashboard, "d", "Monitoring")).RootElement;
            Assert.Equal("Monitoring", created.GetProperty("folder").GetString());
            Assert.False(created.GetProperty("personal").GetBoolean());
            Assert.Equal("mcp", created.GetProperty("created_by").GetString());
            var topLevel = JsonDocument.Parse(await DarlingMcpCustomViewTools.CreateCustomView(dataSource, "Fleet", Dashboard)).RootElement;
            Assert.Equal(JsonValueKind.Null, topLevel.GetProperty("folder").ValueKind);

            /* A name collision names its folder. */
            var clash = JsonDocument.Parse(await DarlingMcpCustomViewTools.CreateCustomView(dataSource, "Fleet", Dashboard, folder: "Monitoring")).RootElement;
            Assert.Equal("conflict", clash.GetProperty("status").GetString());
            Assert.Contains("in folder 'Monitoring'", clash.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* The list carries folder, personal, created_by and updated_by, and includes another person's personal view, marked. */
            var all = JsonDocument.Parse(await DarlingMcpCustomViewTools.ListCustomViews(dataSource)).RootElement;
            Assert.Equal(3, all.GetArrayLength());
            var alices = all.EnumerateArray().Single(v => v.GetProperty("name").GetString() == "Alice scratch");
            Assert.True(alices.GetProperty("personal").GetBoolean());
            Assert.Equal("alice", alices.GetProperty("created_by").GetString());
            Assert.Equal("alice", alices.GetProperty("updated_by").GetString());
            Assert.Equal(JsonValueKind.Null, alices.GetProperty("folder").ValueKind);
            Assert.False(alices.TryGetProperty("owner_key", out _));
            Assert.False(JsonDocument.Parse(await DarlingMcpCustomViewTools.GetCustomView(dataSource, personal.Id)).RootElement.TryGetProperty("owner_key", out _));

            /* The folder filter: a folder, and "" for the top level. */
            var inFolder = JsonDocument.Parse(await DarlingMcpCustomViewTools.ListCustomViews(dataSource, "Monitoring")).RootElement;
            Assert.Equal("Fleet", inFolder.EnumerateArray().Single().GetProperty("name").GetString());
            var atTop = JsonDocument.Parse(await DarlingMcpCustomViewTools.ListCustomViews(dataSource, "")).RootElement;
            Assert.Equal(new[] { "Alice scratch", "Fleet" }, atTop.EnumerateArray().Select(v => v.GetProperty("name").GetString()!).ToArray());

            /* update_custom_view: an omitted folder keeps it, "" moves to the top level, text moves to that folder. */
            var id = created.GetProperty("id").GetInt64();
            var kept = JsonDocument.Parse(await DarlingMcpCustomViewTools.UpdateCustomView(dataSource, id, "Fleet kept", Dashboard, 1)).RootElement;
            Assert.Equal("Monitoring", kept.GetProperty("folder").GetString());
            var cleared = JsonDocument.Parse(await DarlingMcpCustomViewTools.UpdateCustomView(dataSource, id, "Fleet cleared", Dashboard, 2, folder: "")).RootElement;
            Assert.Equal(JsonValueKind.Null, cleared.GetProperty("folder").ValueKind);
            var moved = JsonDocument.Parse(await DarlingMcpCustomViewTools.UpdateCustomView(dataSource, id, "Fleet cleared", Dashboard, 3, folder: " Ops ")).RootElement;
            Assert.Equal("Ops", moved.GetProperty("folder").GetString());
            Assert.False(moved.GetProperty("personal").GetBoolean());

            /* A personal view cannot be updated over MCP, and the refusal says why; nothing changed. */
            var refusedUpdate = JsonDocument.Parse(await DarlingMcpCustomViewTools.UpdateCustomView(dataSource, personal.Id, "Taken", Dashboard, 1)).RootElement;
            Assert.Equal("refused", refusedUpdate.GetProperty("status").GetString());
            Assert.Contains("personal view", refusedUpdate.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Contains("MCP has no person identity", refusedUpdate.GetProperty("message").GetString(), StringComparison.Ordinal);
            var still = Assert.IsType<CustomViewResult.Ok>(await store.GetAsync(personal.Id, cancellationToken: ct)).View!;
            Assert.Equal("Alice scratch", still.Name);
            Assert.Equal(1, still.Version);

            /* Delete is the operator's cleanup for a person who has left: MCP deletes a personal view whoever owns it. */
            var deletedPersonal = JsonDocument.Parse(await DarlingMcpCustomViewTools.DeleteCustomView(dataSource, personal.Id)).RootElement;
            Assert.Equal("deleted", deletedPersonal.GetProperty("status").GetString());
            Assert.IsType<CustomViewResult.NotFound>(await store.GetAsync(personal.Id, cancellationToken: ct));

            /* A folder MCP may not name is an invalid, not a save. */
            var badFolder = JsonDocument.Parse(await DarlingMcpCustomViewTools.CreateCustomView(dataSource, "Sneaky", Dashboard, folder: "my Views")).RootElement;
            Assert.Equal("invalid", badFolder.GetProperty("status").GetString());

            /* A shared view still deletes. */
            Assert.Equal("deleted", JsonDocument.Parse(await DarlingMcpCustomViewTools.DeleteCustomView(dataSource, id)).RootElement.GetProperty("status").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
