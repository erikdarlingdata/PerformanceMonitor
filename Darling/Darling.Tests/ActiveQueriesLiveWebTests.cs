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
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and a role with a per-run name,
   and touches nothing on the shared one, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The web's live Active Queries read (#6386): the route queues exactly one argument-less <c>fetch_active_queries</c> command for
/// the named server, the way the Viewer's Current Active Queries does, and returns the service's stored result; a read-only seat and a
/// store login that cannot execute the queue function are refused. The live facts run the store function as the managed <c>viewer</c>
/// role (taken from <see cref="DarlingManagedRoles.BuildProvisioningSql"/>), which holds no INSERT on the command queue.
/// </summary>
public sealed class ActiveQueriesLiveWebTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
    private const int ServerId = 61;

    // ------------------------------------------------------------------ route

    private sealed class FakeStore : IActiveQueriesLiveStore
    {
        public List<(int ServerId, string By)> Enqueued { get; } = [];

        public Exception? EnqueueFails { get; set; }

        public bool CanQueue { get; set; } = true;

        public SnapshotCommandState? State { get; set; }

        public int Reads { get; private set; }

        public Task<long> EnqueueAsync(int serverId, string requestedBy, CancellationToken cancellationToken)
        {
            if (EnqueueFails is not null)
            {
                throw EnqueueFails;
            }

            Enqueued.Add((serverId, requestedBy));
            return Task.FromResult(100L + Enqueued.Count);
        }

        public Task<SnapshotCommandState?> ReadStateAsync(long commandId, CancellationToken cancellationToken)
        {
            Reads++;
            return Task.FromResult(commandId == 101 ? State : null);
        }

        public Task<bool> CanQueueAsync(CancellationToken cancellationToken) => Task.FromResult(CanQueue);
    }

    private static async Task<WebApplication> StartAsync(FakeStore store, bool canEdit, ReadAdmission? admission = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var seat = new PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat(canEdit ? "ed" : "vi", canEdit);
        app.Use((context, next) =>
        {
            context.Items[PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat.HttpContextItemKey] = seat;
            return next(context);
        });
        DarlingWebEndpoints.MapActiveQueriesLive(
            app, store, NullLogger.Instance,
            (name, _) => Task.FromResult(name == "known" ? (ServerId, (string?)null) : (0, "No server matches.")), managedStore: true,
            readAdmission: admission);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }

    private static async Task<(HttpStatusCode Status, string Body)> PostAsync(HttpClient client, string body)
    {
        var ct = TestContext.Current.CancellationToken;
        using var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/api/active-queries/live", content, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    [Fact]
    public async Task TheRoute_Gives403ToAReadOnlySeat_AndQueuesExactlyOneReadForAnEditSeat()
    {
        var readOnly = new FakeStore();
        await using (var viewerApp = await StartAsync(readOnly, canEdit: false))
        {
            using var client = viewerApp.GetTestClient();
            Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(client, "{\"server\":\"known\"}")).Status);
            Assert.Empty(readOnly.Enqueued);

            using var available = JsonDocument.Parse(await client.GetStringAsync("/api/active-queries/live/available", TestContext.Current.CancellationToken));
            Assert.False(available.RootElement.GetProperty("can_queue").GetBoolean());
        }

        var store = new FakeStore();
        await using var app = await StartAsync(store, canEdit: true);
        using var editor = app.GetTestClient();
        var queued = await PostAsync(editor, "{\"server\":\"known\"}");

        Assert.Equal(HttpStatusCode.Accepted, queued.Status);
        Assert.Equal((ServerId, "ed"), Assert.Single(store.Enqueued));
        using var doc = JsonDocument.Parse(queued.Body);
        Assert.Equal("queued", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(101, doc.RootElement.GetProperty("command_id").GetInt64());

        var unknown = await PostAsync(editor, "{\"server\":\"nope\"}");
        Assert.Equal(HttpStatusCode.NotFound, unknown.Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await PostAsync(editor, "{}")).Status);
        Assert.Single(store.Enqueued);
    }

    [Fact]
    public async Task TheRoute_ReadsTheStoredResultRows_AndAnUnknownCommandIs404()
    {
        const string Result = "{\"success\":true,\"fetchedAtUtc\":\"2026-10-23T10:00:00Z\",\"rows\":[{\"SessionId\":55,\"QueryText\":\"select 1\",\"CpuTimeMs\":12}]}";
        var store = new FakeStore { State = new SnapshotCommandState("succeeded", "active queries fetched", Result) };
        await using var app = await StartAsync(store, canEdit: true);
        using var client = app.GetTestClient();
        var ct = TestContext.Current.CancellationToken;

        using var done = JsonDocument.Parse(await client.GetStringAsync("/api/active-queries/live/101", ct));
        Assert.True(done.RootElement.GetProperty("finished").GetBoolean());
        var row = done.RootElement.GetProperty("result").GetProperty("rows")[0];
        Assert.Equal(55, row.GetProperty("SessionId").GetInt32());
        Assert.Equal("select 1", row.GetProperty("QueryText").GetString());

        store.State = new SnapshotCommandState("pending", null, null);
        using var waiting = JsonDocument.Parse(await client.GetStringAsync("/api/active-queries/live/101", ct));
        Assert.False(waiting.RootElement.GetProperty("finished").GetBoolean());

        using var missing = await client.GetAsync("/api/active-queries/live/999", ct);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }

    [Theory]
    [InlineData("DL400", HttpStatusCode.BadRequest)]
    [InlineData("DL409", HttpStatusCode.Conflict)]
    [InlineData("DL404", HttpStatusCode.NotFound)]
    [InlineData("42883", HttpStatusCode.Forbidden)]
    [InlineData("42501", HttpStatusCode.Forbidden)]
    public async Task TheRoute_AnswersTheStoreFunctionsRefusals(string sqlState, HttpStatusCode expected)
    {
        var store = new FakeStore { EnqueueFails = new PostgresException("refused", "ERROR", "ERROR", sqlState) };
        await using var app = await StartAsync(store, canEdit: true);
        using var client = app.GetTestClient();

        var refused = await PostAsync(client, "{\"server\":\"known\"}");

        Assert.Equal(expected, refused.Status);
        using var doc = JsonDocument.Parse(refused.Body);
        Assert.False(string.IsNullOrWhiteSpace(doc.RootElement.GetProperty("error").GetString()));
    }

    [Fact]
    public async Task TheRoute_TooSoon_Is429_WithRetryAfter()
    {
        var next = DateTime.UtcNow.AddSeconds(8);
        var store = new FakeStore
        {
            EnqueueFails = new PostgresException("too soon", "ERROR", "ERROR", "DL429", hint: next.ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture)),
        };
        await using var app = await StartAsync(store, canEdit: true);
        using var client = app.GetTestClient();
        using var content = new StringContent("{\"server\":\"known\"}", Encoding.UTF8, "application/json");

        using var response = await client.PostAsync("/api/active-queries/live", content, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.True(response.Headers.RetryAfter?.Delta?.TotalSeconds is >= 1 and <= 10);
    }

    // ------------------------------------------------------------------ the store function, as the viewer role

    [Fact]
    public async Task TheViewerRole_QueuesOneArgumentlessCommand_ThroughTheFunction_AndCannotInsertDirectly()
    {
        await WithViewerAsync(async (owner, asRole, ct) =>
        {
            var store = new PostgresActiveQueriesLiveStore(asRole);
            Assert.True(await store.CanQueueAsync(ct));

            var id = await store.EnqueueAsync(ServerId, "web:tester", ct);

            await using var read = owner.CreateCommand(
                "SELECT command_type, status, target_server_id, requested_by, args_json IS NULL FROM config.config_command WHERE command_id = $1");
            read.Parameters.AddWithValue(id);
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal("fetch_active_queries", reader.GetString(0));
                Assert.Equal("pending", reader.GetString(1));
                Assert.Equal(ServerId, reader.GetInt32(2));
                Assert.Equal("web:tester", reader.GetString(3));
                Assert.True(reader.GetBoolean(4));
            }

            /* The result the service writes back is what the web reads, through the viewer's own column grant. */
            await ExecAsync(owner,
                $"UPDATE config.config_command SET status = 'succeeded', result_status = 'active queries fetched', result_json = '{{\"success\":true,\"rows\":[]}}'::jsonb WHERE command_id = {id}", ct);
            var state = await store.ReadStateAsync(id, ct);
            Assert.Equal("succeeded", state!.Status);
            Assert.Contains("\"rows\"", state.ResultJson, StringComparison.Ordinal);

            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var insert = asRole.CreateCommand(
                    "INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at) " +
                    "VALUES ('x', 'fetch_active_queries', 61, NULL, 'pending', now() AT TIME ZONE 'UTC')");
                await insert.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);
        });
    }

    [Fact]
    public async Task TheFunction_RefusesAnUnknownServer_ASecondReadWhileOneIsPending_AndOneWithin10Seconds()
    {
        await WithViewerAsync(async (owner, asRole, ct) =>
        {
            var store = new PostgresActiveQueriesLiveStore(asRole);

            Assert.Equal("DL404", (await Assert.ThrowsAsync<PostgresException>(() => store.EnqueueAsync(999, "web", ct))).SqlState);

            var first = await store.EnqueueAsync(ServerId, "web", ct);
            Assert.Equal("DL409", (await Assert.ThrowsAsync<PostgresException>(() => store.EnqueueAsync(ServerId, "web", ct))).SqlState);

            await ExecAsync(owner, $"UPDATE config.config_command SET status = 'succeeded' WHERE command_id = {first}", ct);
            var tooSoon = await Assert.ThrowsAsync<PostgresException>(() => store.EnqueueAsync(ServerId, "web", ct));
            Assert.Equal("DL429", tooSoon.SqlState);
            Assert.True(DateTime.TryParse(tooSoon.Hint, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var nextAllowed));
            Assert.InRange(nextAllowed, DateTime.UtcNow, DateTime.UtcNow.AddSeconds(11));

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(1L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));

            await ExecAsync(owner, $"UPDATE config.config_command SET created_at = created_at - interval '11 seconds' WHERE command_id = {first}", ct);
            Assert.NotEqual(first, await store.EnqueueAsync(ServerId, "web", ct));
        });
    }

    [Fact]
    public async Task ALoginWithoutTheFunction_CannotQueue_AndIsToldSo()
    {
        await WithViewerAsync(async (owner, asRole, ct) =>
        {
            await ExecAsync(owner, "DROP FUNCTION config.request_active_queries_live(integer, text)", ct);
            var store = new PostgresActiveQueriesLiveStore(asRole);

            Assert.False(await store.CanQueueAsync(ct));
            Assert.Equal("42883", (await Assert.ThrowsAsync<PostgresException>(() => store.EnqueueAsync(ServerId, "web", ct))).SqlState);
        });
    }

    /// <summary>The result read is admitted like the other web reads: with the only heavy slot taken, the wait passes the cap, the
    /// read is refused with the busy answer and its Retry-After, and the store is never asked.</summary>
    [Fact(Timeout = 60_000)]
    public async Task TheResultRoute_IsRefusedWithBusy_WhenAdmissionRefuses_AndDoesNotReadTheStore()
    {
        var route = DarlingWebEndpoints.ActiveQueriesLiveResultRoute;
        var clock = new TimerTimeProvider();
        var admission = new ReadAdmission(clock, ReadAdmission.DefaultWaitCap, null, 1);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long>
        {
            [(ReadSurface.Web, route, ReadWindowBand.None)] = 5_000,
        });
        var store = new FakeStore { State = new SnapshotCommandState("succeeded", "active queries fetched", "{\"rows\":[]}") };
        await using var app = await StartAsync(store, canEdit: true, admission);
        using var client = app.GetTestClient();
        var ct = TestContext.Current.CancellationToken;

        var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, ReadWindowBand.None, ct), ct);
        HttpResponseMessage response;
        try
        {
            var pending = client.GetAsync("/api/active-queries/live/101", ct);
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (admission.Stats().Queued < 1)
            {
                Assert.True(DateTime.UtcNow < deadline, "the result read never queued behind the held slot");
                await Task.Delay(10, ct);
            }

            clock.Advance(ReadAdmission.DefaultWaitCap);
            response = await pending.WaitAsync(TimeSpan.FromSeconds(20), ct);
        }
        finally
        {
            held.Dispose();
        }

        using (response)
        {
            Assert.Equal((HttpStatusCode)429, response.StatusCode);
            Assert.NotNull(response.Headers.RetryAfter);
        }

        Assert.Equal(0, store.Reads);
    }

    [Fact]
    public async Task TheFunction_RefusesARepeatableReadTransaction_WithDL400_AndQueuesNothing()
    {
        await WithViewerAsync(async (owner, asRole, ct) =>
        {
            await using (var connection = await asRole.OpenConnectionAsync(ct))
            {
                await using (var begin = new NpgsqlCommand("BEGIN ISOLATION LEVEL REPEATABLE READ", connection))
                {
                    await begin.ExecuteNonQueryAsync(ct);
                }

                await using (var snapshot = new NpgsqlCommand("SELECT 1", connection))
                {
                    await snapshot.ExecuteScalarAsync(ct);
                }

                await using var call = new NpgsqlCommand($"SELECT config.request_active_queries_live({ServerId}, 'web')", connection);
                var refused = await Assert.ThrowsAsync<PostgresException>(async () => await call.ExecuteScalarAsync(ct));
                Assert.Equal("DL400", refused.SqlState);
                Assert.Contains("READ COMMITTED", refused.MessageText, StringComparison.Ordinal);
            }

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));

            /* At READ COMMITTED, the default, the same request queues. */
            var outcome = await ActiveQueriesLiveRequests.RequestAsync(new PostgresActiveQueriesLiveStore(asRole), ServerId, "web", ct);
            Assert.True(outcome.Accepted);
        });
    }

    [Fact]
    public async Task TheFunction_RemovesOnlyThisServersFinishedReadsOlderThanAnHour_AndLeavesEverythingElse()
    {
        await WithViewerAsync(async (owner, asRole, ct) =>
        {
            const int OtherServer = 62;
            await ExecAsync(owner,
                $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({OtherServer}, 'aq-other', 'aq-other', TRUE)", ct);

            async Task<long> SeedAsync(string type, int server, string status, string ageOfFinish)
            {
                await using var insert = owner.CreateCommand(
                    "INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at, completed_at, result_json) " +
                    $"VALUES ('seed', '{type}', {server}, NULL, '{status}', (now() AT TIME ZONE 'UTC') - interval '{ageOfFinish}' - interval '5 seconds', " +
                    (status == "pending" ? "NULL" : $"(now() AT TIME ZONE 'UTC') - interval '{ageOfFinish}'") +
                    ", '{\"rows\":[]}'::jsonb) RETURNING command_id");
                return Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
            }

            async Task<HashSet<long>> RowsAsync()
            {
                await using var read = owner.CreateCommand("SELECT command_id FROM config.config_command");
                await using var reader = await read.ExecuteReaderAsync(ct);
                var ids = new HashSet<long>();
                while (await reader.ReadAsync(ct))
                {
                    ids.Add(reader.GetInt64(0));
                }

                return ids;
            }

            var oldSucceeded = await SeedAsync("fetch_active_queries", ServerId, "succeeded", "2 hours");
            var oldFailed = await SeedAsync("fetch_active_queries", ServerId, "failed", "90 minutes");
            var recentFinished = await SeedAsync("fetch_active_queries", ServerId, "succeeded", "30 minutes");
            var otherServerOld = await SeedAsync("fetch_active_queries", OtherServer, "succeeded", "3 hours");
            var otherTypeOld = await SeedAsync("snapshot_now", ServerId, "succeeded", "3 hours");
            var oldPending = await SeedAsync("fetch_active_queries", ServerId, "pending", "2 hours");
            var store = new PostgresActiveQueriesLiveStore(asRole);

            /* A pending read refuses the request before anything is removed. */
            Assert.Equal("DL409", (await Assert.ThrowsAsync<PostgresException>(() => store.EnqueueAsync(ServerId, "web", ct))).SqlState);
            Assert.Contains(oldSucceeded, await RowsAsync());

            /* Once that read finished a minute ago, the next request removes the two reads finished over an hour ago and nothing else. */
            await ExecAsync(owner,
                $"UPDATE config.config_command SET status = 'succeeded', completed_at = (now() AT TIME ZONE 'UTC') - interval '1 minute', created_at = (now() AT TIME ZONE 'UTC') - interval '2 minutes' WHERE command_id = {oldPending}", ct);
            var queued = await store.EnqueueAsync(ServerId, "web", ct);

            var left = await RowsAsync();
            Assert.DoesNotContain(oldSucceeded, left);
            Assert.DoesNotContain(oldFailed, left);
            Assert.Equivalent(new[] { recentFinished, otherServerOld, otherTypeOld, oldPending, queued }, left);
        });
    }

    private static async Task WithViewerAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Active Queries role tests (they mint their own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "aq_live_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner,
                $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerId}, 'aq-target', 'aq-target', TRUE)", ct);

            /* The function is created by the owner, as the managed batch does; the batch's own EXECUTE grant, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildRequestActiveQueriesLiveFunctionSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "viewer"))
            {
                await ExecAsync(owner, statement, ct);
            }

            var execute = ViewerGrantReplay.BatchStatements().Single(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.request_active_queries_live", StringComparison.Ordinal));
            var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
            Assert.Equal("viewer", execute[(at + 4)..].Trim());
            await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);

            await using var asRole = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            await body(owner, asRole, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
