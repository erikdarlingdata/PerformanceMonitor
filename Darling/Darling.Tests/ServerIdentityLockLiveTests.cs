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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one;
   the advisory lock it contends on is scoped to that database. */

/// <summary>
/// Every write that gives a monitored server an address takes ONE store lock and re-checks the address under it
/// (#5240, review finding 6). <c>server_id</c> is the hash of the storage key, an edit keeps its row's old id, and
/// the table has no unique index on the key, so without the lock an <c>add_servers</c> INSERT and an
/// <c>edit_server</c> UPDATE (or two edits) can each find an address free during their connection probe and each
/// take it: two definitions with one storage key, both monitored.
///
/// <para><b>How the race is made to happen.</b> Both callers are parked inside their probe, which both reach after
/// their first read of the table showed the address free. The test then takes the lock from a second connection and
/// lets the probes finish, so both callers reach their write with the address still free in the table. With the
/// lock in both write paths, both queue behind the held lock and the winner's commit is what the loser re-checks
/// against. Without it, both writes land: the assertions on the table fail, and the queue assertion with them.</para>
///
/// <para><b>The desktop viewer's writes (#5240, PR 2).</b> The viewer's add (<c>AddMonitoredServerAsync</c>) and edit
/// (<c>UpsertMonitoredServerAsync</c>) take the same lock and re-read the addresses under it. The viewer has no probe
/// to park in, so its facts make the race the other way round: the lock is taken from a second connection FIRST, the
/// two callers are started one at a time (each waits until the one before it is queued, so the lock is granted in call
/// order), and the lock is released once both are queued. The caller that queued first wins; the second's re-check
/// runs against the first's committed write and must refuse. Each viewer fact runs in both orders, so the viewer's
/// re-check is what refuses in one of them. A viewer write that took no lock would not queue (the queue assertion
/// fails); one that did not re-check would write over the other caller (the answers and the table assertions fail).
/// The startup seed's INSERT loop takes the lock too, and skips an address that was claimed while it waited.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ServerIdentityLockLiveTests
{
    private const string AddressK = "race-k.example.test";

    private static readonly ConnectionProbeResult Reached = new(true, 15, 3, "Enterprise", false, false, false, true, null);

    /// <summary>The probe both callers share: it counts arrivals and waits for <see cref="Release"/>.</summary>
    private sealed class ParkedProbe
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _arrived;

        public int Arrived => Volatile.Read(ref _arrived);

        public Edit.ServerProbe Probe => async (_, probeToken) =>
        {
            Interlocked.Increment(ref _arrived);
            await _release.Task.WaitAsync(probeToken);
            return Reached;
        };

        public void Release() => _release.TrySetResult();
    }

    private sealed record Rig(ScratchPostgres Scratch, NpgsqlDataSource Owner, string OwnerString) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private sealed record Race(string FirstAnswer, string SecondAnswer, bool BothQueuedOnTheLock, long LocksLeftAfterwards);

    private static async Task<Rig> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server identity lock tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(ownerString);
        await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        return new Rig(scratch, owner, ownerString);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static Task SeedServerAsync(NpgsqlDataSource owner, int id, string name, string host, CancellationToken ct) =>
        ExecAsync(owner, $@"INSERT INTO config_monitored_servers
            (server_id, name, host, auth, username, encrypted_password, excluded_databases, capture_plans, is_enabled,
             alert_delivery_mode_override, plan_force_bot_enabled, remediation_username, remediation_encrypted_password, monthly_cost_usd)
            VALUES ({id}, '{name}', '{host}', 'integrated', NULL, NULL,
                    ARRAY['tempdb','model'], TRUE, FALSE, 'PerEvent', TRUE, 'rem-user', 'rem-blob', 7)", ct);

    /// <summary>The storage key of every definition, mapped from the same five columns the product reads (a NULL
    /// engine or port is the default one), in <c>server_id</c> order.</summary>
    private static async Task<List<string>> StorageKeysAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        var keys = new List<string>();
        await using var command = owner.CreateCommand(
            "SELECT host, database, read_only_intent, engine, port FROM config_monitored_servers ORDER BY server_id");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add(ServerIdHelper.BuildStorageName(
                reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), !reader.IsDBNull(2) && reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetString(3), reader.IsDBNull(4) ? 0 : reader.GetInt32(4)));
        }

        return keys;
    }

    /// <summary>Waiters (<c>granted = false</c>) or holders (<c>true</c>) of advisory locks in THIS database.</summary>
    private static Task<long> AdvisoryLocksAsync(NpgsqlDataSource owner, bool granted, CancellationToken ct) =>
        CountAsync(owner, @"SELECT count(*) FROM pg_locks
            WHERE locktype = 'advisory' AND granted = " + (granted ? "true" : "false") + @"
              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())", ct);

    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (await condition())
            {
                return true;
            }

            if (DateTime.UtcNow >= deadline)
            {
                return false;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
        }
    }

    private static JsonNode Parse(string answer) => JsonNode.Parse(answer)!;

    private static string AddJson(string host) => "[{\"host\":\"" + host + "\"}]";

    private static string AddStatusOf(string answer) => Parse(answer)["results"]![0]!["status"]!.GetValue<string>();

    /// <summary>
    /// Parks both callers in their probe, takes the identity lock from a second connection, lets the probes finish,
    /// waits until both callers are queued on the lock (or one of them is already done, which is what a write path
    /// without the lock looks like), then releases the lock and returns both answers.
    /// </summary>
    private static async Task<Race> RaceAsync(
        Rig rig, Func<Edit.ServerProbe, Task<string>> first, Func<Edit.ServerProbe, Task<string>> second, CancellationToken ct)
    {
        var parked = new ParkedProbe();
        var firstCall = Task.Run(() => first(parked.Probe), ct);
        var secondCall = Task.Run(() => second(parked.Probe), ct);

        var holder = new NpgsqlConnection(rig.OwnerString);
        NpgsqlTransaction? held = null;
        var bodySucceeded = false;
        try
        {
            Assert.True(
                await WaitUntilAsync(() => Task.FromResult(parked.Arrived == 2), TimeSpan.FromSeconds(30), ct),
                $"Both callers should reach their probe (the address read as free for each); {parked.Arrived} did.");

            await holder.OpenAsync(ct);
            held = await holder.BeginTransactionAsync(ct);
            await using (var take = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtext('config_monitored_servers.identity'))", holder, held))
            {
                await take.ExecuteNonQueryAsync(ct);
            }

            parked.Release();
            var bothQueued = false;
            await WaitUntilAsync(async () =>
            {
                if (await AdvisoryLocksAsync(rig.Owner, false, ct) >= 2)
                {
                    bothQueued = true;
                    return true;
                }

                return firstCall.IsCompleted || secondCall.IsCompleted;
            }, TimeSpan.FromSeconds(30), ct);

            await held.RollbackAsync(ct);
            var answers = await Task.WhenAll(firstCall, secondCall).WaitAsync(TimeSpan.FromSeconds(60), ct);
            var left = await AdvisoryLocksAsync(rig.Owner, true, ct) + await AdvisoryLocksAsync(rig.Owner, false, ct);
            bodySucceeded = true;
            return new Race(answers[0], answers[1], bothQueued, left);
        }
        finally
        {
            parked.Release();

            /* The holder's own rollback: only the session holding the lock can end its transaction. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                if (held is not null)
                {
                    await held.DisposeAsync();
                }

                await holder.DisposeAsync();
            });
        }
    }

    [Fact]
    public async Task AnAddAndAnEditThatClaimOneAddress_QueueOnTheLock_AndExactlyOneRowEndsWithIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await SeedServerAsync(rig.Owner, 7301, "race-x", "race-x.example.test", ct);

        var race = await RaceAsync(rig,
            probe => Edit.AddServersAsync(rig.Owner, AddJson(AddressK), probe, ct),
            probe => Edit.EditServerByNameAsync(rig.Owner, "race-x", "{\"host\":\"" + AddressK + "\"}", probe, true, null, ct),
            ct);

        var addStatus = AddStatusOf(race.FirstAnswer);
        var edit = Parse(race.SecondAnswer);
        var editStatus = edit["status"]!.GetValue<string>();
        var addWon = addStatus == "added" && editStatus == "collides";
        var editWon = addStatus == "duplicate" && editStatus == "updated";
        Assert.True(addWon ^ editWon, $"Exactly one caller should win. add: {addStatus}, edit: {editStatus}.");
        if (addWon)
        {
            Assert.Equal("occupied", edit["reason"]!.GetValue<string>());
        }
        else
        {
            Assert.Contains("claimed this address", Parse(race.FirstAnswer)["results"]![0]!["detail"]!.GetValue<string>(), StringComparison.Ordinal);
        }

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(addWon ? 2 : 1, keys.Count);
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both the add and the edit should have queued behind the held identity lock.");
    }

    [Fact]
    public async Task TwoEditsThatMoveTwoServersOntoOneAddress_QueueOnTheLock_AndExactlyOneRowEndsWithIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await SeedServerAsync(rig.Owner, 7311, "race-x", "race-x.example.test", ct);
        await SeedServerAsync(rig.Owner, 7312, "race-y", "race-y.example.test", ct);

        var race = await RaceAsync(rig,
            probe => Edit.EditServerByNameAsync(rig.Owner, "race-x", "{\"host\":\"" + AddressK + "\"}", probe, true, null, ct),
            probe => Edit.EditServerByNameAsync(rig.Owner, "race-y", "{\"host\":\"" + AddressK + "\"}", probe, true, null, ct),
            ct);

        var statuses = new[] { Parse(race.FirstAnswer), Parse(race.SecondAnswer) }.Select(a => a["status"]!.GetValue<string>()).Order().ToArray();
        Assert.Equal(new[] { "collides", "updated" }, statuses);

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(2, keys.Count);
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both edits should have queued behind the held identity lock.");
    }

    [Fact]
    public async Task AnAddAndAnEditOnDifferentAddresses_BothSucceed_AfterQueueingOnTheLock()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await SeedServerAsync(rig.Owner, 7321, "race-x", "race-x.example.test", ct);

        var race = await RaceAsync(rig,
            probe => Edit.AddServersAsync(rig.Owner, AddJson("race-added.example.test"), probe, ct),
            probe => Edit.EditServerByNameAsync(rig.Owner, "race-x", "{\"host\":\"race-moved.example.test\"}", probe, true, null, ct),
            ct);

        Assert.Equal("added", AddStatusOf(race.FirstAnswer));
        Assert.Equal("updated", Parse(race.SecondAnswer)["status"]!.GetValue<string>());

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(2, keys.Count);
        Assert.Single(keys, key => key.Contains("race-added.example.test", StringComparison.OrdinalIgnoreCase));
        Assert.Single(keys, key => key.Contains("race-moved.example.test", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both the add and the edit should have queued behind the held identity lock.");
    }

    [Fact]
    public async Task TheConnectionProbeRunsOutsideTheLock_SoASlowProbeNeverHoldsUpOtherWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await SeedServerAsync(rig.Owner, 7331, "race-x", "race-x.example.test", ct);

        var parked = new ParkedProbe();
        var add = Task.Run(() => Edit.AddServersAsync(rig.Owner, AddJson("race-added.example.test"), parked.Probe, ct), ct);
        var edit = Task.Run(() => Edit.EditServerByNameAsync(rig.Owner, "race-x", "{\"host\":\"race-moved.example.test\"}", parked.Probe, true, null, ct), ct);
        var bodySucceeded = false;
        try
        {
            Assert.True(
                await WaitUntilAsync(() => Task.FromResult(parked.Arrived == 2), TimeSpan.FromSeconds(30), ct),
                $"Both callers should be waiting in their probe; {parked.Arrived} were.");

            /* While both sit in a probe (up to about 45 seconds in the field) neither holds the lock: nothing in the
               database holds or waits on an advisory lock, and a third session takes it at once. */
            Assert.Equal(0, await AdvisoryLocksAsync(rig.Owner, true, ct));
            Assert.Equal(0, await AdvisoryLocksAsync(rig.Owner, false, ct));
            await using (var probeConnection = await rig.Owner.OpenConnectionAsync(ct))
            await using (var transaction = await probeConnection.BeginTransactionAsync(ct))
            await using (var tryLock = new NpgsqlCommand("SELECT pg_try_advisory_xact_lock(hashtext('config_monitored_servers.identity'))", probeConnection, transaction))
            {
                Assert.True((bool)(await tryLock.ExecuteScalarAsync(ct))!, "The identity lock should be free while both callers are in their probe.");
                await transaction.RollbackAsync(ct);
            }

            parked.Release();
            Assert.Equal("added", AddStatusOf(await add.WaitAsync(TimeSpan.FromSeconds(60), ct)));
            Assert.Equal("updated", Parse(await edit.WaitAsync(TimeSpan.FromSeconds(60), ct))["status"]!.GetValue<string>());
            Assert.Equal(0, await AdvisoryLocksAsync(rig.Owner, true, ct));
            bodySucceeded = true;
        }
        finally
        {
            /* A body that failed before the release must not leave both callers parked in their probe. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
            {
                parked.Release();
                return Task.CompletedTask;
            });
        }
    }

    /// <summary>A probe that answers at once: the viewer's callers have none, and the MCP caller in the same race
    /// must reach its write without waiting.</summary>
    private static readonly Edit.ServerProbe ReachedAtOnce = (_, _) => Task.FromResult(Reached);

    private static MonitoredServerRow ViewerRow(int id, string name, string host) => new() { ServerId = id, Name = name, Host = host };

    /// <summary>The viewer's edit save, as one word: <c>Updated</c>, or <c>Claimed</c> when it refused the address.</summary>
    private static async Task<string> ViewerEditAsync(ViewerDataService viewer, MonitoredServerRow row, CancellationToken ct)
    {
        try
        {
            await viewer.UpsertMonitoredServerAsync(row, ct);
            return "Updated";
        }
        catch (MonitoredServerAddressClaimedException)
        {
            return "Claimed";
        }
    }

    /// <summary>
    /// The race the viewer's writes are held to: takes the SERVICE's identity lock (<see cref="Edit.IdentityLockSql"/>)
    /// from a second connection first, starts the callers one at a time, waits until both are queued on it (or one is
    /// already done, which is what a write path without the lock looks like), then releases it and returns both answers
    /// in call order. The first caller wins the lock.
    /// </summary>
    private static async Task<Race> QueuedRaceAsync(Rig rig, Func<Task<string>> first, Func<Task<string>> second, CancellationToken ct)
    {
        var holder = new NpgsqlConnection(rig.OwnerString);
        NpgsqlTransaction? held = null;
        var bodySucceeded = false;
        try
        {
            await holder.OpenAsync(ct);
            held = await holder.BeginTransactionAsync(ct);
            await using (var take = new NpgsqlCommand(Edit.IdentityLockSql, holder, held))
            {
                await take.ExecuteNonQueryAsync(ct);
            }

            /* One at a time: the lock is granted to its waiters in the order they queued, so the caller started first
               wins it and the second one's re-check is what runs against the first one's committed write. Starting both
               at once would leave the winner to the scheduler, and a viewer write whose re-check was gone could pass by
               luck whenever it happened to queue first. */
            var firstCall = Task.Run(first, ct);
            await WaitUntilAsync(
                async () => await AdvisoryLocksAsync(rig.Owner, false, ct) >= 1 || firstCall.IsCompleted, TimeSpan.FromSeconds(30), ct);
            var secondCall = Task.Run(second, ct);
            var bothQueued = false;
            await WaitUntilAsync(async () =>
            {
                if (await AdvisoryLocksAsync(rig.Owner, false, ct) >= 2)
                {
                    bothQueued = true;
                    return true;
                }

                return firstCall.IsCompleted || secondCall.IsCompleted;
            }, TimeSpan.FromSeconds(30), ct);

            await held.RollbackAsync(ct);
            var answers = await Task.WhenAll(firstCall, secondCall).WaitAsync(TimeSpan.FromSeconds(60), ct);
            var left = await AdvisoryLocksAsync(rig.Owner, true, ct) + await AdvisoryLocksAsync(rig.Owner, false, ct);
            bodySucceeded = true;
            return new Race(answers[0], answers[1], bothQueued, left);
        }
        finally
        {
            /* The holder's own rollback: only the session holding the lock can end its transaction. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                if (held is not null)
                {
                    await held.DisposeAsync();
                }

                await holder.DisposeAsync();
            });
        }
    }

    /// <param name="viewerFirst">Whose write queues on the lock first, and so wins it: the viewer's add (the MCP edit's
    /// re-check then refuses), or the MCP edit (the VIEWER's re-check then refuses the add).</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AViewerAddAndAnMcpEditThatClaimOneAddress_QueueOnTheLock_AndTheSecondToQueueIsRefused(bool viewerFirst)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        await SeedServerAsync(rig.Owner, 7341, "race-x", "race-x.example.test", ct);

        Func<Task<string>> viewerAdd = async () => (await viewer.AddMonitoredServerAsync(
            ViewerRow(ViewerDataService.ComputeServerId(AddressK, null, false), "viewer-added", AddressK), ct)).Outcome.ToString();
        Func<Task<string>> mcpEdit = async () => Parse(await Edit.EditServerByNameAsync(
            rig.Owner, "race-x", "{\"host\":\"" + AddressK + "\"}", ReachedAtOnce, true, null, ct))["status"]!.GetValue<string>();

        var race = viewerFirst
            ? await QueuedRaceAsync(rig, viewerAdd, mcpEdit, ct)
            : await QueuedRaceAsync(rig, mcpEdit, viewerAdd, ct);

        if (viewerFirst)
        {
            Assert.Equal(nameof(MonitoredServerAddOutcome.Added), race.FirstAnswer);
            Assert.Equal("collides", race.SecondAnswer);
        }
        else
        {
            Assert.Equal("updated", race.FirstAnswer);
            Assert.Equal(nameof(MonitoredServerAddOutcome.Duplicate), race.SecondAnswer);
        }

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(viewerFirst ? 2 : 1, keys.Count);
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both the viewer's add and the MCP edit should have queued behind the held identity lock.");
    }

    /// <param name="viewerFirst">Whose write queues on the lock first, and so wins it: the viewer's edit (the MCP add's
    /// re-check then answers duplicate), or the MCP add (the VIEWER's re-check then refuses the edit).</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AViewerEditAndAnMcpAddThatClaimOneAddress_QueueOnTheLock_AndTheSecondToQueueIsRefused(bool viewerFirst)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        await SeedServerAsync(rig.Owner, 7351, "race-x", "race-x.example.test", ct);

        Func<Task<string>> viewerEdit = () => ViewerEditAsync(viewer, ViewerRow(7351, "race-x", AddressK), ct);
        Func<Task<string>> mcpAdd = async () => AddStatusOf(await Edit.AddServersAsync(rig.Owner, AddJson(AddressK), ReachedAtOnce, ct));

        var race = viewerFirst
            ? await QueuedRaceAsync(rig, viewerEdit, mcpAdd, ct)
            : await QueuedRaceAsync(rig, mcpAdd, viewerEdit, ct);

        if (viewerFirst)
        {
            Assert.Equal("Updated", race.FirstAnswer);
            Assert.Equal("duplicate", race.SecondAnswer);
        }
        else
        {
            Assert.Equal("added", race.FirstAnswer);
            Assert.Equal("Claimed", race.SecondAnswer);
        }

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(viewerFirst ? 1 : 2, keys.Count);
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both the viewer's edit and the MCP add should have queued behind the held identity lock.");
    }

    [Fact]
    public async Task TwoViewerEditsThatMoveTwoServersOntoOneAddress_QueueOnTheLock_AndTheSecondToQueueIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        await SeedServerAsync(rig.Owner, 7361, "race-x", "race-x.example.test", ct);
        await SeedServerAsync(rig.Owner, 7362, "race-y", "race-y.example.test", ct);

        var race = await QueuedRaceAsync(rig,
            () => ViewerEditAsync(viewer, ViewerRow(7361, "race-x", AddressK), ct),
            () => ViewerEditAsync(viewer, ViewerRow(7362, "race-y", AddressK), ct),
            ct);

        Assert.Equal("Updated", race.FirstAnswer);
        Assert.Equal("Claimed", race.SecondAnswer);

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(2, keys.Count);
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both viewer edits should have queued behind the held identity lock.");
    }

    /// <summary>The viewer's insert-if-absent (the migrate-in's old write) is an identity write too: an address an edit
    /// left a definition at, under an older id, is refused (false, nothing written); a free address is written.</summary>
    [Fact]
    public async Task TheViewersInsertIfAbsent_RefusesAnAddressHeldUnderAnotherId_AndWritesAFreeOne()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        await SeedServerAsync(rig.Owner, 7391, "moved", AddressK, ct);

        Assert.False(await viewer.InsertMonitoredServerIfAbsentAsync(
            ViewerRow(ViewerDataService.ComputeServerId(AddressK, null, false), "again", AddressK), ct));
        Assert.True(await viewer.InsertMonitoredServerIfAbsentAsync(
            ViewerRow(ViewerDataService.ComputeServerId("free.example.test", null, false), "free", "free.example.test"), ct));

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(2, keys.Count);
        Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
        Assert.Single(keys, key => key.Contains("free.example.test", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, await AdvisoryLocksAsync(rig.Owner, true, ct) + await AdvisoryLocksAsync(rig.Owner, false, ct));
    }

    [Fact]
    public async Task AViewerAddAndAViewerEditOnDifferentAddresses_BothSucceed_AfterQueueingOnTheLock()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        await SeedServerAsync(rig.Owner, 7371, "race-x", "race-x.example.test", ct);

        var race = await QueuedRaceAsync(rig,
            async () => (await viewer.AddMonitoredServerAsync(
                ViewerRow(ViewerDataService.ComputeServerId("race-added.example.test", null, false), "viewer-added", "race-added.example.test"), ct)).Outcome.ToString(),
            () => ViewerEditAsync(viewer, ViewerRow(7371, "race-x", "race-moved.example.test"), ct),
            ct);

        Assert.Equal(nameof(MonitoredServerAddOutcome.Added), race.FirstAnswer);
        Assert.Equal("Updated", race.SecondAnswer);

        var keys = await StorageKeysAsync(rig.Owner, ct);
        Assert.Equal(2, keys.Count);
        Assert.Single(keys, key => key.Contains("race-added.example.test", StringComparison.OrdinalIgnoreCase));
        Assert.Single(keys, key => key.Contains("race-moved.example.test", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(0, race.LocksLeftAfterwards);
        Assert.True(race.BothQueuedOnTheLock, "Both the viewer's add and its edit should have queued behind the held identity lock.");
    }

    /// <summary>
    /// The startup seed's INSERT loop takes the identity lock, and reads the addresses again under it. The lock is held
    /// from a second connection; the seed (two servers from darling.json, the second at address K) queues behind it and
    /// writes nothing while it waits; the holder then commits a definition that an edit would have left at K under
    /// another id, and releases the lock. The seed's own INSERT for K would land under <c>hash(K)</c>, a different id,
    /// so nothing but the re-read keeps the registry at one row for K.
    /// </summary>
    [Fact]
    public async Task TheStartupSeed_QueuesOnTheLock_WritesNothingWhileItWaits_AndSkipsAnAddressClaimedMeanwhile()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var config = new DarlingConfig();
        config.Servers.Add(new MonitoredServer { Name = "seed-a", Host = "seed-a.example.test", Auth = "integrated" });
        config.Servers.Add(new MonitoredServer { Name = "seed-k", Host = AddressK, Auth = "integrated" });
        var provider = new StoreConfigProvider(rig.Owner);

        var holder = new NpgsqlConnection(rig.OwnerString);
        NpgsqlTransaction? held = null;
        var bodySucceeded = false;
        try
        {
            await holder.OpenAsync(ct);
            held = await holder.BeginTransactionAsync(ct);
            await using (var take = new NpgsqlCommand(Edit.IdentityLockSql, holder, held))
            {
                await take.ExecuteNonQueryAsync(ct);
            }

            var seed = Task.Run(() => provider.SeedIfEmptyAsync(config, ct), ct);
            Assert.True(
                await WaitUntilAsync(async () => await AdvisoryLocksAsync(rig.Owner, false, ct) >= 1, TimeSpan.FromSeconds(30), ct),
                "The seed should queue behind the held identity lock.");
            Assert.Equal(0, await CountAsync(rig.Owner, "SELECT count(*) FROM config_monitored_servers", ct));

            /* The definition an edit left at K under an older id, committed in the lock's own transaction. */
            await using (var claim = new NpgsqlCommand(
                $@"INSERT INTO config_monitored_servers (server_id, name, host, auth, excluded_databases, is_enabled, monthly_cost_usd)
                   VALUES (7381, 'mover', '{AddressK}', 'integrated', ARRAY[]::text[], TRUE, 0)", holder, held))
            {
                await claim.ExecuteNonQueryAsync(ct);
            }

            await held.CommitAsync(ct);
            await seed.WaitAsync(TimeSpan.FromSeconds(60), ct);

            var keys = await StorageKeysAsync(rig.Owner, ct);
            Assert.Equal(2, keys.Count);
            Assert.Equal(keys.Count, keys.Distinct(StringComparer.OrdinalIgnoreCase).Count());
            Assert.Single(keys, key => key.Contains(AddressK, StringComparison.OrdinalIgnoreCase));
            Assert.Single(keys, key => key.Contains("seed-a.example.test", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(1, await CountAsync(rig.Owner, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 7381 AND name = 'mover'", ct));
            Assert.Equal(0, await AdvisoryLocksAsync(rig.Owner, true, ct) + await AdvisoryLocksAsync(rig.Owner, false, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                if (held is not null)
                {
                    await held.DisposeAsync();
                }

                await holder.DisposeAsync();
            });
        }
    }
}
