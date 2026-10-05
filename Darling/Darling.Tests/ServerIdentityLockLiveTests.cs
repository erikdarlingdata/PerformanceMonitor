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
}
