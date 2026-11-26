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
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9642: the MCP tools that manage web access groups and the advanced web access switch run the Viewer's own write code
/// (<see cref="WebAccessGroupAdmin"/>) on a live store. A create and a rotate answer with the token once, sealed to the service's key
/// for the group and generation, and the token is in no log line and no journal row.
/// </summary>
public sealed class DarlingMcpWebAccessToolsLiveTests
{
    private const string Actor = "mcp:test-caller";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AGroupLifecycle_RunsThroughTheTools_SealingEachTokenToItsGeneration_AndJournalingTheCaller()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);

            var empty = JsonNode.Parse(await DarlingMcpWebAccessTools.ListAsync(owner, ct))!;
            Assert.Equal(0, (int)empty["groups_returned"]!);
            Assert.False((bool)empty["advanced_web_access_enabled"]!);

            var on = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, true, Actor, null, ct))!;
            Assert.Equal("set", (string)on["status"]!);
            Assert.NotNull(await store.ReadAdvancedSessionEpochAsync(ct));
            Assert.Equal("unchanged", (string)JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, true, Actor, null, ct))!["status"]!);

            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "  Support  ", "all_servers", null, Actor, null, ct))!;
            Assert.Equal("created", (string)created["status"]!);
            var id = Guid.Parse((string)created["group"]!["group_id"]!);
            Assert.Equal("Support", (string)created["group"]!["name"]!);
            var firstToken = (string)created["token"]!;
            Assert.True((bool)created["advanced_web_access_enabled"]!);

            /* The stored copy opens to the token the answer gave, bound to this group and its generation. */
            var credential = (await store.ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
            Assert.Equal(firstToken, ring.Open(credential.SealedToken, PasswordBinding.ForWebAccessToken(id, credential.Group.TokenGeneration)));

            var tag = (int)(await ScalarAsync(owner, "INSERT INTO config.server_tags(name) VALUES ('web-tag') RETURNING id", ct))!;
            var updated = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Support", "tags", [tag], false, false, Actor, null, ct))!;
            Assert.Equal("updated", (string)updated["status"]!);
            var group = (await store.ListAsync(ct)).Single();
            Assert.Equal(WebAccessGroupScope.Tags, group.Scope);
            Assert.Equal([tag], group.TagIds);
            Assert.False(group.Enabled);

            var refusedRotate = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, id.ToString(), false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refusedRotate["code"]!);
            Assert.Null(refusedRotate["token"]);
            Assert.Equal(group.TokenGeneration, (await store.ReadAsync(id, ct))!.TokenGeneration);

            var rotated = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, id.ToString(), true, Actor, null, ct))!;
            var secondToken = (string)rotated["token"]!;
            Assert.NotEqual(firstToken, secondToken);
            var after = (await store.ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
            Assert.NotEqual(group.TokenGeneration, after.Group.TokenGeneration);
            Assert.Equal(secondToken, ring.Open(after.SealedToken, PasswordBinding.ForWebAccessToken(id, after.Group.TokenGeneration)));

            var refusedDelete = JsonNode.Parse(await DarlingMcpWebAccessTools.DeleteAsync(owner, "Support", false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refusedDelete["code"]!);
            Assert.Single(await store.ListAsync(ct));
            var deleted = JsonNode.Parse(await DarlingMcpWebAccessTools.DeleteAsync(owner, "Support", true, Actor, null, ct))!;
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.Empty(await store.ListAsync(ct));

            Assert.Equal(Actor, await ScalarAsync(owner, "SELECT actor FROM config.web_access_group_events WHERE event = 'created'", ct));
            Assert.Equal("rotated", await ScalarAsync(owner, "SELECT event FROM config.web_access_group_events WHERE event = 'rotated'", ct));
        });
    }

    [Fact]
    public async Task ACreateAndARotate_KeepTheTokenOutOfEveryLogLine_AndEveryJournalRow()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var logger = new CapturingTestLogger();
            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, TestKeyRings.Healthy, "Ops", "all_servers", null, Actor, logger, ct))!;
            var id = (string)created["group"]!["group_id"]!;
            var rotated = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, TestKeyRings.Healthy, id, true, Actor, logger, ct))!;
            var tokens = new[] { (string)created["token"]!, (string)rotated["token"]! };
            Assert.All(tokens, t => Assert.True(t.Length >= 32));

            Assert.True(logger.CountAtLevel(LogLevel.Warning) >= 2);
            var journal = (string)(await ScalarAsync(owner, "SELECT coalesce(string_agg(e::text, ' '), '') FROM config.web_access_group_events e", ct))!;
            foreach (var token in tokens)
            {
                Assert.DoesNotContain(token, logger.Joined, StringComparison.Ordinal);
                Assert.DoesNotContain(token, journal, StringComparison.Ordinal);
            }
        });
    }

    [Fact]
    public async Task BadInput_IsRefusedOrReportedWithoutDatabaseDetail_AndWritesNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);

            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "everyone", null, Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "tags", null, Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "all_servers", [1], Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, " ", "all_servers", null, Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "has space", "all_servers", null, Actor, null, ct)));
            Assert.Equal("key_unavailable", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, TestKeyRings.NotReady, "x", "all_servers", null, Actor, null, ct)));
            Assert.Equal("key_unavailable", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, null, "x", "all_servers", null, Actor, null, ct)));
            Assert.Equal("no_owner_source", Code(await DarlingMcpWebAccessTools.CreateAsync(null, ring, "x", "all_servers", null, Actor, null, ct)));
            Assert.Equal("not_found", Code(await DarlingMcpWebAccessTools.DeleteAsync(owner, "nobody", true, Actor, null, ct)));
            Assert.Empty(await store.ListAsync(ct));

            /* A tag the store does not know is the store's own refusal, reported in the Viewer's words and with no SQL state. */
            var unknownTag = await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "tags", [987654], Actor, null, ct);
            Assert.True(unknownTag.Contains("Check the group name and selected tags", StringComparison.Ordinal), unknownTag);
            Assert.DoesNotContain("WG400", unknownTag, StringComparison.Ordinal);
            Assert.Empty(await store.ListAsync(ct));

            await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "dup", "all_servers", null, Actor, null, ct);
            var duplicate = await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "dup", "all_servers", null, Actor, null, ct);
            Assert.True(duplicate.Contains("A group with this name already exists", StringComparison.Ordinal), duplicate);
            Assert.Single(await store.ListAsync(ct));
        });
    }

    [Fact]
    public async Task TurningAdvancedAccessOn_NeedsConfirm_AndNamesWhatItBringsBack_TurningItOffDoesNot()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var store = new WebAccessGroupStore(owner);
            await DarlingMcpWebAccessTools.CreateAsync(owner, TestKeyRings.Healthy, "Support", "all_servers", null, Actor, null, ct);

            var refused = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refused["code"]!);
            Assert.Contains("'Support'", (string)refused["message"]!, StringComparison.Ordinal);
            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));

            var on = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, true, Actor, null, ct))!;
            Assert.Equal("set", (string)on["status"]!);
            Assert.NotNull(await store.ReadAdvancedSessionEpochAsync(ct));

            var off = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, false, false, Actor, null, ct))!;
            Assert.Equal("set", (string)off["status"]!);
            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));
            /* The undo of a switch-off turns it on, so the hint carries the confirm it will need. */
            Assert.True((bool)off["undo"]!["arguments"]!["confirm"]!);
        });
    }

    [Fact]
    public async Task AnUpdateThatEnablesAGroupOrWidensItToAllServers_NeedsConfirm_AndChangesNothingWithout()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);
            var tag = (int)(await ScalarAsync(owner, "INSERT INTO config.server_tags(name) VALUES ('t1') RETURNING id", ct))!;
            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "Narrow", "tags", [tag], Actor, null, ct))!;
            var id = Guid.Parse((string)created["group"]!["group_id"]!);
            Assert.Equal("updated", (string)JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Narrow", null, null, false, false, Actor, null, ct))!["status"]!);

            var enable = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Narrow", null, null, true, false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)enable["code"]!);
            Assert.False((await store.ReadAsync(id, ct))!.Enabled);

            var widen = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Narrow", "all_servers", null, null, false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)widen["code"]!);
            Assert.Equal(WebAccessGroupScope.Tags, (await store.ReadAsync(id, ct))!.Scope);

            var confirmed = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Narrow", "all_servers", null, true, true, Actor, null, ct))!;
            Assert.Equal("updated", (string)confirmed["status"]!);
            var after = (await store.ReadAsync(id, ct))!;
            Assert.Equal(WebAccessGroupScope.AllServers, after.Scope);
            Assert.True(after.Enabled);

            /* Narrowing and disabling widen nothing, so they take no confirm. */
            var narrowed = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Narrow", "tags", [tag], false, false, Actor, null, ct))!;
            Assert.Equal("updated", (string)narrowed["status"]!);
        });
    }

    [Fact]
    public async Task ARotateWithoutConfirm_ChangesNothing_NotTheTokenNorTheJournal()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);
            await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "Ops", "all_servers", null, Actor, null, ct);
            var before = (await store.ReadCredentialsAsync(ct)).Single();

            var refused = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, "Ops", false, Actor, null, ct))!;

            Assert.Equal("confirm_required", (string)refused["code"]!);
            Assert.Null(refused["token"]);
            var after = (await store.ReadCredentialsAsync(ct)).Single();
            Assert.Equal(before.Group.TokenGeneration, after.Group.TokenGeneration);
            Assert.Equal(before.SealedToken, after.SealedToken);
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events WHERE event = 'rotated'", ct));
        });
    }

    [Fact]
    public async Task AGroupNamedLikeAnotherGroupsId_IsRefusedAsAmbiguous_NotResolvedToEither()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);
            var first = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "First", "all_servers", null, Actor, null, ct))!;
            var firstId = (string)first["group"]!["group_id"]!;
            await DarlingMcpWebAccessTools.CreateAsync(owner, ring, firstId, "all_servers", null, Actor, null, ct);

            Assert.Equal("ambiguous", Code(await DarlingMcpWebAccessTools.DeleteAsync(owner, firstId, true, Actor, null, ct)));
            Assert.Equal("ambiguous", Code(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, firstId, true, Actor, null, ct)));
            Assert.Equal("ambiguous", Code(await DarlingMcpWebAccessTools.UpdateAsync(owner, firstId, null, null, false, false, Actor, null, ct)));
            Assert.Equal(2, (await store.ListAsync(ct)).Count);

            /* A name that is no other group's id still resolves by name. */
            Assert.Equal("deleted", (string)JsonNode.Parse(await DarlingMcpWebAccessTools.DeleteAsync(owner, "First", true, Actor, null, ct))!["status"]!);
        });
    }

    [Fact]
    public async Task ACreateAndARotate_ReturnTheTokenEvenWhenTheFollowUpReadsFail()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);
            var broken = NpgsqlDataSource.Create(owner.ConnectionString);
            await broken.DisposeAsync();

            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "Ops", "all_servers", null, Actor, null, ct, broken))!;
            Assert.Equal("created", (string)created["status"]!);
            var id = Guid.Parse((string)created["group"]!["group_id"]!);
            var token = (string)created["token"]!;
            var credential = (await store.ReadCredentialsAsync(ct)).Single();
            Assert.Equal(token, ring.Open(credential.SealedToken, PasswordBinding.ForWebAccessToken(id, credential.Group.TokenGeneration)));

            var rotated = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, "Ops", true, Actor, null, ct, broken))!;
            Assert.Equal("rotated", (string)rotated["status"]!);
            var second = (string)rotated["token"]!;
            var after = (await store.ReadCredentialsAsync(ct)).Single();
            Assert.Equal(second, ring.Open(after.SealedToken, PasswordBinding.ForWebAccessToken(id, after.Group.TokenGeneration)));
        });
    }

    [Fact]
    public async Task AnUpdateThatAddsATag_NeedsConfirm_AndAnUndoNeedsOneOnlyWhenGoingBackGivesMore()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var store = new WebAccessGroupStore(owner);
            var one = (int)(await ScalarAsync(owner, "INSERT INTO config.server_tags(name) VALUES ('t1') RETURNING id", ct))!;
            var two = (int)(await ScalarAsync(owner, "INSERT INTO config.server_tags(name) VALUES ('t2') RETURNING id", ct))!;
            var three = (int)(await ScalarAsync(owner, "INSERT INTO config.server_tags(name) VALUES ('t3') RETURNING id", ct))!;
            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, TestKeyRings.Healthy, "Tagged", "tags", [one], Actor, null, ct))!;
            var id = Guid.Parse((string)created["group"]!["group_id"]!);

            /* More tags give the group's token more servers, so the update asks first and changes nothing without the confirm. */
            var refused = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Tagged", null, [one, two, three], null, false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refused["code"]!);
            Assert.Equal([one], (await store.ReadAsync(id, ct))!.TagIds);

            var widened = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Tagged", null, [one, two, three], null, true, Actor, null, ct))!;
            Assert.Equal("updated", (string)widened["status"]!);
            Assert.Equal([one, two, three], (await store.ReadAsync(id, ct))!.TagIds.Order().ToArray());

            /* Going back to the old tags gives nothing more, so the hint carries no confirm key at all (not a null one). */
            var undoOfWiden = (JsonObject)widened["undo"]!["arguments"]!;
            Assert.False(undoOfWiden.ContainsKey("confirm"));

            /* Dropping a tag, or keeping a subset, widens nothing and takes no confirm. */
            var narrowed = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Tagged", null, [two], null, false, Actor, null, ct))!;
            Assert.Equal("updated", (string)narrowed["status"]!);

            /* The hint of that narrowing restores two more tags, so it carries the confirm it will need. */
            var undoOfNarrow = (JsonObject)narrowed["undo"]!["arguments"]!;
            Assert.True((bool)undoOfNarrow["confirm"]!);
        });
    }

    [Fact]
    public async Task ACreateAndARotate_CancelledWhileWaitingForTheGroupLock_StillFinishTheWrite_AndTheTokenOpensTheStoredCopy()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);

            var created = JsonNode.Parse(await CancelledWhileTheGroupLockIsHeldAsync(owner, token =>
                DarlingMcpWebAccessTools.CreateAsync(owner, ring, "Ops", "all_servers", null, Actor, null, token), ct))!;
            Assert.Equal("created", (string)created["status"]!);
            var id = Guid.Parse((string)created["group"]!["group_id"]!);
            var credential = (await store.ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
            Assert.Equal((string)created["token"]!, ring.Open(credential.SealedToken, PasswordBinding.ForWebAccessToken(id, credential.Group.TokenGeneration)));

            var rotated = JsonNode.Parse(await CancelledWhileTheGroupLockIsHeldAsync(owner, token =>
                DarlingMcpWebAccessTools.RotateAsync(owner, ring, "Ops", true, Actor, null, token), ct))!;
            Assert.Equal("rotated", (string)rotated["status"]!);
            var after = (await store.ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
            Assert.NotEqual(credential.Group.TokenGeneration, after.Group.TokenGeneration);
            Assert.Equal((string)rotated["token"]!, ring.Open(after.SealedToken, PasswordBinding.ForWebAccessToken(id, after.Group.TokenGeneration)));
        });
    }

    /// <summary>Holds the group administration lock on a second connection, starts the call, waits until it is queued behind the lock,
    /// cancels its token, and only then lets the lock go.</summary>
    private static async Task<string> CancelledWhileTheGroupLockIsHeldAsync(NpgsqlDataSource owner, Func<CancellationToken, Task<string>> call, CancellationToken ct)
    {
        await using var holder = await owner.OpenConnectionAsync(ct);
        await using var transaction = await holder.BeginTransactionAsync(ct);
        await using (var hold = new NpgsqlCommand("SELECT pg_advisory_xact_lock(7988, 1)", holder, transaction))
            await hold.ExecuteNonQueryAsync(ct);

        using var cancel = new CancellationTokenSource();
        var running = call(cancel.Token);
        const string queued = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted " +
            "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())";
        for (var attempt = 0; attempt < 200 && Convert.ToInt32(await ScalarAsync(owner, queued, ct), System.Globalization.CultureInfo.InvariantCulture) == 0; attempt++)
            await Task.Delay(50, ct);
        Assert.True(Convert.ToInt32(await ScalarAsync(owner, queued, ct), System.Globalization.CultureInfo.InvariantCulture) > 0, "the write never queued behind the lock");

        await cancel.CancelAsync();
        await transaction.CommitAsync(ct);
        return await running;
    }

    private const string OperatorBearer = "operator-bearer-value-web-access-9657";

    [Fact]
    public async Task ACreateThroughTheServer_ByAnOperatorNarrowedToMaskLiterals_ReturnsTheToken_AndLogsNone()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var logger = new CapturingTestLogger();
            var ownerSource = new StoreOwnerSource();
            ownerSource.Publish(owner);
            var mcp = new McpConfig
            {
                AllowWrites = true,
                AllowServerAdmin = true,
                Tokens = new List<McpTokenEntry> { new() { Name = "ops-lead", Token = OperatorBearer, Role = "operator", DataDetail = "maskLiterals" } },
            };
            using var server = await BuildServerAsync(mcp, ownerSource, logger);

            DarlingMcpWebAccessTools.RingForTests = TestKeyRings.Healthy;
            try
            {
                var (_, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
                    server, "create_web_access_group", "{\"name\":\"Ops\",\"scope\":\"all_servers\"}", OperatorBearer);
                var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
                using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
                var answer = JsonNode.Parse(document.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!)!;

                /* The one-time token reaches a caller narrowed to maskLiterals: the data detail filter masks query text, not this credential. */
                Assert.True((string)answer["status"]! == "created", answer.ToJsonString());
                var token = (string)answer["token"]!;
                Assert.True(token.Length >= 32);
                var id = Guid.Parse((string)answer["group"]!["group_id"]!);
                var credential = (await new WebAccessGroupStore(owner).ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
                Assert.Equal(token, TestKeyRings.Healthy.Open(credential.SealedToken, PasswordBinding.ForWebAccessToken(id, credential.Group.TokenGeneration)));

                Assert.True(logger.CountAtLevel(LogLevel.Warning) >= 1, logger.Joined);
                Assert.True(logger.Lines.All(l => !l.Contains(token, StringComparison.Ordinal)), string.Join(" || ", logger.Lines.Where(l => l.Contains(token, StringComparison.Ordinal)).Select(l => l[..Math.Min(150, l.Length)])));
            }
            finally
            {
                DarlingMcpWebAccessTools.RingForTests = null;
            }
        });
    }

    private const string AdvancedJournalSql = "SELECT actor || '|' || database_actor FROM config.web_access_group_events WHERE event = $1 ORDER BY event_id DESC LIMIT 1";

    private static async Task<string?> LastAdvancedJournalAsync(NpgsqlDataSource owner, string eventName, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(AdvancedJournalSql);
        command.Parameters.AddWithValue(eventName);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    /// <summary>#9662: the tool writes on the service's store owner connection, so the journal row named the store owner. It names the
    /// caller now, and keeps the database role in its own column. The Viewer's one argument setter still names the session user.</summary>
    [Fact]
    public async Task AnAdvancedSwitchFlip_OverMcp_JournalsTheCaller_AndAViewerFlipStillJournalsTheSessionUser()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var sessionUser = (string)(await ScalarAsync(owner, "SELECT session_user", ct))!;
            var store = new WebAccessGroupStore(owner);

            await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, true, Actor, null, ct);
            Assert.Equal(Actor + "|" + sessionUser, await LastAdvancedJournalAsync(owner, "advanced_enabled", ct));

            await store.SetAdvancedEnabledAsync(false, ct);
            Assert.Equal(sessionUser + "|" + sessionUser, await LastAdvancedJournalAsync(owner, "advanced_disabled", ct));

            await store.SetAdvancedEnabledAsync(true, ct);
            Assert.Equal(sessionUser + "|" + sessionUser, await LastAdvancedJournalAsync(owner, "advanced_enabled", ct));

            await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, false, false, Actor, null, ct);
            Assert.Equal(Actor + "|" + sessionUser, await LastAdvancedJournalAsync(owner, "advanced_disabled", ct));
        });
    }

    /// <summary>#9662: the confirm is decided under the store's row lock, from the value held then. A switch that was on at the tool's
    /// earlier look and is off when the write runs is not turned on without the confirm, and the answer is the refusal, never unchanged.</summary>
    [Fact]
    public async Task AnUnconfirmedEnable_WhenTheStoredValueIsOff_ChangesNothing_EvenIfItWasOnAtAnEarlierRead()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var store = new WebAccessGroupStore(owner);
            await store.SetAdvancedEnabledAsync(true, ct);
            Assert.NotNull(await store.ReadAdvancedSessionEpochAsync(ct));
            await store.SetAdvancedEnabledAsync(false, ct);
            var epoch = await ScalarAsync(owner, "SELECT session_epoch FROM config.web_access_advanced", ct);
            var journalRows = await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct);

            /* The function itself: it returns the value it held and changes nothing. */
            Assert.False(await store.SetAdvancedEnabledAsync(true, Actor, false, ct));
            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));

            var refused = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refused["code"]!);
            Assert.Null(refused["undo"]);
            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));
            Assert.Equal(epoch, await ScalarAsync(owner, "SELECT session_epoch FROM config.web_access_advanced", ct));
            Assert.Equal(journalRows, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct));

            /* Confirmed, it turns on and returns the prior value, which the answer builds its undo from. */
            Assert.False(await store.SetAdvancedEnabledAsync(true, Actor, true, ct));
            Assert.NotNull(await store.ReadAdvancedSessionEpochAsync(ct));
            Assert.True(await store.SetAdvancedEnabledAsync(false, Actor, false, ct));
            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));
        });
    }

    /// <summary>Setting the switch to the value it holds changes nothing, whatever the confirm says, and the answer is unchanged with no undo.</summary>
    [Fact]
    public async Task ASwitchAlreadyOn_AnsweredWithoutConfirm_IsUnchanged_WithNoUndo_AndNoJournalRow()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, true, Actor, null, ct);
            var epoch = await ScalarAsync(owner, "SELECT session_epoch FROM config.web_access_advanced", ct);
            var journalRows = await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct);

            var answer = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, false, Actor, null, ct))!;
            Assert.Equal("unchanged", (string)answer["status"]!);
            Assert.True((bool)answer["advanced_web_access_enabled"]!);
            Assert.Null(answer["undo"]);
            Assert.Equal(epoch, await ScalarAsync(owner, "SELECT session_epoch FROM config.web_access_advanced", ct));
            Assert.Equal(journalRows, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct));
        });
    }

    /// <summary>A blank or oversized actor is refused by the store before it changes anything, as the group writes refuse one.</summary>
    [Fact]
    public async Task ABlankOrOversizedActor_IsRefusedByTheStore_AndChangesNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var store = new WebAccessGroupStore(owner);
            await Assert.ThrowsAsync<PostgresException>(() => store.SetAdvancedEnabledAsync(true, "   ", true, ct));
            await Assert.ThrowsAsync<PostgresException>(() => store.SetAdvancedEnabledAsync(true, new string('a', 257), true, ct));
            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct));
        });
    }

    /// <summary>An actor with a control character is refused like a blank one: a tab, a newline, a bell and the bidirectional marks that
    /// reorder text (U+202E) would otherwise reach the journal as a name that reads differently from what it holds.</summary>
    [Fact]
    public async Task AnActorWithAControlCharacter_IsRefusedByTheStore_AndChangesNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var store = new WebAccessGroupStore(owner);
            foreach (var actor in new[] { "mcp:\t\n", "mcp:a\tb", "mcp:a\nb", "mcp:a\ab", "mcp:rtl\u202Egnp", "mcp:\u2066x\u2069" })
            {
                var failure = await Assert.ThrowsAsync<PostgresException>(() => store.SetAdvancedEnabledAsync(true, actor, true, ct));
                Assert.Equal("WG400", failure.SqlState);
            }

            Assert.Null(await store.ReadAdvancedSessionEpochAsync(ct));
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct));
            Assert.True(await store.SetAdvancedEnabledAsync(true, "mcp:plain-name", true, ct) is false);
        });
    }

    /// <summary>A store with no switch row changed nothing, so the tool refuses and says so: no flip answer, no undo hint and no warning line.</summary>
    [Fact]
    public async Task AMissingSwitchRow_IsRefusedByTheTool_WithNoUndoAndNoLogLine()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            await ScalarAsync(owner, "DELETE FROM config.web_access_advanced", ct);
            var store = new WebAccessGroupStore(owner);
            Assert.Null(await store.SetAdvancedEnabledAsync(true, Actor, true, ct));

            var logger = new CapturingTestLogger();
            var refused = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, true, Actor, logger, ct))!;
            Assert.Equal("switch_missing", (string)refused["code"]!);
            Assert.Null(refused["undo"]);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM config.web_access_group_events", ct));
        });
    }

    /// <summary>Rewinding past the three argument setter leaves the one argument setter working: its V319 body calls the three argument form,
    /// so the rewind puts the V262 body back before it drops that form.</summary>
    [Fact]
    public async Task AfterARewindPastTheThreeArgumentSetter_TheOneArgumentSetterStillFlipsTheSwitch()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            await using (var connection = await owner.OpenConnectionAsync(ct))
                await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 318, ct);
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM pg_proc WHERE proname = 'set_web_access_advanced' AND pronargs = 3", ct));

            var store = new WebAccessGroupStore(owner);
            await store.SetAdvancedEnabledAsync(true, ct);
            Assert.NotNull(await store.ReadAdvancedSessionEpochAsync(ct));
        });
    }

    /// <summary>Through a built server, the journal row of a flip names the token's identity, not the store owner.</summary>
    [Fact]
    public async Task AFlipThroughTheServer_JournalsTheCallersTokenIdentity()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var logger = new CapturingTestLogger();
            var ownerSource = new StoreOwnerSource();
            ownerSource.Publish(owner);
            var mcp = new McpConfig
            {
                AllowWrites = true,
                AllowServerAdmin = true,
                Tokens = new List<McpTokenEntry> { new() { Name = "ops-lead", Token = OperatorBearer, Role = "operator" } },
            };
            using var server = await BuildServerAsync(mcp, ownerSource, logger);

            await DarlingMcpWritePolicyTests.ToolsCallAsync(server, "set_advanced_web_access", "{\"enabled\":true,\"confirm\":true}", OperatorBearer);

            var row = await LastAdvancedJournalAsync(owner, "advanced_enabled", ct);
            Assert.NotNull(row);
            var sessionUser = (string)(await ScalarAsync(owner, "SELECT session_user", ct))!;
            Assert.StartsWith("mcp:", row, StringComparison.Ordinal);
            Assert.Contains("ops-lead", row, StringComparison.Ordinal);
            Assert.EndsWith("|" + sessionUser, row, StringComparison.Ordinal);
        });
    }

    /// <summary>A built MCP server over the real tools, every provider of the host's logging replaced by <paramref name="logger"/>.</summary>
    private static async Task<TestServer> BuildServerAsync(McpConfig mcp, StoreOwnerSource ownerSource, ILogger logger)
    {
        var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, "", out _);
        var tokens = DarlingMcpTokenSet.Resolve(mcp, "", legacy, null);
        var builder = DarlingMcpHostService.CreateHostBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(new FixedLoggerProvider(logger));
        builder.Services.AddSingleton(NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling"));
        builder.Services.AddSingleton(ownerSource);
        builder.Services.AddSingleton(logger);
        builder.Services.AddSingleton<IGcfOutputFormat>(new FixedGcfOutputFormat(false));
        DarlingMcpHostService.RegisterDataDetail(builder.Services, McpDataDetail.Full);
        DarlingMcpHostService.ConfigureMcpServices(builder.Services, DarlingPeerDirectory.Snapshot.Empty, writes: tokens.Union, tokens: tokens);

        var app = builder.Build();
        var host = new DarlingMcpHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
        host.ConfigurePipeline(
            app, networkMode: false, networkListenIp: null, allowedCidr: System.Net.IPNetwork.Parse("192.168.1.0/24"), bearerToken: "",
            requireTokenWhenLoopbackOnly: true, tokens: tokens);
        await app.StartAsync();
        return app.GetTestServer();
    }

    private sealed class FixedLoggerProvider(ILogger logger) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => logger;
        public void Dispose() { }
    }

    private static string? Code(string answer) => (string?)JsonNode.Parse(answer)!["code"];

    private static async Task<object?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the MCP web access live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await body(owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
