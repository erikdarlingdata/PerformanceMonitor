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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3367 gated live round-trips (DARLING_TEST_PG) for tag-SUBTREE scope resolution
/// (<see cref="CustomAlertRuleStore.ListTagMembersAsync"/>). The recursive-CTE walk cannot be exercised without a
/// real Postgres tag tree (the <c>parent_id</c> self-join, the per-root keying, and the cycle/self-parent depth
/// bound), so these pin: a parent tag with servers only under its children resolves to the whole subtree; a
/// mid-tree tag resolves to itself + its descendants but NOT its siblings or ancestors; a leaf tag resolves to
/// exactly its direct members (the pre-#3367 behavior); the walk de-dups a server reachable under several
/// descendant tags; several requested roots resolve in one call each keyed on the tag the RULE names; and a
/// malformed tree (a cycle A→B→A, a self-parent A→A) resolves safely without hanging or crashing. The pure
/// gate <see cref="CustomAlertEvaluator.RuleAppliesToServer"/> is pinned in <c>CustomAlertTagScopeTests</c>.
///
/// <para>All tags are named with a per-run prefix and deleted in cleanup (which cascades their
/// <c>server_tag_map</c> rows away), so the shared store is left as it was found. Server ids are synthetic and
/// never touch <c>config_monitored_servers</c> (the map carries no FK to it).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertTagSubtreeLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the tag-subtree live tests.");
        return connectionString!;
    }

    private static async Task<NpgsqlDataSource> MigrateAndOpenAsync(string connectionString, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        return NpgsqlDataSource.Create(dataSourceConnectionString);
    }

    /// <summary>Inserts one tag (name = <paramref name="prefix"/> + <paramref name="suffix"/>) under
    /// <paramref name="parentId"/> (NULL = a root) and returns its generated id.</summary>
    private static async Task<int> InsertTagAsync(
        NpgsqlDataSource dataSource, string prefix, string suffix, int? parentId, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "INSERT INTO server_tags (name, parent_id) VALUES ($1, $2) RETURNING id");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix + suffix });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)parentId ?? DBNull.Value });
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct));
    }

    /// <summary>Points <paramref name="tagId"/>'s parent at <paramref name="parentId"/> — used to bend a tree
    /// into a cycle or a self-parent AFTER both rows exist (the FK needs the parent present).</summary>
    private static async Task SetParentAsync(
        NpgsqlDataSource dataSource, int tagId, int parentId, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("UPDATE server_tags SET parent_id = $2 WHERE id = $1");
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = parentId });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task AssignAsync(
        NpgsqlDataSource dataSource, int serverId, int tagId, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "INSERT INTO server_tag_map (server_id, tag_id) VALUES ($1, $2)");
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Breaks any cycle first (null the parents, so a cyclic cascade cannot surprise), then deletes the
    /// run's tags — their <c>server_tag_map</c> rows cascade away with them.</summary>
    private static async Task DeleteByPrefixAsync(NpgsqlConnection cleanup, string prefix, CancellationToken ct)
    {
        using (var unparent = new NpgsqlCommand("UPDATE config.server_tags SET parent_id = NULL WHERE name LIKE $1", cleanup))
        {
            unparent.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix + "%" });
            await unparent.ExecuteNonQueryAsync(ct);
        }

        using var delete = new NpgsqlCommand("DELETE FROM config.server_tags WHERE name LIKE $1", cleanup);
        delete.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix + "%" });
        await delete.ExecuteNonQueryAsync(ct);
    }

    private static void AssertResolvesTo(
        IReadOnlyDictionary<int, IReadOnlySet<int>> result, int tagId, params int[] expected)
    {
        Assert.True(result.TryGetValue(tagId, out var actual), $"Tag {tagId} was not in the resolved map.");
        Assert.Equal(expected.OrderBy(x => x), actual!.OrderBy(x => x));
    }

    [Fact]
    public async Task Subtree_ResolvesParentToDescendants_ExcludesSiblings_AndDeDups()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var store = new CustomAlertRuleStore(dataSource);
        var prefix = "tsub_" + Guid.NewGuid().ToString("N") + "_";

        // Synthetic server ids (never real): the assertions compare exactly these sets.
        const int SvrA = 970001, SvrG = 970002, SvrB = 970003, SvrSib = 970004, SvrDup = 970005;

        var bodySucceeded = false;
        try
        {
            /* Tree:  root(R) [no direct servers] ── childA(A) ── grandchild(G)
                                                  └─ childB(B)
                      sibling(SIB) [separate root]
               Assignments: A={SvrA,SvrDup}  G={SvrG,SvrDup}  B={SvrB}  SIB={SvrSib}
               R carries NO direct server — its members come only from the subtree (the #3367 point). SvrDup sits
               under both A and G so the walk must return it once per requested root. */
            var r = await InsertTagAsync(dataSource, prefix, "R", null, ct);
            var a = await InsertTagAsync(dataSource, prefix, "A", r, ct);
            var g = await InsertTagAsync(dataSource, prefix, "G", a, ct);
            var b = await InsertTagAsync(dataSource, prefix, "B", r, ct);
            var sib = await InsertTagAsync(dataSource, prefix, "SIB", null, ct);

            await AssignAsync(dataSource, SvrA, a, ct);
            await AssignAsync(dataSource, SvrDup, a, ct);
            await AssignAsync(dataSource, SvrG, g, ct);
            await AssignAsync(dataSource, SvrDup, g, ct);
            await AssignAsync(dataSource, SvrB, b, ct);
            await AssignAsync(dataSource, SvrSib, sib, ct);

            // One round-trip for every requested root, plus a never-created id that must be absent from the map.
            const int MissingTagId = -12345;
            var result = await store.ListTagMembersAsync(new[] { r, a, g, b, sib, MissingTagId }, ct);

            // Parent with servers only under its children -> the whole subtree, SvrDup de-duped to one entry.
            AssertResolvesTo(result, r, SvrA, SvrDup, SvrG, SvrB);

            // Mid-tree tag -> itself + descendants (A's own + G's), never its sibling B or the other root SIB.
            AssertResolvesTo(result, a, SvrA, SvrDup, SvrG);

            // Leaf tags -> exactly their direct members (byte-for-byte the pre-#3367 lookup).
            AssertResolvesTo(result, g, SvrG, SvrDup);
            AssertResolvesTo(result, b, SvrB);

            // A separate root is isolated: no ancestor/sibling bleed from R's subtree.
            AssertResolvesTo(result, sib, SvrSib);

            // A tag id that does not exist resolves to nothing -> absent from the map (rule never fires).
            Assert.False(result.ContainsKey(MissingTagId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteByPrefixAsync(cleanup, prefix, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task Subtree_MalformedTree_ResolvesSafely_NoHangNoCrash()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var store = new CustomAlertRuleStore(dataSource);
        var prefix = "tsubcyc_" + Guid.NewGuid().ToString("N") + "_";

        const int SvrCycle = 980001, SvrSelf = 980002;

        var bodySucceeded = false;
        try
        {
            /* Two malformed shapes the depth bound must tame:
                 - a 2-node cycle  cycA.parent = cycB, cycB.parent = cycA  (server on cycB)
                 - a self-parent   self.parent = self                       (server on self)
               A UNION-ALL walk with no bound would spin forever on either; the `depth < 64` guard stops it. */
            var cycA = await InsertTagAsync(dataSource, prefix, "cycA", null, ct);
            var cycB = await InsertTagAsync(dataSource, prefix, "cycB", cycA, ct);
            await SetParentAsync(dataSource, cycA, cycB, ct); // close the cycle: cycA.parent = cycB
            await AssignAsync(dataSource, SvrCycle, cycB, ct);

            var self = await InsertTagAsync(dataSource, prefix, "self", null, ct);
            await SetParentAsync(dataSource, self, self, ct); // self-parent
            await AssignAsync(dataSource, SvrSelf, self, ct);

            // Both calls must RETURN (the assertion running at all proves no hang) with the correct member set.
            var cycleResult = await store.ListTagMembersAsync(new[] { cycA }, ct);
            AssertResolvesTo(cycleResult, cycA, SvrCycle); // reachable through the cycle, still resolved

            var selfResult = await store.ListTagMembersAsync(new[] { self }, ct);
            AssertResolvesTo(selfResult, self, SvrSelf);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteByPrefixAsync(cleanup, prefix, cleanupCt);
            });
        }
    }
}
