/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2188: retiring the per-database <c>collector_state</c> rows query_store leaves behind for databases the
/// server no longer has. The #2022 backfill worker writes <c>done:</c> and <c>hole:</c> rows per database
/// and the #2312 open-interval stamp writes <c>qsowm:</c> the same way, and until this nothing deleted any
/// of them for a dropped or renamed database. (The #2164/#2150 watermark families this prune originally
/// existed for retired with the watermarks themselves in #2312; V77 deleted their rows wholesale.)
///
/// <para><b>What actually needs pinning is the input, not the delete.</b> A delete keyed on the wrong list is
/// the failure mode: query_store's own enumeration is filtered by ONLINE state, AG primary-ness, the
/// excluded-database list, a vendor-name screen, <c>HAS_DBACCESS</c>, and a per-database probe that can fail,
/// so a database absent from one cycle's items is far more often offline or unprobeable than dropped. Pruning
/// on that absence would delete LIVE watermarks on exactly the servers that have such databases, and because
/// the consequence is a silent refetch rather than an error, nothing downstream would ever report it.
/// <see cref="QueryStoreStatePruneLivePostgresTests"/> is where that is proven against a real store; this
/// class holds the policy and the cross-host wiring, which no store can see.</para>
///
/// <para><b>Both SKUs.</b> Lite writes <c>done:</c> and <c>hole:</c> through its own backfill worker, and
/// it only ever deletes a hole it services or expires — so the orphan class is real on both sides and the
/// prune is ported, not declared Darling-only.
/// <c>LiteWritesTheBackfillKeysButNeverTheWatermark</c> pins that in both directions, and the key set
/// itself lives in the shared <see cref="QueryStorePerDatabaseState"/> so a prefix cannot end up pruned on
/// one SKU and orphaning on the other.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreStatePruneTests
{
    /* ---------------- the design's premise, pinned without a store ---------------- */

    [Fact]
    public void LitesTwinCarriesTheSameTwoGuards()
    {
        /* The DuckDB statement is a separate string in a separate project, so nothing but a source pin keeps
           it honest. Both guards are what stop a hygiene sweep becoming a data event, and Lite is the SKU
           where a mistake lands on somebody's laptop with no DBA watching a fleet dashboard.

           DuckDB gets the freshness guard for free as the empty-snapshot guard too — `<` against a NULL MAX
           is NULL — so it carries one predicate where Darling spells out two; what must not drift is that
           the comparison against the snapshot's own timestamp is THERE. */
        var root = FindRepoRoot();
        Assert.True(root is not null, "repo root not found -- the source pin cannot run");

        var liteBackfill = File.ReadAllText(Path.Combine(
            root!, "Lite", "Services", "RemoteCollectorService.QueryStoreBackfill.cs"));

        Assert.Contains("FROM database_states", liteBackfill, StringComparison.Ordinal);
        Assert.Contains(
            "updated_at < (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)",
            liteBackfill, StringComparison.Ordinal);

        /* And the anti-join is NOT EXISTS on both sides, not NOT IN. They are not equivalent: a single NULL
           anywhere in a NOT IN list makes the whole predicate NULL, so the prune would silently stop
           retiring anything. Fail-safe, and therefore exactly the kind of divergence that would sit
           undetected for a release — the two statements answer the same question the same way or the SKUs
           have quietly forked. */
        Assert.Contains("AND   NOT EXISTS", liteBackfill, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT IN", liteBackfill, StringComparison.Ordinal);
        Assert.Contains("AND   NOT EXISTS", DarlingCollectorRunner.PruneOrphanedDatabaseStateKeysSql, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryPerDatabaseKeyPrefixQueryStoreOwnsIsAccountedFor()
    {
        /* The drift guard that matters more than the prune itself: a fourth key prefix added to either state
           class is a new orphan class if it is per-database, and nothing about adding one would fail a test.
           Derived from the state classes' own consts rather than a hand-written list, so the two cannot
           disagree.

           It demands a DECISION rather than an addition. A prefix must appear in exactly one of the two
           shared lists, because the wrong answer here is not "forgot to prune" — it is adding a
           SERVER-scoped key to PrunableKeys, whose rows can never equal prefix || databaseName and so would
           be deleted on every single cycle. The message has to say that, or the obvious fix is the bug. */
        /* Every query_store state class in the collectors assembly, discovered rather than listed: a
           hand-written pair would have made a THIRD state class invisible to this guard, which is the same
           silent-omission shape the guard exists to catch. */
        var stateClasses = typeof(QueryStorePlanXmlState).Assembly.GetTypes()
            .Where(type => type.IsClass && type.IsAbstract && type.IsSealed  /* static */
                && type.Name.StartsWith("QueryStore", StringComparison.Ordinal)
                && type.Name.EndsWith("State", StringComparison.Ordinal))
            .ToArray();

        Assert.Contains(typeof(QueryStoreBackfillState), stateClasses);
        /* #2312: the open-interval stamp, found by the discovery without being told — which is the property
           this guard exists for. Named here anyway so a rename that quietly drops it out of the pattern
           fails rather than silently shrinking the set under test. (QueryStorePlanXmlState still matches the
           name pattern but declares no key prefixes any more — its watermark retired in #2312 — so it
           contributes nothing to `declared`, which is exactly right.) */
        Assert.Contains(typeof(QueryStoreOpenIntervalState), stateClasses);

        var declared = stateClasses
            .SelectMany(type => type.GetFields(BindingFlags.Public | BindingFlags.Static))
            .Where(field => field.IsLiteral && field.FieldType == typeof(string)
                && field.Name.EndsWith("KeyPrefix", StringComparison.Ordinal))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToArray();

        Assert.NotEmpty(declared);

        var pruned = QueryStorePerDatabaseState.PrunableKeys.Select(pair => pair.Prefix).ToArray();

        foreach (var prefix in declared)
        {
            Assert.True(
                pruned.Contains(prefix, StringComparer.Ordinal)
                    || QueryStorePerDatabaseState.NotKeyedByDatabase.Contains(prefix, StringComparer.Ordinal),
                $"The query_store state key prefix '{prefix}' is in neither shared list, so #2188 has no "
                + "verdict on it. Decide which it is:\n"
                + "  - keyed by DATABASE NAME (the key is prefix + databaseName): add it to "
                + "QueryStorePerDatabaseState.PrunableKeys with its owning collector_name, and both hosts "
                + "prune it when the database is dropped.\n"
                + "  - keyed by anything ELSE (server-scoped, or a compound key): add it to "
                + "QueryStorePerDatabaseState.NotKeyedByDatabase. Do NOT put it in PrunableKeys to silence "
                + "this — both prunes test a key by rebuilding it as prefix + databaseName, so a key that "
                + "is not shaped that way matches no live database and gets DELETED every cycle.");
        }

        /* Owner and prefix must travel together: a prefix pruned under the wrong collector_name silently
           deletes nothing, which looks exactly like "there was nothing to prune". */
        Assert.Contains(
            (QueryStoreBackfillState.StateCollectorName, QueryStoreBackfillState.HoleKeyPrefix),
            QueryStorePerDatabaseState.PrunableKeys);
        /* #2312: the open-interval stamp, per database like the backfill pair, under its own owner. */
        Assert.Contains(
            (QueryStoreOpenIntervalState.StateCollectorName, QueryStoreOpenIntervalState.WatermarkKeyPrefix),
            QueryStorePerDatabaseState.PrunableKeys);
    }

    [Fact]
    public void BothHostsPruneOnTheQueryStoreCycle()
    {
        /* Wiring invisible to everything else here: delete either call and every assertion in this file
           still passes, because they drive the prunes directly. The rows would simply never be pruned in
           production. Source-pinned in BOTH hosts together, for the reason CollectorStateContractTests
           gives — a fix applied to one host and not the other is the drift this product keeps paying for.

           The GATE is pinned, not just the call: an ungated prune would run for all 38 collectors, and
           since it deletes by collector_name that would be 37 harmless no-ops hiding one real behaviour
           change nobody chose. */
        var root = FindRepoRoot();
        Assert.True(root is not null, "repo root not found -- the source pin cannot run");

        var hosts = new[]
        {
            Path.Combine(root!, "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"),
            Path.Combine(root!, "Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"),
        };

        foreach (var host in hosts)
        {
            var source = RepoFile.ReadRepoFile(host);
            var name = Path.GetFileName(host);

            var call = source.IndexOf("await PruneOrphanedQueryStoreDatabaseStateIfDueAsync(", StringComparison.Ordinal);
            Assert.True(call >= 0, $"{name} must prune orphaned per-database query_store state");

            /* The 400 characters immediately BEFORE the call — the `if` that guards it. Checking the whole
               file would be vacuous: DarlingCollectorRunner already tests definition.Name against
               query_store in five other places for unrelated reasons, so a file-wide Contains would pass
               for an ungated prune. */
            var guard = source.Substring(Math.Max(0, call - 400), Math.Min(400, call));

            Assert.Contains(
                "string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)",
                guard, StringComparison.Ordinal);

            /* And on the collector that supplies the snapshot: without this the prune runs three guaranteed
               no-op deletes per cycle forever on Azure SQL DB, and #2191's boundary is emergent rather than
               declared. */
            Assert.Contains("DatabaseStateCollector.Instance.AppliesTo(", guard, StringComparison.Ordinal);
        }
    }

    /* #2312: the RecreatedDatabase_IsBoundedByTheRefreshHorizon fact that sat here retired with the
       watermark. The recreate-with-the-same-name exposure it bounded (a recreated database restarts plan_id
       numbering, so old state suppressed the new database's XML for up to a day) no longer exists in that
       shape: the store-as-watermark probe sees a recreated database's plan_ids as unresolved-or-hash-stale
       and refetches them within ONE cycle — pinned as SQL shape in QueryStorePlanFetchTests and as a live
       round-trip in the gated Postgres suite. The prune keeps its own job either way: retiring rows for
       databases that are gone for good. */

    /// <summary>
    /// Walks up from the test output directory to the repo root — the same walk-up idiom
    /// <c>CollectorStateContractTests</c> uses.
    /// </summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
