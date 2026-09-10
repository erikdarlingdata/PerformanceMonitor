/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2902: budget-deferred plan/text fetch debt belongs to the collector that incurred it, and no other
/// collector may drain it.
///
/// <para><b>The defect.</b> The two carryover dictionaries were keyed by (server, database) with no
/// collector in the key, while the fetch that spends the debt is gated on a SKU flag
/// (<c>CapturePlanXml</c> for plans, <c>FetchQueryTextSeparately</c> for text) rather than on a collector
/// identity. Five collectors take the enumerated dispatch path off Azure, so all five reached the fetch —
/// and only query_store can ever CREATE debt there, because the reference extractors return empty for any
/// other collector's batch. The other four therefore arrived with nothing referenced, found query_store's
/// deferred ids under the key they shared, drained them, and had the fetch's wall clock recorded against
/// their own <c>collection_log</c> row. Measured over 38 h on one monitoring host: ~346 s of Query Store
/// fetch time billed to plan_correction, query_store_health and index_object_stats.</para>
///
/// <para><b>Why the assertions here are behavioural rather than shape.</b> "The key has three fields" is
/// not the property that matters and would pass for a key of <c>(ServerId, Database, CollectionTime)</c>,
/// which fixes nothing and breaks carry-over entirely. What matters is that debt written under one
/// collector's key is invisible under another's and still visible under its own, so both directions are
/// pinned — <see cref="NoOtherCollectorOnTheFetchPath_CanDrainTheOwnersDeferredIds"/> and
/// <see cref="TheOwningCollector_StillFindsItsOwnDeferredIds_SoTheKeyIsScopedRatherThanMerelyUnique"/> —
/// together with the two axes the widening must not have cost
/// (<see cref="TheKeyStillSeparatesServersAndDatabases_WhichTheWideningMustNotHaveCost"/>).</para>
///
/// <para><b>The family is DERIVED from <see cref="CollectorCatalog.All"/>,</b> the way
/// <c>ServerWatermarkDispatchGateTests</c> derives its own and for the same reason: a sixth enumerating
/// collector is covered the day it appears, and one that stops enumerating is un-covered automatically.
/// The only collector named by hand is the OWNER, and that is structural rather than a guess — the fetch
/// runs <c>QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery</c> and its text twin whichever collector
/// is executing, so query_store owns the debt by construction. That naming is itself checked: the owner
/// must be a member of the derived family, so if query_store ever stops enumerating this class fails
/// rather than quietly guarding nothing.</para>
///
/// <para><b>Scope, stated plainly.</b> These are unit assertions over two shipped artifacts — the
/// dictionaries' own declared key type and the runner's own key builder — plus an IL read proving the two
/// fetch bodies really call that builder. They do not run a collection cycle, so they cannot prove the
/// call site passes the RIGHT collector name; nothing short of a live sweep can. What they do prove is
/// that the key discriminates collectors, that the builder is not dead code the pin drives alone, and that
/// the discrimination survives a rebuild.</para>
/// </summary>
public class PlanFetchCarryoverKeyTests
{
    private const string PlanDebtField = "_planFetchCarryover";
    private const string TextDebtField = "_textFetchCarryover";
    private const string KeyBuilder = "FetchStateKey";

    private const int ServerId = 1;
    private const string Database = "alpha";

    /* Off Azure, because that is where the fetch lives: the runner branches on RunsPerDatabase FIRST (the
       Azure / PostgreSQL per-database connection loop, which has no plan or text fetch in it at all), and
       only the else-arm's non-null BuildEnumerationQuery drives the per-item loop the two fetches hang
       off. */
    private static readonly CollectorTargetInfo OnPrem = new() { SqlMajorVersion = 16 };

    private static CollectorContext Probe() => new()
    {
        ServerId = ServerId,
        ServerName = "alpha",
        CollectionTime = new DateTime(2026, 9, 4, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
        Target = OnPrem,
    };

    /* CollectorCatalog.All is typed as the non-generic ICollectorSchemaInfo, so the two dispatch members
       this question turns on — they live on ICollectorDefinition<TRow> — are reached by reflection, the
       same way ServerWatermarkDispatchGateTests reaches them. */
    private static bool RunsPerDatabase(ICollectorSchemaInfo definition) =>
        (bool)definition.GetType()
            .GetMethod("RunsPerDatabase", BindingFlags.Public | BindingFlags.Instance, [typeof(CollectorTargetInfo)])!
            .Invoke(definition, [OnPrem])!;

    private static bool Enumerates(ICollectorSchemaInfo definition)
    {
        var method = definition.GetType()
            .GetMethod("BuildEnumerationQuery", BindingFlags.Public | BindingFlags.Instance, [typeof(CollectorContext)])!;
        try
        {
            return method.Invoke(definition, [Probe()]) is not null;
        }
        catch (TargetInvocationException)
        {
            /* A definition that throws off its own path is not enumerating. */
            return false;
        }
    }

    /// <summary>Every collector that reaches the plan/text fetch — derived by asking the definitions.</summary>
    private static List<ICollectorSchemaInfo> FetchPathFamily() =>
        CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer
                        && !RunsPerDatabase(d)
                        && Enumerates(d))
            .ToList();

    private static string Owner => QueryStoreCollector.Instance.Name;

    private static List<string> OthersOnTheFetchPath() =>
        FetchPathFamily()
            .Select(d => d.Name)
            .Where(n => !string.Equals(n, Owner, StringComparison.Ordinal))
            .ToList();

    /// <summary>
    /// The SHIPPED key construction, reached by reflection rather than called directly, and that is
    /// load-bearing rather than ceremony: a pin whose own source names the key's arity stops COMPILING
    /// when the key changes shape instead of going red, and a regression pin that cannot go red is worth
    /// nothing. Read this way, reverting the key to (ServerId, Database) leaves this class compiling and
    /// fails the drain assertion below by collector name.
    /// </summary>
    private static object ShippedKey(int serverId, string database, string collector)
    {
        var builder = typeof(DarlingCollectorRunner)
            .GetMethod(KeyBuilder, BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            ?? throw new InvalidOperationException(
                $"DarlingCollectorRunner.{KeyBuilder} is gone, so the fetch-state key is now built somewhere "
                + "this pin cannot see and it can no longer say anything about who drains whose debt.");

        return builder.Invoke(null, [serverId, database, collector])!;
    }

    /// <summary>
    /// A fresh dictionary of the FIELD'S OWN declared type, so the key type under test is the one the
    /// runner actually stores debt in rather than one retyped here.
    /// </summary>
    private static IDictionary FreshDebtStore(string fieldName)
    {
        var field = typeof(DarlingCollectorRunner)
            .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException($"DarlingCollectorRunner.{fieldName} is gone.");

        return (IDictionary)Activator.CreateInstance(field.FieldType)!;
    }

    private static string[] DebtFields() => [PlanDebtField, TextDebtField];

    [Fact]
    public void TheFetchPathFamily_HoldsTheOwnerAndSeveralOthers_SoTheDrainPinsCannotPassVacuously()
    {
        var family = FetchPathFamily();
        var names = family.Select(d => d.Name).ToList();

        Assert.Contains(Owner, names);

        /* Floors, not equalities: the set may grow on its own but must not quietly shrink to the owner
           alone, because at that point every drain assertion below iterates an empty sequence and passes
           for the wrong reason. Five is what the catalog held when this was written — query_store,
           plan_correction, query_store_health, index_object_stats, database_scoped_config — and the three
           #2902 caught in the act are among the four non-owners. */
        Assert.True(family.Count >= 5,
            $"Only {family.Count} collectors reach the plan/text fetch ({string.Join(", ", names)}); expected "
            + "at least 5. The family shrank, so the assertions below are guarding less than they were "
            + "written to guard.");

        var others = OthersOnTheFetchPath();
        Assert.True(others.Count >= 4,
            $"Only {others.Count} collectors other than {Owner} reach the fetch ({string.Join(", ", others)}); "
            + "expected at least 4. That set IS the population #2902 measured draining another collector's "
            + "debt — if it empties, nothing here catches the regression any more.");
    }

    [Fact]
    public void NoOtherCollectorOnTheFetchPath_CanDrainTheOwnersDeferredIds()
    {
        var others = OthersOnTheFetchPath();
        Assert.NotEmpty(others);

        foreach (var fieldName in DebtFields())
        {
            var debt = FreshDebtStore(fieldName);
            debt[ShippedKey(ServerId, Database, Owner)] = new[] { 4242L, 4243L };

            foreach (var other in others)
            {
                Assert.False(
                    debt.Contains(ShippedKey(ServerId, Database, other)),
                    $"{other} finds {Owner}'s deferred ids in {fieldName} under its own key. It probed "
                    + "nothing — the reference extractors return empty for its batch — so it did not choose "
                    + "those ids, it inherited them, and the fetch they pay for is recorded against ITS "
                    + $"collection_log row instead of {Owner}'s. That is #2902.");
            }
        }
    }

    [Fact]
    public void TheOwningCollector_StillFindsItsOwnDeferredIds_SoTheKeyIsScopedRatherThanMerelyUnique()
    {
        foreach (var fieldName in DebtFields())
        {
            var debt = FreshDebtStore(fieldName);
            var owed = new[] { 4242L, 4243L };
            debt[ShippedKey(ServerId, Database, Owner)] = owed;

            /* The other half of the fix, and the half a "make every key unique" mistake would break: the
               debt exists so a plan referenced ONCE cannot starve, which requires the NEXT visit by the
               same collector to the same database to find it. A key carrying anything per-pass — a
               collection time, a cycle id — passes the drain pin above and silently drops every deferred
               id on the floor forever. */
            Assert.True(
                debt.Contains(ShippedKey(ServerId, Database, Owner)),
                $"{Owner} cannot find the ids it just deferred in {fieldName}. The key is unique per call "
                + "rather than scoped per collector, so deferred ids are dropped instead of carried and a "
                + "once-referenced plan never gets its XML.");

            Assert.Equal(owed, (long[])debt[ShippedKey(ServerId, Database, Owner)]!);
        }
    }

    [Fact]
    public void TheKeyStillSeparatesServersAndDatabases_WhichTheWideningMustNotHaveCost()
    {
        foreach (var fieldName in DebtFields())
        {
            var debt = FreshDebtStore(fieldName);
            debt[ShippedKey(ServerId, Database, Owner)] = new[] { 4242L };

            Assert.False(
                debt.Contains(ShippedKey(ServerId, "beta", Owner)),
                $"Two databases on one server share a {fieldName} entry. The debt is per-database because "
                + "the ids are per-database (plan_id and query_id are Query Store identities, scoped to the "
                + "database that issued them), so aliasing them would send one database's ids to another's "
                + "fetch.");

            Assert.False(
                debt.Contains(ShippedKey(ServerId + 1, Database, Owner)),
                $"Two servers share a {fieldName} entry for a same-named database. Every monitored fleet "
                + "has databases with the same name on different servers, so this would cross-feed them.");

            Assert.True(
                debt.Contains(ShippedKey(ServerId, Database, Owner)),
                $"The entry just written to {fieldName} is not readable back under the key that wrote "
                + "it, so the two negatives above prove nothing — everything misses.");
        }
    }

    /* Prefix-matched: the compiler appends its own ordinal (<FetchAndStorePlansAsync>d__NN). Named rather
       than enumerated so renaming a fetch fails loudly instead of quietly shrinking the set under test —
       the convention ProbeDenominatorTests established for these two bodies. */
    private static readonly string[] FetchStateMachines =
    [
        "<FetchAndStorePlansAsync>",
        "<FetchAndStoreQueryTextAsync>",
    ];

    /* Positive controls that MUST appear in both machines: the probe calls themselves. They live in another
       assembly, resolve through exactly the same MemberReference machinery as the key builder, and are
       untouched by #2902 — so a scanner that has stopped resolving tokens fails these too and cannot report
       a missing key builder for the wrong reason. */
    private static readonly string[] Controls =
    [
        "TouchAndProbePlansAsync",
        "TouchAndProbeTextsAsync",
    ];

    /// <summary>
    /// The pins above drive <c>FetchStateKey</c>; this proves the SHIPPED fetches do too, so they are not
    /// asserting things about a helper the runner ignores while it keeps building keys inline — which is
    /// exactly the shape #2902 was. Read from the built assembly's IL rather than from source text for the
    /// reason <see cref="IlCallSiteScanner"/> records: a byte scan of this same assembly has previously
    /// reported a shipped change as absent. The walk is the shared scanner as of #2898 rather than a private
    /// copy, so it decodes instructions and resolves MethodSpec instead of testing every byte offset.
    ///
    /// <para><b>What this does and does not prove.</b> It proves each fetch body builds its key through the
    /// one builder, so re-inlining a tuple literal at either site fails here. It cannot prove the value
    /// passed for the collector is the CYCLE'S collector rather than a constant — only a live sweep can say
    /// that, and it is named in this class's scope note.</para>
    /// </summary>
    [Fact]
    public void BothFetchBodies_BuildTheirKeyThroughTheOneBuilder_SoThesePinsAreNotDrivingDeadCode()
    {
        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        var scan = IlCallSiteScanner.CountCallsByStateMachine(
            assemblyPath,
            FetchStateMachines,
            [KeyBuilder, .. Controls]);

        /* Scanner guard first: a prefix that matches no type gets no key at all, so without this every
           count below would be zero and the assertion would fail for a reason unrelated to the key. */
        foreach (var machine in FetchStateMachines)
        {
            Assert.True(
                scan.ContainsKey(machine),
                $"No async state machine matching '{machine}' was found in the service assembly. The scanner "
                + "resolved nothing, so this test cannot say anything about how the fetch builds its key.");
        }

        /* Positive controls, checked BEFORE the real assertion. */
        foreach (var machine in FetchStateMachines)
        {
            var controlHits = Controls.Sum(c => scan[machine].GetValueOrDefault(c));
            Assert.True(
                controlHits > 0,
                $"{machine} contains no call to any of [{string.Join(", ", Controls)}]. Token resolution is "
                + $"broken, so a zero count for {KeyBuilder} would be meaningless.");
        }

        foreach (var machine in FetchStateMachines)
        {
            Assert.True(
                scan[machine].GetValueOrDefault(KeyBuilder) > 0,
                $"{machine} never calls {KeyBuilder}, so it builds its fetch-state key some other way — and a "
                + "key built inline is free to omit the collector again, which is precisely #2902. The "
                + "behavioural pins in this class would still pass, because they drive the builder directly.");
        }
    }
}
