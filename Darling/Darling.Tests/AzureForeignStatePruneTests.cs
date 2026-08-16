/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2191: the Azure SQL DB arm of the #2188 per-database state prune.
///
/// <para><b>Why it was unreachable, and why it is not now.</b> The on-prem prune anti-joins
/// <c>database_states</c>, which is an unfiltered <c>sys.databases</c> read — the only list that answers "does
/// this name still exist" without confusing a dropped database for an offline, excluded or unprobeable one.
/// <c>DatabaseStateCollector.AppliesTo</c> is <c>!IsAzureSqlDb</c>, so an Azure server has no such snapshot and
/// the prune correctly no-opped there, leaving <c>planwm:</c> / <c>done:</c> / <c>hole:</c> rows to accumulate.
/// #2191 asked for "an authoritative unfiltered sys.databases read from master, used only on the success
/// path", and rejected the per-cycle enumeration because it filters ONLINE, applies the excluded-database
/// list, and falls back to a single database on a master-access error.</para>
///
/// <para><b>#2220 removed the need for any of that.</b> A registration that names a database now sweeps only
/// that database, so its one legitimate key is derivable from the connection string's own catalog — current by
/// construction, nothing filtered, nothing that can go stale. The question changed from "which databases still
/// exist on the instance" to "which database does this registration own", and the second needs no snapshot.</para>
///
/// <para><b>What it deletes in the field.</b> Mostly #2220's residue: before that fix every Azure registration
/// swept each sibling database and wrote a watermark for it under its own <c>server_id</c>.
/// <c>collector_state</c> carries no retention, so unlike the collected rows those orphans would persist
/// indefinitely instead of ageing out — which is why this matters even though the contamination has stopped.</para>
/// </summary>
public sealed class AzureForeignStatePruneTests
{
    /// <summary>
    /// The statement keeps the registration's OWN key and nothing else — asserted on the SQL because the
    /// delete is the whole behaviour and it has no in-process seam.
    /// </summary>
    [Fact]
    public void ThePruneKeepsExactlyTheRegistrationsOwnKey()
    {
        var sql = DarlingCollectorRunner.PruneForeignDatabaseStateKeysSql;

        /* Scoped to one server, one state owner, and one key prefix, so it cannot reach another collector's
           state or another server's. */
        Assert.Contains("s.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("s.collector_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("starts_with(s.state_key, $3)", sql, StringComparison.Ordinal);

        /* The rule itself: everything under the prefix EXCEPT this registration's own database. */
        Assert.Contains("s.state_key <> $3 || $4", sql, StringComparison.Ordinal);

        /* Names what it deleted. The only symptom of a wrong delete here is a silent plan-XML refetch, so a
           rows-affected count would leave nothing to diagnose with. */
        Assert.Contains("RETURNING s.state_key", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// It must NOT carry the on-prem statement's snapshot machinery. Pinned as an absence because copying
    /// those guards over would be the natural-looking mistake, and they cannot work here: there is no
    /// snapshot to be empty or stale, so an anti-join against <c>database_states</c> — which is empty on
    /// every Azure server by design — would delete every row instead of none.
    /// </summary>
    [Fact]
    public void ThePruneDoesNotReachForASnapshotThatCannotExistOnAzure()
    {
        var sql = DarlingCollectorRunner.PruneForeignDatabaseStateKeysSql;

        Assert.DoesNotContain("database_states", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("updated_at", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT EXISTS", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The on-prem statement keeps ITS guards. Same file, opposite requirement — pinned together so a future
    /// edit cannot "simplify" the two into one shape, which would either delete every on-prem row when a
    /// snapshot is missing or leave Azure orphaning again.
    /// </summary>
    [Fact]
    public void TheOnPremPruneStillGuardsOnTheSnapshotAndItsFreshness()
    {
        var sql = DarlingCollectorRunner.PruneOrphanedDatabaseStateKeysSql;

        Assert.Contains("database_states", sql, StringComparison.Ordinal);
        Assert.Contains("snapshot.newest IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("s.updated_at < snapshot.newest", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE SAFETY PROPERTY. A registration naming no database — or naming <c>master</c>, where a
    /// catalog-less Azure connection lands — is a registration of the logical SERVER, whose legitimate
    /// database set is everything on it. Pruning against a single name there would delete every live
    /// watermark it has, so the caller's gate is <see cref="AzureSweepScope.OwnDatabaseOrEmpty"/> and this
    /// pins that the gate closes for exactly those cases.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("master")]
    [InlineData("MASTER")]
    public void ALogicalServerRegistrationIsNeverPrunedAgainstASingleName(string? catalog)
    {
        Assert.Empty(AzureSweepScope.OwnDatabaseOrEmpty(catalog));
    }

    /// <summary>And it opens for a registration that does name one, which is the case #2191 is about.</summary>
    [Theory]
    [InlineData("db1")]
    [InlineData("Payments")]
    public void ADatabaseNamedRegistrationIsPruned(string catalog)
    {
        Assert.Single(AzureSweepScope.OwnDatabaseOrEmpty(catalog));
    }

    /// <summary>
    /// Both arms iterate the SHARED prefix set, so a prefix cannot be pruned on one target type and left
    /// orphaning on the other — the drift this list exists to prevent.
    /// </summary>
    [Fact]
    public void BothArmsPruneEveryPerDatabasePrefix()
    {
        Assert.Equal(4, QueryStorePerDatabaseState.PrunableKeys.Count);
        Assert.Contains(QueryStorePerDatabaseState.PrunableKeys,
            k => k.Prefix == QueryStorePlanXmlState.WatermarkKeyPrefix);
        Assert.Contains(QueryStorePerDatabaseState.PrunableKeys,
            k => k.Prefix == QueryStoreBackfillState.DoneKeyPrefix);
        Assert.Contains(QueryStorePerDatabaseState.PrunableKeys,
            k => k.Prefix == QueryStoreBackfillState.HoleKeyPrefix);
        /* #2150: the text watermark, keyed prefix + databaseName exactly like the plan watermark. */
        Assert.Contains(QueryStorePerDatabaseState.PrunableKeys,
            k => k.Prefix == QueryStoreTextState.WatermarkKeyPrefix);

        /* Nothing may sit outside both lists — a server-scoped key added to PrunableKeys by reflex would be
           deleted every cycle. */
        Assert.Empty(QueryStorePerDatabaseState.NotKeyedByDatabase);
    }
}
