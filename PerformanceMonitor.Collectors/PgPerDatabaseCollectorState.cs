/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Every <c>collector_state</c> key a POSTGRESQL collector owns that is keyed by database name (#3153) —
/// the set the host prunes against the database list its own sweep enumerated.
///
/// <para><b>Why this is a sibling of <see cref="QueryStorePerDatabaseState"/> and not an entry in it.</b>
/// The difference is the MECHANISM, not the owner. That list is pruned by
/// <c>PruneOrphanedDatabaseStateKeysSql</c>, which anti-joins <c>collect.database_states</c> — the
/// <c>sys.databases</c> snapshot from <see cref="DatabaseStateCollector"/>, whose
/// <c>TargetEngine</c> is SQL Server, so it has NO rows for a PostgreSQL <c>server_id</c>. That statement is
/// guarded by <c>snapshot.newest IS NOT NULL</c>, so a PostgreSQL prefix added to that list would delete
/// nothing, on every cycle, forever — which is precisely the failure that list's own comment warns about:
/// a prefix pruned under a source that cannot see it "silently deletes nothing, which is indistinguishable
/// from having nothing to prune". Putting a PostgreSQL prefix there would look like a fix, pass any pin
/// that only checked the prune ran, and leave the rows orphaning.</para>
///
/// <para><b>The live-database source here is the sweep's own enumeration</b>, not a stored snapshot. A
/// <c>RunsPerDatabase</c> PostgreSQL collector already lists databases from <c>pg_database</c> to know what
/// to connect to, so the authoritative set is in the host's hand at the end of the cycle: current by
/// construction, unfilterable, and with no second collector to depend on. The host must only prune when
/// that list came back NON-EMPTY — an empty list is how a login that cannot read <c>pg_database</c>
/// presents, and pruning against it would delete every cursor on the server.</para>
///
/// <para>Membership carries the same rule as the query_store list: a key must be
/// <c>&lt;prefix&gt;&lt;databaseName&gt;</c>, because the prune reconstructs it that way. A server-scoped
/// key belongs in <see cref="NotKeyedByDatabase"/>, and
/// <c>CollectorStateContractTests.EveryDeclaredStateKeyPrefixHasAPruneVerdict</c> is what makes that a
/// decision rather than an omission — it censuses every <c>*KeyPrefix</c> in this assembly, not only the
/// ones on the query_store state classes, which is the gap that let this prefix ship unpruned.</para>
/// </summary>
public static class PgPerDatabaseCollectorState
{
    /// <summary>
    /// The (state owner, key prefix) pairs to prune. Owner and prefix travel together for the reason
    /// <see cref="QueryStorePerDatabaseState.PrunableKeys"/> gives: a prefix pruned under the wrong
    /// <c>collector_name</c> silently deletes nothing.
    ///
    /// <para>The owner here is the DEFINITION's own name, unlike the query_store entries, which carry
    /// their own <c>StateCollectorName</c>. <c>pg_index_bloat</c> declares
    /// <see cref="ICollectorSchemaInfo.StateKeys"/>, so both hosts load and persist its state under
    /// <c>definition.Name</c>, and the prune has to delete under the same name the writer used.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string Owner, string Prefix)> PrunableKeys = new[]
    {
        /* The definition's own Name, read from it rather than retyped: the owner and the name the writer
           persists under are the same fact, and a literal here would let a rename desync them silently -
           which the prune reports as "nothing to prune". */
        (PgIndexBloatCollector.Instance.Name, PgIndexBloatCollector.RotationCursorKeyPrefix),
    };

    /// <summary>
    /// PostgreSQL state key prefixes deliberately NOT pruned because they are not keyed by database name.
    /// Empty today, and it exists for the same reason its query_store twin does: so that stays a recorded
    /// decision rather than a guard failure whose obvious "fix" is to add a server-scoped key to
    /// <see cref="PrunableKeys"/> and have it deleted on every cycle.
    /// </summary>
    public static readonly IReadOnlyList<string> NotKeyedByDatabase = System.Array.Empty<string>();
}
