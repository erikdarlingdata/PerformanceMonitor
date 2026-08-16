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
/// Every <c>collector_state</c> key query_store owns that is keyed by DATABASE NAME (#2188) — the set both
/// hosts prune when a database is dropped or renamed.
///
/// <para>Nothing ever retired these. The #2164 plan-XML watermark writes one <c>planwm:</c> row per database
/// and the #2022/#2058 backfill worker writes <c>done:</c> and <c>hole:</c>, and while the worker deletes a
/// hole when it SERVICES or expires it, a dropped database will never service one — its hole can never be
/// dug and its tail can never drain. <c>collector_state</c> is a keyed registry rather than a hypertable
/// (pinned by <c>CollectorStateContractTests</c>), so no retention policy caught them either.</para>
///
/// <para><b>Shared rather than one list per host</b>, which is the whole reason this file exists. The two
/// stores prune with different dialects (Postgres anti-join, DuckDB <c>NOT IN</c>) and the SKUs write
/// different subsets — Lite never sets <c>CollectorContext.CapturePlanXml</c>, so it writes no
/// <c>planwm:</c> at all, while both write the backfill pair. A per-host list would make a fourth prefix a
/// two-place edit whose omission fails nothing: the rows would simply orphan on one SKU, invisibly, which is
/// the drift this product keeps paying for. Both hosts iterate THIS, so a prefix is pruned everywhere or
/// nowhere. Lite running the <c>planwm:</c> statement against rows it never writes costs one no-op delete
/// and buys the guarantee that enabling plan capture there cannot quietly create an unpruned orphan
/// class.</para>
///
/// <para>Membership is a real decision, not a listing of every key: a key must be
/// <c>&lt;prefix&gt;&lt;databaseName&gt;</c>, because both prunes reconstruct it that way to test it against
/// the live database list. A server-scoped key added here would match no database and be deleted on every
/// cycle — see <see cref="NotKeyedByDatabase"/>, which records the keys deliberately left out so the
/// distinction is written down rather than rediscovered.</para>
/// </summary>
public static class QueryStorePerDatabaseState
{
    /// <summary>
    /// The (state owner, key prefix) pairs to prune, in the order the hosts run them. Owner and prefix
    /// travel together because a prefix pruned under the wrong <c>collector_name</c> silently deletes
    /// nothing, which is indistinguishable from having nothing to prune.
    /// </summary>
    public static readonly IReadOnlyList<(string Owner, string Prefix)> PrunableKeys = new[]
    {
        (QueryStorePlanXmlState.StateCollectorName, QueryStorePlanXmlState.WatermarkKeyPrefix),
        (QueryStoreBackfillState.StateCollectorName, QueryStoreBackfillState.DoneKeyPrefix),
        (QueryStoreBackfillState.StateCollectorName, QueryStoreBackfillState.HoleKeyPrefix),
        /* #2150: the text watermark is keyed prefix + databaseName exactly like the plan watermark above,
           so a dropped database's key must go with it. Paired with its OWN collector name rather than the
           plan fetch's — the two watermarks are stored separately on purpose, and a prefix pruned under
           the wrong owner silently deletes nothing. */
        (QueryStoreTextState.StateCollectorName, QueryStoreTextState.WatermarkKeyPrefix),
    };

    /// <summary>
    /// Key prefixes on the query_store state classes that are deliberately NOT pruned because they are not
    /// keyed by database name. Empty today — every prefix either state class declares is per-database — and
    /// it exists so that stays a recorded decision: the drift guard demands that every declared
    /// <c>*KeyPrefix</c> appear in one list or the other, so a new server-scoped key is a deliberate entry
    /// here rather than a test failure whose obvious "fix" is to add it to <see cref="PrunableKeys"/> and
    /// have it deleted every cycle.
    /// </summary>
    public static readonly IReadOnlyList<string> NotKeyedByDatabase = System.Array.Empty<string>();
}
