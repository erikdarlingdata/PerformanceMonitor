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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One PostgreSQL inner tab: which inner-tab index it occupies, what its header reads, and which
/// COLLECTORS serve it. Panels are declared by collector NAME rather than by store table, so the table can
/// only ever be what <see cref="ICollectorSchemaInfo.TargetTable"/> says it is — the name is also the key
/// <see cref="CollectorEngineCapability.NotCollectedMessage"/> takes, so a panel's data and a panel's
/// explanation for having none are keyed on the same string by construction.
/// </summary>
/// <param name="InnerTabIndex">Its position in <c>ViewerServerTab.xaml</c>'s <c>InnerTabs</c>.</param>
/// <param name="Id">A stable identifier for pins and for the SQL Server registry's deep links to collide
/// with deliberately (<c>overview</c> means the same place on either engine).</param>
/// <param name="Header">The tab strip label, which must match the XAML <c>&lt;TabItem Header=&gt;</c>.</param>
/// <param name="Collectors">Every collector whose stored rows this tab renders, in panel order.</param>
/// <param name="Note">Why these panels sit together, when that is not obvious from the header. Rendered
/// above the tab's panels. Null for a tab whose single panel needs no framing.</param>
internal sealed record ViewerPostgresTab(
    int InnerTabIndex,
    string Id,
    string Header,
    IReadOnlyList<string> Collectors,
    string? Note);

/// <summary>
/// The PostgreSQL inner-tab registry — the viewer's half of #2530, and the direct counterpart of the web
/// dashboard's <c>POSTGRES_TABS</c> in <c>server-tabs.js</c> (#2547). Same seven tabs, same ids, same
/// grouping, so the two front ends do not teach an operator two different shapes for one engine.
///
/// <para><b>Why a registry rather than "read the XAML".</b> The SQL Server surface has nineteen inner tabs
/// declared in XAML and dispatched by integer index, and nothing there is machine-readable enough to ask
/// "does every PostgreSQL collector reach a screen?". That question is the whole point of this issue: the
/// first nine PostgreSQL collectors spent three releases MCP-only precisely because no check could see that the
/// graphical surface had fallen behind the data. This list is what such a check reads, and
/// <c>ViewerPostgresTabsTests</c> derives the other side of it from <see cref="CollectorCatalog"/> — so a
/// THIRTEENTH PostgreSQL collector turns the pin red naming itself, rather than quietly shipping invisible.</para>
///
/// <para><b>Seven tabs against SQL Server's nineteen is the design.</b> Parity was never the constraint;
/// the missing tabs (tempdb, Query Store, trace flags, plan cache, <c>system_health</c>, Always On) have no
/// PostgreSQL analogue to fill, and the signals that DO matter here — wraparound headroom, the xmin
/// horizon, vacuum backlog, WAL retention — have no SQL Server analogue either. A PostgreSQL surface built
/// by asking "what does the SQL Server viewer have" would have missed most of them.</para>
///
/// <para><b>The Aurora-only panel is SHOWN, not hidden.</b> <c>pg_wait_stats</c> gates on
/// <c>CollectorTargetInfo.IsAurora</c> — it reads Aurora's own wait instrumentation, and
/// <c>pg_wait_sampling</c> is the stock-PostgreSQL answer to the same question — so on stock PostgreSQL it
/// can never have content. (<c>pg_statement_stats</c> was in this paragraph until #2625, when it learned to
/// read the vanilla <c>pg_stat_statements</c> view; its panel now fills on every PostgreSQL target.) Since
/// #2532 the reason is a sentence
/// (<see cref="CollectorEngineCapability.NotCollectedMessage"/>) naming the server, the engine, the
/// collector and the exact Aurora surface, ending "and never will". The defect #2530 is about is
/// UNEXPLAINED emptiness, not emptiness: a panel that says that is the opposite of the twelve blank SQL
/// Server tabs it replaces. Hiding them would also make the tab strip a different shape on two PostgreSQL
/// servers in one fleet, and would make the one Aurora-specific capability the product has invisible from
/// a stock instance — while re-deriving in the viewer a gate the collectors already decide.</para>
/// </summary>
internal static class ViewerPostgresTabs
{
    /// <summary>
    /// The seven tabs, in strip order. Indices continue the SQL Server run rather than displacing it: for a
    /// PostgreSQL server every SQL Server <c>TabItem</c> is collapsed and these seven are shown, so both sets
    /// keep their fixed indices and <c>ViewerServerTab</c>'s existing index constants — which drill-down
    /// navigation keys on — are untouched by this feature.
    /// </summary>
    public static readonly IReadOnlyList<ViewerPostgresTab> All = new[]
    {
        new ViewerPostgresTab(
            ViewerServerTab.PgOverviewInnerTabIndex,
            "overview",
            "Overview",
            new[] { "pg_extension_availability", "pg_server_config" },
            /* No collector of its own: it reports on every one of them, from collection_log joined to the
               catalog.
               That join is the point — a collector gated off for this engine writes NO collection_log row
               at all (pre-dispatch filtering, deliberately, so a permanent gap does not manufacture ~2,880
               fake SUCCESS rows a day), so a grid built from the log alone would show a PostgreSQL
               operator one row short and never mention the missing collector. Derived from the catalog, it
               appears with the reason it is missing. */
            "Every PostgreSQL collector for this server: when it last ran, what it returned, and — for a "
            + "collector this engine cannot run at all — why it never will. The extension panel below is the "
            + "third capability axis (#2545) and the only one that is ACTIONABLE: engine kind and edition "
            + "are walls, but an extension that is available and not installed is one command away."),

        new ViewerPostgresTab(
            ViewerServerTab.PgActivityInnerTabIndex,
            "activity",
            "Activity",
            new[] { "pg_blocking", "pg_lock_stats", "pg_statement_stats", "pg_database_stats", "pg_kernel_stats", "pg_plan_capture", "pg_deadlocks" },
            /* pg_database_stats sits under the statement grid rather than on a tab of its own because it
               answers the question the statement grid raises and cannot answer: a statement whose time
               makes no sense from its row count usually spilled, and pg_stat_database's temp counters are
               the only evidence of that we collect. */
            "What ran, what waited on what, and what it cost. Blocking is a periodic SAMPLE rather than an "
            + "event log, so the capture counts above the grid are its denominator — three chains means "
            + "something different in 60 captures than in 4."),

        new ViewerPostgresTab(
            ViewerServerTab.PgVacuumInnerTabIndex,
            "vacuum",
            "Vacuum",
            new[] { "pg_session_states", "pg_xmin_horizon", "pg_autovacuum_stats", "pg_wraparound_stats", "pg_plan_capture_readiness" },
            /* One tab, four panels, in causal order. Read separately each of the four looks survivable.

               Session states is FIRST rather than on a tab of its own because it is the link UPSTREAM of
               what was previously the first panel, and the order here is the causal one. pg_xmin_horizon
               names the CLASS of thing holding the horizon — a session, a slot, standby feedback, a
               prepared transaction — and #2540 is the panel that names WHICH session, which is where the
               fix has to be made: the horizon panel can tell an operator the problem is a backend, and
               only this one can tell them which application opened it.

               It also carries the correction the rest of the tab cannot make. A long idle-in-transaction
               session is the shape everybody recognises and it is NOT automatically a cause: measured on a
               live instance, a READ COMMITTED transaction that only read, and one whose UPDATE matched no
               rows, both sat idle in transaction indefinitely holding neither a snapshot nor a transaction
               id. Those sessions starve vacuum of nothing, and a tab that opened with a vacuum backlog
               would have an operator killing them. */
            "One story in four panels: a session holds a transaction open, that transaction holds the xmin "
            + "horizon, the horizon starves vacuum, and vacuum falling behind ends in wraparound. Read on "
            + "their own each of these looks survivable. Only the first panel can name the session, and "
            + "only it says whether that session pins anything at all — an open transaction that holds no "
            + "snapshot and no transaction id costs vacuum nothing, however long it has been idle."),

        new ViewerPostgresTab(
            ViewerServerTab.PgWaitsInnerTabIndex,
            "waits",
            "Waits",
            /* Two instruments for one question, and which of them has rows says what the server
               offers: pg_wait_stats is Aurora-native, pg_wait_sampling is the extension (#2603) that
               brings wait analysis to everything else. */
            new[] { "pg_wait_stats", "pg_wait_sampling" },
            null),

        new ViewerPostgresTab(
            ViewerServerTab.PgIoInnerTabIndex,
            "io",
            "I/O",
            new[] { "pg_io_stats", "pg_write_stats", "pg_buffer_usage" },
            /* The write side sits on the I/O tab rather than a tab of its own because it is the same
               subject from the other end. pg_stat_io says WHO issued an I/O and in what context; the write
               panel says whether the server is keeping up with the writes it was given. They also complete
               each other on 17+, where buffers_backend left pg_stat_bgwriter and its successor information
               lives only in pg_stat_io - so on a modern target the two panels together are the answer that
               either alone stopped being. */
            "Two ends of the same subject: what issued the I/O, and whether the server kept up with it. "
            + "The write panel reports the CHANGE across the window rather than the counters' levels, "
            + "because a cumulative total since the last stats reset answers nothing on its own."),

        new ViewerPostgresTab(
            ViewerServerTab.PgReplicationInnerTabIndex,
            "replication",
            "Replication",
            new[] { "pg_replication_slots", "pg_replication_stats" },
            /* Slots and connections are different facts and the tab carries both because either alone
               misleads: a slot with no standby attached is the classic way to fill a disk, and a standby
               streaming without a slot can be cut off the moment the primary recycles WAL it still needed. */
            "Two halves of one question. A SLOT is a promise to retain WAL and exists whether or not "
            + "anybody is attached; a CONNECTION is a standby actually streaming. The connection panel "
            + "reports the worst each standby reached across the window, not just where it is now - a "
            + "replica that falls far behind and recovers looks perfect in any single sample."),

        new ViewerPostgresTab(
            ViewerServerTab.PgStorageInnerTabIndex,
            "storage",
            "Storage",
            new[] { "pg_table_bloat_stats", "pg_index_usage_stats", "pg_index_bloat", "pg_column_stats", "pg_predicate_stats" },
            /* One tab, because both panels answer the same question — where is the space going and is it
               earning its keep — and the two remedies compete for the same maintenance window. Bloat is
               deliberately NOT on the Vacuum tab despite being what vacuum lag costs: that tab is the
               CAUSE chain read in causal order, and dropping the damage into the middle of it would break
               the sequence that makes those three panels one story. The bloat panel's own note points back
               at it instead.

               Bloat sits above index usage because it is the more urgent of the two and the more
               dangerous to act on: a bloat percentage is an ESTIMATE and the panel has to say so before
               anyone reads a number off it. */
            "Where the space went, and whether it is earning its keep. The bloat figures are ESTIMATES "
            + "computed from column-width statistics — the table itself is never read — so confirm one with "
            + "pgstattuple before rewriting anything. An index nothing scans is a candidate, never a "
            + "conclusion: check the constraint and validity columns beside it first. The column-statistics "
            + "panel is the INPUT those bloat estimates are computed from, and it answers a different "
            + "question of its own: why the planner chose what it chose."),
    };

    /// <summary>The first PostgreSQL tab's index — what a PostgreSQL server's tab strip selects, since
    /// every index below it belongs to a collapsed SQL Server tab.</summary>
    public static int FirstInnerTabIndex => All[0].InnerTabIndex;

    /// <summary>True when <paramref name="innerTabIndex"/> belongs to this registry.</summary>
    public static bool Owns(int innerTabIndex) => All.Any(t => t.InnerTabIndex == innerTabIndex);

    /// <summary>
    /// A tab's framing note by ID — empty for a tab that has none, and empty rather than throwing for an id
    /// this registry does not carry, because a missing note must never take a tab down with it. Looked up by
    /// id rather than by position so the strip order stays free to change.
    /// </summary>
    public static string NoteFor(string tabId) =>
        All.FirstOrDefault(t => string.Equals(t.Id, tabId, StringComparison.Ordinal))?.Note ?? string.Empty;

    /// <summary>
    /// The store table a declared collector writes, from the collector's own definition. Null for a name
    /// the catalog does not know, which <c>ViewerPostgresTabsTests</c> refuses — the registry may not name
    /// a collector that does not exist.
    /// </summary>
    public static string? TableOf(string collectorName) =>
        CollectorCatalog.Find(collectorName)?.TargetTable;

    /// <summary>
    /// Every PostgreSQL collector the catalog ships, derived rather than listed. The pins compare this
    /// against <see cref="All"/> in BOTH directions: a collector missing from the registry is a screen that
    /// was never built, and a registry entry that is not a PostgreSQL collector is a panel wired to
    /// something that will never fill it.
    /// </summary>
    public static IReadOnlyList<ICollectorSchemaInfo> PostgresCollectors() =>
        CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql)
            .OrderBy(d => d.Name, StringComparer.Ordinal)
            .ToList();
}
