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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Collection Health tab's three ported reads (W1i), copied from Lite's
/// <c>LocalDataService.CollectionHealth.cs</c> and run on the <c>v_collection_log</c> passthrough view.
/// This REPLACES the shell's placeholder Collection Health read (the DISTINCT-ON latest-run-per-collector
/// query and its simple <c>CollectorHealthRow</c> record, both removed from <c>ViewerDataService.cs</c>)
/// with Lite's rich 7-day aggregate: per-collector run/success/error counts, average duration, last
/// success / last run / last error timestamps, and the <see cref="CollectorHealthRow.HealthStatus"/>
/// banding. The SQL is byte-portable between DuckDB and Postgres (positional params, plain aggregates),
/// so only the parameter binding differs: window <see cref="DateTime"/>s go in with
/// <c>DateTimeKind.Unspecified</c> (the naive-UTC store convention), and the SUM/COUNT/AVG results are
/// read type-agnostically (Postgres returns <c>bigint</c> for the counts and <c>numeric</c> for AVG,
/// where DuckDB returned HUGEINT/DECIMAL) — the same intent as Lite's <c>ToInt64</c>/<c>ToDouble</c>
/// helpers, minus the DuckDB BigInteger case Postgres never produces.
/// <para>
/// The <b>Health Summary</b> aggregate keeps Lite's fixed 7-day horizon (its staleness banding needs a
/// stable window regardless of the toolbar). The <b>Collection Log</b> read (feeding the log grid) diverges
/// from Lite in ONE deliberate way: it bounds <c>collection_time</c> on
/// BOTH sides so the per-server toolbar's custom From/To is honored EXACTLY — Lite (and this file's first
/// port) took a single now-relative <c>hoursBack</c> lower bound, which rounded a custom range to a
/// hours-back-from-now span. The <b>Duration Trends</b> chart does not draw that page (#4966): it has its own
/// bucketed read over the whole range (<see cref="GetCollectorDurationTrendAsync"/>).
/// </para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Lite's 7-day per-collector health aggregate (<c>GetCollectionHealthAsync</c>) verbatim: one row
    /// per collector over the trailing window, with the SKIPPED-counts-as-a-healthy-run rule baked into
    /// the last-success MAX and the PERMISSIONS bucket split out for the NO_PERMISSIONS banding. $1
    /// server_id, $2 window start (naive UTC).
    ///
    /// <para><b>Keyed lookups, not ranks (#4955).</b> The three newest-row columns — last_error, last_note and
    /// latest_run_note — used to come from three <c>ROW_NUMBER()</c> ranks over every run of the window, which
    /// sorted the whole seven days to read one row per collector. They are lookups now: the plain aggregate
    /// finds the instant each one needs, and a LATERAL join reads the winning row of that collector at that
    /// instant through <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>.
    /// The columns, their order, their types and their values are the ranks' own, tie-breaks included
    /// (<c>CollectionHealthKeyedLookupParityTests</c> holds each to the pre-change statement).</para>
    /// </summary>
    public const string CollectionHealthSql = $"""
        WITH health AS
        (
            SELECT
                collector_name,
                COUNT(*) AS total_runs,
                -- #2926: SUCCESS excludes an abandonment that predates #2803, so the Success column beside
                -- Abandoned cannot count the same run twice. Post-#2803 rows need no exclusion - ABANDONED
                -- is not SUCCESS - and an ordinary empty run stays counted, which is what the COALESCE in
                -- the shared predicate is for: NULL under this NOT would have dropped it.
                SUM(CASE WHEN status = 'SUCCESS'
                          AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
                         THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
                AVG(duration_ms) AS avg_duration_ms,
                -- SKIPPED counts as a healthy run (dedup / version-gated collectors no-op without being stale)
                MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
                MAX(collection_time) AS last_run_time,
                -- The newest failure OUTRIGHT, text or not — "when did this last fail" means the run, not
                -- the message. It can only name a different row than last_error if a failure was written
                -- with no text.
                MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN collection_time END) AS last_error_time,
                -- #4955: the instant the last_error lookup below starts from — the newest failure that
                -- CARRIED text. It can be older than last_error_time when a later failure was written with
                -- none, and it is the class (a failing status AND a message), not the status alone, so a
                -- SUCCESS row's note at the same instant cannot be the row the lookup lands on.
                MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING')
                          AND error_message IS NOT NULL
                         THEN collection_time END) AS last_failure_text_time,
                SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
                -- YIELDED = the 1s LOCK_TIMEOUT guard fired (#1805): deliberate, benign for collection,
                -- counted apart from errors because clustering here is a signal about the TARGET's lock
                -- contention rather than a monitoring fault.
                SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
                -- #4955: the instant the last_note lookup below starts from — the newest SUCCESS run that
                -- CARRIED a note (see last_note).
                MAX(CASE WHEN status = 'SUCCESS' AND error_message IS NOT NULL THEN collection_time END) AS last_note_time,
                -- How many of the window's runs carried one. note_count = total_runs is the
                -- persistently-empty signal: EVERY run this week came back with nothing.
                COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
                -- #1852: the one thing that makes a persistently-empty enumeration interesting — does this
                -- target actually HAVE user databases? Zero items on a server with none is legitimate and
                -- stays quiet; zero items on a server that HAS them is a login that cannot enter any, or a
                -- filter that swallowed everything. database_size_stats rather than database_config for
                -- three reasons: it runs on the scheduled loop (60 min) where database_config is on-load
                -- and can age past this window on a long-running service; it is indexed on
                -- (server_id, collection_time) where database_config has no index at all; and it reads
                -- sys.master_files, so it still sees databases the monitoring login cannot ENTER — exactly
                -- the case being diagnosed. database_id > 4 excludes the system databases, tempdb
                -- included: the size collector takes every ONLINE database, so a bare row check
                -- would be true on every server alive. A NULL database_id is an Azure sibling row
                -- (#2643/#3262): sys.resource_stats carries no id, and it bills only USER databases,
                -- so those rows are inventory too — without the IS NULL arm a master-connected Azure
                -- target with fifty user databases would read as having none.
                --
                -- The inventory window is the health read's OWN ($2) — no second parameter, and an
                -- inventory that aged out says nothing rather than something stale. Uncorrelated, so both
                -- engines evaluate it once per query (a Postgres InitPlan) instead of per row, and it
                -- needs no GROUP BY entry and no join. Feeds display text only, through the shared
                -- formatter; the banding never sees it.
                CASE
                    WHEN EXISTS
                         (
                             SELECT 1
                             FROM v_database_size_stats
                             WHERE server_id = $1
                             AND   collection_time >= $2
                             AND   (database_id > 4 OR database_id IS NULL)
                         )
                    THEN 1
                    ELSE 0
                END AS has_user_databases,
                -- #2804: runs the #2673 wall-clock budget abandoned. Appended last — this result set is
                -- read positionally by one shared mapper serving BOTH the per-server and fleet reads.
                --
                -- #2926: keyed on the ROW, not on the status alone. collection_log is append-only, so a
                -- window can still hold cycles written before #2803 gave abandonment its own status:
                -- status = 'SUCCESS' beside rows_collected = 0 and the budget note. Counted by status
                -- alone this read 0 for them, and the collector banded HEALTHY while losing cycles - a
                -- filter correct against current writes and silently wrong against older ones, failing in
                -- the reassuring direction. The pattern is one LIKE because the budget is INTERPOLATED and
                -- the shipped values differ (120 s for procedure_stats/query_stats/plan_correction, 600 s
                -- for query_store), so equality against one rendered sentence matches one collector.
                SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
                         THEN 1 ELSE 0 END) AS abandoned_count,
                -- #3240: runs skipped because a PostgreSQL extension the collector DECLARES is not installed
                -- — the EXTENSION_MISSING status the fault mapper split out of PERMISSIONS, counted apart so
                -- the banding stops calling an uninstalled optional extension NO_PERMISSIONS. APPENDED, never
                -- inserted: this result set is read positionally by one shared mapper.
                SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
                -- #3819: the two instants that, with last_run_time above, say whether this collector
                -- STOPPED producing rather than never having produced here. The same named skip carries
                -- opposite meanings on those two rows, and the band read both as the benign resting state.
                -- This grid bands through the SAME shared classifier as the service's reads, so it has to
                -- feed it the same inputs — left unselected they default to null, this COMPILES, and the
                -- grid would call a regressed collector HEALTHY while get_collection_health called it
                -- WARNING. That is #3240's lesson and #2804's before it. The FINDING that names the rows and
                -- the status is deliberately not carried here: this projection has no rows_stored to count
                -- and the grid has no column to render prose in, so the band is the whole of what this
                -- surface needs to agree about. APPENDED, read positionally by the one shared mapper.
                MAX(CASE WHEN status IS NULL
                          OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
                         THEN collection_time END) AS last_non_skip_time,
                MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
            GROUP BY collector_name
        )
        SELECT
            h.collector_name,
            h.total_runs,
            h.success_count,
            h.error_count,
            h.avg_duration_ms,
            h.last_success_time,
            h.last_run_time,
            -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest
            -- one. The lookup is filtered to the failing class rather than to the instant alone, which is
            -- load-bearing rather than belt-and-braces: a run of ANY other class at the same instant (a
            -- SUCCESS row's note) must not surface here as a fake last error, and when no failing run in
            -- the window carried text the lookup finds nothing and this is NULL. error_message DESC only
            -- breaks an exact-timestamp tie, and breaks it identically here and in Lite's DuckDB twin,
            -- which binary-vs-locale collation would not.
            -- #3240: EXTENSION_MISSING is in the exemplar set because its stored sentence IS the remedy
            -- (it names the extension and the database) — without it an EXTENSION_MISSING band would sit
            -- beside a blank Last Error and the operator would have to open the run log to learn why.
            failed.error_message AS last_error,
            h.last_error_time,
            h.permission_denied_count,
            h.yield_count,
            -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items,
            -- items whose enumeration probe failed). Gated on SUCCESS specifically, rather than on
            -- every non-failure status: the runners attach a note only to the SUCCESS write, and the looser
            -- complement would drag SESSION_MISSING and CANCELLED messages into a column whose whole
            -- claim is that it is NOT an error. Display text only: no band, no count, no threshold
            -- reads it, and a legitimately empty target stays HEALTHY exactly as before. The NEWEST run
            -- that carried one (#1855): a later clean run no longer blanks a note the window still holds.
            -- MAX() was never wrong about WHICH rows to consider, only about which of them wins, and text
            -- does not sort like the number #1837's probe note carries: 12 item(s) sorts below 9 item(s).
            -- collection_time settles it; error_message DESC only breaks an exact-timestamp tie.
            noted.error_message AS last_note,
            h.note_count,
            h.has_user_databases,
            h.abandoned_count,
            h.extension_missing_count,
            h.last_non_skip_time,
            h.last_productive_time,
            -- #4748: the note the collector's NEWEST run left, which is not last_note above (that is the
            -- newest run that CARRIED a note, so a clean run after a partial-failure cycle still shows the
            -- older cycle's note there). The band reads only this one, because the loss an older note names
            -- is not the collector's current state. APPENDED, read positionally by the one shared mapper.
            CASE WHEN newest.status = 'SUCCESS' THEN newest.error_message END AS latest_run_note
        FROM health h
        -- #4955: the keyed lookups. Each reads ONE collector's rows through
        -- idx_collection_log_watermark (server_id, collector_name, collection_time DESC), at an instant the
        -- aggregate above already found, instead of ranking every run of the window to reach them. The
        -- window bound ($2) is repeated on each so a chunk older than the window is excluded at plan time.
        --
        -- The newest run: the rows at the window's newest instant, the greater status first (the order
        -- the rank this replaces broke an exact tie in, the way the Darling service read breaks it;
        -- PostgreSQL's DESC puts a NULL status first).
        LEFT JOIN LATERAL
        (
            SELECT n.status,
                   n.error_message
            FROM v_collection_log n
            WHERE n.server_id = $1
            AND   n.collector_name = h.collector_name
            AND   n.collection_time >= $2
            AND   n.collection_time = h.last_run_time
            ORDER BY n.status DESC
            LIMIT 1
        ) newest ON TRUE
        -- The newest failing run that carried text. No such run: no row, and last_error is NULL.
        LEFT JOIN LATERAL
        (
            SELECT f.error_message
            FROM v_collection_log f
            WHERE f.server_id = $1
            AND   f.collector_name = h.collector_name
            AND   f.collection_time >= $2
            AND   f.collection_time = h.last_failure_text_time
            AND   f.status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING')
            AND   f.error_message IS NOT NULL
            ORDER BY f.error_message DESC
            LIMIT 1
        ) failed ON TRUE
        -- The newest SUCCESS run that carried a note.
        LEFT JOIN LATERAL
        (
            SELECT t.error_message
            FROM v_collection_log t
            WHERE t.server_id = $1
            AND   t.collector_name = h.collector_name
            AND   t.collection_time >= $2
            AND   t.collection_time = h.last_note_time
            AND   t.status = 'SUCCESS'
            AND   t.error_message IS NOT NULL
            ORDER BY t.error_message DESC
            LIMIT 1
        ) noted ON TRUE
        ORDER BY h.collector_name
        """;

    /// <summary>
    /// #1591: how many DISTINCT collectors were permission-denied in the window — the badge count for the
    /// Collection Health tab header. Lite's twin, <c>LocalDataService.GetPermissionDeniedCollectorCountAsync</c>,
    /// still runs this shape directly (DuckDB has no rollup to route through). The Darling viewer's own
    /// <see cref="GetPermissionDeniedCollectorCountAsync"/> no longer runs it: every server-tab refresh (auto
    /// default 1 min, plus tab activation) used to issue this as its OWN raw <c>v_collection_log</c> scan per
    /// open tab; it now filters the shared fleet-by-server rollup read instead (#4226), so this constant is
    /// kept only as the documented raw shape the pins below hold it to.
    ///
    /// <para>Its own narrow COUNT rather than a reuse of <see cref="CollectionHealthSql"/>: that one is
    /// per-collector and only runs when its tab is selected, which is exactly why a permission problem stayed
    /// invisible until someone thought to look. Counts collectors, not rows, so one collector failing every cycle
    /// for a week reads as "1" rather than a meaningless four-figure number.</para>
    /// </summary>
    public const string PermissionDeniedCollectorCountSql = """
        SELECT COUNT(DISTINCT collector_name)
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   status = 'PERMISSIONS'
        """;

    /// <summary>
    /// #3691 part a2: <c>collect.analysis_collection_caveats</c> (V141, part a1) — the data families the
    /// scheduled analysis pass could not read on this server, as of its most recent run. $1 server_id.
    /// </summary>
    public const string CollectionCaveatsSql = """
        SELECT family, reason, first_seen_utc, last_seen_utc
        FROM collect.analysis_collection_caveats
        WHERE server_id = $1
        ORDER BY family
        """;

    /// <summary>
    /// Reads <see cref="CollectionCaveatsSql"/> for one server — gated on the same connect-time schema probe
    /// every other rung-dependent Viewer read uses (<see cref="GetStoreSchemaVersionAsync"/>), because V141
    /// is the newest rung and a store behind it has no <c>collect.analysis_collection_caveats</c> table at
    /// all. Returns an empty list both below V141 and on any read failure, rather than letting a lagging
    /// store's missing-relation error (42P01) reach the Collection Health tab: this section is purely
    /// informational, so a store that can't answer reads the same as a store with nothing to report.
    /// </summary>
    public async Task<List<CollectionCaveatRow>> GetCollectionCaveatsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<CollectionCaveatRow>();

        /* #4767: the cached field the Query Store and trend reads use. This tab refreshes every 30 seconds by
           default and the probe is one round trip of about 130 EXISTS arms, so it is read once per session; a
           null result (the probe could not answer) is not cached and reads again. A store upgraded mid-session
           keeps reading as the older rung until the viewer reconnects, the same as those two reads. */
        var storeVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        if (storeVersion is not int version || version < 141)
        {
            return items;
        }

        try
        {
            await using var command = _dataSource.CreateCommand(CollectionCaveatsSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new CollectionCaveatRow
                {
                    Family = reader.GetString(0),
                    Reason = reader.GetString(1),
                    FirstSeenUtc = reader.GetDateTime(2),
                    LastSeenUtc = reader.GetDateTime(3),
                });
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new List<CollectionCaveatRow>();
        }

        return items;
    }

    /// <summary>
    /// #1591's badge count, one server's slice of the same 7-day window the health grid uses — served from the
    /// shared fleet-by-server rollup read (#4226) instead of its own raw <c>v_collection_log</c> scan. That read
    /// already carries <see cref="CollectorHealthRow.PermissionDeniedCount"/> per (server, collector), so the
    /// badge is a filter over rows already in memory (rollup-backed when usable, memoized briefly — see
    /// <see cref="GetFleetCollectionHealthByServerAsync"/>) rather than a fourth per-tick read of its own.
    /// </summary>
    public async Task<int> GetPermissionDeniedCollectorCountAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var byServer = await GetFleetCollectionHealthByServerAsync(cancellationToken);
        return byServer.TryGetValue(serverId, out var rows)
            ? rows.Count(row => row.PermissionDeniedCount > 0)
            : 0;
    }

    /// <summary>
    /// The fleet-cumulative variant of <see cref="CollectionHealthSql"/>: the same 16-column per-collector
    /// aggregate but across ALL enabled monitored servers (GROUP BY server_id, collector_name — one row per
    /// server/collector pair), for the status bar's aggregate-view total (mirrors Lite's cumulative
    /// GetHealthSummary(null)). Scoped to enabled servers so a removed server's aged-out rows don't read as
    /// erroring. $1 window start (naive UTC).
    /// <para>
    /// The two exemplar MESSAGE columns are deliberately NULL here rather than ranked as the per-server
    /// read ranks them (#1855). This query's only caller is <c>UpdateCollectorHealthTextAsync</c>, which
    /// reads <see cref="CollectorHealthRow.HealthStatus"/> and the collector NAME — no surface renders a
    /// fleet row's message, and the per-server read behind the Collection Health grid is where the note
    /// and the last error are actually shown. Ranking them costs more than the whole rest of the query:
    /// PostgreSQL cannot parallelize above a WindowAgg, so adding the ranks turned this from a parallel
    /// hash aggregate into a serial sort of every row in the window — 0.84s to 13.9s over a 200-server /
    /// 4M-row store, measured on PG 18.4, on a status-bar refresh that would display none of it. NULL,
    /// not the old lexicographic MAX: a fleet rollup carries band INPUTS, exactly like the service-side
    /// <c>DarlingFleetReader.FleetCollectionHealthSql</c>, which projects no message columns at all. The
    /// columns stay in the projection because both reads share <see cref="MapHealthRow"/> and its
    /// ordinals; a surface that ever needs fleet-wide exemplars needs a different read, not this one.
    /// </para>
    /// <para>
    /// #1852's <c>has_user_databases</c> is NULL here for the same two reasons and one more. It qualifies
    /// a DISPLAYED note, and this read displays none — with <c>last_note</c> already NULL the shared
    /// formatter returns blank whatever the flag says, so a real value could not change one rendered
    /// character. And the flag is per-SERVER while this read groups server_id INTO the result, so there is
    /// no single $1 to probe the inventory for: a truthful fleet version would be a second cross-collector
    /// join across every enabled server, on a query the status bar re-runs on every aggregate-tab refresh
    /// — precisely the cost #1855 measured and declined. The column exists to hold the ordinal.
    /// </para>
    /// <para>
    /// #4748: the one note this read DOES carry is the newest run's partial-database-failure note
    /// (<c>latest_run_note</c>), because the band reads it and a fleet total that banded a collector
    /// differently from its own tab is the #3240 disagreement. It rides plain aggregates rather than the
    /// ranks above, for the cost those ranks were measured at; <c>last_note</c> itself stays NULL.
    /// </para>
    /// </summary>
    public const string FleetCollectionHealthSql = $"""
        SELECT
            collector_name,
            COUNT(*) AS total_runs,
            -- #2926: SUCCESS excludes an abandonment that predates #2803, so the Success column beside
            -- Abandoned cannot count the same run twice. Post-#2803 rows need no exclusion - ABANDONED
            -- is not SUCCESS - and an ordinary empty run stays counted, which is what the COALESCE in
            -- the shared predicate is for: NULL under this NOT would have dropped it.
            SUM(CASE WHEN status = 'SUCCESS'
                      AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
                     THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
            AVG(duration_ms) AS avg_duration_ms,
            MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
            MAX(collection_time) AS last_run_time,
            CAST(NULL AS text) AS last_error,
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN collection_time END) AS last_error_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
            CAST(NULL AS text) AS last_note,
            COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
            CAST(NULL AS integer) AS has_user_databases,
            -- #2804: runs the #2673 wall-clock budget abandoned. Appended last — this result set is
            -- read positionally by one shared mapper serving BOTH the per-server and fleet reads.
            --
            -- #2926: keyed on the ROW, not on the status alone. collection_log is append-only, so a
            -- window can still hold cycles written before #2803 gave abandonment its own status:
            -- status = 'SUCCESS' beside rows_collected = 0 and the budget note. Counted by status
            -- alone this read 0 for them, and the collector banded HEALTHY while losing cycles - a
            -- filter correct against current writes and silently wrong against older ones, failing in
            -- the reassuring direction. The pattern is one LIKE because the budget is INTERPOLATED and
            -- the shipped values differ (120 s for procedure_stats/query_stats/plan_correction, 600 s
            -- for query_store), so equality against one rendered sentence matches one collector.
            SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
                     THEN 1 ELSE 0 END) AS abandoned_count,
            -- #3240: the fleet rollup bands through the SAME shared classifier as the per-server grid,
            -- so it must feed it the same inputs — an unselected count defaults to 0, COMPILES, and
            -- quietly bands an extension-missing collector FAILING here while the per-server grid says
            -- EXTENSION_MISSING (the #2804 lesson, same shape). APPENDED, read positionally.
            SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
            -- #3819: #3240's reasoning again — this rollup bands through the same shared classifier as
            -- the per-server grid, so the regression floor has to see the same inputs on both or the
            -- status bar's fleet total would disagree with the tab beside it.
            --
            -- Both are plain aggregates, so this cumulative read gains no subquery and no sort.
            MAX(CASE WHEN status IS NULL
                      OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
                     THEN collection_time END) AS last_non_skip_time,
            MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
            -- #4748: the newest run's partial-database-failure note, as plain aggregates only (no window
            -- function, no ordered aggregate, so the parallel hash aggregate survives). It is the fleet
            -- reads' one shared expression (CollectionHealthRollupSupport.LatestRunNoteRawSql, #4812), so
            -- this read, the per-server fleet read and the hourly rollup keep the same run's note.
            {CollectionHealthRollupSupport.LatestRunNoteRawSql}
        FROM v_collection_log
        WHERE collection_time >= $1
        AND   server_id IN (SELECT server_id FROM config_monitored_servers WHERE is_enabled)
        GROUP BY server_id, collector_name
        """;

    /// <summary>
    /// The Collection Log sub-tab's recent-log read, adapted from Lite's <c>GetRecentCollectionLogAsync</c>
    /// to honor the per-server toolbar's settable window EXACTLY: the collection_log rows for one server
    /// between the window's start and end bounds, newest first, capped. Where Lite (and the shell's first
    /// port) passed a single now-relative <c>hoursBack</c> lower bound — so a custom From/To rounded to a
    /// hours-back-from-now span — this bounds <c>collection_time</c> on BOTH sides, matching how the Wait
    /// Stats / Blocking tabs window their reads. $1 server_id, $2 window start, $3 window end (all naive
    /// UTC), $4 row cap.
    /// </summary>
    public const string RecentCollectionLogSql = """
        SELECT
            collector_name,
            collection_time,
            duration_ms,
            sql_duration_ms,
            duckdb_duration_ms,
            rows_collected,
            status,
            error_message,
            server_name
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time DESC
        LIMIT $4
        """;

    /// <summary>
    /// The Duration Trends chart's own read (#4966): the successful runs of one server over the WHOLE toolbar range, per
    /// collector and bucket, as the bucket's longest and average duration and its run count. The chart used to draw the
    /// Collection Log grid's page (<see cref="RecentCollectionLogSql"/>, the newest <see cref="CollectionLogRowCap"/> runs),
    /// so a server logging about 20 runs a minute drew the newest ~25 minutes of "Last 24 hours" and left the rest of the
    /// axis empty. This read has no row cap: a bucket is the unit, <see cref="CollectorDurationBucketMinutes"/> widens it
    /// until the range fits the chart budget, and the bucket origin is the one every trend read shares
    /// (<see cref="TrendBucketSql.OriginSql"/>), so a width bins the same runs the same way as the other trends. The first
    /// bucket's start is clamped to the window's start, as the other trend reads clamp it. A run counts when the chart
    /// counted it before: status SUCCESS with a duration. $1 server_id, $2 window start, $3 window end (naive UTC),
    /// $4 bucket width in minutes.
    /// </summary>
    public const string CollectorDurationTrendSql = $$"""
        SELECT
            collector_name,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            MAX(duration_ms) AS max_duration_ms,
            AVG(duration_ms) AS avg_duration_ms,
            COUNT(*) AS run_count
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   status = 'SUCCESS'
        AND   duration_ms IS NOT NULL
        GROUP BY collector_name, 2
        ORDER BY collector_name, 2
        """;

    /// <summary>
    /// Lite's per-collector drill read (<c>GetCollectionLogByCollectorAsync</c>) verbatim: every
    /// collection_log row for one collector on one server since the window start, newest first. Feeds
    /// the CollectionLogWindow the Health Summary grid opens on double-click. $1 server_id, $2
    /// collector_name, $3 window start (naive UTC).
    /// </summary>
    public const string CollectionLogByCollectorSql = """
        SELECT
            collector_name,
            collection_time,
            duration_ms,
            sql_duration_ms,
            duckdb_duration_ms,
            rows_collected,
            status,
            error_message,
            server_name
        FROM v_collection_log
        WHERE server_id = $1
        AND   collector_name = $2
        AND   collection_time >= $3
        ORDER BY collection_time DESC
        """;

    /// <summary>
    /// Per-collector health summary for one server over the trailing 7 days. Copied from Lite's
    /// <c>GetCollectionHealthAsync</c> — same window, same columns, HealthStatus computed on the row.
    /// </summary>
    public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<CollectorHealthRow>();

        await using var command = _dataSource.CreateCommand(CollectionHealthSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(MapHealthRow(reader));
        }

        return items;
    }

    /// <summary>
    /// Fleet-cumulative per-collector health across ALL enabled monitored servers over the trailing 7 days —
    /// the status bar's aggregate-view total (Overview / Alert History / FinOps / Recommendations), where Lite
    /// shows a cumulative count rather than one server's. ONE query (<see cref="FleetCollectionHealthSql"/>)
    /// regardless of fleet size; each row is a (server, collector) pair carrying its own
    /// <see cref="CollectorHealthRow.HealthStatus"/>, so the caller counts total + FAILING exactly as per-server.
    /// </summary>
    public async Task<List<CollectorHealthRow>> GetFleetCollectionHealthAsync(CancellationToken cancellationToken = default)
    {
        var items = new List<CollectorHealthRow>();

        await using var command = _dataSource.CreateCommand(FleetCollectionHealthSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(MapHealthRow(reader));
        }

        return items;
    }

    /// <summary>
    /// The per-(server, collector) breakdown <see cref="FleetCollectionHealthSql"/> groups by but does not
    /// project (#4226): identical eleven aggregates, with <c>server_id</c> added as column 0 so the Overview
    /// cards, the status bar and the server-tab badge can take their per-server counts from ONE fleet-wide
    /// read instead of one raw <c>CollectionHealthSql</c> / <see cref="PermissionDeniedCollectorCountSql"/>
    /// scan per server. Shaped for <see cref="CollectionHealthRollupSupport.ComposeFleetSql"/> — fourteen
    /// columns (#4812 appended the newest run note), in the order it requires.
    ///
    /// <para><b>Scoped to <c>server_id &lt;&gt; 0</c> (the fleet-maintenance sentinel), NOT to
    /// <c>config_monitored_servers.is_enabled</c> — a #4226 regression, found by
    /// <c>ServerSummary_ReadsEnrichedThreadsMemoryBlockingCollectors_AgainstDevPostgres</c>.</b> An earlier
    /// version of this statement copied <see cref="FleetCollectionHealthSql"/>'s <c>is_enabled</c> scope,
    /// on the reasoning that the two shared "identical" scope — but that scope belongs to a DIFFERENT
    /// consumer (the status bar's fleet-cumulative total, where a removed server's aged-out rows should
    /// not read as erroring). Every caller of THIS statement, through
    /// <see cref="GetFleetCollectionHealthByServerAsync"/>, keys its lookup by ONE server's own
    /// <c>server_id</c> — the Overview card, the badge, and a per-server status-bar tab all need that
    /// server's exact rows whether or not it is currently enabled, or even registered in
    /// <c>config_monitored_servers</c> yet (the bootstrap window before the config store is seeded, see
    /// <c>IsConfigSeededAsync</c>), exactly as their old raw per-server scans (<see cref="CollectionHealthSql"/>,
    /// <see cref="PermissionDeniedCollectorCountSql"/>) never filtered by enable state either. The right
    /// precedent was the service's own by-server fleet read this statement mirrors
    /// (<c>DarlingFleetReader.FleetCollectionHealthSql</c>, moved here via <see cref="CollectionHealthRollupSupport"/>):
    /// <c>server_id &lt;&gt; 0</c> only, same as the aggregate this composes with
    /// (<see cref="TimescaleSupport.CreateCollectionHealthHourlySql"/>). The one caller that still wants
    /// "enabled fleet only" — the status bar's cumulative branch, no tab scope selected — applies that
    /// filter itself against the registry already in memory (<c>MainWindow.ServerManagement.cs</c>'s
    /// <c>UpdateCollectorHealthTextAsync</c>), rather than baking it into the shared read every other
    /// caller also pays for.</para>
    /// </summary>
    public const string FleetCollectionHealthByServerSql = $"""
        SELECT
            server_id,
            collector_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'SUCCESS'
                      AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
                     THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
            MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            MAX(collection_time) AS last_run_time,
            SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
                     THEN 1 ELSE 0 END) AS abandoned_count,
            SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
            MAX(CASE WHEN status IS NULL
                      OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
                     THEN collection_time END) AS last_non_skip_time,
            MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
            MAX(CASE WHEN NOT (status = 'SUCCESS'
                               AND COALESCE(rows_collected, 0) = 0
                               AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
                     THEN collection_time END) AS last_zero_row_streak_break_time,
            -- #4812: the newest run's partial-database-failure note, so the Overview cards band a collector that
            -- lost half its databases the way its own Collection Health tab does (the rollup keeps the same value
            -- per hour). Plain aggregates only. APPENDED, read positionally.
            {CollectionHealthRollupSupport.LatestRunNoteRawSql}
        FROM v_collection_log
        WHERE collection_time >= $1
        AND   server_id <> 0
        GROUP BY server_id, collector_name
        """;

    /// <summary><see cref="FleetCollectionHealthByServerSql"/> served from
    /// <c>collect.collection_health_hourly</c> when <see cref="CollectionHealthRollupSupport.RollupUsableAsync"/>
    /// says the rollup is usable, computed once (#4226).</summary>
    private static readonly string FleetCollectionHealthByServerComposedSql =
        CollectionHealthRollupSupport.ComposeFleetSql(FleetCollectionHealthByServerSql);

    /// <summary>#4477: the single-flight, TTL-memoized gate <see cref="GetFleetCollectionHealthByServerAsync"/>
    /// reads through — <see cref="SingleFlightTtlCache{T}"/> in <c>PerformanceMonitor.Common</c>. The old
    /// "benignly racy" TTL cache let every concurrent caller that missed a cold cache start its OWN fleet-wide
    /// scan, because nothing was shared until the first one finished and wrote back — on a production fleet
    /// the Overview loader's per-server lanes did this once per card, measured at 40 store round trips in one
    /// 4.5-minute session (#4477). Re-probed at most every 20 s (#4226): a fresh call within the window is
    /// served from memory with no store round trip at all.</summary>
    private readonly SingleFlightTtlCache<Dictionary<int, List<CollectorHealthRow>>> _fleetHealthByServerCache =
        new(TimeSpan.FromSeconds(20));

    /// <summary>
    /// The 7-day per-(server, collector) health breakdown, ONE store round trip (rollup-backed when usable,
    /// else the exact raw scan), grouped by server for the caller — the read #4226 gives the Overview cards and
    /// the status bar so a 30 s refresh tick costs one fleet-wide read instead of 43 per-server raw scans plus a
    /// second raw fleet scan. Memoized for the 20 s TTL <see cref="_fleetHealthByServerCache"/> carries, and
    /// single-flighted (#4477) so a whole Overview refresh — every card's lane racing a cold cache at once —
    /// still issues exactly one fleet-wide statement rather than one per racing lane.
    /// </summary>
    public Task<Dictionary<int, List<CollectorHealthRow>>> GetFleetCollectionHealthByServerAsync(CancellationToken cancellationToken = default)
        => _fleetHealthByServerCache.GetOrStartAsync(FetchFleetCollectionHealthByServerAsync, cancellationToken);

    /// <summary>The actual fleet-wide fetch behind <see cref="GetFleetCollectionHealthByServerAsync"/>'s
    /// single-flight gate — runs exactly once per cold cache regardless of how many callers are racing it.
    /// Runs with <see cref="CancellationToken.None"/> (via <see cref="SingleFlightTtlCache{T}"/>): it is
    /// shared work, not any one caller's, so one caller cancelling its own wait must not cancel the read for
    /// the others still waiting on it.</summary>
    private async Task<Dictionary<int, List<CollectorHealthRow>>> FetchFleetCollectionHealthByServerAsync()
    {
        var now = DateTime.UtcNow;
        var windowStart = DateTime.SpecifyKind(now.AddDays(-7), DateTimeKind.Unspecified);
        var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);
        var plan = await CollectionHealthRollupSupport.RollupPlanAsync(_dataSource, headEnd, CancellationToken.None);

        /* #4477: any hole hours below the watermark are read raw alongside the rollup instead of forcing this
           whole 7-day window to the raw scan — the shape that sent the Viewer's fleet health read to the raw
           arm 819 times in three days on a store measured at 6.6 s (raw) against 184 ms (composed). */
        var composedSql = plan.Usable
            ? (plan.HoleHours.Count > 0
                ? CollectionHealthRollupSupport.ComposeFleetSql(FleetCollectionHealthByServerSql, plan.HoleHours)
                : FleetCollectionHealthByServerComposedSql)
            : null;
        await using var command = _dataSource.CreateCommand(composedSql ?? FleetCollectionHealthByServerSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
        if (plan.Usable)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = headEnd });
            if (plan.HoleHours.Count > 0)
            {
                command.Parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = plan.HoleHours as DateTime[] ?? plan.HoleHours.ToArray() });
            }
        }

        var byServer = new Dictionary<int, List<CollectorHealthRow>>();
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var serverId = reader.GetInt32(0);
            if (!byServer.TryGetValue(serverId, out var rows))
            {
                rows = new List<CollectorHealthRow>();
                byServer[serverId] = rows;
            }

            rows.Add(MapFleetByServerRow(reader));
        }

        return byServer;
    }

    /// <summary>Maps one row of <see cref="FleetCollectionHealthByServerSql"/> / its composed twin (ordinals
    /// 0-13) to a <see cref="CollectorHealthRow"/>. Only the fields <see cref="CollectorHealthRow.HealthStatus"/>
    /// and <see cref="CollectorHealthRow.RegressedFromProductive"/> read are populated — the Overview cards and
    /// the status bar band collectors and count them, and render neither an exemplar message nor a note
    /// (#4226); AvgDurationMs, LastError(Time), YieldCount, LastNote, NoteCount and TargetHasUserDatabases stay
    /// at their defaults, as they never reach this projection.</summary>
    internal static CollectorHealthRow MapFleetByServerRow(System.Data.Common.DbDataReader reader) => new()
    {
        CollectorName = reader.GetString(1),
        TotalRuns = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
        SuccessCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
        ErrorCount = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)),
        LastSuccessTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        PermissionDeniedCount = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
        LastRunTime = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
        AbandonedCount = reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8)),
        ExtensionMissingCount = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
        LastNonSkipTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
        LastProductiveTime = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
        /* Appended (#4812): the newest run's partial-database-failure note, which the band reads. Left unmapped,
           the Overview card would band a collector that lost half its databases HEALTHY beside its own tab's
           WARNING. */
        LatestRunNote = reader.IsDBNull(13) ? null : reader.GetString(13),
    };

    /// <summary>Maps one row of the shared 19-column health projection (per-server or fleet, ordinals 0-18) to a
    /// <see cref="CollectorHealthRow"/>. The count is load-bearing: both projections are read POSITIONALLY
    /// through this one mapper, so it must match them exactly (19 since #4748 appended latest_run_note at
    /// ordinal 18; #3819's last_non_skip_time and last_productive_time sit at ordinals 16-17,
    /// #3240's extension_missing_count at 15 and #2804's abandoned_count at 14).</summary>
    private static CollectorHealthRow MapHealthRow(NpgsqlDataReader reader) => new()
    {
        CollectorName = reader.GetString(0),
        TotalRuns = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
        SuccessCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
        ErrorCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
        AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
        LastSuccessTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        LastRunTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
        LastError = reader.IsDBNull(7) ? null : reader.GetString(7),
        LastErrorTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
        PermissionDeniedCount = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
        YieldCount = reader.IsDBNull(10) ? 0 : Convert.ToInt64(reader.GetValue(10)),
        LastNote = reader.IsDBNull(11) ? null : reader.GetString(11),
        NoteCount = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
        /* #1852: NULL on the fleet read (see FleetCollectionHealthSql) reads as "no inventory to go on",
           which is the same silence an install with no size stats gets. */
        TargetHasUserDatabases = !reader.IsDBNull(13) && Convert.ToInt64(reader.GetValue(13)) != 0,
        /* Appended (#2804). Both reads above compute it, so unlike has_user_databases it is never a
           NULL placeholder on the fleet side — an abandoned cycle is data loss on either surface. */
        AbandonedCount = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
        /* Appended (#3240). Both reads compute it, for the same reason: the band it feeds must agree
           between the per-server grid and the fleet rollup. */
        ExtensionMissingCount = reader.IsDBNull(15) ? 0 : Convert.ToInt64(reader.GetValue(15)),
        /* Appended (#3819). Computed by BOTH reads, for the reason the two counts above are: the band
           floor they feed must agree between the grid and the fleet total. */
        LastNonSkipTime = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
        LastProductiveTime = reader.IsDBNull(17) ? null : reader.GetDateTime(17),
        /* Appended (#4748). Both reads compute it, so the band agrees between the grid and the fleet total. */
        LatestRunNote = reader.IsDBNull(18) ? null : reader.GetString(18),
    };

    /// <summary>
    /// The Collection Log grid's row cap: the newest 500 runs by collection time. <see cref="GetRecentCollectionLogAsync"/> binds it to
    /// <see cref="RecentCollectionLogSql"/>'s <c>LIMIT $4</c> by default, and the tab names the same number to the "Showing since"
    /// note (#4966), so a read that returned this many runs names its oldest one: the grid reaches back no further.
    /// </summary>
    public const int CollectionLogRowCap = 500;

    /// <summary>
    /// The per-collector drill's window in hours (the trailing 7 days): <see cref="GetCollectionLogByCollectorAsync(int, string, int, CancellationToken)"/>
    /// reads from this far back by default, and the drill window works one start out of it for both its read and the "Showing since"
    /// probe (#4966). The read has no row cap, so the note follows the coverage rule.
    /// </summary>
    public const int CollectionLogDrillHours = 168;

    /// <summary>
    /// Where this server's collection log starts for the window (#4966), through the shared probe (<see cref="DataWindowFloor"/>)
    /// over the log's own source (<see cref="DataWindowFloor.Source.ForCollectionLog"/>): the later of the server's first collection
    /// and the log's own retention edge, or its first run in the window if that is earlier. Both Collection Log surfaces ask it, the
    /// grid over the toolbar's range and the per-collector drill over its trailing week. Null when the window holds no logged run,
    /// when it lies wholly before the coverage, and for a window of 90 minutes or less (the probe starts no query for one).
    /// </summary>
    public Task<DateTime?> GetCollectionLogDataStartAsync(int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DataWindowFloor.GetForServerAsync(_dataSource, DataWindowFloor.Source.ForCollectionLog(), serverId, startUtc, endUtc,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>
    /// Collection_log entries for one server between the window's naive-UTC start/end bounds, most recent
    /// first (<see cref="CollectionLogRowCap"/>-row cap). Feeds the Collection Log sub-tab grid, and only the grid: the Duration Trends
    /// chart reads its own buckets over the whole range (<see cref="GetCollectorDurationTrendAsync"/>, #4966), because this page ends
    /// wherever the 500th newest run falls. The window
    /// is the per-server toolbar's settable range (<c>GetWindowUtc</c>): a preset ends "now", a custom
    /// From/To bounds EXACTLY — unlike the old hours-back read, a custom range no longer rounds to a
    /// hours-back-from-now span. Mirrors how <see cref="GetDistinctWaitTypesAsync"/> windows its read.
    /// </summary>
    public async Task<List<CollectionLogRow>> GetRecentCollectionLogAsync(int serverId, DateTime startUtc, DateTime endUtc, int maxRows = CollectionLogRowCap, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RecentCollectionLogSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = maxRows });

        return await ReadCollectionLogAsync(command, cancellationToken);
    }

    /// <summary>
    /// The bucket width, in minutes, the Duration Trends chart reads at for the range (#4966): the shared ladder's narrowest width that
    /// keeps the range within <see cref="TrendBudget.Chart"/>'s point budget, sized as <see cref="GetCpuUtilizationAsync"/> and the
    /// blocking trends size theirs (one series, so a collector's line holds at most that many points). A minute for 24 hours, ten
    /// for 7 days. A function of its own so a test holds it to the shared helper without a store.
    /// </summary>
    /// <param name="startUtc">The range's start.</param>
    /// <param name="endUtc">The range's end.</param>
    public static int CollectorDurationBucketMinutes(DateTime startUtc, DateTime endUtc)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        return TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);
    }

    /// <summary>
    /// The Duration Trends chart's feed (#4966): per collector and bucket, the longest and average duration of the successful runs
    /// and their count, over the whole range (<see cref="CollectorDurationTrendSql"/>), in collector then time order. Read beside
    /// <see cref="GetRecentCollectionLogAsync"/>, not from it: the grid's page keeps the newest <see cref="CollectionLogRowCap"/>
    /// runs, and the chart's axis spans the range. Each bucket is <see cref="CollectorDurationBucketMinutes"/> wide.
    /// </summary>
    public async Task<List<CollectorDurationBucket>> GetCollectorDurationTrendAsync(int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CollectorDurationTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = CollectorDurationBucketMinutes(startUtc, endUtc) });

        var items = new List<CollectorDurationBucket>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* MAX and AVG read type-agnostically (an integer column's AVG is numeric); COUNT is bigint. The WHERE keeps NULL durations out,
               so no bucket here has a NULL maximum or average. */
            items.Add(new CollectorDurationBucket(
                reader.GetString(0),
                reader.GetDateTime(1),
                Convert.ToInt32(reader.GetValue(2)),
                Convert.ToDouble(reader.GetValue(3)),
                reader.GetInt64(4)));
        }

        return items;
    }

    /// <summary>
    /// Collection_log entries for a specific collector on one server, most recent first, from <paramref name="hoursBack"/> hours
    /// before now. Copied from Lite's <c>GetCollectionLogByCollectorAsync</c> (default 168-hour / 7-day window). It reads from the
    /// start <see cref="GetCollectionLogByCollectorAsync(int, string, DateTime, CancellationToken)"/> takes, which is where a caller
    /// that also asks the "Showing since" probe about the window names the one start for both.
    /// </summary>
    public Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync(int serverId, string collectorName, int hoursBack = CollectionLogDrillHours, CancellationToken cancellationToken = default) =>
        GetCollectionLogByCollectorAsync(serverId, collectorName, DateTime.UtcNow.AddHours(-hoursBack), cancellationToken);

    /// <summary>
    /// Collection_log entries for a specific collector on one server since <paramref name="startUtc"/> (naive UTC), most recent
    /// first, with no row cap and no upper bound. Feeds the CollectionLogWindow drill, which passes the start its
    /// <see cref="GetCollectionLogDataStartAsync"/> probe asks about (#4966): a start the read worked out from the clock on its own
    /// would move apart from the probe's whenever the drill pins the window's end.
    /// </summary>
    public async Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync(int serverId, string collectorName, DateTime startUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CollectionLogByCollectorSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = collectorName });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });

        return await ReadCollectionLogAsync(command, cancellationToken);
    }

    /// <summary>
    /// #4825: the run records a manual <c>purge_now</c> writes when its background run finishes. The purge is fleet-wide,
    /// so they sit under the reserved fleet server_id (0, <c>DarlingObservability.FleetServerId</c>) as
    /// <c>data_retention</c> rows, which is why this reads the table rather than the per-server
    /// <c>v_collection_log</c> reads above. Two records per run: the sweep's totals, then (on a TimescaleDB store)
    /// the raw-table line whose text contains <c>, raw tables:</c>. Both lead with the run label, "Manual purge
    /// (purge_now" plus a custom horizon when one was set, which is what tells them apart from the daily purge's
    /// rows. Oldest first. $1 is the lower bound: the <c>startedAtUtc</c> the service answered the command with
    /// (naive UTC, the service's own clock, the one <c>collection_time</c> is written from).
    /// </summary>
    public const string ManualPurgeRunRecordsSql = """
        SELECT
            collection_time,
            status,
            error_message,
            rows_collected,
            duration_ms
        FROM collect.collection_log
        WHERE server_id = 0
        AND   collector_name = 'data_retention'
        AND   collection_time >= $1
        AND   error_message LIKE 'Manual purge (purge_now%'
        ORDER BY collection_time
        """;

    /// <summary>
    /// The manual purge's run records written at or after <paramref name="sinceUtc"/> (see
    /// <see cref="ManualPurgeRunRecordsSql"/>), oldest first. <paramref name="sinceUtc"/> is the service's
    /// <c>startedAtUtc</c>; it goes in as naive UTC.
    /// </summary>
    public async Task<List<ManualPurgeRunRecord>> GetManualPurgeRunRecordsAsync(DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ManualPurgeRunRecordsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });

        var items = new List<ManualPurgeRunRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ManualPurgeRunRecord(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4))));
        }

        return items;
    }

    /// <summary>Shared reader for the two collection-log projections (identical column list).</summary>
    private static async Task<List<CollectionLogRow>> ReadCollectionLogAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var items = new List<CollectionLogRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new CollectionLogRow
            {
                CollectorName = reader.GetString(0),
                CollectionTime = reader.GetDateTime(1),
                DurationMs = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2)),
                SqlDurationMs = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                DuckDbDurationMs = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4)),
                RowsCollected = reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5)),
                Status = reader.GetString(6),
                ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
                ServerName = reader.IsDBNull(8) ? null : reader.GetString(8),
            });
        }

        return items;
    }
}

/// <summary>
/// One <c>data_retention</c> run record a manual <c>purge_now</c> wrote to collection_log (#4825): the sweep's
/// totals line, or the raw-table line written after the gated raw step. <see cref="ErrorMessage"/> carries the
/// summary text (the column holds the message for every status, not only failures); the raw-table record is the
/// one whose text contains <c>, raw tables:</c>.
/// </summary>
public sealed record ManualPurgeRunRecord(
    DateTime CollectionTime, string Status, string? ErrorMessage, int? RowsCollected, int? DurationMs);

/// <summary>
/// One collector's successful runs in one bucket of the Duration Trends chart (#4966): the longest and the average duration among
/// them and how many runs that is. <see cref="BucketStart"/> is the bucket's start in the store's naive UTC, clamped to the range's
/// start for the first bucket. The chart draws <see cref="MaxDurationMs"/>, so a slow run shows however wide the bucket is.
/// </summary>
public sealed record CollectorDurationBucket(
    string CollectorName, DateTime BucketStart, int MaxDurationMs, double AvgDurationMs, long RunCount);

/// <summary>
/// One row of the Collection Log grid / drill window — a single collector run's outcome. Copied
/// VERBATIM from Lite's <c>CollectionLogRow</c> (LocalDataService.CollectionHealth.cs): every display
/// property is a pure format of stored values, and <see cref="CollectionTimeFormatted"/> routes the
/// store's naive-UTC collection_time through <see cref="ViewerTimeHelper.ForDisplay"/> — the viewer's
/// mode-aware Server/Local/UTC conversion for text, which every other Darling grid timestamp also uses. The
/// collector-duration chart does not use it: it plots <c>CollectionTime</c> itself as X, the naive-UTC
/// instant, and draws it in the display zone (#4766).
/// <see cref="DuckDbDurationMs"/> keeps its store column name (<c>duckdb_duration_ms</c>)
/// but in the Darling store that column records the POSTGRES write phase — the Collection Log grid
/// labels it "Store (ms)".
/// </summary>
public class CollectionLogRow
{
    public string CollectorName { get; set; } = "";
    public string? ServerName { get; set; }
    public DateTime CollectionTime { get; set; }
    public int? DurationMs { get; set; }
    public int? SqlDurationMs { get; set; }

    /// <summary>Stored under the legacy <c>duckdb_duration_ms</c> column name; in the Darling store it
    /// carries the Postgres storage-phase milliseconds. Surfaced as the grid's "Store (ms)" column.</summary>
    public int? DuckDbDurationMs { get; set; }
    public int? RowsCollected { get; set; }
    public string Status { get; set; } = "";
    public string? ErrorMessage { get; set; }

    public string CollectionTimeFormatted => ViewerTimeHelper.FormatForDisplay(CollectionTime, "g");

    public string DurationFormatted => DurationMs.HasValue
        ? (DurationMs.Value < 1000 ? $"{DurationMs.Value} ms" : $"{DurationMs.Value / 1000.0:F1} s")
        : "";

    public string SqlDurationFormatted => SqlDurationMs.HasValue ? $"{SqlDurationMs.Value} ms" : "";

    public string DuckDbDurationFormatted => DuckDbDurationMs.HasValue ? $"{DuckDbDurationMs.Value} ms" : "";
}

/// <summary>
/// One Collection Health "Health Summary" grid row — a collector's 7-day roll-up with its health band.
/// Copied VERBATIM from Lite's rich <c>CollectorHealthRow</c> (LocalDataService.CollectionHealth.cs);
/// it REPLACES the shell's placeholder <c>CollectorHealthRow</c> record (a single latest-run snapshot).
/// Every property is a pure computation over the aggregate — <see cref="HealthStatus"/> delegates to the
/// shared <see cref="CollectorHealthClassifier"/> in PerformanceMonitor.Common (#1573), so Lite, this
/// viewer, and the service band identically and cannot drift; it resolves the collector's cadence from the
/// shared <see cref="CollectorScheduleDefaults"/> so a healthy DAILY collector is no longer flagged
/// stale/failing on the frequent-collector thresholds. The <see cref="DateTime.UtcNow"/> arithmetic in
/// <see cref="HoursSinceLastSuccess"/> is correct against the store's naive-UTC timestamps because both
/// sides are UTC instants (tick subtraction ignores Kind), matching Lite.
/// </summary>
public class CollectorHealthRow
{
    public string CollectorName { get; set; } = "";
    public long TotalRuns { get; set; }
    public long SuccessCount { get; set; }
    public long ErrorCount { get; set; }
    public double AvgDurationMs { get; set; }
    public DateTime? LastSuccessTime { get; set; }
    public DateTime? LastRunTime { get; set; }
    public string? LastError { get; set; }
    public DateTime? LastErrorTime { get; set; }
    public long PermissionDeniedCount { get; set; }

    /// <summary>Runs skipped because a PostgreSQL extension the collector declares is not installed
    /// (#3240) — the <c>EXTENSION_MISSING</c> status split out of PERMISSIONS so an uninstalled optional
    /// extension stops banding NO_PERMISSIONS. Always 0 for SQL Server collectors.</summary>
    public long ExtensionMissingCount { get; set; }

    /* ── Regressed from productive (#3819) ────────────────────────────────────────────────────────
       A named skip on a collector that had been producing is a different fact from the same status on
       one that never has. These two instants are what tells them apart; the
       predicate below is composed from the shared classifier so this grid, the service's MCP tool and
       Lite's grid cannot answer differently. The SENTENCE that names the rows and the status stays with
       the two surfaces that have a rows_stored to count and a field to render it in. */

    /// <summary>
    /// The newest run whose status was NOT one of <c>CollectorRuntimePrecondition.NamedSkipStatuses</c>
    /// (<c>last_non_skip_time</c>) — the instant the current skip streak began after. Null when every
    /// run in the window was a skip, which is the never-produced-here case the benign band already
    /// describes correctly.
    /// </summary>
    public DateTime? LastNonSkipTime { get; set; }

    /// <summary>
    /// The newest run that stored anything (<c>last_productive_time</c>). Its ORDER against
    /// <see cref="LastNonSkipTime"/> is what makes a regression a regression rather than two unrelated
    /// facts.
    /// </summary>
    public DateTime? LastProductiveTime { get; set; }

    /// <summary>
    /// Whether this collector WAS producing rows and now reports a named skip every cycle (#3819) — the
    /// distinction <see cref="HealthStatus"/> could not make on its own, because the benign skip bands
    /// are gated on the window holding no success and a regressed collector's window holds its
    /// productive days.
    /// </summary>
    public bool RegressedFromProductive => CollectorHealthClassifier.RegressedFromProductive(
        LastRunTime, LastNonSkipTime, LastProductiveTime);

    /// <summary>1s lock-timeout yields (#1805) — deliberate, benign, counted apart from errors.</summary>
    public long YieldCount { get; set; }

    /// <summary>Runs the #2673 whole-server wall-clock budget gave up on (#2804). Counted apart from
    /// errors for the same reason <see cref="YieldCount"/> is — a guard firing is not a fault — but unlike
    /// a yield it is data LOSS: the cycle stored nothing and advanced no watermark.</summary>
    public long AbandonedCount { get; set; }

    /// <summary>
    /// The note a non-failing run left behind (#1837): an enumeration that yielded 0 items, items whose
    /// enumeration probe failed. Null for the ordinary run, which is why the column reads blank for a
    /// plainly healthy collector. Informational only — see <see cref="NoteFormatted"/>.
    /// </summary>
    public string? LastNote { get; set; }

    /// <summary>How many of <see cref="TotalRuns"/> carried a <see cref="LastNote"/>.</summary>
    public long NoteCount { get; set; }

    /// <summary>
    /// The note the collector's NEWEST run left (#4748), or null when that run left none. Unlike
    /// <see cref="LastNote"/>, which is the newest note in the window whatever run wrote it, this is the
    /// newest RUN's, so a clean run after a partial-failure cycle clears it. It is the one note the band reads
    /// (<see cref="CollectorHealthClassifier.Classify"/>): a cycle that lost half or more of its databases
    /// still records SUCCESS, and the note is the only record of the loss.
    /// </summary>
    public string? LatestRunNote { get; set; }

    /// <summary>
    /// #1852: whether the store saw user databases on this target inside the health window
    /// (<c>has_user_databases</c>) — what tells a legitimately empty server apart from one that is
    /// enumerating nothing despite having databases. False also covers "no inventory to go on" (the fleet
    /// read's NULL, an install without size stats), which deliberately reads the same as "nothing to say".
    /// Informational, like <see cref="LastNote"/>: <see cref="HealthStatus"/> never sees it.
    /// </summary>
    public bool TargetHasUserDatabases { get; set; }

    public double FailureRatePercent => TotalRuns > 0 ? (double)ErrorCount / TotalRuns * 100 : 0;

    /// <summary>Share of runs the #2673 budget abandoned (#2804) — its own rate, not folded into
    /// <see cref="FailureRatePercent"/>, because the two carry very different thresholds and merging
    /// them would report a 2%-abandoning collector as a 2%-erroring one.</summary>
    public double AbandonRatePercent => TotalRuns > 0 ? (double)AbandonedCount / TotalRuns * 100 : 0;
    public double HoursSinceLastSuccess => LastSuccessTime.HasValue
        ? (DateTime.UtcNow - LastSuccessTime.Value).TotalHours
        : 999;

    /// <summary>Hours since the newest run of ANY status — the input <see cref="CollectorHealthClassifier"/>'s
    /// STOPPED band reads. Distinct from <see cref="HoursSinceLastSuccess"/>: a collector that keeps being
    /// invoked and keeps failing has a small value here even while its success clock runs out; a collector
    /// whose gate flipped off and stopped being invoked entirely has a large value here too, which is what
    /// tells the two apart. Falls back to <see cref="HoursSinceLastSuccess"/> rather than the bare 999
    /// sentinel when the column is unset: a run can never be MORE certain than a known success, so absent
    /// better information this must not read more dormant than the success clock alone already says.</summary>
    public double HoursSinceLastRun => LastRunTime.HasValue
        ? (DateTime.UtcNow - LastRunTime.Value).TotalHours
        : HoursSinceLastSuccess;

    /// <summary>The collector's cadence, routed through <c>EffectiveRecurringIntervalMinutes</c> (#4000) so an
    /// on-load collector's catalog 0 reads as the daily recapture interval, which is what lets
    /// <see cref="CollectorHealthClassifier.Classify"/> band it on the SAME ladder as any other. A name the
    /// catalog doesn't know keeps 0 and the classifier's floor thresholds, as before #4000: resolving it to
    /// daily too would leave a collector that went dark HEALTHY for a day and a half. The banding uses the
    /// shipped default, not any per-install override: the viewer has no cheap per-collector effective
    /// frequency at the row level, and using the same default across all three surfaces keeps them in
    /// parity.</summary>
    private int FrequencyMinutes =>
        CollectorScheduleDefaults.All.TryGetValue(CollectorName, out var schedule)
            ? CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes)
            : 0;

    /// <summary>
    /// The row's band: the shared ladder's verdict, with #3819's regression FLOOR applied over it —
    /// WARNING where the ladder said HEALTHY and this collector stopped producing, the ladder's own
    /// answer everywhere else. Applied outside <c>Classify</c> because that signature takes RUN-CLASS
    /// COUNTS (plus, since #4748, the newest run's partial-failure note - the run's own outcome) and nothing
    /// about output, a discipline both suites pin off the type.
    /// </summary>
    public string HealthStatus => CollectorHealthClassifier.BandWithRegression(
        CollectorHealthClassifier.Classify(
            TotalRuns, SuccessCount, ErrorCount, PermissionDeniedCount, ExtensionMissingCount, AbandonedCount,
            HoursSinceLastSuccess, HoursSinceLastRun, FrequencyMinutes, LatestRunNote),
        RegressedFromProductive);

    public string AvgDurationFormatted => AvgDurationMs < 1000
        ? $"{AvgDurationMs:F0} ms"
        : $"{AvgDurationMs / 1000:F1} s";

    public string LastSuccessFormatted => LastSuccessTime.HasValue
        ? ViewerTimeHelper.FormatForDisplay(LastSuccessTime.Value, "g")
        : "Never";

    public string LastRunFormatted => LastRunTime.HasValue
        ? ViewerTimeHelper.FormatForDisplay(LastRunTime.Value, "g")
        : "Never";

    public string LastErrorFormatted => LastErrorTime.HasValue
        ? ViewerTimeHelper.FormatForDisplay(LastErrorTime.Value, "g")
        : "";

    /// <summary>
    /// The informational note plus its "all N runs" / "N of M runs" qualifier (#1837) and, when a
    /// persistently empty enumeration lands on a target the store has seen user databases on, #1852's
    /// "target has user databases" — or blank. Shared with Lite through
    /// <see cref="CollectorHealthClassifier.FormatCollectionNote"/> so the two apps' health grids read
    /// identically. Never feeds <see cref="HealthStatus"/>.
    /// </summary>
    public string NoteFormatted =>
        CollectorHealthClassifier.FormatCollectionNote(LastNote, NoteCount, TotalRuns, CollectorName, TargetHasUserDatabases);
}

/// <summary>
/// One row of <see cref="ViewerDataService.CollectionCaveatsSql"/> (#3691 part a2) — a data family the
/// analysis pass currently cannot read on this server, and why. Feeds the Collection Health tab's
/// "Analysis could not read these data families" section.
/// </summary>
public class CollectionCaveatRow
{
    public string Family { get; set; } = "";
    public string Reason { get; set; } = "";
    public DateTime FirstSeenUtc { get; set; }
    public DateTime LastSeenUtc { get; set; }

    public string FirstSeenFormatted => ViewerTimeHelper.FormatForDisplay(FirstSeenUtc, "g");
    public string LastSeenFormatted => ViewerTimeHelper.FormatForDisplay(LastSeenUtc, "g");
}
