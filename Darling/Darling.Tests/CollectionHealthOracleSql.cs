/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>
/// The per-server collection-health statements as they read before #4955 replaced their four (MCP) and three
/// (viewer) full-window <c>ROW_NUMBER()</c> ranks with keyed lookups, kept VERBATIM as the oracle the rewrite is
/// held to: <see cref="CollectionHealthKeyedLookupParityTests"/> runs each beside its current statement over the
/// same seeded rows and requires every column of every row to agree. Do not edit these to follow the product
/// statements; they are the definition of "the same values".
/// </summary>
internal static class CollectionHealthOracleSql
{
    /// <summary>The MCP service's statement (<c>DarlingDataReader.CollectionHealthSql</c>) before #4955: 31 columns.</summary>
    public const string Mcp = $"""
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
            -- #2460: the mean above describes a collector whose runs all cost about the same, and
            -- says nothing true about one whose runs come in two sizes. query_store on a dense shard
            -- reported a 13,834 ms average over 1,155 runs where 958 of them yielded nothing and cost
            -- ~36 ms, which puts the other 197 at ~80,900 ms EACH — each one on its own larger than
            -- the whole 60,000 ms sweep budget. duration_ms has been written per run since V2;
            -- nothing had ever read it as anything but a mean.
            --
            -- p95 rather than the max for the number a decision is made from: a max is one run, so a
            -- single pathological cycle would make a collector look permanently terrible for the rest
            -- of the window. p95 also scales itself to the sample — over 3,500 runs it discards the
            -- one bad cycle, and over the six runs a daily collector gets in a week it lands on the
            -- max, which is right, because with six samples there is no outlier anyone can afford to
            -- throw away. DISC rather than CONT so the answer is a duration some run actually took
            -- instead of an interpolation between the two modes, which would be a number describing
            -- no run at all — the exact defect this column exists to end. Both engines ignore NULL
            -- duration_ms here, as AVG already does. Byte-identical to Lite's DuckDB read.
            MAX(duration_ms) AS max_duration_ms,
            PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms,
            MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
            MAX(collection_time) AS last_run_time,
            -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest
            -- one. The status re-check is load-bearing rather than belt-and-braces: when no failing run
            -- in the window carried text, error_rank = 1 falls through to the newest row of ANY class,
            -- and without it a SUCCESS row's note could surface here as a fake last error.
            -- #3240: EXTENSION_MISSING is in the exemplar set because its stored sentence IS the remedy
            -- (it names the extension and the database to create it in).
            MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) AS last_error,
            -- The newest failure OUTRIGHT, text or not — "when did this last fail" means the run, not
            -- the message.
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN collection_time END) AS last_error_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
            -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items,
            -- items whose enumeration probe failed) and how many runs carried one. Gated on SUCCESS
            -- specifically — the runners attach a note only to the SUCCESS write. Informational: it
            -- feeds no band, and a legitimately empty target stays HEALTHY.
            MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note,
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
            -- #2472: the per-database fan-out, described for ONE run — the dearest one in the window.
            -- Five collectors run once per database and the run writes a single blended duration_ms, so
            -- "eight databases at 10.1s" and "one at 62s beside seven at 2.7s" are the same 80,900 ms and
            -- want opposite fixes. These four are that one run's parts, and their ratio
            -- (slowest_item_ms * fanout_items / slowest_run_duration_ms) is 1.0 for an even fan-out and
            -- 6.1 for the dominated example. All four come from the SAME row via slowest_rank rather than
            -- four independent aggregates, because a slowest item taken from one run and a width taken
            -- from another compose into a ratio describing no run that ever happened — the same defect
            -- PERCENTILE_CONT was rejected for above. NULL throughout for a collector that never fans
            -- out, which is most of them.
            MAX(CASE WHEN slowest_rank = 1 THEN fanout_item_count END) AS fanout_items,
            MAX(CASE WHEN slowest_rank = 1 THEN slowest_item END) AS slowest_item,
            MAX(CASE WHEN slowest_rank = 1 THEN slowest_item_ms END) AS slowest_item_ms,
            MAX(CASE WHEN slowest_rank = 1 THEN duration_ms END) AS slowest_run_duration_ms,
            -- #2804: runs the #2673 wall-clock budget abandoned. Appended LAST rather than placed beside
            -- yield_count, which is where it belongs by meaning: both MCP surfaces read this result set
            -- POSITIONALLY and Lite's DuckDB read mirrors these ordinals, so inserting mid-list would
            -- silently re-map every column after it in whichever surface was not edited in the same
            -- breath. An ABANDONED run was previously counted by total_runs and by nothing else, so it
            -- grew the failure-rate denominator while contributing nothing to the numerator.
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
            -- #3010: the newest DENIAL on its own, which is what dates the last_error slot above.
            -- last_error_time cannot stand in for it: that column is a MAX over ERROR and PERMISSIONS
            -- together, so on a collector carrying both it hands a reader an error's instant and lets
            -- them call it a denial. Compared against last_success_time this separates a collector
            -- still being refused from one whose refusals all predate a later success -- the exact
            -- distinction pg_deadlocks needed and no surface could make.
            --
            -- Appended LAST, like abandoned_count before it: both MCP surfaces read this result set
            -- POSITIONALLY and Lite's DuckDB read mirrors these ordinals, so a mid-list insert would
            -- silently re-map every column after it in whichever surface was not edited in the same
            -- breath.
            MAX(CASE WHEN status = 'PERMISSIONS' THEN collection_time END) AS last_denied_time,
            -- #3017: what the spend BOUGHT. Every other statistic on a health row describes cost --
            -- total_runs, the three durations, and the sweep-pressure roll-up built from them -- and the
            -- rows figure lived on get_collector_cost, a different tool over a different (hourly, fleet-
            -- wide) series. Correlating spend against output was a join a reader had to know to make.
            -- Measured: pg_deadlocks was the dearest collector on a managed store, 49,258,335 ms over
            -- 79,333 runs in seven days, and stored zero rows.
            --
            -- Read from THIS query's own window so cost and output cannot describe different runs. That
            -- is the same reason #3010's two instants come out of one aggregate: a rows figure taken
            -- from one window beside a duration taken from another composes into a ratio describing no
            -- collector that ever ran.
            --
            -- COALESCE so a zero is unambiguous at the STORE. Without it a collector whose every
            -- rows_collected is NULL returns NULL here, which a reader would have to guess between "this
            -- read did not measure output" and "it stored nothing" -- and the whole point of the column
            -- is that the second of those becomes a fact rather than an absence.
            COALESCE(SUM(rows_collected), 0) AS rows_stored,
            -- The DENOMINATOR's partner, and the honest half of a cost/output pair: 12 rows over 3 of
            -- 79,333 runs is a different collector from 12 rows over all of them. get_pg_blocking already
            -- reports captures_with_blocking beside captures_total for exactly this reason, off this same
            -- rows_collected > 0 test. total_runs above is the denominator; this is the numerator.
            --
            -- APPENDED, like every column since #2472: both MCP surfaces read this result set
            -- POSITIONALLY and Lite's DuckDB read mirrors these ordinals, so a mid-list insert would
            -- silently re-map every column after it in whichever surface was not edited in the same
            -- breath.
            SUM(CASE WHEN rows_collected > 0 THEN 1 ELSE 0 END) AS runs_with_rows,
            -- #3240: runs skipped because a PostgreSQL extension the collector DECLARES is not installed
            -- — the EXTENSION_MISSING status the fault mapper split out of PERMISSIONS, counted apart so
            -- the banding stops calling an uninstalled optional extension NO_PERMISSIONS. APPENDED, like
            -- every column since #2472, because this result set is read positionally.
            SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
            -- #3754: runs whose XE session was missing or could not be created - the SESSION_MISSING status
            -- the tolerant XE readers write for a permission-denied session read and, since #3754, the
            -- long-query reconcile writes for a session refused in every database. Until now this status
            -- reached this surface as total_runs and NOTHING else: not an error, not a success, not a
            -- denial, so a collector whose every run was SESSION_MISSING banded FAILING on the never-
            -- succeeded clock while the row said errors 0 and the output finding said it had read and
            -- found nothing. Counted apart from error_count on purpose - it is not fed to the band, whose
            -- capture-down story belongs to the self-alert - and fed with error_count to the output
            -- finding as the runs that could not read. APPENDED, like every column since #2472, because
            -- this result set is read positionally and Lite's DuckDB read mirrors the ordinals.
            SUM(CASE WHEN status = 'SESSION_MISSING' THEN 1 ELSE 0 END) AS session_missing_count,
            -- #3819: the three columns that tell a collector which STOPPED producing apart from one that
            -- never produced here. The same named skip carries opposite meanings on those two rows, and
            -- until now the surface read both as the benign resting state -- which is how an install that
            -- took pg_statement_stats from 85% productive to EXTENSION_MISSING on 23 of 50 clusters was
            -- accepted by the countersign for 24 hours. APPENDED, like every column since #2472, because
            -- this result set is read positionally and Lite's DuckDB read mirrors the ordinals.
            --
            -- current_status is what the collector is reporting NOW, for the finding's prose. Taken at
            -- recency_rank = 1 rather than as a MAX over the skip rows: MAX is lexicographic, so on a
            -- streak whose status changed it would name whichever word sorts highest instead of the one
            -- being reported.
            MAX(CASE WHEN recency_rank = 1 THEN status END) AS current_status,
            -- The instant the current skip streak began AFTER: the newest run that was NOT a named skip.
            -- The vocabulary is interpolated from CollectorRuntimePrecondition, which is where each of
            -- those statuses is declared, so this cannot ask about three of four after a fourth is split
            -- out. A NULL status counts as non-skip: it is not one of the declared skip words, and reading
            -- it as one would let an unwritten status manufacture a streak.
            MAX(CASE WHEN status IS NULL
                      OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
                     THEN collection_time END) AS last_non_skip_time,
            -- The newest run that stored anything. Off the same rows_collected > 0 test as runs_with_rows
            -- above, so productive means one thing on this row. Compared against last_non_skip_time it
            -- says the productivity sits BEFORE the streak rather than inside it, which is the ORDER that
            -- makes this a regression rather than two unrelated facts.
            MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
            -- #3885: how many runs, counting back from the NEWEST, were SUCCESS with zero rows and nothing
            -- else. The column that makes the #3819 regression arm able to see the class it could not:
            -- job_history recorded SUCCESS / 0 rows run after run on 41 of 43 servers of the largest
            -- production store for up to a fortnight -- 1,900+ consecutive such runs on one of them -- and
            -- banded HEALTHY throughout, because #3819 keys on a skip STATUS and this collector's status was
            -- the most reassuring word the vocabulary has.
            --
            -- Exact, and FREE: recency_rank already exists in the subquery below (#3819 added it for
            -- current_status), so this buys the streak's true width with no new window function and no new
            -- sort. MIN of the rank of the newest run that BREAKS the streak, minus one, is the count of
            -- runs ahead of it; NULL (no run breaks it -- every run in the window is a zero-row success)
            -- falls back to COUNT(*), which is that same count. A streak broken by an error, a denial, a
            -- skip or a productive run therefore reads 0 rather than reaching past it, because what this
            -- measures is the collector's CURRENT state and any of those is a different current state.
            --
            -- The abandonment exclusion is the one success_count above uses, for the same #2926 reason: a
            -- pre-#2803 abandoned cycle is stored as SUCCESS with zero rows and the budget note, which is
            -- data LOSS rather than a source that went quiet, and counting it here would attribute an
            -- abandonment to a regression. APPENDED, like every column since #2472, because this result set
            -- is read positionally.
            COALESCE(
                MIN(CASE WHEN NOT (status = 'SUCCESS'
                                   AND COALESCE(rows_collected, 0) = 0
                                   AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
                         THEN recency_rank END) - 1,
                COUNT(*)) AS trailing_zero_row_success_runs,
            -- #4748: the note the collector's NEWEST run left, which is not last_note above. last_note is the
            -- newest run that CARRIED a note (note_rank), so a clean run after a partial-failure cycle still
            -- shows the older cycle's note there; the band must not read that, because the loss it names
            -- is not the collector's current state. recency_rank = 1 is the newest run of any status, and the
            -- SUCCESS gate matches last_note's (only the SUCCESS write carries a note). APPENDED like every
            -- column since #2472, because this result set is read positionally and Lite's DuckDB read
            -- mirrors the ordinals.
            MAX(CASE WHEN recency_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS latest_run_note
        FROM
        (
            -- #1855: rank each class of message newest-first so the two exemplar columns above can take
            -- the LATEST one instead of the lexicographically greatest. Ordering on whether the class's
            -- CASE came back empty puts every row that carries such a message ahead of every row that
            -- does not, so rank 1 is the newest one that has text — and a later clean run no longer
            -- blanks a note the window still holds. Text does not sort like the number #1837's probe
            -- note carries: 12 item(s) sorts below 9 item(s). One byte-identical shape with Lite's
            -- DuckDB read and the Viewer's, down to the error_message DESC tie-break.
            SELECT
                collector_name,
                collection_time,
                duration_ms,
                status,
                error_message,
                -- #2926: the abandonment predicate above reads it. Projected here for the same reason
                -- #2472's three columns are: this subquery ENUMERATES its columns, so an aggregate
                -- outside naming one it does not carry fails at the STORE and nowhere earlier.
                rows_collected,
                -- #2472: projected here because this subquery enumerates its columns rather than
                -- SELECT *-ing them, so an aggregate outside that names a column the inner query does
                -- not carry fails at the STORE and nowhere earlier — no compiler, no text assertion and
                -- no local build can see it.
                fanout_item_count,
                slowest_item,
                slowest_item_ms,
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY (CASE WHEN status = 'SUCCESS' THEN error_message END) IS NULL,
                             collection_time DESC,
                             error_message DESC
                ) AS note_rank,
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY (CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) IS NULL,
                             collection_time DESC,
                             error_message DESC
                ) AS error_rank,
                -- #2472: the window's dearest single ITEM, and with it the run that carried it. Ranked on
                -- slowest_item_ms rather than duration_ms because the question is which database is
                -- expensive, not which cycle was: a run whose total is the largest only because every
                -- database was busy is exactly the shape a per-database override should NOT be aimed at.
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY slowest_item_ms IS NULL,
                             slowest_item_ms DESC,
                             collection_time DESC
                ) AS slowest_rank,
                -- #3819: newest run first, so current_status above can take the status the collector is
                -- reporting NOW. status DESC only breaks an exact-timestamp tie, and breaks it identically
                -- on DuckDB and Postgres -- the same reason the ranks above tie-break on error_message.
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY collection_time DESC,
                             status DESC
                ) AS recency_rank
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
        ) runs
        GROUP BY collector_name
        ORDER BY collector_name
        """;

    /// <summary>The WPF viewer's statement (<c>ViewerDataService.CollectionHealthSql</c>) before #4955: 19 columns.</summary>
    public const string Viewer = $"""
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
            -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest
            -- one. The status re-check is load-bearing rather than belt-and-braces: when no failing run
            -- in the window carried text, error_rank = 1 falls through to the newest row of ANY class,
            -- and without it a SUCCESS row's note could surface here as a fake last error.
            -- #3240: EXTENSION_MISSING is in the exemplar set because its stored sentence IS the remedy
            -- (it names the extension and the database) — without it an EXTENSION_MISSING band would sit
            -- beside a blank Last Error and the operator would have to open the run log to learn why.
            MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) AS last_error,
            -- The newest failure OUTRIGHT, text or not — "when did this last fail" means the run, not
            -- the message. It can only name a different row than last_error if a failure was written
            -- with no text.
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN collection_time END) AS last_error_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            -- YIELDED = the 1s LOCK_TIMEOUT guard fired (#1805): deliberate, benign for collection,
            -- counted apart from errors because clustering here is a signal about the TARGET's lock
            -- contention rather than a monitoring fault.
            SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
            -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items,
            -- items whose enumeration probe failed). Gated on SUCCESS specifically, rather than on
            -- every non-failure status: the runners attach a note only to the SUCCESS write, and the looser
            -- complement would drag SESSION_MISSING and CANCELLED messages into a column whose whole
            -- claim is that it is NOT an error. Display text only: no band, no count, no threshold
            -- reads it, and a legitimately empty target stays HEALTHY exactly as before.
            MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note,
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
            MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
            -- #4748: the note the collector's NEWEST run left, which is not last_note above (that is the
            -- newest run that CARRIED a note, so a clean run after a partial-failure cycle still shows the
            -- older cycle's note there). The band reads only this one, because the loss an older note names
            -- is not the collector's current state. APPENDED, read positionally by the one shared mapper.
            MAX(CASE WHEN recency_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS latest_run_note
        FROM
        (
            -- #1855: rank each class of message newest-first so the two exemplar columns above can take
            -- the LATEST one instead of the lexicographically greatest. Ordering on whether the class's
            -- CASE came back empty puts every row that carries such a message ahead of every row that
            -- does not, so rank 1 is the newest one that has text — and a later clean run no longer
            -- blanks a note the window still holds. MAX() was never wrong about WHICH rows to consider,
            -- only about which of them wins, and text does not sort like the number #1837's probe note
            -- carries: 12 item(s) sorts below 9 item(s). collection_time settles it; error_message DESC
            -- only breaks an exact-timestamp tie, and breaks it identically here and in Lite's DuckDB
            -- twin, which binary-vs-locale collation would not.
            SELECT
                collector_name,
                collection_time,
                duration_ms,
                status,
                error_message,
                -- #2926: the abandonment predicate above reads it. Projected here for the same reason
                -- #2472's three columns are: this subquery ENUMERATES its columns, so an aggregate
                -- outside naming one it does not carry fails at the STORE and nowhere earlier.
                rows_collected,
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY (CASE WHEN status = 'SUCCESS' THEN error_message END) IS NULL,
                             collection_time DESC,
                             error_message DESC
                ) AS note_rank,
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY (CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) IS NULL,
                             collection_time DESC,
                             error_message DESC
                ) AS error_rank,
                -- #4748: newest run first, so latest_run_note above takes the newest run's note. status DESC
                -- only breaks an exact-timestamp tie, the way the Darling service read breaks it.
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY collection_time DESC,
                             status DESC
                ) AS recency_rank
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
        ) runs
        GROUP BY collector_name
        ORDER BY collector_name
        """;
}
