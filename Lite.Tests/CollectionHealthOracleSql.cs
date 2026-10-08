/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's Collection Health statement as it stood before #5371, kept VERBATIM as a test-only oracle. It ranked
/// every run of the 7-day window four times (<c>ROW_NUMBER() OVER (PARTITION BY collector_name ...)</c>) to reach the
/// newest row of four classes. <c>CollectionHealthKeyedLookupParityTests</c> runs it and the shipped statement over
/// the same seeded store and requires every column of every row to agree, so the aggregates-plus-keyed-lookups
/// rewrite is held to the old statement's answers (and its tie-breaks) rather than to a reading of them.
///
/// <para>Do not "fix" this text: its job is to be the old behaviour. The comments inside are the old statement's own.</para>
/// </summary>
internal static class CollectionHealthOracleSql
{
    internal const string Lite = $@"
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
    -- #2460: the mean above describes a collector whose runs all cost about the same, and says
    -- nothing true about one whose runs come in two sizes. query_store on a dense shard reported a
    -- 13,834 ms average over 1,155 runs where 958 of them yielded nothing and cost ~36 ms, which
    -- puts the other 197 at ~80,900 ms EACH — each one on its own larger than the whole 60,000 ms
    -- sweep budget. duration_ms has been written per run since the table existed; nothing had ever
    -- read it as anything but a mean.
    --
    -- p95 rather than the max for the number a decision is made from: a max is one run, so a single
    -- pathological cycle would make a collector look permanently terrible for the rest of the
    -- window. p95 also scales itself to the sample — over 3,500 runs it discards the one bad cycle,
    -- and over the six runs a daily collector gets in a week it lands on the max, which is right,
    -- because with six samples there is no outlier anyone can afford to throw away. DISC rather
    -- than CONT so the answer is a duration some run actually took instead of an interpolation
    -- between the two modes, which would be a number describing no run at all — the exact defect
    -- this column exists to end. Both engines ignore NULL duration_ms here, as AVG already does.
    MAX(duration_ms) AS max_duration_ms,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms,
    -- SKIPPED counts as a healthy run (dedup / version-gated collectors no-op without being stale)
    MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
    MAX(collection_time) AS last_run_time,
    -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest one. The
    -- status re-check is load-bearing rather than belt-and-braces: when no failing run in the window
    -- carried text, error_rank = 1 falls through to the newest row of ANY class, and without it a
    -- SUCCESS row's note could surface here as a fake last error.
    -- #3240: EXTENSION_MISSING is in the exemplar set for twin-parity with Darling's reads — its stored
    -- sentence IS the remedy there. Lite's SQL Server collectors never write the status, so on this SKU
    -- the branch is inert.
    MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) AS last_error,
    -- The newest failure OUTRIGHT, text or not: when did this last FAIL is about the run, not the
    -- message. It can only name a different row than last_error if a failure was written with no text.
    MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN collection_time END) AS last_error_time,
    SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
    -- YIELDED = the 1s LOCK_TIMEOUT guard fired (#1805): deliberate, benign for collection,
    -- counted apart from errors because clustering here is a signal about the TARGET's lock
    -- contention rather than a monitoring fault.
    SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
    -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items, items
    -- whose enumeration probe failed). Gated on SUCCESS specifically, rather than on every non-failure status:
    -- the runners attach a note only to the SUCCESS write, and the looser complement would drag
    -- SESSION_MISSING and CANCELLED messages into a column whose whole claim is that it is NOT an
    -- error. Display text only: no band, no count, no threshold reads it, and a legitimately empty
    -- target (no user databases, no AGs) stays HEALTHY exactly as before.
    MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note,
    -- How many of the window's runs carried one. note_count = total_runs is the persistently-empty
    -- signal the operator is actually looking for: EVERY run this week came back with nothing.
    COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
    -- #1852: the one thing that makes a persistently-empty enumeration interesting — does this target
    -- actually HAVE user databases? Zero items on a server with none is legitimate and stays quiet;
    -- zero items on a server that HAS them is a login that cannot enter any, or a filter that
    -- swallowed everything. database_size_stats rather than database_config for three reasons: it runs
    -- on the scheduled loop (60 min) where database_config is on-load and can age past this window on
    -- a long-running install; it is indexed on (server_id, collection_time) where database_config has
    -- no index at all; and it reads sys.master_files, so it still sees databases the monitoring login
    -- cannot ENTER — which is exactly the case being diagnosed. database_id > 4 excludes the system
    -- databases, tempdb included: the size collector takes every ONLINE database, so a bare row check
    -- would be true on every server alive. A NULL database_id is an Azure sibling row (#2643/#3262):
    -- sys.resource_stats carries no id, and it bills only USER databases, so those rows are inventory
    -- too — without the IS NULL arm a master-connected Azure target with fifty user databases would
    -- read as having none.
    --
    -- The inventory window is the health read's OWN ($2) — no second parameter, and an inventory that
    -- aged out says nothing rather than something stale. Uncorrelated, so both engines evaluate it
    -- once per query (a Postgres InitPlan) instead of per row, and it needs no GROUP BY entry and no
    -- join. Feeds display text only, through the shared formatter; the banding never sees it.
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
    -- #2472: the per-database fan-out, described for ONE run — the dearest one in the window. Five
    -- collectors run once per database and the run writes a single blended duration_ms, so eight
    -- databases at 10.1s and one at 62s beside seven at 2.7s are the same 80,900 ms and want
    -- opposite fixes. (No double quotes anywhere in this string: it is a verbatim literal, where a
    -- lone quote ends it.) The four parts compose into slowest_item_ms * fanout_items /
    -- slowest_run_duration_ms, which is 1.0 for an even fan-out and 6.1 for the dominated example.
    -- All four come from the SAME row via slowest_rank: parts taken from different runs would
    -- compose into a ratio describing no run that ever happened. Twins Darling's read.
    MAX(CASE WHEN slowest_rank = 1 THEN fanout_item_count END) AS fanout_items,
    MAX(CASE WHEN slowest_rank = 1 THEN slowest_item END) AS slowest_item,
    MAX(CASE WHEN slowest_rank = 1 THEN slowest_item_ms END) AS slowest_item_ms,
    MAX(CASE WHEN slowest_rank = 1 THEN duration_ms END) AS slowest_run_duration_ms,
    -- #2804: runs the #2673 wall-clock budget abandoned. APPENDED, never inserted — this result set
    -- is read positionally and Darling's CollectionHealthSql mirrors these ordinals, so a mid-list
    -- insert would silently re-map every later column in whichever surface was not edited with it.
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
    -- last_error_time cannot stand in for it -- that column is a MAX over ERROR and PERMISSIONS
    -- together, so on a collector carrying both it hands a reader an error's instant and lets them
    -- call it a denial. Compared against last_success_time this separates a collector still being
    -- refused from one whose refusals all predate a later success. APPENDED, and Darling's
    -- CollectionHealthSql mirrors the ordinal, because both are read positionally.
    MAX(CASE WHEN status = 'PERMISSIONS' THEN collection_time END) AS last_denied_time,
    -- #3017: what the spend BOUGHT. Every other statistic on a health row describes cost -- total_runs,
    -- the three durations, and the sweep-pressure roll-up built from them -- and the rows figure lived
    -- on a different tool over a different (hourly, fleet-wide) series, so correlating spend against
    -- output was a join a reader had to know to make. Read from THIS query's own window so cost and
    -- output cannot describe different runs, the same reason #3010's two instants come out of one
    -- aggregate. COALESCE so a zero is unambiguous at the store: without it a collector whose every
    -- rows_collected is NULL returns NULL, which a reader would have to guess between not-measured and
    -- stored-nothing, and the whole point is that the second becomes a fact rather than an absence.
    -- APPENDED, and Darling's CollectionHealthSql mirrors both ordinals, because both are read
    -- positionally.
    COALESCE(SUM(rows_collected), 0) AS rows_stored,
    -- The denominator's partner, and the honest half of a cost/output pair: 12 rows over 3 of 79,333
    -- runs is a different collector from 12 rows over all of them. get_pg_blocking already reports
    -- captures_with_blocking beside captures_total off this same rows_collected > 0 test.
    SUM(CASE WHEN rows_collected > 0 THEN 1 ELSE 0 END) AS runs_with_rows,
    -- #3240: runs skipped because a PostgreSQL extension the collector DECLARES is not installed — the
    -- EXTENSION_MISSING status Darling's fault mapper split out of PERMISSIONS. Lite's SQL Server
    -- collectors never write it, so this counts 0 on this SKU; selected anyway because the two health
    -- reads are ordinal twins and the shared classifier takes the count. APPENDED, read positionally.
    SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
    -- #3754: runs whose XE session was missing or could not be created - Darling's SESSION_MISSING
    -- status. Lite never writes it: its long-query XE reader swallows a permission-denied session read to
    -- zero rows, and an ensure failure (and, since #4731, a blocked-process or deadlock read failure)
    -- classifies PERMISSIONS / ERROR through XeSessionEnsureException,
    -- so this counts 0 on this SKU; selected anyway because the two health reads are ordinal twins and
    -- the shared output finding takes the count beside error_count as the runs that could not read.
    -- Counted apart from error_count on purpose - it is not fed to the band. APPENDED, read positionally.
    SUM(CASE WHEN status = 'SESSION_MISSING' THEN 1 ELSE 0 END) AS session_missing_count,
    -- #3819: the three columns that tell a collector which STOPPED producing apart from one that never
    -- produced here. Darling's twin carries the same three at the same ordinals; both MCP surfaces read
    -- this result set positionally. Lite's SQL Server collectors write PERMISSIONS but neither of the
    -- other two skip words, so on this SKU the streak this detects is a permission that was granted and
    -- has been revoked since -- a narrower population than Darling's, and the same regression.
    --
    -- current_status is what the collector is reporting NOW, for the finding's prose. Taken at
    -- recency_rank = 1 rather than as a MAX over the skip rows: MAX is lexicographic, so on a streak whose
    -- status changed it would name whichever word sorts highest instead of the one being reported.
    MAX(CASE WHEN recency_rank = 1 THEN status END) AS current_status,
    -- The instant the current skip streak began AFTER: the newest run that was NOT a named skip. The
    -- vocabulary is interpolated from CollectorRuntimePrecondition, which is where each of those statuses
    -- is declared, so this cannot ask about three of four after a fourth is split out. A NULL status
    -- counts as non-skip: it is not one of the declared skip words, and reading it as one would let an
    -- unwritten status manufacture a streak.
    MAX(CASE WHEN status IS NULL
              OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
             THEN collection_time END) AS last_non_skip_time,
    -- The newest run that stored anything. Off the same rows_collected > 0 test as runs_with_rows above,
    -- so productive means one thing on this row. Compared against last_non_skip_time it says the
    -- productivity sits BEFORE the streak rather than inside it, which is the ORDER that makes this a
    -- regression rather than two unrelated facts.
    MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
    -- #3885: how many runs, counting back from the NEWEST, were SUCCESS with zero rows and nothing else.
    -- The second regression class, and the one #3819 could not see: it keys on a skip STATUS, and a
    -- collector whose source went away while its query stayed VALID records the most reassuring word the
    -- vocabulary has. Darling's twin carries this at the same ordinal; both MCP surfaces read this result
    -- set positionally. The class is not Darling-specific -- Lite dedups on the same watermarks and its
    -- collectors read the same sources -- so this SKU detects it identically rather than counting 0.
    --
    -- Exact, and free: recency_rank already exists in the subquery below (#3819 added it for
    -- current_status), so this buys the streak's true width with no new window function and no new sort.
    -- MIN of the rank of the newest run that BREAKS the streak, minus one, is the count of runs ahead of
    -- it; NULL (nothing breaks it -- every run in the window is a zero-row success) falls back to COUNT(*),
    -- which is that same count. The abandonment exclusion is success_count's own (#2926): a pre-#2803
    -- abandoned cycle is stored as SUCCESS with zero rows plus the budget note, which is data LOSS rather
    -- than a source that went quiet. APPENDED, read positionally.
    COALESCE(
        MIN(CASE WHEN NOT (status = 'SUCCESS'
                           AND COALESCE(rows_collected, 0) = 0
                           AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
                 THEN recency_rank END) - 1,
        COUNT(*)) AS trailing_zero_row_success_runs,
    -- #4748: the note the collector's NEWEST run left, which is not last_note above. last_note is the newest
    -- run that CARRIED a note (note_rank), so a clean run after a partial-failure cycle still shows the
    -- older cycle's note there; the band must not read that, because the loss it names is not the
    -- collector's current state. recency_rank = 1 is the newest run of any status, and the SUCCESS gate
    -- matches last_note's (only the SUCCESS write carries a note). APPENDED, read positionally; Darling's
    -- twin carries it at the same ordinal.
    MAX(CASE WHEN recency_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS latest_run_note
FROM
(
    -- #1855: rank each class of message newest-first so the two exemplar columns above can take the
    -- LATEST one instead of the lexicographically greatest. Ordering on whether the class's CASE came
    -- back empty puts every row that carries such a message ahead of every row that does not, so rank 1
    -- is the newest one that has text — and a later clean run no longer blanks a note the window still
    -- holds. MAX() was never wrong about WHICH rows to consider, only about which of them wins, and
    -- text does not sort like the number #1837's probe note carries: 12 item(s) sorts below 9 item(s).
    -- collection_time settles it; error_message DESC only breaks an exact-timestamp tie, and breaks it
    -- identically on DuckDB and Postgres, which binary-vs-locale collation would not.
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
        -- #2472: projected here because this subquery enumerates its columns rather than SELECT *-ing
        -- them, so an aggregate outside that names a column the inner query does not carry fails at the
        -- store and nowhere earlier.
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
        -- slowest_item_ms rather than duration_ms because the question is which database is expensive,
        -- not which cycle was.
        ROW_NUMBER() OVER
        (
            PARTITION BY collector_name
            ORDER BY slowest_item_ms IS NULL,
                     slowest_item_ms DESC,
                     collection_time DESC
        ) AS slowest_rank,
        -- #3819: newest run first, so current_status above can take the status the collector is reporting
        -- NOW. status DESC only breaks an exact-timestamp tie, and breaks it identically on DuckDB and
        -- Postgres -- the same reason the ranks above tie-break on error_message.
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
ORDER BY collector_name";
}
