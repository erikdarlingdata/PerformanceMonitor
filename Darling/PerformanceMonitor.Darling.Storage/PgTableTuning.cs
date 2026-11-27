/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Composer + analyze_*_plan performance tuning — RUNTIME setup, deliberately NOT a versioned migration. These
/// are RESULTS-INVARIANT covering indexes plus a per-table autovacuum-insert override: the Custom Views composer
/// and the analyze_*_plan reads return identical rows with or without them, just slower, so they must NOT bump
/// <see cref="StorageVersion"/> — that would gate the Viewer's connect-time schema check on perf indexes it does
/// not need (the same reasoning that keeps role GUCs / GRANTs in role provisioning rather than a migration). The
/// service applies them at startup AFTER migrations and the TimescaleDB block, BEFORE collectors: so a fresh
/// store's collector tables are already hypertables when indexed (CREATE INDEX / ALTER TABLE SET propagate to all
/// existing and future chunks), a plain-PostgreSQL store gets them too, and a first build on a large adopted store
/// never contends with live inserts. The same pass also runs on the hourly convergence tick beside live
/// collection, but there it NEVER builds an index: an index build takes a ShareLock that queues every writer
/// behind it, so a missing index is logged once per process and left for the next service start. On the start
/// path a CREATE is issued only when the catalog shows the index absent (<c>IF NOT EXISTS</c> takes its table
/// locks BEFORE it looks) and then under a bounded <c>lock_timeout</c> (<see cref="GuardedCreate"/>).
/// Idempotent (catalog check / ALTER TABLE SET), so every run re-converges and a store that already has them
/// (a field box hand-indexed, or a prior run) issues no CREATE.
///
/// <para>EXPLAIN-backed field fix (2026-07-22): the composer filtered by <c>object_name</c> / <c>query_hash</c>
/// fell to a Seq Scan and timed out at the 15 s statement_timeout; a PLAIN index still paid a Bitmap Heap Scan
/// re-fetching the aggregate columns, so the three composer indexes are COVERING (INCLUDE exactly the
/// measure-catalog columns the Procedures / Queries / Query Store measures SUM/AVG) for an Index Only Scan. The
/// three <c>_server_..._time</c> indexes back the single-row analyze_*_plan lookups (ORDER BY collection_time DESC
/// LIMIT 1 — one heap fetch, no INCLUDE needed). The per-table
/// <c>autovacuum_vacuum_insert_scale_factor = 0.02</c> override keeps the visibility map current on the pure-insert
/// hypertable chunks (the Postgres default 0.2 leaves the day's hot chunk stale before the daily TimescaleDB
/// rollover, degrading the Index Only Scan back to heap fetches). Verified: the two panels that timed out at 15 s
/// then ran in 139 ms / 514 ms with 0 heap fetches on vacuumed chunks. DarlingStoredPlanReader and ComposeCompiler
/// are unchanged — correct as-is, they just needed these indexes + the vacuum state to perform.</para>
///
/// <para><b>The alerting pass joined the composer here (#3573)</b>, for the same reason and with the same
/// EXPLAIN-backed shape: <c>DarlingAlertReadAdapter.ForcePlanFailuresSql</c> takes the fourth covering index
/// below, an Index Only Scan replacing a plan that streamed the whole fleet's two-hour slice of
/// <c>query_store_stats</c> to keep one server's rows. Its derivation is on the statement itself — including
/// why it is here and not a ladder rung, and why it is a plain <c>CREATE INDEX</c> and not the per-chunk form.</para>
///
/// <para><b>#4247: five of what were originally eight indexes here are DROPS, not CREATEs.</b> Every row
/// inserted into these three hypertables lands on an arbitrary leaf of a day-wide chunk index; with data
/// checksums on and 5-minute checkpoints, the first touch of each leaf after a checkpoint writes a full 8 KB
/// page image to WAL, so a random-key index bigger than one checkpoint's worth of inserts costs close to a
/// page image PER INSERTED ROW. Production measurement (the issue): these five cost ~24-28% of two stores'
/// WAL and were scanned at most 13 times across several retained days, against hundreds of thousands of scans
/// for the three composer/alerting indexes that stay. Ruling: drop an index only if its slowest reader,
/// WITHOUT it, stays under HALF that reader's deadline. Measured on a rig seeded at one busy server's rate
/// (11 servers, 5 days, ~60k rows/day/table/server) with EXPLAIN (ANALYZE, BUFFERS); the two handle readers
/// timed at a handle seen in the last hour and one last seen on day 1 of the range. All five passed by wide
/// margins (13x-75x headroom, one skip-scan case ~3780x) — the numbers are on each DROP statement below.</para>
///
/// <para>A drop is <c>DROP INDEX IF EXISTS</c> here, never a migration (same RESULTS-INVARIANT / runtime-setup
/// reasoning as a CREATE), wrapped in its own <c>BEGIN; SET LOCAL lock_timeout; DROP; COMMIT;</c>
/// (<see cref="GuardedDrop"/>) so a build stuck behind a live reader gives up after
/// <see cref="DropLockTimeoutSeconds"/> instead of holding ACCESS EXCLUSIVE on the hypertable — and every
/// chunk — for up to <see cref="SetupTimeoutSeconds"/>; the next start's idempotent re-run retries it. Proven
/// live on the rig: a blocked guarded drop raises <c>55P03 lock_not_available</c> right at the timeout, NOT at
/// <see cref="SetupTimeoutSeconds"/>. That failure leaves the connection inside the statement's own aborted
/// implicit transaction — Npgsql's simple-query protocol stops at the first error and never reaches the
/// trailing <c>COMMIT</c> text in the same command, so the session stays "in failed transaction" for whatever
/// runs on it next. <see cref="ApplyAsync"/> therefore issues a best-effort <c>ROLLBACK</c> after EVERY caught
/// failure, not just a drop's, so one blocked or invalid statement can never take the rest of the sweep down
/// with it — proven live by seeding a block, forcing the failure, and asserting the statement after it still
/// applies.</para>
///
/// <para>#6539: every <c>ALTER TABLE ... SET (...)</c> (the literal statements and the catalog-derived hourly sweep) is
/// covered by the same doctrine through <see cref="GuardedAlter"/>: its own transaction under
/// <see cref="AlterLockTimeoutSeconds"/>. A busy table (<c>55P03</c>) is rolled back, logged once at Information as
/// skipped until the next pass, and the sweep goes on; the settings are idempotent, so a skipped pass costs nothing.</para>
///
/// <para>#8949: a literal <c>ALTER TABLE ... SET (...)</c> is issued only when the catalog shows a difference
/// (<see cref="AlterIsConvergedAsync"/>). The statement takes SHARE UPDATE EXCLUSIVE even when it changes nothing, and that
/// cancels an autovacuum running on the table (a wraparound one excepted). On a hypertable the ALTER recurses to every chunk, so
/// it cancels the chunk vacuums too, and a TOAST table with no reloptions of its own is vacuumed under its owner and is
/// cancelled with it. An ALTER issued every hour keeps a multi-hour vacuum of a large table from ever finishing.</para>
/// </summary>
public static class PgTableTuning
{
    /* A first CREATE INDEX on a large adopted store can take a while (at startup it runs before collection and
       blocks no live inserts; the build itself is real work) — the same generous budget the TimescaleDB first
       conversion uses. A start-path CREATE is bounded by CreateLockTimeoutSeconds first. */
    private const int SetupTimeoutSeconds = 300;

    /// <summary>
    /// #4247: the lock_timeout every guarded <c>DROP INDEX</c> statement below runs under. A DROP on a
    /// hypertable takes ACCESS EXCLUSIVE on it and every chunk; without a short timeout, a drop queued behind
    /// a live reader would hold up every LATER reader of the table for up to <see cref="SetupTimeoutSeconds"/>.
    /// With it, the drop gives up (proven live: raises <c>55P03 lock_not_available</c> right at this bound) and
    /// the next idempotent start retries it. <c>SET LOCAL</c> so it cannot outlive the statement's own
    /// transaction either way.
    /// </summary>
    private const string DropLockTimeoutSeconds = "5s";

    /// <summary>
    /// The lock_timeout a CREATE INDEX that is still issued runs under. The build takes ShareLock on the table
    /// (and every chunk), which conflicts with every writer's RowExclusiveLock; an unbounded wait behind a chunk
    /// compression or a reader would hold every INSERT for up to <see cref="SetupTimeoutSeconds"/>. The CREATE
    /// gives up with <c>55P03</c>, and the next start retries. Only the start path issues a CREATE.
    /// </summary>
    private const string CreateLockTimeoutSeconds = "5s";

    /// <summary>
    /// The same bound as <see cref="CreateLockTimeoutSeconds"/>, as the <c>CommandTimeout</c> (seconds) of the start-path
    /// emptiness check. That check is a bare SELECT, so it takes ACCESS SHARE on the table and queues behind an ACCESS EXCLUSIVE
    /// holder (drop_chunks, decompress_chunk, VACUUM FULL) the way a CREATE does; with <see cref="SetupTimeoutSeconds"/> it
    /// would hold the start for 300 s. It gives up here instead and fails closed onto the shared build budget (#6460).
    /// </summary>
    private const int EmptinessCheckTimeoutSeconds = 5;

    private const string CreateIndexPrefix = "CREATE INDEX IF NOT EXISTS ";

    /// <summary>
    /// Wraps a <c>CREATE INDEX IF NOT EXISTS</c> in its own transaction under <see cref="CreateLockTimeoutSeconds"/>.
    /// Plain (not <c>CONCURRENTLY</c>: hypertables refuse it, and it cannot run inside a transaction block).
    /// </summary>
    private static string GuardedCreate(string statement) =>
        Guarded(CreateLockTimeoutSeconds, statement);

    /// <summary>
    /// #6539: the lock_timeout every <c>ALTER TABLE ... SET (...)</c> runs under, on the start path and in the hourly pass. The
    /// statement takes SHARE UPDATE EXCLUSIVE, which an ACCESS EXCLUSIVE holder (drop_chunks, decompress_chunk, VACUUM FULL)
    /// blocks, and unbounded it held a start for <see cref="SetupTimeoutSeconds"/> per statement. It gives up with
    /// <c>55P03</c>; the settings are idempotent, so skipping one pass is harmless and the next pass applies them.
    /// </summary>
    private const string AlterLockTimeoutSeconds = "5s";

    private const string AlterTablePrefix = "ALTER TABLE ";

    /// <summary>Wraps an <c>ALTER TABLE ... SET</c> in its own transaction under <see cref="AlterLockTimeoutSeconds"/>.</summary>
    private static string GuardedAlter(string statement) =>
        Guarded(AlterLockTimeoutSeconds, statement);

    /// <summary>
    /// The one guarded shape every bounded-wait statement in this file uses: its own transaction, <c>SET LOCAL lock_timeout</c>
    /// (reset by the transaction boundary either way), the statement, <c>COMMIT</c>. A lock-timeout failure leaves the
    /// connection "in failed transaction"; <see cref="ApplyAsync"/> rolls back after every caught failure.
    /// </summary>
    private static string Guarded(string lockTimeout, string statement) =>
        "BEGIN; SET LOCAL lock_timeout = '" + lockTimeout + "'; " + statement + "; COMMIT;";

    /// <summary>The <c>schema.table</c> an <c>ALTER TABLE &lt;table&gt; SET (...)</c> statement tunes.</summary>
    private static string AlteredTable(string statement)
    {
        var rest = statement.Substring(AlterTablePrefix.Length);
        return rest.Substring(0, rest.IndexOf(' ', StringComparison.Ordinal));
    }

    /// <summary>
    /// The <c>schema.table</c> and the <c>(key, value)</c> pairs of an <c>ALTER TABLE &lt;table&gt; SET (k = v, k = v)</c> statement,
    /// or null when the statement is not exactly that shape. A statement that does not parse is issued on every pass. A quoted
    /// value, a nested parenthesis or a namespaced key (<c>toast.</c>, which is stored on the TOAST table and not in the table's
    /// own reloptions) does not parse, because none of them can be compared with <see cref="ReloptionsCarry"/>.
    /// </summary>
    internal static (string Table, IReadOnlyList<KeyValuePair<string, string>> Options)? ParseAlterSet(string statement)
    {
        const string set = " SET (";
        if (statement is null || !statement.StartsWith(AlterTablePrefix, StringComparison.Ordinal) || !statement.EndsWith(')'))
        {
            return null;
        }

        var setAt = statement.IndexOf(set, AlterTablePrefix.Length, StringComparison.Ordinal);
        if (setAt < 0)
        {
            return null;
        }

        var table = statement.Substring(AlterTablePrefix.Length, setAt - AlterTablePrefix.Length);
        var list = statement.Substring(setAt + set.Length, statement.Length - setAt - set.Length - 1);
        if (table.Length == 0 || table.Contains(' ', StringComparison.Ordinal) || list.AsSpan().IndexOfAny("()'\"") >= 0)
        {
            return null;
        }

        var options = new List<KeyValuePair<string, string>>();
        foreach (var item in list.Split(','))
        {
            var equalsAt = item.IndexOf('=', StringComparison.Ordinal);
            if (equalsAt < 0)
            {
                return null;
            }

            var key = item.Substring(0, equalsAt).Trim();
            var value = item.Substring(equalsAt + 1).Trim();
            if (key.Length == 0 || value.Length == 0 || key.Contains('.', StringComparison.Ordinal))
            {
                return null;
            }

            options.Add(new KeyValuePair<string, string>(key, value));
        }

        return (table, options);
    }

    /// <summary>
    /// True when <paramref name="reloptions"/> (a <c>pg_class.reloptions</c> array of <c>key=value</c> entries, null when the
    /// relation has none) holds every option in <paramref name="wanted"/> at the wanted value. Options the relation carries
    /// beyond those are ignored. Keys compare without case. Values compare as numbers when both are numbers, because the catalog
    /// keeps the spelling the option was set with (<c>0.020</c> stays <c>0.020</c>), and as text without case otherwise.
    /// </summary>
    internal static bool ReloptionsCarry(IReadOnlyList<KeyValuePair<string, string>> wanted, IReadOnlyList<string>? reloptions)
    {
        if (reloptions is null)
        {
            return wanted.Count == 0;
        }

        foreach (var want in wanted)
        {
            var carried = false;
            foreach (var entry in reloptions)
            {
                var equalsAt = entry?.IndexOf('=', StringComparison.Ordinal) ?? -1;
                if (equalsAt < 0 || !string.Equals(entry!.Substring(0, equalsAt).Trim(), want.Key, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                carried = OptionValuesEqual(want.Value, entry.Substring(equalsAt + 1).Trim());
                break;
            }

            if (!carried)
            {
                return false;
            }
        }

        return true;
    }

    private static bool OptionValuesEqual(string wanted, string actual) =>
        decimal.TryParse(wanted, NumberStyles.Float, CultureInfo.InvariantCulture, out var wantedNumber)
        && decimal.TryParse(actual, NumberStyles.Float, CultureInfo.InvariantCulture, out var actualNumber)
            ? wantedNumber == actualNumber
            : string.Equals(wanted, actual, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The reloptions of a relation ($1, a <c>schema.table</c>) and of every relation that inherits from it, one row each. The
    /// chunks of a hypertable inherit from its root; a plain table has no children. No row means the relation does not exist.
    /// It reads <c>pg_class</c> and <c>pg_inherits</c> only, and <c>to_regclass</c> resolves the name without locking the
    /// relation, so the read never waits on a lock held on the table itself.
    /// </summary>
    private const string RelationAndChildOptionsSql =
        "SELECT c.reloptions FROM pg_class AS c WHERE c.oid = to_regclass($1) "
        + "UNION ALL "
        + "SELECT c.reloptions FROM pg_inherits AS i JOIN pg_class AS c ON c.oid = i.inhrelid WHERE i.inhparent = to_regclass($1)";

    /// <summary>
    /// The <c>CommandTimeout</c> (seconds) of the catalog read in <see cref="AlterIsConvergedAsync"/>. The read waits on no
    /// table lock, so this only bounds a catalog that is itself unavailable; a read that gives up issues the ALTER.
    /// </summary>
    private const int ReloptionsReadTimeoutSeconds = 5;

    /// <summary>
    /// True when the table an <c>ALTER TABLE ... SET (...)</c> statement tunes, and every relation that inherits from it, already
    /// carries every option in the statement at the statement's value, so issuing the statement would change nothing. On a
    /// hypertable the ALTER recurses to the chunks, so a chunk that differs makes the whole statement due again.
    /// Anything else answers false and the caller issues the ALTER: a statement that does not parse, a table that does not exist,
    /// one option missing or different on one relation, or a catalog read that failed. The connection stays usable after a failed
    /// read, which runs outside a transaction.
    /// </summary>
    internal static async Task<bool> AlterIsConvergedAsync(NpgsqlConnection connection, string statement, ILogger? logger, CancellationToken cancellationToken)
    {
        var parsed = ParseAlterSet(statement);
        if (parsed is null)
        {
            return false;
        }

        var (table, options) = parsed.Value;

        try
        {
            using var command = new NpgsqlCommand(RelationAndChildOptionsSql, connection) { CommandTimeout = ReloptionsReadTimeoutSeconds };
            command.Parameters.AddWithValue(table);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var relations = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                relations++;
                if (!ReloptionsCarry(options, reader.IsDBNull(0) ? null : reader.GetFieldValue<string[]>(0)))
                {
                    return false;
                }
            }

            return relations > 0;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(
                "Could not read the current settings of {Table}, so its tuning setting is applied without the check: {Message}",
                table, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// #9795: the time, in seconds, the builds of the <see cref="s_startBuildBudget"/> indexes may take in one start, all of them
    /// together. It is the limit of one build, so a start pauses collection for these builds no longer than one bounded build can.
    /// </summary>
    private const int StartBuildBudgetSeconds = SetupTimeoutSeconds;

    /// <summary>
    /// The indexes whose first build reads the heap of a large hypertable. They share one time budget per start
    /// (<see cref="StartBuildBudgetSeconds"/>, #9795): a start builds every missing one, in statement order, while the builds so far
    /// have taken less than the budget, and each build runs under the smaller of its own limit and the time left. A store whose
    /// builds are quick, such as one upgraded across several releases with little history, has every index after one start. On a
    /// store whose builds use the budget up, the rest wait for the next start, so each start pauses collection for no longer than
    /// one bounded build, not for the sum of several.
    /// </summary>
    private static readonly HashSet<string> s_startBuildBudget = new(StringComparer.Ordinal)
    {
        LegacyRowIndexName,
        QueryStatsRestartRowIndexName,
        ProcedureStatsRestartRowIndexName,
        ProcedureStatsNameIndex.IndexName,
        WaitStatsDailyCoverIndexName,
        CpuWindowCoverIndexName,
        QueryStoreServerHashIndexName,
    };

    /// <summary>True for an index whose build shares the start's build time budget.</summary>
    internal static bool SharesStartBuildBudget(string indexName) => s_startBuildBudget.Contains(indexName);

    /// <summary>The index name in a <c>CREATE INDEX IF NOT EXISTS &lt;name&gt; ON ...</c> statement.</summary>
    private static string CreatedIndexName(string statement)
    {
        var rest = statement.Substring(CreateIndexPrefix.Length);
        return rest.Substring(0, rest.IndexOf(' ', StringComparison.Ordinal));
    }

    /// <summary>The <c>schema.table</c> a <c>CREATE INDEX IF NOT EXISTS &lt;name&gt; ON &lt;table&gt; ...</c> statement indexes.</summary>
    private static string CreatedIndexTable(string statement)
    {
        const string on = " ON ";
        var rest = statement.Substring(statement.IndexOf(on, StringComparison.Ordinal) + on.Length);
        var end = rest.IndexOf(' ', StringComparison.Ordinal);
        return end < 0 ? rest : rest.Substring(0, end);
    }

    /// <summary>
    /// True when <paramref name="table"/> (a <c>schema.table</c> taken from <see cref="Statements"/>, never from input) has no
    /// row at all. A failed check, including one that gives up on a busy table after <see cref="EmptinessCheckTimeoutSeconds"/>,
    /// answers false, so the index stays on the shared build budget; the command is cancelled on the server and the connection stays usable.
    /// </summary>
    private static async Task<bool> TableIsEmptyAsync(NpgsqlConnection connection, string table, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            using var command = new NpgsqlCommand("SELECT NOT EXISTS (SELECT 1 FROM " + table + ")", connection) { CommandTimeout = EmptinessCheckTimeoutSeconds };
            return await command.ExecuteScalarAsync(cancellationToken) is true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not tell whether {Table} is empty, so its index build counts against this start's shared build time: {Message}",
                table, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Whether <paramref name="table"/> is a TimescaleDB hypertable (#7152). The extension being installed says nothing, since a
    /// store can have it and still run in plain-PostgreSQL mode with plain heap tables. A failed or timed-out check answers null
    /// (unknown), and an unknown table is neither given an index build nor has its index dropped this start: a drop on a guess
    /// would remove a valid index from a plain table, and the next start would build it and the next failed check drop it again.
    /// </summary>
    private static async Task<bool?> TableIsHypertableAsync(NpgsqlConnection connection, string table, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            return await QueryStoreBackgroundIndexes.IsHypertableAsync(connection, table, cancellationToken, EmptinessCheckTimeoutSeconds);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not tell whether {Table} is a hypertable, so {Index} is neither built nor dropped this start: {Message}",
                table, WaitStatsDailyCoverIndexName, ex.Message);
            return null;
        }
    }

    /// <summary>NULL when no such index exists in <c>collect</c>, otherwise whether it is valid.</summary>
    private const string IndexValidSql =
        "SELECT i.indisvalid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        + "JOIN pg_index i ON i.indexrelid = c.oid WHERE n.nspname = 'collect' AND c.relname = $1";

    /// <summary>CommandTimeout for the post-failure recovery <c>ROLLBACK</c> in <see cref="ApplyAsync"/> — a
    /// ROLLBACK never waits on a lock, so this is a generous ceiling, not a measured budget.</summary>
    private const int DropLockTimeoutRollbackSeconds = 10;

    /// <summary>
    /// Builds a guarded <c>DROP INDEX IF EXISTS</c> statement: its own transaction, a short
    /// <see cref="DropLockTimeoutSeconds"/> lock_timeout, reset by the transaction boundary either way. NOT
    /// <c>CONCURRENTLY</c> — hypertables refuse it, same as every CREATE in this file.
    ///
    /// <para>A lock-timeout failure aborts THIS statement's own implicit transaction and never reaches the
    /// trailing <c>COMMIT</c> text (Npgsql's simple-query protocol stops at the first error in a multi-statement
    /// command), so the connection is left "in failed transaction" for whatever runs on it next —
    /// <see cref="ApplyAsync"/>'s best-effort post-failure <c>ROLLBACK</c> is what keeps that from taking the
    /// rest of the sweep down with it.</para>
    /// </summary>
    private static string GuardedDrop(string indexName) =>
        Guarded(DropLockTimeoutSeconds, "DROP INDEX IF EXISTS collect." + indexName);

    /// <summary>The replacement index when <paramref name="statement"/> is the guarded drop of an index in <see cref="s_replacedBy"/>, else null.</summary>
    private static string? ReplacedIndexOf(string statement)
    {
        foreach (var (old, replacement) in s_replacedBy)
        {
            if (statement == GuardedDrop(old))
            {
                return replacement;
            }
        }

        return null;
    }

    /// <summary>
    /// The #3573 covering index's name, and the columns it carries in key-then-INCLUDE order. Exposed so the
    /// pin (<c>ForcePlanFailuresAccessPathTests</c>) can hold the read's column references against THIS list
    /// rather than against a second copy of the statement text, and so the live test can find the index by
    /// name in <c>pg_indexes</c>. The first two are the key; the rest are INCLUDE. Every column the read
    /// references must appear here or the Index Only Scan silently degrades to the heap plan it replaced.
    /// </summary>
    public const string ForcePlanFailuresIndexName = "idx_query_store_stats_server_time_forcing";

    /// <summary>
    /// The partial index that answers <see cref="QueryStoreIntervalWide.HasLegacyRowSql"/> without reading the
    /// heap: <c>(server_id, collection_time)</c> over only the rows whose <c>interval_start_time_utc</c> is NULL.
    /// Exposed so the live pin can find it by name in <c>pg_indexes</c> and in the plan.
    /// </summary>
    public const string LegacyRowIndexName = "idx_query_store_stats_server_time_null_start";

    /// <summary>
    /// The partial index over the restart rows of <c>collect.query_stats</c>:
    /// <c>(collection_time) WHERE sample_interval_seconds = 0</c>. It serves
    /// <see cref="IntervalRollupRestartRows.QueryStatsRestartRowsSql"/>. The statement that builds it carries the
    /// whole account (what it holds, why it builds at start, what the build costs); the procedure_stats twin,
    /// <see cref="ProcedureStatsRestartRowIndexName"/>, is the same shape on the other table.
    /// </summary>
    public const string QueryStatsRestartRowIndexName = "idx_query_stats_restart_rows_time";

    /// <summary>The restart-row partial index on <c>collect.procedure_stats</c>, built and read like <see cref="QueryStatsRestartRowIndexName"/>.</summary>
    public const string ProcedureStatsRestartRowIndexName = "idx_procedure_stats_restart_rows_time";

    /// <summary>#7147: the covering index for the Daily Summary's per-day wait sums, on a store where <c>collect.wait_stats</c> is a plain table.</summary>
    public const string WaitStatsDailyCoverIndexName = "idx_wait_stats_daily_cover";

    /// <summary>
    /// #9223: the narrow <c>(server_id, query_hash, collection_time DESC)</c> index on <c>collect.query_store_stats</c>, the twin of
    /// <c>idx_query_stats_server_hash_time</c>. The Query Store fleet summary's raw tail reads it, one range per server.
    /// </summary>
    public const string QueryStoreServerHashIndexName = "idx_query_store_stats_server_hash_time";

    /// <summary>#9172: the covering index for the 24 hour CPU window, on <c>collect.cpu_utilization_stats</c> (a hypertable or a plain table).</summary>
    public const string CpuWindowCoverIndexName = "idx_cpu_utilization_stats_window_cover";

    /// <summary>
    /// #9171: the covering index for the dropped-database read (<c>QueryStoreBackfillState.LastSeenPerDatabaseSql</c>, run by
    /// <c>CurrentDatabaseScope.ReadDroppedAsync</c> for the top-queries reads): <c>(server_id, collection_time) INCLUDE (database_name)</c>.
    /// </summary>
    public const string DatabaseStatesNameCoverIndexName = "idx_database_states_server_time_cover";

    /// <summary>#9171: the narrow <c>database_states</c> index the cover replaces. It was created by migration rung 49.</summary>
    public const string DatabaseStatesNarrowIndexName = "idx_database_states_time";

    /// <summary>
    /// #9171: an index that a covering one replaces, mapped to the replacement. The drop of the old index runs only when the
    /// replacement exists and is valid, so a build that gave up (lock timeout, a start that built another index first) never
    /// leaves the table with neither.
    /// </summary>
    private static readonly Dictionary<string, string> s_replacedBy = new(StringComparer.Ordinal)
    {
        [DatabaseStatesNarrowIndexName] = DatabaseStatesNameCoverIndexName,
    };

    /// <summary>
    /// #7147, #7152: the indexes built only while the table they are on (the <c>ON</c> table of their statement) is a plain
    /// table. A hypertable compresses a chunk after <see cref="TimescaleSupport.CompressAfterDays"/> (1) day, so all but about the
    /// newest day of a month sits in compressed chunks, where a heap btree index on the hypertable cannot serve the read, and the
    /// index would still cost every insert (see the statement). The test is the table, not the extension: a store can have
    /// TimescaleDB installed and still run in plain-PostgreSQL mode with plain heaps. On a hypertable the pass drops the index if it
    /// is there (one built while the table was plain survives the conversion) and counts it as applied, like any index that is
    /// already there.
    /// </summary>
    private static readonly HashSet<string> s_plainTableOnly = new(StringComparer.Ordinal)
    {
        WaitStatsDailyCoverIndexName,
    };

    public static IReadOnlyList<string> ForcePlanFailuresIndexColumns { get; } = new[]
    {
        "server_id", "collection_time",
        "database_name", "query_id", "plan_id", "force_failure_count", "is_forced_plan", "plan_forcing_type", "last_force_failure_reason",
    };

    /// <summary>
    /// The idempotent tuning statements, applied in order, each on its own command (failure-isolated). #4247
    /// DROPPED the three composer covering indexes and two of the three single-row lookup indexes (measured:
    /// near-zero reads, ~25% of two production stores' WAL). What remains under CREATE: the lookup index that
    /// IS read constantly (<c>server_id, query_hash, collection_time DESC</c>), the QS db+query+plan lookup,
    /// the #3573 covering index for the alerting pass (INCLUDE exactly that read's columns, for an Index Only
    /// Scan), and the #3934 store-metrics skip-scan index — then the per-table autovacuum-insert override on
    /// exactly the four growing tables. Bare collect-qualified names; every identifier is a compile-time
    /// constant, never user input, so interpolation is not a concern.
    /// </summary>
    public static IReadOnlyList<string> Statements { get; } = new[]
    {
        /* #4247 DROP (was CREATE): idx_procedure_stats_object_name (object_name, collection_time) INCLUDE
           (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count) — 0-2 scans/store,
           1.1-1.3% of WAL. Reader: the composer's procedure_stats "object_name" dimension (Procedures
           measures), 15 s statement_timeout deadline. Rig-measured (11 servers x 5 days x ~60k rows/day,
           WHERE object_name = <one of 50 procs> AND a 24 h collection_time window): 6.762 ms with the index
           (Index Only Scan) vs. 140.747 ms without it (Parallel Seq Scan, no usable fallback index on this
           table) — 53x under the 7.5 s half-deadline. ProcedureStatsFallbackByObjectSql (oversized-plan
           backlog) also filters object_name but keys server_id/database_name/schema_name first; it is a rare
           fallback read, not measured separately. */
        GuardedDrop("idx_procedure_stats_object_name"),
        /* #4247 DROP (was CREATE): idx_procedure_stats_server_handle_time (server_id, sql_handle,
           collection_time DESC) — 1-6 scans/store, 5.9-8.3% of WAL. Reader:
           DarlingStoredPlanReader.ProcedurePlanXmlBySqlHandleSql (server_id, sql_handle) -> ORDER BY
           collection_time DESC LIMIT 1, McpCommandDeadlines.ReadSeconds = 20 s deadline. Rig-measured, a
           handle seen in the last hour and one last seen on day 1 of 5: 0.137 ms / 0.190 ms with the index vs.
           40.089 ms / 396.139 ms without it (falls back to idx_procedure_stats_time, an Index Scan Backward
           filtered by sql_handle) — worst case 25x under the 10 s half-deadline. */
        GuardedDrop("idx_procedure_stats_server_handle_time"),
        /* #4247 DROP (was CREATE): idx_query_stats_server_handle_time (server_id, sql_handle,
           collection_time DESC) — the #1981 ProcStats comparison's representative-statement LATERAL twin of
           the procedure_stats handle index above. 0-13 scans/store, 8.7-11.3% of WAL. Reader at the time: the
           LEFT JOIN LATERAL in ViewerDataService.ProcedureStats.GetProcedureStatsComparisonAsync (qs.server_id,
           qs.sql_handle) -> ORDER BY collection_time DESC LIMIT 1, ViewerCommandDeadlines base
           InteractiveReadSeconds = 15 s deadline. #5420 removed that lateral: the comparison now finds every
           handle's text with one time-bounded window scan (its texts CTE), not a handle lookup, so no reader
           is left for this index. Rig-measured then, fresh/stale handle: 3.951 ms / 6.462 ms with the index
           vs. 68.200 ms / 565.808 ms without it (idx_query_stats_time, Index Scan Backward filtered by
           sql_handle) — worst case 13x under the 7.5 s half-deadline. */
        GuardedDrop("idx_query_stats_server_handle_time"),
        /* #4247 DROP (was CREATE): idx_query_stats_query_hash (query_hash, collection_time) INCLUDE
           (database_name, delta_worker_time, delta_elapsed_time, delta_execution_count) — 0-1 scans/store,
           2.5-3.2% of WAL. Reader: the composer's query_stats "query_hash" dimension (Queries measures), 15 s
           statement_timeout deadline. Rig-measured (WHERE query_hash = <1 of 3000> AND a 24 h window):
           0.403 ms with the index vs. 1.983 ms without it — PG18 picks a native Index Skip Scan on the KEPT
           idx_query_stats_server_hash_time sibling below (Bitmap Index Scan, 13 index searches, one per
           server_id), ~3780x under the 7.5 s half-deadline. DarlingStoredPlanReader.QueryStatsPlanXmlByHashSql
           also keys on query_hash, but WITH server_id too, so it was already served by that same KEPT sibling
           and was never at risk. */
        GuardedDrop("idx_query_stats_query_hash"),
        "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)",
        /* #4247 DROP (was CREATE): idx_query_store_stats_query_hash (query_hash, collection_time) INCLUDE
           (database_name, module_name, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us,
           max_cpu_time_us) — 0 scans/store (no fallback sibling keyed on query_hash exists for this table), 5.2-8.4%
           of WAL. Reader: the composer's query_store_stats "query_hash" dimension (Query Store measures), 15 s
           statement_timeout deadline. Rig-measured: 0.398 ms with the index (Index Only Scan) vs. 99.968 ms
           without it (Parallel Index Scan on idx_query_store_stats_time filtered by query_hash) — 75x under
           the 7.5 s half-deadline. */
        GuardedDrop("idx_query_store_stats_query_hash"),
        "CREATE INDEX IF NOT EXISTS idx_query_store_stats_server_db_query_plan_time ON collect.query_store_stats (server_id, database_name, query_id, plan_id, collection_time DESC)",
        /* #3573: the alerting pass's forced-plan-failures read (DarlingAlertReadAdapter.ForcePlanFailuresSql,
           WHERE server_id = $1 AND collection_time > $2, a two-hour window, per server, every 30 s pass) outgrew
           its 10 s deadline on the largest production store: 1,744.9 ms cold when the deadline was derived over
           ~6 GB of query_store_stats, 10.3 s excursions at 23 GB. The live plan named the mechanism —

               Index Scan Backward using _hyper_.._chunk_query_store_stats_collection_time_idx
                 Index Cond: (collection_time > now() - '02:00:00')
                 Filter: (server_id = ...)   Rows Removed by Filter: 691,058   actual rows: 37,878
                 Buffers: shared hit=54,664 read=2,643

           — the read walks the ENTIRE fleet's two-hour slice through the TimescaleDB default time index and
           discards 95% of it to keep one server. Forty-three servers deep, that is the whole slice re-read
           forty-three times per pass cycle; warm it is 422 ms, and the excursions are the cold tail whenever
           cache pressure evicts a slice ~4x the size it had on measurement day.

           THE INDEX THAT READ WANTED ALREADY EXISTED. V1's generated idx_query_store_stats_time is exactly
           (server_id, collection_time), it is on the hypertable and on the very chunk in that plan, and the
           planner declined it. Read from the production catalog: server_id's physical correlation is 0.022
           (43 servers interleaved by collection pass) while collection_time's is 0.99999, so the cost model
           prices the composite's heap fetches as one random page per tuple — ~50K pages at random_page_cost
           4 — and the perfectly-correlated time index's 54,664-page stream wins on paper at 58,677. It loses
           in fact by 11x: forced under random_page_cost = 1.1 the SAME statement took the existing composite
           (Bitmap Index Scan, Index Cond on both columns) and touched 5,063 buffers (Heap Blocks: exact=5001)
           instead of 57,307, because a server's rows land in one contiguous run per collection pass (~13
           rows a page) that the planner's single correlation statistic cannot see. A second plain composite,
           however ordered, would be priced identically and ignored identically — which is why this is not
           the (server_id, collection_time DESC) rung the issue first proposed.

           COVERING, so the choice stops depending on the cost model. With every column the read touches in
           the key or INCLUDE, the plan is an Index Only Scan whose cost is the index pages for ONE server's
           two hours and nothing else — no heap component to misprice, at any random_page_cost and at any
           share of the fleet the busiest server grows into. Both uncompressed production chunks read
           relallvisible = 100% of relpages (the insert-autovacuum override below is what keeps them there),
           so heap fetches for visibility are the newest pass's pages at most. Measured on a PG18 /
           TimescaleDB 2.28.1 rig seeded in the production's write pattern: the shipped statement went from
           the identical time-index-plus-Filter plan at 1,514 buffers to Index Only Scan, Heap Fetches: 0,
           50 buffers. The INCLUDE list IS the read's column list, deliberately and exactly — a column added
           to the read and not to this list silently degrades it back to the heap plan, so
           ForcePlanFailuresAccessPathTests pins the two against each other.

           THE COST, stated rather than implied: INCLUDE disables btree deduplication, so this index is
           ~86 bytes/row on the rig (V1's deduplicated composite is ~7) — roughly 0.7-0.9 GB per day-chunk on
           the largest store's 8-16 M rows/day, against a 4.7-9.4 GB heap per chunk. It is self-limiting:
           on 2.28.1 a compressed chunk's uncompressed relation is an empty shell, and CREATE INDEX on the
           hypertable builds an 8 KB page for each one (measured: 4 compressed chunks at 8192 bytes each,
           the 2 live chunks at 46 MB and 27 MB), so the footprint is the one or two uncompressed chunks and
           the compression policy erases the rest a day later. That is also why the owner's "index only the
           uncompressed/new chunks" needs no mechanism: it is what the engine does. New chunks inherit the
           index at creation; a decompressed chunk fills it and recompression empties it (both measured).

           PLAIN CREATE INDEX, ONE TRANSACTION, deliberately. The build takes a ShareLock on the hypertable
           root for its duration — reads proceed, every INSERT into any chunk queues behind it — which is
           why this list runs BEFORE collectors start and why this statement belongs here rather than in the
           ladder (whose applier wraps each rung in a transaction block, the one place the per-chunk form
           below is refused outright). A cancel at SetupTimeoutSeconds rolls the whole build back and the next
           start retries with nothing left behind. The per-chunk form, WITH (timescaledb.transaction_per_chunk),
           was measured and rejected: it takes the same root ShareLock first, buying no write concurrency here,
           and a cancel MID-build — exactly what the command timeout is — commits the chunks built so far,
           leaves the parent index indisvalid = false and the live chunk unindexed, after which this very
           idempotent statement reports "already exists, skipping" on every start forever. CREATE INDEX
           CONCURRENTLY is refused on hypertables ("hypertables do not support concurrent index creation").
           A collision with the compression job on yesterday's chunk makes one of them wait for the other;
           if that is this build past its budget, the cancel-and-retry above is the outcome.

           NOT random_page_cost, though it flipped the plan: at 1.1 the composite won by 53,095 to 58,677 for
           a server holding ~8% of the fleet's rows, and the ratio scales with that share, so the next-busiest
           store or the same one a month on flips back. A store-wide planner setting is also an owner's
           ruling for every read at once, not a lane's fix for one. NOT a partial index WHERE is_forced_plan,
           which would be a few MB: the read aggregates unforced rows on purpose (a plan's previous sighting
           may be its unforced one), so that index needs the statement reshaped and its first-sighting
           semantics changed — the cheaper long-run shape, deferred rather than smuggled in. */
        "CREATE INDEX IF NOT EXISTS " + ForcePlanFailuresIndexName + " ON collect.query_store_stats (server_id, collection_time DESC) INCLUDE (database_name, query_id, plan_id, force_failure_count, is_forced_plan, plan_forcing_type, last_force_failure_reason)",
        /* The legacy-row check (QueryStoreIntervalWide.HasLegacyRowSql, clause 6 of ReadsTableAsync, which only
           the Viewer's duration trend asks for since #6267's follow-up) asks whether the server's window holds a row with no interval start.
           On a field store it holds none, and with only the (server_id, collection_time) index that "no" costs
           a fetch of every heap tuple the window touches: a mean of 4.1 s and 6.8 M blocks per call over a
           day on the largest store measured (#4969), most of it heap fetches in the uncompressed chunks. The read stays exact, because the collector can still store a NULL start when the
           catalog join misses, so the statement is unchanged and this index gives it an index read of a near-empty
           tree.

           WHAT IT HOLDS: only rows WHERE interval_start_time_utc IS NULL. On a field store that is nothing, so
           the index costs near-zero writes and WAL (contrast the random-key WAL that #4247 removed from the
           composer indexes), and it grows only with the rare legacy row.

           WHY THE START PATH: the plain CREATE INDEX takes a ShareLock on the hypertable root, which is safe
           before collectors start and not while they write. CONCURRENTLY is refused on hypertables, and the
           per-chunk form and a background build are unsafe because the Query Store backfill writes backdated
           rows into older chunks (see QueryStoreBackgroundIndexes).

           COMPRESSED CHUNKS: CREATE INDEX on the hypertable builds one index per chunk from that chunk's own
           heap, so each compressed chunk gets an empty 8 KB index and the compressed relations get none. The
           check reads those chunks through the columnar scan on the compressed chunk's server_id segmentby
           index, with the NULL test as a vectorized filter (a few thousand buffers per chunk; 65-153 ms each on the largest production
           store measured). Uncompressed chunks take an index
           scan of this index, which is empty on a field store, so 1-2 buffers per chunk. Before, they took a
           scan of the (server_id, collection_time) index with a heap fetch per row to test the NULL filter.
           QueryStoreLegacyRowIndexLiveTests holds the plan. Measured on a local TimescaleDB 2.30.1 rig with
           the real schema (4 servers x 4 daily chunks x 855,360 rows per server per day, 585 B per row, the
           2 oldest chunks compressed and the 2 newest uncompressed at 2.0 GB of heap each), one server,
           bound parameters, median of 3:
             24 h:  255-359 ms -> 0.043 ms
             168 h: 1.7-3.0 s (126-138 k buffers) -> 5.9 ms (~7.1 k buffers, all in the compressed chunks'
                    columnar scans, which this index does not change).
           On the largest production store measured (6-hour chunks, the last ~27 hours uncompressed), the check for the
           busiest server took 614 ms at 24 h, all of it Bitmap Heap Scans of the five uncompressed chunks that removed
           667,802 rows by the NULL filter; the index serves that whole window. At 168 h it took 1,723 ms, 1.34 s of it
           in the twelve compressed chunks' columnar scans, which this index does not reach.
           As a prepared statement, the check's sixth execution switches to a generic plan with no chunk
           pruning: 57-76 s at 168 h without the index, 6.2 ms with it. The product does not prepare it today.

           THE BUILD'S COST: one read of every uncompressed chunk's heap, under SetupTimeoutSeconds. On that
           rig, 4.0 GB of uncompressed heap built in 0.59-0.66 s warm with the default 2 parallel maintenance
           workers and 1.33 s with none; a serial scan of the same heaps takes 1.21-1.33 s, so the build costs
           about half a serial scan of the uncompressed heaps. Compressed and empty chunks get an 8 KB index,
           and NULL-start rows cost about 35 B each. On the largest production store measured, the build's read set (12
           compressed chunks with empty heaps, 5 uncompressed holding 11.2 GB of heap and 19.7 M rows, none with a NULL
           start) took 56.1 s as one serial scan, 48.3 s of it reading 1.37 M blocks from disk; at the rig's ratio that
           is a first-start build of roughly 30-70 s, inside SetupTimeoutSeconds with over 4x headroom. A build that runs
           out the clock is abandoned and retried at the next start; the hourly pass never builds it. */
        "CREATE INDEX IF NOT EXISTS " + LegacyRowIndexName + " ON collect.query_store_stats (server_id, collection_time) WHERE interval_start_time_utc IS NULL",
        /* The restart rows of query_stats and procedure_stats (#4605). A restart row is a row in which no counter was
           knowable (sample_interval_seconds = 0): a first sighting, an uncredited counter reset (a restart, a
           plan-cache eviction) or a gap past the policy (CollectorDeltaCalculator). The hourly interval
           rollups exclude them, so a read of a long window from the rollups adds the raw restart rows back
           (IntervalRollupRestartRows). With only the (server_id, collection_time) index that arm walks every raw
           row in the window to find a few: 109 s and 12 s on the largest production store measured.

           WHAT IT HOLDS: only rows WHERE sample_interval_seconds = 0. On the largest production store measured,
           a 23-hour window of query_stats held 34,254 of them, a small fraction of its rows, so the index costs few
           writes and little WAL and its tree stays small.

           WHY THE START PATH: the plain CREATE INDEX takes a ShareLock on the hypertable root, which is safe
           before collectors start and not while they write. CONCURRENTLY is refused on hypertables, so the build
           runs only here, under SetupTimeoutSeconds, and the hourly pass never builds it (as for LegacyRowIndexName).

           COMPRESSED CHUNKS: CREATE INDEX on the hypertable builds one index per chunk from that chunk's own
           heap, so each compressed chunk gets an empty 8 KB index and the compressed relations get none. Those
           chunks are still read by the columnar scan; the index serves the uncompressed chunks.

           THE BUILD'S COST: one read of every uncompressed chunk's heap. Measured on a local TimescaleDB 2.30.1 rig
           with the real schema (4 chunks of 12 hours, warm, 2 parallel maintenance workers, median of 3):
             query_stats:     1.5 M rows, 1,065 MB of heap (740 B per row):  CREATE INDEX 242 ms (213-322 ms);
                              a serial count(*) of the same heap, 275 ms; with no parallel workers the build took 465 ms.
             procedure_stats: 3.5 M rows, 1,288 MB of heap (386 B per row):  CREATE INDEX 297 ms (293-321 ms);
                              a serial count(*) of the same heap, 355 ms; with no parallel workers the build took 630 ms.
           So the build costs about one serial scan of the heap, and about 1.7 serial scans if the server has no
           parallel workers free. On the largest production store measured, the read set is 12 GB of uncompressed
           query_stats heap and 6.5 GB of procedure_stats in three chunks each, and 11.2 GB of heap was read in 56.1 s
           serially, cold (#5021). At that rate the serial scans take about 60 s and 33 s, so the first-start builds
           are roughly 60-100 s and 33-56 s, one statement each: at least 3x headroom inside the 300 s
           SetupTimeoutSeconds per statement. A build that runs out of time is abandoned and retried at the next
           start; the hourly pass never builds it. RestartRowIndexLiveTests holds the plan and the definitions. */
        "CREATE INDEX IF NOT EXISTS " + QueryStatsRestartRowIndexName + " ON collect.query_stats (collection_time) WHERE sample_interval_seconds = 0",
        "CREATE INDEX IF NOT EXISTS " + ProcedureStatsRestartRowIndexName + " ON collect.procedure_stats (collection_time) WHERE sample_interval_seconds = 0",
        /* #8570: the one-procedure duration trend (DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql) fetches its
           procedure's rows WHERE server_id = $1, the database and the procedure's full name, inside its collection_time window.
           Every other index on the table leads on the server and the time, or on the time alone, so without this one that read
           fetches every row the server stored in the window and keeps a few. The third key is the expression the read compares
           (DurationTrendRouting.ProcedureFullNameSql), so the planner matches the predicate to the key. The reader runs that
           statement only on a store where the index exists and is valid (ProcedureStatsNameIndex.IsPresentAsync); on any other
           store it runs the FILTER statement, which needs no index and returns the same rows.

           WHAT IT HOLDS: every row of the uncompressed chunks, so unlike the partial indexes above every procedure_stats insert
           writes it. On the rig below it is 2.9 GB over 51.8 M rows and 25 GB of heap, 11% of the heap. Measured there with
           810-row batches (full_page_writes on, wal_compression off): 2.5 -> 4.2 ms a batch, and 461 -> 918 KB of WAL a batch
           averaged over 20 checkpoint cycles of 15 batches. Between checkpoints a batch is 664 KB; the first batch after a
           checkpoint is 4.5 MB, because each procedure's newest entry lands on its own leaf page and logs a full page image.

           WHY THE START PATH: the plain CREATE INDEX takes a ShareLock on the hypertable root, which is safe before collectors
           start and not while they write: an insert waits for the whole build. CONCURRENTLY is refused on hypertables, so the
           build runs only here, under SetupTimeoutSeconds, and the hourly pass never builds it (as for LegacyRowIndexName). A
           build that runs out of time here is rolled back, logged and retried at the next start, and the store keeps working
           without it: the read keeps the FILTER statement. A ladder rung that ran out of time would fail the start instead.

           COMPRESSED CHUNKS: CREATE INDEX on the hypertable builds one index per chunk from that chunk's own heap, so each
           compressed chunk gets an empty 8 KB index and the compressed relations get none. Those chunks are still read by the
           columnar scan; the index serves the uncompressed chunks. Compressing a chunk empties its index, decompressing one
           refills it and a new chunk inherits it (each measured on TimescaleDB 2.30.1).

           THE BUILD'S COST: one read of every uncompressed chunk's heap and a sort of its rows. Measured on a local
           TimescaleDB 2.30.1 rig with the real schema (73 hypertables, 21,900 chunks; procedure_stats with 300 daily chunks,
           298 of them compressed, and 51.8 M uncompressed rows in 25 GB of heap, three times the server's memory):
           the pass built it in 43.6 s and 59.5 s with the default 2 parallel maintenance workers and in 89.4 s with none,
           and the statement on its own took 35.1 s and 72.8 s; a pass over the converged store took 0.09 s. An insert from
           a second session waited once, for the length of the build. With the server cancelling the build at 10 s in place
           of SetupTimeoutSeconds, each start ran 10.5 s, logged the build's warning, left no index and no chunk index
           behind and applied every other statement, and the start after it tried again. A store whose build cannot fit
           the budget pauses for it at every start and keeps the FILTER statement.

           SHARED BUILD TIME: the index shares the start's build time budget (s_startBuildBudget, #9795) with the other
           large indexes. On a store where earlier ones are also missing from tables that have rows, this start builds
           those first and this index gets the time they leave; if that runs out it waits for the next start, and until
           then the read keeps the FILTER statement. On a plain-table store the wait_stats daily cover
           (WaitStatsDailyCoverIndexName) is later in this list and takes what this index leaves.

           THE READ, on a rig of one server with 8.0 M rows in 7 daily chunks, 5 of them compressed, warm, one procedure in
           one database:
             24 h:  355 ms (356,099 buffers) -> 8.2 ms (3,620 buffers)
             72 h:  651 ms (172,271 buffers) -> 59 ms (36,949 buffers)
             168 h: 1,217 ms (268,296 buffers) -> 189 ms (113,391 buffers)
           Without the index the by-name statement is slower than the FILTER statement in 11 of 48 cases and reads 3.4-12x
           the buffers, which is why the reader asks for the index first. ProcedureTrendNameIndexLiveTests holds the plan and
           the equality of the two statements. */
        ProcedureStatsNameIndex.CreateSql,
        "ALTER TABLE collect.procedure_stats SET (" + InsertTuningOptions + ")",
        "ALTER TABLE collect.query_stats SET (" + InsertTuningOptions + ")",
        "ALTER TABLE collect.query_store_stats SET (" + InsertTuningOptions + ")",
        /* pg_statement_stats is query_stats' per-minute PostgreSQL twin — same shape, same cadence, same
           pure-insert hypertable chunks — so the identical reasoning applies: the stock 0.2 scale factor
           leaves the day's hot chunk stale before the TimescaleDB rollover and the Index Only Scan degrades
           to heap fetches. It was simply missed when the PostgreSQL collectors landed, since this list is
           hand-maintained rather than derived from the catalog. */
        "ALTER TABLE collect.pg_statement_stats SET (" + InsertTuningOptions + ")",
        /* #2402: query_plan_dim needs the OTHER knob, and needs it for the opposite reason. Every override
           above tunes INSERT vacuuming so the visibility map stays current on pure-insert hypertable chunks.
           This table is not a hypertable and is not insert-only: retention DELETEs from it, so its churn is
           DEAD TUPLES, which autovacuum_vacuum_insert_* does not govern at all.

           Left at the stock 0.2 it needs 20% of the table dead before autovacuum will look at it — on the
           dogfood store that is ~2.4 M rows, and it showed: 1,223,777 dead tuples with the last autovacuum
           two days earlier, while its own TOAST table (which has its own thresholds against 57 M chunks) had
           been vacuumed 152 times. The parent is the half that matters here, because the dead rows a purge
           leaves behind are precisely the index entries the NEXT purge has to visit and test for visibility
           before discarding (from V298, #9304, those are the idx_query_plan_dim_seen_last_seen entries of the
           narrow stamp table, which cascades with the row; the wide row's own last_seen is no longer indexed).
           Untuned, each purge makes the following one slower and then waits days for cleanup.

           0.02 + 10000 sizes the trigger to roughly one purge's worth of deletions rather than to the
           table's total size, so cleanup follows the work that created it. */
        "ALTER TABLE collect.query_plan_dim SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 10000)",
        /* #8790: five more plain tables that are written like queues and ran on the stock settings. None is a hypertable, so
           each ALTER here tunes the table itself. Fillfactor only affects pages written from now on, so a table that is
           already mostly empty space needs one rewrite before the settings show (the store health page flags it, #8787).

           Each table was classified by its write path:
           - oversized_plan_backlog: UPDATE-heavy and DELETE-heavy. A re-sighting updates last_seen_at and observed_bytes, a
             fetch updates plan_xml, captured_at and the attempt stamps, and the trim and the retention horizon delete. Its
             indexes lead on observed_bytes and on expired_at, so a deleted row leaves half-empty pages in the middle of the
             index that VACUUM never merges (#8570). Fillfactor 80 lets an update that leaves those columns alone stay on its
             page (HOT), and 0.02 + 1000 starts cleanup after about 2% of the rows are dead instead of 20%.
           - query_stats_hour_ledger and procedure_stats_hour_ledger (#9357): UPDATE-heavy. Every collection adds its count to the current hour's row, so one row is
             updated about once a minute per server for an hour. n is not indexed, so those updates can be HOT with room on
             the page. The table grows with servers x 24 x the rollup's retention days, so at the stock 0.2 the dead rows of
             a large fleet wait a long time for a vacuum; 0.02 + 1000 keeps that trigger near a few minutes of updates.
           - collector_state: UPDATE-heavy. SaveCollectorStateAsync upserts each state key after every collection cycle, and
             only the key is indexed. Fillfactor 80 is the whole change: the table is small, so the stock trigger (50 + 20%
             of a few hundred rows) already fires within a cycle or two, and a lower scale factor would add nothing.
           - query_text_dim: DELETE-heavy, the same shape as query_plan_dim above. Since V298 (#9304) the guarded last_seen refresh
             writes the narrow collect.query_text_dim_seen row, not this one: this row is rewritten only every
             PayloadDimensionSeen.WideRowRefreshHours, for an older build, and retention deletes it by the stamp table's
             order. Its own last_seen is no longer indexed, but the rewrite is rare, so fillfactor would not help; only the
             vacuum trigger matters. Its rows are far fewer than the plan dimension's, so the threshold is 1000 and not 10000.
           - pg_statement_text: DELETE-heavy, with a few updates. The hourly refresh writes a statement only when its text or
             own-entry list changed or its last_seen is more than PayloadDimensions.LastSeenRefreshGuardHours behind (#8988), so
             an unchanged statement is no longer rewritten each hour. last_seen is indexed twice, so the updates that remain can
             never be HOT; retention deletes the rest. Autovacuum settings only, for the reason given for query_text_dim.

           Left alone on purpose: query_store_watermarks inserts one row per collection and retention deletes the oldest rows
           in key order, so whole pages empty out and VACUUM recycles them (its upsert updates an existing row only when a
           batch is retried). query_store_text and query_store_plan_map have had fillfactor 90 since V149 (#4250).
           query_store_text_seen is created at fillfactor 90 (V281, #9043): its last_seen is not indexed, so the guarded
           stamp stays heap-only where the page has room, and its rows are deleted with their text row. */
        "ALTER TABLE collect.oversized_plan_backlog SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.query_stats_hour_ledger SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.procedure_stats_hour_ledger SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.collector_state SET (fillfactor = 80)",
        "ALTER TABLE collect.query_text_dim SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.pg_statement_text SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        /* #8966: twelve more plain tables with steady DELETE churn. None is a hypertable, and every one is created by a
           migration, so it exists before the first tuning pass. The stock trigger (50 rows + 20% of the table) lets a
           fifth of the rows sit dead before a vacuum starts, and their space is reusable only after that vacuum, so
           these tables spend much of their life a fifth to a third empty.

           Classified by write path:
           - Delete-and-rebuild rollups: query_store_compose_stamp, query_store_compose_fleet and query_store_hash_hourly
             clear an hour and insert it again, query_store_hash_daily, query_store_top_daily and plan_regression_daily do
             the same for a day, and each ages out the rows below its floor. Every rebuild leaves the previous build dead.
           - attention_events: inserts plus the time-sliced retention DELETE while it is a plain table (#8967: on a TimescaleDB store it is a
             hypertable and retention drops chunks, and the ALTER below runs without a warning on it).
           These seven are the largest plain tables on a busy store, so they take 0.05 + 10000 rather than 0.02: a vacuum
           pass also scans every index on its table, and the autovacuum cost budget is shared by every worker, so a
           tighter trigger on a table this size would hold a worker that other tables are waiting for.

           - pg_io_stats_hourly: clears and rebuilds an hour, and prunes below the retention floor.
           - analysis_findings, fleet_sweep_server_verdicts and procedure_stats_totals: inserts plus a retention DELETE of the
             oldest rows. Whole pages empty out, but only a vacuum makes them reusable.
           - collection_log_hourly: UPDATE-heavy. Every pass upserts the newest CollectionLogHourly.RecomputeHours hours, so a
             row is rewritten many times. The upsert changes only value columns (the key columns, the only indexed ones,
             never change), so every update can be HOT when the page has room; fillfactor 80 gives it that room.
           These are smaller, so 0.02 + 1000 starts cleanup after about 2% of the rows are dead. */
        "ALTER TABLE collect.query_store_compose_stamp SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.query_store_compose_fleet SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.query_store_hash_hourly SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.query_store_hash_daily SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.query_store_top_daily SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.plan_regression_daily SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        /* #9111: the Query Store closed-day rollups. query_store_grid_daily and query_store_slicer_hourly clear a day and insert it again on every build and
           every rebuild of a marked day, and both age out the days below the retention floor, so they follow query_store_top_daily. The built-days table and
           the tick heartbeat are one row per (server, day) and per server, updated in place; they take the smaller trigger. */
        "ALTER TABLE collect.query_store_grid_daily SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.query_store_slicer_hourly SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.query_store_closed_day_built SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.query_store_closed_day_tick SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        /* #9482: the clutter plan rollups are plain tables, so the hypertable insert sweep never reaches them. They are written an hour or a
           day at a time (a rebuild deletes and re-inserts its hour or day, and retention prunes the old ones). The fleet median's narrow reads
           (PlanChurnP95ClosedDaysSql, PlanChurnP95ClosedHoursSql) select key columns only, so they can walk ux_query_store_clutter_plan_hourly
           and ux_query_store_clutter_plan_daily index-only, and every row the visibility map does not cover yet is a heap fetch. At the stock
           0.2 insert scale factor that lag runs to many hours of builds. The insert tuning bounds it to about one trigger's worth. The
           dead-tuple knobs stay at stock on purpose: each insert-triggered vacuum also clears the deleted rows. */
        "ALTER TABLE collect.query_store_clutter_plan_hourly SET (" + InsertTuningOptions + ")",
        "ALTER TABLE collect.query_store_clutter_plan_daily SET (" + InsertTuningOptions + ")",
        "ALTER TABLE collect.attention_events SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 10000)",
        "ALTER TABLE collect.pg_io_stats_hourly SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.analysis_findings SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.fleet_sweep_server_verdicts SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.procedure_stats_totals SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE collect.collection_log_hourly SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        /* #3934: the store-metrics latest read (DarlingStoreMetricsReader.StoreMetricsLatestSql) took each
           object's newest row with DISTINCT ON (object_kind, object_name) ... ORDER BY object_kind,
           object_name, metric_time DESC over the WHOLE table — the only index was idx_store_metrics_time
           (metric_time) alone, so every call sorted every retained row. The table keeps 400 days of hourly
           sweeps at ~250 objects/sweep, so the sort grows linearly for over a year after a store is created:
           7,772 ms and an external merge sort spilling ~180 MB at full retention on a CI-sized rig (2.4 M
           rows), 225 ms on DARLING01 today (83,355 rows). A RESULTS-INVARIANT covering-shape index, same
           reasoning as every other statement in this list — Erik's ruling on the issue was that this needed
           no migration rung, since the composer's Tuning stage already creates exactly this kind of index
           idempotently at every start and, for the ALTER TABLE settings, hourly (#3817, #3913). Paired with the skip-scan rewrite of
           StoreMetricsLatestSql below, the latest read went from 7,772 ms to 15 ms on the same seed with
           identical rows (251). */
        "CREATE INDEX IF NOT EXISTS idx_store_metrics_kind_name_time ON collect.store_metrics (object_kind, object_name, metric_time DESC)",
        /* #7147: the Daily Summary and calendar month read on a store where wait_stats is a plain table (a bring-your-own
           PostgreSQL store, or one with TimescaleDB installed that runs in plain-PostgreSQL mode: plain heaps, no rollups). That read has no rollup to take, so it scans a month of one server's raw
           wait_stats; the servers interleave in the heap by time, so with only the (server_id, collection_time) index it
           reads every heap page of the month for one server's share of the rows. This is covering for exactly that read
           (DailySummarySql: server_id and collection_time to filter, wait_type and delta_wait_time_ms for the per-day wait
           sums), so it becomes an Index Only Scan over the server's own slice of the index. The distinct-query count needs
           no new index: it reads collect.query_stats directly and takes idx_query_stats_server_hash_time above. Measured on a
           plain-heap test store, 4 servers x 33 days, 3.8 M query_stats and 5.7 M wait_stats rows, 31-day read for one server,
           EXPLAIN (ANALYZE, BUFFERS): 219,525 buffers and 2,420 ms before, about 31,000 buffers and 1,550 ms after (both warm:
           the buffer count is the cold-disk cost). Plain tables only (s_plainTableOnly, decided by whether wait_stats itself is
           a hypertable, #7152): a hypertable compresses a chunk after TimescaleSupport.CompressAfterDays (1) day, so all but
           about the newest day of the month sits in compressed chunks, where a heap btree index on the hypertable cannot serve
           the read, and the index would still cost every insert. */
        "CREATE INDEX IF NOT EXISTS " + WaitStatsDailyCoverIndexName + " ON collect.wait_stats (server_id, collection_time) INCLUDE (wait_type, delta_wait_time_ms)",
        /* #9172: the Server Inventory's 24 hour CPU window (DarlingFinOpsInventoryReader.CpuCtes) reads three columns of every
           row in the window: server_id and sqlserver_cpu_utilization, filtered on collection_time. With only the
           collection_time index it was an Index Scan with a heap fetch per row (or a Seq Scan of the day's whole chunk when
           random_page_cost is the stock 4), so a cold first call paid a disk wait for each of about 650 pages. This index is
           covering for exactly that read, so each of the window's chunks is an Index Only Scan over its own time range.
           Time leads, so a new row lands at the right edge of the index, and the index does not scatter page images through
           the WAL as the #4247 random-key indexes did. On a hypertable store that is the same place as the default
           collection_time index TimescaleDB creates; on a plain store this is the first time-leading index the table has
           (the table's own idx_cpu_utilization_stats_time leads with server_id). Measured
           on a rig with the store's random_page_cost of 1.1, 39 servers sampled once a minute for 30 days (1.68 M rows,
           chunks older than a day compressed), the window is 55,700 rows: EXPLAIN (ANALYZE, BUFFERS) after a PostgreSQL
           restart reads 654 execution pages and 104 planning pages before, 221 and 111 after, with identical rows. The
           window only ever touches the two newest day chunks, which are uncompressed, so the compressed chunks cost the
           index nothing (their heaps are empty). */
        "CREATE INDEX IF NOT EXISTS " + CpuWindowCoverIndexName + " ON collect.cpu_utilization_stats (collection_time) INCLUDE (server_id, sqlserver_cpu_utilization)",
        /* #9223: the Query Store fleet summary's raw tail (DarlingMcpQueryFleetSummaryTools.QueryStoreTailCtes) filters query_hash = one value
           over the newest raw chunk. #4247 dropped idx_query_store_stats_query_hash (5.2-8.4% of WAL, 0 scans then), and no index with
           query_hash in it was left on this table, so on a busy store the tail was a Parallel Seq Scan of the whole newest chunk (about
           244k blocks for a window of about two hours). This is the mirror of the kept query_stats sibling above: server_id leads so the
           fleet summary can run its tail once per server (a plain index range on every supported PostgreSQL, not only through PostgreSQL
           18's skip scan), query_hash second, collection_time last. No INCLUDE list: the tail is a few hundred rows per call, so the heap
           fetches are cheap, and a narrow index keeps the write cost down.

           WHY THE START PATH: the plain CREATE INDEX takes a ShareLock on the hypertable root for the whole build: reads proceed, every
           INSERT queues. That is safe before collectors start and not while they write, which is also why QueryStoreBackgroundIndexes
           builds nothing on a hypertable (CONCURRENTLY is refused, and the Query Store backfill writes backdated rows into older chunks,
           so a per-chunk build would block it). The hourly pass never builds it: it warns once that the next start does. It shares the
           start's build time budget (#9795), last of the budget indexes, because it is the one that sorts every row of the uncompressed chunks
           rather than testing a filter. Compressed chunks get an empty 8 KB index. A build that outruns SetupTimeoutSeconds rolls back
           whole and is recorded (#9289, PgIndexBuildAttempts): the next start skips it for 24 hours unless the table has fewer uncompressed
           pages than recorded, and a successful build deletes the record. A build that ran under less than that, because earlier builds
           used part of the start's budget, is not recorded when it runs out: the next start tries it again. The reader keeps its one-filter shape until the index is valid. */
        "CREATE INDEX IF NOT EXISTS " + QueryStoreServerHashIndexName + " ON collect.query_store_stats (server_id, query_hash, collection_time DESC)",
        /* #9171: the dropped-database read behind the top-queries reads (QueryStoreBackfillState.LastSeenPerDatabaseSql: server_id and
           collection_time to filter, database_name to group) fetched the heap row of every snapshot row in the window, because the only
           index was (server_id, collection_time). With database_name INCLUDEd the read is an Index Only Scan with the same rows. Not
           plain-table-only: the newest day of a TimescaleDB store is uncompressed and takes the same scan. The snapshots are one row per
           database per cycle, so the write cost is small. Measured on a test store (1,500 databases, 5-minute snapshots, the read's
           statement, EXPLAIN (ANALYZE, BUFFERS)): plain tables 24 h window 6,157 buffers before and 2,150 after, 7 days 30,159 before and
           14,919 after (the 7-day plan takes the index at random_page_cost 1.1 and stays a sequential scan at the default 4);
           TimescaleDB 24 h 5,818 before and 3,856 after, 7 days unchanged because the older chunks are compressed. The narrow index
           it replaces is dropped on the next line once this one is valid: every reader of the narrow one (MAX(collection_time) probes,
           the database-name lists) is served by the cover, which carries the same two key columns. */
        "CREATE INDEX IF NOT EXISTS " + DatabaseStatesNameCoverIndexName + " ON collect.database_states (server_id, collection_time) INCLUDE (database_name)",
        GuardedDrop(DatabaseStatesNarrowIndexName),
    };

    /// <summary>
    /// Applies every <see cref="Statements"/> statement, each on its own command and FAILURE-ISOLATED: a single
    /// failure (e.g. a column absent on an odd store) warns and the sweep continues — the store keeps working,
    /// just without that one index. Returns the count that applied (or no-op'd) cleanly. Idempotent, so a re-run
    /// on an already-tuned store no-ops. The start path builds a missing index under a bounded lock_timeout.
    /// </summary>
    public static Task<int> ApplyAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default) =>
        ApplyAsync(connection, logger, hourly: false, cancellationToken);

    /// <summary>
    /// Indexes the hourly pass found absent and has already warned about, so the warning is logged once per
    /// process rather than every hour.
    /// </summary>
    private static readonly ConcurrentDictionary<string, byte> s_hourlyMissingWarned = new(StringComparer.Ordinal);

    /// <summary>Clears the once-per-process warned set so a test can observe the first warning again.</summary>
    internal static void ResetHourlyMissingWarnings() => s_hourlyMissingWarned.Clear();

    /// <summary>
    /// <see cref="ApplyAsync(NpgsqlConnection, ILogger?, CancellationToken)"/> with a choice of pass. With
    /// <paramref name="hourly"/> set (the convergence tick, beside live collection) a missing index is NOT
    /// built: one Warning per index per process says the next service start builds it, and it is not counted
    /// as applied. Every other statement runs as on the start path: an <c>ALTER TABLE ... SET</c> is issued on either
    /// pass only when <see cref="AlterIsConvergedAsync"/> finds a setting missing or different.
    /// </summary>
    public static Task<int> ApplyAsync(NpgsqlConnection connection, ILogger? logger, bool hourly, CancellationToken cancellationToken = default) =>
        ApplyAsync(connection, logger, hourly, clock: null, SetupTimeoutSeconds, cancellationToken);

    /// <summary>
    /// The pass with its two test seams (#9289): the clock the build-attempt record is stamped and aged with and the builds are timed
    /// with (#9795), and the CommandTimeout (seconds) the bounded start-path index build runs under. Production passes
    /// <see cref="SetupTimeoutSeconds"/> and the system clock.
    /// </summary>
    internal static async Task<int> ApplyAsync(NpgsqlConnection connection, ILogger? logger, bool hourly, TimeProvider? clock, int buildTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        clock ??= TimeProvider.System;
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var applied = 0;
        var budgetSpent = TimeSpan.Zero;
        var buildStartedAt = 0L;
        Dictionary<string, bool?>? hypertableByTable = null;
        foreach (var statement in Statements)
        {
            var sql = statement;
            var dropIssued = false;
            var bounded = false;
            var budgeted = false;
            var shortened = false;
            TimeSpan? buildElapsed = null;
            PgIndexBuildAttempts.Attempt? priorAttempt = null;
            long? buildPages = null;
            var commandTimeout = SetupTimeoutSeconds;
            try
            {
                if (statement.StartsWith(CreateIndexPrefix, StringComparison.Ordinal))
                {
                    var indexName = CreatedIndexName(statement);
                    if (s_plainTableOnly.Contains(indexName))
                    {
                        var indexTable = CreatedIndexTable(statement);
                        hypertableByTable ??= new Dictionary<string, bool?>(StringComparer.Ordinal);
                        if (!hypertableByTable.TryGetValue(indexTable, out var isHypertable))
                        {
                            isHypertable = await TableIsHypertableAsync(connection, indexTable, logger, cancellationToken);
                            hypertableByTable[indexTable] = isHypertable;
                        }

                        if (isHypertable is null)
                        {
                            /* Unknown (the check failed and logged its Warning): neither build nor drop. Counted as applied, as a skip is. */
                            applied++;
                            continue;
                        }

                        if (isHypertable == true)
                        {
                            /* #7152: a cover built while the table was plain survives the conversion (create_hypertable with
                               migrate_data keeps existing indexes) and then only costs every insert, so a hypertable drops it.
                               A failed drop falls to the catch below: one Warning, the pass goes on, the next pass retries. */
                            dropIssued = true;
                            using var drop = new NpgsqlCommand(GuardedDrop(indexName), connection) { CommandTimeout = SetupTimeoutSeconds };
                            await drop.ExecuteNonQueryAsync(cancellationToken);
                            applied++;
                            continue;
                        }
                    }

                    using var probe = new NpgsqlCommand(IndexValidSql, connection) { CommandTimeout = SetupTimeoutSeconds };
                    probe.Parameters.AddWithValue(indexName);
                    var valid = await probe.ExecuteScalarAsync(cancellationToken);
                    if (valid is not null and not DBNull)
                    {
                        if (valid is false)
                        {
                            logger?.LogWarning(
                                "Index collect.{Index} exists but is not valid; it is left as it is and is not rebuilt",
                                indexName);
                        }

                        applied++;
                        continue;
                    }

                    if (hourly)
                    {
                        if (s_hourlyMissingWarned.TryAdd(indexName, 0))
                        {
                            logger?.LogWarning(
                                "Index collect.{Index} on {Table} is missing; the hourly pass does not build indexes beside live collection, so the next service start builds it",
                                indexName, CreatedIndexTable(statement));
                        }

                        continue;
                    }

                    /* #6460: a table with no rows builds its index in no time and pauses nothing, so it does not use the
                       shared build budget. A brand-new store builds every large index at its first start. */
                    var onBudget = SharesStartBuildBudget(indexName)
                        && !await TableIsEmptyAsync(connection, CreatedIndexTable(statement), logger, cancellationToken);

                    /* #9795: the budget builds of one start share StartBuildBudgetSeconds. Whole seconds, because CommandTimeout is
                       whole seconds and 0 there means no limit: with under a second left the build waits for the next start. */
                    var secondsLeft = (int)Math.Floor((TimeSpan.FromSeconds(StartBuildBudgetSeconds) - budgetSpent).TotalSeconds);
                    if (onBudget && secondsLeft < 1)
                    {
                        logger?.LogInformation(
                            "Index collect.{Index} on {Table} is missing and is built at the next start: the index builds of this start have used the {BudgetSeconds} s a start gives them in total",
                            indexName, CreatedIndexTable(statement), StartBuildBudgetSeconds);
                        continue;
                    }

                    if (indexName == QueryStoreServerHashIndexName)
                    {
                        /* #9289: this build is bounded. A recorded timeout skips it while it would only time out again, without using the budget. */
                        var bound = await BuildBoundAsync(connection, logger, indexName, CreatedIndexTable(statement), clock, cancellationToken);
                        if (bound.Skip)
                        {
                            continue;
                        }

                        bounded = true;
                        priorAttempt = bound.Prior;
                        buildPages = bound.Pages;
                        commandTimeout = buildTimeoutSeconds;
                    }

                    if (onBudget && secondsLeft < commandTimeout)
                    {
                        /* #9795: earlier builds used part of the budget, so this one runs under what is left of it. */
                        commandTimeout = secondsLeft;
                        shortened = true;
                    }

                    if (onBudget)
                    {
                        /* #9827: collection waits for this build, so the log says which index, on which table, and for how long at most.
                           commandTimeout is the smaller of the build's own limit and what is left of the start's shared build time. */
                        logger?.LogInformation(
                            "Index collect.{Index} on {Table} is missing and is being built at this start: collection waits while it builds, for up to {LimitSeconds} s",
                            indexName, CreatedIndexTable(statement), commandTimeout);
                    }

                    budgeted = onBudget;
                    buildStartedAt = clock.GetTimestamp();
                    sql = GuardedCreate(statement);
                }
                else if (ReplacedIndexOf(statement) is { } replacement)
                {
                    /* #9171: a drop of an index a covering one replaces waits for the replacement to exist and be valid. */
                    using var replacementProbe = new NpgsqlCommand(IndexValidSql, connection) { CommandTimeout = SetupTimeoutSeconds };
                    replacementProbe.Parameters.AddWithValue(replacement);
                    if (await replacementProbe.ExecuteScalarAsync(cancellationToken) is not true)
                    {
                        continue;
                    }
                }
                else if (statement.StartsWith(AlterTablePrefix, StringComparison.Ordinal))
                {
                    if (await AlterIsConvergedAsync(connection, statement, logger, cancellationToken))
                    {
                        /* #8949: the table and its chunks already hold these settings. The ALTER would take SHARE UPDATE
                           EXCLUSIVE to change nothing and cancel an autovacuum running there. Counted as applied, as a skip is. */
                        applied++;
                        continue;
                    }

                    sql = GuardedAlter(statement);
                }

                using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeout };
                await command.ExecuteNonQueryAsync(cancellationToken);
                if (budgeted)
                {
                    buildElapsed = clock.GetElapsedTime(buildStartedAt);

                    /* #9827: the build the line before it announced finished. A timeout or a busy table logs its own line instead. */
                    logger?.LogInformation(
                        "Index collect.{Index} on {Table} was built at this start in {ElapsedSeconds:F1} s",
                        CreatedIndexName(statement), CreatedIndexTable(statement), buildElapsed.Value.TotalSeconds);
                }

                applied++;
                if (bounded && priorAttempt is not null)
                {
                    await ClearBuildAttemptAsync(connection, logger, CreatedIndexName(statement), cancellationToken);
                }
            }
            catch (PostgresException lockBusy) when (lockBusy.SqlState == PostgresErrorCodes.LockNotAvailable
                && (statement.StartsWith(CreateIndexPrefix, StringComparison.Ordinal)
                    || statement.StartsWith(AlterTablePrefix, StringComparison.Ordinal)))
            {
                if (statement.StartsWith(AlterTablePrefix, StringComparison.Ordinal))
                {
                    logger?.LogInformation(
                        "Tuning setting on {Table} not applied this pass: the table was busy; the next pass tries again",
                        AlteredTable(statement));
                }
                else if (dropIssued)
                {
                    /* #7152: the statement that hit the lock was the cover's drop, so the cover is still on the hypertable. */
                    logger?.LogInformation(
                        "Index {Index} not dropped this pass: the table was busy; the next pass tries again",
                        CreatedIndexName(statement));
                }
                else
                {
                    logger?.LogInformation(
                        "Index {Index} not built this pass: the table was busy; the next pass tries again",
                        CreatedIndexName(statement));
                }

                await RollbackBestEffortAsync(connection, logger, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (statement.StartsWith(CreateIndexPrefix, StringComparison.Ordinal) && IsBuildTimeout(ex))
                {
                    var elapsed = clock.GetElapsedTime(buildStartedAt);
                    buildElapsed = elapsed;
                    var recorded = false;

                    /* #9795: a shortened build had less than its own limit, so running out says nothing about whether it fits the
                       limit. No record is written for it (#9289), and the next start tries it again. */
                    if (!shortened && bounded && buildPages is { } pages)
                    {
                        /* The record is written outside the aborted transaction, so the ROLLBACK comes first. */
                        await RollbackBestEffortAsync(connection, logger, cancellationToken);
                        recorded = await RecordBuildAttemptAsync(
                            connection, logger, CreatedIndexName(statement),
                            new PgIndexBuildAttempts.Attempt(clock.GetUtcNow().UtcDateTime, pages, (long)elapsed.TotalMilliseconds), cancellationToken);
                    }

                    logger?.LogWarning(
                        "{Message}",
                        shortened
                            ? ShortenedBuildTimeoutMessage(CreatedIndexName(statement), CreatedIndexTable(statement), elapsed.TotalSeconds, commandTimeout)
                            : recorded
                                ? BoundedBuildTimeoutMessage(CreatedIndexName(statement), CreatedIndexTable(statement), elapsed.TotalSeconds, buildPages!.Value, buildTimeoutSeconds)
                                : BuildTimeoutMessage(CreatedIndexName(statement), CreatedIndexTable(statement), elapsed.TotalSeconds));
                }
                else
                {
                    logger?.LogWarning(
                        "Composer performance-tuning statement failed — the store keeps working without it ({Statement}): {Message}",
                        statement, ex.Message);
                }

                /* #4247: a guarded DROP that hits its lock_timeout fails INSIDE its own explicit BEGIN, and
                   Npgsql's simple-query protocol stops at that first error — the trailing COMMIT text in the
                   same command never runs, so the connection is left "in failed transaction" for whatever this
                   loop sends next. A plain autocommit statement never does this (no explicit BEGIN of its own
                   to leave open), so this is a no-op for every statement above except a failed guarded drop —
                   but it must run unconditionally, since a future statement could open a transaction the same
                   way. ROLLBACK outside a transaction is a harmless no-op (a WARNING, not an error), so this is
                   always safe; if it too fails the connection is unusable and every later statement's own
                   failure will say so. */
                await RollbackBestEffortAsync(connection, logger, cancellationToken);
            }
            finally
            {
                if (budgeted)
                {
                    /* #9795: a budget build is charged for the time it held the start, whether it built, timed out or found the table busy. */
                    budgetSpent += buildElapsed ?? clock.GetElapsedTime(buildStartedAt);
                }
            }
        }

        logger?.LogInformation(
            "Composer performance tuning applied ({Applied}/{Total} covering-index / autovacuum statements)",
            applied, Statements.Count);

        applied += await ApplyHypertableInsertTuningAsync(connection, logger, cancellationToken);
        return applied;
    }

    /// <summary>
    /// True when a start-path <c>CREATE INDEX</c> failed because it ran out of its <c>CommandTimeout</c>: an
    /// Npgsql exception with an inner <see cref="TimeoutException"/>, or a server <c>57014</c> (query_canceled).
    /// Anything else is an ordinary statement failure.
    /// </summary>
    internal static bool IsBuildTimeout(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            if (e is TimeoutException || (e is PostgresException pg && pg.SqlState == PostgresErrorCodes.QueryCanceled))
            {
                return true;
            }

            if (e.InnerException is null)
            {
                break;
            }
        }

        return false;
    }

    /// <summary>The warning for a start-path index build that timed out.</summary>
    internal static string BuildTimeoutMessage(string indexName, string table, double elapsedSeconds) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Index collect.{indexName} on {table} was not built: the build ran out of time after {elapsedSeconds:F0} s (limit {SetupTimeoutSeconds} s) and was rolled back; it is retried at the next start, and each try pauses collection at start for up to {SetupTimeoutSeconds} s");

    /// <summary>
    /// The warning for a start-path index build that ran under what earlier builds left of the start's shared build time (#9795)
    /// and ran out of it.
    /// </summary>
    internal static string ShortenedBuildTimeoutMessage(string indexName, string table, double elapsedSeconds, int limitSeconds) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Index collect.{indexName} on {table} was not built at this start: the index builds of one start share {StartBuildBudgetSeconds} s, the builds before this one left it {limitSeconds} s, and it ran out of that after {elapsedSeconds:F0} s and was rolled back; the next service start tries it again");

    /// <summary>The warning for a bounded start-path index build (#9289) that timed out and was recorded.</summary>
    internal static string BoundedBuildTimeoutMessage(string indexName, string table, double elapsedSeconds, long heapPages, int limitSeconds) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Index collect.{indexName} on {table} was not built: the build ran out of time after {elapsedSeconds:F0} s (limit {limitSeconds} s) with {heapPages} uncompressed heap pages and was rolled back; the next start does not try it again for {PgIndexBuildAttempts.RetryAfter.TotalHours:F0} hours unless the table has fewer uncompressed pages by then, and the reader keeps its old shape until the index exists");

    /// <summary>What <see cref="BuildBoundAsync"/> decided: skip the build, or build it with the prior record (if any) and the current page count.</summary>
    private readonly record struct BuildBound(bool Skip, PgIndexBuildAttempts.Attempt? Prior, long? Pages);

    /// <summary>
    /// #9289: whether the bounded index's build is skipped this start. It is skipped while a recorded timeout is under 24 hours old and the
    /// table's uncompressed heap pages are at least the recorded count: the build would run out of time again and hold the start for the
    /// full timeout. A failed read answers "build", the behaviour before the bound, and returns no page count, so a timeout then records nothing.
    /// </summary>
    private static async Task<BuildBound> BuildBoundAsync(
        NpgsqlConnection connection, ILogger? logger, string indexName, string table, TimeProvider clock, CancellationToken cancellationToken)
    {
        try
        {
            var prior = await PgIndexBuildAttempts.ReadAsync(connection, indexName, EmptinessCheckTimeoutSeconds, cancellationToken);
            var pages = await PgIndexBuildAttempts.UncompressedHeapPagesAsync(connection, table, EmptinessCheckTimeoutSeconds, cancellationToken);
            var now = clock.GetUtcNow().UtcDateTime;
            if (prior is not null && PgIndexBuildAttempts.Bars(prior, pages, now))
            {
                logger?.LogWarning(
                    "Index collect.{Index} on {Table} is not built this start: its last build ran out of time after {ElapsedSeconds} s on {RecordedPages} uncompressed heap pages ({AttemptedAt:yyyy-MM-dd HH:mm:ss} UTC) and the table has {CurrentPages} now; the next try is at {RetryAt:yyyy-MM-dd HH:mm:ss} UTC, or earlier if the table has fewer uncompressed pages than that; the reader keeps its old shape until the index exists",
                    indexName, table, (prior.ElapsedMs / 1000.0).ToString("F0", CultureInfo.InvariantCulture), prior.HeapPages, prior.AttemptedAtUtc, pages,
                    prior.AttemptedAtUtc + PgIndexBuildAttempts.RetryAfter);
                return new BuildBound(true, prior, pages);
            }

            return new BuildBound(false, prior, pages);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not read the build record or the page count of {Table}, so {Index} is built as it was before the record existed: {Message}",
                table, indexName, ex.Message);
            return new BuildBound(false, null, null);
        }
    }

    /// <summary>Writes the record of a timed-out build. A failed write warns and the next start tries the build again, as before the record.</summary>
    private static async Task<bool> RecordBuildAttemptAsync(
        NpgsqlConnection connection, ILogger? logger, string indexName, PgIndexBuildAttempts.Attempt attempt, CancellationToken cancellationToken)
    {
        try
        {
            await PgIndexBuildAttempts.WriteAsync(connection, indexName, attempt, EmptinessCheckTimeoutSeconds, cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("Could not record the timed-out build of {Index}, so the next start tries it again: {Message}", indexName, ex.Message);
            return false;
        }
    }

    /// <summary>Deletes the record once the index is built. A failed delete warns; the index exists, so the stale record bars nothing.</summary>
    private static async Task ClearBuildAttemptAsync(NpgsqlConnection connection, ILogger? logger, string indexName, CancellationToken cancellationToken)
    {
        try
        {
            await PgIndexBuildAttempts.ClearAsync(connection, indexName, EmptinessCheckTimeoutSeconds, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("Could not delete the build record of {Index}: {Message}", indexName, ex.Message);
        }
    }

    /// <summary>Best-effort <c>ROLLBACK</c> that leaves the connection usable after a failed statement.</summary>
    private static async Task RollbackBestEffortAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            using var rollback = new NpgsqlCommand("ROLLBACK", connection) { CommandTimeout = DropLockTimeoutRollbackSeconds };
            await rollback.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception rollbackEx) when (rollbackEx is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Post-failure ROLLBACK also failed — the connection may be unusable for the rest of this sweep: {Message}",
                rollbackEx.Message);
        }
    }

    /// <summary>The reloptions every pure-insert hypertable gets. Shared with the literal statements above so
    /// the two spellings cannot drift apart.</summary>
    public const string InsertTuningOptions =
        "autovacuum_vacuum_insert_scale_factor = 0.02, autovacuum_vacuum_insert_threshold = 10000";

    /// <summary>
    /// Every hypertable that does not already carry the insert tuning, resolved from the TimescaleDB catalog.
    /// The name is built with <c>format('%I.%I')</c> so it comes back correctly quoted.
    /// </summary>
    public const string UntunedHypertablesSql = @"
SELECT format('%I.%I', h.hypertable_schema, h.hypertable_name)
FROM timescaledb_information.hypertables h
JOIN pg_class c ON c.relname = h.hypertable_name
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = h.hypertable_schema
WHERE COALESCE(array_to_string(c.reloptions, ','), '') NOT LIKE '%insert_scale_factor%'";

    /// <summary>
    /// Applies the insert tuning to every hypertable still missing it, DERIVED from the catalog rather than
    /// from the hand-written list above (#2405).
    ///
    /// <para>The literal statements exist because a plain-PostgreSQL store has no TimescaleDB catalog to read,
    /// and those four tables are the ones whose read paths were measured; this sweep is what makes the property
    /// hold for the rest. It matters because "pure-insert append-only fact table" describes EVERY collector
    /// target by construction, while the literal list described whichever four had been the subject of an
    /// EXPLAIN investigation — 4 of 51 hypertables on the dogfood store, with untuned tables (perfmon_stats at
    /// 5.3 GB, spinlock_stats at 3.7 GB, wait_stats at 2.8 GB) larger than tuned ones. A list that has to be
    /// remembered when a collector lands is a list that gets missed, and this one already had been: the file's
    /// own comment records pg_statement_stats being added late for exactly that reason.</para>
    ///
    /// <para>Deliberately scoped to HYPERTABLES, not to everything in <c>collect</c>. Insert-driven autovacuum
    /// governs the visibility map and freezing on append-only data; a plain table whose churn is DELETEs needs
    /// <c>autovacuum_vacuum_scale_factor</c> instead, which is a different knob for a different failure and is
    /// set explicitly where it applies. Sweeping every table here would silently apply the inert one.</para>
    ///
    /// <para>The 10,000-row threshold is what keeps this safe on the many small hypertables: the scale factor is
    /// not consulted until that many inserts have accumulated, so a low-rate table is not pushed into
    /// constant autovacuum. Per-table failure isolation matches <see cref="ApplyAsync"/>, and a store without
    /// TimescaleDB simply finds no catalog and no-ops.</para>
    /// </summary>
    public static async Task<int> ApplyHypertableInsertTuningAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var targets = new List<string>();
        try
        {
            using var query = new NpgsqlCommand(UntunedHypertablesSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                targets.Add(reader.GetString(0));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* No TimescaleDB (a plain-PostgreSQL store) or an unreadable catalog — the literal statements above
               already ran, so this is a no-op rather than a failure. */
            logger?.LogDebug("Hypertable insert-tuning sweep skipped: {Message}", ex.Message);
            return 0;
        }

        if (targets.Count == 0)
        {
            return 0;
        }

        var applied = 0;
        foreach (var target in targets)
        {
            /* Identifier comes from the catalog already quoted by format('%I.%I') — never user input. */
            var statement = $"{AlterTablePrefix}{target} SET ({InsertTuningOptions})";
            try
            {
                using var command = new NpgsqlCommand(GuardedAlter(statement), connection) { CommandTimeout = SetupTimeoutSeconds };
                await command.ExecuteNonQueryAsync(cancellationToken);
                applied++;
            }
            catch (PostgresException lockBusy) when (lockBusy.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                /* #6539: busy table (an ACCESS EXCLUSIVE holder): skip it; the setting is idempotent, so the next pass applies it. */
                logger?.LogInformation(
                    "Insert-autovacuum tuning not applied to {Table} this pass: the table was busy; the next pass tries again",
                    target);
                await RollbackBestEffortAsync(connection, logger, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Insert-autovacuum tuning failed for {Table} — the store keeps working without it: {Message}",
                    target, ex.Message);
                await RollbackBestEffortAsync(connection, logger, cancellationToken);
            }
        }

        logger?.LogInformation(
            "Insert-autovacuum tuning applied to {Applied}/{Total} previously-untuned hypertable(s)",
            applied, targets.Count);
        return applied;
    }
}
