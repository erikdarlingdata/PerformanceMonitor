/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's half of #1912: repair the Query Store rows collected before #1907, in the hot DuckDB table AND in
/// the parquet archive that unions into the read views.
///
/// <para>Before #1907 the collector stored Query Store's FLUSHED and still-IN-MEMORY slice of one interval as
/// two rows. They are ADDITIVE, so the read-side dedup — which keeps one row per key — reports a fraction of
/// the interval's work. #1907 made that choice deterministic; this makes it correct.</para>
///
/// <para><b>Lite gets a FULL repair where Darling gets a bounded one.</b> Darling can only reach rows still in
/// its 4-day raw tier, because its long tier is a materialized rollup that cannot be rebuilt from raw that no
/// longer exists. Lite's long tier IS the parquet archive — the same rows, rewritable — so the repair reaches
/// the whole history. Measured on a real archive: the Query Store files totalled ~46 MB across three monthly
/// parquets, 6.6–8.5% of their rows carried the split signature, and the largest (28 MB / 533,109 rows)
/// rewrote in under a second.</para>
///
/// <para><b>Automatic, once, on the first launch after upgrading</b> — unlike Darling's
/// <c>--collapse-legacy-slices</c> verb, and the asymmetry is deliberate. What the two apps must share is the
/// SEMANTICS of the fix — the same collapse, the same weighted math, the same disclosure — not the way it is
/// invoked; each follows its own precedent, and Lite's is the automatic startup store migration that v39 and
/// #1832's data-root move already established. A button was considered and rejected: a repair gated behind UI
/// discovery leaves the users least equipped to find it holding wrong numbers permanently. See
/// <see cref="RepairOnStartupAsync"/> for the once-only marker and the retry-on-partial behavior.</para>
///
/// <para><b>What cancellation means here (#2465).</b> Every entry point takes a token, and every one of them
/// takes the DATABASE LOCK BEFORE it opens its connection — so a token that does not reach
/// <c>AcquireReadLock</c> stops at the door, which is the hole #2454 found on the analysis pass and the one
/// the analyzer surfaced here the moment that overload existed. The five acquisitions are split by what
/// abandoning COSTS, not by whether the site reads or writes. The marker read, the survey and the archive
/// rewrite-to-temp FORWARD it: each abandons into a state the next launch reproduces for free — a question
/// re-asked, a survey re-run, an original file left untouched by construction. The two marker WRITES take
/// <see cref="CancellationToken.None"/>: they record work that is already done and cannot be undone, and
/// dropping the record does not undo it either — it only makes the next launch pay the whole survey again to
/// rediscover there is nothing left to do. Every site states its own choice at the site.</para>
///
/// <para>The two <c>AcquireWriteLock</c> acquisitions are deliberately untouched by that, and not merely
/// because CA2016 cannot see them (there is no token-taking overload). They are the genuinely
/// maintenance-shaped acquisitions — the hot-table mutation with its CHECKPOINT, and the instant a live
/// path's bytes are replaced — which is the one category #2463 does not put in question.</para>
/// </summary>
public sealed partial class QueryStoreSliceRepairService
{
    private const string Table = "query_store_stats";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private readonly ILogger<QueryStoreSliceRepairService>? _logger;

    public QueryStoreSliceRepairService(DuckDbInitializer duckDb, string archivePath, ILogger<QueryStoreSliceRepairService>? logger = null)
    {
        _duckDb = duckDb ?? throw new ArgumentNullException(nameof(duckDb));
        _archivePath = archivePath ?? throw new ArgumentNullException(nameof(archivePath));
        _logger = logger;
    }

    /// <summary>
    /// What a repair did. <paramref name="Failures"/> is per FILE, and a non-empty list is not an exception:
    /// each file's original survives its own failure and the next startup retries it, so the honest report is
    /// "these were repaired, these were not, here is why" rather than one thrown error that hides the rest.
    /// </summary>
    public sealed record RepairResult(long RowsRemoved, IReadOnlyList<string> Failures)
    {
        public bool FullyRepaired => Failures.Count == 0;
    }

    /// <summary>What a survey found, in the hot store and across the archive.</summary>
    public sealed record Survey(long HotGroups, long HotRows, IReadOnlyList<ArchiveFileSurvey> Archive, IReadOnlyList<string> Unreadable)
    {
        public long HotRowsRemoved => HotRows - HotGroups;

        public long ArchiveRowsRemoved => Archive.Sum(a => a.RowsRemoved);

        public bool HasWork => HotGroups > 0 || Archive.Any(a => a.Groups > 0);
    }

    /// <summary>
    /// One archive file's share of the work. <paramref name="Executions"/> is the file's total
    /// <c>SUM(execution_count)</c> BEFORE the rewrite — the conservation baseline, since the collapse sums the
    /// counter within a group and therefore cannot change the file's total.
    /// </summary>
    public sealed record ArchiveFileSurvey(string Path, long Rows, long Groups, long SplitRows, long Executions, bool HasIntervalId)
    {
        public long RowsRemoved => SplitRows - Groups;
    }

    /// <summary>
    /// The completion marker. Deliberately its OWN table rather than a schema-version bump.
    ///
    /// <para>Two reasons, both load-bearing. <c>RunMigrationsAsync</c> DROPS AND RECREATES tables per version
    /// step, so a data repair has no business living on that path — a future migration that drops
    /// <c>query_store_stats</c> would destroy the rows this exists to fix. And the requirement is that a
    /// partial repair RETRIES on the next launch, which means the marker must be withheld on failure; the
    /// schema version cannot be withheld, because leaving it behind would re-run the table-dropping schema
    /// migrations too.</para>
    ///
    /// <para>So the marker is written only on a FULLY successful pass. A store that could not repair one
    /// archive file tries again next launch, which is safe because the pre-fix signature makes the whole
    /// thing idempotent — the files that already succeeded present no work the second time.</para>
    /// </summary>
    internal const string MarkerTable = "query_store_slice_repair";

    /// <summary>
    /// True when a completed repair has already been recorded for this store.
    ///
    /// <para>#2465 forwards the token to the LOCK as well as to the reads. This method asks a question and
    /// writes nothing, so abandoning it costs nothing at all — the next launch asks it again — and there is
    /// no reason for a caller holding a fired token to sit in an uninterruptible <c>EnterReadLock()</c>
    /// behind an archival to learn an answer it has stopped wanting.</para>
    /// </summary>
    public async Task<bool> AlreadyRepairedAsync(CancellationToken cancellationToken = default)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{MarkerTable}'";
        var exists = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken) ?? 0L, CultureInfo.InvariantCulture) > 0;
        if (!exists)
        {
            return false;
        }

        using var rows = connection.CreateCommand();
        rows.CommandText = $"SELECT COUNT(*) FROM {MarkerTable}";
        return Convert.ToInt64(await rows.ExecuteScalarAsync(cancellationToken) ?? 0L, CultureInfo.InvariantCulture) > 0;
    }

    private async Task MarkRepairedAsync(DuckDBConnection connection, long rowsRemoved, CancellationToken cancellationToken)
    {
        using var create = connection.CreateCommand();
        create.CommandText = $"CREATE TABLE IF NOT EXISTS {MarkerTable} (completed_at TIMESTAMP NOT NULL, rows_removed BIGINT NOT NULL)";
        await create.ExecuteNonQueryAsync(cancellationToken);

        using var insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {MarkerTable} (completed_at, rows_removed) VALUES (CURRENT_TIMESTAMP, {rowsRemoved})";
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Attempts recorded BEFORE the repair runs, so a repair that never returns still leaves a trace (#2748).
    ///
    /// <para><b>Why a separate table from <see cref="MarkerTable"/>, and why written first.</b> The completion
    /// marker answers "did this finish"; this one answers "did we try". The distinction only matters in the one
    /// case that brought it into being, and in that case it is the whole ballgame: on 2026-09-02 a reporter's
    /// store took <c>duckdb.dll</c> down with a native fast-fail (<c>0xc0000409</c>, subcode 7 —
    /// <c>FAST_FAIL_FATAL_APP_EXIT</c>, the native library aborting) partway through this repair, on a store
    /// with 31,426 split intervals. A native abort is not a managed exception: the <c>catch</c> in
    /// <see cref="RepairOnStartupAsync"/> never runs, the process is gone mid-statement, and nothing is
    /// recorded. So the next launch surveyed the same store, found the same work, ran the same repair and died
    /// the same way — a permanent, self-perpetuating crash loop with the app never reaching a usable state.
    /// Recording the attempt first is the only thing that survives that, precisely because it is committed
    /// before the dangerous work begins.</para>
    /// </summary>
    internal const string AttemptTable = "query_store_slice_repair_attempts";

    /// <summary>
    /// How many times a startup repair may be attempted before this store stops trying on startup.
    ///
    /// <para>Two, not one: a single failure is genuinely often transient — a file locked by a backup agent, a
    /// machine suspended mid-pass — and the existing retry-on-partial behavior is load-bearing for exactly
    /// those. What must not survive is the THIRD identical launch, because by then the evidence is that this
    /// store reproduces the failure deterministically and every further attempt just denies the user their
    /// app.</para>
    /// </summary>
    internal const int MaxStartupAttempts = 2;

    /// <summary>
    /// Attempts recorded so far. Zero when the table does not exist, which is every store that has not yet run
    /// a build carrying #2748's fix.
    /// </summary>
    internal async Task<long> AttemptCountAsync(CancellationToken cancellationToken = default) =>
        (await ReadAttemptsAsync(cancellationToken)).Count;

    /// <summary>
    /// The attempt count AND the shape the last attempt recorded, in ONE acquisition.
    ///
    /// <para>The shape is carried here so the capped path never has to survey. Reading it alongside the count
    /// rather than in its own method is deliberate: a separate call would be a second uncancelable-lock site
    /// for #2761 to weigh, for two numbers that are written together and read together.</para>
    /// </summary>
    internal async Task<AttemptState> ReadAttemptsAsync(CancellationToken cancellationToken = default)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        using var exists = connection.CreateCommand();
        exists.CommandText =
            $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{AttemptTable}'";
        if (Convert.ToInt64(await exists.ExecuteScalarAsync(cancellationToken) ?? 0L, CultureInfo.InvariantCulture) == 0)
        {
            return new AttemptState(0, 0, 0, 0);
        }

        using var rows = connection.CreateCommand();
        /* The LAST attempt's row, not MAX() across all of them. Those diverge the moment two attempts see
           different work, which is the ordinary partial-repair case rather than an exotic one: attempt 1
           collapses the hot table and commits, an archive file then fails, the completion marker is withheld,
           and attempt 2's survey correctly finds zero hot groups left. Independent MAX()es would go on
           reporting attempt 1's hot count and tell the user an already-clean hot store still has work
           outstanding - the same misreporting caught earlier for the hot/archive split, reintroduced across
           attempts instead of within one. Ordered by attempted_at then rowid, so attempts landing inside the
           same timestamp still resolve to insertion order. */
        rows.CommandText = $@"
SELECT
    (SELECT COUNT(*) FROM {AttemptTable}),
    COALESCE((SELECT hot_groups FROM {AttemptTable} ORDER BY attempted_at DESC, rowid DESC LIMIT 1), 0),
    COALESCE((SELECT archive_files FROM {AttemptTable} ORDER BY attempted_at DESC, rowid DESC LIMIT 1), 0),
    COALESCE((SELECT unreadable_files FROM {AttemptTable} ORDER BY attempted_at DESC, rowid DESC LIMIT 1), 0)";
        using var reader = await rows.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new AttemptState(0, 0, 0, 0);
        }

        return new AttemptState(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
    }

    /// <summary>What the attempt table knows: how many attempts, and how much work the last one saw.</summary>
    internal readonly record struct AttemptState(long Count, long HotGroups, long ArchiveFiles, long UnreadableFiles);

    /// <summary>
    /// Records an attempt marker and COMMITS it before the caller does anything dangerous.
    ///
    /// <para>#2748: <see cref="CancellationToken.None"/> throughout, for the reason the completion marker uses
    /// it and one that is sharper here. This is bookkeeping whose entire value is that it is already durable
    /// when the next thing goes wrong. Abandon this write and no attempt is recorded, so a store that then
    /// takes <c>duckdb.dll</c> down natively comes back on the next launch with its count unchanged — the exact
    /// infinite crash loop this marker exists to end. Forwarding the token would make the protection
    /// abandonable at the one moment it is about to be needed.</para>
    ///
    /// <para>Noted for #2761, which has this service's uncancelable lock under review: this is not that shape.
    /// It is two statements against a table holding at most a handful of rows, not the survey's full GROUP BY
    /// over the hot store, so the window that cannot be abandoned is bounded and short.</para>
    ///
    /// <para><b>That the commit alone survives the abort is MEASURED, not assumed.</b> The whole gate rests on
    /// it: if the marker did not come back, the count would never reach the cap and the crash loop would
    /// continue with a fix that merely looks like one. Tested against DuckDB 1.5.5 by writing the marker
    /// exactly as this method does and then killing the process without any unwinding — once via
    /// <c>Environment.FailFast</c> (SIGABRT, exit 134, no finalizers, no Dispose) and once via an external
    /// SIGKILL, the latter being uninterceptable. In both cases a fresh process read the marker back and
    /// <see cref="AttemptCountAsync"/> returned 1; the abort visibly leaves a populated <c>.wal</c> beside the
    /// store, which is replayed on the next open. Simulating the reporter's loop end to end then went
    /// 0 → 1 → 2, and the third launch skipped the repair and started, which is the escape this exists for.</para>
    ///
    /// <para><b>The commit is the durability, not a CHECKPOINT.</b> This wants to survive the process being
    /// killed mid-repair without unwinding, and a committed DuckDB transaction already does: the WAL is written
    /// and flushed at commit, and it replays when the store is next opened. The failure being defended against
    /// is a native <c>abort()</c> — the process dies, the machine does not — so anything the OS has taken is
    /// safe. An explicit CHECKPOINT would buy nothing here and cost something real: CHECKPOINT is MAINTENANCE
    /// under this store's locking model (it reorganizes the database file, and a reader mid-query gets "Reached
    /// the end of the file"), so it belongs under the WRITE lock — and this service is fired un-awaited at
    /// startup precisely so the UI stays interactive and reading. Taking the write lock for two rows of
    /// bookkeeping would stall those readers for no durability gain.</para>
    /// </summary>
    private async Task RecordAttemptAsync(long hotGroups, long archiveFiles, long unreadableFiles)
    {
        /* #2748 DECLINES the token: this attempt marker is NOT abandonable, deliberately. Abandon the write
           and no attempt is recorded, so a store that then aborts natively comes back with its count
           unchanged — the crash loop this marker exists to end, defeated at the one moment it is needed.

           Stated plainly for #2761, which is about this service holding uncancelable locks: this is a SECOND
           such site, so that issue now has two, not one. It is the small one — two statements against a table
           of a handful of rows — where #2761's is the survey's full GROUP BY over the hot store plus a
           read_parquet of every archive file. Bounded and short is not the same as free, and whoever picks
           #2761 up should know this exists rather than discover it. */
        using var readLock = _duckDb.AcquireReadLock(CancellationToken.None);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(CancellationToken.None);

        using var create = connection.CreateCommand();
        create.CommandText =
            $"CREATE TABLE IF NOT EXISTS {AttemptTable} (attempted_at TIMESTAMP NOT NULL, hot_groups BIGINT NOT NULL, archive_files BIGINT NOT NULL, unreadable_files BIGINT NOT NULL)";
        await create.ExecuteNonQueryAsync(CancellationToken.None);

        using var insert = connection.CreateCommand();
        insert.CommandText =
            $"INSERT INTO {AttemptTable} (attempted_at, hot_groups, archive_files, unreadable_files) VALUES (CURRENT_TIMESTAMP, {hotGroups}, {archiveFiles}, {unreadableFiles})";
        await insert.ExecuteNonQueryAsync(CancellationToken.None);
    }

    /// <summary>
    /// The startup entry point: repair once, automatically, and record it — or leave it to be retried.
    ///
    /// <para><b>Automatic rather than operator-invoked, unlike Darling's verb, and that asymmetry is
    /// deliberate.</b> Consistency between the two apps is the same SEMANTICS for the same defect — the same
    /// collapse, the same weighted math, the same disclosure — not the same invocation surface. Each follows
    /// its own precedent: Darling's is #1849's operator verb, Lite's is the automatic startup store migration
    /// that v39 and #1832's data-root move already established. The decisive argument against a button is that
    /// a repair gated behind UI discovery leaves the users least equipped to find it holding wrong numbers
    /// permanently.</para>
    ///
    /// <para>Never throws. A store that cannot be repaired must still START — the app is a monitoring tool and
    /// refusing to launch over historical Query Store numbers would be a far worse failure than the one being
    /// fixed. Problems are logged and the marker is withheld so the next launch tries again.</para>
    /// </summary>
    public async Task RepairOnStartupAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (await AlreadyRepairedAsync(cancellationToken))
            {
                return;
            }

            /* #2748 review: the cap is checked BEFORE the survey, and that ordering is the point.
               SurveyAsync is the expensive thing this service has — a full GROUP BY over the hot table plus a
               read_parquet of every monthly archive file. Checking the cap after it would mean a permanently
               capped store paid that cost on every launch forever, purely to produce the message below, which
               flatly contradicts the "the app starts and collects normally" this fix promises. Reading the
               attempt table is a COUNT over a handful of rows, so a capped store now short-circuits on
               something cheap. The shape quoted in the message is what the last attempt RECORDED rather than
               what a fresh survey would find, which is the same number that survey would have cost us. */
            var attempts = await ReadAttemptsAsync(cancellationToken);
            if (attempts.Count >= MaxStartupAttempts)
            {
                /* Report BOTH halves of the outstanding work. Naming only the hot groups would read as "0 left
                   undone" on a store whose hot table is already clean but whose archive files are not — the
                   repair reaches both, and the archive half is the half a partial pass leaves behind, which is
                   exactly the state a capped store is most likely to be in. */
                _logger?.LogError(
                    "#1912 Query Store repair SKIPPED: {Attempts} previous attempt(s) on this store did not complete, " +
                    "so it will not be retried automatically — see issue #2748. The app starts normally and collects " +
                    "normally; what is left undone is the one-time collapse of {HotGroups} pre-#1907 split interval(s) " +
                    "in the hot store, {ArchiveFiles} archive file(s), and {UnreadableFiles} unreadable archive file(s) " +
                    "as of the last attempt, and #1907's read-side tie-break still resolves those rows " +
                    "deterministically. To retry after upgrading, drop the '{Table}' table from the store.",
                    attempts.Count,
                    attempts.HotGroups,
                    attempts.ArchiveFiles,
                    attempts.UnreadableFiles,
                    AttemptTable);
                return;
            }

            var survey = await SurveyAsync(cancellationToken);
            if (!survey.HasWork && survey.Unreadable.Count == 0)
            {
                /* Nothing to do — a fresh store, or one that only ever ran fixed builds. Record it so the
                   survey does not run on every launch forever.

                   #2465: CancellationToken.None, and all the way through — the lock, the open and both
                   statements. The survey immediately above ran under the token and RETURNED, so the token is
                   known unfired a moment ago and forwarding here would buy an abandonment window microseconds
                   wide. What that window would cost is the sentence directly above it: drop the marker and the
                   survey runs again on the next launch, and the survey is the expensive thing this service has
                   — a full GROUP BY over the hot table plus a read_parquet of every monthly archive file.
                   None reaches the statements too rather than stopping at the lock, because "this write
                   completes once started" contradicted three lines later would be worse than either choice
                   made whole. */
                using var readLock = _duckDb.AcquireReadLock(CancellationToken.None);
                using var connection = _duckDb.CreateConnection();
                await connection.OpenAsync(CancellationToken.None);
                await MarkRepairedAsync(connection, 0, CancellationToken.None);
                return;
            }

            var archiveFiles = survey.Archive.Count(a => a.Groups > 0);

            _logger?.LogInformation(
                "#1912 one-time Query Store repair starting (attempt {Attempt} of {Max}): {HotGroups} split interval(s) in the hot store, {ArchiveFiles} archive file(s) affected",
                attempts.Count + 1,
                MaxStartupAttempts,
                survey.HotGroups,
                archiveFiles);

            await RecordAttemptAsync(survey.HotGroups, archiveFiles, survey.Unreadable.Count);

            var result = await RepairAsync(
                new Progress<string>(message => _logger?.LogInformation("#1912 {Message}", message)),
                cancellationToken);

            if (result.FullyRepaired)
            {
                /* #2465: CancellationToken.None, for the reason above and one more that is only true here.
                   The repair is DONE — the hot table is collapsed and committed, every affected archive file
                   has been rewritten and swapped — and none of that is undone by abandoning its record.
                   Dropping the marker does not stop a repair; it only makes the next launch pay the full
                   survey to rediscover that there is nothing left to repair. That makes this the one place in
                   the file where abandoning loses something the next launch does not get back for free, and
                   what it is protecting is two statements and one row. */
                using var readLock = _duckDb.AcquireReadLock(CancellationToken.None);
                using var connection = _duckDb.CreateConnection();
                await connection.OpenAsync(CancellationToken.None);
                await MarkRepairedAsync(connection, result.RowsRemoved, CancellationToken.None);

                _logger?.LogInformation("#1912 one-time Query Store repair complete: {Rows} row(s) collapsed", result.RowsRemoved);
            }
            else
            {
                /* Marker deliberately NOT written: the next launch retries the files that failed. Until then
                   the union_by_name view still reads them, with #1907's read-side tie-break resolving their
                   split slices deterministically. */
                _logger?.LogWarning(
                    "#1912 Query Store repair finished with {Count} file(s) unrepaired; they will be retried on the next start. {Failures}",
                    result.Failures.Count,
                    string.Join(" | ", result.Failures));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogError(ex, "#1912 Query Store repair could not run; it will be retried on the next start");
        }
    }

    /// <summary>
    /// Counts the split slices waiting in the hot table and in every archive file. Reads only; writes nothing.
    ///
    /// <para>#2465 forwards the token to the LOCK. This is the longest read the service has — a full GROUP BY
    /// over <c>query_store_stats</c> and then a <c>read_parquet</c> of every monthly archive file — and the
    /// lock is taken before any of it starts, so a survey queued behind an archival is precisely the pass that
    /// is "abandonable everywhere except where it is actually stuck". Abandoning costs the survey and nothing
    /// else: the marker is withheld, and the next launch redoes it.</para>
    /// </summary>
    public async Task<Survey> SurveyAsync(CancellationToken cancellationToken = default)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var hotColumns = await ColumnsOfTableAsync(connection, Table, cancellationToken);
        var hotKey = KeyColumnsFor(hotColumns);

        long groups = 0;
        long rows = 0;
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
SELECT
    COUNT(*) AS groups,
    CAST(COALESCE(SUM(c), 0) AS BIGINT) AS rows_in_groups
FROM (SELECT COUNT(*) AS c FROM {Table} GROUP BY {string.Join(", ", hotKey)} HAVING COUNT(*) > 1)";
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                groups = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
                rows = Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
            }
        }

        var archive = new List<ArchiveFileSurvey>();
        var unreadable = new List<string>();
        foreach (var file in ArchiveFiles())
        {
            /* Per-file isolation in the SURVEY too, not only in the rewrite. An archive file that cannot even
               be inspected — a shape the collapse cannot express, a truncated file — must not stop the others
               being surveyed and repaired. It is reported and skipped, and its original is untouched by
               definition because nothing was written. */
            try
            {
                archive.Add(await SurveyArchiveFileAsync(connection, file, cancellationToken));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                unreadable.Add($"{Path.GetFileName(file)}: {ex.Message}");
                _logger?.LogWarning(ex, "#1912 could not survey archive file {File}; skipped and left untouched", file);
            }
        }

        return new Survey(groups, rows, archive, unreadable);
    }

    /// <summary>The archived Query Store parquet files, oldest name first.</summary>
    private IEnumerable<string> ArchiveFiles()
        => Directory.Exists(_archivePath)
            ? Directory.GetFiles(_archivePath, $"*_{Table}.parquet").OrderBy(f => f, StringComparer.Ordinal)
            : [];

    private static async Task<ArchiveFileSurvey> SurveyArchiveFileAsync(
        DuckDBConnection connection, string file, CancellationToken cancellationToken)
    {
        var columns = await ColumnsOfParquetAsync(connection, file, cancellationToken);
        var key = KeyColumnsFor(columns);

        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT
    (SELECT COUNT(*) FROM read_parquet('{EscapePath(file)}')) AS total_rows,
    COUNT(*) AS groups,
    CAST(COALESCE(SUM(c), 0) AS BIGINT) AS rows_in_groups,
    (SELECT CAST(COALESCE(SUM(execution_count), 0) AS BIGINT) FROM read_parquet('{EscapePath(file)}')) AS executions
FROM (SELECT COUNT(*) AS c FROM read_parquet('{EscapePath(file)}') GROUP BY {string.Join(", ", key)} HAVING COUNT(*) > 1)";

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new ArchiveFileSurvey(file, 0, 0, 0, 0, key.Contains("runtime_stats_interval_id"));
        }

        return new ArchiveFileSurvey(
            file,
            Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
            key.Contains("runtime_stats_interval_id"));
    }

    /// <summary>
    /// Collapses the hot table and every archive file that carries split slices. Returns rows removed.
    ///
    /// <para>Each archive file is rewritten to a temp sibling and swapped in only after the rewrite succeeded
    /// AND its row count checks out, so an interruption leaves the original in place rather than a truncated
    /// archive. The COPY carries <c>COMPRESSION ZSTD</c> because that is what wrote these files — omitting it
    /// silently switches codec and inflates them (measured: 28 MB becoming 69 MB).</para>
    /// </summary>
    public async Task<RepairResult> RepairAsync(IProgress<string>? progress = null, CancellationToken cancellationToken = default)
    {
        var survey = await SurveyAsync(cancellationToken);
        long removed = 0;
        var failures = new List<string>(survey.Unreadable);

        if (survey.HotGroups > 0)
        {
            /* WRITE LOCK — the collapse MUTATES table data, and the CHECKPOINT that follows reorganizes the
               database file. A reader mid-query when that happens gets "Reached the end of the file". The read
               lock permits concurrent readers by design, and this service is fired un-awaited at startup
               precisely so the UI stays interactive, so the UI genuinely can be reading while this runs. Same
               idiom, and the same reasoning, as ArchiveService's purge. Held only around the mutation, not
               around the survey or the archive rewrites. */
            using var writeLock = _duckDb.AcquireWriteLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            var hotColumns = await ColumnsOfTableAsync(connection, Table, cancellationToken);
            var key = KeyColumnsFor(hotColumns);
            var projection = BuildProjection(hotColumns, key);

            using var transaction = connection.BeginTransaction();

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = $@"
CREATE OR REPLACE TEMP TABLE qs_slice_repair AS
SELECT {projection}
FROM {Table}
GROUP BY {string.Join(", ", key)}
HAVING COUNT(*) > 1";
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            /* DELETE by the key, then re-insert the combined rows. IS NOT DISTINCT FROM because the key
               carries NULLABLE columns — runtime_stats_interval_id is NULL on every row collected before
               #1841 tier 2 — and = against NULL is NULL, which would skip exactly the oldest rows. */
            var match = string.Join(" AND ", key.Select(k => $"t.{k} IS NOT DISTINCT FROM r.{k}"));
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = $"DELETE FROM {Table} AS t WHERE EXISTS (SELECT 1 FROM qs_slice_repair AS r WHERE {match})";
                removed += Convert.ToInt64(await command.ExecuteNonQueryAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = $"INSERT INTO {Table} SELECT * FROM qs_slice_repair";
                removed -= Convert.ToInt64(await command.ExecuteNonQueryAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            transaction.Commit();
            progress?.Report($"Hot store: {survey.HotRowsRemoved:N0} row(s) removed from {survey.HotGroups:N0} split interval(s).");
        }

        /* PER-FILE ISOLATION. One archive file that cannot be repaired must not stop the others: each file is
           an independent unit, its original survives its own failure, and the next startup retries it. Letting
           the first failure abort the run would leave every LATER file un-repaired too — and since files are
           processed in name order, that means one bad old file could permanently block every newer one. */
        var rewroteArchive = false;
        foreach (var file in survey.Archive.Where(a => a.Groups > 0))
        {
            try
            {
                removed += await RewriteArchiveFileAsync(file, progress, cancellationToken);
                rewroteArchive = true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failures.Add($"{Path.GetFileName(file.Path)}: {ex.Message}");
                _logger?.LogError(ex, "#1912 archive repair failed for {File}; the original was left in place and will be retried", file.Path);
                progress?.Report($"Archive {Path.GetFileName(file.Path)}: NOT repaired — {ex.Message}");
            }
        }

        if (rewroteArchive)
        {
            /* The cache eviction and the view rebuild are NOT done here — PromoteRewrittenFileAsync does both
               as part of the swap itself, so a rewrite cannot be correct-only-if-the-caller-remembers. This is
               just the operator-facing line. */
            progress?.Report("Archive views rebuilt so readers pick up the repaired files.");
        }

        return new RepairResult(removed, failures);
    }

    /// <summary>
    /// Rewrites one archive file and promotes it.
    ///
    /// <para><b>The two phases take DIFFERENT locks, per file.</b> Reading the original and writing the temp
    /// sibling touches nothing any reader can see, so it runs under the READ lock and collection carries on.
    /// Only the promotion — where the bytes behind a live path are replaced — needs exclusivity, and it takes
    /// the WRITE lock for that instant alone. Per file rather than once around the whole run: a store with a
    /// large archive would otherwise freeze the UI for the entire repair when only the swap instants require
    /// it, and the swaps are the fast part.</para>
    /// </summary>
    private async Task<long> RewriteArchiveFileAsync(
        ArchiveFileSurvey file, IProgress<string>? progress, CancellationToken cancellationToken)
    {
        var temp = file.Path + ".repair-tmp";
        var beforeBytes = new FileInfo(file.Path).Length;
        long rewrittenRows;
        long rewrittenExecutions;

        /* READ LOCK: everything up to and including verification. The original is only read; the only thing
           written is the temp sibling, which nothing else knows about yet.

           #2465 forwards the token, to the lock as well as to the reads. This phase is the expensive one — a
           COPY across a whole monthly parquet — and abandoning it lands in a state the design already
           handles rather than a new one: the original is untouched by construction, the marker is withheld,
           and the next launch retries a repair the pre-fix signature makes idempotent. A cancelled COPY
           strands the same temp sibling a FAILED one does, and the retry's COPY overwrites it; the archive
           glob does not match the .repair-tmp suffix, so a stranded one is never read as an archive file. */
        using (_duckDb.AcquireReadLock(cancellationToken))
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            var columns = await ColumnsOfParquetAsync(connection, file.Path, cancellationToken);
            var key = KeyColumnsFor(columns);
            var projection = BuildProjection(columns, key);

            await ArchiveService.WithRaisedCopyMemoryLimit(connection, async () =>
            {
                using var command = connection.CreateCommand();
                command.CommandText = $@"
COPY (
    SELECT {projection}
    FROM read_parquet('{EscapePath(file.Path)}')
    GROUP BY {string.Join(", ", key)}
) TO '{EscapePath(temp)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
                await command.ExecuteNonQueryAsync(cancellationToken);
            });

            (rewrittenRows, rewrittenExecutions) = await MeasureRewrittenAsync(connection, temp, cancellationToken);
        }

        /* VERIFY BEFORE PROMOTING — this is the safety, since no backup copy is kept (the v39 migration kept
           none either). Two CONSERVATION invariants, not a smoke test, and they are the ones the collapse's own
           arithmetic guarantees:

             - Row count must land on exactly (rows - rowsRemoved): every split group becomes one row and
               nothing else moves, so any other number means the GROUP BY grouped something it should not have
               — which is precisely the damage an era-wrong key would do.
             - SUM(execution_count) must be UNCHANGED. The collapse sums the counter within a group, so the
               file's total is invariant across the rewrite. This is the check that would catch a mis-typed
               aggregate that still produced the right row count, and it is the number the whole defect is
               about.

           A failure deletes the temp file and leaves the ORIGINAL in place. Startup logs it loudly and the
           next launch retries: the pre-fix signature makes the repair idempotent, and until it succeeds the
           union_by_name view keeps reading the un-rewritten file with the #1907 read-side tie-break still
           resolving it deterministically. A half-repaired archive is therefore a delay, never a corruption. */
        var expectedRows = file.Rows - file.RowsRemoved;
        var name = Path.GetFileName(file.Path);

        if (rewrittenRows != expectedRows || rewrittenExecutions != file.Executions)
        {
            File.Delete(temp);
            throw new InvalidOperationException(
                $"Archive repair of {name} did not conserve its contents — " +
                $"{rewrittenRows:N0} rows (expected {expectedRows:N0}) and {rewrittenExecutions:N0} executions " +
                $"(expected {file.Executions:N0}). The original was left untouched and will be retried.");
        }

        /* WRITE LOCK, for the swap instant only — this is where a live path's bytes are replaced, and a reader
           mid-read_parquet on it would otherwise fail. */
        using (_duckDb.AcquireWriteLock())
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            await PromoteRewrittenFileAsync(connection, file.Path, temp, cancellationToken);
        }

        var afterBytes = new FileInfo(file.Path).Length;

        progress?.Report($"Archive {name}: {file.RowsRemoved:N0} row(s) removed ({FormatBytes(beforeBytes)} -> {FormatBytes(afterBytes)}).");

        /* The size can GROW despite the repair removing rows, and an operator watching their archive get
           bigger after a row-removing repair deserves the reason rather than a support ticket: parquet size is
           dominated by per-column compression of row groups, and the rewrite re-lays those out. Measured on a
           real archive file, 22,760 rows removed and the file still went 28.0 MB to 31.6 MB. */
        if (afterBytes > beforeBytes)
        {
            progress?.Report(
                $"  ({name} grew despite losing rows — parquet size follows row-group layout and per-column " +
                "compression, not row count. The data is correct and smaller; its encoding is simply less lucky.)");
        }

        _logger?.LogInformation(
            "#1912 archive repair: {File} {Removed} rows removed, {Before} -> {After} bytes",
            name, file.RowsRemoved, beforeBytes, afterBytes);

        return file.RowsRemoved;
    }

    /// <summary>
    /// Promotes a rewritten file over the original, and makes the store able to READ it — the two are one
    /// operation, which is why they live in one helper rather than being two things a caller must remember.
    ///
    /// <para><b>Replacing the bytes at a path DuckDB has already read leaves that path unreadable.</b> DuckDB
    /// caches a parquet file's state against its PATH at INSTANCE scope, so after an in-place swap every
    /// subsequent read fails with "No magic bytes found at end of file" — on NEW connections too, because the
    /// cache is not per connection — until the process restarts. Reproduced standalone rather than inferred,
    /// and the instance scope is the whole trap: opening a fresh connection LOOKS like a fix when tried
    /// against a separate in-memory instance, and is not one against a real store.</para>
    ///
    /// <para>So the swap evicts the cache and rebuilds the archive views here, by construction. Any future
    /// in-place rewrite that goes through this helper is correct without its author having to know any of the
    /// above; one that hand-rolls <c>File.Move</c> instead will silently break the archive it just fixed.</para>
    ///
    /// <para><b>The eviction is the OFF/ON toggle, and the alternatives do not work.</b>
    /// <c>PRAGMA clear_cache</c> does not exist, and <c>enable_object_cache</c> is a different cache that
    /// leaves this one populated. Turning <c>enable_external_file_cache</c> off evicts the entries and turning
    /// it back on restores normal behavior, verified including that a later connection reads the promoted file
    /// and the setting is left enabled.</para>
    ///
    /// <para><b>Deliberately NOT applied to the monthly archive cycle, and that is not an oversight.</b> That
    /// path only ever writes NEW file names and deletes old ones — it never replaces the bytes behind a path
    /// that has been read — so it was never exposed to this and adding the eviction there would be defensive
    /// noise that implies a hazard it does not have.</para>
    /// </summary>
    private async Task PromoteRewrittenFileAsync(
        DuckDBConnection connection, string originalPath, string tempPath, CancellationToken cancellationToken)
    {
        File.Delete(originalPath);
        File.Move(tempPath, originalPath);

        await FlushExternalFileCacheAsync(connection, cancellationToken);
        await _duckDb.CreateArchiveViewsAsync();
    }

    /// <summary>The rewritten file's row count and total execution count, for the conservation check.</summary>
    private static async Task<(long Rows, long Executions)> MeasureRewrittenAsync(
        DuckDBConnection connection, string path, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT COUNT(*), CAST(COALESCE(SUM(execution_count), 0) AS BIGINT) FROM read_parquet('{EscapePath(path)}')";
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (
            Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Evicts DuckDB's cached view of every external file, by toggling the cache off and back on. The WHY is
    /// on <see cref="PromoteRewrittenFileAsync"/>, which is the only thing that should ever need this.
    /// </summary>
    private static async Task FlushExternalFileCacheAsync(DuckDBConnection connection, CancellationToken cancellationToken)
    {
        foreach (var state in new[] { "false", "true" })
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SET enable_external_file_cache={state}";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<IReadOnlyList<string>> ColumnsOfTableAsync(
        DuckDBConnection connection, string table, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position";
        return await ReadStringsAsync(command, cancellationToken);
    }

    /// <summary>
    /// A parquet file's columns, read through DESCRIBE rather than <c>parquet_schema()</c> — the latter
    /// returns the schema TREE, whose root node is not a column and which therefore yields a phantom name.
    /// </summary>
    private static async Task<IReadOnlyList<string>> ColumnsOfParquetAsync(
        DuckDBConnection connection, string file, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"DESCRIBE SELECT * FROM read_parquet('{EscapePath(file)}')";
        return await ReadStringsAsync(command, cancellationToken);
    }

    private static async Task<IReadOnlyList<string>> ReadStringsAsync(DuckDBCommand command, CancellationToken cancellationToken)
    {
        var values = new List<string>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            values.Add(reader.GetString(0));
        }
        return values;
    }

    private static string EscapePath(string path) => path.Replace("'", "''", StringComparison.Ordinal);

    private static string FormatBytes(long bytes) =>
        bytes >= 1024L * 1024L
            ? $"{bytes / (1024.0 * 1024.0):N1} MB"
            : $"{bytes / 1024.0:N0} KB";
}
