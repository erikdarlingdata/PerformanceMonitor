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

    /// <summary>
    /// The staging column that names which slice of a collapsed group SURVIVES (#2771). Prefixed so it
    /// cannot collide with a real Query Store column, present or future.
    /// </summary>
    private const string KeepRowIdColumn = "pm_keep_rowid";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private readonly ILogger<QueryStoreSliceRepairService>? _logger;
    private int _surveysRun;

    /// <summary>
    /// How many times <see cref="SurveyAsync"/> has run on this instance (#5653). A survey is the most
    /// expensive thing this service does, so a startup that runs one more than it needs is a defect the
    /// tests can pin by number.
    /// </summary>
    internal int SurveysRun => Volatile.Read(ref _surveysRun);

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
        /* Which attempt this start is, once known. Zero until the attempt table has been read. */
        long attemptNumber = 0;
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
            attemptNumber = attempts.Count + 1;
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
                survey,
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
                    "#1912 Query Store repair finished with {Count} file(s) unrepaired. {Outlook} {Failures}",
                    result.Failures.Count,
                    RetryOutlook(attemptNumber),
                    string.Join(" | ", result.Failures));
            }
        }
        catch (OperationCanceledException)
        {
            /* #2761 made this REACHABLE. Until the call site handed over a real token this could not fire, so
               the un-awaited Task.Run that starts the repair had nothing to swallow. Now that shutdown can
               abandon a survey mid-flight, an uncaught OperationCanceledException would become an unobserved
               task exception — a repair that stopped, with nothing anywhere saying so. Logged at Information
               rather than Error because being abandoned on shutdown is this service working as designed: the
               marker is withheld and the next launch redoes the work. */
            _logger?.LogInformation(
                "#1912 Query Store repair abandoned before completing (shutdown); the completion marker was " +
                "withheld, so the next launch retries it");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "#1912 Query Store repair could not run. {Outlook}", RetryOutlook(attemptNumber));
        }
    }

    /// <summary>
    /// What happens to a repair that did not finish (#5653). The old text said "will be retried on the next
    /// start" after every attempt, including the last one, after which the next start skips the repair. The last
    /// attempt says so, and names what the user can do about it.
    /// </summary>
    internal static string RetryOutlook(long attemptNumber) =>
        attemptNumber >= MaxStartupAttempts
            ? $"This was attempt {attemptNumber} of {MaxStartupAttempts}, the last one: the repair will NOT run again on the next start. " +
              "The app collects normally, and reads still resolve the duplicate rows deterministically. To try again, free up disk space " +
              $"if the failures above mention it, then drop the '{AttemptTable}' table from the store."
            : $"This was attempt {attemptNumber} of {MaxStartupAttempts}; the unrepaired files will be retried on the next start.";

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
        Interlocked.Increment(ref _surveysRun);
        long groups = 0;
        long rows = 0;

        /* #2761 PHASE 1 of N, not one lock around everything. The read lock is what a collection write waits
           behind — RemoteCollectorService takes the WRITE lock for every write, and ReaderWriterLockSlim
           blocks a writer while any reader holds it — so the duration of a single hold is the duration of a
           fleet-wide collection stall. This survey used to take ONE lock and keep it across the hot GROUP BY
           and a read_parquet of every monthly archive file; on the store that motivated #2748 (31,426 split
           intervals plus multiple archives) that is minutes during which no data arrives anywhere, which is
           indistinguishable from the app having failed to start.

           Scoping the lock per phase lets a waiting writer through at every boundary. The idiom is the one
           RewriteArchiveFileAsync already uses for exactly this reason: the lock and the connection are
           acquired together and released together, because a connection outliving its lock is a connection
           open across a maintenance operation that reorganizes the file, which is where "Reached the end of
           the file" comes from. */
        using (_duckDb.AcquireReadLock(cancellationToken))
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            var hotColumns = await ColumnsOfTableAsync(connection, Table, cancellationToken);
            var hotKey = KeyColumnsFor(hotColumns);

            using var command = connection.CreateCommand();
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
               definition because nothing was written.

               #2761: and per-file LOCKING too. Each file is an independent read, so holding one lock across
               all of them buys nothing and costs a writer the whole sweep rather than one file. */
            try
            {
                using (_duckDb.AcquireReadLock(cancellationToken))
                {
                    using var connection = _duckDb.CreateConnection();
                    await connection.OpenAsync(cancellationToken);
                    archive.Add(await SurveyArchiveFileAsync(connection, file, cancellationToken));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                unreadable.Add($"{Path.GetFileName(file)}: {ex.Message}");
                _logger?.LogWarning(ex, "#1912 could not survey archive file {File}; skipped and left untouched", file);
            }
        }

        return new Survey(groups, rows, archive, unreadable);
    }

    /// <summary>
    /// The archived Query Store parquet files, oldest name first: the whole-month files and the part files
    /// (<c>YYYYMM_query_store_stats_ptNNN.parquet</c>) compaction splits a month into when its input is too big
    /// for one merge (#4721), and, since compaction merges this table one day at a time (#5410), the day files
    /// (<c>YYYYMMDD_query_store_stats.parquet</c>) and their parts. The archive views read these shapes, so the
    /// repair must too (the same two globs match all of them). Each part is a file in
    /// its own right, surveyed and rewritten on its own with the same checks as a whole-month file.
    /// </summary>
    private IEnumerable<string> ArchiveFiles()
    {
        if (!Directory.Exists(_archivePath))
        {
            return [];
        }

        /* MatchType.Simple: '?' is exactly one character, as it is in the archive views' own part glob, so the
           two shapes are the ones compaction writes and no other name. */
        var exact = new EnumerationOptions { MatchType = MatchType.Simple, MatchCasing = MatchCasing.CaseInsensitive, AttributesToSkip = 0 };
        return new[] { $"*_{Table}.parquet", $"*_{Table}_pt???.parquet" }
            .SelectMany(pattern => Directory.EnumerateFiles(_archivePath, pattern, exact))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(f => f, StringComparer.Ordinal);
    }

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
    public async Task<RepairResult> RepairAsync(
        IProgress<string>? progress = null, Survey? survey = null, CancellationToken cancellationToken = default)
    {
        /* #5653: the startup path already surveyed to decide whether there is work and to record the attempt,
           and hands that survey over. Surveying again here ran the hot GROUP BY and three scans of every archive
           file twice per start. A caller with no survey (the public API) still gets a fresh one. */
        survey ??= await SurveyAsync(cancellationToken);
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

            /* #2771: stage the MEASUREMENTS ONLY, never the wide payload. The original staged the full
               projection — every column of every collapsed group, including query_plan_text's showplan XML —
               and on a store with a large pre-#1907 backlog that aggregate does not fit in Lite's 1 GB
               memory_limit. Reproduced with tools/SliceRepairRepro at the reporting store's own 31,426 split
               intervals with 64 KB of plan text per row: "Out of Memory Error: failed to pin block of size
               256.0 KiB (953.4 MiB/953.6 MiB used)". On Windows x64 that same pressure fast-fails the process
               natively (0xc0000409), which is why RepairOnStartupAsync's catch never ran and #2748's reporter
               could not start the app at all.

               The attributes never needed to be in the aggregate. ANY_VALUE was picking an arbitrary row's
               copy of a value every slice of the interval already carries identically — so leaving them on a
               row that survives is the same answer, reached without moving the bytes. Only the numbers are
               recombined, against a staging table that is the key plus a rowid plus the measurements.

               Measured with the same harness: staging drops from OOM to 37-132 ms at 31,426 groups, and the
               repair still completes at 100,000 groups / ~12.8 GB of plan text — 3.2x the reporting store —
               inside the same 1 GB budget. Verified byte-identical to the original at a size where BOTH
               strategies complete: same final row count, same SUM(execution_count), same full-row hash. */
            var measurements = BuildMeasurementProjection(hotColumns, key, "MIN(rowid)", KeepRowIdColumn);
            var assignments = BuildMeasurementAssignments(hotColumns, key, "r");

            using var transaction = connection.BeginTransaction();

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = $@"
CREATE OR REPLACE TEMP TABLE qs_slice_repair AS
SELECT {measurements}
FROM {Table}
GROUP BY {string.Join(", ", key)}
HAVING COUNT(*) > 1";
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            /* Match on the key with IS NOT DISTINCT FROM because the key carries NULLABLE columns —
               runtime_stats_interval_id is NULL on every row collected before #1841 tier 2 — and = against
               NULL is NULL, which would skip exactly the oldest rows. */
            var match = string.Join(" AND ", key.Select(k => $"t.{k} IS NOT DISTINCT FROM r.{k}"));

            /* DELETE the slices that are not the survivor, THEN write the recombined numbers onto the one
               that is. DuckDB carries out an UPDATE that writes an indexed column as delete+insert, which
               CHANGES the row's rowid; the SET list below writes measurements only, which DuckDB updates in
               place, but deleting first means an indexed column joining that list could never leave the survivor
               carrying a rowid the delete predicate no longer recognises — deleting the row it had just
               repaired. After the delete each
               collapsed group holds exactly one row, so the update matches on the key alone and never has to
               trust a rowid across a mutation. */
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText =
                    $"DELETE FROM {Table} AS t WHERE EXISTS (SELECT 1 FROM qs_slice_repair AS r WHERE {match} AND t.rowid <> r.{KeepRowIdColumn})";
                removed += Convert.ToInt64(await command.ExecuteNonQueryAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = $"UPDATE {Table} AS t SET {assignments} FROM qs_slice_repair AS r WHERE {match}";
                await command.ExecuteNonQueryAsync(cancellationToken);
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
            /* A handed-in survey can be older than the file set: archiving or compaction may have removed a file
               since it ran. A file that is gone has nothing left to repair, and treating it as a failure would
               withhold the completion marker for good. A file REPLACED since the survey is not caught here: it is
               rewritten from its current bytes, and the row and execution checks against the survey's figures
               refuse to promote it if they no longer agree. */
            if (!File.Exists(file.Path))
            {
                _logger?.LogInformation("#1912 archive file {File} no longer exists; nothing to repair", file.Path);
                continue;
            }

            try
            {
                removed += await RewriteArchiveFileAsync(file, progress, cancellationToken);
                rewroteArchive = true;
            }
            catch (RewriteSkippedException skipped)
            {
                /* Not an error: nothing was written and the original is untouched. It still goes into the failures
                   so the completion marker is withheld and a later start, once space exists, retries it. */
                failures.Add($"{Path.GetFileName(file.Path)}: {skipped.Message}");
                _logger?.LogWarning("#1912 archive repair skipped for {File}: {Reason}", file.Path, skipped.Message);
                progress?.Report($"Archive {Path.GetFileName(file.Path)}: SKIPPED - {skipped.Message}");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failures.Add($"{Path.GetFileName(file.Path)}: {ex.Message}");
                _logger?.LogError(ex, "#1912 archive repair failed for {File}; the original was left in place", file.Path);
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

    /// <summary>The file could not be rewritten safely right now; nothing was written. Carries the reason for the log.</summary>
    private sealed class RewriteSkippedException(string message) : Exception(message);

    /// <summary>The most DuckDB may spill to its temp folder while one archive file is rewritten (#5653).</summary>
    internal const long RewriteSpillCeilingBytes = 4L * 1024 * 1024 * 1024;

    /// <summary>The folder next to the archive where the rewrite's own DuckDB instance spills (#5653). Emptied and removed after each file.</summary>
    internal const string RewriteSpillFolderName = "slice_repair_tmp";

    /// <summary>Below this much temp room the rewrite is not started: it would only fail part way through.</summary>
    internal const long RewriteSpillFloorBytes = 256L * 1024 * 1024;

    /// <summary>
    /// The rewritten copy that has to fit beside the original: the file plus half again, since a rewrite has been
    /// seen to grow (before the row order was kept, a file that lost 40 rows grew by a third).
    /// </summary>
    internal static long RewriteCopyBytes(long fileBytes) => fileBytes + (fileBytes / 2);

    /// <summary>
    /// How much DuckDB may spill to its temp folder while one archive file is rewritten (#5653), taken from what
    /// the volume can give rather than from a guess about the file: the free space, less the rewritten copy and
    /// less the reserve the store itself keeps writing into, within <see cref="RewriteSpillCeilingBytes"/>. It can
    /// come out negative on a nearly full disk. An unreadable volume (null) gets the ceiling, as compaction treats
    /// it as enough. The caller skips the file when this is under <see cref="RewriteSpillFloorBytes"/>.
    ///
    /// <para>The cap is not a prediction. An earlier version asked for 16 times the file, from one 199 MiB file that
    /// spilled 2.7 GiB under the old query, which carried the text and plan columns through the aggregate. The
    /// current query aggregates only the key and the measurements. Measured on a generated 159 MiB file with the real
    /// <c>query_store_stats</c> columns, it completed with the temp folder capped at 2 MiB (under 0.013 times the
    /// file, against 13.6 times before; that is the unsorted join, and the sorted one needs
    /// <see cref="RewriteSortSpillFactor"/> times the file, which is why the sort is only asked for when the cap can
    /// hold it), so sizing the ask from the file would skip files the disk can hold. If the
    /// rewrite does run out of its cap, DuckDB fails the statement, the temp file goes, and the original stays.</para>
    /// </summary>
    internal static long RewriteSpillCapBytes(long? freeBytes, long fileBytes, long reserveBytes) =>
        freeBytes is long free
            ? Math.Min(RewriteSpillCeilingBytes, free - RewriteCopyBytes(fileBytes) - reserveBytes)
            : RewriteSpillCeilingBytes;

    /// <summary>
    /// How many times the file's size the temp folder must be allowed to hold before the rewrite sorts its output
    /// (#5653). Measured once: sorting the wide rows spilled 9.3 times the file (2.5 GiB for a 272 MiB file with plan
    /// text), so 12 leaves room. Below it the rewrite skips the sort and keeps the join's order, which is the file's
    /// order in all but a handful of rows (2 in 2.4 million, in the measurement) and costs no spill.
    /// </summary>
    internal const long RewriteSortSpillFactor = 12;

    /// <summary>
    /// True when the spill cap is big enough to sort the rewritten rows into the file's row order (#5653). A cap that
    /// is not would turn a file the disk can rewrite into a failure, because the sort needs room the unsorted join
    /// does not.
    /// </summary>
    internal static bool RewriteKeepsExplicitOrder(long capBytes, long fileBytes) =>
        capBytes >= fileBytes * RewriteSortSpillFactor;

    /// <summary>
    /// True when a DuckDB failure is the temp folder filling up: its own cap (<c>max_temp_directory_size</c>) or the
    /// disk under it.
    /// </summary>
    internal static bool IsOutOfTempSpace(Exception ex)
    {
        var text = ex.ToString();
        return text.Contains("max_temp_directory_size", StringComparison.OrdinalIgnoreCase)
            || text.Contains("Out of Disk Space", StringComparison.OrdinalIgnoreCase)
            || text.Contains("not enough space", StringComparison.OrdinalIgnoreCase)
            || text.Contains("offload data block", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Test seam only: the smallest spill cap a rewrite will start with. A test lowers it; production keeps <see cref="RewriteSpillFloorBytes"/>.</summary>
    internal long SpillFloorBytes { get; set; } = RewriteSpillFloorBytes;

    /// <summary>Test seam only, null in production: runs just before the COPY, on the rewrite's own DuckDB instance (the one the cap is set on).</summary>
    internal Func<DuckDBConnection, Task>? InsideSpillCapHook { get; set; }

    /// <summary>
    /// Opens the DuckDB instance one archive file is rewritten on (#5653): its own in-memory instance, with its own
    /// <c>temp_directory</c>, its own <c>max_temp_directory_size</c> (the cap) and its own <c>memory_limit</c>, the way
    /// archive compaction runs its merges (<see cref="ParquetCompaction.MergeBatchToFile"/>).
    ///
    /// <para><b>Why not the store's own instance.</b> <c>max_temp_directory_size</c> and <c>memory_limit</c> are
    /// instance-wide. The repair runs un-awaited at startup while the UI keeps reading, so a cap set on the store's
    /// instance would hold every other query to it (as low as 256 MiB) for as long as a rewrite runs, and a
    /// concurrent archive read that needs more temp room than that would fail. The rewrite reads one parquet file and
    /// writes one parquet file, so it needs nothing from the store's tables and loses nothing by running apart.</para>
    /// </summary>
    internal static async Task<DuckDBConnection> OpenRewriteInstanceAsync(
        string spillDirectory, long capBytes, CancellationToken cancellationToken)
    {
        var capMiB = Math.Max(1, capBytes / (1024 * 1024));
        Directory.CreateDirectory(spillDirectory);
        var connection = new DuckDBConnection("DataSource=:memory:");
        try
        {
            await connection.OpenAsync(cancellationToken);
            using var settings = connection.CreateCommand();
            settings.CommandText =
                $"SET memory_limit = '{ArchiveService.MainConnectionCopyMemoryLimit}'; " +
                $"SET threads = {ParquetCompaction.DefaultThreads}; " +
                $"SET temp_directory = '{EscapePath(spillDirectory.Replace('\\', '/'))}'; " +
                $"SET max_temp_directory_size = '{capMiB}MiB';";
            await settings.ExecuteNonQueryAsync(cancellationToken);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Removes the rewrite's spill folder, whatever is left in it. Best effort: the next rewrite clears it first.</summary>
    private static void DeleteSpillFolder(string spillDirectory)
    {
        try
        {
            if (Directory.Exists(spillDirectory))
            {
                Directory.Delete(spillDirectory, recursive: true);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A file still held open, or an antivirus scan; the next rewrite clears the folder before it starts. */
        }
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

        /* #5653: look at the disk BEFORE the COPY, and size the spill allowance from what the volume can give: the
           free space less the rewritten copy and the reserve, up to 4 GiB. A file is skipped only when that leaves
           under the floor, so a nearly full disk is not asked for more than it has, and a roomy one is not turned
           away by a guess about how much this file will spill. If the rewrite then runs out of its cap, DuckDB
           fails the statement and the catch around the COPY below turns it into a failure that names temp space.
           The reader is the one compaction uses (#5377); an unreadable volume (null) counts as enough. */
        var freeBytes = _duckDb.AvailableFreeBytesProvider(_archivePath);
        var reserveBytes = DataVolumeSpace.CompactionReserveBytes(_duckDb.DatabasePath);
        var copyBytes = RewriteCopyBytes(beforeBytes);
        var spillCapBytes = RewriteSpillCapBytes(freeBytes, beforeBytes, reserveBytes);
        if (spillCapBytes < SpillFloorBytes)
        {
            throw new RewriteSkippedException(
                $"not enough free disk space to rewrite this file safely: {FormatBytes(freeBytes ?? 0)} free, " +
                $"{FormatBytes(copyBytes)} needed for the rewritten copy ({FormatBytes(beforeBytes)} file), " +
                $"{FormatBytes(reserveBytes)} kept as the reserve, which leaves {FormatBytes(Math.Max(0, spillCapBytes))} for " +
                $"temporary spill, under the {FormatBytes(SpillFloorBytes)} a rewrite needs. The file was left as it is. " +
                "This start's repair attempt was used.");
        }

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

            var columnTypes = await ColumnTypesOfParquetAsync(connection, file.Path, cancellationToken);
            var columns = columnTypes.Select(c => c.Name).ToList();
            var key = KeyColumnsFor(columns);
            var ordered = RewriteKeepsExplicitOrder(spillCapBytes, beforeBytes);
            var rewrite = BuildRewriteSelect(columnTypes, key, EscapePath(file.Path), ordered);
            if (!ordered)
            {
                _logger?.LogInformation(
                    "#5653 archive repair of {File}: the temp allowance ({Cap}) is under {Factor} times the file, so the rewrite " +
                    "keeps the join's row order instead of sorting by file position",
                    file.Path, FormatBytes(spillCapBytes), RewriteSortSpillFactor);
            }

            /* The COPY runs on its OWN DuckDB instance (OpenRewriteInstanceAsync), so the spill cap and the memory
               limit it sets do not reach the connections the UI is reading through. The store lock above is still
               held for the whole rewrite, exactly as before. The folder is cleared first because a crash can strand
               one, and removed after because it is only ever scratch. */
            var spillDirectory = Path.Combine(_archivePath, RewriteSpillFolderName);
            DeleteSpillFolder(spillDirectory);
            try
            {
                using (var rewriteInstance = await OpenRewriteInstanceAsync(spillDirectory, spillCapBytes, cancellationToken))
                {
                    if (InsideSpillCapHook is { } hook)
                    {
                        await hook(rewriteInstance);
                    }

                    using var command = rewriteInstance.CreateCommand();
                    command.CommandText = $@"
COPY (
{rewrite}
) TO '{EscapePath(temp)}' ({ParquetCompaction.ArchiveCopyOptions})";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException && IsOutOfTempSpace(ex))
            {
                /* The statement failed and the original was only read, so it is untouched. A half-written sibling
                   would only take disk the next try needs. */
                try { File.Delete(temp); } catch (IOException) { /* the retry's COPY overwrites it */ }
                throw new InvalidOperationException(
                    $"the rewrite ran out of temporary space (DuckDB was allowed {FormatBytes(spillCapBytes)} of temp folder, " +
                    $"with {FormatBytes(freeBytes ?? 0)} free on the volume). The original was left untouched. " +
                    $"DuckDB said: {ex.Message}", ex);
            }
            finally
            {
                DeleteSpillFolder(spillDirectory);
            }

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
                $"(expected {file.Executions:N0}). The original was left untouched.");
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

        /* The rewrite keeps the file's row order and its column types (#5653), so a repaired file is not expected to
           be bigger than the original. Before that, it could be: the old rewrite returned rows in hash order and
           stored execution_count as DOUBLE, and a file that lost 40 rows grew from 17.4 to 23.4 MB. If it grows
           anyway, say so plainly instead of explaining it away: the contents were just verified (rows and execution
           total), so the operator needs to know the data is fine and the size is unexpected. */
        if (afterBytes > beforeBytes)
        {
            progress?.Report(
                $"  ({name} is larger after the repair despite losing rows, which is not expected: the rewrite keeps the " +
                "file's row order and column types. Its rows and execution total were checked against the original before " +
                "it replaced it, so the data is intact.)");
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
    /// <para><b>Not applied to the monthly archive cycle, though compaction does replace a path (#5377).</b> It
    /// writes the merged month over an existing <c>YYYYMM_table.parquet</c> or part file, so it is a swap of the
    /// same kind. Measured on DuckDB 1.5.5 the trap above did not reproduce for it (or for this helper's own
    /// delete-and-move) in 54 swap variants, with <c>parquet_metadata_cache</c> off and on:
    /// <c>validate_external_file_cache</c> defaults to <c>VALIDATE_ALL</c>, so an entry whose file changed is
    /// dropped on its next read. <c>ArchiveCompactionLowDiskTests.ACompactedMonthFile_ReadsItsNewBytes_OnTheSameInstanceThatReadItBefore</c>
    /// pins it on one instance; if it fails after a DuckDB upgrade, compaction's swap needs the eviction too.</para>
    /// </summary>
    private async Task PromoteRewrittenFileAsync(
        DuckDBConnection connection, string originalPath, string tempPath, CancellationToken cancellationToken)
    {
        File.Delete(originalPath);
        File.Move(tempPath, originalPath);

        await FlushExternalFileCacheAsync(connection, cancellationToken);

        /* CreateArchiveViewsCoreAsync, not CreateArchiveViewsAsync (#4262 round 1 finding 3): the caller
           (RepairAsync, ~line 767) already holds the write lock for this exact swap, and s_dbLock's
           NoRecursion policy makes a nested read lock throw. */
        await _duckDb.CreateArchiveViewsCoreAsync();
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
    /// The SELECT that rewrites one archive file with its split slices collapsed (#5653).
    ///
    /// <para><b>Shape.</b> A narrow aggregate (the key, the first row's position in the file and the recombined
    /// measurements, with no text column in it) is joined back to the file on that position, and when
    /// <paramref name="ordered"/> the result is ordered by that position, so the output is the file's own rows in the
    /// file's own order. DuckDB does not promise any order through a join: on a 272 MiB generated file with the real
    /// columns, 2 rows of 2.4 million came out of place without the ORDER BY. The ORDER BY sorts the whole wide row,
    /// though, and the same file spilled 2.5 GiB (9.3 times its size) and took 2.3 times as long with it, against no
    /// spill without it. See <see cref="RewriteKeepsExplicitOrder"/> for when the disk is asked for that. The old shape was one wide <c>GROUP BY</c> over every column.
    /// That carried every attribute through the hash aggregate (a reporter's 199 MiB file spilled 2.7 GiB), and its
    /// output came back in hash order, which lost the time order the archiver wrote and the compression that rides
    /// on it (a file that lost 40 rows grew from 17.4 to 23.4 MB).</para>
    ///
    /// <para><b>Types.</b> The recombined values are cast back to the column's own type. <c>SUM</c> of a BIGINT is
    /// a HUGEINT, which parquet stores as DOUBLE, so <c>execution_count</c> used to come back as DOUBLE. A group of
    /// one row is not recombined at all: its row passes through byte for byte (the old shape also turned a lone
    /// row's average into NULL when its execution count was 0).</para>
    /// </summary>
    internal static string BuildRewriteSelect(
        IReadOnlyList<(string Name, string Type)> columns, IReadOnlyList<string> key, string escapedPath, bool ordered)
    {
        var names = columns.Select(c => c.Name).ToList();
        var keySet = new HashSet<string>(key, StringComparer.OrdinalIgnoreCase);
        var narrow = BuildMeasurementProjection(names, key, "MIN(file_row_number)", "__survivor");
        var select = string.Join(
            ", ",
            columns.Select(c => keySet.Contains(c.Name) || !IsMeasurement(c.Name)
                ? $"t.{c.Name}"
                : $"CASE WHEN s.__slices > 1 THEN CAST(s.{c.Name} AS {c.Type}) ELSE t.{c.Name} END AS {c.Name}"));

        var orderBy = ordered ? Environment.NewLine + "    ORDER BY t.file_row_number" : "";

        return $@"    SELECT {select}
    FROM read_parquet('{escapedPath}', file_row_number = true) AS t
    JOIN (
        SELECT {narrow}, COUNT(*) AS __slices
        FROM read_parquet('{escapedPath}', file_row_number = true)
        GROUP BY {string.Join(", ", key)}
    ) AS s ON t.file_row_number = s.__survivor{orderBy}";
    }

    /// <summary>
    /// A parquet file's columns with their DuckDB types.
    /// </summary>
    private static async Task<IReadOnlyList<(string Name, string Type)>> ColumnTypesOfParquetAsync(
        DuckDBConnection connection, string file, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"DESCRIBE SELECT * FROM read_parquet('{EscapePath(file)}')";
        var values = new List<(string, string)>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            values.Add((reader.GetString(0), reader.GetString(1)));
        }

        return values;
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
