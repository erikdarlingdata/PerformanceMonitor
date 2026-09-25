using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Initializes the DuckDB database and creates tables on first run.
/// </summary>
public class DuckDbInitializer : IDisposable
{
    private readonly string _databasePath;
    private readonly ILogger<DuckDbInitializer>? _logger;

    /// <summary>
    /// The sentinel connection (#4262): one <see cref="DuckDBConnection"/> held open on
    /// <see cref="ConnectionString"/> for the life of this instance. DuckDB.NET's connection manager
    /// dedups by connection string onto one native handle per string, so every other caller's
    /// <see cref="CreateConnection"/> (an identical string) attaches to the handle this field pins open
    /// instead of paying DuckDB's full open/close cost on every call — that cost, not any query, was the
    /// bulk of an Overview tick's latency. No caller other than this class changes: they keep creating
    /// their own <see cref="DuckDBConnection"/> exactly as before.
    ///
    /// <para><b>The hazard this trades in: with the sentinel open, a fresh connection cannot see a file
    /// deleted out from under it.</b> DuckDB.NET hands the fresh connection the SAME cached native handle
    /// rather than reopening the path, so <see cref="ResetDatabaseAsync"/> deleting <c>_databasePath</c>
    /// became a silent no-op — a "new" connection kept reading the deleted file's rows. Every path that
    /// deletes, moves or replaces the database file or its <c>.wal</c> MUST call <see cref="ReleaseSentinel"/>
    /// before doing so and <see cref="ReopenSentinel"/> after, both while still holding the write lock, so
    /// no caller can open in the gap between the file coming down and the sentinel reflecting it.</para>
    /// </summary>
    private DuckDBConnection? _sentinel;

    /// <summary>
    /// Trims the sentinel's memory back down periodically (#4262 round 1 finding 1). The sentinel keeps
    /// DuckDB's buffer pool resident for the app's life — up to <c>memory_limit</c> in <see cref="ConnectionString"/>
    /// — where a pre-sentinel <see cref="CreateConnection"/> released it back to the OS on every close.
    /// Measured on a 1,049 MB store: one full-width 7-day read left the process at 1,074 MB, still 1,067 MB
    /// after 60s idle, versus 56 MB with no sentinel. Self-rescheduling (<c>Change</c> at the end of each
    /// tick, not a repeating period) so a slow cycle can never overlap the next one and touch the shared
    /// <see cref="_sentinel"/> connection from two threads at once. Created in the constructor so
    /// <see cref="Dispose"/> can always stop it, whether or not <see cref="InitializeAsync"/> ever ran.
    /// </summary>
    private readonly Timer _trimTimer;

    /// <summary>
    /// The sentinel's memory usage, in bytes, above which the trim timer takes the write lock and cycles
    /// <c>memory_limit</c> down and back up (#4262 round 1 finding 1). Not const — a test lowers this so a
    /// database with only a few MB of real data still crosses it, instead of needing to manufacture 256 MB
    /// of actual buffer-pool pressure. Mutable, process-wide state: a test that lowers it must restore the
    /// original value when done.
    /// </summary>
    internal static long TrimThresholdBytes = 256L * 1024 * 1024;

    /// <summary>
    /// What the trim cycle sets <c>memory_limit</c> to before setting it back to the value parsed out of
    /// <see cref="ConnectionString"/> (#4262 round 1 finding 1) — low enough that DuckDB's buffer manager
    /// actually evicts pages rather than merely capping future growth. The review's own repro used 32MB and
    /// measured 169ms round-trip; 64MB is used here to leave more headroom for a database with genuinely
    /// large working metadata.
    /// </summary>
    internal const string TrimTargetMemoryLimit = "64MB";

    /// <summary>
    /// Count of trim cycles whose SET-based reset actually ran to completion (#4262 round 3). Instance
    /// state, not static — each test constructs its own <see cref="DuckDbInitializer"/>, so there is
    /// nothing to restore afterward, unlike <see cref="TrimThresholdBytes"/>. Internal so a test can read
    /// it directly instead of inferring "did the cycle run" from a side effect that holds either way.
    /// </summary>
    internal int CompletedTrimCycleCount;

    /// <summary>
    /// How often the trim timer checks the sentinel's memory usage (#4262 round 1 finding 1). Not const —
    /// read fresh by the constructor, so a test can lower it before constructing a
    /// <see cref="DuckDbInitializer"/>; most tests instead call <see cref="RunMemoryTrimCycle"/> directly
    /// rather than waiting on the real timer.
    /// </summary>
    internal static TimeSpan TrimInterval = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Test-only hooks for holding a trim tick "in flight" past <see cref="Dispose"/>'s 1s wait (#4262
    /// round 4), so a test can force that exact race deterministically instead of needing real timing luck.
    /// Both null in production. <see cref="TestTrimTickEntered"/> is set the moment a tick starts, so a test
    /// knows it is safe to call <see cref="Dispose"/>; <see cref="TestTrimTickHoldGate"/> is waited on before
    /// the tick returns, so a test controls exactly when the tick finishes.
    /// </summary>
    internal static ManualResetEventSlim? TestTrimTickEntered;
    internal static ManualResetEventSlim? TestTrimTickHoldGate;

    /// <summary>
    /// Coordinates every DuckDB caller in the process against MAINTENANCE — CHECKPOINT, the archive
    /// export and its DELETEs, compaction, the in-place parquet swap, schema migration — each of which
    /// reorganizes the database file, or the paths under it, while other statements are running. Read
    /// locks allow unlimited concurrent holders. Write locks are exclusive and wait for every reader to
    /// drain first.
    ///
    /// <para>ONE lock for the whole process, deliberately: Lite constructs several
    /// <see cref="DuckDbInitializer"/> instances over the same file (MainWindow,
    /// DatabaseStateOverridesWindow, DuckDbAlertHistoryStore), and making it per-instance would trade a
    /// slow test suite for a real data race.</para>
    ///
    /// <para><b>WHICH LOCK A CALLER TAKES IS DECIDED BY WHAT IT MUST EXCLUDE, NOT BY WHETHER IT READS OR
    /// WRITES (#2463).</b> That one sentence is the rule, and it is written here because the answer had
    /// drifted into looking like two rival conventions: <c>FindingStore</c>'s three write paths take the
    /// READ lock (#2455/#2464), while <c>DuckDbAlertHistoryStore</c> (6), <c>DuckDbMuteRuleStore</c> (5)
    /// and the fourteen callers of <c>LocalDataService.OpenWriteConnectionAsync</c> take the WRITE lock —
    /// twenty-five sites against three, with no note anywhere saying why either was right. They are not
    /// two answers to one question. They are answers to two questions, and the axis that separates them
    /// belongs to DuckDB rather than to anything in this file.</para>
    ///
    /// <para><b>Take the READ lock when all you need is "the file must not be reorganized under me."</b>
    /// That covers every ordinary statement, SELECT and INSERT alike. It is shared, so it costs other
    /// readers nothing, and a held read lock blocks <c>EnterWriteLock</c> — which means holding one IS how
    /// a write says "not while I am in flight", and is the whole of the exclusion an append needs.</para>
    ///
    /// <para><b>Take the WRITE lock when you need one of two stronger things.</b> (1) <b>You ARE the
    /// maintenance</b> — you reorganize the file or the paths under it. <c>ArchiveService</c>,
    /// <c>RemoteCollectorService</c>'s post-collection CHECKPOINT, this class's own re-key, and
    /// <c>QueryStoreSliceRepairService</c>'s hot collapse and file promotion. Never in question. (2)
    /// <b>Your statement can COLLIDE with another writer of the same rows.</b> DuckDB's concurrency
    /// control is optimistic: it does not queue the second writer, it FAILS it. An UPDATE, an upsert, or
    /// a delete-then-reinsert compound can lose that race; an append of new rows cannot. So the write
    /// lock buys serialization exactly where DuckDB would otherwise hand somebody an exception.</para>
    ///
    /// <para>Measured on DuckDB.NET 1.5.5 with the two transactions strictly interleaved — the second
    /// statement runs while the first transaction is still open and uncommitted, which is the only
    /// arrangement that measures anything (let the connections merely start together and one commits
    /// before the other begins, and every case reads as "both committed"):</para>
    ///
    /// <list type="table">
    /// <item><description>two APPENDS of brand-new rows — <b>both commit</b></description></item>
    /// <item><description>a retention DELETE overlapping a disjoint append — <b>both commit</b></description></item>
    /// <item><description>two UPDATEs of the same row — second fails, <c>Conflict on update!</c></description></item>
    /// <item><description>two upserts of the same key — second fails, <c>Conflict on update!</c></description></item>
    /// <item><description>two delete-all-then-reinsert compounds — second fails, <c>Conflict on tuple deletion!</c></description></item>
    /// </list>
    ///
    /// <para><b>That rule describes the code as it stands, with two known exceptions</b>, both appends
    /// holding the write lock: <c>DuckDbAlertHistoryStore.RecordAlertAsync</c> (an INSERT into
    /// <c>config_alert_log</c>) and <c>DuckDbMuteRuleStore.InsertAsync</c> (an INSERT of a fresh rule id).
    /// Neither can collide, so by clause (2) neither needs exclusivity. They are left alone on purpose:
    /// each is one method on a store whose siblings genuinely do need it, both are operator- or
    /// alert-frequency rather than hot, and making two of eleven different would cost more in legibility
    /// than it recovers in lock time. Recorded as over-locked so the next reader knows it was seen.</para>
    ///
    /// <para>It also answers the pair that made the split look like a contradiction.
    /// <c>DuckDbMuteRuleStore.InsertAsync</c> writing <c>config_mute_rules</c> under a write lock and
    /// <c>FindingStore.MuteStoryAsync</c> writing <c>analysis_muted</c> under a read lock are NOT the same
    /// species: nothing but the analysis pass ever writes <c>analysis_muted</c>, while
    /// <c>config_mute_rules</c> is UPDATEd by an operator and DELETEd by a timer-driven expiry purge that
    /// can be running at the same moment.</para>
    ///
    /// <para><b>The WAIT is a separate question from the lock.</b> <c>AcquireWriteLock()</c> with no
    /// timeout waits behind an archival for however long the archival takes. Eleven current callers do
    /// that, and can: they are alert-sweep and mute-CRUD paths whose failures are caught, logged and
    /// non-fatal. <c>LocalDataService.OpenWriteConnectionAsync</c> is the one that cannot, because it is on
    /// the path the UI thread awaits, so it passes <c>LocalDataService.WriteLockBudget</c> — five seconds
    /// in the shipped app, and whatever a host with no dispatcher to protect states instead — and #2208's
    /// maintenance block treats the timeout as "skip this cycle". If you are adding a caller, decide which
    /// of those two you are.</para>
    /// </summary>
    private static readonly ReaderWriterLockSlim s_dbLock = new(LockRecursionPolicy.NoRecursion);

    /// <summary>
    /// Acquires a read lock on the database. Multiple readers can hold this concurrently.
    /// Dispose the returned object to release the lock.
    /// If the current thread already owns a read lock (e.g., leaked by an unhandled exception),
    /// returns a no-op disposable to allow the operation to proceed.
    /// </summary>
    public IDisposable AcquireReadLock() => AcquireReadLock(CancellationToken.None);

    /// <summary>
    /// The same read lock, abandonable (#2443). <see cref="AcquireWriteLock"/> has taken a timeout
    /// since it was written and this one took nothing, which left a real hole in the analysis budget:
    /// every store read on the pass takes this lock BEFORE it opens its connection, so a pass queued
    /// behind a long archival or a compaction sat in an uninterruptible <c>EnterReadLock()</c> however
    /// carefully its reads were threaded. Cancellation reached the reads and stopped at the door.
    ///
    /// <para>A token rather than a timeout, because a timeout would need a NUMBER and there is no
    /// honest one to pick — the right budget for waiting on this lock is exactly the caller's remaining
    /// budget, which the caller already holds. A poll is the only way to say that to
    /// <see cref="ReaderWriterLockSlim"/>, which has no token-taking overload; the interval is only
    /// reached under contention, since an uncontended <c>TryEnterReadLock</c> returns immediately.
    /// <see cref="CancellationToken.None"/> takes the original uninterruptible path, so the fourteen
    /// callers outside the analysis pass are unchanged rather than quietly re-timed.</para>
    /// </summary>
    public IDisposable AcquireReadLock(CancellationToken cancellationToken)
    {
        try
        {
            if (!cancellationToken.CanBeCanceled)
            {
                s_dbLock.EnterReadLock();
            }
            else
            {
                while (!s_dbLock.TryEnterReadLock(ReadLockPollInterval))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
        }
        catch (LockRecursionException)
        {
            /* The current thread already owns a read lock — likely leaked by an unhandled exception
               that prevented Dispose() — OR already owns the write lock (#4262 round 1 finding 3): a
               writer already has every guarantee a reader would get, so this is the same "already
               protected" case, not a bug to surface. Either way, return a no-op disposable so the caller
               proceeds normally. */
            return NoOpDisposable.Instance;
        }
        return new LockReleaser(s_dbLock, write: false);
    }

    /// <summary>
    /// How long a cancellable read-lock wait blocks before re-checking its token. Only reached under
    /// contention, so it costs nothing on the normal path; 50 ms keeps the worst-case overshoot far
    /// below the smallest budget anyone would set while leaving the wait almost entirely in the kernel
    /// rather than spinning.
    /// </summary>
    private static readonly TimeSpan ReadLockPollInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// The read lock for a caller that would rather have NO answer than wait for one — returns null if
    /// the lock is not free within <paramref name="timeout"/>, instead of blocking.
    ///
    /// <para>This exists for the status bar (#2594). Every other read here takes the lock and waits,
    /// which is right when the caller needs the data; the status-bar size figure is cosmetic, refreshed
    /// on a 30-second UI timer, and runs on the dispatcher thread — so waiting on a lock held by a long
    /// archival would freeze the window to render a number nobody is reading. The two honest options for
    /// that caller are "skip the lock" and "skip the read", and skipping the lock is how a connection
    /// ends up open against a file the reset path deletes.</para>
    ///
    /// <para>A timeout rather than a token here, unlike <see cref="AcquireReadLock(CancellationToken)"/>,
    /// because this caller genuinely has a number to pick: it is bounded by what a UI thread may spend,
    /// not by a caller's remaining budget.</para>
    /// </summary>
    public IDisposable? TryAcquireReadLock(TimeSpan timeout)
    {
        try
        {
            if (!s_dbLock.TryEnterReadLock(timeout))
            {
                return null;
            }
        }
        catch (LockRecursionException)
        {
            /* Already held by this thread — same reasoning as AcquireReadLock: we are protected, so
               hand back a no-op rather than reporting a failure the caller cannot act on. */
            return NoOpDisposable.Instance;
        }

        return new LockReleaser(s_dbLock, write: false);
    }

    /// <summary>
    /// How long the status bar will wait for the read lock before giving up and reporting no used-size
    /// figure. Short on purpose: this runs on the dispatcher thread, and the caller already renders the
    /// file size alone when this returns null, so the degraded answer is a smaller status bar rather
    /// than a stalled window.
    /// </summary>
    private static readonly TimeSpan StatusBarReadLockTimeout = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Acquires an exclusive write lock on the database. Blocks until all readers finish.
    /// Dispose the returned object to release the lock.
    /// When a timeout is specified, throws <see cref="TimeoutException"/> if the lock
    /// cannot be acquired within the given duration (e.g., archival is in progress).
    /// </summary>
    public IDisposable AcquireWriteLock(TimeSpan? timeout = null)
    {
        if (timeout.HasValue)
        {
            if (!s_dbLock.TryEnterWriteLock(timeout.Value))
                throw new TimeoutException(
                    "Could not acquire database write lock — another operation (archival or maintenance) may be in progress. Please try again in a few moments.");
        }
        else
        {
            s_dbLock.EnterWriteLock();
        }
        return new LockReleaser(s_dbLock, write: true);
    }

    private sealed class NoOpDisposable : IDisposable
    {
        public static readonly NoOpDisposable Instance = new();
        public void Dispose() { }
    }

    /// <summary>
    /// Releases the lock entry its constructor was handed.
    ///
    /// <para><b>Thread affinity is a LATENT hazard here, and the obvious mitigation is worse than the
    /// hazard (#2463).</b> <see cref="ReaderWriterLockSlim"/> is thread-affine: <c>ExitReadLock</c> throws
    /// <see cref="SynchronizationLockException"/> when called from a thread that did not enter. Every one
    /// of these locks is held across <c>await</c> — <c>AcquireReadLock</c>, then <c>OpenAsync</c>, then the
    /// reads — and the analysis pass runs under <c>Task.Run</c> with no <c>SynchronizationContext</c>, so a
    /// continuation is free to resume on a different pool thread. It does not, and the reason is measured
    /// rather than lucky: <b>DuckDB.NET 1.5.5's async methods complete synchronously</b>, so no await ever
    /// yields and the entering thread is always the exiting thread. Thread id across <c>OpenAsync</c>, a
    /// DDL statement, 200 <c>ExecuteNonQueryAsync</c> calls and a reader drain: <c>4, 4, 4, 4, 4</c>.</para>
    ///
    /// <para><b>Do not "harden" this with an <c>IsReadLockHeld</c> guard.</b> It looks like the cheap fix
    /// and it is a bug amplifier, measured: on a thread that did not enter, <c>IsReadLockHeld</c> is
    /// <c>false</c>, so the guard SKIPS the exit — and the entry the original thread took is then held
    /// forever. With it still held, <c>TryEnterWriteLock(400 ms)</c> fails. So the guard would convert a
    /// loud, attributable <see cref="SynchronizationLockException"/> into a silently leaked reader that
    /// permanently wedges every maintenance operation in the process: no CHECKPOINT, no archival, no
    /// compaction, for the life of the app. Throwing is the better failure.</para>
    ///
    /// <para>A real fix would have to make the releaser not thread-affine at all — replacing
    /// <see cref="ReaderWriterLockSlim"/> with something like a <c>SemaphoreSlim</c>, which would also
    /// retire <see cref="AcquireReadLock(CancellationToken)"/>'s poll — and that is a change to the lock
    /// primitive, not to this class. It is not worth doing while the premise holds. What would break the
    /// premise is a DuckDB.NET release whose async genuinely yields, so
    /// <c>DuckDbLockModelTests.DuckDbAsyncStillCompletesOnTheCallingThread</c> is a tripwire on exactly
    /// that: if it goes red after a driver bump, this paragraph is where to start.</para>
    /// </summary>
    private sealed class LockReleaser : IDisposable
    {
        private readonly ReaderWriterLockSlim _lock;
        private readonly bool _write;
        private bool _disposed;

        public LockReleaser(ReaderWriterLockSlim rwLock, bool write)
        {
            _lock = rwLock;
            _write = write;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (_write) _lock.ExitWriteLock();
            else _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Current schema version. Increment this when schema changes require table rebuilds.
    /// </summary>
    internal const int CurrentSchemaVersion = 64;

    private readonly string _archivePath;

    public DuckDbInitializer(string databasePath, ILogger<DuckDbInitializer>? logger = null)
    {
        _databasePath = databasePath;
        _logger = logger;
        _archivePath = Path.Combine(Path.GetDirectoryName(databasePath) ?? ".", "archive");

        /* #4262 round 1 finding 1. The callback no-ops while _sentinel is null (before InitializeAsync,
           or after Dispose), so starting this here rather than in ReopenSentinel costs nothing and keeps
           Dispose's "stop the timer" guarantee unconditional. */
        _trimTimer = new Timer(OnTrimTimerTick, null, TrimInterval, Timeout.InfiniteTimeSpan);
    }

    private void OnTrimTimerTick(object? state)
    {
        TestTrimTickEntered?.Set();
        try
        {
            RunMemoryTrimCycle();
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Trim cycle failed");
        }
        finally
        {
            /* Test-only (#4262 round 4): block here until a test releases the gate, so Dispose's 1s wait
               can be made to time out on a real, still-running tick on demand. No-op in production. */
            TestTrimTickHoldGate?.Wait();

            /* Self-reschedule rather than a repeating period — see _trimTimer's doc comment. Dispose may
               have already disposed the timer from another thread while this tick was running. */
            try
            {
                _trimTimer.Change(TrimInterval, Timeout.InfiniteTimeSpan);
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    /// <summary>
    /// Checks the sentinel's current memory usage and, if it is over <see cref="TrimThresholdBytes"/>,
    /// cycles <c>memory_limit</c> down to <see cref="TrimTargetMemoryLimit"/> and back up to force DuckDB's
    /// buffer manager to release cached pages (#4262 round 1 finding 1). Internal so a test can call this
    /// directly instead of waiting on <see cref="_trimTimer"/>'s real interval.
    /// </summary>
    internal void RunMemoryTrimCycle()
    {
        /* Snapshot rather than repeated field reads: a concurrent ResetDatabaseAsync/Dispose can set
           _sentinel to null (or a new instance) between any two reads here, and ObjectDisposedException
           from a torn read is expected, not a bug — this cycle just skips and tries again in
           TrimInterval. */
        var sentinel = _sentinel;
        if (sentinel is null)
            return;

        /* #4262 round 3 finding 3: take the read lock around the memory read, unlike round 1's unlocked
           read. Unlocked, this call could run on the sentinel at the exact moment a reset or Dispose
           (both under the write lock) closes it out from under it — a native call on a disposed DuckDB
           connection can take the whole process down, not just throw a catchable exception. Never wait
           (same "a trim cycle must never block a real caller" rule as the write lock below): if the read
           lock is busy, skip this cycle and try again in TrimInterval. Released before the write-lock
           attempt below so the trim never holds both locks at once. */
        if (!s_dbLock.TryEnterReadLock(0))
            return;

        double? beforeBytes;
        try
        {
            beforeBytes = ReadSentinelMemoryUsageBytes(sentinel);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Trim cycle: could not read sentinel memory usage");
            return;
        }
        finally
        {
            s_dbLock.ExitReadLock();
        }

        if (beforeBytes is null || beforeBytes < TrimThresholdBytes)
            return;

        /* Never wait — a reader must never block behind the trim (#4262 round 1 finding 1). If the write
           lock is busy, skip this cycle; the next one TrimInterval later gets another try. */
        if (!s_dbLock.TryEnterWriteLock(0))
        {
            _logger?.LogDebug(
                "Trim cycle: skipped, write lock unavailable ({BeforeMb:F0} MB in use)",
                beforeBytes.Value / (1024.0 * 1024.0));
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            /* Re-read under the lock: _sentinel can't change underneath us now (s_dbLock is exclusive),
               but Dispose could have released it between the unlocked read above and taking this lock. */
            var lockedSentinel = _sentinel;
            if (lockedSentinel is null)
                return;

            var configuredMemoryLimit = ConfiguredMemoryLimit;

            using (var cmd = lockedSentinel.CreateCommand())
            {
                cmd.CommandText = $"SET memory_limit='{TrimTargetMemoryLimit}'";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = lockedSentinel.CreateCommand())
            {
                cmd.CommandText = $"SET memory_limit='{configuredMemoryLimit}'";
                cmd.ExecuteNonQuery();
            }

            /* #4262 round 3: only incremented once both SET statements above actually ran, so a test can
               assert the cycle really executed instead of asserting side effects that hold whether or not
               it did (memory_limit round-trips either way if nothing touched it; process working set
               drifts down from GC noise alone around a bare GC.Collect()). Plain increment, not
               Interlocked: only reachable while this thread holds s_dbLock's write lock, which already
               serializes every writer against this field. */
            CompletedTrimCycleCount++;

            stopwatch.Stop();
            var afterBytes = ReadSentinelMemoryUsageBytes(lockedSentinel);
            _logger?.LogDebug(
                "Trim cycle: {BeforeMb:F0} MB -> {AfterMb:F0} MB in {ElapsedMs}ms (memory_limit {Target} -> {Configured})",
                beforeBytes.Value / (1024.0 * 1024.0),
                (afterBytes ?? 0) / (1024.0 * 1024.0),
                stopwatch.ElapsedMilliseconds,
                TrimTargetMemoryLimit,
                configuredMemoryLimit);
        }
        finally
        {
            s_dbLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Reads the sentinel's current memory usage in bytes (#4262 round 1 finding 1). Returns null if the
    /// read fails — a busy or torn connection is a "skip this cycle", not an error.
    ///
    /// <para><b>Casts the sum to BIGINT in SQL rather than converting in C# (#4262 round 3, my ruling).</b>
    /// <c>sum(memory_usage_bytes)</c> is a DuckDB <c>SUM</c> over a <c>BIGINT</c> column, which DuckDB
    /// always promotes to <c>HUGEINT</c> — regardless of the summed magnitude, not just on overflow — and
    /// DuckDB.NET maps <c>HUGEINT</c> to <see cref="System.Numerics.BigInteger"/>, which does not implement
    /// <see cref="IConvertible"/>. <c>Convert.ToDouble(object)</c> requires that interface and threw
    /// <see cref="InvalidCastException"/> on every call before this fix, confirmed empirically (a 3.2MB
    /// sentinel: "Unable to cast object of type 'System.Numerics.BigInteger' to type
    /// 'System.IConvertible'") — always caught by the try/catch below and logged at Debug, so
    /// <see cref="RunMemoryTrimCycle"/> always saw <c>beforeBytes is null</c> and returned before ever
    /// taking the write lock. The <c>CAST(... AS BIGINT)</c> here keeps the result a plain <c>long</c>,
    /// which <see cref="Convert.ToDouble(object)"/> handles directly. <c>duckdb_memory()</c> exists on the
    /// DuckDB version Lite ships, so there is no fallback to an older, table-function-less build.</para>
    ///
    /// <para>Internal so a test can drive it against a real connection instead of unit-testing a
    /// conversion helper in isolation — the isolated version passed while the real read always failed,
    /// which is exactly how the original bug went unnoticed.</para>
    /// </summary>
    internal double? ReadSentinelMemoryUsageBytes(DuckDBConnection connection)
    {
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT CAST(sum(memory_usage_bytes) AS BIGINT) FROM duckdb_memory()";
            var result = cmd.ExecuteScalar();
            if (result != null && result != DBNull.Value)
                return Convert.ToDouble(result);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Trim cycle: could not read sentinel memory usage");
        }

        return null;
    }

    /// <summary>
    /// The <c>memory_limit</c> the trim cycle restores after trimming — parsed out of
    /// <see cref="ConnectionString"/> rather than repeated as a second "1GB" literal, so the two can never
    /// drift apart (#4262 round 1 finding 1).
    /// </summary>
    private string ConfiguredMemoryLimit
    {
        get
        {
            foreach (var part in ConnectionString.Split(';'))
            {
                var eq = part.IndexOf('=');
                if (eq > 0 && part[..eq].Trim().Equals("memory_limit", StringComparison.OrdinalIgnoreCase))
                    return part[(eq + 1)..].Trim();
            }
            return "1GB"; // unreachable — ConnectionString always sets memory_limit
        }
    }

    /// <summary>
    /// Closes the sentinel (#4262) so the next open genuinely re-reads <c>_databasePath</c> from disk
    /// instead of reattaching to DuckDB.NET's cached handle for the file that is about to come down.
    /// Only safe under the write lock — a live reader could otherwise still be attached when the caller
    /// deletes the file right after this returns. A no-op if the sentinel was never opened (a fresh
    /// install's first <see cref="InitializeAsync"/>) or is already closed (a repeat call).
    /// </summary>
    private void ReleaseSentinel()
    {
        _sentinel?.Dispose();
        _sentinel = null;
    }

    /// <summary>
    /// Opens the sentinel (#4262) once the on-disk file reflects what callers should now see — after
    /// <see cref="InitializeCoreAsync"/> has finished creating tables (including any storage-version
    /// migration) or rebuilding them for a reset. Must run before the write lock guarding that rebuild is
    /// released, or a caller could open in the gap between the file landing and the sentinel pinning it.
    /// </summary>
    private void ReopenSentinel()
    {
        var connection = new DuckDBConnection(ConnectionString);
        connection.Open();
        _sentinel = connection;
    }

    /// <summary>
    /// Closes the sentinel connection so the database file is free to move or delete once the app is
    /// shutting down (#4262). Safe to call more than once and safe to call before the sentinel was ever
    /// opened — both collapse to <see cref="ReleaseSentinel"/>'s existing no-op cases.
    ///
    /// <para><b>Never throws, and never waits unboundedly (#4262 round 1 finding 2).</b> This runs on the
    /// UI thread from <c>MainWindow_Closing</c>, so the old unbounded <c>AcquireWriteLock()</c> could hang
    /// the window close behind an in-flight archival or compaction. Bounded to
    /// <see cref="DisposeWriteLockTimeout"/>, and on a timeout the sentinel is released anyway — a closing
    /// app has nobody left to protect from a torn read, and a leaked handle would block the file from ever
    /// being deleted or moved. Calls <see cref="s_dbLock"/> directly rather than <see cref="AcquireWriteLock"/>,
    /// which throws <see cref="TimeoutException"/> on its own timeout path — exactly what Dispose must not do.</para>
    /// </summary>
    public void Dispose()
    {
        /* Stop the trim timer (#4262 round 1) and wait briefly for an in-flight tick to finish, before
           the lock attempt below (#4262 round 3 finding 3). Timer.Dispose() alone only stops FUTURE
           callbacks — a callback already running on a thread pool thread keeps running after this call
           returns, so without waiting here an in-flight tick could still be reading through, or writing
           through, the sentinel while ReleaseSentinel(WithoutLock) below closes it out from under it. A
           tick's own read- or write-lock-held section is a handful of quick queries — milliseconds — so
           1s is generous, not a real wait in the common case; never unbounded, same "Dispose must never
           hang" reasoning as DisposeWriteLockTimeout below. Its own try/catch: this must never throw
           either, same reason as the rest of Dispose. */
        try
        {
            var trimTimerStopped = new ManualResetEvent(false);
            if (_trimTimer.Dispose(trimTimerStopped) && trimTimerStopped.WaitOne(TimeSpan.FromSeconds(1)))
            {
                trimTimerStopped.Dispose();
            }
            /* else: a tick outlived the wait (or the timer was already disposed) - the runtime still signals
               this handle when the tick ends, so it must stay open; the finalizer reclaims it (#4262). */
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Dispose failed while stopping the trim timer");
        }

        try
        {
            /* If this thread already holds a read lock, TryEnterWriteLock does not return false — it
               throws LockRecursionException immediately, since s_dbLock is NoRecursion. Check first and
               release without the lock in that case: nothing else can be reorganizing the file while this
               thread holds a read lock, because a write can't be in flight at the same time. */
            if (s_dbLock.IsReadLockHeld)
            {
                ReleaseSentinelWithoutLock();
                return;
            }

            if (s_dbLock.TryEnterWriteLock(DisposeWriteLockTimeout))
            {
                try
                {
                    ReleaseSentinel();
                }
                finally
                {
                    s_dbLock.ExitWriteLock();
                }
            }
            else
            {
                _logger?.LogWarning(
                    "Dispose timed out after {Timeout} waiting for the write lock; releasing the sentinel without it",
                    DisposeWriteLockTimeout);
                ReleaseSentinelWithoutLock();
            }
        }
        catch (Exception ex)
        {
            /* Dispose must never throw — this runs on the UI thread's Closing handler, where an exception
               would abort shutdown (#4262 round 1 finding 2). */
            _logger?.LogError(ex, "Dispose failed while releasing the DuckDB sentinel");
        }
    }

    /// <summary>
    /// How long <see cref="Dispose"/> waits for the write lock before giving up and releasing the sentinel
    /// unprotected (#4262 round 1 finding 2). Short and fixed: unlike <see cref="LocalDataService"/>'s
    /// configurable write-lock budget, there is no host to declare a different number for — this is the
    /// one caller that runs on a UI thread during shutdown, where the app is going away either way.
    /// </summary>
    private static readonly TimeSpan DisposeWriteLockTimeout = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Releases the sentinel without the write lock (#4262 round 1 finding 2) — used when this thread
    /// already holds the read lock, or when the write lock wait timed out. <see cref="Interlocked"/> rather
    /// than the plain null-then-dispose <see cref="ReleaseSentinel"/> uses, because without the write lock
    /// excluding other threads, a plain check-then-dispose could race a concurrent <see cref="ReopenSentinel"/>
    /// and dispose the wrong connection. This path only runs at shutdown or on a rare read-lock-held
    /// re-entry, so the residual race (this thread and a reset both touching <see cref="_sentinel"/>
    /// without a shared lock) is accepted rather than engineered away.
    /// </summary>
    private void ReleaseSentinelWithoutLock()
    {
        var connection = Interlocked.Exchange(ref _sentinel, null);
        connection?.Dispose();
    }

    /* Tables that have parquet archives — views are created to UNION hot data with archived parquet files.
       Catalog-driven: every collector table (from CollectorCatalog) plus the two non-collector time-series
       tables (config_alert_log, collection_log). Adding a collector to the catalog gives it an archive view
       for free — no hand-maintained list to keep in sync. Mirrors ArchiveService.ArchivableTables (same set,
       same derivation); a test pins the two against each other and against the catalog. */
    internal static readonly string[] ArchivableTables =
        /* StoredCollectors, not CollectorCatalog.All: Lite does not CREATE the PostgreSQL collectors' tables
           (it has no PostgreSQL target and cannot get one), so an archive view over them would reference a
           table that does not exist. */
        DuckDbSchemaGenerator.StoredCollectors.Select(c => c.TargetTable)
            .Concat(["config_alert_log", "collection_log"])
            .ToArray();

    /* Archive views for these tables must DEDUP the hot∪parquet union on a server-side natural key.
       The 512MB emergency reset (ArchiveService.ArchiveAllAndResetAsync) archives all hot data to parquet
       AND wipes collection_log, so the next cycle re-collects recent history into the hot store while the
       parquet tier still holds it — the plain UNION ALL would then show each re-collected event twice.
       The local surrogate prefix id (job_history_id / default_trace_event_id) is a per-process counter
       (CollectionIdGenerator), so it is NOT stable across re-collection and cannot be the key — only the
       SQL-Server-side identity is. Other archivable tables can't double up this way (normal archival keeps
       hot and parquet disjoint, and their rows aren't re-collected after a reset), so they keep the plain
       union. Value = the PARTITION BY column list for the QUALIFY ROW_NUMBER dedup. */
    private static readonly Dictionary<string, string> ArchiveViewDedupKeys =
        new(StringComparer.Ordinal)
        {
            /* sysjobhistory.instance_id: a unique monotonic IDENTITY per server that survives
               sp_purge_jobhistory — JobHistoryCollector's exact-and-complete dedup watermark. */
            ["job_history"] = "server_id, instance_id",
            /* The default trace's EventSequence is unique within a trace; pairing it with event_time
               (the StartTime watermark) keeps events distinct across the server restarts that reset
               EventSequence, and groups identical re-collected rows (NULLs included) for dedup. */
            ["default_trace_events"] = "server_id, event_time, event_sequence",
        };

    /// <summary>
    /// Gets the connection string for the DuckDB database.
    /// - checkpoint_threshold=1GB: disables automatic WAL checkpoints to prevent
    ///   2-3s stop-the-world stalls during collector writes. Manual CHECKPOINT
    ///   runs between collection cycles instead.
    /// - memory_limit=1GB: caps the resting buffer pool so it doesn't grow
    ///   unbounded as the archive directory fills with parquet files (the
    ///   ".tmp dir caching" path is the actual driver of #933's titled
    ///   complaint — uncapped, buffer pool grows toward 80% of system RAM).
    ///   ArchiveService raises this temporarily for parquet COPY operations,
    ///   which need more headroom due to a DuckDB pre-reservation behavior.
    /// </summary>
    public string ConnectionString => $"Data Source={_databasePath};memory_limit=1GB;checkpoint_threshold=1GB";

    /// <summary>
    /// Ensures the database exists and all tables are created, then opens the sentinel (#4262).
    /// Handles DuckDB version mismatches by exporting data to Parquet, recreating the database, and importing.
    ///
    /// <para>Takes the write lock for its whole body — including <see cref="ResetDatabaseAsync"/>'s
    /// destructive path calling the lock-free <see cref="InitializeCoreAsync"/> directly instead of this
    /// method, since <see cref="s_dbLock"/> is <see cref="LockRecursionPolicy.NoRecursion"/> and a second
    /// <c>AcquireWriteLock</c> on the same (already-holding) thread would throw rather than nest.</para>
    /// </summary>
    public async Task InitializeAsync()
    {
        using var writeLock = AcquireWriteLock();

        /* A repeat call (a test re-initializing the same instance, say) must not leak the previous
           sentinel — ReleaseSentinel is a no-op the first time, when there is nothing to release yet. */
        ReleaseSentinel();

        await InitializeCoreAsync();

        /* Only now, with tables created (or migrated) and archive views/analysis schema in place, is the
           on-disk file what callers should see. Opening here — still under the write lock — means no
           caller can attach to a partially-initialized file. */
        ReopenSentinel();
    }

    /// <summary>
    /// The body of <see cref="InitializeAsync"/>, split out so <see cref="ResetDatabaseAsync"/> can run it
    /// while it is already holding the write lock itself. Takes no lock of its own — every caller must
    /// already hold the write lock before calling this.
    /// </summary>
    private async Task InitializeCoreAsync()
    {
        _logger?.LogInformation("Initializing DuckDB database at {Path}", _databasePath);

        var directory = Path.GetDirectoryName(_databasePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
            _logger?.LogInformation("Created database directory: {Directory}", directory);
        }

        var archivePath = Path.Combine(directory ?? ".", "archive");
        if (!Directory.Exists(archivePath))
        {
            Directory.CreateDirectory(archivePath);
            _logger?.LogInformation("Created archive directory: {ArchivePath}", archivePath);
        }

        /* Open the database. Only a genuine storage-version mismatch triggers the
           destructive Parquet rebuild; transient lock contention is retried instead. */
        DuckDBConnection connection = await OpenDatabaseAsync(archivePath);

        using (connection)
        {
            await ExecuteNonQueryAsync(connection,
                "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)");

            var existingVersion = await GetSchemaVersionAsync(connection);

            /* On a fresh/reset database (v0), skip migrations entirely — they DROP tables
               expecting CREATE TABLE to follow, which is destructive on a blank DB.
               Just create tables with the current schema and stamp the version. */
            if (existingVersion > 0 && existingVersion < CurrentSchemaVersion)
            {
                _logger?.LogInformation("Schema upgrade needed: v{Old} -> v{New}", existingVersion, CurrentSchemaVersion);
                await RunMigrationsAsync(connection, existingVersion);
            }

            foreach (var tableStatement in Schema.GetAllTableStatements())
            {
                await ExecuteNonQueryAsync(connection, tableStatement);
            }

            foreach (var indexStatement in Schema.GetAllIndexStatements())
            {
                await ExecuteNonQueryAsync(connection, indexStatement);
            }

            if (existingVersion < CurrentSchemaVersion)
            {
                await SetSchemaVersionAsync(connection, CurrentSchemaVersion);
            }

            /* Table count on the init connection — makes a failed reset (schema not persisting to the
               file for the next connection to see) diagnosable from the log alone. */
            using (var tableCountCmd = connection.CreateCommand())
            {
                tableCountCmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
                var tableCount = Convert.ToInt64(await tableCountCmd.ExecuteScalarAsync());
                _logger?.LogInformation("Schema initialization created {Count} tables", tableCount);
            }

            _logger?.LogInformation("Database initialization complete. Schema version: {Version}", CurrentSchemaVersion);
        }

        /* CreateArchiveViewsCoreAsync, not CreateArchiveViewsAsync: this thread already holds the write
           lock (#4262 round 1 finding 3), and s_dbLock's NoRecursion policy makes a nested read lock
           throw. */
        await CreateArchiveViewsCoreAsync();

        await InitializeAnalysisSchemaAsync();
    }

    /// <summary>
    /// Opens the DuckDB database, handling the two failure modes distinctly:
    /// a genuine storage-version mismatch is migrated via Parquet export/import,
    /// while transient lock contention (e.g. an instance that was just killed,
    /// or antivirus holding the file) is retried before giving up.
    /// Any other open failure is rethrown — it must NOT trigger the destructive
    /// Parquet rebuild, which would move the live database aside (Issue #977).
    /// </summary>
    private async Task<DuckDBConnection> OpenDatabaseAsync(string archivePath)
    {
        const int maxLockRetries = 5;
        const int lockRetryDelayMs = 1000;

        for (int attempt = 1; ; attempt++)
        {
            var connection = new DuckDBConnection(ConnectionString);
            try
            {
                await connection.OpenAsync();
                return connection;
            }
            catch (Exception ex) when (IsStorageVersionError(ex))
            {
                connection.Dispose();
                _logger?.LogWarning("DuckDB storage version mismatch detected. Migrating data via Parquet export/import.");
                await MigrateViaParquetAsync(archivePath);

                var migrated = new DuckDBConnection(ConnectionString);
                await migrated.OpenAsync();
                return migrated;
            }
            catch (Exception ex) when (IsTransientLockError(ex) && attempt < maxLockRetries)
            {
                connection.Dispose();
                _logger?.LogWarning(
                    "DuckDB database is locked (attempt {Attempt}/{Max}); retrying in {Delay}ms. {Error}",
                    attempt, maxLockRetries, lockRetryDelayMs, ex.Message);
                /* Thread.Sleep, not await Task.Delay (#4262 round 1 finding 4): this loop runs inside
                   InitializeCoreAsync while the caller's write lock is held, and that lock is thread-affine
                   (ReaderWriterLockSlim). On a thread with no SynchronizationContext — the archive reset
                   path is one — an awaited Task.Delay is free to resume on a different pool thread, and
                   ExitWriteLock from a thread that never entered throws SynchronizationLockException,
                   leaking the lock for the rest of the process's life. This was already true before #4262;
                   the sentinel didn't create it. A blocking sleep keeps the same thread the whole way
                   through, at the cost of tying up one pool thread for the retry delay — affordable for a
                   five-retry, one-second-apart file-lock retry. */
                Thread.Sleep(lockRetryDelayMs);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }
    }

    /// <summary>
    /// Checks if an exception is a genuine DuckDB storage-version mismatch — an
    /// incompatible on-disk format that cannot be opened as-is and must be
    /// rebuilt. Deliberately narrow: generic open failures and lock contention
    /// must NOT match, or the destructive Parquet rebuild fires on a database
    /// that is merely locked or recovering a WAL from an unclean shutdown.
    /// </summary>
    private static bool IsStorageVersionError(Exception ex)
    {
        /* DuckDB reports a genuine version mismatch as one of:
           - "Serialization Error: Failed to deserialize: ..." (incompatible storage format)
           - "IO Error: Trying to read a database file with version number X,
              but we can only read version Y"
           Since DuckDB v0.10+, newer libraries read older files, so this almost
           always means an older library was pointed at a newer file. */
        var message = ex.ToString().ToLowerInvariant();
        return message.Contains("failed to deserialize")
            || message.Contains("trying to read a database file with version")
            || message.Contains("storage version");
    }

    /// <summary>
    /// Checks if an exception is transient lock contention on the database file —
    /// another process (a just-killed prior instance, antivirus) is holding it.
    /// These are safe to retry and must never trigger the Parquet rebuild.
    /// </summary>
    private static bool IsTransientLockError(Exception ex)
    {
        var message = ex.ToString().ToLowerInvariant();
        return message.Contains("conflicting lock")
            || message.Contains("could not set lock")
            || message.Contains("being used by another process");
    }

    /// <summary>
    /// Exports all tables from the old database to Parquet, deletes the database, and reimports.
    /// Uses DuckDB's EXPORT DATABASE which writes one Parquet file per table.
    /// </summary>
    private async Task MigrateViaParquetAsync(string archivePath)
    {
        var exportDir = Path.Combine(archivePath, $"upgrade_{DateTime.Now:yyyyMMdd_HHmmss}");
        Directory.CreateDirectory(exportDir);

        /* Step 1: Try to export from the old database using EXPORT DATABASE.
           Since DuckDB v0.10+, newer versions can read older files (backward compat),
           so upgrading DuckDB should normally open the file without hitting this path.
           This mainly handles edge cases (e.g., downgrade, corruption).
           If the file is truly unreadable, the backup preserves it for manual recovery
           using the original DuckDB version's CLI: duckdb old.db "EXPORT DATABASE 'dir'" */
        var exported = false;
        try
        {
            /* Attempt read-only open — some version mismatches allow read but not write */
            var readOnlyConnStr = $"Data Source={_databasePath};ACCESS_MODE=READ_ONLY";
            using (var oldConn = new DuckDBConnection(readOnlyConnStr))
            {
                await oldConn.OpenAsync();

                /* Export all tables to Parquet */
                using var cmd = oldConn.CreateCommand();
                cmd.CommandText = $"EXPORT DATABASE '{exportDir.Replace("'", "''")}' (FORMAT PARQUET)";
                await cmd.ExecuteNonQueryAsync();
                exported = true;
                _logger?.LogInformation("Exported old database to {ExportDir}", exportDir);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Could not export old database — data will be preserved as backup file only");
        }

        /* Step 2: Back up and delete the old database file */
        var backupPath = _databasePath + $".backup_{DateTime.Now:yyyyMMdd_HHmmss}";
        try
        {
            /* DuckDB may have .wal files too */
            File.Move(_databasePath, backupPath);
            _logger?.LogInformation("Backed up old database to {BackupPath}", backupPath);

            var walPath = _databasePath + ".wal";
            if (File.Exists(walPath))
            {
                File.Move(walPath, backupPath + ".wal");
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to back up old database, deleting instead");
            File.Delete(_databasePath);

            var walPath = _databasePath + ".wal";
            if (File.Exists(walPath)) File.Delete(walPath);
        }

        /* Step 3: If we exported successfully, import into the fresh database */
        if (exported)
        {
            try
            {
                using var newConn = new DuckDBConnection(ConnectionString);
                await newConn.OpenAsync();

                using var cmd = newConn.CreateCommand();
                cmd.CommandText = $"IMPORT DATABASE '{exportDir.Replace("'", "''")}' ";
                await cmd.ExecuteNonQueryAsync();
                _logger?.LogInformation("Imported data from Parquet export into new database");
            }
            catch (Exception ex)
            {
                /* Import may fail if schema changed between versions — that's okay,
                   the normal initialization will create fresh tables */
                _logger?.LogWarning(ex, "Could not import Parquet data — starting with fresh tables. " +
                    "Parquet files preserved at {ExportDir} for manual recovery.", exportDir);
            }
        }
    }

    private async Task<int> GetSchemaVersionAsync(DuckDBConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT COALESCE(MAX(version), 0) FROM schema_version";
            var result = await command.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
        catch
        {
            return 0;
        }
    }

    private async Task SetSchemaVersionAsync(DuckDBConnection connection, int version)
    {
        await ExecuteNonQueryAsync(connection, "DELETE FROM schema_version");
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO schema_version (version) VALUES ($1)";
        command.Parameters.Add(new DuckDBParameter { Value = version });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Runs schema migrations from the given version up to CurrentSchemaVersion.
    /// Each migration drops and recreates affected tables.
    ///
    /// IMPORTANT: When adding a new data collection table, you must also register it in:
    ///   1. Schema.cs — GetAllTableStatements() and GetAllIndexStatements()
    ///   2. DuckDbInitializer.cs — ArchivableTables (archive view creation)
    ///   3. ArchiveService.cs — ArchivableTables (parquet export + purge)
    /// Forgetting any of these causes unbounded growth and 512 MB reset loops.
    /// </summary>
    private async Task RunMigrationsAsync(DuckDBConnection connection, int fromVersion)
    {
        if (fromVersion < 2)
        {
            /* v2: Added delta columns to query_stats (delta_logical_writes, delta_physical_reads, delta_spills)
                   and procedure_stats (delta_logical_reads, delta_logical_writes, delta_physical_reads).
                   Added plan_id, avg_logical_writes, avg_physical_reads to query_store_stats.
                   Restructured blocked_process_reports. */
            _logger?.LogInformation("Running migration to v2: rebuilding query_stats, procedure_stats, query_store_stats, blocked_process_reports");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_store_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS blocking_snapshots"); /* Cleanup - table no longer used */
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS blocked_process_reports");
        }

        if (fromVersion < 3)
        {
            /* v3: Fix server_id values. Previously used string.GetHashCode() which is
                   randomized per process in .NET Core, producing different IDs on each restart.
                   Now uses a deterministic FNV-1a hash of server_name. This migration updates
                   all existing rows to use the correct deterministic server_id. */
            _logger?.LogInformation("Running migration to v3: fixing server_id values (non-deterministic hash -> deterministic)");
            await FixServerIdsAsync(connection);
        }

        if (fromVersion < 4)
        {
            /* v4: Added sql_duration_ms and duckdb_duration_ms columns to collection_log
                   for split collector timing (SQL query vs DuckDB insert).
                   Only ALTER if the table already exists — on fresh installs it will be
                   created with these columns by GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v4: adding timing columns to collection_log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS sql_duration_ms INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS duckdb_duration_ms INTEGER");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 5)
        {
            /* v5: Added database_scoped_config and trace_flags tables
                   for database-scoped configuration and active trace flag collection. */
            _logger?.LogInformation("Running migration to v5: adding database_scoped_config and trace_flags tables");
            /* Generated from the catalog (same source as GetAllTableStatements, which also recreates these
               with IF NOT EXISTS immediately after migrations); byte-equivalent to the former hand-written
               Schema constants this migration used before the schema was made catalog-driven. The index is
               null-checked (both collectors have one today) rather than asserted, mirroring the generator. */
            foreach (ICollectorSchemaInfo collector in new[]
                { (ICollectorSchemaInfo)DatabaseScopedConfigCollector.Instance, TraceFlagsCollector.Instance })
            {
                await ExecuteNonQueryAsync(connection, DuckDbSchemaGenerator.CreateTable(collector));
                var collectorIndex = DuckDbSchemaGenerator.CreateIndex(collector);
                if (collectorIndex is not null)
                {
                    await ExecuteNonQueryAsync(connection, collectorIndex);
                }
            }
        }

        if (fromVersion < 6)
        {
            /* v6: Added sql_handle and plan_handle to query_stats and procedure_stats,
                   and query_plan_hash to query_store_stats for cross-referencing.
                   Must drop/recreate because ALTER TABLE appends columns at the end,
                   but the DuckDB appender writes by position and expects specific column order. */
            _logger?.LogInformation("Running migration to v6: rebuilding query_stats, procedure_stats, query_store_stats for handle columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_store_stats");
        }

        if (fromVersion < 7)
        {
            /* v7: Changed collection_log.log_id from INTEGER to BIGINT.
                   GenerateCollectionId() returns a long seeded from DateTime.UtcNow.Ticks
                   which overflows 32-bit INTEGER, causing all collection_log INSERTs to fail silently. */
            _logger?.LogInformation("Running migration to v7: rebuilding collection_log (log_id INTEGER -> BIGINT)");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS collection_log");
        }

        if (fromVersion < 8)
        {
            /* v8: Added min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   and total_spills columns to procedure_stats for parity with Dashboard.
                   Must drop/recreate because DuckDB appender writes by position. */
            _logger?.LogInformation("Running migration to v8: rebuilding procedure_stats for min/max/spills columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
        }

        if (fromVersion < 9)
        {
            /* v9: Added dismissed column to config_alert_log for hide/dismiss functionality.
                   Safe to ALTER because this table uses INSERT (not appender). */
            _logger?.LogInformation("Running migration to v9: adding dismissed column to config_alert_log");
            try
            {
                /* DuckDB does not support ADD COLUMN with NOT NULL — use nullable with DEFAULT */
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS dismissed BOOLEAN DEFAULT false");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 10)
        {
            /* v10: Added server_name column to collection_log so log entries
                    can be identified by server without needing a lookup table. */
            _logger?.LogInformation("Running migration to v10: adding server_name column to collection_log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS server_name VARCHAR");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 11)
        {
            /* v11: Expanded database_config from 9 to 28 columns (sys.databases).
                    Added state_desc, collation, RCSI, snapshot isolation, stats settings,
                    encryption, security, and version-gated columns (ADR, memory optimized, optimized locking). */
            _logger?.LogInformation("Running migration to v11: rebuilding database_config for expanded sys.databases columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS database_config");
        }

        if (fromVersion < 12)
        {
            /* v12: Added login_name, host_name, program_name, open_transaction_count,
                    percent_complete columns to query_snapshots for Issue #149. */
            _logger?.LogInformation("Running migration to v12: adding session columns to query_snapshots");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS login_name VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS host_name VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS program_name VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS open_transaction_count INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS percent_complete DECIMAL(5,2)");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 13)
        {
            /* v13: Full column parity with Dashboard for all three query/procedure collectors.
                    query_stats: added creation_time, last_execution_time, total_clr_time,
                      min/max physical_reads, rows, spills, memory grant columns (6), thread columns (4).
                    procedure_stats: added cached_time, last_execution_time,
                      min/max logical_reads, physical_reads, logical_writes, spills.
                    query_store_stats: complete rebuild with all min/max columns, DOP, CLR,
                      memory, tempdb, plan forcing, compilation metrics, version-gated columns.
                    Must drop/recreate because DuckDB appender writes by position. */
            _logger?.LogInformation("Running migration to v13: rebuilding query_stats, procedure_stats, query_store_stats for full Dashboard column parity");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS procedure_stats");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS query_store_stats");
        }

        if (fromVersion < 14)
        {
            /* v14: Switched memory_grant_stats from per-session (dm_exec_query_memory_grants)
                    to semaphore-level (dm_exec_query_resource_semaphores) for parity with Dashboard.
                    Old schema had session_id, query_text, dop, etc. New schema has
                    resource_semaphore_id, pool_id, and delta columns.
                    Must drop/recreate because column layout is completely different. */
            _logger?.LogInformation("Running migration to v14: rebuilding memory_grant_stats for resource semaphore schema");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS memory_grant_stats");
        }

        if (fromVersion < 15)
        {
            /* v15: Added queued I/O columns (io_stall_queued_read_ms, io_stall_queued_write_ms)
                    and their delta counterparts to file_io_stats for latency overlay charts.
                    Must drop/recreate because DuckDB appender writes by position. */
            _logger?.LogInformation("Running migration to v15: rebuilding file_io_stats for queued I/O columns");
            await ExecuteNonQueryAsync(connection, "DROP TABLE IF EXISTS file_io_stats");
        }

        if (fromVersion < 16)
        {
            /* v16: Added database_size_stats and server_properties tables for FinOps monitoring.
                    New tables only — no existing table changes needed. Tables created by
                    GetAllTableStatements() during initialization. */
            _logger?.LogInformation("Running migration to v16: adding FinOps tables (database_size_stats, server_properties)");
        }

        if (fromVersion < 17)
        {
            /* v17: Added volume-level drive space columns to database_size_stats.
                    Columns appended at end — safe for DuckDB appender positional writes. */
            _logger?.LogInformation("Running migration to v17: adding volume stats columns to database_size_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS volume_mount_point VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS volume_total_mb DECIMAL(19,2)");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS volume_free_mb DECIMAL(19,2)");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with correct schema below */
            }
        }

        if (fromVersion < 18)
        {
            /* v18: Added session_stats table for per-application connection tracking
                    from sys.dm_exec_sessions. New table only — created by GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v18: adding session_stats table for application connections");
        }

        if (fromVersion < 19)
        {
            _logger?.LogInformation("Running migration to v19: adding worker thread columns to memory_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE memory_stats ADD COLUMN IF NOT EXISTS max_workers_count INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE memory_stats ADD COLUMN IF NOT EXISTS current_workers_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v19 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 20)
        {
            _logger?.LogInformation("Running migration to v20: adding mute rules table and muted column to alert log");
            try
            {
                /* DuckDB does not support ADD COLUMN with NOT NULL — use nullable with DEFAULT */
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS muted BOOLEAN DEFAULT false");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v20 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 21)
        {
            _logger?.LogInformation("Running migration to v21: adding detail_text column to alert log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS detail_text VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v21 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 22)
        {
            _logger?.LogInformation("Running migration to v22: adding growth rate and VLF count columns to database_size_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS is_percent_growth BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS growth_pct INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE database_size_stats ADD COLUMN IF NOT EXISTS vlf_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v22 failed");
                throw;
            }
        }

        if (fromVersion < 23)
        {
            _logger?.LogInformation("Running migration to v23: adding dismissed_archive_alerts sidecar table");
            try
            {
                await ExecuteNonQueryAsync(connection, Schema.CreateDismissedArchiveAlertsTable);
                await ExecuteNonQueryAsync(connection, Schema.CreateDismissedArchiveAlertsIndex);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v23 failed");
                throw;
            }
        }

        if (fromVersion < 24)
        {
            _logger?.LogInformation("Running migration to v24: adding vcore_count column to server_properties for Azure SQL DB vCore tracking");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS vcore_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v24 failed");
                throw;
            }
        }

        if (fromVersion < 25)
        {
            /* v25: Added memory_pressure_events table for RING_BUFFER_RESOURCE_MONITOR notifications.
                    New table only — created by GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v25: adding memory_pressure_events table");
        }

        if (fromVersion < 26)
        {
            _logger?.LogInformation("Running migration to v26: adding context_json column to alert log");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_alert_log ADD COLUMN IF NOT EXISTS context_json VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v26 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 27)
        {
            _logger?.LogInformation("Running migration to v27: adding server-health columns (LPIM/IFI/memory dumps) to server_properties");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS lock_pages_in_memory BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS instant_file_initialization_enabled BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS memory_dump_count INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Migration to v27 failed");
                throw;
            }
        }

        if (fromVersion < 28)
        {
            /* v28: Added is_cdc_capture flag to query_snapshots so the long-running query
                    alert can exclude CDC capture sessions. The collector computes the flag
                    server-side (program_name -> job_id via msdb.dbo.cdc_jobs, text fallback).
                    Appended at the end to match the DuckDB appender's positional order. */
            _logger?.LogInformation("Running migration to v28: adding is_cdc_capture column to query_snapshots");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS is_cdc_capture BOOLEAN DEFAULT false");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v28 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 30)
        {
            /* v30 (#1140): dedup-fingerprint support. blocked_process_reports gains the contentious
               object the blocked_process_report event already carries (object_id/database_id) plus the
               resolved name; query_snapshots gains query_hash for the long-running-query dedup key.
               Appended at the end to keep the positional appender aligned; the v_ views union BY NAME
               so old parquet reads back NULL for these. */
            _logger?.LogInformation("Running migration to v30: dedup fingerprint columns (#1140)");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS object_id INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS database_id INTEGER");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS contentious_object VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS query_hash VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v30 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 31)
        {
            /* v31: failed-Agent-job watermark persistence. The blocking/deadlock edge-trigger
               watermarks already survive restart (#1145); the failed-job watermark did not, so a
               reopen re-fired tray toasts for failures still inside the lookback window that the
               user had already seen and dismissed. Adds a nullable watermark_time column to the
               existing watermark table to hold the newest already-alerted failure's server-local
               run time. Only ALTER if the table exists — fresh installs get the column from
               GetAllTableStatements(). */
            _logger?.LogInformation("Running migration to v31: failed-job watermark column");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_edge_trigger_watermarks ADD COLUMN IF NOT EXISTS watermark_time TIMESTAMP");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v31 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 32)
        {
            /* v32: block-chain reconstruction now keys sessions by spid:ecid within monitor_loop (mirroring
               sp_HumanEventsBlockViewer). blocked_process_reports gains monitor_loop (the blocked-process-report
               episode). Appended at the end to keep the positional appender aligned; the v_ view (SELECT *,
               recreated on startup) surfaces it; old parquet reads back NULL (union BY NAME). The collector
               appender now writes monitor_loop, so an un-migrated DB would mis-align — this ALTER is required. */
            _logger?.LogInformation("Running migration to v32: blocked_process_reports.monitor_loop");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS monitor_loop INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v32 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 33)
        {
            /* v33: procedure_stats gains delta_spills, and query_stats gains plan_generation_num +
               sample_interval_seconds. All appended at the end to keep the positional appenders aligned; the
               v_ views (SELECT *) surface them; old parquet reads back NULL (union BY NAME). The collectors now
               write these columns, so an un-migrated DB would mis-align — these ALTERs are required.
               - procedure_stats.delta_spills: spill-delta parity with query_stats (proc Total/Avg Spills now
                 reflect per-window work, not summed cumulative DMV totals).
               - query_stats.plan_generation_num: plan-stability signal.
               - query_stats.sample_interval_seconds: lets the display derive worker_time_per_second (CPU-ms/sec). */
            _logger?.LogInformation("Running migration to v33: proc delta_spills + query_stats signal columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS delta_spills BIGINT");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS plan_generation_num BIGINT");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS sample_interval_seconds INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v33 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 34)
        {
            /* v34: query_snapshots gains the wait-drilldown triage columns Dashboard collects via
               sp_WhoIsActive — memory-grant requested/used/max-used (MB), tempdb current/allocations
               (MB), transaction log used (MB) + transaction start time, and request_id. All appended
               at the end to keep the positional appender aligned; the v_ view (SELECT *) surfaces
               them and old parquet reads back NULL (union BY NAME). The snapshot collector writes
               these columns, so an un-migrated DB would mis-align — these ALTERs are required. */
            _logger?.LogInformation("Running migration to v34: query_snapshots memory-grant/tempdb/transaction columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS requested_memory_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS used_memory_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS max_used_memory_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tempdb_current_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tempdb_allocations_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tran_log_used_mb DOUBLE PRECISION");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS tran_start_time TIMESTAMP");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_snapshots ADD COLUMN IF NOT EXISTS request_id INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v34 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 35)
        {
            /* v35: deferred execution-plan capture (#1262) — procedure_stats.query_plan_xml,
               blocked_process_reports.blocked_query_plan_xml + blocking_query_plan_xml, and
               deadlocks.victim_query_plan_xml. All appended at the end to keep the positional appenders
               aligned; the v_ views (SELECT *) surface them and old parquet reads back NULL (union BY
               NAME). The collectors now write these columns unconditionally — always NULL on Lite, which
               never sets CapturePlanXml (Darling-only) — so an un-migrated DB would mis-align on the next
               append; these ALTERs are required. */
            _logger?.LogInformation("Running migration to v35: procedure/blocked-process/deadlock plan columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS query_plan_xml VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocked_query_plan_xml VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocking_query_plan_xml VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS victim_query_plan_xml VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v35 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 36)
        {
            /* v36: server_properties gains sqlserver_start_time / host_os_version / ag_replica_role — the
               three fields the shared ServerPropertiesCollector now SELECTs (previously read only from a
               live query in the FinOps Server Inventory). All appended at the end to keep the positional
               appender aligned; the collector writes them unconditionally, so an un-migrated DB would
               mis-align on the next append — these ALTERs are required. */
            _logger?.LogInformation("Running migration to v36: server_properties start-time / host-OS / AG-role columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS sqlserver_start_time TIMESTAMP");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS host_os_version VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS ag_replica_role VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v36 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 37)
        {
            /* v37: added latch_stats (sys.dm_os_latch_stats) and spinlock_stats
                    (sys.dm_os_spinlock_stats) shared collectors for Dashboard->Darling collection
                    parity. New tables only — created by GetAllTableStatements() below; the v_ views
                    come from CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v37: adding latch_stats and spinlock_stats tables");
        }

        if (fromVersion < 38)
        {
            /* v38: added cpu_scheduler_stats (sys.dm_os_schedulers + workload groups + NUMA nodes +
                    OS memory) and plan_cache_stats (sys.dm_exec_cached_plans) shared collectors for
                    Dashboard->Darling collection parity. New tables only — created by
                    GetAllTableStatements() below; the v_ views come from CreateArchiveViewsAsync via
                    ArchivableTables. */
            _logger?.LogInformation("Running migration to v38: adding cpu_scheduler_stats and plan_cache_stats tables");
        }

        if (fromVersion < 39)
        {
            /* v39: added session_summary_stats (server-wide session SUMMARY from sys.dm_exec_sessions
                    + sys.dm_exec_requests: total/running/sleeping/background/dormant sessions, idle
                    sessions over 30 min, memory-wait count, top application/host) — the Dashboard->
                    Darling connection-leak / idle parity collector. Distinct from the per-application
                    session_stats table. New table only — created by GetAllTableStatements() below; the
                    v_ view comes from CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v39: adding session_summary_stats table");
        }

        if (fromVersion < 40)
        {
            /* v40: added system_health_events (Stage 1 raw system_health Extended Events capture —
                    one row per event, raw XML only, no shredding) for Dashboard->Darling health-parser
                    parity. New table only — created by GetAllTableStatements() below; the v_ view comes
                    from CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v40: adding system_health_events table");
        }

        if (fromVersion < 41)
        {
            /* v41: index_object_stats gains the per-index DEFINITION metadata monitor-side
                    UNUSED/DUPLICATE analysis needs (FinOps Index Analysis, Stage 1): the ordered
                    key_columns / included_columns lists (sp_IndexCleanup's delimited representation),
                    filter_definition, the uniqueness/constraint/FK discriminators + is_disabled, and
                    the reconstruct-a-CREATE options (data_compression_desc, optimize_for_sequential_key,
                    fill_factor, is_padded, allow_page_locks, allow_row_locks). All appended at the end
                    to keep the positional appender aligned; the collector now writes them, so an
                    un-migrated DB would mis-align on the next append — these ALTERs are required. The
                    v_ view (SELECT *) surfaces them and old parquet reads back NULL (union BY NAME). */
            _logger?.LogInformation("Running migration to v41: adding index_object_stats index-definition columns");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS key_columns VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS included_columns VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS filter_definition VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_unique_constraint BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key_reference BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_disabled BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS data_compression_desc VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS optimize_for_sequential_key BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS fill_factor SMALLINT");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_padded BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_page_locks BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_row_locks BOOLEAN");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_indexed_view BOOLEAN");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v41 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 42)
        {
            /* v42: server_properties gains utc_offset_minutes — the monitored server's UTC offset the
                    shared collector now writes (DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())). Appended at
                    the end to keep the positional appender aligned; the collector now writes it, so an
                    un-migrated DB would mis-align on the next append — this ALTER is required. Nullable
                    (DuckDB has no ADD COLUMN NOT NULL); the offset is a stored fact, not a delta. */
            _logger?.LogInformation("Running migration to v42: adding utc_offset_minutes column to server_properties");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS utc_offset_minutes INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v42 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 43)
        {
            /* v43: added default_trace_events (built-in Default Trace read via sys.fn_trace_gettable —
                    file auto-grow/shrink stalls, severe ErrorLog writes, schema DDL, security audits,
                    Server Memory Change) for Dashboard->shared parity. New table only — created by
                    GetAllTableStatements() below; the v_ view comes from CreateArchiveViewsAsync via
                    ArchivableTables. */
            _logger?.LogInformation("Running migration to v43: adding default_trace_events table");
        }

        if (fromVersion < 44)
        {
            /* v44: added job_history (retained SQL Agent job-run history from msdb.dbo.sysjobhistory —
                    every step row + the job-outcome row, deduped on the monotonic instance_id high-water
                    mark, 365-day retention) for the fleet-wide Job History tab (issue #1433). New table
                    only — created by GetAllTableStatements() below; the v_ view comes from
                    CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v44: adding job_history table");
        }

        if (fromVersion < 45)
        {
            /* v45: added agent_status (SQL Agent service Running/Stopped from sys.dm_server_services +
                    next scheduled run from msdb.dbo.sysjobschedules) — the current-state snapshot behind the
                    Job History tab header (and Darling's "Agent Not Running" alert; Lite has no such alert of
                    its own — issue #1433 Phase 2). New table
                    only — created by GetAllTableStatements() below; the v_ view comes from
                    CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v45: adding agent_status table");
        }

        if (fromVersion < 46)
        {
            /* v46: deadlocks.database_name — the victim process's currentdbname, keying the Azure SQL DB
                    per-database watermark (#1535: capture is now one database-scoped session per monitored
                    database). Appended at the end to keep the positional appender aligned; the collector
                    writes it unconditionally (null when the graph carries no currentdbname), so an
                    un-migrated DB would mis-align on the next append — this ALTER is required. The v_ view
                    (SELECT *) is rebuilt every startup and picks it up; old parquet reads back NULL (union
                    BY NAME). blocked_process_reports already had database_name. */
            _logger?.LogInformation("Running migration to v46: deadlocks database_name column");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS database_name VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v46 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 47)
        {
            /* v47: query_store_stats.replica_role — the replica role SQL Server 2022+ attributed each
                    runtime-stats row to (sys.query_store_replicas.replica_name). With "Query Store for
                    secondary replicas" on, an AG has ONE shared Query Store living on the primary, so the
                    primary's rows silently blend in secondary workload unless split by replica. Appended at
                    the end to keep the positional appender aligned; the collector writes it unconditionally
                    (NULL pre-2022, and NULL on a 2022 standalone whose sys.query_store_replicas is empty),
                    so an un-migrated DB would mis-align on the next append — this ALTER is required. The v_
                    view (SELECT *) is rebuilt every startup and picks it up; old parquet reads back NULL
                    (union BY NAME). */
            _logger?.LogInformation("Running migration to v47: query_store_stats replica_role column");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS replica_role VARCHAR");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v47 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 48)
        {
            /* v48: drop NOT NULL from server_properties.cpu_count / hyperthread_ratio /
                    physical_memory_mb (#1591). Those three are the only columns in the collector
                    sourced from sys.dm_os_sys_info, which needs VIEW SERVER STATE (VIEW DATABASE
                    STATE on Azure SQL DB). The collector now reads them in a TRY/CATCH so a login
                    without that grant keeps every permission-free column instead of losing the whole
                    row — but that only helps if the column can actually hold NULL, so an existing
                    database has to have the constraint dropped. New databases get it from the
                    generator. Column types and ordinals are unchanged, so the positional appender
                    and old parquet are unaffected. */
            _logger?.LogInformation("Running migration to v48: server_properties hardware columns become nullable");

            /* #2748: on any database that has ever completed a prior startup, DuckDbSchemaGenerator.CreateIndex's
               default case already created idx_server_properties_time ON server_properties(server_id,
               collection_time) — a real catalog object persisted in the .duckdb file, surviving a restart.
               DuckDB's ALTER COLUMN dependency check refuses to touch a table with ANY index on it, even one
               that names none of the altered columns — confirmed empirically, not merely by reading the error
               text: "Dependency Error: Cannot alter entry ... because there are entries that depend on it."
               (An archive view on the table does NOT trigger this — only the index does.) Drop the index
               first; Schema.GetAllIndexStatements()'s loop (called unconditionally right after migrations,
               inside this same InitializeAsync) recreates it, so nothing is left dangling. */
            try
            {
                await ExecuteNonQueryAsync(connection, "DROP INDEX IF EXISTS idx_server_properties_time");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v48 could not drop idx_server_properties_time ahead of the ALTERs (non-fatal, the ALTERs below may still fail): {Error}", ex.Message);
            }

            foreach (var column in new[] { "cpu_count", "hyperthread_ratio", "physical_memory_mb" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection, $"ALTER TABLE server_properties ALTER COLUMN {column} DROP NOT NULL");
                }
                catch (Exception ex)
                {
                    /* Already nullable, or the table does not exist yet (fresh install creates it
                       correctly from the generator) — neither is fatal. */
                    _logger?.LogWarning("Migration to v48 on {Column} encountered an error (non-fatal): {Error}", column, ex.Message);
                }
            }
        }

        if (fromVersion < 49)
        {
            /* v49: query_store_stats gains the REAL Query Store interval identity (#1841 tier 2) —
                    runtime_stats_interval_id + interval_start_time_utc. The rows are cumulative
                    per-interval snapshots and the collector re-fetches the OPEN interval every cycle, so
                    every aggregate read has to collapse an interval to its latest snapshot before summing;
                    until now the only identity in the schema was the first_execution_time PROXY, and the
                    only time axis was collection_time (the cycle that last FETCHED an interval, reliably
                    one bucket after the one it ran in on Query Store's default 60-minute interval).

                    Both appended at the end to keep the positional appender aligned; the collector writes
                    them unconditionally, so an un-migrated database would mis-align on the next append —
                    this ALTER is required, not cosmetic. Nullable and NOT backfilled: rows already in the
                    store were collected without the identity and nothing can reconstruct it, so every
                    reader keys on the real id only WHEN PRESENT and falls back to the proxy otherwise. The
                    v_ view (SELECT *) is rebuilt every startup and picks them up; old parquet reads back
                    NULL (union BY NAME). */
            _logger?.LogInformation("Running migration to v49: query_store_stats interval identity columns");
            foreach (var column in new[] { "runtime_stats_interval_id BIGINT", "interval_start_time_utc TIMESTAMP" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection, $"ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS {column}");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v49 on {Column} encountered an error (non-fatal): {Error}", column, ex.Message);
                }
            }
        }

        if (fromVersion < 50)
        {
            /* v50: added plan_correction (automatic plan correction — per-database FORCE_LAST_GOOD_PLAN
                    enablement from sys.database_automatic_tuning_options, plus the engine's live
                    recommendation set from sys.dm_db_tuning_recommendations with the regressed query's
                    text resolved through Query Store at collection time; issue #1952). New table only —
                    created by GetAllTableStatements() below; the v_ view comes from
                    CreateArchiveViewsAsync via ArchivableTables. */
            _logger?.LogInformation("Running migration to v50: adding plan_correction table");
        }

        if (fromVersion < 51)
        {
            /* v51: added pvs_stats (Accelerated Database Recovery persistent version store size and
                    cleanup state per database from sys.dm_tran_persistent_version_store_stats, SQL
                    Server 2019+; issue #1951). New table only — created by GetAllTableStatements()
                    below; the v_pvs_stats view the FinOps grid reads comes from CreateArchiveViewsAsync
                    via ArchivableTables, which is derived from CollectorCatalog. */
            _logger?.LogInformation("Running migration to v51: adding pvs_stats table");
        }

        if (fromVersion < 52)
        {
            /* v52 (#2012 stage 2): the statement's HOST OBJECT on query_stats — dm_exec_sql_text.objectid
               resolved to schema.name at collection, NULL for ad-hoc/prepared text. Splits INSERT...EXEC
               callers that share a query_hash in the hash-grouped readers; history rows stay NULL and age
               out with retention. Appended last to match the collector's append-only payload; the
               v_query_stats archive union re-derives per start with union_by_name, so no view work. */
            _logger?.LogInformation("Running migration to v52: adding host_object_name to query_stats");
            try
            {
                await ExecuteNonQueryAsync(connection, "ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS host_object_name VARCHAR");
            }
            catch
            {
                /* Table doesn't exist yet — will be created with the full schema below */
            }
        }

        if (fromVersion < 53)
        {
            /* v53 (#2203): the database-state alert's edge-trigger memory, porting Darling's V60 pair.
               Without it Lite's alreadyAnnounced is always false, so a database parked OFFLINE for a month
               alerts every cooldown forever - the original #2166 complaint, still live in Lite after the
               Darling half shipped. Nullable on purpose: NULL means "never announced", which is what a
               first observation, a fresh store and a recovered database all look like. */
            _logger?.LogInformation("Running migration to v53: adding the alerted-state memory to config_database_state_expected");
            try
            {
                /* #2748: config_database_state_expected itself was never given its own numbered migration —
                   it only exists because Schema.GetAllTableStatements() unconditionally CREATE TABLE IF NOT
                   EXISTS-es it, which does not run until AFTER RunMigrationsAsync returns. A database old
                   enough to predate the table (upgrading through v53 for the first time) hits this ALTER
                   before that later step ever creates it. CreateDatabaseStateExpectedTable is itself
                   idempotent, so calling it here is a no-op for anyone who already has the table (from a
                   prior run) and a correct fresh create — new columns included — for anyone who does not. */
                await ExecuteNonQueryAsync(connection, Schema.CreateDatabaseStateExpectedTable);
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_database_state_expected ADD COLUMN IF NOT EXISTS last_alerted_state VARCHAR");
                await ExecuteNonQueryAsync(connection, "ALTER TABLE config_database_state_expected ADD COLUMN IF NOT EXISTS last_alerted_at TIMESTAMP");
            }
            catch (Exception ex)
            {
                /* The CREATE above means "table doesn't exist yet" can no longer be the cause — a catch here
                   now means something else went wrong. Log it rather than swallow it silently, matching every
                   sibling migration block; this whole PR exists because a silently-swallowed failure here is
                   exactly what left #2748's database unfixed for two releases. */
                _logger?.LogWarning("Migration to v53 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 54)
        {
            /* v54 (#2216): the per-fingerprint occurrence counters, porting Darling's V61. The count on an
               alert incident is a rolling-window gauge, so a consumer receiving only throttled deliveries
               cannot recover the true total from a sequence of readings. New table only — fresh installs
               get it from GetAllTableStatements(); this CREATE is for an existing database, and it is
               idempotent so a re-run is a no-op. Nothing to backfill: an absent row means "no incident in
               flight for this fingerprint", which is what every fingerprint looks like before the feature
               existed, so the first delivery after the upgrade opens an incident and counts from there. */
            _logger?.LogInformation("Running migration to v54: adding config_incident_occurrences");
            try
            {
                await ExecuteNonQueryAsync(connection, Schema.CreateIncidentOccurrencesTable);
            }
            catch (Exception ex)
            {
                /* Non-fatal, matching v31's posture: without the table the store's load returns empty and
                   the accumulator degrades to reporting the total as the window count — the pre-#2216
                   information rather than a broken alert path. */
                _logger?.LogWarning("Migration to v54 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 55)
        {
            /* v55 (#2472): the per-database fan-out rollup on collection_log, twinning Darling's V80. One
               collector run that fans out over N databases writes ONE row whose duration_ms is the sum, so
               "eight databases at 10.1s" and "one at 62s beside seven at 2.7s" are the same number and want
               opposite fixes. These three carry the ratio that separates them.

               Fresh installs get the columns from GetAllTableStatements(); this is for an existing database
               and is idempotent. Nothing to backfill and nothing that COULD be: a row written before the
               upgrade genuinely does not know its fan-out, so NULL is the honest value rather than a zero
               that would read as "fanned out over nothing".

               Non-fatal, matching the blocks above: without the columns the writer's INSERT would fail and
               take collection logging with it, so a failed ADD COLUMN must not also break the run — the
               write path treats a log failure as non-fatal already. */
            _logger?.LogInformation("Running migration to v55: adding collection_log fan-out rollup columns");
            try
            {
                await ExecuteNonQueryAsync(connection,
                    "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS fanout_item_count INTEGER");
                await ExecuteNonQueryAsync(connection,
                    "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS slowest_item VARCHAR");
                await ExecuteNonQueryAsync(connection,
                    "ALTER TABLE collection_log ADD COLUMN IF NOT EXISTS slowest_item_ms INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v55 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 56)
        {
            /* v56 (#2515): tempdb's growth CEILING on tempdb_stats, twinning Darling's V81. Every other
               column here comes from dm_db_file_space_usage, which reports the data files AS CURRENTLY
               ALLOCATED — so the tempdb Space alert's percentage measured distance to the next AUTOGROW
               rather than distance to the point where tempdb cannot grow at all. SUM(max_size) over the
               ROWS files is the denominator that means the same thing on every engine.

               Fresh installs get the column from GetAllTableStatements(); this is for an existing database
               and is idempotent. Nothing to backfill: a row collected before the upgrade genuinely does not
               know the ceiling, and NULL says so — the read maps it to 0, which is the "no ceiling measured"
               state where the denominator stays the allocation and the reported percentage does not move.

               The v_tempdb_stats view needs no work here: Lite rebuilds every v_ passthrough on start
               (CreateArchiveViewsAsync, called after this), which is the difference from Darling, where the
               view's SELECT * column list is frozen at CREATE and the rung has to refresh it. */
            _logger?.LogInformation("Running migration to v56: adding max_size_mb to tempdb_stats");
            try
            {
                await ExecuteNonQueryAsync(connection,
                    "ALTER TABLE tempdb_stats ADD COLUMN IF NOT EXISTS max_size_mb DECIMAL(18,2)");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v56 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 57)
        {
            /* v57: drop NOT NULL from database_size_stats.database_id / file_id / physical_name
                    (#3262). The Azure sibling arm (#2643) reads sys.resource_stats, which has
                    per-DATABASE sizes and no per-file breakdown, so a sibling row deliberately
                    carries NULL in all three — and the reader now passes those NULLs through
                    instead of dying on the cast. An existing database has to have the constraint
                    dropped or the appender fails the first sibling row and the whole batch with
                    it. New databases get it from the generator; Darling's Postgres store was
                    always nullable here. Column types and ordinals are unchanged, so the
                    positional appender and old parquet are unaffected. */
            _logger?.LogInformation("Running migration to v57: database_size_stats sibling-row columns become nullable");

            /* Same trap as v48 (#2748): DuckDB's ALTER COLUMN refuses on a table with ANY index,
               even one naming none of the altered columns. Drop it first;
               Schema.GetAllIndexStatements()'s loop (called unconditionally right after
               migrations, inside this same InitializeAsync) recreates it. */
            try
            {
                await ExecuteNonQueryAsync(connection, "DROP INDEX IF EXISTS idx_database_size_stats_time");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v57 could not drop idx_database_size_stats_time ahead of the ALTERs (non-fatal, the ALTERs below may still fail): {Error}", ex.Message);
            }

            foreach (var column in new[] { "database_id", "file_id", "physical_name" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection, $"ALTER TABLE database_size_stats ALTER COLUMN {column} DROP NOT NULL");
                }
                catch (Exception ex)
                {
                    /* Already nullable, or the table does not exist yet (fresh install creates it
                       correctly from the generator) — neither is fatal. */
                    _logger?.LogWarning("Migration to v57 on {Column} encountered an error (non-fatal): {Error}", column, ex.Message);
                }
            }
        }

        if (fromVersion < 58)
        {
            /* v58 (#3282): the built-in alert catalog's persistence-gate state, porting Darling's V118.
               Before this no built-in alert required its condition to PERSIST — one sample over the bar
               fired and the next sample under it resolved — so a momentary CPU spike was indistinguishable
               from sustained saturation. New table only; fresh installs get it from
               GetAllTableStatements() and this CREATE is for an existing database, idempotent so a re-run
               is a no-op.

               Nothing to backfill, and nothing that could be: an absent row means "no streak and no open
               incident", which is exactly what every server looked like before the gate existed. The first
               few sweeps after the upgrade build the streak, so the first post-upgrade High CPU arrives
               once the condition has actually held.

               Non-fatal, matching v54's posture: without the table the load returns null and the gate
               lives for one process lifetime — a restart re-arms it from zero rather than breaking the
               alert path. */
            _logger?.LogInformation("Running migration to v58: adding config_alert_persistence_state");
            try
            {
                await ExecuteNonQueryAsync(connection, Schema.CreateAlertPersistenceStateTable);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v58 encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 59)
        {
            /* v59 (#3392): query_stats and procedure_stats gain query_plan_xml_bytes — the measured
               DATALENGTH of the row's cached-plan XML, selected beside the plan itself on the hosts that
               capture plans. Appended at the end of both PayloadColumns lists, so the positional appender
               and old parquet are unaffected.

               Lite never sets CapturePlanXml, so this column is always NULL here and the query text Lite
               sends is unchanged. It is still REQUIRED on this side: the appender writes one value per
               declared payload column, so a database without the column fails EndRow() on the first
               query_stats or procedure_stats batch — the whole batch, not the column. Fresh installs get
               it from DuckDbSchemaGenerator; this ALTER is for an existing database and is idempotent.

               Non-fatal per column, matching v35's posture — the plan-column rung this mirrors. */
            _logger?.LogInformation("Running migration to v59: query_stats + procedure_stats gain query_plan_xml_bytes");

            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection,
                        $"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS query_plan_xml_bytes BIGINT");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v59 on {Table} encountered an error (non-fatal): {Error}", table, ex.Message);
                }
            }
        }

        if (fromVersion < 60)
        {
            /* v60 (#3540): wait_stats, file_io_stats, latch_stats and spinlock_stats gain sample_interval_seconds
               — the measured seconds each row's deltas accrued over, twinning Darling's V127. The shared delta
               calculator reports (delta 0, interval 0) when no delta is knowable (first sighting, counter
               reset, a gap past the 3600 s policy) and (0, n) when the interval was genuinely idle, and the
               interval is the ONLY thing that tells those apart. These four collectors discarded it at the
               write, so a restart's fabricated zero survived as a measured one and every per-second reader
               LAG-divided it into a confident 0.00 ms/sec. perfmon_stats and query_stats have carried the
               column from the start; this gives the other four the same column in the same type.

               Appended at the end of each PayloadColumns list, so the positional appender and old parquet
               are unaffected. Nothing to backfill and nothing that COULD be: a row collected before the
               upgrade never recorded its interval, so NULL is the honest value — the readers treat NULL as
               "pre-v60, derive the interval from the previous collection_time" (exactly what they always
               did) and 0 as "unknowable, render nothing". A backfilled 0 would stamp all of history as
               unknowable and blank every rate chart for 30 days.

               REQUIRED on this side even though Lite collects the same DMVs Darling does: the appender writes
               one value per declared payload column, so a database without the column fails EndRow() on the
               first batch of any of these four collectors — the whole batch, not the column. Fresh installs
               get it from DuckDbSchemaGenerator; this ALTER is for an existing database and is idempotent.
               The v_ passthrough views need no work here: Lite rebuilds every v_ view on start
               (CreateArchiveViewsAsync, called after this), which is the difference from Darling, where the
               view's SELECT * column list is frozen at CREATE and the rung has to refresh it.

               Non-fatal per table, matching v59's posture. */
            _logger?.LogInformation("Running migration to v60: the four naked delta families gain sample_interval_seconds");

            foreach (var table in new[] { "wait_stats", "file_io_stats", "latch_stats", "spinlock_stats" })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection,
                        $"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sample_interval_seconds INTEGER");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v60 on {Table} encountered an error (non-fatal): {Error}", table, ex.Message);
                }
            }
        }

        if (fromVersion < 61)
        {
            /* v61 (#3540): the completion of v60, twinning Darling's V128. procedure_stats and
               memory_grant_stats gain sample_interval_seconds — the measured seconds each row's deltas
               accrued over — so EVERY delta family Lite stores now carries the interval, and the shared
               calculator's (delta 0, interval 0) "no delta knowable" marker survives every family's write.
               These two took the calculator's bare long and discarded the interval, so a restart's
               fabricated zero read as a measured idle one: the procedure duration trend LAG-divided it into
               a confident 0.00 ms/sec, and a memory-grant row's 0 timeouts over a restart read as a quiet
               semaphore. Darling's V128 also dresses pg_wait_stats and pg_statement_stats; Lite stores no
               pg_* tables (DuckDbSchemaGenerator.StoredCollectors), so this side has only the two.

               query_stats gains statement_start_offset and statement_end_offset — the two halves of its
               delta key (sql_handle:start:end:plan_handle) the store never persisted, so the restart seed
               could restore this family's pass window but not one baseline (#3614 named it as the residual).
               Their semantics, stated here because this is where the next reader will look: they are
               sys.dm_exec_query_stats' own columns, the statement's position inside its batch text in BYTES
               of the nvarchar text, not characters — a character slice divides by two, which is the
               collector's SUBSTRING(st.text, (statement_start_offset / 2) + 1, ...) — and
               statement_end_offset = -1 means "to the end of the batch" ((0, -1) is the whole batch). They
               are stored VERBATIM as the DMV reports them, -1 included and never normalized to a length,
               because the collector's key is built over the raw ints and the seed has to spell the same
               string byte for byte (DeltaCalculator.QueryStatsSeedSql).

               All three appended at the end of their PayloadColumns lists, so the positional appender and
               old parquet are unaffected. Nothing to backfill and nothing that COULD be: a row collected
               before the upgrade never recorded its interval or its offsets, so NULL is the honest value —
               the readers treat a NULL interval as "pre-v61, derive from the previous collection_time"
               (exactly what they always did) and 0 as "unknowable, render nothing"; the seed treats NULL
               offsets as "no key can be rebuilt from this row" and still takes its collection time for the
               pass window. A backfilled 0 interval would stamp all of history unknowable and blank every
               procedure rate chart for 30 days; a backfilled 0/-1 offset pair would seed baselines under a
               key nothing will ever present.

               REQUIRED on this side for the v60 reason: the appender writes one value per declared payload
               column, so a database without these columns fails EndRow() on the first batch of either
               collector — the whole batch, not the column. Fresh installs get them from
               DuckDbSchemaGenerator; these ALTERs are for an existing database and are idempotent. The v_
               passthrough views need no work here: Lite rebuilds every v_ view on start
               (CreateArchiveViewsAsync, called after this). Non-fatal per statement, matching v59/v60. */
            _logger?.LogInformation("Running migration to v61: every delta family stores its interval, and query_stats stores the statement offsets its delta key is made of");

            foreach (var (table, column) in new[]
            {
                ("procedure_stats", "sample_interval_seconds"),
                ("memory_grant_stats", "sample_interval_seconds"),
                ("query_stats", "statement_start_offset"),
                ("query_stats", "statement_end_offset"),
            })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection,
                        $"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} INTEGER");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v61 on {Table}.{Column} encountered an error (non-fatal): {Error}", table, column, ex.Message);
                }
            }
        }

        if (fromVersion < 62)
        {
            /* v62 (#3653 A7): perfmon_stats gains cntr_type — the Windows performance-counter type id
               sys.dm_os_performance_counters reports for every row, twinning Darling's V132. Until now the
               store could not say which of its perfmon rows were COUNTS and which were LEVELS, so the
               collector differenced every counter it read: Total Server Memory (KB) exactly like Batch
               Requests/sec, and a target that released memory — a falling level — read to the shared delta
               calculator as a counter reset, storing the (0, 0) "no delta knowable" marker at exactly the
               moment the drop mattered. With the type stored the collector writes a gauge (65792,
               PERF_COUNTER_LARGE_RAWCOUNT) as its raw value with NULL delta and NULL interval — NULL, not
               (0, 0), because 0 is a claim about a delta and a gauge has none — and a rate (272696576,
               PERF_COUNTER_BULK_COUNT) exactly as before; the readers classify by the stored type through
               PerformanceMonitor.Common.PerfmonCounterTypes and fall back to #3702's name-suffix proxy only
               for a NULL type.

               Appended at the end of the PayloadColumns list, so the positional appender and old parquet are
               unaffected. Nothing to backfill and nothing that COULD be: a row collected before the upgrade
               never recorded its type, the store holds no way to recover one from a name, and NULL is the
               honest value — the readers treat it as "classify as you did before the rung", so history
               renders as the operator last saw it. Because a counter's type does not change, the viewers take
               any row's non-null type as the series' type, so a gauge's whole history plots as a level the
               morning after the upgrade (its pre-rung rows stored cntr_value all along).

               REQUIRED on this side for the v60 reason: the appender writes one value per declared payload
               column, so a database without the column fails EndRow() on the first perfmon batch — the
               whole batch, not the column. Fresh installs get it from DuckDbSchemaGenerator; this ALTER is
               for an existing database and is idempotent. The v_ passthrough view needs no work here: Lite
               rebuilds every v_ view on start (CreateArchiveViewsAsync, called after this). Non-fatal,
               matching v59/v60/v61. */
            _logger?.LogInformation("Running migration to v62: perfmon_stats stores each counter's type, so gauges are no longer differenced");

            try
            {
                await ExecuteNonQueryAsync(connection,
                    "ALTER TABLE perfmon_stats ADD COLUMN IF NOT EXISTS cntr_type INTEGER");
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Migration to v62 on perfmon_stats.cntr_type encountered an error (non-fatal): {Error}", ex.Message);
            }
        }

        if (fromVersion < 63)
        {
            /* v63 (#3653 item 13, rulings Q7 + Q8 — the "time honesty" rung, twinning Darling's V134): two
               nullable columns that let the store say WHICH clock two of its values are in.

               cpu_utilization_stats gains sample_time_utc. sample_time is, and stays, the MONITORED SERVER'S
               LOCAL wall clock (SYSDATETIME() minus each ring-buffer entry's age) — the one column in this
               store that is not naive UTC, by a documented convention: the CPU chart plots it in the
               server's own frame, and it is the collector's watermark. Every read that windows it against a
               UTC instant therefore had to DERIVE the UTC value, and this side derived it by shifting the
               whole window by the single utc_offset_minutes the store holds NOW (GetTimeRangeServerLocal):
               exact while every sample and the collected offset sit on the same side of a DST transition,
               an hour wrong for every sample on the far side, silently and in the plausible direction. The
               collector now writes sample_time_utc beside it — the SAME instant, the same DATEADD arithmetic
               anchored on SYSUTCDATETIME() (on Azure SQL DB it is end_time, which is UTC and which
               sample_time already reads, because Azure's clock IS UTC) — and GetCpuUtilizationAsync windows
               on COALESCE(sample_time_utc, sample_time - the offset) against UTC bounds, so a post-rung row
               is selected by a stored UTC instant and a pre-rung row exactly as before. The projected value
               stays sample_time: the chart wants the server's frame, and the local stamp IS that frame with
               no offset applied at all.

               server_properties gains time_zone_id beside utc_offset_minutes (v42). The offset is
               DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) — the one IN FORCE at collection — and #3231
               documented that subtracting it from a stored instant that can outlive a DST transition
               (get_index_usage's last_user_access, the PVS cleaner stamps) is exact on one side and an hour
               wrong on the other, with nothing in the store able to say which. A ZONE can, because AT TIME
               ZONE applies the rule in force at the instant. CURRENT_TIMEZONE_ID() exists on SQL Server
               2022+ and Azure SQL only, and on an older engine it is a missing BUILT-IN rather than a missing
               object — the batch fails to compile — so the collector reads it in its own gated sp_executesql
               batch inside TRY/CATCH and writes NULL where the engine cannot say. NULL is a real and common
               value: "pre-2022 engine, only the offset is known", and get_server_properties says so. The
               offset stays alongside rather than being replaced, because every de-skew in the tree still
               runs on it and the zone is NULL on most of today's fleet.

               Both appended at the end of their PayloadColumns lists, so the positional appender and old
               parquet are unaffected (v_ views UNION ALL BY NAME the parquet with union_by_name, so an
               archived file without the column reads NULL through the view). Nothing to backfill and nothing
               that COULD be: a pre-rung sample_time could be converted only by the offset its server had AT
               THAT INSTANT, which is exactly what the store never recorded — the collected offset and the
               derivation are the DST-blind instruments this column replaces, so a backfilled value would be
               the old lie written into the new column where a reader could no longer tell it from a
               measurement; and no zone can be recovered from an offset, since several zones share every one.

               REQUIRED on this side for the v60 reason: the appender writes one value per declared payload
               column, so a database without the column fails EndRow() on the first CPU batch and the first
               server_properties batch — the whole batch, not the column. Fresh installs get both from
               DuckDbSchemaGenerator; these ALTERs are for an existing database and are idempotent. The v_
               passthrough views need no work here: Lite rebuilds every v_ view on start
               (CreateArchiveViewsAsync, called after this). Non-fatal per statement, matching v59–v62. */
            _logger?.LogInformation("Running migration to v63: cpu_utilization_stats stores each sample's UTC instant beside the server-local one, and server_properties stores the engine's time-zone id beside its offset");

            foreach (var (table, column, type) in new[]
            {
                ("cpu_utilization_stats", "sample_time_utc", "TIMESTAMP"),
                ("server_properties", "time_zone_id", "VARCHAR"),
            })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection,
                        $"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {type}");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v63 on {Table}.{Column} encountered an error (non-fatal): {Error}", table, column, ex.Message);
                }
            }
        }

        if (fromVersion < 64)
        {
            /* v64 (#3796, twinning Darling's V137): query_store_health gains the two Query Store CAPTURE modes,
               query_capture_mode and wait_stats_capture_mode — the sys.database_query_store_options columns
               the shared collector read around. The health row stored every option that says whether Query
               Store WORKS (actual vs desired state, the cap, the cleanup thresholds) and none that says what it
               CAPTURES, and QUERY_CAPTURE_MODE = ALL on an ad-hoc workload is the one setting that turns Query
               Store into a plan-churn factory: measured on one production store class, ~755 k new distinct
               plans a day from 42 servers, with the other three knobs uniform across the fleet. The row could
               not say whether ALL was the cause because it never asked. Both are stored as the DMV's *_desc
               spelling verbatim (ALL / AUTO / CUSTOM / NONE; ON / OFF).

               ONE of the two is version-gated on the collector side, and this store must expect the NULL it
               produces: query_capture_mode_desc shipped with the view in 2016 and is always selected, but
               wait_stats_capture_mode_desc arrived in SQL Server 2017 (v14), and on a 2016 engine a body that
               names it fails to compile for the whole database. QueryStoreHealthCollector.HasWaitStatsCaptureMode
               (the DatabaseConfigCollector idiom) leaves the column out of the SELECT there and the row stores
               NULL, which a reader publishes as "engine predates the option" — never as OFF. NULL on every
               pre-v64 row means "never asked", for both columns.

               Appended at the end of the PayloadColumns list, so the positional appender and old parquet are
               unaffected (v_ views UNION ALL BY NAME the parquet with union_by_name, so an archived file
               without the columns reads NULL through the view). Nothing to backfill and nothing that COULD
               be: a row collected before the upgrade never asked the engine, and the store holds no way to
               recover a capture mode from the other nine options.

               REQUIRED on this side for the v60 reason: the appender writes one value per declared payload
               column, so a database without the columns fails EndRow() on the first query_store_health batch
               — the whole batch, not the column. Fresh installs get both from DuckDbSchemaGenerator; these
               ALTERs are for an existing database and are idempotent. The v_ passthrough view needs no work
               here: Lite rebuilds every v_ view on start (CreateArchiveViewsAsync, called after this).
               Non-fatal per statement, matching v59–v63. Nothing on this side reads either column yet; the
               Query Store clutter view (#3797) is the consumer. */
            _logger?.LogInformation("Running migration to v64: query_store_health stores each database's Query Store capture modes, so plan churn can be told apart from configuration");

            foreach (var (table, column, type) in new[]
            {
                ("query_store_health", "query_capture_mode", "VARCHAR"),
                ("query_store_health", "wait_stats_capture_mode", "VARCHAR"),
            })
            {
                try
                {
                    await ExecuteNonQueryAsync(connection,
                        $"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {type}");
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning("Migration to v64 on {Table}.{Column} encountered an error (non-fatal): {Error}", table, column, ex.Message);
                }
            }
        }
    }

    /// <summary>
    /// Fixes server_id values in all tables by recomputing from server_name using the
    /// deterministic hash function. Previous versions used string.GetHashCode() which
    /// is randomized per process in .NET Core.
    /// </summary>
    private async Task FixServerIdsAsync(DuckDBConnection connection)
    {
        var tablesWithServerId = new[]
        {
            "servers", "collection_log", "wait_stats", "query_stats", "cpu_utilization_stats",
            "file_io_stats", "memory_stats", "memory_clerks", "memory_pressure_events",
            "deadlocks", "procedure_stats", "query_store_stats", "query_snapshots",
            "tempdb_stats", "perfmon_stats", "server_config", "database_config",
            "blocked_process_reports", "memory_grant_stats", "waiting_tasks"
        };

        foreach (var table in tablesWithServerId)
        {
            try
            {
                /* Get distinct server_name values from this table */
                using var queryCmd = connection.CreateCommand();
                queryCmd.CommandText = $"SELECT DISTINCT server_name FROM {table}";
                var serverNames = new List<string>();
                using (var reader = await queryCmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        if (!reader.IsDBNull(0))
                            serverNames.Add(reader.GetString(0));
                    }
                }

                /* Update server_id for each server_name */
                foreach (var serverName in serverNames)
                {
                    var newId = Services.RemoteCollectorService.GetDeterministicHashCode(serverName);
                    using var updateCmd = connection.CreateCommand();
                    updateCmd.CommandText = $"UPDATE {table} SET server_id = $1 WHERE server_name = $2";
                    updateCmd.Parameters.Add(new DuckDBParameter { Value = newId });
                    updateCmd.Parameters.Add(new DuckDBParameter { Value = serverName });
                    await updateCmd.ExecuteNonQueryAsync();
                }

                if (serverNames.Count > 0)
                    _logger?.LogInformation("Fixed server_id in {Table} for {Count} server(s)", table, serverNames.Count);
            }
            catch (Exception ex)
            {
                /* Table might not exist yet — that's fine, it will be created with correct IDs */
                _logger?.LogDebug(ex, "Skipped server_id fix for {Table} (may not exist yet)", table);
            }
        }
    }

    /// <summary>
    /// Creates a new connection to the database.
    /// </summary>
    public DuckDBConnection CreateConnection()
    {
        return new DuckDBConnection(ConnectionString);
    }

    /// <summary>
    /// Creates or refreshes views that UNION hot DuckDB tables with archived parquet files.
    /// Call at startup and after each archive cycle so newly archived data is queryable.
    ///
    /// <para>Takes the READ lock (#4262 round 1 finding 3). <c>ArchiveService</c> and
    /// <c>DataImportService</c> call this with no lock of their own, and now that the sentinel pins one
    /// native handle open per connection string, a connection this method opened could outlive a
    /// concurrent <see cref="ResetDatabaseAsync"/> — keeping the pre-reset instance alive so the reopened
    /// sentinel attached to the OLD handle instead of the fresh file. The read lock makes
    /// <see cref="ResetDatabaseAsync"/>'s write lock wait for this connection to close first, same as
    /// every other reader. A caller that already holds the write lock (<see cref="InitializeCoreAsync"/>,
    /// and <c>QueryStoreSliceRepairService.PromoteRewrittenFileAsync</c>) must call
    /// <see cref="CreateArchiveViewsCoreAsync"/> directly instead — <see cref="s_dbLock"/> is
    /// <see cref="LockRecursionPolicy.NoRecursion"/>, so entering the read lock here would throw for
    /// them; <see cref="AcquireReadLock()"/>'s recursion catch backstops any caller that does not.</para>
    /// </summary>
    public async Task CreateArchiveViewsAsync()
    {
        using var readLock = AcquireReadLock();
        await CreateArchiveViewsCoreAsync();
    }

    /// <summary>
    /// The lock-free body of <see cref="CreateArchiveViewsAsync"/> (#4262 round 1 finding 3), split out so
    /// a caller that already holds the write lock can call it directly rather than nesting a read lock —
    /// <see cref="InitializeCoreAsync"/> (already under the write lock via <see cref="InitializeAsync"/> or
    /// <see cref="ResetDatabaseAsync"/>) and <c>QueryStoreSliceRepairService.PromoteRewrittenFileAsync</c>
    /// (already under the write lock its caller takes for the file swap) both do.
    /// </summary>
    internal async Task CreateArchiveViewsCoreAsync()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync();

        /* This fresh connection must see every table InitializeAsync just created. If it sees none,
           the reset left an empty database — surface it loudly rather than only failing per-table below. */
        using (var tableCountCmd = connection.CreateCommand())
        {
            tableCountCmd.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'";
            var tableCount = Convert.ToInt64(await tableCountCmd.ExecuteScalarAsync());
            if (tableCount == 0)
                _logger?.LogError("Archive-view refresh opened a database with no tables — the reset did not persist the schema; collectors will fail until restart");
            else
                _logger?.LogInformation("Archive-view refresh sees {Count} tables", tableCount);
        }

        foreach (var table in ArchivableTables)
        {
            try
            {
                var parquetGlob = Path.Combine(_archivePath, $"*_{table}.parquet");
                var hasParquetFiles = Directory.Exists(_archivePath)
                    && Directory.GetFiles(_archivePath, $"*_{table}.parquet").Length > 0;

                string viewSql;
                if (hasParquetFiles)
                {
                    var globPath = EscapeSqlPath(parquetGlob.Replace("\\", "/"));
                    if (table == "config_alert_log")
                    {
                        viewSql = $@"CREATE OR REPLACE VIEW v_{table} AS
SELECT *, 'live' AS source FROM {table}
UNION ALL BY NAME
SELECT *, 'archive' AS source FROM read_parquet('{globPath}', union_by_name=true) p
WHERE NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts d
    WHERE d.alert_time = p.alert_time
    AND   d.server_id  = p.server_id
    AND   d.metric_name = p.metric_name
)";
                    }
                    else if (ArchiveViewDedupKeys.TryGetValue(table, out var dedupKey))
                    {
                        /* Dedup the hot∪parquet union on the server-side natural key so a logical event that was
                           re-collected after the 512MB emergency reset (still present in parquet) appears exactly
                           once. QUALIFY keeps the newest-collected copy — the re-collected hot row outranks its
                           archived parquet twin (identical content either way). */
                        viewSql = $@"CREATE OR REPLACE VIEW v_{table} AS
SELECT *
FROM
(
    SELECT * FROM {table}
    UNION ALL BY NAME
    SELECT * FROM read_parquet('{globPath}', union_by_name=true)
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY {dedupKey} ORDER BY collection_time DESC) = 1";
                    }
                    else
                    {
                        viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table} UNION ALL BY NAME SELECT * FROM read_parquet('{globPath}', union_by_name=true)";
                    }
                }
                else
                {
                    if (table == "config_alert_log")
                        viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT *, 'live' AS source FROM {table}";
                    else
                        viewSql = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table}";
                }

                using var cmd = connection.CreateCommand();
                cmd.CommandText = viewSql;
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                /* Schema mismatch between hot table and old parquet — fall back to table-only view */
                _logger?.LogWarning(ex, "Failed to create archive view for {Table}, using table-only view", table);
                try
                {
                    using var fallbackCmd = connection.CreateCommand();
                    if (table == "config_alert_log")
                        fallbackCmd.CommandText = $"CREATE OR REPLACE VIEW v_{table} AS SELECT *, 'live' AS source FROM {table}";
                    else
                        fallbackCmd.CommandText = $"CREATE OR REPLACE VIEW v_{table} AS SELECT * FROM {table}";
                    await fallbackCmd.ExecuteNonQueryAsync();
                }
                catch (Exception fallbackEx)
                {
                    _logger?.LogError(fallbackEx, "Failed to create fallback view for {Table}", table);
                }
            }
        }

        _logger?.LogDebug("Archive views created/refreshed for {Count} tables", ArchivableTables.Length);
    }

    /// <summary>
    /// Initializes the analysis engine schema (separate version track from main schema).
    /// Only called when App.AnalysisEnabled is true.
    /// Internal for test access.
    /// </summary>
    internal async Task InitializeAnalysisSchemaAsync()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync();

        await ExecuteNonQueryAsync(connection,
            "CREATE TABLE IF NOT EXISTS analysis_schema_version (version INTEGER NOT NULL)");

        var existingVersion = 0;
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COALESCE(MAX(version), 0) FROM analysis_schema_version";
            var result = await cmd.ExecuteScalarAsync();
            existingVersion = Convert.ToInt32(result);
        }
        catch { /* Table doesn't exist yet */ }

        foreach (var tableStatement in AnalysisSchema.GetAllTableStatements())
        {
            await ExecuteNonQueryAsync(connection, tableStatement);
        }

        foreach (var indexStatement in AnalysisSchema.GetAllIndexStatements())
        {
            await ExecuteNonQueryAsync(connection, indexStatement);
        }

        if (existingVersion < AnalysisSchema.CurrentVersion)
        {
            // Run migrations for version upgrades
            foreach (var migration in AnalysisSchema.GetMigrationStatements(existingVersion))
            {
                try { await ExecuteNonQueryAsync(connection, migration); }
                catch { /* Column/table may already exist */ }
            }

            await ExecuteNonQueryAsync(connection, "DELETE FROM analysis_schema_version");
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "INSERT INTO analysis_schema_version (version) VALUES ($1)";
            cmd.Parameters.Add(new DuckDBParameter { Value = AnalysisSchema.CurrentVersion });
            await cmd.ExecuteNonQueryAsync();
            _logger?.LogInformation("Analysis schema initialized at version {Version}", AnalysisSchema.CurrentVersion);
        }
    }

    /// <summary>
    /// Executes a non-query SQL statement.
    /// </summary>
    private async Task ExecuteNonQueryAsync(DuckDBConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to execute SQL: {Sql}", sql.Substring(0, Math.Min(100, sql.Length)));
            throw;
        }
    }

    /// <summary>
    /// Checks if the database file exists.
    /// </summary>
    private bool DatabaseExists()
    {
        return File.Exists(_databasePath);
    }

    /// <summary>
    /// Gets the database file size in megabytes.
    /// </summary>
    public double GetDatabaseSizeMb()
    {
        if (!DatabaseExists())
        {
            return 0;
        }

        var fileInfo = new FileInfo(_databasePath);
        return fileInfo.Length / (1024.0 * 1024.0);
    }

    /// <summary>
    /// Gets the actual used data size inside the database by querying pragma_database_size().
    /// Returns null if the query fails (e.g., database busy).
    /// </summary>
    public double? GetUsedDataSizeMb()
    {
        /* Under the read lock (#2594). This was the one periodic connection site that opened without it,
           on the UI's 30-second timer, which put a live handle on the database file at moments the
           archival path may be deleting and recreating it. Bounded rather than blocking - see
           StatusBarReadLockTimeout for why this caller may give up where others may not. */
        using var readLock = TryAcquireReadLock(StatusBarReadLockTimeout);

        if (readLock is null)
        {
            return null;
        }

        try
        {
            using var connection = CreateConnection();
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT (used_blocks * block_size)::BIGINT FROM pragma_database_size()";
            var result = cmd.ExecuteScalar();
            if (result != null && result != DBNull.Value)
            {
                return Convert.ToInt64(result) / (1024.0 * 1024.0);
            }
        }
        catch
        {
            /* Database may be busy — fall back to null */
        }
        return null;
    }

    /// <summary>
    /// Deletes the database and WAL files, then reinitializes with fresh empty tables
    /// and archive views pointing at the parquet files.
    /// Acquires its own write lock — caller must NOT already hold the lock.
    /// </summary>
    public async Task ResetDatabaseAsync()
    {
        using var writeLock = AcquireWriteLock();

        /* Close the sentinel BEFORE deleting the file (#4262). Left open, DuckDB.NET would hand the
           reinitialized database's own connections — and every other caller's CreateConnection() after
           this returns — the SAME cached native handle this held, so the delete below would be invisible
           to them: a "fresh" connection would keep reading the rows this was about to remove. */
        ReleaseSentinel();

        if (File.Exists(_databasePath))
            File.Delete(_databasePath);

        var walPath = _databasePath + ".wal";
        if (File.Exists(walPath))
            File.Delete(walPath);

        _logger?.LogInformation("Database files deleted, reinitializing");

        /* InitializeCoreAsync, not InitializeAsync: this thread already holds the write lock above, and
           InitializeAsync would try to take it again and throw (NoRecursion). */
        await InitializeCoreAsync();

        /* Reopen only once the fresh tables exist and archive views/analysis schema are rebuilt, and
           still under the write lock above — the same ordering InitializeAsync uses. */
        ReopenSentinel();
    }

    /// <summary>
    /// Escapes single quotes in a file path for safe interpolation into DuckDB SQL.
    /// DuckDB does not support parameterized paths in read_parquet() or COPY TO.
    /// </summary>
    internal static string EscapeSqlPath(string path) => path.Replace("'", "''");
}
