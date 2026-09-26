/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4262: <see cref="DuckDbInitializer"/> now keeps one sentinel <see cref="DuckDBConnection"/> open on
/// <c>ConnectionString</c> for the app's life, so every other caller's transient connection attaches to
/// DuckDB.NET's cached native handle instead of paying full open/close cost. That handle is exactly why a
/// live sentinel turned <see cref="DuckDbInitializer.ResetDatabaseAsync"/> into a silent no-op before this
/// fix: deleting the file did not drop the handle a fresh connection would be handed, so the "reinitialized"
/// database kept serving the deleted file's rows. These tests seed that exact precondition — a live
/// sentinel — for every path that deletes, replaces or re-creates the database file, and assert the reset
/// is actually visible afterward.
/// </summary>
/* CollectionResetGate and ArchiveService's own static s_archiveLock are process-wide: a test here calling
   ArchiveAllAndResetAsync races any other test doing the same under xUnit's default cross-class
   parallelism (confirmed — this class flaked against MuteRulesSurviveResetTests before both were tagged
   into the same serialized collection CollectionResetGateTests already defines). */
[Collection("CollectionResetGate")]
public class DuckDbSentinelConnectionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public DuckDbSentinelConnectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>
    /// G1's repro (issuecomment-5836854869): with the sentinel live, seed a row, reset, and a brand-new
    /// connection must see an empty table — not the deleted file's rows served back out of the cached
    /// handle. Fails on the pre-fix shape where <c>ResetDatabaseAsync</c> deletes the file but never closes
    /// the sentinel (confirmed by temporarily no-opping <c>ReleaseSentinel</c> and re-running this test).
    /// </summary>
    [Fact]
    public async Task ResetDatabaseAsync_WithLiveSentinel_NewConnectionSeesEmptyTable()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await SeedCollectionLogRowAsync(initializer, "TestCollector");

        using (var precondition = initializer.CreateConnection())
        {
            await precondition.OpenAsync();
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(precondition, "SELECT COUNT(*) FROM collection_log")));
        }

        await initializer.ResetDatabaseAsync();

        using var afterReset = initializer.CreateConnection();
        await afterReset.OpenAsync();
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(afterReset, "SELECT COUNT(*) FROM collection_log")));
    }

    /// <summary>
    /// The same hazard through the real production path (#4262 ruling item 2): the 512MB emergency reset
    /// runs <c>ArchiveService.ArchiveAllAndResetAsync</c> -&gt; <c>ResetDatabaseAsync</c> on every archive
    /// cycle, not just when a test calls <c>ResetDatabaseAsync</c> directly. A live sentinel must not leave
    /// archived rows sitting in the hot DuckDB table beside their parquet copies.
    /// </summary>
    [Fact]
    public async Task ArchiveAllAndResetAsync_WithLiveSentinel_ClearsHotTableThroughRealPath()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await SeedCollectionLogRowAsync(initializer, "ArchivedCollector");

        var archiveService = new ArchiveService(initializer, _archiveDir);
        await archiveService.ArchiveAllAndResetAsync();

        using var afterReset = initializer.CreateConnection();
        await afterReset.OpenAsync();
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(afterReset, "SELECT COUNT(*) FROM collection_log")));
    }

    /// <summary>
    /// A start with a schema migration pending (#2748's exact v47 precondition, reused here because it is
    /// already proven to migrate cleanly) must still run <c>RunMigrationsAsync</c> to completion, and the
    /// sentinel must open only after — never mid-migration, when the schema is not yet the caller-visible
    /// shape. Proof that a handle is actually live: <see cref="File.Delete(string)"/> alone does NOT
    /// conflict with DuckDB's own share-delete handle (confirmed empirically — the file unlinks
    /// immediately regardless), so this opens with <see cref="FileShare.None"/> instead, which Windows
    /// refuses whenever ANY other handle is open on the file, no matter what sharing that other handle
    /// requested.
    /// </summary>
    [Fact]
    public async Task InitializeAsync_WithPendingSchemaMigration_MigratesThenOpensSentinel()
    {
        using (var seed = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await seed.OpenAsync();
            await ExecAsync(seed, "CREATE TABLE schema_version (version INTEGER NOT NULL)");
            await ExecAsync(seed, "INSERT INTO schema_version VALUES (47)");
            await ExecAsync(seed, @"CREATE TABLE server_properties (
                server_id INTEGER NOT NULL,
                collection_time TIMESTAMP NOT NULL,
                cpu_count INTEGER NOT NULL,
                hyperthread_ratio INTEGER NOT NULL,
                physical_memory_mb BIGINT NOT NULL
            )");
            await ExecAsync(seed, "INSERT INTO server_properties VALUES (1, current_timestamp, 4, 1, 16384)");
            await ExecAsync(seed, "CREATE INDEX idx_server_properties_time ON server_properties(server_id, collection_time)");
        }

        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (var verify = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await verify.OpenAsync();
            Assert.Equal(
                (long)DuckDbInitializer.CurrentSchemaVersion,
                Convert.ToInt64(await ScalarAsync(verify, "SELECT MAX(version) FROM schema_version")));
        }

        Assert.Throws<IOException>(() => File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose());

        initializer.Dispose();
    }

    /// <summary>
    /// Dispose closes the sentinel (#4262): once the app is shutting down, nothing should still be
    /// pinning the file open, so an exclusive open — refused above while the sentinel was live — must now
    /// succeed, and a plain delete (the same operation any uninstall/reset path would need) must too.
    /// </summary>
    [Fact]
    public async Task Dispose_ClosesSentinel_FileDeletableAfterward()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        Assert.Throws<IOException>(() => File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose());

        initializer.Dispose();

        File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose();

        var ex = Record.Exception(() => File.Delete(_dbPath));
        Assert.Null(ex);
        Assert.False(File.Exists(_dbPath));
    }

    private static async Task SeedCollectionLogRowAsync(DuckDbInitializer initializer, string collector)
    {
        using var readLock = initializer.AcquireReadLock();
        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }

    /// <summary>
    /// #4262 round 3, my ruling: casts the sum to <c>BIGINT</c> in SQL instead of converting a
    /// <c>HUGEINT</c>/<c>BigInteger</c> in C# — see <see cref="DuckDbInitializer.ReadSentinelMemoryUsageBytes"/>'s
    /// remarks for why the old unconditional <c>Convert.ToDouble(result)</c> never returned a value. Drives
    /// the method against a real connection instead of unit-testing a conversion helper in isolation: the
    /// isolated version (this test's round-2 predecessor) passed while the real read still always failed,
    /// which is exactly how the original bug went unnoticed.
    /// </summary>
    [Fact]
    public async Task ReadSentinelMemoryUsageBytes_RealConnection_ReturnsPositiveNumber()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();

        var result = initializer.ReadSentinelMemoryUsageBytes(connection);

        Assert.NotNull(result);
        Assert.True(result > 0, $"Expected a positive byte count, got {result}");
    }

    /// <summary>
    /// #4262 round 1 finding 1. Forces <see cref="DuckDbInitializer.TrimThresholdBytes"/> low so the
    /// sentinel's ordinary resting usage counts as "over", then runs one cycle directly rather than waiting
    /// on the real 60s timer. <c>memory_limit</c> must come back to exactly what it was before the cycle —
    /// read through a fresh connection first (DuckDB's own formatting of the configured value, whatever
    /// that is) and compared after, rather than asserted against a guessed literal.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_OverThreshold_RestoresConfiguredMemoryLimit()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 1;
        try
        {
            string beforeLimit;
            using (var connection = initializer.CreateConnection())
            {
                await connection.OpenAsync();
                beforeLimit = Convert.ToString(await ScalarAsync(connection, "SELECT current_setting('memory_limit')")) ?? "";
            }

            initializer.RunMemoryTrimCycle();

            using (var connection = initializer.CreateConnection())
            {
                await connection.OpenAsync();
                var afterLimit = Convert.ToString(await ScalarAsync(connection, "SELECT current_setting('memory_limit')")) ?? "";
                Assert.Equal(beforeLimit, afterLimit);
            }
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
            initializer.Dispose();
        }
    }

    /// <summary>
    /// #4262 round 1 finding 1: "never wait, so a reader is never blocked behind the trim." With another
    /// thread holding the read lock and the threshold forced to 0 (so the cycle always wants to trim),
    /// <c>RunMemoryTrimCycle</c> must return immediately rather than wait for that reader to finish.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_ReadLockHeldByAnotherThread_SkipsWithoutWaiting()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 0;
        var readerReady = new ManualResetEventSlim();
        var releaseReader = new ManualResetEventSlim();
        try
        {
            var readerTask = Task.Run(() =>
            {
                using var readLock = initializer.AcquireReadLock();
                readerReady.Set();
                releaseReader.Wait(TimeSpan.FromSeconds(5));
            });

            Assert.True(readerReady.Wait(TimeSpan.FromSeconds(5)));

            var stopwatch = Stopwatch.StartNew();
            initializer.RunMemoryTrimCycle();
            stopwatch.Stop();

            Assert.True(stopwatch.ElapsedMilliseconds < 500,
                $"Trim cycle waited {stopwatch.ElapsedMilliseconds}ms behind a held read lock instead of skipping at once");

            releaseReader.Set();
            await readerTask;
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
            initializer.Dispose();
        }
    }

    /// <summary>
    /// #4262 round 3, my ruling. The tests above assert side effects that hold whether or not the cycle's
    /// SET statements ever ran (<c>memory_limit</c> round-trips either way if nothing touched it) — this
    /// asserts the run itself, through <see cref="DuckDbInitializer.CompletedTrimCycleCount"/>, which only
    /// increments once both SET statements below it actually complete.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_OverThreshold_IncrementsCompletedCycleCount()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 1;
        try
        {
            Assert.Equal(0, initializer.CompletedTrimCycleCount);

            initializer.RunMemoryTrimCycle();

            Assert.Equal(1, initializer.CompletedTrimCycleCount);
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
            initializer.Dispose();
        }
    }

    /// <summary>
    /// #4262 round 3 finding 3. Before this fix, the trim's memory read ran unlocked, so it could touch
    /// the sentinel at the exact moment a concurrent <see cref="DuckDbInitializer.ResetDatabaseAsync"/>
    /// (which runs its whole body under the write lock, and disposes the sentinel in the first few
    /// instructions after acquiring it) disposed it and swapped in a new one — a native call on a
    /// disposed DuckDB connection can take the whole process down, not a catchable exception. Fires trim
    /// cycles in a tight loop from another thread for the whole duration of a real reset.
    ///
    /// <para><b>Tried reverting just the read-lock guard to confirm this fails on the old shape, the
    /// usual bar for a regression test (round 1-2's tests in this class all clear it) — it did not
    /// reproduce, even hammered with 8 racer threads across 30 resets (~21s). The window between
    /// <c>ResetDatabaseAsync</c> taking the write lock and disposing the sentinel is a handful of CPU
    /// instructions wide, not a span this kind of loop can reliably land in.</b> This still pins the
    /// documented contract (a racing tick must never throw and the reset must still complete cleanly) and
    /// exercises the exact interleaving the finding describes on every run, which is worth keeping even
    /// unproven-by-revert; treat it as a stress/regression test, not a confirmed repro.</para>
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_RacingReset_NeverThrowsAndResetStillCompletes()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        await SeedCollectionLogRowAsync(initializer, "TestCollector");

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 0;
        var stop = new CancellationTokenSource();
        var racerExceptions = new List<Exception>();
        try
        {
            var racer = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        initializer.RunMemoryTrimCycle();
                    }
                    catch (Exception ex)
                    {
                        racerExceptions.Add(ex);
                    }
                }
            });

            var resetEx = await Record.ExceptionAsync(() => initializer.ResetDatabaseAsync());

            stop.Cancel();
            await racer;

            Assert.Null(resetEx);
            Assert.Empty(racerExceptions);

            using var afterReset = initializer.CreateConnection();
            await afterReset.OpenAsync();
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(afterReset, "SELECT COUNT(*) FROM collection_log")));
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
            initializer.Dispose();
        }
    }

    /// <summary>
    /// #4262 round 1 finding 2. With a read lock held on another thread for longer than
    /// <c>Dispose</c>'s write-lock timeout, <c>Dispose</c> must still return in roughly 2s rather than hang
    /// the caller (the UI thread, in production), must not throw, and must still release the sentinel —
    /// proven here by the file becoming exclusively openable right after.
    /// </summary>
    [Fact]
    public async Task Dispose_ReadLockHeldOnAnotherThread_ReturnsWithoutHangingOrThrowing()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var readerReady = new ManualResetEventSlim();
        var releaseReader = new ManualResetEventSlim();
        var readerTask = Task.Run(() =>
        {
            using var readLock = initializer.AcquireReadLock();
            readerReady.Set();
            releaseReader.Wait(TimeSpan.FromSeconds(10));
        });

        Assert.True(readerReady.Wait(TimeSpan.FromSeconds(5)));

        var stopwatch = Stopwatch.StartNew();
        var ex = Record.Exception(() => initializer.Dispose());
        stopwatch.Stop();

        Assert.Null(ex);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(3),
            $"Dispose took {stopwatch.Elapsed} with a read lock held elsewhere");

        releaseReader.Set();
        await readerTask;

        File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose();
    }

    /// <summary>
    /// #4262 round 4. Before this fix, <c>Dispose</c>'s <c>using var trimTimerStopped</c> disposed the wait
    /// handle the instant its 1s <c>WaitOne</c> timed out, even though the tick it was waiting on was still
    /// running. When that tick later finished, the runtime signals the SAME handle from a thread-pool thread
    /// (<c>TimerQueueTimer.SignalNoCallbacksRunning</c>), which throws <see cref="ObjectDisposedException"/>
    /// against the closed <c>SafeWaitHandle</c> there. Holds a real trim tick past the 1s wait with
    /// <see cref="DuckDbInitializer.TestTrimTickHoldGate"/>, disposes, confirms <c>Dispose</c> returned
    /// promptly instead of hanging behind the still-running tick, then releases the tick and confirms that
    /// exact exception never fires once the runtime signals the handle.
    ///
    /// <para><b>Confirmed failing on the old <c>using var trimTimerStopped</c> shape</b> — reverting to it
    /// and running this test alone reproduced precisely the failure this fix exists to prevent: a
    /// <see cref="AppDomain.CurrentDomain"/>-level <c>[FATAL ERROR] System.ObjectDisposedException</c>
    /// ("Object name: 'Microsoft.Win32.SafeHandles.SafeWaitHandle'") from
    /// <c>System.Threading.TimerQueueTimer.SignalNoCallbacksRunning()</c> on a thread-pool worker thread —
    /// the exact stack the finding describes. On this runtime it did not tear down the whole test process
    /// (the exception surfaces as a first-chance event rather than terminating), which is why this test
    /// hooks <see cref="AppDomain.FirstChanceException"/> rather than <see cref="AppDomain.UnhandledException"/>
    /// (that event never fired against the old shape either) — the first-chance hook is what actually
    /// observed the bug when this test was written and run against the reverted code.</para>
    /// </summary>
    [Fact]
    public async Task Dispose_TrimTickStillRunningPastTheWait_ReturnsPromptlyAndNeverSignalsADisposedHandle()
    {
        var originalInterval = DuckDbInitializer.TrimInterval;
        DuckDbInitializer.TrimInterval = TimeSpan.FromMilliseconds(20);
        DuckDbInitializer.TestTrimTickEntered = new ManualResetEventSlim(false);
        DuckDbInitializer.TestTrimTickHoldGate = new ManualResetEventSlim(false);

        var safeWaitHandleDisposedExceptions = new ConcurrentBag<Exception>();
        void OnFirstChance(object? sender, FirstChanceExceptionEventArgs e)
        {
            if (e.Exception is ObjectDisposedException ode &&
                ode.ObjectName == typeof(Microsoft.Win32.SafeHandles.SafeWaitHandle).FullName)
            {
                safeWaitHandleDisposedExceptions.Add(e.Exception);
            }
        }
        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;

        var initializer = new DuckDbInitializer(_dbPath);
        try
        {
            Assert.True(DuckDbInitializer.TestTrimTickEntered.Wait(TimeSpan.FromSeconds(5)),
                "Trim tick never started");

            var stopwatch = Stopwatch.StartNew();
            var ex = Record.Exception(() => initializer.Dispose());
            stopwatch.Stop();

            Assert.Null(ex);
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(3),
                $"Dispose took {stopwatch.Elapsed} — it should give up on the still-running tick after ~1s, not hang");

            /* The tick is still parked on the gate; only now does it finish and the runtime signal the
               handle Dispose left open for it. */
            DuckDbInitializer.TestTrimTickHoldGate.Set();

            /* Give the thread pool a moment to run the tick's completion, including the runtime's own
               signal of the handle Dispose decided not to dispose. */
            await Task.Delay(TimeSpan.FromSeconds(1));

            Assert.Empty(safeWaitHandleDisposedExceptions);
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;
            DuckDbInitializer.TestTrimTickHoldGate?.Set();
            DuckDbInitializer.TestTrimTickEntered?.Dispose();
            DuckDbInitializer.TestTrimTickEntered = null;
            DuckDbInitializer.TestTrimTickHoldGate?.Dispose();
            DuckDbInitializer.TestTrimTickHoldGate = null;
            DuckDbInitializer.TrimInterval = originalInterval;
        }
    }

    /// <summary>
    /// #4262 round 1 finding 3. Before this fix, <c>CreateArchiveViewsAsync</c> opened its connection with
    /// no lock at all, so it would have returned immediately here regardless of the concurrent write lock.
    /// Now it must wait behind the held write lock, proving the read lock is actually taken.
    /// </summary>
    [Fact]
    public async Task CreateArchiveViewsAsync_WaitsForConcurrentWriteLock()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var writeLockAcquired = new ManualResetEventSlim();
        var releaseWriteLock = new ManualResetEventSlim();
        var holderTask = Task.Run(() =>
        {
            using var writeLock = initializer.AcquireWriteLock();
            writeLockAcquired.Set();
            releaseWriteLock.Wait(TimeSpan.FromSeconds(5));
        });

        Assert.True(writeLockAcquired.Wait(TimeSpan.FromSeconds(5)));

        var archiveViewsTask = Task.Run(() => initializer.CreateArchiveViewsAsync());
        var raced = await Task.WhenAny(archiveViewsTask, Task.Delay(TimeSpan.FromMilliseconds(300)));
        Assert.NotSame(archiveViewsTask, raced);

        releaseWriteLock.Set();
        await holderTask;
        await archiveViewsTask;

        initializer.Dispose();
    }

    /// <summary>
    /// #4262 round 1 finding 3. <c>QueryStoreSliceRepairService.PromoteRewrittenFileAsync</c> calls
    /// <c>CreateArchiveViewsAsync</c> while its own caller already holds the write lock — contrary to the
    /// review's claim that call site held no lock. <c>s_dbLock</c> is <c>NoRecursion</c>, so this proves
    /// the public method survives being called from a write-lock holder (backstopped by
    /// <c>AcquireReadLock</c>'s recursion catch) rather than throwing <c>LockRecursionException</c>.
    /// </summary>
    [Fact]
    public async Task CreateArchiveViewsAsync_CalledWhileHoldingWriteLockOnSameThread_DoesNotThrow()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (initializer.AcquireWriteLock())
        {
            var ex = await Record.ExceptionAsync(() => initializer.CreateArchiveViewsAsync());
            Assert.Null(ex);
        }

        initializer.Dispose();
    }

    /// <summary>
    /// #4262 round 1 finding 1, measured rather than only unit-asserted: seeds a synthetic table large
    /// enough to genuinely inflate DuckDB's buffer pool, reads it wide-open (the shape of the hazard —
    /// "one full-width read" in the review), and asserts on DuckDB's OWN memory accounting
    /// (<see cref="DuckDbInitializer.ReadSentinelMemoryUsageBytes"/>, the same <c>duckdb_memory()</c> sum
    /// the trim cycle itself reads) rather than the process-wide <c>WorkingSet64</c> this test used before.
    ///
    /// <para><b>Why the swap:</b> <c>WorkingSet64</c> is an OS-level, whole-process metric that also
    /// reflects the GC heap, the CLR/JIT, and every other native allocation in the process — nightly CI hit
    /// exactly this: before=1433563136, after=1433567232 (one 4KB page HIGHER, not lower), a pass/fail
    /// margin of literally nothing on a metric with no relationship to what the trim actually touches. The
    /// trim only ever does one thing (<see cref="DuckDbInitializer.RunMemoryTrimCycle"/>): cycles
    /// <c>memory_limit</c> down and back up so DuckDB's own buffer manager releases cached pages.
    /// <c>duckdb_memory()</c> is that buffer manager's own ledger, so it isolates the assertion from every
    /// other allocator in the process.</para>
    ///
    /// <para><b>What this asserts:</b> the trim cycle (<see cref="DuckDbInitializer.RunMemoryTrimCycle"/>)
    /// sets <c>memory_limit</c> down to <see cref="DuckDbInitializer.TrimTargetMemoryLimit"/> and back, so
    /// DuckDB's buffer manager evicts pages down to AT MOST that target — how far below it lands is
    /// platform-dependent (Windows CI measured before=105906176/after=63700992, a ~40% drop; macOS measured
    /// before≈98 MB/after≈0.75 MB). A ratio-based assertion is therefore not a stable contract across
    /// platforms. What IS the trim's own contract: the before-reading must be above the target (otherwise
    /// the wide read never filled the buffer past what the trim would remove anyway, and the test proves
    /// nothing), and the after-reading must be at or below the target. A do-nothing trim leaves
    /// after≈before, which is still above the target, so it still fails this check.</para>
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_AfterWideRead_ReducesProcessMemory()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (var seed = initializer.CreateConnection())
        {
            await seed.OpenAsync();
            /* md5-derived payload, not repeat('x', ...): DuckDB's storage layer dictionary/RLE-compresses
               a low-cardinality repeated literal down to a few hundred KB in its own buffer manager
               (measured), which starves this test's own signal before the trim ever runs. The md5 chain
               defeats that compression the same way real Query Store plan XML would. */
            await ExecAsync(seed, "CREATE TABLE trim_probe AS SELECT i AS id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) || md5((i + 2)::VARCHAR) AS payload FROM range(2000000) t(i)");
        }

        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT count(*) FROM trim_probe WHERE payload LIKE 'x%'";
            await cmd.ExecuteScalarAsync();
        }

        double? beforeBytes;
        using (var measure = initializer.CreateConnection())
        {
            await measure.OpenAsync();
            beforeBytes = initializer.ReadSentinelMemoryUsageBytes(measure);
        }

        Assert.NotNull(beforeBytes);

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 1;
        try
        {
            initializer.RunMemoryTrimCycle();
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }

        double? afterBytes;
        using (var measure = initializer.CreateConnection())
        {
            await measure.OpenAsync();
            afterBytes = initializer.ReadSentinelMemoryUsageBytes(measure);
        }

        Assert.NotNull(afterBytes);

        initializer.Dispose();

        Console.WriteLine($"#4262 round 1 trim measurement (duckdb_memory): before={beforeBytes / (1024.0 * 1024.0):F1} MB, after={afterBytes / (1024.0 * 1024.0):F1} MB");

        // DuckDB's "64MB" memory_limit unit is decimal (64 * 1000 * 1000 bytes), not binary (MiB).
        const double trimTargetBytes = 64.0 * 1000.0 * 1000.0;

        Assert.True(beforeBytes > trimTargetBytes,
            $"Expected the wide read to fill DuckDB's buffer past the trim target before trimming, or this test proves nothing: before={beforeBytes}, target={trimTargetBytes}");
        Assert.True(afterBytes <= trimTargetBytes,
            $"Expected the trim to bring DuckDB's own reported buffer usage down to its memory_limit target: before={beforeBytes}, after={afterBytes}, target={trimTargetBytes}");
    }
}
