using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the status bar's used-size read against the two ways it can be wrong (#2594).
///
/// <para><b>It must not open a connection without the lock.</b> This was the one PERIODIC connection site
/// in <see cref="DuckDbInitializer"/> that did — driven by the dashboard's 30-second
/// <c>DispatcherTimer</c>, so a live handle sat on the database file at arbitrary moments, including
/// moments the archival path may be deleting and recreating it.</para>
///
/// <para><b>And it must not block the dashboard to get it.</b> The obvious fix — take the read lock like
/// every other read — runs on the dispatcher thread, so a size figure nobody is reading would freeze the
/// window behind a long archival. So the contract is specifically a BOUNDED attempt: acquire if free,
/// give up quickly otherwise, and let the caller render the file size alone.</para>
///
/// <para><b>Proved directly, not by the clock.</b> This used to measure a <c>Stopwatch</c> against a 2s
/// ceiling and trust that "returned fast" meant "gave up". <see cref="DuckDbInitializer"/>'s lock is one
/// static field shared by every instance in the process, so under a loaded full-suite run even the
/// WRITER's own wait to take the lock in the first place can legitimately run out — other tests hold it
/// too — which has nothing to do with the 100ms read budget under test. Confirmed directly under
/// synthetic contention during triage: with the lock artificially busy, a five-second writer wait ran out
/// 30/30 times, consistently landing right at its own ~5000-5030ms bound rather than blowing past it by
/// seconds — that is real contention for a turn on the lock, not the wait itself overshooting. A wall
/// clock also cannot tell "gave up early" from "got lucky and the writer had just released", so this
/// instead asserts the one thing that actually distinguishes them: when <c>GetUsedDataSizeMb()</c>
/// returns, the holder has NOT yet been released. The one wall-clock number left is a generous hang
/// backstop, not a timing budget.</para>
/// </summary>
public class StatusBarSizeReadLockTests
{
    private static readonly TimeSpan WriteLockHold = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long the test waits for <see cref="DuckDbInitializer.GetUsedDataSizeMb"/> to RETURN, once the
    /// writer confirmably holds the lock. This is not the 100ms budget under test — it is a hang backstop,
    /// generous against that budget (80x) and strictly below <see cref="WriteLockHold"/>, so a genuine
    /// "blocked instead of gave up" regression fails on ITS OWN timeout instead of coincidentally passing
    /// because the holder's ten-second hold happened to end first.
    /// </summary>
    private static readonly TimeSpan HangBackstop = TimeSpan.FromSeconds(8);

    [Fact]
    public async Task GetUsedDataSizeMb_WhenTheWriteLockIsHeld_GivesUpInsteadOfBlocking()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "pmlite-statusbar-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var initializer = new DuckDbInitializer(Path.Combine(tempDir, "test.duckdb"));

            var lockHeld = new ManualResetEventSlim(false);
            var release = new ManualResetEventSlim(false);
            var holderReleased = new ManualResetEventSlim(false);

            /* The write lock is thread-affine, so it has to be taken and released on its own thread.
               holderReleased is set AFTER the using block disposes — i.e., after s_dbLock is genuinely
               free again — which is what turns the assertion below into a proof instead of a guess. */
            var holder = Task.Run(() =>
            {
                using (initializer.AcquireWriteLock())
                {
                    lockHeld.Set();
                    release.Wait(WriteLockHold);
                }
                holderReleased.Set();
            });

            try
            {
                /* Generous, not tight: a hang backstop, not a timing budget. s_dbLock is one static
                   field shared by every DuckDbInitializer in the process, so a parallel run of the rest
                   of the suite can legitimately queue this holder behind another test's own hold of the
                   same lock — see the class doc for the reproduction. */
                Assert.True(lockHeld.Wait(TimeSpan.FromSeconds(30)), "the write lock was never acquired");

                /* Off the calling thread, and bounded only on the WAIT for its result — not on the call's
                   own timing. A Stopwatch on the calling thread cannot distinguish "gave up" from "this
                   never returns"; WaitAsync's TimeoutException makes a genuine hang fail loudly here
                   instead of hanging the run. Entering and exiting the lock both happen synchronously
                   inside this one lambda (no await between them), so the thread-affine release is safe
                   regardless of which pool thread runs it. */
                var read = Task.Run(() => initializer.GetUsedDataSizeMb());
                var used = await read.WaitAsync(HangBackstop);

                /* The direct proof: the read must have given up WHILE the writer still held the lock, not
                   merely returned before some clock ran out. Both of these are true only if s_dbLock was
                   still exclusively held at the moment GetUsedDataSizeMb() produced its answer. */
                Assert.False(holderReleased.IsSet,
                    "the write lock had already been released — this does not prove the read gave up");
                Assert.False(holder.IsCompleted,
                    "the holder task had already finished — this does not prove the read gave up");

                /* Null, not a number: the caller renders the file size alone in this state, which is the
                   degraded answer this is choosing on purpose. */
                Assert.Null(used);
            }
            finally
            {
                /* In `finally` rather than after the asserts: if an assertion above throws, the holder
                   must still be released here, or it sits on s_dbLock — a process-wide static — for the
                   rest of WriteLockHold and stalls every other test sharing it. */
                release.Set();
                await holder.WaitAsync(TimeSpan.FromSeconds(15));
            }
        }
        finally
        {
            try
            {
                Directory.Delete(tempDir, recursive: true);
            }
            catch (IOException)
            {
                /* A leftover temp directory is not worth failing a passing test over. */
            }
        }
    }
}
