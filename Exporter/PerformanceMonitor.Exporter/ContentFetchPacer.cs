using System.Diagnostics;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Paces content-fetch calls against the store: after every batch, sleep for as long as the call took, at least 50 ms
/// and at most 2 s. This is the one shared implementation for every content batch -- <see
/// cref="PgStatementTextExporter"/> had its own copy of this math (<c>PaceSleepMs</c>/<c>DefaultSleep</c>) before this
/// type existed; that copy now delegates here so there is exactly one pacing implementation. The clock and sleeper are
/// both injectable, so a test can assert the pacing math and the call/sleep sequence without a real clock or a real
/// delay.
/// </summary>
public sealed class ContentFetchPacer
{
    /// <summary>Floor on the pacing sleep, regardless of how fast the call was.</summary>
    public const int MinSleepMs = 50;

    /// <summary>Ceiling on the pacing sleep, regardless of how slow the call was.</summary>
    public const int MaxSleepMs = 2000;

    private readonly Func<long> _timestampNow;
    private readonly long _frequency;
    private readonly Func<int, CancellationToken, Task> _sleep;
    private readonly bool _allowCompressed;
    private long _totalSleepMs;

    /// <summary>
    /// Builds a pacer. <paramref name="timestampNow"/> defaults to <see
    /// cref="Stopwatch.GetTimestamp"/>, and <paramref name="sleep"/> defaults to
    /// <see cref="Task.Delay(int, CancellationToken)"/> -- both are injectable so tests can
    /// run the pacing math against a fake clock and a no-op sleeper.
    /// </summary>
    public ContentFetchPacer(bool allowCompressed = false, Func<long>? timestampNow = null, Func<int, CancellationToken, Task>? sleep = null)
    {
        _allowCompressed = allowCompressed;
        _timestampNow = timestampNow ?? Stopwatch.GetTimestamp;
        _frequency = Stopwatch.Frequency;
        _sleep = sleep ?? ((ms, ct) => Task.Delay(ms, ct));
    }

    /// <summary>Running total of the pacing sleeps this pacer has requested, in ms. Reported as <c>pace_sleep_ms</c>; it does not feed back into the pacing math.</summary>
    public long TotalSleepMs => Interlocked.Read(ref _totalSleepMs);

    /// <summary>
    /// The pacing sleep for a call that took <paramref name="callElapsedMs"/>: clamped to
    /// [<see cref="MinSleepMs"/>, <see cref="MaxSleepMs"/>], then doubled (and re-clamped to
    /// the same ceiling) when <c>--allow-compressed</c> is set for this run.
    /// </summary>
    public static int SleepMsFor(double callElapsedMs, bool allowCompressed = false)
    {
        var ms = (int)Math.Round(callElapsedMs, MidpointRounding.AwayFromZero);
        var clamped = Math.Clamp(ms, MinSleepMs, MaxSleepMs);
        if (!allowCompressed)
        {
            return clamped;
        }

        return Math.Min(clamped * 2, MaxSleepMs * 2);
    }

    /// <summary>
    /// Times <paramref name="call"/>, sleeps for the paced amount, and returns the call's
    /// result. Every content batch in the exporter (plan digests, query text digests,
    /// Query Store plan-map lookups, Query Store text lookups, pg_statement_text) should run
    /// through this instead of timing and sleeping by hand.
    /// </summary>
    public async Task<T> RunAsync<T>(Func<Task<T>> call, CancellationToken ct)
    {
        var start = _timestampNow();
        var result = await call().ConfigureAwait(false);
        var elapsedMs = (_timestampNow() - start) * 1000.0 / _frequency;
        var sleepMs = SleepMsFor(elapsedMs, _allowCompressed);
        Interlocked.Add(ref _totalSleepMs, sleepMs);
        await _sleep(sleepMs, ct).ConfigureAwait(false);
        return result;
    }
}
