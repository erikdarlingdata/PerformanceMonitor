using System;
using System.Diagnostics;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #9195: the live plan fetches one interactive read (Lite's <c>get_analysis_facts</c> or <c>compare_analysis</c>) may make
/// for plan regression pairs no earlier pass cached. One allowance is shared by every <see cref="AnalysisContext"/> of the
/// call, so a two-window compare spends one allowance, not one per window. It holds the fetches left and a wall clock for
/// the whole fetch step: the clock starts at the call's first fetch attempt, and each fetch is limited to what is left.
/// </summary>
public sealed class InteractivePlanFetchAllowance
{
    /// <summary>Plan fetches one interactive call may make.</summary>
    public const int DefaultFetches = 3;

    /// <summary>The wall clock for all of one interactive call's plan fetches together.</summary>
    public static readonly TimeSpan DefaultTotalTime = TimeSpan.FromSeconds(3);

    /// <summary>A fetch granted with less than this left would be cut off at once and logged as a failed fetch, so the pair counts as not reached instead.</summary>
    public static readonly TimeSpan MinimumFetchTime = TimeSpan.FromMilliseconds(50);

    private readonly TimeSpan _total;
    private long _started;

    public InteractivePlanFetchAllowance(int fetches = DefaultFetches, TimeSpan? total = null)
    {
        Left = fetches;
        _total = total ?? DefaultTotalTime;
    }

    /// <summary>Fetches still allowed.</summary>
    public int Left { get; private set; }

    /// <summary>Fetches the call made, and the time they took, for the call's slow-read line.</summary>
    public int Fetches { get; private set; }

    public TimeSpan FetchTime { get; private set; }

    /// <summary>
    /// Takes one fetch when one is left and the step's time is not spent, starting the clock on the first attempt. The caller
    /// limits the fetch to <paramref name="remaining"/>, so no fetch outlives the step.
    /// </summary>
    public bool TryTake(out TimeSpan remaining)
    {
        if (_started == 0) _started = Stopwatch.GetTimestamp();
        remaining = _total - Stopwatch.GetElapsedTime(_started);
        if (Left <= 0 || remaining < MinimumFetchTime) return false;

        Left--;
        Fetches++;
        return true;
    }

    /// <summary>Adds one finished fetch's time.</summary>
    public void RecordFetchTime(TimeSpan elapsed) => FetchTime += elapsed;
}
