using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Times each fact collector of one analysis pass (#9195). The pass awaits its collectors one after another, and a field
/// log could only say the whole pass was slow (get_analysis_facts took 8.5 s), never which collectors the time went to.
/// Each collector gets one Debug line with its own elapsed time, and a pass that takes longer than
/// <see cref="SlowPassThreshold"/> gets one Info line naming its three slowest collectors, which is the line a default-level
/// log carries. Nothing here runs collectors concurrently or touches what they read.
/// </summary>
internal sealed class CollectorPassTimer
{
    /// <summary>A pass longer than this logs its slowest collectors at Info.</summary>
    internal static readonly TimeSpan SlowPassThreshold = TimeSpan.FromSeconds(2);

    private const string LogSource = "DuckDbFactCollector";
    private const int SlowestShown = 3;

    private readonly string _serverName;
    private readonly int _serverId;
    private readonly TimeSpan _slowPassThreshold;
    private readonly long _passStarted = Stopwatch.GetTimestamp();
    private readonly List<(string Name, TimeSpan Elapsed)> _timings = [];

    internal CollectorPassTimer(string serverName, int serverId, TimeSpan? slowPassThreshold = null)
    {
        _serverName = serverName;
        _serverId = serverId;
        _slowPassThreshold = slowPassThreshold ?? SlowPassThreshold;
    }

    /// <summary>Runs one collector and logs how long it took. A collector that throws (the pass was cancelled) logs nothing and rethrows.</summary>
    internal async Task RunAsync(string collectorName, Func<Task> collect)
    {
        var started = Stopwatch.GetTimestamp();
        await collect();
        var elapsed = Stopwatch.GetElapsedTime(started);

        _timings.Add((collectorName, elapsed));
        AppLogger.Debug(LogSource,
            $"{collectorName} took {FormatMs(elapsed)} ms for {_serverName} (server {_serverId})");
    }

    /// <summary>Ends the pass: one Info line naming the slowest collectors when the pass was slow, nothing otherwise.</summary>
    internal void Finish()
    {
        var total = Stopwatch.GetElapsedTime(_passStarted);
        if (total <= _slowPassThreshold)
        {
            return;
        }

        var slowest = string.Join(", ", _timings
            .OrderByDescending(t => t.Elapsed)
            .Take(SlowestShown)
            .Select(t => $"{t.Name} {FormatMs(t.Elapsed)} ms"));
        AppLogger.Info(LogSource,
            $"Fact collection took {FormatMs(total)} ms for {_serverName} (server {_serverId}) across {_timings.Count} collectors, slowest: {slowest}");
    }

    private static string FormatMs(TimeSpan elapsed) =>
        ((long)elapsed.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
}
