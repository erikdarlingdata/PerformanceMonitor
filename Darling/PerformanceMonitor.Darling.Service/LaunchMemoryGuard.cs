/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One memory reading for the launch guard (#1556, #5479): the figure, the limit it is measured against, and
/// which metric and which limit those are, so a log line can say what was actually compared.
/// </summary>
/// <param name="Bytes">The process's current figure under <paramref name="Metric"/>.</param>
/// <param name="LimitBytes">What the figure is measured against; the guard's line is
/// <see cref="DarlingWorker.MemoryGuardFraction"/> of it. Non-positive means unknown, which never blocks.</param>
/// <param name="Metric">"private bytes" (Windows) or "resident memory" (Linux).</param>
/// <param name="LimitSource">"the GC memory budget", "cgroup memory limit" or "total RAM".</param>
internal readonly record struct LaunchMemoryReading(long Bytes, long LimitBytes, string Metric, string LimitSource);

/// <summary>
/// The memory launch guard's state (#1556), and its release rule (#5479). Once per sweep pass the worker asks
/// <see cref="MayLaunch"/> whether it may start NEW collection bodies. Over the line the answer is no and the
/// in-flight bodies drain, but a drained process allocates nothing, so no garbage collection ever runs to give the
/// memory back, and a guard that only waited for the figure to fall held one fleet for 25 hours. So the guard does
/// the releasing itself: at the trip, and at most once a minute while it holds, it runs a decommitting full
/// collection and measures again in the same pass; under the line it releases at once. The guard has no other
/// release path and no forced release: memory that is still over after the collection keeps the hold, and the
/// stopped-collection alerts report the stop.
/// </summary>
/// <remarks>
/// The sampler, the collection and the clock are injected so a test drives every branch without real memory.
/// The clock is monotonic on purpose: a wall clock stepped backwards would otherwise stop the once-a-minute
/// collection and the five-minute warning for as long as the step.
/// </remarks>
internal sealed class LaunchMemoryGuard
{
    /// <summary>The shortest gap between two guard-run collections while the hold lasts.</summary>
    internal static readonly TimeSpan CollectionInterval = TimeSpan.FromMinutes(1);

    /// <summary>The gap between the "still holding" warnings.</summary>
    internal static readonly TimeSpan HoldWarningInterval = TimeSpan.FromMinutes(5);

    private readonly Func<LaunchMemoryReading> _sample;
    private readonly Action _collectGarbage;
    private readonly Func<TimeSpan> _clock;
    private readonly ILogger _logger;

    private bool _holding;
    private TimeSpan _heldSince;
    private TimeSpan _lastCollection;
    private TimeSpan _lastWarning;
    private bool _warnedSamplerFault;

    /// <param name="sample">Reads the current figure, its limit and which metric that is.</param>
    /// <param name="collectGarbage">Runs the decommitting full collection.</param>
    /// <param name="clock">A monotonic clock (time since any fixed start).</param>
    /// <param name="logger">Where the trip, collection, hold and release lines go.</param>
    internal LaunchMemoryGuard(Func<LaunchMemoryReading> sample, Action collectGarbage, Func<TimeSpan> clock, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(sample);
        ArgumentNullException.ThrowIfNull(collectGarbage);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(logger);
        _sample = sample;
        _collectGarbage = collectGarbage;
        _clock = clock;
        _logger = logger;
    }

    /// <summary>True while the guard is holding new launches off.</summary>
    internal bool IsHolding => _holding;

    /// <summary>
    /// The production guard: this platform's sampler, the decommitting full collection and the process's monotonic
    /// tick count.
    /// </summary>
    internal static LaunchMemoryGuard CreateDefault(ILogger logger)
    {
        Func<LaunchMemoryReading> sample;
        if (OperatingSystem.IsLinux())
        {
            sample = new LinuxLaunchMemorySampler(
                DarlingStoreHostProfile.TryReadFile,
                () => { using var process = Process.GetCurrentProcess(); return process.WorkingSet64; },
                () => GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
                logger).Sample;
        }
        else
        {
            sample = SampleWindowsPrivateBytes;
        }

        return new LaunchMemoryGuard(
            sample,
            CollectAndDecommit,
            () => TimeSpan.FromMilliseconds(Environment.TickCount64),
            logger);
    }

    /// <summary>
    /// Windows (and anything that is not Linux): committed private bytes against the GC's available-memory budget.
    /// Private bytes are the metric that matched the field incident, a 13GB commit-limit blowout the GC heap alone
    /// did not show; on Windows they are the commit charge, which is what the commit limit kills on. The GC's
    /// budget is the machine's physical memory or the job or container limit.
    /// </summary>
    private static LaunchMemoryReading SampleWindowsPrivateBytes()
    {
        long privateBytes;
        using (var process = Process.GetCurrentProcess())
        {
            privateBytes = process.PrivateMemorySize64;
        }

        return new LaunchMemoryReading(
            privateBytes, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes, "private bytes", "the GC memory budget");
    }

    /// <summary>The full blocking compacting collection that also decommits the memory it frees (#5479). Aggressive
    /// is what gives the memory back to the operating system: a plain forced full collection frees the heap but
    /// leaves the process's private bytes where they were (measured: 553MB to 488MB, against 553MB to 8MB).</summary>
    internal static void CollectAndDecommit()
        => GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);

    /// <summary>
    /// Whether the fleet sweep may launch NEW collection bodies this pass, and the guard's whole state machine: the
    /// trip (one Critical per episode, then a collection), a repeat collection at most once a minute while it holds,
    /// a Warning every five minutes while it holds, and the release (an Information line). A pass that is under the
    /// line and not holding logs nothing.
    /// </summary>
    /// <param name="inFlightBodies">How many collection bodies are still running, for the hold warning.</param>
    internal bool MayLaunch(int inFlightBodies)
    {
        LaunchMemoryReading reading;
        TimeSpan now;
        try
        {
            now = _clock();
            reading = _sample();
        }
        catch (Exception ex)
        {
            /* A guard that cannot read memory must not stop collection: fail open, once in the log. */
            if (!_warnedSamplerFault)
            {
                _warnedSamplerFault = true;
                _logger.LogWarning(
                    ex, "The memory launch guard could not read the process memory ({Reason}) — it lets collection launch until it can.", ex.Message);
            }

            return true;
        }

        if (DarlingWorker.ShouldLaunchSweeps(reading.Bytes, reading.LimitBytes))
        {
            if (_holding)
            {
                Release(now, reading);
            }

            return true;
        }

        if (!_holding)
        {
            _holding = true;
            _heldSince = now;
            _lastWarning = now;
            _logger.LogCritical(
                "Memory over the line: {Metric} {FigureMb}MB is over {Pct:P0} of the {LimitMb}MB limit ({LimitSource}) — PAUSING new collection-body launches so in-flight bodies drain (the #1556 commit-limit backstop). Purge/disk/analysis continue. A full garbage collection runs now, and about once a minute while the pause holds, and the pause ends when the figure is back under the line.",
                reading.Metric, Mb(reading.Bytes), DarlingWorker.MemoryGuardFraction, Mb(reading.LimitBytes), reading.LimitSource);
            reading = CollectAndMeasure(reading, now);
        }
        else if (now - _lastCollection >= CollectionInterval)
        {
            reading = CollectAndMeasure(reading, now);
        }

        if (DarlingWorker.ShouldLaunchSweeps(reading.Bytes, reading.LimitBytes))
        {
            Release(now, reading);
            return true;
        }

        if (now - _lastWarning >= HoldWarningInterval)
        {
            _lastWarning = now;
            _logger.LogWarning(
                "The memory launch guard has held collection off for {HeldFor}: {Metric} {FigureMb}MB is still over {Pct:P0} of the {LimitMb}MB limit ({LimitSource}); {InFlight} collection bodies still in flight. No new collection runs while it holds.",
                FormatHeld(now - _heldSince), reading.Metric, Mb(reading.Bytes), DarlingWorker.MemoryGuardFraction,
                Mb(reading.LimitBytes), reading.LimitSource, inFlightBodies);
        }

        return false;
    }

    /// <summary>Runs the collection, logs the figure before and after, and returns the fresh reading. A failed
    /// collection or a failed second reading leaves the old reading, so the hold simply continues.</summary>
    private LaunchMemoryReading CollectAndMeasure(LaunchMemoryReading before, TimeSpan now)
    {
        _lastCollection = now;
        var after = before;
        try
        {
            _collectGarbage();
            after = _sample();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "The memory launch guard's garbage collection or the measurement after it failed ({Reason}); the hold continues.", ex.Message);
        }

        _logger.LogInformation(
            "The memory launch guard ran a full garbage collection: {Metric} {BeforeMb}MB before, {AfterMb}MB after ({LimitMb}MB limit, {LimitSource}).",
            before.Metric, Mb(before.Bytes), Mb(after.Bytes), Mb(after.LimitBytes), after.LimitSource);
        return after;
    }

    private void Release(TimeSpan now, LaunchMemoryReading reading)
    {
        _holding = false;
        _logger.LogInformation(
            "Memory recovered after a hold of {HeldFor}: {Metric} {FigureMb}MB of {LimitMb}MB ({LimitSource}) — resuming collection-body launches.",
            FormatHeld(now - _heldSince), reading.Metric, Mb(reading.Bytes), Mb(reading.LimitBytes), reading.LimitSource);
    }

    private static long Mb(long bytes) => bytes / (1024 * 1024);

    /// <summary>"42s", "7m 05s" or "25h 04m": the hold's length, readable at both ends of what it can be.</summary>
    internal static string FormatHeld(TimeSpan held)
    {
        if (held < TimeSpan.Zero)
        {
            held = TimeSpan.Zero;
        }

        if (held.TotalHours >= 1)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{(long)held.TotalHours}h {held.Minutes:00}m");
        }

        if (held.TotalMinutes >= 1)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{held.Minutes}m {held.Seconds:00}s");
        }

        return string.Create(CultureInfo.InvariantCulture, $"{(long)held.TotalSeconds}s");
    }
}

/// <summary>
/// The Linux launch-guard sampler (#5479): the process's RESIDENT memory (<c>VmRSS</c> in
/// <c>/proc/self/status</c>) against the limit that actually kills the process, the cgroup memory limit when one is
/// set and total RAM otherwise. The old Linux figure was <c>Process.PrivateMemorySize64</c>, which the runtime reads
/// as <c>VmData</c>: every private writable mapping, resident or not. In the field incident it read 1426MB while
/// the container's resident memory was about 810MB and its limit 2Gi, so the guard paused collection far from any
/// kill. The limit is read once, on the first sample (it does not change while the process runs); the figure on
/// every sample. A read that fails falls back to <c>Process.WorkingSet64</c> against the GC's budget, logged once.
/// </summary>
internal sealed class LinuxLaunchMemorySampler
{
    internal const string ProcStatusPath = "/proc/self/status";

    private readonly Func<string, string?> _readFile;
    private readonly Func<long> _workingSetBytes;
    private readonly Func<long> _gcBudgetBytes;
    private readonly ILogger _logger;

    private bool _limitResolved;
    private (long Bytes, string Source)? _limit;
    private bool _loggedFallback;

    /// <param name="readFile">Reads a whole text file, or null when it cannot be read.</param>
    /// <param name="workingSetBytes">The runtime's own working-set reading, the fallback figure.</param>
    /// <param name="gcBudgetBytes">The GC's available-memory budget, the fallback limit.</param>
    /// <param name="logger">Where the one fallback line goes.</param>
    internal LinuxLaunchMemorySampler(Func<string, string?> readFile, Func<long> workingSetBytes, Func<long> gcBudgetBytes, ILogger logger)
    {
        _readFile = readFile;
        _workingSetBytes = workingSetBytes;
        _gcBudgetBytes = gcBudgetBytes;
        _logger = logger;
    }

    /// <summary>The current reading.</summary>
    internal LaunchMemoryReading Sample()
    {
        if (!_limitResolved)
        {
            _limit = ResolveLimit(
                _readFile(DarlingStoreHostProfile.ProcMeminfoPath),
                _readFile(DarlingStoreHostProfile.CgroupV2MemoryMaxPath),
                _readFile(DarlingStoreHostProfile.CgroupV1MemoryLimitPath));
            _limitResolved = true;
        }

        if (_limit is not { } limit)
        {
            return Fallback("no memory limit could be read from /proc/meminfo or the cgroup files");
        }

        var resident = ParseProcStatusBytes(_readFile(ProcStatusPath), "VmRSS");
        if (resident is null)
        {
            return Fallback("VmRSS could not be read from /proc/self/status");
        }

        return new LaunchMemoryReading(resident.Value, limit.Bytes, "resident memory", limit.Source);
    }

    private LaunchMemoryReading Fallback(string reason)
    {
        if (!_loggedFallback)
        {
            _loggedFallback = true;
            _logger.LogWarning(
                "The memory launch guard falls back to the process working set against the GC memory budget: {Reason}.", reason);
        }

        return new LaunchMemoryReading(_workingSetBytes(), _gcBudgetBytes(), "resident memory", "the GC memory budget");
    }

    /// <summary>
    /// The effective limit and where it came from, from the text of <c>/proc/meminfo</c> and the two cgroup files:
    /// the cgroup limit when it is set and under total RAM, else total RAM (a cgroup v2 <c>max</c> and a v1
    /// near-<see cref="long.MaxValue"/> both mean no limit). Null when neither is readable. The parsers and the
    /// min-of-the-two rule are the store host profile's, so the guard and <c>--check-settings</c> agree on the limit.
    /// </summary>
    internal static (long Bytes, string Source)? ResolveLimit(string? meminfoText, string? cgroupV2MemoryMaxText, string? cgroupV1MemoryLimitText)
    {
        var ram = DarlingStoreHostProfile.ParseProcMeminfoTotalBytes(meminfoText);
        var cgroup = DarlingStoreHostProfile.ParseCgroupV2MemoryMaxBytes(cgroupV2MemoryMaxText)
            ?? DarlingStoreHostProfile.ParseCgroupV1MemoryLimitBytes(cgroupV1MemoryLimitText);

        if (ram is not { } totalRam)
        {
            return cgroup is { } onlyCgroup ? (onlyCgroup, "cgroup memory limit") : null;
        }

        var effective = DarlingStoreHostProfile.ComputeEffectiveMemoryLimitBytes(totalRam, cgroup);
        return (effective, cgroup is not null && effective != totalRam ? "cgroup memory limit" : "total RAM");
    }

    /// <summary>
    /// One <c>Key:   N kB</c> line of <c>/proc/self/status</c> (for example <c>VmRSS</c> or <c>VmData</c>) as bytes,
    /// or null when the text has no parseable line for that key.
    /// </summary>
    internal static long? ParseProcStatusBytes(string? statusText, string key)
    {
        if (string.IsNullOrEmpty(statusText))
        {
            return null;
        }

        var prefix = key + ":";
        foreach (var rawLine in statusText.Split('\n'))
        {
            var line = rawLine.TrimEnd('\r').TrimStart();
            if (!line.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var rest = line[prefix.Length..].Trim();
            var spaceIndex = rest.IndexOf(' ');
            var numberText = spaceIndex >= 0 ? rest[..spaceIndex] : rest;
            return long.TryParse(numberText, NumberStyles.None, CultureInfo.InvariantCulture, out var kb) && kb >= 0
                ? kb * 1024L
                : null;
        }

        return null;
    }
}
