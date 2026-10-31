/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The hidden <c>--self-check-launch-memory [&lt;expected-limit-mib&gt;]</c> verb (#6995): samples the Linux launch
/// guard's memory reader once, prints the metric and where the limit came from, and exits non-zero unless the reader
/// measures resident memory against the cgroup limit when the container has one. The Linux container smoke runs it
/// inside the built image under <c>docker run --memory 512m</c>, because the test project runs on Windows and so
/// only ever feeds the sampler fake <c>/proc</c> files (#5479 read VmData against a limit that was not the container's,
/// and no test ran the real files).
///
/// <para>The sampler is the guard's own (<see cref="LaunchMemoryGuard.CreateLinuxSampler"/>), so the verb fails when the
/// guard's reader regresses. The cgroup limit is read here a second time, through the same resolver the host profile
/// uses, so a sampler that stops asking the cgroup is caught rather than agreeing with itself. With an expected limit
/// the reading must also be that size, which proves the limit the container was started with reaches the guard.</para>
/// </summary>
internal static class DarlingLaunchMemorySelfCheck
{
    private const string Verb = "--self-check-launch-memory";

    /// <summary>What the verb prints for a call with the wrong arguments.</summary>
    internal const string UsageLine = Verb + " [<expected-limit-mib>]";

    internal const string ResidentMetric = "resident memory";
    internal const string CgroupSource = "cgroup memory limit";
    internal const string FallbackSource = "the GC memory budget";

    private const long Mib = 1024L * 1024L;

    /// <summary>The expected limit may differ from the reading by this fraction: a container's limit is rounded to a page.</summary>
    private const double ExpectedLimitTolerance = 0.05;

    public static int Run(string[] rest, TextWriter output, TextWriter error)
    {
        ArgumentNullException.ThrowIfNull(rest);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        long? expectedLimitBytes = null;
        if (rest.Length > 1)
        {
            error.WriteLine("Usage: " + UsageLine);
            return 1;
        }

        if (rest.Length == 1)
        {
            if (!TryParseExpectedLimitBytes(rest[0], out var parsed))
            {
                error.WriteLine("Usage: " + UsageLine);
                return 1;
            }

            expectedLimitBytes = parsed;
        }

        if (!OperatingSystem.IsLinux())
        {
            error.WriteLine(Verb + " reads the Linux memory files, so it runs on Linux only.");
            return 1;
        }

        var reading = LaunchMemoryGuard.CreateLinuxSampler(NullLogger.Instance).Sample();
        var cgroupLimit = DarlingStoreHostProfile.ResolveCgroupMemoryLimitBytes(
            DarlingStoreHostProfile.TryReadFile(DarlingStoreHostProfile.ProcSelfCgroupPath), DarlingStoreHostProfile.TryReadFile);

        output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"metric: {reading.Metric}"));
        output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"figure: {reading.Bytes / Mib} MiB"));
        output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"limit: {reading.LimitBytes / Mib} MiB ({reading.LimitSource})"));
        output.WriteLine(cgroupLimit is { } cgroup
            ? string.Create(CultureInfo.InvariantCulture, $"cgroup limit read separately: {cgroup / Mib} MiB")
            : "cgroup limit read separately: none");

        var failures = Evaluate(reading, cgroupLimit, expectedLimitBytes);
        foreach (var failure in failures)
        {
            error.WriteLine("FAILED: " + failure);
        }

        if (failures.Count > 0)
        {
            error.WriteLine(string.Create(CultureInfo.InvariantCulture, $"The launch memory self-check failed ({failures.Count} problem(s))."));
            return 1;
        }

        output.WriteLine("The launch memory self-check passed.");
        return 0;
    }

    /// <summary>
    /// The decision, PURE so a test can feed it readings. Every sentence in the returned list is one way the reader is wrong;
    /// an empty list is a pass. <paramref name="cgroupLimitBytes"/> is the cgroup limit read independently of the sampler, null
    /// when there is none; <paramref name="expectedLimitBytes"/> is the limit the container was started with, null when the
    /// caller does not say.
    /// </summary>
    internal static IReadOnlyList<string> Evaluate(LaunchMemoryReading reading, long? cgroupLimitBytes, long? expectedLimitBytes)
    {
        var failures = new List<string>();

        if (!string.Equals(reading.Metric, ResidentMetric, StringComparison.Ordinal))
        {
            failures.Add($"the metric is '{reading.Metric}', not '{ResidentMetric}'.");
        }

        if (reading.Bytes <= 0)
        {
            failures.Add($"the figure is {reading.Bytes} bytes, so nothing was measured.");
        }

        if (string.Equals(reading.LimitSource, FallbackSource, StringComparison.Ordinal))
        {
            failures.Add("the reader fell back to the GC memory budget, so /proc/self/status or the memory limit files could not be read.");
        }

        /* A cgroup limit above the machine's RAM is not the effective limit, and the guard then rightly says "total RAM". */
        if (cgroupLimitBytes is { } cgroup
            && reading.LimitBytes > cgroup
            && !string.Equals(reading.LimitSource, CgroupSource, StringComparison.Ordinal))
        {
            failures.Add($"the container has a cgroup memory limit of {cgroup} bytes, but the limit came from '{reading.LimitSource}' ({reading.LimitBytes} bytes).");
        }

        if (expectedLimitBytes is { } expected)
        {
            if (!string.Equals(reading.LimitSource, CgroupSource, StringComparison.Ordinal))
            {
                failures.Add($"expected the cgroup limit of {expected / Mib} MiB, but the limit came from '{reading.LimitSource}'.");
            }
            else if (Math.Abs(reading.LimitBytes - expected) > expected * ExpectedLimitTolerance)
            {
                failures.Add($"expected a limit of about {expected / Mib} MiB, but the guard reads {reading.LimitBytes / Mib} MiB.");
            }
        }

        return failures;
    }

    /// <summary>The expected limit as bytes from whole MiB, more than zero.</summary>
    internal static bool TryParseExpectedLimitBytes(string text, out long bytes)
    {
        bytes = 0;
        if (!long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var mib) || mib <= 0 || mib > long.MaxValue / Mib)
        {
            return false;
        }

        bytes = mib * Mib;
        return true;
    }
}
