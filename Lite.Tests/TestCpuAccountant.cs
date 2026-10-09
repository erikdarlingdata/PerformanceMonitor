/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Xunit;
using Xunit.v3;

// #5208 (W2b): charge managed CPU time to the test that spent it, so a later change can cut Lite shards by CPU
// instead of by wall time (wall time includes waiting on the other tests that run in parallel). Measurement
// only: this attribute does not skip, filter or re-order a test.
[assembly: Lite.Tests.TestCpuTime]
[assembly: AssemblyFixture(typeof(Lite.Tests.TestCpuRunSummary))]

namespace Lite.Tests;

/// <summary>
/// One test's running CPU total, in 100 ns units. Written from whichever thread the test's work happens to be on.
/// </summary>
internal sealed class TestCpuCharge
{
    private long _ticks;

    public long Ticks => Interlocked.Read(ref _ticks);

    public void Add(long ticks)
    {
        if (ticks > 0)
        {
            Interlocked.Add(ref _ticks, ticks);
        }
    }
}

/// <summary>
/// The accountant (#5208). An <see cref="AsyncLocal{T}"/> carries the current test's <see cref="TestCpuCharge"/>
/// through every await and every thread-pool hop that flows the execution context. Its value-changed callback runs
/// on the thread that is leaving or entering a context, and there the accountant reads that thread's CPU time and
/// charges what the thread spent since its last reading to the context it is leaving. So CPU spent on a test's own
/// thread, and CPU spent after an await on another pool thread, both land on the same test, and two tests running
/// in parallel are charged separately.
///
/// What it cannot see, by design: native threads (DuckDB's own worker threads), and work queued without the
/// execution context (<c>ThreadPool.UnsafeQueueUserWorkItem</c>, <c>ExecutionContext.SuppressFlow</c>, a
/// dedicated thread started inside <c>SuppressFlow</c>). That work is not charged to any test. The run summary
/// (<see cref="TestCpuRunSummary"/>) reports how much of the process's CPU the charged total covers.
/// Thread CPU comes from <c>GetThreadTimes</c>, whose resolution is the scheduler's accounting tick, so a
/// single short test reads coarse; the sum over many tests is what to trust. Windows only; elsewhere it is a no-op.
/// </summary>
internal static class TestCpuAccountant
{
    /// <summary>Set PM_LITE_TEST_CPU_TIME=off to switch the hook off entirely.</summary>
    internal static readonly bool Enabled =
        OperatingSystem.IsWindows()
        && !string.Equals(Environment.GetEnvironmentVariable("PM_LITE_TEST_CPU_TIME"), "off", StringComparison.OrdinalIgnoreCase);

    private static readonly AsyncLocal<TestCpuCharge?> CurrentCharge = new(OnChanged);

    [ThreadStatic] private static long t_lastMark;
    [ThreadStatic] private static bool t_marked;

    internal static TestCpuCharge? Current => CurrentCharge.Value;

    internal static TestCpuCharge Begin()
    {
        var charge = new TestCpuCharge();
        CurrentCharge.Value = charge;
        return charge;
    }

    /// <summary>
    /// Ends the charge: counts what this thread has spent since its last reading when it is still inside the test's
    /// context, detaches the context, and returns the total in 100 ns ticks.
    /// </summary>
    internal static long End(TestCpuCharge charge)
    {
        if (ReferenceEquals(CurrentCharge.Value, charge) && t_marked)
        {
            charge.Add(ReadAndMark());
        }
        // Setting the value runs OnChanged, which charges nothing further now (the mark above is current).
        if (ReferenceEquals(CurrentCharge.Value, charge))
        {
            CurrentCharge.Value = null;
        }
        return charge.Ticks;
    }

    private static void OnChanged(AsyncLocalValueChangedArgs<TestCpuCharge?> args)
    {
        long delta = 0;
        if (t_marked && args.PreviousValue is not null)
        {
            delta = ReadAndMark();
        }
        else
        {
            t_lastMark = ThreadCpuTicks();
            t_marked = true;
        }
        args.PreviousValue?.Add(delta);
    }

    private static long ReadAndMark()
    {
        long now = ThreadCpuTicks();
        long delta = now - t_lastMark;
        t_lastMark = now;
        return delta;
    }

    internal static long ThreadCpuTicks()
    {
        if (!Enabled)
        {
            return 0;
        }
        return GetThreadTimes(GetCurrentThread(), out _, out _, out long kernel, out long user) ? kernel + user : 0;
    }

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetThreadTimes(IntPtr thread, out long creation, out long exit, out long kernel, out long user);
}

/// <summary>
/// Assembly-level hook (#5208): opens a charge before each test and, after it, adds the attachment <c>cpu-ms</c>
/// (the milliseconds charged) so it lands in the <c>-xml</c> result file beside the test's wall time.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
internal sealed class TestCpuTimeAttribute : BeforeAfterTestAttribute
{
    private static readonly ConcurrentDictionary<string, TestCpuCharge> Open = new();

    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        if (TestCpuAccountant.Enabled)
        {
            Open[test.UniqueID] = TestCpuAccountant.Begin();
        }
    }

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        if (!Open.TryRemove(test.UniqueID, out var charge))
        {
            return;
        }
        long ms = TestCpuAccountant.End(charge) / TimeSpan.TicksPerMillisecond;
        TestCpuRunSummary.Record(ms);
        TestContext.Current.AddAttachment("cpu-ms", ms.ToString(CultureInfo.InvariantCulture));
    }
}

/// <summary>
/// Assembly fixture (#5208): at the end of the run, logs one line with the sum of every <c>cpu-ms</c>, the
/// process's own CPU time, and the ratio of the two (the coverage), and writes the same numbers to a sidecar
/// <c>&lt;xml&gt;.cpu.json</c> next to the <c>-xml</c> file when the run was given one. The sidecar is separate from
/// the XML because the runner owns that file; <c>lite-timing-top.py</c> and <c>lite-shard-pack.py backtest</c> read both.
/// </summary>
public sealed class TestCpuRunSummary : IDisposable
{
    private static long s_chargedMs;
    private static int s_tests;
    private static string? s_sidecarPath;

    /// <summary>
    /// Resolves the sidecar path when the run starts, while the process still has the directory the runner was started in: under
    /// <c>dotnet run</c> the test process's working directory is later the build output folder, so a relative
    /// <c>-xml</c> path resolved at the end of the run would point at a folder that does not exist.
    /// </summary>
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void CaptureSidecarPath()
    {
        string? path = SidecarPath(Environment.GetCommandLineArgs());
        s_sidecarPath = path is null ? null : Path.GetFullPath(path);
    }

    internal static void Record(long ms)
    {
        Interlocked.Add(ref s_chargedMs, ms);
        Interlocked.Increment(ref s_tests);
    }

    internal static string Line(long chargedMs, double processMs, int tests)
    {
        double ratio = processMs > 0 ? chargedMs / processMs : 0;
        return string.Create(CultureInfo.InvariantCulture,
            $"Lite test CPU coverage: charged {chargedMs} ms over {tests} tests, process {processMs:F0} ms, ratio {ratio:P1}");
    }

    internal static string? SidecarPath(string[] args)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i] is "-xml" or "--xml")
            {
                return args[i + 1] + ".cpu.json";
            }
        }
        return null;
    }

    public void Dispose()
    {
        if (!TestCpuAccountant.Enabled)
        {
            return;
        }
        long charged = Interlocked.Read(ref s_chargedMs);
        int tests = Volatile.Read(ref s_tests);
        double processMs = Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds;
        string line = Line(charged, processMs, tests);
        Console.Out.WriteLine(line);
        try
        {
            string? path = s_sidecarPath;
            if (path is not null)
            {
                // The runner writes its own XML after this fixture is disposed, so the folder may not exist yet.
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, string.Create(CultureInfo.InvariantCulture,
                    $"{{\"cpu_ms\":{charged},\"process_cpu_ms\":{processMs:F0},\"tests\":{tests}}}"));
            }
            Console.Out.WriteLine($"Lite test CPU coverage: sidecar {(path ?? "not requested (no -xml argument in: " + string.Join(" ", Environment.GetCommandLineArgs()) + ")")}");
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Console.Out.WriteLine($"Lite test CPU coverage: sidecar not written: {ex.Message}");
        }
    }
}
