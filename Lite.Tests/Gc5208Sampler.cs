// EXPERIMENT (#5208, never merged): split the unsharded Lite run's memory growth into managed and native.
//
// What is known: in one process the Lite suite runs as fast as the sharded jobs for ~11 minutes, then 2-4.6x
// slower, while the process's committed memory grows to ~15 GB over ~106 minutes (mostly not resident, free RAM
// stays above 9 GB). The open question is whether that growth is the GC heap (managed) or native memory
// (for example DuckDB databases or connections that are never closed).
//
// This file lives on the exp/5208-gc-sample branch only. A module initializer starts a dedicated background
// thread (not a thread-pool timer: a pool starved by the slowdown would stop sampling exactly when the data
// matters) that appends one CSV row every 30 seconds and flushes it, so a timeout still leaves the data. The
// CSV path is PM_5208_SAMPLE_CSV, or TestResults/lite-gc-sample.csv under the current directory; the period is
// 30 seconds unless PM_5208_SAMPLE_SECONDS says otherwise (the local smoke run uses 2).
//
// The DuckDB columns do not come from counters at the creation sites. Lite opens DuckDB connections from
// scattered places (eleven `new DuckDBConnection` sites in product code, a hundred more in the tests), so
// there is no one place to put an opened-minus-disposed counter. DuckDB.NET itself, though, funnels every
// file-backed open through ConnectionManager.ConnectionCache (a database file name -> FileReference whose
// ConnectionCount is the number of open connections), and drops the entry when the last connection closes.
// Reading that cache by reflection counts live native databases and live native connections no matter who
// created them; -1 means the cache could not be read. In-memory databases are not in that cache.

using System;
using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Xunit.v3;

[assembly: PerformanceMonitorLite.Tests.Gc5208CountTests]

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Assembly-level hook that counts tests as they start and finish, so each sample row says how far the run is.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class Gc5208CountTestsAttribute : BeforeAfterTestAttribute
{
    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        Interlocked.Increment(ref Gc5208Sampler.TestsStarted);
        Gc5208Sampler.LastClass = test.TestCase.TestClassSimpleName ?? "";
    }

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        Interlocked.Increment(ref Gc5208Sampler.TestsFinished);
    }
}

internal static class Gc5208Sampler
{
    internal static long TestsStarted;
    internal static long TestsFinished;
    internal static volatile string LastClass = "";

    private const string CsvEnv = "PM_5208_SAMPLE_CSV";
    private const string SecondsEnv = "PM_5208_SAMPLE_SECONDS";
    private const int OpenDbSnapshotEveryTicks = 20;
    private const int OpenDbSnapshotMaxLines = 300;

    private static readonly object WriteGate = new();
    private static readonly ManualResetEventSlim StopSignal = new(false);
    private static readonly Stopwatch Clock = new();
    private static StreamWriter? _csv;
    private static string _openDbsPath = "";
    private static Process? _self;
    private static bool _headerWritten;
    private static long _ticks;
    private static bool _duckLooked;
    private static IDictionary? _duckCache;
    private static PropertyInfo? _duckConnectionCount;

    [ModuleInitializer]
    internal static void Start()
    {
        try
        {
            var csv = Environment.GetEnvironmentVariable(CsvEnv);
            if (string.IsNullOrWhiteSpace(csv))
                csv = Path.Combine(Directory.GetCurrentDirectory(), "TestResults", "lite-gc-sample.csv");
            csv = Path.GetFullPath(csv);
            var dir = Path.GetDirectoryName(csv)!;
            Directory.CreateDirectory(dir);
            _openDbsPath = Path.Combine(dir, "lite-gc-sample-open-dbs.txt");

            var seconds = 30;
            if (int.TryParse(Environment.GetEnvironmentVariable(SecondsEnv), NumberStyles.None, CultureInfo.InvariantCulture, out var s) && s >= 1)
                seconds = s;

            _csv = new StreamWriter(new FileStream(csv, FileMode.Create, FileAccess.Write, FileShare.ReadWrite), new UTF8Encoding(false))
            {
                AutoFlush = true,
            };
            Clock.Start();
            WriteMeta(Path.Combine(dir, "lite-gc-sample-meta.txt"), seconds);

            var thread = new Thread(() => Loop(TimeSpan.FromSeconds(seconds)))
            {
                IsBackground = true,
                Name = "gc5208-sampler",
                Priority = ThreadPriority.AboveNormal,
            };
            thread.Start();
            AppDomain.CurrentDomain.ProcessExit += (_, _) => OnExit();
        }
        catch
        {
            // An experiment must never stop the suite from starting.
        }
    }

    private static void Loop(TimeSpan period)
    {
        try { WriteRow("start"); } catch { }
        var next = period;
        while (true)
        {
            var due = next - Clock.Elapsed;
            if (due > TimeSpan.Zero && StopSignal.Wait(due)) break;
            if (StopSignal.IsSet) break;
            try { WriteRow("tick"); } catch { }
            next += period;
        }
    }

    private static void OnExit()
    {
        StopSignal.Set();
        try { WriteRow("exit"); } catch { }
        WriteOpenDbs();
    }

    private static void WriteRow(string kind)
    {
        var row = new Row();
        row.Add("utc", DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture));
        row.Add("elapsed_s", Clock.Elapsed.TotalSeconds, "0.0");
        row.Add("kind", kind);

        // Managed side: the GC's own view.
        var gc = GC.GetGCMemoryInfo();
        var gens = gc.GenerationInfo;
        row.Add("gc_heap_bytes", gc.HeapSizeBytes);
        row.Add("gc_committed_bytes", gc.TotalCommittedBytes);
        row.Add("gc_fragmented_bytes", gc.FragmentedBytes);
        row.Add("gc_pause_pct", gc.PauseTimePercentage, "0.00");
        row.Add("gc_pause_total_ms", GC.GetTotalPauseDuration().TotalMilliseconds, "0.0");
        row.Add("gc0", GC.CollectionCount(0));
        row.Add("gc1", GC.CollectionCount(1));
        row.Add("gc2", GC.CollectionCount(2));
        row.Add("gc_last_gen", gc.Generation);
        row.Add("gc_gen2_after_bytes", gens.Length > 2 ? gens[2].SizeAfterBytes : -1);
        row.Add("gc_loh_after_bytes", gens.Length > 3 ? gens[3].SizeAfterBytes : -1);
        row.Add("gc_poh_after_bytes", gens.Length > 4 ? gens[4].SizeAfterBytes : -1);
        row.Add("gc_total_memory", GC.GetTotalMemory(false));
        row.Add("gc_allocated_total", GC.GetTotalAllocatedBytes(false));

        // Whole process: private bytes minus the GC's committed bytes is the native share.
        _self ??= Process.GetCurrentProcess();
        _self.Refresh();
        row.Add("private_bytes", _self.PrivateMemorySize64);
        row.Add("working_set", _self.WorkingSet64);
        row.Add("peak_working_set", _self.PeakWorkingSet64);
        row.Add("virtual_bytes", _self.VirtualMemorySize64);
        row.Add("handles", _self.HandleCount);
        row.Add("threads", _self.Threads.Count);
        row.Add("cpu_s", _self.TotalProcessorTime.TotalSeconds, "0.0");
        row.Add("tp_threads", ThreadPool.ThreadCount);
        row.Add("tp_pending", ThreadPool.PendingWorkItemCount);
        row.Add("lock_contention", Monitor.LockContentionCount);

        // The prior experiment's per-process sums over every Lite.Tests / dotnet process, for comparison.
        AddProcessSums(row);

        // Native side: live DuckDB databases and connections, read from the library's own cache.
        var (dbs, conns) = DuckLive();
        row.Add("duckdb_dbs", dbs);
        row.Add("duckdb_conns", conns);

        row.Add("tests_started", Interlocked.Read(ref TestsStarted));
        row.Add("tests_finished", Interlocked.Read(ref TestsFinished));
        row.Add("last_class", LastClass);

        // Whole machine: the prior experiment's free MB and committed percent, plus the commit and kernel pools.
        AddMachine(row);

        lock (WriteGate)
        {
            if (_csv is null) return;
            if (!_headerWritten)
            {
                _csv.WriteLine(row.Names);
                _headerWritten = true;
            }
            _csv.WriteLine(row.Values);
        }

        var tick = kind == "tick" ? Interlocked.Increment(ref _ticks) : 0;
        if (kind == "start" || (kind == "tick" && tick % OpenDbSnapshotEveryTicks == 0)) WriteOpenDbs();
    }

    private static void AddProcessSums(Row row)
    {
        long privateBytes = 0, workingSet = 0;
        try
        {
            foreach (var p in Process.GetProcesses())
            {
                try
                {
                    var name = p.ProcessName;
                    if (name.Equals("Lite.Tests", StringComparison.OrdinalIgnoreCase) || name.Equals("dotnet", StringComparison.OrdinalIgnoreCase))
                    {
                        privateBytes += p.PrivateMemorySize64;
                        workingSet += p.WorkingSet64;
                    }
                }
                catch
                {
                    // A process that exits mid-read is simply skipped.
                }
                finally
                {
                    p.Dispose();
                }
            }
        }
        catch
        {
            privateBytes = workingSet = -1;
        }
        row.Add("sum_private_mb", privateBytes < 0 ? -1.0 : privateBytes / 1048576.0, "0.0");
        row.Add("sum_ws_mb", workingSet < 0 ? -1.0 : workingSet / 1048576.0, "0.0");
    }

    private static void AddMachine(Row row)
    {
        var info = new PerformanceInformation { cb = Marshal.SizeOf<PerformanceInformation>() };
        var ok = false;
        try { ok = GetPerformanceInfo(ref info, info.cb); } catch { }
        double Mb(nuint pages) => ok ? (double)pages * (double)info.PageSize / 1048576.0 : -1;
        row.Add("mach_free_mb", Mb(info.PhysicalAvailable), "0.0");
        row.Add("mach_total_mb", Mb(info.PhysicalTotal), "0.0");
        row.Add("mach_committed_pct", ok && info.CommitLimit != 0 ? 100.0 * (double)info.CommitTotal / (double)info.CommitLimit : -1, "0.0");
        row.Add("mach_commit_mb", Mb(info.CommitTotal), "0.0");
        row.Add("mach_commit_limit_mb", Mb(info.CommitLimit), "0.0");
        row.Add("mach_kernel_paged_mb", Mb(info.KernelPaged), "0.0");
        row.Add("mach_kernel_nonpaged_mb", Mb(info.KernelNonpaged), "0.0");
        row.Add("mach_procs", ok ? info.ProcessCount : -1);
        row.Add("mach_threads", ok ? info.ThreadCount : -1);
    }

    private static (long Dbs, long Conns) DuckLive()
    {
        try
        {
            if (!_duckLooked)
            {
                _duckLooked = true;
                var asm = typeof(DuckDB.NET.Data.DuckDBConnection).Assembly;
                const BindingFlags any = BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
                var cache = asm.GetType("DuckDB.NET.Data.Connection.ConnectionManager")?.GetField("ConnectionCache", any)?.GetValue(null);
                _duckCache = cache as IDictionary;
                _duckConnectionCount = asm.GetType("DuckDB.NET.Data.Connection.FileReference")?.GetProperty("ConnectionCount", any);
            }
            if (_duckCache is null || _duckConnectionCount is null) return (-1, -1);

            long dbs = 0, conns = 0;
            var e = _duckCache.GetEnumerator();
            while (e.MoveNext())
            {
                dbs++;
                if (e.Value is not null) conns += Convert.ToInt64(_duckConnectionCount.GetValue(e.Value), CultureInfo.InvariantCulture);
            }
            return (dbs, conns);
        }
        catch
        {
            return (-1, -1);
        }
    }

    // The database files DuckDB.NET still holds open, overwritten each time: after a timeout the last snapshot
    // survives, and the exit snapshot names every database the suite never closed.
    private static void WriteOpenDbs()
    {
        try
        {
            if (_openDbsPath.Length == 0 || _duckCache is null || _duckConnectionCount is null) return;
            var sb = new StringBuilder();
            sb.Append("utc=").Append(DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture)).Append('\n');
            var lines = new StringBuilder();
            long dbs = 0;
            var e = _duckCache.GetEnumerator();
            while (e.MoveNext())
            {
                dbs++;
                if (dbs > OpenDbSnapshotMaxLines) continue;
                var file = e.Key?.ToString() ?? "";
                lines.Append(Convert.ToString(_duckConnectionCount.GetValue(e.Value!), CultureInfo.InvariantCulture))
                    .Append('\t').Append(File.Exists(file) ? '1' : '0')
                    .Append('\t').Append(file).Append('\n');
            }
            sb.Append("open_databases=").Append(dbs).Append(" (connections<TAB>file_exists<TAB>file, first ").Append(OpenDbSnapshotMaxLines).Append(")\n").Append(lines);
            File.WriteAllText(_openDbsPath, sb.ToString());
        }
        catch
        {
            // Best effort.
        }
    }

    private static void WriteMeta(string path, int seconds)
    {
        try
        {
            var sb = new StringBuilder();
            sb.Append("utc_start=").Append(DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture)).Append('\n');
            sb.Append("sample_period_s=").Append(seconds).Append('\n');
            sb.Append("os=").Append(RuntimeInformation.OSDescription).Append('\n');
            sb.Append("framework=").Append(RuntimeInformation.FrameworkDescription).Append('\n');
            sb.Append("process_arch=").Append(RuntimeInformation.ProcessArchitecture).Append('\n');
            sb.Append("processor_count=").Append(Environment.ProcessorCount).Append('\n');
            sb.Append("gc_server=").Append(GCSettings.IsServerGC).Append('\n');
            sb.Append("gc_latency_mode=").Append(GCSettings.LatencyMode).Append('\n');
            sb.Append("gc_concurrent_config=").Append(AppContext.GetData("System.GC.Concurrent")).Append('\n');
            sb.Append("gc_total_available_memory_bytes=").Append(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes).Append('\n');
            foreach (System.Collections.DictionaryEntry v in Environment.GetEnvironmentVariables())
            {
                var key = v.Key?.ToString() ?? "";
                if (key.StartsWith("DOTNET_", StringComparison.OrdinalIgnoreCase) || key.StartsWith("COMPlus_", StringComparison.OrdinalIgnoreCase))
                    sb.Append("env.").Append(key).Append('=').Append(v.Value).Append('\n');
            }
            File.WriteAllText(path, sb.ToString());
        }
        catch
        {
            // Best effort.
        }
    }

    [DllImport("psapi.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetPerformanceInfo(ref PerformanceInformation info, int size);

    [StructLayout(LayoutKind.Sequential)]
    private struct PerformanceInformation
    {
        public int cb;
        public nuint CommitTotal;
        public nuint CommitLimit;
        public nuint CommitPeak;
        public nuint PhysicalTotal;
        public nuint PhysicalAvailable;
        public nuint SystemCache;
        public nuint KernelTotal;
        public nuint KernelPaged;
        public nuint KernelNonpaged;
        public nuint PageSize;
        public int HandleCount;
        public int ProcessCount;
        public int ThreadCount;
    }

    // One CSV row: names and values are appended together so the header can never drift from the data.
    private sealed class Row
    {
        private readonly StringBuilder _names = new();
        private readonly StringBuilder _values = new();

        public string Names => _names.ToString();
        public string Values => _values.ToString();

        public void Add(string name, long value) => Put(name, value.ToString(CultureInfo.InvariantCulture));

        public void Add(string name, double value, string format) => Put(name, value.ToString(format, CultureInfo.InvariantCulture));

        public void Add(string name, string value)
        {
            var needsQuotes = value.IndexOfAny(CsvSpecials) >= 0;
            Put(name, needsQuotes ? "\"" + value.Replace("\"", "\"\"") + "\"" : value);
        }

        private static readonly char[] CsvSpecials = [',', '"', '\r', '\n'];

        private void Put(string name, string value)
        {
            if (_names.Length > 0)
            {
                _names.Append(',');
                _values.Append(',');
            }
            _names.Append(name);
            _values.Append(value);
        }
    }
}
