using System;
using System.IO;
using System.Threading;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;

namespace Darling.Tests;

/// <summary>
/// TEMPORARY (#5596): finds what keeps the Darling.Tests process alive after the run returns. Only active when the
/// diag workflow sets DIAG_5596=1. Writes timestamped lines to a file (not stdout, which the runner owns) from a
/// background thread every second, plus the exit stages: AssemblyLoadContext.Unloading, the first ProcessExit
/// handler (which also lists every registered handler), and the last one. Deleted before the fix merges.
/// </summary>
internal static class Diag5596
{
    private static readonly string LogPath = Path.Combine(
        Environment.GetEnvironmentVariable("RUNNER_TEMP") ?? Path.GetTempPath(), "diag5596-heartbeat.txt");

    private static readonly object Gate = new();
    private static int _stage; // 0 running, 1 unloading seen, 2 first process-exit handler seen, 3 last handler seen

    [ModuleInitializer]
    internal static void Init()
    {
        if (Environment.GetEnvironmentVariable("DIAG_5596") != "1")
        {
            return;
        }

        _mainOsThread = GetCurrentThreadId();
        Log("module initializer ran; pid " + Environment.ProcessId + " main os thread " + _mainOsThread);
        AssemblyLoadContext.Default.Unloading += _ => { _stage = Math.Max(_stage, 1); Log("AssemblyLoadContext.Default.Unloading raised"); };
        AppDomain.CurrentDomain.ProcessExit += First;
        AppDomain.CurrentDomain.ProcessExit += Last;
        var beat = new Thread(Beat) { IsBackground = true, Name = "diag5596-heartbeat" };
        beat.Start();
    }

    internal static void Note(string text) => Log(text);

    private static void First(object? sender, EventArgs e)
    {
        _stage = Math.Max(_stage, 2);
        Log("FIRST ProcessExit handler running on managed thread " + Environment.CurrentManagedThreadId);
        try
        {
            foreach (var owner in new[] { typeof(AppContext), typeof(AppDomain) })
            {
                foreach (var field in owner.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance))
                {
                    if (field.FieldType != typeof(EventHandler) || !field.Name.Contains("xit", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    var value = field.GetValue(field.IsStatic ? null : AppDomain.CurrentDomain) as Delegate;
                    Log("  field " + owner.Name + "." + field.Name + (value is null ? " = null" : ""));
                    var list = value?.GetInvocationList() ?? Array.Empty<Delegate>();
                    Log("    " + list.Length + " handlers");
                    foreach (var d in list)
                    {
                        Log("    handler: " + d.Method.DeclaringType?.FullName + "." + d.Method.Name + " target=" + (d.Target?.GetType().FullName ?? "static"));
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log("  handler list failed: " + ex.GetType().Name);
        }
    }

    private static void Last(object? sender, EventArgs e)
    {
        _stage = 3;
        Log("LAST ProcessExit handler running");
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern uint GetCurrentThreadId();

    private static uint _mainOsThread;

    private static void Beat()
    {
        var clock = Stopwatch.StartNew();
        var n = 0;
        var lastCpu = new System.Collections.Generic.Dictionary<int, double>();
        while (true)
        {
            Thread.Sleep(500);
            n++;
            var detail = "";
            int threads;
            try
            {
                using var me = Process.GetCurrentProcess();
                threads = me.Threads.Count;
                if (_stage >= 1)
                {
                    foreach (ProcessThread t in me.Threads)
                    {
                        double cpu;
                        try { cpu = t.TotalProcessorTime.TotalMilliseconds; } catch { cpu = -1; }
                        lastCpu.TryGetValue(t.Id, out var before);
                        lastCpu[t.Id] = cpu;
                        var wait = t.ThreadState == System.Diagnostics.ThreadState.Wait ? t.WaitReason.ToString() : "";
                        detail += $" [{t.Id}{(t.Id == _mainOsThread ? "=main" : "")} {t.ThreadState} {wait} +{cpu - before:0}ms]";
                    }
                }
            }
            catch
            {
                threads = -1;
            }

            Log($"beat {n} stage={_stage} osThreads={threads} elapsed={clock.Elapsed.TotalSeconds:0.0}s gc={GC.CollectionCount(0)}/{GC.CollectionCount(1)}/{GC.CollectionCount(2)}{detail}");
            if (_stage == 0)
            {
                try
                {
                    /* keep the LAST handler last: handlers run in registration order */
                    AppDomain.CurrentDomain.ProcessExit -= Last;
                    AppDomain.CurrentDomain.ProcessExit += Last;
                }
                catch
                {
                    /* diag only */
                }
            }
        }
    }

    private static void Log(string text)
    {
        try
        {
            lock (Gate)
            {
                File.AppendAllText(LogPath, DateTime.UtcNow.ToString("HH:mm:ss.fff") + " " + text + Environment.NewLine);
            }
        }
        catch
        {
            /* diag only */
        }
    }
}
