/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The headless <c>--test</c> flag (#2005, the half #1954 deferred): runs the SAME layered store
/// connection self-test the connect-failure overlay's button runs — config resolution with the #1966
/// provenance block, then <see cref="StoreConnectionSelfTest.RunAsync"/>'s six-layer ladder — printing to
/// the console and the viewer log without ever creating a window. Exit 0 when no layer failed, 1 otherwise,
/// so an RDP/SSM script can assert on it.
///
/// <para><b>The console dance:</b> a WPF app is a GUI-subsystem binary, so it has no console to print to.
/// <c>AttachConsole(-1)</c> joins the PARENT's console (the PowerShell/cmd that launched us — the honest
/// case for scripted diagnostics); when there is no parent console (double-clicked with <c>--test</c>),
/// <c>AllocConsole</c> creates one so the report is not silently lost. Either way the standard-output
/// stream is rebound, because a GUI process's <c>Console.Out</c> was initialized to the null device before
/// any console existed. One honest limit inherited from the subsystem: the launching shell does not WAIT
/// for a GUI process, so an interactive prompt gets its prompt back immediately and the report prints over
/// it — script with <c>Start-Process -Wait</c> (or <c>start /wait</c>) to sequence on the exit code.</para>
/// </summary>
public static class HeadlessSelfTest
{
    /// <summary>The flag itself. PURE for the arg-contract tests.</summary>
    public static bool IsRequested(string[] args)
        => args.Any(a => string.Equals(a, "--test", StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The explicit config path for a <c>--test</c> run, PURE: an explicit <c>--config &lt;path&gt;</c> pair
    /// wins; otherwise the startup convention applies (first argument that is not an option flag or an
    /// option pair's value — the same rule MainWindow's <c>ExplicitConfigPathFromArgs</c> uses, extended to
    /// skip any <c>--</c>-prefixed flag so <c>--test</c> itself, or the upgrade-takeover flag, is never
    /// mistaken for a file path). Null falls through to <see cref="ViewerSettings.ResolveConfigLocation"/>'s
    /// remaining rules (DARLING_CONFIG, then the conventional locations), exactly like startup.
    /// </summary>
    public static string? ExplicitConfigPath(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            if (string.Equals(args[i], "--config", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                return args[i + 1];
            }
        }

        for (var i = 0; i < args.Length; i++)
        {
            if (string.Equals(args[i], "--open-server", StringComparison.OrdinalIgnoreCase)
                || string.Equals(args[i], "--config", StringComparison.OrdinalIgnoreCase))
            {
                i++; /* Skip the option's value too. */
                continue;
            }

            if (args[i].StartsWith("--", StringComparison.Ordinal))
            {
                continue; /* A bare flag (--test, --upgrade-takeover) is never a config path. */
            }

            return args[i];
        }

        return null;
    }

    /// <summary>Exit-code rule, PURE: 0 when no layer failed (Skipped / NotReached are not failures —
    /// NotReached only ever accompanies a Failed layer anyway), 1 otherwise.</summary>
    public static int ExitCodeFor(IReadOnlyList<StoreConnectionSelfTest.LayerResult> results)
        => results.Any(r => r.Outcome == StoreConnectionSelfTest.LayerOutcome.Failed) ? 1 : 0;

    /// <summary>
    /// The whole headless run: resolve config exactly like startup (explicit path → DARLING_CONFIG →
    /// conventional locations), print the provenance block, load, print the non-secret connection summary,
    /// run the ladder, print the report. Every failure path returns 1 with the reason printed — never a
    /// throw, because the exit code IS the interface.
    /// </summary>
    public static async Task<int> RunAsync(string? explicitConfigPath)
    {
        AttachOrAllocConsole();
        try
        {
            ViewerLogger.Initialize();
        }
        catch
        {
            /* Console output still works; the log trail is best-effort in headless mode. */
        }

        var location = ViewerSettings.ResolveConfigLocation(explicitConfigPath);
        foreach (var line in ViewerConfigDiagnostics.DescribeConfigLocation(location))
        {
            Print(line);
        }

        ViewerSettings? settings;
        try
        {
            settings = ViewerSettings.TryLoad(location.Path);
        }
        catch (Exception ex)
        {
            Print($"darling.json could not be read: {ex.Message}");
            return Finish(1);
        }

        if (settings is null)
        {
            Print("darling.json not found — copy darling.sample.json from the service and point DARLING_CONFIG at it (or pass --config <path>).");
            return Finish(1);
        }

        foreach (var line in ViewerConfigDiagnostics.DescribeConnection(
            settings.ConfiguredConnectionString, settings.Managed, location.Directory))
        {
            Print(line);
        }

        Print("");

        /* #2578: test the string STARTUP uses, not the raw one. MainWindow builds its ViewerDataService as
           new ViewerDataService(settings.ConnectionString, appSettings.ConnectionTimeoutSeconds), and that
           constructor rewrites the string through ApplyConnectionTimeout. This probe used the raw string, so
           the two diverged the moment a Connection timeout preference existed — which it always does, since
           ViewerAppSettings defaults it to 5 and clamps it to 5..60.

           A diagnostic that validates a different connection string than the application opens cannot do the
           job it exists for: it can pass every layer while startup fails, which is precisely the report that
           found this, and it makes "self-test passes but the Viewer will not connect" look like a
           contradiction rather than the expected consequence it is. Read the preference the same way
           MainWindow does, apply the same transform, and say so when it changed anything. */
        var effectiveConnectionString = settings.ConnectionString;
        try
        {
            var appSettings = new ViewerAppSettingsStore().Load();
            effectiveConnectionString =
                ViewerDataService.ApplyConnectionTimeout(settings.ConnectionString, appSettings.ConnectionTimeoutSeconds);
            if (!string.Equals(effectiveConnectionString, settings.ConnectionString, StringComparison.Ordinal))
            {
                Print($"[note ] applied the Viewer's Connection timeout preference ({appSettings.ConnectionTimeoutSeconds}s) — "
                    + "testing the same connection string startup uses, not the raw darling.json one");
            }
        }
        catch (Exception ex)
        {
            /* Unreadable viewer settings must not stop the probe: the raw string is still worth testing, and
               saying which one was tested keeps the result interpretable. */
            Print($"[note ] could not read the Viewer's settings ({ex.Message}) — testing darling.json's connection string as-is, "
                + "which may differ from what startup opens");
        }

        var results = await StoreConnectionSelfTest.RunAsync(effectiveConnectionString);
        foreach (var line in StoreConnectionSelfTest.FormatReport(results).Split('\n'))
        {
            Print(line.TrimEnd());
        }

        return Finish(ExitCodeFor(results));
    }

    private static int Finish(int exitCode)
    {
        Print($"Exit code: {exitCode}");
        try
        {
            ViewerLogger.Flush();
        }
        catch
        {
            /* Best-effort. */
        }

        return exitCode;
    }

    private static void Print(string line)
    {
        Console.Out.WriteLine(line);
        try
        {
            ViewerLogger.Info(ViewerConfigDiagnostics.LogSource, line);
        }
        catch
        {
            /* Logging is the secondary channel here. */
        }
    }

    /* ── the GUI-subsystem console dance ── */

    private const int AttachParentProcess = -1;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AttachConsole(int dwProcessId);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool AllocConsole();

    private static void AttachOrAllocConsole()
    {
        if (!OperatingSystem.IsWindows())
        {
            return; /* Non-Windows dev builds already have a console. */
        }

        if (!AttachConsole(AttachParentProcess))
        {
            AllocConsole();
        }

        /* A GUI process's Console.Out was bound to the null device before any console existed —
           rebind it to the (now real) standard output handle. */
        var stdout = new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true };
        Console.SetOut(stdout);
        Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });
    }
}
