/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9142: what one start of Lite does, decided from its command line before anything is built. Lite can run with no
/// window, no tray icon and no signed-in user (a scheduled task at machine start), and a second start can ask the running
/// one to shut down. <see cref="App"/> reads the plan once and every start-up step that would put something on a screen
/// asks it first.
/// </summary>
public sealed record LiteStartupPlan(bool Headless, bool Stop, bool PrintMcpToken = false, bool AddServer = false)
{
    public const string HeadlessArg = "--headless";
    public const string StopArg = "--stop";

    /// <summary>#9185: prints the stored MCP token to standard output and exits (Darling's <c>--print-mcp-token</c>).</summary>
    public const string PrintMcpTokenArg = "--print-mcp-token";

    /// <summary>#9237: adds monitored servers without the window and exits (Darling's <c>--add-server</c>). <c>--add-servers</c> is the same verb.</summary>
    public const string AddServerArg = "--add-server";
    public const string AddServersArg = "--add-servers";

    public static LiteStartupPlan FromArgs(IEnumerable<string> args)
    {
        var headless = false;
        var stop = false;
        var printMcpToken = false;
        var addServer = false;
        foreach (var arg in args)
        {
            if (string.Equals(arg, HeadlessArg, StringComparison.OrdinalIgnoreCase))
                headless = true;
            else if (string.Equals(arg, StopArg, StringComparison.OrdinalIgnoreCase))
                stop = true;
            else if (string.Equals(arg, PrintMcpTokenArg, StringComparison.OrdinalIgnoreCase))
                printMcpToken = true;
            else if (string.Equals(arg, AddServerArg, StringComparison.OrdinalIgnoreCase)
                || string.Equals(arg, AddServersArg, StringComparison.OrdinalIgnoreCase))
                addServer = true;
        }

        return new LiteStartupPlan(headless, stop, printMcpToken, addServer);
    }

    /// <summary>
    /// A stop request starts nothing: it asks the running instance to stop and leaves. So does a token print (#9185): it reads one
    /// Credential Manager entry and leaves, with no window, no tray icon, no data folder, no log file and no single-instance lock.
    /// </summary>
    public bool StartsServices => !Stop && !PrintMcpToken && !AddServer;

    /// <summary>The single-instance lock is taken by a start that runs Lite. A stop, a token print or an add leaves a running Lite alone (the add refuses to run beside one, see AddServerCommand).</summary>
    public bool TakesInstanceLock => StartsServices;

    /// <summary>The main window is shown. Headless never shows it, and never creates a window handle for it.</summary>
    public bool ShowsMainWindow => StartsServices && !Headless;

    /// <summary>The tray icon exists. It lives on the taskbar, which a start with no signed-in user does not have.</summary>
    public bool CreatesTrayIcon => ShowsMainWindow;

    /// <summary>
    /// A start that nobody is at the screen for takes over from an older running instance without asking. The elevated
    /// relaunch of the upgrade handoff already does the same, through its own flag.
    /// </summary>
    public bool ConfirmsHandoffWithoutAsking => Headless;

    /// <summary>The exit code of a headless start whose initialization failed. Any nonzero code is a failure to Task Scheduler.</summary>
    public const int ExitInitFailed = 3;

    /// <summary>
    /// #9197: the single-instance mutex could not be created or opened and no other instance is running, so nothing will ever
    /// open. Both start modes end with <see cref="ExitInitFailed"/>, so a scheduled task sees a failure and its restart-on-failure
    /// can fire. A start with a window first shows <paramref name="showDialog"/> with the reason, once, because the person at
    /// the screen has no other way to learn why Lite closed. A headless start shows nothing: its reason goes to the log.
    /// </summary>
    /// <returns>The exit code for the start.</returns>
    public int ExitCodeAfterOwnershipFailure(string reason, Action<string> showDialog)
    {
        if (!Headless)
            showDialog(reason);

        return ExitInitFailed;
    }

    /// <summary>
    /// What a failed initialization does. A start with a window shows the error dialog and stays open, so the person at the
    /// screen can read it. A headless start has no one to read a dialog, and a process that stays up with no listener would hold
    /// the single-instance mutex for good: <c>--stop</c> would find nobody, a second <c>--headless</c> start would exit, and a
    /// task's restart-on-failure would never fire. So it shuts down with <see cref="ExitInitFailed"/> (#9142).
    /// </summary>
    /// <param name="shutdown">Ends the process with the given exit code.</param>
    /// <returns>True when the start was ended, so the caller skips the dialog.</returns>
    public bool EndsStartAfterInitFailure(Action<int> shutdown)
    {
        if (!Headless)
            return false;

        shutdown(ExitInitFailed);
        return true;
    }
}

/// <summary>
/// #9142: the screen-bound steps a headless start skips. Each is skipped and logged once at Information, by name, so an
/// operator reading the log sees what Lite would have shown and did not.
/// </summary>
public static class HeadlessUi
{
    private static readonly HashSet<string> s_logged = [];

    /// <summary>Whether this process runs without a window. Set once by <see cref="App"/> at the start of OnStartup.</summary>
    public static bool IsHeadless { get; set; }

    /// <summary>The log sink. Production writes to AppLogger; a test passes its own.</summary>
    public static Action<string> Info { get; set; } = static _ => { };

    /// <summary>
    /// True when this process is headless: the caller skips what it was about to show. The first skip of each
    /// <paramref name="what"/> is logged at Information.
    /// </summary>
    public static bool Skips(string what)
    {
        if (!IsHeadless)
            return false;

        bool first;
        lock (s_logged)
            first = s_logged.Add(what);

        if (first)
            Info($"Headless: skipped {what}.");

        return true;
    }
}

/// <summary>
/// #9142: builds the tray icon for a start that wants one, and says so in the log when it does not get one.
/// </summary>
public static class TrayIconStarter
{
    /// <summary>
    /// The tray service, or null. Null for a headless start (logged at Information) and for a start whose tray icon
    /// could not be created (one Warning that names the cause). Collection, alerts and the other delivery channels do not
    /// depend on it.
    /// </summary>
    public static T? TryStart<T>(LiteStartupPlan plan, Func<T> create, Action<string> info, Action<string> warn)
        where T : class
    {
        if (!plan.CreatesTrayIcon)
        {
            info("Headless: no tray icon is created.");
            return null;
        }

        try
        {
            return create();
        }
        catch (Exception ex)
        {
            warn($"The tray icon could not be created ({ex.Message}). Tray notifications are off for this run; "
                + "email, Slack, Teams and webhook alerts are not affected.");
            return null;
        }
    }
}

/// <summary>
/// #9142: the tray as one alert channel that can be unavailable. With no taskbar (a session no one is signed in to, or
/// Explorer not running) every balloon throws "Failed to find an AppBar that matched the given criteria" from the tray
/// library. The first such failure turns the channel off for the rest of the run and logs one Warning that names the cause,
/// instead of an Error with a stack for every alert. A failure of any other kind is not a missing taskbar and may not repeat,
/// so it does not turn the channel off: it is logged once per distinct message, and the next balloon is tried.
/// </summary>
public sealed class TrayChannelGate
{
    private readonly Action<string> _warn;
    private readonly HashSet<string> _warned = [];
    private readonly object _lock = new();
    private volatile bool _unavailable;

    public TrayChannelGate(Action<string> warn)
    {
        _warn = warn ?? throw new ArgumentNullException(nameof(warn));
    }

    /// <summary>True once the tray has failed for want of a taskbar. Every later call returns false without trying.</summary>
    public bool Unavailable => _unavailable;

    /// <summary>
    /// Runs one tray notification. True only when it ran and reported that it was shown. Never throws: a failing tray must
    /// not take the alert's other channels down with it.
    /// </summary>
    public bool Run(Func<bool> show)
    {
        if (_unavailable)
            return false;

        try
        {
            return show();
        }
        catch (Exception ex)
        {
            var noTaskbar = IsNoTaskbar(ex);
            string? message = null;
            lock (_lock)
            {
                if (noTaskbar)
                {
                    if (!_unavailable)
                    {
                        _unavailable = true;
                        message = $"Tray notifications are unavailable: there is no taskbar to show them on ({ex.Message}). "
                            + "Tray delivery is skipped for the rest of this run; email, Slack, Teams and webhook alerts are not affected. "
                            + "Start Lite with --headless to run with no tray.";
                    }
                }
                else if (_warned.Add(ex.Message))
                {
                    message = $"A tray notification failed ({ex.GetType().Name}: {ex.Message}). Other alert channels are not affected.";
                }
            }

            if (message is not null)
                _warn(message);

            return false;
        }
    }

    /// <summary>The tray library throws a plain exception from AppBarInfo.GetPosition when the taskbar is missing.</summary>
    internal static bool IsNoTaskbar(Exception ex)
    {
        for (Exception? e = ex; e is not null; e = e.InnerException)
        {
            if (e.Message.Contains("AppBar", StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}

/// <summary>
/// #9142: <c>--stop</c>. A scheduled task's End (and Stop-Process) ends the process without running any shutdown code. A
/// second start with <c>--stop</c> instead asks the running instance to shut down through the same channel the upgrade
/// handoff uses, then waits for it to go.
/// </summary>
public static class StopCommand
{
    public const int ExitStopped = 0;
    public const int ExitNotRunning = 1;
    public const int ExitStillRunning = 2;

    /// <summary>
    /// Asks the running instance to stop and waits up to <paramref name="timeout"/> for it to exit.
    /// </summary>
    /// <param name="signal">Sends the stop request. True when a running instance was listening.</param>
    /// <param name="isRunning">Whether another Lite process still exists.</param>
    /// <param name="pause">Waits between checks; a test passes a no-op.</param>
    public static int Run(Func<bool> signal, Func<bool> isRunning, TimeSpan timeout, Action<TimeSpan> pause, Action<string> log)
    {
        if (!signal())
        {
            log("--stop: no running instance is listening. Nothing to stop, or it is still starting.");
            return ExitNotRunning;
        }

        log("--stop: asked the running instance to shut down.");
        var step = TimeSpan.FromMilliseconds(250);
        for (var waited = TimeSpan.Zero; waited < timeout; waited += step)
        {
            if (!isRunning())
            {
                log("--stop: the running instance has exited.");
                return ExitStopped;
            }

            pause(step);
        }

        if (!isRunning())
        {
            log("--stop: the running instance has exited.");
            return ExitStopped;
        }

        log($"--stop: the running instance had not exited after {timeout.TotalSeconds:0} seconds.");
        return ExitStillRunning;
    }

    /// <summary>One running process, as far as <see cref="OtherInstanceInSession"/> needs to know it.</summary>
    public readonly record struct RunningProcess(int Id, int SessionId);

    /// <summary>
    /// True when a process of this name other than this one is running in this process's Windows session. The stop signal is a
    /// named event that reaches only the current session, so a Lite in another session can neither hear it nor be waited on.
    /// </summary>
    public static bool OtherInstanceRunning(string processName)
    {
        var self = Environment.ProcessId;
        int selfSession;
        using (var current = Process.GetCurrentProcess())
            selfSession = current.SessionId;
        var processes = new List<RunningProcess>();
        foreach (var process in Process.GetProcessesByName(processName))
        {
            using (process)
            {
                try
                {
                    processes.Add(new RunningProcess(process.Id, process.SessionId));
                }
                catch (InvalidOperationException)
                {
                    /* The process exited between the listing and the read: it is not running. */
                }
            }
        }

        return OtherInstanceInSession(processes, self, selfSession);
    }

    /// <summary>The decision behind <see cref="OtherInstanceRunning"/>, over a given process list.</summary>
    public static bool OtherInstanceInSession(IEnumerable<RunningProcess> processes, int selfId, int selfSession)
    {
        foreach (var process in processes)
        {
            if (process.Id != selfId && process.SessionId == selfSession)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Writes the lines <c>--stop</c> logged to the Lite log, only when the log folder already exists. <c>--stop</c> starts
    /// nothing, so it never creates the data folder, and a machine that never ran Lite has no log to write to.
    /// </summary>
    /// <returns>True when the folder existed and the lines were written.</returns>
    public static bool FlushLog(string logDirectory)
    {
        if (!Directory.Exists(logDirectory))
            return false;

        AppLogger.FlushTo(logDirectory);
        return true;
    }
}

/// <summary>
/// #9197: writes a failed start's reason to the Lite log before <see cref="AppLogger.Initialize"/> has run, which is when the
/// single-instance check fails. Unlike <c>--stop</c>, which starts nothing and never creates the data folder, a failed start
/// is worth a log folder: without one the failure is invisible to a scheduled task that only sees an exit code.
/// </summary>
public static class StartFailureLog
{
    /// <summary>Logs the reason at Error, and writes it with everything else buffered so far to <paramref name="logDirectory"/>.</summary>
    /// <returns>True when the lines were written.</returns>
    public static bool Write(string reason, string logDirectory)
    {
        AppLogger.Error("SingleInstance", $"Start failed: {reason}");
        try
        {
            Directory.CreateDirectory(logDirectory);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }

        AppLogger.FlushTo(logDirectory);
        return true;
    }
}

/// <summary>
/// #9142: the upgrade handoff's dialogs for a headless start, which has no one to answer them. It never asks: the start
/// takes over without confirming (see <see cref="LiteStartupPlan.ConfirmsHandoffWithoutAsking"/>), and the two cases that
/// would have raised a dialog are logged and end the start.
/// </summary>
public sealed class HeadlessHandoffPrompts : IHandoffPrompts
{
    public bool ConfirmCloseAndContinue(string oldVersion, string newVersion) => true;

    public bool ConfirmRestartAsAdmin(string oldVersion)
    {
        HeadlessUi.Skips($"the \"restart as administrator\" prompt (version {oldVersion} is running elevated; stop it, then start this one)");
        return false;
    }

    public void ShowMustCloseElevatedManually(string oldVersion) =>
        HeadlessUi.Skips($"the \"close the elevated instance\" notice (version {oldVersion} is running elevated; stop it, then start this one)");
}
