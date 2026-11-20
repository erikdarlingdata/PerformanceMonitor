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
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9142: what one start of Lite does, decided from its command line before anything is built. Lite can run with no
/// window, no tray icon and no signed-in user (a scheduled task at machine start), and a second start can ask the running
/// one to shut down. <see cref="App"/> reads the plan once and every start-up step that would put something on a screen
/// asks it first.
/// </summary>
public sealed record LiteStartupPlan(bool Headless, bool Stop)
{
    public const string HeadlessArg = "--headless";
    public const string StopArg = "--stop";

    public static LiteStartupPlan FromArgs(IEnumerable<string> args)
    {
        var headless = false;
        var stop = false;
        foreach (var arg in args)
        {
            if (string.Equals(arg, HeadlessArg, StringComparison.OrdinalIgnoreCase))
                headless = true;
            else if (string.Equals(arg, StopArg, StringComparison.OrdinalIgnoreCase))
                stop = true;
        }

        return new LiteStartupPlan(headless, stop);
    }

    /// <summary>A stop request starts nothing: it asks the running instance to stop and leaves.</summary>
    public bool StartsServices => !Stop;

    /// <summary>The main window is shown. Headless never shows it, and never creates a window handle for it.</summary>
    public bool ShowsMainWindow => StartsServices && !Headless;

    /// <summary>The tray icon exists. It lives on the taskbar, which a start with no signed-in user does not have.</summary>
    public bool CreatesTrayIcon => ShowsMainWindow;

    /// <summary>
    /// A start that nobody is at the screen for takes over from an older running instance without asking. The elevated
    /// relaunch of the upgrade handoff already does the same, through its own flag.
    /// </summary>
    public bool ConfirmsHandoffWithoutAsking => Headless;
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

    /// <summary>True when a process of this name other than this one is running.</summary>
    public static bool OtherInstanceRunning(string processName)
    {
        var self = Environment.ProcessId;
        foreach (var process in Process.GetProcessesByName(processName))
        {
            using (process)
            {
                if (process.Id != self)
                    return true;
            }
        }

        return false;
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
