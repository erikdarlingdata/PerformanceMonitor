/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Darling viewer — a Postgres client of the central store. MainWindow owns data startup:
/// it loads the viewer's sliver of darling.json (<see cref="ViewerSettings"/>) and connects
/// on first render. App startup applies the operator's saved color theme (Dark / Light / CoolBreeze)
/// through <see cref="ThemeManager"/> so copied Lite XAML resolves its theme keys; the Settings window's
/// theme selector live-previews and persists the choice to <see cref="ViewerAppSettings"/>, and
/// <see cref="ThemeManager.CurrentTheme"/> drives whether <c>ChartStyle</c> draws dark or light chrome.
/// Single-instance enforcement uses the shared <see cref="SingleInstanceCoordinator"/> (as Lite /
/// Dashboard) so a second launch surfaces the existing window instead of opening a duplicate viewer,
/// and a newer Velopack build launched over an older one takes over.
/// </summary>
public partial class App : Application
{
    private const string MutexName = "PerformanceMonitorDarlingViewer_SingleInstance";
    private const string ExitForUpgradeEventName = "PerformanceMonitorDarlingViewer_ExitForUpgrade";
    private const string ShowWindowEventName = "PerformanceMonitorDarlingViewer_ShowWindow";
    private SingleInstanceCoordinator? _instanceCoordinator;
    private SingleInstanceSignal? _instanceSignal;

    protected override void OnStartup(StartupEventArgs e)
    {
        /* #2005: the headless --test flag runs the #1954 connection self-test (provenance block + the
           six-layer ladder) to the console and the log, then exits with 0/1 — no window, no theme, no
           single-instance dance (a diagnostics run must not surface or fight the running viewer).
           Blocking here is fine: nothing UI exists yet, and Shutdown before base.OnStartup prevents
           StartupUri from ever creating MainWindow (the second-instance pattern below). */
        if (HeadlessSelfTest.IsRequested(e.Args))
        {
            /* Task.Run is LOAD-BEARING, found the hard way on the first live SSM run: OnStartup executes on
               the STA UI thread, which already carries WPF's DispatcherSynchronizationContext — so awaiting
               RunAsync directly posts its continuations to a dispatcher this GetResult() is blocking, the
               classic WPF sync-over-async deadlock (the process hung forever with exit code never written).
               Task.Run hops the whole ladder onto the thread pool, where continuations resume freely. */
            var exitCode = System.Threading.Tasks.Task.Run(
                    () => HeadlessSelfTest.RunAsync(HeadlessSelfTest.ExplicitConfigPath(e.Args)))
                .GetAwaiter().GetResult();
            Shutdown(exitCode);
            return;
        }

        /* Minimal file logging (ported from Lite's AppLogger) so the sidebar's View Log / Open Log
           Folder buttons have a real target and operator bug reports carry viewer diagnostics.

           Initialized HERE, ahead of the theme read below, rather than after the single-instance dance
           where it used to sit (#2434). That read is the viewer's FIRST touch of viewer-settings.json, so
           it is the first thing that can discover the file is unreadable — and ViewerLogger.Log drops
           anything enqueued before Initialize, which would have made the earliest and most useful
           diagnostic the one guaranteed to vanish. Nothing between here and its old position depends on
           the ordering: Initialize creates a directory and starts a timer, and it swallows its own
           failure. */
        ViewerLogger.Initialize();

        /* Apply the saved color theme through ThemeManager (App.xaml merges Dark as the design-time
           default) so ThemeManager owns the app-level merged dictionary at runtime, before StartupUri
           creates MainWindow. Reads the viewer-local settings directly (cheap JSON read) so the very
           first paint is already in the operator's chosen theme — no flash of Dark. Falls back to Dark
           on any error. Light / CoolBreeze are honored via the Settings window's theme selector. */
        try
        {
            ThemeManager.Apply(new ViewerAppSettingsStore().Load().ColorTheme);
        }
        catch
        {
            ThemeManager.Apply("Dark");
        }

        /* #1050 (companion to the ported tray's WindowResumeGuard): WPF's GPU render thread can zombie its
           surface across sleep/wake or RDP, leaving a live-but-blank window — now reachable in the viewer
           because minimize-to-tray can hide it. Software rendering removes the GPU dependency entirely; charts
           are unaffected (ScottPlot renders via SkiaSharp/CPU into a bitmap, not WPF's GPU path). Matches Lite. */
        System.Windows.Media.RenderOptions.ProcessRenderMode =
            System.Windows.Interop.RenderMode.SoftwareOnly;

        /* Single-instance with upgrade handoff (shared PerformanceMonitor.Ui coordinator, mirroring Lite /
           Dashboard). Runs before base.OnStartup creates the StartupUri window, so a second launch surfaces
           the existing window and exits instead of opening a duplicate viewer; a newer build launched over an
           older one closes it and takes over (Velopack upgrade). The viewer holds no exclusive local resource
           (it is a stateless Postgres read-client), so the exit-for-upgrade listener can open immediately. */
        _instanceCoordinator = new SingleInstanceCoordinator(new SingleInstanceOptions
        {
            MutexName = MutexName,
            ProcessName = "PerformanceMonitor.Darling.Viewer",
            ExitEventName = ExitForUpgradeEventName,
            SurfaceRunningInstance = () => SingleInstanceSignal.TrySignal(ShowWindowEventName),
            GracefulSelfExit = () => Dispatcher.BeginInvoke(new Action(Shutdown)),
            Prompts = new MessageBoxHandoffPrompts("Performance Monitor Darling"),
            AutoConfirm = Array.Exists(e.Args, a => string.Equals(a, HandoffArgs.AutoConfirm, StringComparison.OrdinalIgnoreCase)),
            Log = msg => { try { ViewerLogger.Info("SingleInstance", msg); } catch { /* logger not yet ready */ } },
        });

        if (!_instanceCoordinator.TryBecomeOwner())
        {
            Shutdown();
            return;
        }

        /* Own the "surface me" channel before the (possibly slow) first render, so a fast second launch finds
           it; a signal that lands before the window exists is a harmless no-op (the callback null-checks). */
        _instanceSignal = new SingleInstanceSignal(ShowWindowEventName, OnSurfaceWindowRequested);

        /* No risky exclusive init to protect, so let a newer build ask us to step aside for an upgrade now. */
        _instanceCoordinator.EnableUpgradeHandoff();

        ViewerLogger.Info("App", $"Starting PerformanceMonitor Darling Viewer v{Assembly.GetExecutingAssembly().GetName().Version}");

        /* Surface otherwise-invisible crashes into the log now that we have one. */
        DispatcherUnhandledException += OnDispatcherUnhandledException;
        AppDomain.CurrentDomain.UnhandledException += OnDomainUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

        base.OnStartup(e);

        // Right-click selects the DataGrid row under the cursor app-wide, so context-menu actions
        // (e.g. View Stored Plan) act on the clicked row even after an auto-refresh cleared the selection.
        // (Lite/Dashboard already do this; the viewer's plan handlers read CurrentItem, so without it a
        // right-click could act on a different row than the one whose enablement was evaluated.)
        PerformanceMonitor.Ui.DataGridRowSelectionBehavior.Enable();
    }

    /// <summary>
    /// Invoked on <see cref="SingleInstanceSignal"/>'s background thread when a second launch asks us to
    /// surface the window. Marshals to the UI thread and brings the existing window to the front via WPF's
    /// own path — the shared <see cref="MainWindow.SurfaceWindow"/> the tray Restore also uses, so a
    /// tray-hidden window is un-hidden consistently.
    /// </summary>
    private void OnSurfaceWindowRequested()
    {
        Dispatcher.BeginInvoke(new Action(() =>
        {
            if (Current.MainWindow is MainWindow window)
            {
                window.SurfaceWindow();
            }
        }));
    }

    protected override void OnExit(ExitEventArgs e)
    {
        ViewerLogger.Info("App", "Shutting down");
        _instanceSignal?.Dispose();
        _instanceCoordinator?.Dispose();
        ViewerLogger.Shutdown();
        base.OnExit(e);
    }

    private static void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        /* Silently swallow the Hardcodet TrayToolTip race condition (issue #422): showing the custom
           visual tooltip can throw "root Visual of a VisualTarget cannot have a parent" in
           Popup.CreateWindow, and it is harmless (the tooltip just does not show that one time). Setting
           e.Handled here on the Dispatcher path is what actually keeps the viewer alive. Ported from
           Lite's App.xaml.cs (562269f6 / 50257182) to restore Lite<->Darling parity -- the shared
           SystemTrayService custom TrayToolTip is the common crash source. */
        if (IsTrayToolTipCrash(e.Exception))
        {
            ViewerLogger.Warn("Dispatcher", "Suppressed Hardcodet TrayToolTip crash (issue #422)");
            e.Handled = true;
            return;
        }

        ViewerLogger.Error("App", "Unhandled UI exception", e.Exception);
    }

    private static void OnDomainUnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        var exception = e.ExceptionObject as Exception;

        /* Same #422 crash when it escapes the Dispatcher path (e.g. during tray-Exit shutdown after the
           Dispatcher hooks are torn down). AppDomain.UnhandledException cannot mark it handled, but the
           Dispatcher branch above catches the live case; log-and-return so it is not surfaced as a fatal
           domain crash. */
        if (exception != null && IsTrayToolTipCrash(exception))
        {
            ViewerLogger.Warn("AppDomain", "Suppressed Hardcodet TrayToolTip crash (issue #422)");
            ViewerLogger.Flush();
            return;
        }

        ViewerLogger.Error("App", "Unhandled domain exception", exception);
    }

    private static void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        /* Fire-and-forget Tasks (e.g. the discarded `_ = OpenPlanTab(...)` View-Plan loaders, #2870) that
           fault would otherwise vanish here -- Lite and the deprecated Dashboard already observe this, so
           Darling matching restores parity and keeps a failed plan load from failing silently. */
        ViewerLogger.Error("App", "Unobserved task exception", e.Exception);
        e.SetObserved(); /* Prevent process termination */
    }

    /// <summary>
    /// Detects the Hardcodet TrayToolTip race-condition crash (issue #422): an ArgumentException from
    /// Popup.CreateWindow ("root Visual of a VisualTarget cannot have a parent") surfacing on the
    /// TaskbarIcon tooltip path. Matches Lite's detector so both front ends suppress identically.
    /// </summary>
    private static bool IsTrayToolTipCrash(Exception ex)
    {
        return ex is System.ArgumentException
            && ex.Message.Contains("VisualTarget")
            && ex.StackTrace?.Contains("TaskbarIcon") == true;
    }
}
