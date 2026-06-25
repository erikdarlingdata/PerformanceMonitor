/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Markup;
using System.Windows.Threading;
using PerformanceMonitorDashboard.Helpers;
using Velopack;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorDashboard
{
    public partial class App : Application
    {
        private const string MutexName = "PerformanceMonitorDashboard_SingleInstance";
        /* Version-aware single-instance + upgrade handoff (plans/single-instance-upgrade-handoff.md):
           a newer build launched over an older tray-resident one closes it and takes over instead of
           being handed back the stale in-memory version. The coordinator owns the mutex + the
           exit-for-upgrade listener for the life of the owning process. */
        private const string ExitForUpgradeEventName = "PerformanceMonitorDashboard_ExitForUpgrade";
        private SingleInstanceCoordinator? _instanceCoordinator;

        protected override void OnStartup(StartupEventArgs e)
        {
            NativeMethods.SetAppUserModelId("DarlingData.PerformanceMonitor.Dashboard");

            /* Single-instance with upgrade handoff. Runs synchronously at the top of OnStartup before
               base.OnStartup and any window/MCP init, so a stale older build is closed before we bind
               the MCP port / touch shared config. A same/newer instance just surfaces (today's behavior
               via WM_SHOWMONITOR); an older-but-elevated one raises an actionable error. */
            _instanceCoordinator = new SingleInstanceCoordinator(new SingleInstanceOptions
            {
                MutexName = MutexName,
                ProcessName = "PerformanceMonitorDashboard",
                ExitEventName = ExitForUpgradeEventName,
                SurfaceRunningInstance = NativeMethods.BroadcastShowMessage,
                GracefulSelfExit = () => Dispatcher.BeginInvoke(new Action(() =>
                {
                    if (MainWindow is MainWindow mw) mw.ExitApplication();
                    else Shutdown();
                })),
                Prompts = new MessageBoxHandoffPrompts("Performance Monitor Dashboard"),
                AutoConfirm = Array.Exists(e.Args, a => string.Equals(a, HandoffArgs.AutoConfirm, StringComparison.OrdinalIgnoreCase)),
                Log = msg => { try { Logger.Info($"[SingleInstance] {msg}"); } catch { /* logger not yet initialized */ } },
            });

            if (!_instanceCoordinator.TryBecomeOwner())
            {
                Shutdown();
                return;
            }

            base.OnStartup(e);

            // Right-click selects the DataGrid row under the cursor app-wide, so context-menu actions
            // (e.g. View Plan) act on the clicked row even after an auto-refresh cleared the selection.
            PerformanceMonitor.Ui.DataGridRowSelectionBehavior.Enable();

            // #1050: WPF's GPU render thread can zombie its surface across sleep/wake or RDP, leaving a
            // live-but-blank window. Software rendering removes the GPU dependency entirely. Charts are
            // unaffected — ScottPlot renders via SkiaSharp (CPU) into a bitmap, not WPF's GPU path.
            System.Windows.Media.RenderOptions.ProcessRenderMode =
                System.Windows.Interop.RenderMode.SoftwareOnly;

            // Use the user's locale for date/time formatting in WPF bindings (issue #459)
            FrameworkElement.LanguageProperty.OverrideMetadata(
                typeof(FrameworkElement),
                new FrameworkPropertyMetadata(XmlLanguage.GetLanguage(Thread.CurrentThread.CurrentCulture.IetfLanguageTag)));

            // Apply saved color theme before the main window is shown
            var prefs = new Services.UserPreferencesService().GetPreferences();
            ThemeManager.Apply(prefs.ColorTheme ?? "Dark");

            // Register global exception handlers
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            DispatcherUnhandledException += OnDispatcherUnhandledException;
            TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

            Logger.Info("=== Application Starting ===");
            Logger.Info($"Version: {System.Reflection.Assembly.GetExecutingAssembly().GetName().Version}");
            Logger.Info($"OS: {Environment.OSVersion}");
            Logger.Info($".NET Runtime: {Environment.Version}");
            Logger.Info($"Log Directory: {Logger.GetLogDirectory()}");

            // Create and show main window (StartupUri removed for Velopack custom Main)
            var mainWindow = new MainWindow();
            mainWindow.Show();
        }

        /// <summary>
        /// Opens the upgrade-handoff "exit" channel once startup is past its risky init. Called by
        /// <see cref="MainWindow"/> after initialization so a newer build won't signal/kill us mid-init
        /// (#single-instance-upgrade-handoff). Safe to call more than once.
        /// </summary>
        public void EnableUpgradeHandoff() => _instanceCoordinator?.EnableUpgradeHandoff();

        protected override void OnExit(ExitEventArgs e)
        {
            Logger.Info($"=== Application Exiting (Exit Code: {e.ApplicationExitCode}) ===");

            // Ensure MainWindow is properly closed to dispose tray icon
            if (MainWindow is MainWindow mainWin)
            {
                mainWin.ExitApplication();
            }

            /* Releases the mutex + disposes the exit-for-upgrade listener. */
            _instanceCoordinator?.Dispose();

            base.OnExit(e);
        }


        private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var exception = e.ExceptionObject as Exception;

            /* Silently swallow Hardcodet TrayToolTip race condition (issue #422) when it
               escapes the Dispatcher path — happens during tray-Exit shutdown when the
               Dispatcher's exception hooks are torn down before the tray library finishes. */
            if (exception != null && IsTrayToolTipCrash(exception))
            {
                Logger.Warning("Suppressed Hardcodet TrayToolTip crash (issue #422) in AppDomain handler");
                return;
            }

            Logger.Fatal("Unhandled AppDomain Exception", exception ?? new Exception("Unknown exception"));

            if (e.IsTerminating)
            {
                CreateCrashDump(exception);
                MessageBox.Show(
                    $"A fatal error occurred and the application must close.\n\n" +
                    $"Error: {exception?.Message}\n\n" +
                    $"Log file: {Logger.GetCurrentLogFile()}",
                    "Fatal Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
        }

        private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
        {
            /* Silently swallow Hardcodet TrayToolTip race condition (issue #422).
               The crash occurs in Popup.CreateWindow when showing the custom visual tooltip
               and is harmless — the tooltip simply doesn't show that one time. */
            if (IsTrayToolTipCrash(e.Exception))
            {
                Logger.Warning("Suppressed Hardcodet TrayToolTip crash (issue #422)");
                e.Handled = true;
                return;
            }

            Logger.Error("Unhandled Dispatcher Exception", e.Exception);

            MessageBox.Show(
                $"An error occurred:\n\n{e.Exception.Message}\n\n" +
                $"The application will attempt to continue.\n\n" +
                $"Log file: {Logger.GetCurrentLogFile()}",
                "Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);

            e.Handled = true; // Prevent application crash
        }

        private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            Logger.Error("Unobserved Task Exception", e.Exception);
            e.SetObserved(); // Prevent process termination
        }

        /// <summary>
        /// Detects the Hardcodet TrayToolTip race condition crash (issue #422).
        /// </summary>
        private static bool IsTrayToolTipCrash(Exception ex)
        {
            return ex is ArgumentException
                && ex.Message.Contains("VisualTarget")
                && ex.StackTrace?.Contains("TaskbarIcon") == true;
        }

        private void CreateCrashDump(Exception? exception)
        {
            try
            {
                var crashDumpDir = Path.Combine(Logger.GetLogDirectory(), "CrashDumps");
                if (!Directory.Exists(crashDumpDir))
                {
                    Directory.CreateDirectory(crashDumpDir);
                }

                var dumpFile = Path.Combine(crashDumpDir, $"crash_{DateTime.Now:yyyyMMdd_HHmmss}.txt");
                var dumpContent = $@"=== CRASH DUMP ===
Timestamp: {DateTime.Now:yyyy-MM-dd HH:mm:ss}
Application Version: {System.Reflection.Assembly.GetExecutingAssembly().GetName().Version}
OS: {Environment.OSVersion}
.NET Runtime: {Environment.Version}
Working Directory: {Environment.CurrentDirectory}

Exception Type: {exception?.GetType().FullName}
Message: {exception?.Message}

Stack Trace:
{exception?.StackTrace}

Inner Exception: {exception?.InnerException?.Message}
Inner Stack Trace:
{exception?.InnerException?.StackTrace}
";
                File.WriteAllText(dumpFile, dumpContent);
                Logger.Info($"Crash dump created: {dumpFile}");
            }
            catch (Exception ex)
            {
                Logger.Error("Failed to create crash dump", ex);
            }
        }
    }
}
