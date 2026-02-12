/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard
{
    public partial class App : Application
    {
        private const string MutexName = "PerformanceMonitorDashboard_SingleInstance";
        private Mutex? _singleInstanceMutex;

        // DPI awareness for proper scaling on high DPI displays
        private enum PROCESS_DPI_AWARENESS
        {
            Process_DPI_Unaware = 0,
            Process_System_DPI_Aware = 1,
            Process_Per_Monitor_DPI_Aware = 2
        }

        private enum DPI_AWARENESS_CONTEXT
        {
            Unaware = -1,
            SystemAware = -2,
            PerMonitorAware = -3,
            PerMonitorAwareV2 = -4
        }

        [DllImport("SHCore.dll", SetLastError = true)]
        private static extern bool SetProcessDpiAwareness(PROCESS_DPI_AWARENESS awareness);

        [DllImport("user32.dll", SetLastError = true)]
        private static extern bool SetProcessDpiAwarenessContext(int dpiFlag);

        protected override void OnStartup(StartupEventArgs e)
        {
            // Enable per-monitor DPI awareness for proper scaling on high DPI displays
            EnableDpiAwareness();

            // Check for existing instance
            _singleInstanceMutex = new Mutex(true, MutexName, out bool isNewInstance);

            if (!isNewInstance)
            {
                // Another instance is already running - activate it and exit
                NativeMethods.BroadcastShowMessage();
                Shutdown();
                return;
            }

            base.OnStartup(e);

            // Register global exception handlers
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            DispatcherUnhandledException += OnDispatcherUnhandledException;
            TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

            Logger.Info("=== Application Starting ===");
            Logger.Info($"Version: {System.Reflection.Assembly.GetExecutingAssembly().GetName().Version}");
            Logger.Info($"OS: {Environment.OSVersion}");
            Logger.Info($".NET Runtime: {Environment.Version}");
            Logger.Info($"Log Directory: {Logger.GetLogDirectory()}");
        }

        protected override void OnExit(ExitEventArgs e)
        {
            Logger.Info($"=== Application Exiting (Exit Code: {e.ApplicationExitCode}) ===");

            // Ensure MainWindow is properly closed to dispose tray icon
            if (MainWindow is MainWindow mainWin)
            {
                mainWin.ExitApplication();
            }

            // Release the mutex
            _singleInstanceMutex?.ReleaseMutex();
            _singleInstanceMutex?.Dispose();

            base.OnExit(e);
        }

        private static void EnableDpiAwareness()
        {
            try
            {
                // Try PerMonitorV2 first (Windows 10 1703+) - best scaling quality
                if (Environment.OSVersion.Version.Major >= 10)
                {
                    try
                    {
                        SetProcessDpiAwarenessContext((int)DPI_AWARENESS_CONTEXT.PerMonitorAwareV2);
                        return;
                    }
                    catch
                    {
                        // Fall through to try other methods
                    }
                }

                // Try PerMonitor awareness (Windows 8.1+)
                try
                {
                    SetProcessDpiAwareness(PROCESS_DPI_AWARENESS.Process_Per_Monitor_DPI_Aware);
                }
                catch
                {
                    // If all else fails, WPF will use system DPI awareness
                }
            }
            catch
            {
                // Silently fail - WPF will handle DPI at a basic level
            }
        }

        private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var exception = e.ExceptionObject as Exception;
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
