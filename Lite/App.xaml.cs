/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite;

public partial class App : Application
{
    private const string MutexName = "PerformanceMonitorLite_SingleInstance";
    private Mutex? _singleInstanceMutex;

    /// <summary>
    /// Gets the application data directory where config and data files are stored.
    /// </summary>
    public static string DataDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the path to the DuckDB database file.
    /// </summary>
    public static string DatabasePath { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the config directory path.
    /// </summary>
    public static string ConfigDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the archive directory path for Parquet files.
    /// </summary>
    public static string ArchiveDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the default time range in hours for new server tabs.
    /// </summary>
    public static int DefaultTimeRangeHours { get; set; } = 4;

    /* Alert settings */
    public static bool AlertsEnabled { get; set; } = true;
    public static bool NotifyConnectionChanges { get; set; } = true;
    public static bool AlertCpuEnabled { get; set; } = true;
    public static int AlertCpuThreshold { get; set; } = 80;
    public static bool AlertBlockingEnabled { get; set; } = true;
    public static int AlertBlockingThreshold { get; set; } = 1;
    public static bool AlertDeadlockEnabled { get; set; } = true;
    public static int AlertDeadlockThreshold { get; set; } = 1;

    /* Update check settings */
    public static bool CheckForUpdatesOnStartup { get; set; } = true;

    /* SMTP email alert settings */
    public static bool SmtpEnabled { get; set; } = false;
    public static string SmtpServer { get; set; } = "";
    public static int SmtpPort { get; set; } = 587;
    public static bool SmtpUseSsl { get; set; } = true;
    public static string SmtpUsername { get; set; } = "";
    public static string SmtpFromAddress { get; set; } = "";
    public static string SmtpRecipients { get; set; } = "";

    private const string SmtpCredentialKey = "SMTP";

    /// <summary>
    /// Gets the SMTP password from Windows Credential Manager.
    /// </summary>
    public static string? GetSmtpPassword()
    {
        try
        {
            var credService = new Services.CredentialService();
            var cred = credService.GetCredential(SmtpCredentialKey);
            return cred?.Password;
        }
        catch (Exception ex)
        {
            AppLogger.Error("App", $"Failed to retrieve SMTP password: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Saves the SMTP password to Windows Credential Manager.
    /// </summary>
    public static void SaveSmtpPassword(string password)
    {
        try
        {
            var credService = new Services.CredentialService();
            credService.SaveCredential(SmtpCredentialKey, string.IsNullOrEmpty(SmtpUsername) ? "smtp" : SmtpUsername, password);
        }
        catch (Exception ex)
        {
            AppLogger.Error("App", $"Failed to save SMTP password: {ex.Message}");
        }
    }

    protected override void OnStartup(StartupEventArgs e)
    {
        // Check for existing instance
        _singleInstanceMutex = new Mutex(true, MutexName, out bool isNewInstance);

        if (!isNewInstance)
        {
            MessageBox.Show(
                "Performance Monitor Lite is already running.",
                "Already Running",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
            Shutdown();
            return;
        }

        base.OnStartup(e);

        // Initialize paths - use executable directory for portability
        var exeDirectory = AppDomain.CurrentDomain.BaseDirectory;
        DataDirectory = exeDirectory;
        ConfigDirectory = Path.Combine(exeDirectory, "config");
        DatabasePath = Path.Combine(exeDirectory, "monitor.duckdb");
        ArchiveDirectory = Path.Combine(exeDirectory, "archive");

        // Ensure directories exist
        if (!Directory.Exists(ConfigDirectory))
        {
            Directory.CreateDirectory(ConfigDirectory);
        }

        // Load settings
        LoadDefaultTimeRange();
        LoadAlertSettings();

        // Initialize logging
        var logDirectory = Path.Combine(exeDirectory, "logs");
        AppLogger.Initialize(logDirectory);
        AppLogger.Info("App", $"Starting PerformanceMonitorLite v{System.Reflection.Assembly.GetExecutingAssembly().GetName().Version}");
        AppLogger.Info("App", $"Data directory: {DataDirectory}");

        // Register global exception handlers
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        DispatcherUnhandledException += OnDispatcherUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
    }

    protected override void OnExit(ExitEventArgs e)
    {
        AppLogger.Info("App", "Shutting down");
        AppLogger.Shutdown();

        // Release the mutex
        _singleInstanceMutex?.ReleaseMutex();
        _singleInstanceMutex?.Dispose();

        base.OnExit(e);
    }

    private static void LoadDefaultTimeRange()
    {
        try
        {
            var path = Path.Combine(ConfigDirectory, "settings.json");
            if (!File.Exists(path)) return;

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            if (doc.RootElement.TryGetProperty("default_time_range_hours", out var val))
            {
                DefaultTimeRangeHours = val.GetInt32();
            }
        }
        catch { /* Use default */ }
    }

    public static void LoadAlertSettings()
    {
        try
        {
            var path = Path.Combine(ConfigDirectory, "settings.json");
            if (!File.Exists(path)) return;

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            var root = doc.RootElement;

            if (root.TryGetProperty("alerts_enabled", out var v)) AlertsEnabled = v.GetBoolean();
            if (root.TryGetProperty("notify_connection_changes", out v)) NotifyConnectionChanges = v.GetBoolean();
            if (root.TryGetProperty("alert_cpu_enabled", out v)) AlertCpuEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_cpu_threshold", out v)) AlertCpuThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_blocking_enabled", out v)) AlertBlockingEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_blocking_threshold", out v)) AlertBlockingThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_deadlock_enabled", out v)) AlertDeadlockEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_deadlock_threshold", out v)) AlertDeadlockThreshold = v.GetInt32();

            /* Update check settings */
            if (root.TryGetProperty("check_for_updates_on_startup", out v)) CheckForUpdatesOnStartup = v.GetBoolean();

            /* SMTP settings */
            if (root.TryGetProperty("smtp_enabled", out v)) SmtpEnabled = v.GetBoolean();
            if (root.TryGetProperty("smtp_server", out v)) SmtpServer = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_port", out v)) SmtpPort = v.GetInt32();
            if (root.TryGetProperty("smtp_use_ssl", out v)) SmtpUseSsl = v.GetBoolean();
            if (root.TryGetProperty("smtp_username", out v)) SmtpUsername = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_from_address", out v)) SmtpFromAddress = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_recipients", out v)) SmtpRecipients = v.GetString() ?? "";
        }
        catch { /* Use defaults */ }
    }

    private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        var exception = e.ExceptionObject as Exception;
        AppLogger.Error("AppDomain", "Unhandled exception (terminating=" + e.IsTerminating + ")", exception);
        AppLogger.Flush();

        var details = FormatExceptionDetails(exception);
        MessageBox.Show(
            $"A fatal error occurred and the application must close.\n\n{details}",
            "Fatal Error",
            MessageBoxButton.OK,
            MessageBoxImage.Error);
    }

    private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        AppLogger.Error("Dispatcher", "Unhandled exception", e.Exception);
        AppLogger.Flush();

        var details = FormatExceptionDetails(e.Exception);
        MessageBox.Show(
            $"An error occurred:\n\n{details}\n\nThe application will attempt to continue.",
            "Error",
            MessageBoxButton.OK,
            MessageBoxImage.Error);

        e.Handled = true; /* Prevent application crash */
    }

    private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        AppLogger.Error("Task", "Unobserved task exception", e.Exception);
        AppLogger.Flush();
        e.SetObserved(); /* Prevent process termination */
    }

    private static string FormatExceptionDetails(Exception? ex)
    {
        if (ex == null) return "Unknown error";

        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"Type: {ex.GetType().FullName}");
        sb.AppendLine($"Message: {ex.Message}");
        sb.AppendLine();
        sb.AppendLine("Stack trace:");
        sb.AppendLine(ex.StackTrace);

        var inner = ex.InnerException;
        var depth = 1;
        while (inner != null)
        {
            sb.AppendLine();
            sb.AppendLine($"--- Inner Exception [{depth}] ---");
            sb.AppendLine($"Type: {inner.GetType().FullName}");
            sb.AppendLine($"Message: {inner.Message}");
            sb.AppendLine("Stack trace:");
            sb.AppendLine(inner.StackTrace);
            inner = inner.InnerException;
            depth++;
        }

        return sb.ToString();
    }
}
