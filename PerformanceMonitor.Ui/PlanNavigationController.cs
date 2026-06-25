/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Shared "View Plan" / "Get Actual Plan" behavior for every query-identifying surface in both apps.
/// The duplicated confirm-dialog / cancellation / error-handling logic that used to live in each app's
/// QueryPerformanceContent.Plans.cs and ServerTab.Plans.cs lives here once. The host supplies two
/// delegates so this class never references an app-specific type (PlanViewerControl, ActualPlanExecutor,
/// or a connection-string source):
///   <c>showPlan</c>      — open the app's PlanViewerWindow and load the plan XML.
///   <c>executeActual</c> — run the app's ActualPlanExecutor against the right connection string.
/// </summary>
public sealed class PlanNavigationController
{
    private readonly Window _owner;
    private readonly Func<string, string, string?, Task> _showPlan;
    private readonly Func<string, string, string?, string?, CancellationToken, Task<string?>> _executeActual;
    private readonly string _targetDescription;
    private readonly Action<string>? _setStatus;
    private CancellationTokenSource? _actualPlanCts;

    /// <param name="owner">Host window — owns the dialogs and cancels a running capture when closed.</param>
    /// <param name="showPlan">(planXml, label, queryText) =&gt; opens the plan in the app's viewer.</param>
    /// <param name="executeActual">(databaseName, queryText, estimatedPlanXml, isolationLevel, ct) =&gt; captured actual-plan XML.</param>
    /// <param name="targetDescription">How the confirm dialog names the server (e.g. "the monitored server" or a server name).</param>
    /// <param name="setStatus">Optional status-text callback for hosts that have a status line.</param>
    public PlanNavigationController(
        Window owner,
        Func<string, string, string?, Task> showPlan,
        Func<string, string, string?, string?, CancellationToken, Task<string?>> executeActual,
        string targetDescription,
        Action<string>? setStatus = null)
    {
        _owner = owner;
        _showPlan = showPlan;
        _executeActual = executeActual;
        _targetDescription = targetDescription;
        _setStatus = setStatus;

        // Closing the host cancels any in-flight actual-plan capture.
        _owner.Closed += (_, _) => _actualPlanCts?.Cancel();
    }

    /// <summary>
    /// Fetches a collected/cached plan via <paramref name="fetchPlanXml"/> and opens it in the viewer.
    /// Shows a friendly message when no plan is available.
    /// </summary>
    public async Task ViewPlanAsync(Func<Task<string?>> fetchPlanXml, string label, string? queryText)
    {
        string? planXml;
        try
        {
            _setStatus?.Invoke("Fetching query plan...");
            using (SetBusy())
                planXml = await fetchPlanXml();
        }
        catch (Exception ex)
        {
            _setStatus?.Invoke("Error fetching query plan.");
            MessageBox.Show(_owner, $"Error fetching the query plan:\n\n{ex.Message}",
                "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            return;
        }

        if (string.IsNullOrWhiteSpace(planXml))
        {
            _setStatus?.Invoke("No plan available.");
            MessageBox.Show(_owner,
                "No query plan is available for this row. The plan may have been evicted from the plan cache since it was last collected.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        _setStatus?.Invoke("Ready");
        await _showPlan(planXml, label, queryText);
    }

    /// <summary>
    /// Re-executes <paramref name="queryText"/> against the monitored server (after confirmation) with
    /// SET STATISTICS XML ON and opens the captured actual plan. No-ops gracefully when there is no
    /// query text (e.g. procedure rows, which carry only a name).
    /// </summary>
    public async Task GetActualPlanAsync(string? queryText, string databaseName, string label,
        string? estimatedPlanXml = null, string? isolationLevel = null)
    {
        if (string.IsNullOrWhiteSpace(queryText))
        {
            MessageBox.Show(_owner, "No query text is available for this row.", "No Query Text",
                MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var confirm = MessageBox.Show(_owner,
            $"You are about to execute this query against {_targetDescription} in database " +
            $"[{(string.IsNullOrEmpty(databaseName) ? "default" : databaseName)}].\n\n" +
            "Make sure you understand what the query does before proceeding.\n" +
            "The query will execute with SET STATISTICS XML ON to capture the actual plan.\n" +
            "All data results will be discarded.",
            "Get Actual Plan", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (confirm != MessageBoxResult.OK) return;

        _actualPlanCts?.Dispose();
        _actualPlanCts = new CancellationTokenSource();

        string? actualPlanXml;
        try
        {
            _setStatus?.Invoke("Executing query for actual plan...");
            using (SetBusy())
                actualPlanXml = await _executeActual(databaseName ?? "", queryText, estimatedPlanXml, isolationLevel, _actualPlanCts.Token);
        }
        catch (OperationCanceledException)
        {
            _setStatus?.Invoke("Actual plan capture cancelled.");
            MessageBox.Show(_owner, "The query was cancelled or timed out.", "Cancelled",
                MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }
        catch (Exception ex)
        {
            _setStatus?.Invoke("Actual plan capture failed.");
            MessageBox.Show(_owner, $"Failed to get the actual plan:\n\n{ex.Message}", "Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
            return;
        }

        if (string.IsNullOrWhiteSpace(actualPlanXml))
        {
            _setStatus?.Invoke("No actual plan captured.");
            MessageBox.Show(_owner, "The query executed but no execution plan was captured.",
                "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        _setStatus?.Invoke("Actual plan captured successfully.");
        await _showPlan(actualPlanXml, label, queryText);
    }

    private static BusyScope SetBusy()
    {
        Mouse.OverrideCursor = Cursors.Wait;
        return new BusyScope();
    }

    private sealed class BusyScope : IDisposable
    {
        public void Dispose() => Mouse.OverrideCursor = null;
    }
}
