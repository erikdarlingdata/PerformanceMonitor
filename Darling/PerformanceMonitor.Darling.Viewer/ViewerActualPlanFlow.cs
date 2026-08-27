/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The ONE shared "Get Actual Plan" flow every viewer surface uses (Top Queries, Query-Stats history, Query
/// Store history, Wait drill-down, FinOps High Impact). It owns the pieces that must be identical everywhere:
/// the read-only-seat guard, the data-modification detection + PROMINENT informed-consent flag, the enqueue/poll
/// of the <c>execute_actual_plan</c> command, and the legible permission / timeout / not-connected / no-plan
/// messaging. Each surface differs only in the identifier it builds, where it reads the estimated plan for
/// detection, and how it hosts the resulting plan (a Plan Viewer tab vs a floated window) — so this returns the
/// captured plan XML (or null when the user declined / it failed / was cancelled, all already surfaced) and the
/// caller opens it however it hosts plans. Enqueuing is a config write, so a read-only seat is short-circuited
/// (and the enqueue's <see cref="ViewerReadOnlyException"/> is the backstop) — the same rule as Fetch Live Plan.
/// </summary>
public static class ViewerActualPlanFlow
{
    /// <summary>
    /// Runs the full shared flow and returns the captured actual-plan XML, or null when it should not be opened
    /// (read-only seat, declined consent, cancelled, failure, or no plan — every non-null-returning path has
    /// already shown the user a message). <paramref name="onStarted"/> / <paramref name="onFinished"/> bracket
    /// the enqueue so each host shows its own progress indicator; consent runs BEFORE <paramref name="onStarted"/>.
    /// </summary>
    /// <param name="estimatedPlanXml">The stored estimated plan for modification detection (the service re-resolves
    /// the text by identifier when it executes); null/unparseable makes the detector fail-safe to "modifying".</param>
    public static async Task<string?> RequestActualPlanAsync(
        Window owner,
        ViewerDataService dataService,
        int serverId,
        string serverDisplayName,
        string databaseName,
        string? queryText,
        string? estimatedPlanXml,
        string argsJson,
        Action? onStarted,
        Action? onFinished,
        CancellationToken cancellationToken)
    {
        /* A read-only seat can't enqueue a command — short-circuit with the explanation (the enqueue's
           ViewerReadOnlyException is the belt-and-braces backstop, handled below too). */
        if (dataService.IsReadOnly)
        {
            ShowReadOnly(owner);
            return null;
        }

        var modification = QueryModificationDetector.Detect(estimatedPlanXml, queryText);
        if (!Confirm(owner, serverDisplayName, databaseName, modification))
        {
            return null;
        }

        onStarted?.Invoke();
        try
        {
            var result = await dataService.ExecuteActualPlanAsync(serverId, argsJson, cancellationToken);
            if (result.Status == ActualPlanStatus.Captured)
            {
                return result.PlanXml;
            }

            ShowOutcome(owner, result);
            return null;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (ViewerReadOnlyException)
        {
            ShowReadOnly(owner);
            return null;
        }
        catch (Exception ex)
        {
            MessageBox.Show(owner, $"Failed to capture the actual plan:\n\n{ex.Message}", "Actual Plan Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
            return null;
        }
        finally
        {
            onFinished?.Invoke();
        }
    }

    /// <summary>
    /// Opens a captured plan in a shared <see cref="PlanViewerControl"/> floated above <paramref name="owner"/>
    /// via <see cref="GraphViewerWindow"/> — the plan host the history / drill-down windows use (they have no
    /// Plan Viewer tab). The Top Queries surface opens into its tab instead, so it does not call this.
    /// </summary>
    public static async Task OpenFloatingPlanAsync(Window owner, string planXml, string label, string? queryText)
    {
        var viewer = new PlanViewerControl();
        try
        {
            await viewer.LoadPlan(planXml, label, queryText);
        }
        catch (Exception ex)
        {
            viewer.Cleanup();
            MessageBox.Show(owner, $"Failed to load the execution plan:\n\n{ex.Message}",
                "Plan Load Error", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        try
        {
            var window = GraphViewerWindow.ShowGraph(owner, viewer, label);
            window.Closed += (_, _) => viewer.Cleanup();
        }
        catch (InvalidOperationException)
        {
            /* The owner window was closed while the plan was loading (a slow capture the user abandoned) —
               setting Owner to a closed window throws. Clean up the orphaned control rather than crash the
               async-void caller. */
            viewer.Cleanup();
        }
    }

    /// <summary>
    /// The informed-consent gate: warns that the query WILL EXECUTE against the server/database, and — when the
    /// modification detector says it writes data — prepends a PROMINENT, distinct block naming the statement types
    /// and stating plainly that re-executing RE-APPLIES those changes. Returns true only on OK.
    /// </summary>
    private static bool Confirm(Window owner, string serverDisplayName, string databaseName, QueryModificationInfo modification)
    {
        var prompt = new StringBuilder();

        var modificationWarning = QueryModificationDetector.BuildConsentWarning(modification, databaseName);
        if (modificationWarning.Length > 0)
        {
            prompt.AppendLine(modificationWarning);
            prompt.AppendLine("──────────────────────────────────────────");
            prompt.AppendLine();
        }

        var db = string.IsNullOrWhiteSpace(databaseName) ? "the default database" : $"[{databaseName}]";
        prompt.AppendLine($"The service will EXECUTE this query against {serverDisplayName} in database {db} to capture its actual plan.");
        prompt.AppendLine();
        prompt.AppendLine("It runs as the service's stored monitoring login, with SET STATISTICS XML ON. All data results are discarded — only the plan is returned.");

        var result = MessageBox.Show(
            owner,
            prompt.ToString(),
            modification.ModifiesData ? "Get Actual Plan (re-run) — DATA WILL BE MODIFIED" : "Get Actual Plan (re-run)",
            MessageBoxButton.OKCancel,
            MessageBoxImage.Warning);

        return result == MessageBoxResult.OK;
    }

    /// <summary>Surfaces a non-captured outcome (no-plan info, or a failure/timeout warning with the service's message).</summary>
    private static void ShowOutcome(Window owner, ActualPlanResult result)
    {
        var (caption, icon) = result.Status switch
        {
            ActualPlanStatus.NoPlanCaptured => ("No Plan Captured", MessageBoxImage.Information),
            ActualPlanStatus.TimedOut => ("Actual Plan Still Running", MessageBoxImage.Warning),
            _ => ("Actual Plan Failed", MessageBoxImage.Warning),
        };
        MessageBox.Show(owner, result.Message ?? "The actual plan could not be captured.", caption, MessageBoxButton.OK, icon);
    }

    /// <summary>The shared read-only-seat explanation (a read-only viewer cannot enqueue a command).</summary>
    private static void ShowReadOnly(Window owner)
        => MessageBox.Show(owner,
            "Capturing an actual plan asks the service to re-execute the query, which it does by running a " +
            "command — a read-only viewer seat can't enqueue commands. The command is queued in the MONITORING " +
            "STORE, not on the monitored server. Reconnect with a read-write store profile to capture actual " +
            "plans.",
            "Read-Only Viewer", MessageBoxButton.OK, MessageBoxImage.Information);
}
