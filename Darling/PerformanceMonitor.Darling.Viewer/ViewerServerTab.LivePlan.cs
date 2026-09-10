/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The "View Cached Plan" surfaces (headless-plan live-plan wave): the piece Darling's stored-plan-only viewer
/// had to drop when it lost Lite's direct SqlClient path. The viewer never touches SQL Server, but the SERVICE
/// holds a live runtime connection to every monitored server, so these hand a handle to the service over the
/// Stage-2 command channel (<see cref="ViewerDataService.FetchLivePlanAsync"/> → a <c>fetch_plan</c> command)
/// and display the plan the service reads back from the target's live plan cache in the SAME
/// <see cref="OpenPlanTab"/> host the stored plans use. Two keys, matching what the store rows carry:
/// <list type="bullet">
///   <item>the query / procedure grids' <c>plan_handle</c> — the currently-cached plan for a row, DISTINCT from
///   the (possibly older) plan the collector stored;</item>
///   <item>a deadlock-graph process's or a blocked-process-report frame's <c>sql_handle</c> + statement offsets
///   — this is what lets a user open the plan for ANY process in the graph (the deadlocker, a blocker), not
///   just the one best-effort victim plan the collector happened to stash.</item>
/// </list>
///
/// <para>Each fetch enqueues a command, which is a config write, so a read-only <c>viewer</c> seat cannot do it
/// (consistent with Pause / Snapshot) — the handlers short-circuit with an explanation, and the enqueue's
/// <see cref="ViewerReadOnlyException"/> is the belt-and-braces backstop. A spinner ("Fetching live plan…") runs
/// in the Plan Viewer while the command is polled; the loading overlay's Cancel button cancels the poll. The
/// "get the ACTUAL plan by re-executing the query" (SET STATISTICS XML) variant is deliberately NOT here — it
/// has side effects and belongs behind Apply Fix's informed-consent gate.</para>
/// </summary>
public partial class ViewerServerTab
{
    // ── Query / Procedure grids: fetch the currently-cached plan by plan_handle ──

    /// <summary>
    /// "View Cached Plan" on a Top Queries / Top Procedures row — fetches the plan CURRENTLY in the server's plan
    /// cache by the row's plan_handle (distinct from the collector's stored plan, which "View Plan" opens). Gated
    /// in XAML on the row's <c>HasLivePlanHandle</c>, so it only fires for a row that carries a plan_handle.
    /// </summary>
    private async void FetchLiveQueryPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is null) return;

        string planHandle;
        string? databaseName;
        string label;
        string? queryText;

        switch (grid.CurrentItem)
        {
            case ViewerQueryStatsRow stats when !string.IsNullOrEmpty(stats.PlanHandle):
                planHandle = stats.PlanHandle;
                databaseName = stats.DatabaseName;
                label = $"Cached Plan - {stats.QueryHash}";
                queryText = stats.QueryText;
                break;
            case ViewerProcedureStatsRow proc when !string.IsNullOrEmpty(proc.PlanHandle):
                planHandle = proc.PlanHandle;
                databaseName = proc.DatabaseName;
                label = $"Cached Plan - {proc.FullName}";
                queryText = null; /* the procedure grid carries no statement text; the plan XML holds its own */
                break;
            default:
                return;
        }

        var argsJson = ViewerDataService.BuildPlanFetchArgsByPlanHandle(planHandle, databaseName);
        await FetchAndOpenLivePlanAsync(argsJson, label, queryText);
    }

    // ── Deadlock grid: fetch the live plan for THIS process (any process in the graph) by its sql_handle ──

    /// <summary>
    /// "View Cached Plan (This Process)" on a deadlock row — fetches the live plan for the RIGHT-CLICKED process
    /// (victim OR deadlocker) by the sql_handle in its deadlock-graph executionStack, closing Lite's "any
    /// process, not just the stored victim" gap. Tries the process-level handle then each frame top-down (Lite's
    /// order), first cached plan wins.
    /// </summary>
    private async void FetchDeadlockProcessLivePlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not DeadlockProcessDetail row) return;

        var sideLabel = row.IsVictim ? "Victim" : "Deadlocker";
        var frames = PlanFrameExtractor.ExtractDeadlockProcessFrames(row.DeadlockGraphXml, row.ProcessId);
        if (frames.Count == 0)
        {
            ShowNoResolvableHandle(sideLabel.ToLowerInvariant() + " process");
            return;
        }

        await FetchAndOpenBySqlHandleFramesAsync(frames, row.DatabaseName, $"Cached Plan - {sideLabel} SPID {row.Spid}", row.SqlText);
    }

    // ── Blocked Process Reports grid: fetch the live plan for the blocked / blocking process by its sql_handle ──

    private async void FetchBlockedSideLivePlan_Click(object sender, RoutedEventArgs e)
        => await FetchBlockedProcessLivePlanAsync(sender, blockingSide: false);

    private async void FetchBlockingSideLivePlan_Click(object sender, RoutedEventArgs e)
        => await FetchBlockedProcessLivePlanAsync(sender, blockingSide: true);

    /// <summary>
    /// "View Blocked/Blocking Cached Plan" on a blocked-process-report row — fetches the live plan for the blocked
    /// or blocking process by the sql_handle in its executionStack frames. Gated in XAML on the row's
    /// <c>HasReportXml</c>, so a DMV-snapshot fallback row (no report XML → no sql_handle) shows the items
    /// disabled rather than shown-and-empty.
    /// </summary>
    private async Task FetchBlockedProcessLivePlanAsync(object sender, bool blockingSide)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not ViewerBlockedProcessRow row) return;

        var sideLabel = blockingSide ? "Blocking" : "Blocked";
        var spid = blockingSide ? row.BlockingSpid : row.BlockedSpid;
        var queryText = blockingSide ? row.BlockingSqlText : row.BlockedSqlText;

        var frames = PlanFrameExtractor.ExtractBlockedProcessFrames(row.BlockedProcessReportXml, blockingSide);
        if (frames.Count == 0)
        {
            ShowNoResolvableHandle(sideLabel.ToLowerInvariant() + " process");
            return;
        }

        await FetchAndOpenBySqlHandleFramesAsync(frames, row.DatabaseName, $"Cached Plan - {sideLabel} SPID {spid}", queryText);
    }

    // ── Shared fetch/display flow ──

    /// <summary>
    /// Fetches ONE plan by plan_handle and opens it. Gates read-only, shows the "Fetching live plan…" spinner,
    /// polls the command (cancelable via the overlay's Cancel button), and either opens the plan or shows the
    /// service's not-in-cache / failure / timeout message.
    /// </summary>
    private async Task FetchAndOpenLivePlanAsync(string argsJson, string label, string? queryText)
    {
        if (BlockedByReadOnlySeat()) return;

        BeginLivePlanFetch(label);
        try
        {
            var result = await _dataService.FetchLivePlanAsync(_server.ServerId, argsJson, _planLoadCts!.Token);
            if (result.Status == LivePlanFetchStatus.Fetched)
            {
                _ = OpenPlanTab(result.PlanXml!, label, queryText); /* HidePlanLoading runs inside OpenPlanTab */
            }
            else
            {
                HidePlanLoading();
                ShowLivePlanOutcome(result);
            }
        }
        catch (OperationCanceledException)
        {
            HidePlanLoading();
        }
        catch (ViewerReadOnlyException)
        {
            HidePlanLoading();
            BlockedByReadOnlySeat(force: true);
        }
        catch (Exception ex)
        {
            HidePlanLoading();
            MessageBox.Show($"Failed to fetch the live plan:\n\n{ex.Message}", "Cached Plan Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    /// <summary>
    /// Fetches by sql_handle across a process's candidate frames (process-level handle, then executionStack
    /// frames top-down), opening the FIRST that is still cached — mirroring Lite's per-frame walk, but each
    /// frame is one <c>fetch_plan</c> command round-trip. A "not connected" hard failure stops the walk; an
    /// evicted frame moves on to the next.
    /// </summary>
    private async Task FetchAndOpenBySqlHandleFramesAsync(
        IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> frames,
        string databaseName, string label, string? queryText)
    {
        if (BlockedByReadOnlySeat()) return;

        BeginLivePlanFetch(label);
        try
        {
            LivePlanFetchResult? last = null;
            foreach (var frame in frames)
            {
                _planLoadCts!.Token.ThrowIfCancellationRequested();

                var argsJson = ViewerDataService.BuildPlanFetchArgsBySqlHandle(
                    frame.SqlHandle, frame.StmtStart, frame.StmtEnd, databaseName);
                var result = await _dataService.FetchLivePlanAsync(_server.ServerId, argsJson, _planLoadCts.Token);
                last = result;

                if (result.Status == LivePlanFetchStatus.Fetched)
                {
                    _ = OpenPlanTab(result.PlanXml!, label, queryText); /* HidePlanLoading runs inside OpenPlanTab */
                    return;
                }

                /* A hard failure (server not connected) won't improve on the next frame — stop and report it.
                   An evicted plan (NotInCache) might be a different frame's, so keep walking. */
                if (result.Status is LivePlanFetchStatus.Failed or LivePlanFetchStatus.TimedOut)
                {
                    break;
                }
            }

            HidePlanLoading();
            if (last is not null && last.Status != LivePlanFetchStatus.NotInCache)
            {
                ShowLivePlanOutcome(last);
            }
            else
            {
                MessageBox.Show(
                    $"The plan for this process is no longer in the plan cache on {_server.DisplayName}. " +
                    "A deadlock graph / blocked-process report only carries a sql_handle — if that plan has been " +
                    "evicted or recompiled, it can't be recovered.",
                    "Plan Not In Cache", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }
        catch (OperationCanceledException)
        {
            HidePlanLoading();
        }
        catch (ViewerReadOnlyException)
        {
            HidePlanLoading();
            BlockedByReadOnlySeat(force: true);
        }
        catch (Exception ex)
        {
            HidePlanLoading();
            MessageBox.Show($"Failed to fetch the live plan:\n\n{ex.Message}", "Cached Plan Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    /// <summary>Shows the Plan Viewer's loading overlay with a fetch-specific label + a fresh cancellation source.</summary>
    private void BeginLivePlanFetch(string label)
    {
        ShowPlanLoading(label);
        PlanLoadingLabel.Text = $"Fetching live plan: {label}";
        _planLoadCts?.Dispose();
        _planLoadCts = new CancellationTokenSource();
    }

    /// <summary>Surfaces a non-fetched outcome (not-in-cache info, or a failure/timeout warning).</summary>
    private void ShowLivePlanOutcome(LivePlanFetchResult result)
    {
        var (caption, icon) = result.Status == LivePlanFetchStatus.NotInCache
            ? ("Plan Not In Cache", MessageBoxImage.Information)
            : ("Cached Plan Fetch Failed", MessageBoxImage.Warning);
        MessageBox.Show(result.Message ?? "The plan could not be fetched.", caption, MessageBoxButton.OK, icon);
    }

    private void ShowNoResolvableHandle(string what)
        => MessageBox.Show(
            $"The {what} has no resolvable sql_handle in its captured XML. This usually means the query ran as " +
            "dynamic SQL or a system context — SQL Server records a zero handle there and the plan can't be recovered.",
            "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);

    /// <summary>
    /// True (and shows an explanation) when the current seat is read-only: fetching a live plan enqueues a
    /// command, which a read-only <c>viewer</c> seat cannot do (same rule as Pause / per-server toggles).
    /// <paramref name="force"/> shows the message even if IsReadOnly went stale (the enqueue already threw).
    /// </summary>
    private bool BlockedByReadOnlySeat(bool force = false)
    {
        if (!force && !_dataService.IsReadOnly) return false;
        MessageBox.Show(
            "Fetching a live plan asks the service to read the target server's plan cache, which it does by " +
            "running a command — a read-only viewer seat can't enqueue commands. Reconnect with a read-write " +
            "profile to fetch live plans.",
            "Read-Only Viewer", MessageBoxButton.OK, MessageBoxImage.Information);
        return true;
    }
}
