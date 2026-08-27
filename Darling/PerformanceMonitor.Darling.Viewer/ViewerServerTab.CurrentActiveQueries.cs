/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Queries → Current Active Queries sub-tab (headless-plan live-snapshot wave): a LIVE, on-demand DMV
/// snapshot of what is running on the monitored server RIGHT NOW, distinct from the stored "Active Queries"
/// sub-tab (the collector's hourly snapshots). The store-only viewer cannot hit the server itself, so the
/// Refresh button drives the SERVICE via the Stage-2 <c>fetch_active_queries</c> command
/// (<see cref="ViewerDataService.RequestActiveQueriesLiveAsync"/>); the service runs the shared Active Queries
/// collector's query on the target and returns the rows, which render in a grid mirroring the stored one.
///
/// <para>The fetch is ON-DEMAND only — it is NOT auto-loaded on tab selection or the toolbar-range refresh (a
/// live server hit should be an explicit operator action), so <c>LoadQueriesAsync</c> leaves this sub-tab to its
/// Refresh button. Enqueuing a command is a config write, so a read-only <c>viewer</c> seat cannot do it: the
/// handler short-circuits with an explanation, and the enqueue's <see cref="ViewerReadOnlyException"/> is the
/// belt-and-braces backstop. A timeout / unreachable-server / service failure each degrade to a friendly status
/// message (mirroring how the live-plan fetch surfaces those). The Estimated / Actual plan buttons reuse the
/// stored grid's handlers (the row is a <see cref="ViewerQuerySnapshotRow"/> either way), opening the plan the
/// live DMV read carried inline.</para>
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// #2400: the read-only refusal names WHICH write is blocked, because the previous wording ("reconnect
    /// with a read-write profile") reads as "grant this viewer write access to my production SQL Server" —
    /// which is the one thing it does not mean, and a reasonable person declines rather than asks. The write
    /// is an INSERT into config_command in the monitoring store; the monitored server is only ever READ, by
    /// the service, over its own connection.
    /// </summary>
    private const string ReadOnlySeatMessage =
        "Read-only viewer — the live fetch queues a request for the service in the MONITORING STORE, which a "
        + "read-only seat can't write to. Nothing is sent to or written on the monitored server. Reconnect "
        + "with a read-write store profile to fetch live active queries.";

    /// <summary>Refresh button — fires the live fetch (never auto-fires; a live server hit is explicit).</summary>
    private async void RefreshCurrentActiveQueries_Click(object sender, RoutedEventArgs e)
        => await LoadCurrentActiveQueriesAsync();

    private async Task LoadCurrentActiveQueriesAsync()
    {
        /* A read-only seat cannot command the service (same rule as Pause / Snapshot / fetch_plan) — surface it
           rather than attempting an enqueue that would throw. */
        if (_dataService.IsReadOnly)
        {
            CurrentActiveQueriesStatus.Text = ReadOnlySeatMessage;
            return;
        }

        RefreshCurrentActiveQueriesButton.IsEnabled = false;
        CurrentActiveQueriesStatus.Text = "Fetching what is running on the server right now...";
        try
        {
            var result = await _dataService.RequestActiveQueriesLiveAsync(_server.ServerId);
            switch (result.Status)
            {
                case ActiveQueriesLiveStatus.Fetched:
                    CurrentActiveQueriesGrid.ItemsSource = result.Rows;
                    var stamp = ViewerTimeHelper.ForDisplay(DateTime.UtcNow).ToString("yyyy-MM-dd HH:mm:ss");
                    CurrentActiveQueriesStatus.Text = result.Rows.Count == 0
                        ? $"No user requests running as of {stamp}."
                        : $"{result.Rows.Count} running request(s) as of {stamp}.";
                    break;

                case ActiveQueriesLiveStatus.TimedOut:
                    CurrentActiveQueriesStatus.Text = result.Message
                        ?? "The service did not return the live active queries in time — try again.";
                    break;

                case ActiveQueriesLiveStatus.Failed:
                    CurrentActiveQueriesStatus.Text = result.Message
                        ?? "The service could not read the live active queries.";
                    break;
            }
        }
        catch (ViewerReadOnlyException)
        {
            /* Grants changed under us between the IsReadOnly check and the enqueue. */
            CurrentActiveQueriesStatus.Text = ReadOnlySeatMessage;
        }
        catch (Exception ex)
        {
            CurrentActiveQueriesStatus.Text = $"Failed to fetch live active queries: {ex.Message}";
            StatusChanged?.Invoke($"live active queries failed: {ex.Message}");
        }
        finally
        {
            RefreshCurrentActiveQueriesButton.IsEnabled = true;
        }
    }
}
