/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private async void DownloadQueryStatsPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not QueryStatsRow row) return;
        if (string.IsNullOrEmpty(row.QueryHash)) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            string? plan = null;
            var source = "collected data";

            // Try DuckDB first
            try
            {
                plan = await _dataService.GetCachedQueryPlanAsync(_serverId, row.QueryHash);
            }
            catch
            {
                // DuckDB lookup failed, fall through to live server
            }

            // Fall back to live server
            if (string.IsNullOrEmpty(plan))
            {
                var connStr = _server.GetConnectionString(_credentialService);
                plan = await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, row.QueryHash);
                source = "live server";
            }

            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show("No query plan found in collected data or the live plan cache for this query hash.", "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            SavePlanFile(plan, $"QueryPlan_{row.QueryHash}");
            btn.Content = $"Saved ({source})";
            return;
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            if (btn.Content is "...")
                btn.Content = "Download";
            btn.IsEnabled = true;
        }
    }

    private async void DownloadProcedurePlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not ProcedureStatsRow row) return;
        if (string.IsNullOrEmpty(row.ObjectName)) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            string? plan = null;
            var source = "collected data";

            // Try DuckDB first — match by plan_handle in query_stats
            if (!string.IsNullOrEmpty(row.PlanHandle))
            {
                try
                {
                    plan = await _dataService.GetCachedProcedurePlanAsync(_serverId, row.PlanHandle);
                }
                catch
                {
                    // DuckDB lookup failed, fall through to live server
                }
            }

            // Fall back to live server
            if (string.IsNullOrEmpty(plan))
            {
                var connStr = _server.GetConnectionString(_credentialService);
                plan = await LocalDataService.FetchProcedurePlanOnDemandAsync(connStr, row.DatabaseName, row.SchemaName, row.ObjectName);
                source = "live server";
            }

            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show("No query plan found in collected data or the live plan cache for this procedure.", "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            SavePlanFile(plan, $"ProcPlan_{row.FullName}");
            btn.Content = $"Saved ({source})";
            return;
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            if (btn.Content is "...")
                btn.Content = "Download";
            btn.IsEnabled = true;
        }
    }

    private void DownloadSnapshotPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not QuerySnapshotRow row) return;

        if (row.QueryPlan == null)
        {
            MessageBox.Show(
                "No estimated plan is available for this snapshot. The plan may have been evicted from the plan cache.",
                "No Plan Available",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
            return;
        }

        SavePlanFile(row.QueryPlan, $"EstimatedPlan_Session{row.SessionId}");
    }

    private void DownloadSnapshotLivePlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not QuerySnapshotRow row) return;

        if (row.LiveQueryPlan == null)
        {
            MessageBox.Show(
                "No live query plan is available for this snapshot. The query may have completed before the plan could be captured.",
                "No Plan Available",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
            return;
        }

        SavePlanFile(row.LiveQueryPlan, $"ActualPlan_Session{row.SessionId}");
    }

    private void ShowPlanLoading(string label)
    {
        PlanLoadingLabel.Text = $"Executing: {label}";
        PlanEmptyState.Visibility = Visibility.Collapsed;
        PlanTabControl.Visibility = Visibility.Collapsed;
        PlanLoadingState.Visibility = Visibility.Visible;
        PlanViewerTabItem.IsSelected = true;
    }

    private void HidePlanLoading()
    {
        PlanLoadingState.Visibility = Visibility.Collapsed;
        if (PlanTabControl.Items.Count > 0)
            PlanTabControl.Visibility = Visibility.Visible;
        else
            PlanEmptyState.Visibility = Visibility.Visible;
    }

    private void OpenPlanTab(string planXml, string label, string? queryText = null)
    {
        try
        {
            System.Xml.Linq.XDocument.Parse(planXml);
        }
        catch (System.Xml.XmlException ex)
        {
            MessageBox.Show(
                $"The plan XML is not valid:\n\n{ex.Message}",
                "Invalid Plan XML",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return;
        }

        HidePlanLoading();
        var viewer = new PlanViewerControl();
        viewer.LoadPlan(planXml, label, queryText);

        var header = new StackPanel { Orientation = System.Windows.Controls.Orientation.Horizontal };
        header.Children.Add(new TextBlock
        {
            Text = label.Length > 30 ? label[..30] + "…" : label,
            VerticalAlignment = System.Windows.VerticalAlignment.Center,
            ToolTip = label
        });
        var closeBtn = new Button
        {
            Style = (Style)FindResource("TabCloseButton")
        };
        header.Children.Add(closeBtn);

        var tab = new TabItem { Header = header, Content = viewer };
        closeBtn.Tag = tab;
        closeBtn.Click += ClosePlanTab_Click;

        PlanTabControl.Items.Add(tab);
        PlanTabControl.SelectedItem = tab;
        PlanEmptyState.Visibility = Visibility.Collapsed;
        PlanTabControl.Visibility = Visibility.Visible;
    }

    private void ClosePlanTab_Click(object sender, RoutedEventArgs e)
    {
        if (sender is Button btn && btn.Tag is TabItem tab)
        {
            PlanTabControl.Items.Remove(tab);
            if (PlanTabControl.Items.Count == 0)
            {
                PlanTabControl.Visibility = Visibility.Collapsed;
                PlanEmptyState.Visibility = Visibility.Visible;
            }
        }
    }

    private void CancelPlanButton_Click(object sender, RoutedEventArgs e)
    {
        _actualPlanCts?.Cancel();
    }

    private async void ViewEstimatedPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        string? planXml = null;
        string? queryText = null;
        string label = "Estimated Plan";

        switch (grid.CurrentItem)
        {
            case QuerySnapshotRow snap:
                planXml = snap.LiveQueryPlan ?? snap.QueryPlan;
                queryText = snap.QueryText;
                label = snap.LiveQueryPlan != null
                    ? $"Plan - SPID {snap.SessionId}"
                    : $"Est Plan - SPID {snap.SessionId}";
                break;
            case QueryStatsRow stats:
                planXml = stats.QueryPlan;
                queryText = stats.QueryText;
                label = $"Est Plan - {stats.QueryHash}";
                // Fetch on demand if not already loaded
                if (string.IsNullOrEmpty(planXml))
                    planXml = await FetchPlanByHash(stats.QueryHash);
                break;
            case QueryStatsHistoryRow hist:
                planXml = hist.QueryPlan;
                label = "Est Plan - History";
                break;
            case ProcedureStatsRow proc:
                label = $"Est Plan - {proc.FullName}";
                queryText = proc.FullName;
                try
                {
                    var connStr = _server.GetConnectionString(_credentialService);
                    planXml = await LocalDataService.FetchProcedurePlanOnDemandAsync(
                        connStr, proc.DatabaseName, proc.SchemaName, proc.ObjectName);
                }
                catch { }
                break;
            case QueryStoreRow qs:
                label = $"Est Plan - QS {qs.QueryId}";
                queryText = qs.QueryText;
                if (qs.PlanId > 0)
                {
                    try
                    {
                        var connStr = _server.GetConnectionString(_credentialService);
                        planXml = await LocalDataService.FetchQueryStorePlanAsync(connStr, qs.DatabaseName, qs.PlanId);
                    }
                    catch { }
                }
                break;
        }

        if (!string.IsNullOrEmpty(planXml))
        {
            OpenPlanTab(planXml, label, queryText);
            PlanViewerTabItem.IsSelected = true;
        }
        else
        {
            MessageBox.Show(
                "No query plan is available for this row. The plan may have been evicted from the plan cache since it was last collected.",
                "No Plan Available",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
        }
    }

    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        string? queryText = null;
        string? databaseName = null;
        string? planXml = null;
        string? isolationLevel = null;
        string label = "Actual Plan";

        switch (grid.CurrentItem)
        {
            case QuerySnapshotRow snapshot:
                queryText = snapshot.QueryText;
                databaseName = snapshot.DatabaseName;
                planXml = snapshot.LiveQueryPlan ?? snapshot.QueryPlan;
                isolationLevel = snapshot.TransactionIsolationLevel;
                label = $"Actual Plan - SPID {snapshot.SessionId}";
                break;
            case QueryStatsRow stats:
                queryText = stats.QueryText;
                databaseName = stats.DatabaseName;
                label = $"Actual Plan - {stats.QueryHash}";
                if (!string.IsNullOrEmpty(stats.QueryHash))
                {
                    try { planXml = await FetchPlanByHash(stats.QueryHash); }
                    catch { }
                }
                break;
            case QueryStoreRow qs:
                queryText = qs.QueryText;
                databaseName = qs.DatabaseName;
                label = $"Actual Plan - QS {qs.QueryId}";
                if (qs.PlanId > 0)
                {
                    try
                    {
                        var connStr = _server.GetConnectionString(_credentialService);
                        planXml = await LocalDataService.FetchQueryStorePlanAsync(connStr, qs.DatabaseName, qs.PlanId);
                    }
                    catch { }
                }
                break;
        }

        if (string.IsNullOrWhiteSpace(queryText))
        {
            MessageBox.Show("No query text available for this row.", "No Query Text",
                MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var result = MessageBox.Show(
            $"You are about to execute this query against {_server.ServerName} in database [{databaseName ?? "default"}].\n\n" +
            "Make sure you understand what the query does before proceeding.\n" +
            "The query will execute with SET STATISTICS XML ON to capture the actual plan.\n" +
            "All data results will be discarded.",
            "Get Actual Plan",
            MessageBoxButton.OKCancel,
            MessageBoxImage.Warning);

        if (result != MessageBoxResult.OK) return;

        ShowPlanLoading(label);

        _actualPlanCts?.Dispose();
        _actualPlanCts = new CancellationTokenSource();

        try
        {
            var connectionString = _server.GetConnectionString(_credentialService);

            var actualPlanXml = await ActualPlanExecutor.ExecuteForActualPlanAsync(
                connectionString,
                databaseName ?? "",
                queryText,
                planXml,
                isolationLevel,
                isAzureSqlDb: false,
                timeoutSeconds: 0,
                _actualPlanCts.Token);

            if (!string.IsNullOrEmpty(actualPlanXml))
            {
                OpenPlanTab(actualPlanXml, label, queryText);
                PlanViewerTabItem.IsSelected = true;
            }
            else
            {
                MessageBox.Show("Query executed but no execution plan was captured.",
                    "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }
        catch (OperationCanceledException)
        {
            MessageBox.Show("The query was cancelled or timed out.",
                "Cancelled", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to get actual plan:\n\n{ex.Message}",
                "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            HidePlanLoading();
        }
    }

    private async Task<string?> FetchPlanByHash(string queryHash)
    {
        if (string.IsNullOrEmpty(queryHash)) return null;

        // Try DuckDB cache first
        try
        {
            var plan = await _dataService.GetCachedQueryPlanAsync(_serverId, queryHash);
            if (!string.IsNullOrEmpty(plan)) return plan;
        }
        catch { }

        // Fall back to live server
        try
        {
            var connStr = _server.GetConnectionString(_credentialService);
            return await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, queryHash);
        }
        catch { return null; }
    }

    // ── Blocked Process Report plan lookup ──

    /* SQL Server writes this 42-byte all-zero handle into executionStack frames
       for dynamic SQL / system contexts where no persistent sql_handle exists.
       Filter matches sp_HumanEventsBlockViewer's XPath exclusion. */
    private static readonly string ZeroSqlHandle = "0x" + new string('0', 84);

    private async void ViewBlockedSidePlan_Click(object sender, RoutedEventArgs e)
        => await ShowBlockedProcessPlanAsync(sender, blockingSide: false);

    private async void ViewBlockingSidePlan_Click(object sender, RoutedEventArgs e)
        => await ShowBlockedProcessPlanAsync(sender, blockingSide: true);

    private async System.Threading.Tasks.Task ShowBlockedProcessPlanAsync(object sender, bool blockingSide)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not BlockedProcessReportRow row) return;

        var sideLabel = blockingSide ? "Blocking" : "Blocked";
        var spid = blockingSide ? row.BlockingSpid : row.BlockedSpid;
        var queryText = blockingSide ? row.BlockingSqlText : row.BlockedSqlText;
        var label = $"Est Plan - {sideLabel} SPID {spid}";

        var frames = ExtractBlockedProcessFrames(row.BlockedProcessReportXml, blockingSide);
        if (frames.Count == 0)
        {
            MessageBox.Show(
                $"The {sideLabel.ToLowerInvariant()} process report has no resolvable sql_handle. " +
                "This usually means the query ran as dynamic SQL or a system context — " +
                "SQL Server records a zero handle in that case and the plan can't be recovered.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        string? planXml = null;
        try
        {
            var connStr = _server.GetConnectionString(_credentialService);
            foreach (var f in frames)
            {
                planXml = await LocalDataService.FetchPlanBySqlHandleAsync(
                    connStr, row.DatabaseName, f.SqlHandle, f.StmtStart, f.StmtEnd);
                if (!string.IsNullOrEmpty(planXml)) break;
            }
        }
        catch { }

        if (!string.IsNullOrEmpty(planXml))
        {
            OpenPlanTab(planXml, label, queryText);
            PlanViewerTabItem.IsSelected = true;
        }
        else
        {
            MessageBox.Show(
                $"The plan for the {sideLabel.ToLowerInvariant()} query is no longer in the plan cache on {_server.ServerName}. " +
                "Blocked process reports only give us a sql_handle — if that plan has been evicted, we can't recover it.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private static IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> ExtractBlockedProcessFrames(
        string bprXml, bool blockingSide)
    {
        var empty = Array.Empty<(string, int, int)>();
        if (string.IsNullOrWhiteSpace(bprXml)) return empty;
        try
        {
            var doc = System.Xml.Linq.XElement.Parse(bprXml);
            var processContainer = blockingSide
                ? doc.Element("blocking-process")
                : doc.Element("blocked-process");
            var stack = processContainer?.Element("process")?.Element("executionStack");
            if (stack == null) return empty;

            var frames = new List<(string, int, int)>();
            foreach (var frame in stack.Elements("frame"))
            {
                var handle = frame.Attribute("sqlhandle")?.Value;
                if (string.IsNullOrWhiteSpace(handle)) continue;
                if (string.Equals(handle, ZeroSqlHandle, StringComparison.OrdinalIgnoreCase)) continue;

                int stmtStart = 0;
                int stmtEnd = -1;
                int.TryParse(frame.Attribute("stmtstart")?.Value, out stmtStart);
                if (int.TryParse(frame.Attribute("stmtend")?.Value, out var se)) stmtEnd = se;

                frames.Add((handle!, stmtStart, stmtEnd));
            }
            return frames;
        }
        catch
        {
            return empty;
        }
    }

    // ── Deadlock process plan lookup ──

    /* Deadlock graph XML puts sqlhandle/stmtstart/stmtend directly on the
       <process> node, with optional <executionStack><frame sqlhandle=...>
       children for the call stack. Try process-level first, then walk frames
       top-down like sp_HumanEventsBlockViewer does for BPRs. */
    private async void ViewDeadlockProcessPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not DeadlockProcessDetail row) return;

        var sideLabel = row.IsVictim ? "Victim" : "Deadlocker";
        var label = $"Est Plan - {sideLabel} SPID {row.Spid}";

        var frames = ExtractDeadlockProcessFrames(row.DeadlockGraphXml, row.ProcessId);
        if (frames.Count == 0)
        {
            MessageBox.Show(
                $"The process has no resolvable sql_handle in the deadlock graph. " +
                "This usually means the query ran as dynamic SQL or a system context — " +
                "SQL Server records a zero handle in that case and the plan can't be recovered.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        string? planXml = null;
        try
        {
            var connStr = _server.GetConnectionString(_credentialService);
            foreach (var f in frames)
            {
                planXml = await LocalDataService.FetchPlanBySqlHandleAsync(
                    connStr, row.DatabaseName, f.SqlHandle, f.StmtStart, f.StmtEnd);
                if (!string.IsNullOrEmpty(planXml)) break;
            }
        }
        catch { }

        if (!string.IsNullOrEmpty(planXml))
        {
            OpenPlanTab(planXml, label, row.SqlText);
            PlanViewerTabItem.IsSelected = true;
        }
        else
        {
            MessageBox.Show(
                $"The plan for this {sideLabel.ToLowerInvariant()} process is no longer in the plan cache on {_server.ServerName}. " +
                "Deadlock graphs only give us a sql_handle — if that plan has been evicted, we can't recover it.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }

    private static IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> ExtractDeadlockProcessFrames(
        string graphXml, string processId)
    {
        var empty = Array.Empty<(string, int, int)>();
        if (string.IsNullOrWhiteSpace(graphXml) || string.IsNullOrWhiteSpace(processId)) return empty;
        try
        {
            var doc = System.Xml.Linq.XElement.Parse(graphXml);
            var process = doc.Descendants("process")
                .FirstOrDefault(p => string.Equals(p.Attribute("id")?.Value, processId, StringComparison.OrdinalIgnoreCase));
            if (process == null) return empty;

            var frames = new List<(string, int, int)>();

            /* Try process-level sqlhandle first — deadlock graphs frequently put it on <process>. */
            var procHandle = process.Attribute("sqlhandle")?.Value;
            if (!string.IsNullOrWhiteSpace(procHandle) &&
                !string.Equals(procHandle, ZeroSqlHandle, StringComparison.OrdinalIgnoreCase))
            {
                int ps = 0, pe = -1;
                int.TryParse(process.Attribute("stmtstart")?.Value, out ps);
                if (int.TryParse(process.Attribute("stmtend")?.Value, out var peParsed)) pe = peParsed;
                frames.Add((procHandle!, ps, pe));
            }

            /* Then walk the executionStack frames. */
            var stack = process.Element("executionStack");
            if (stack != null)
            {
                foreach (var frame in stack.Elements("frame"))
                {
                    var handle = frame.Attribute("sqlhandle")?.Value;
                    if (string.IsNullOrWhiteSpace(handle)) continue;
                    if (string.Equals(handle, ZeroSqlHandle, StringComparison.OrdinalIgnoreCase)) continue;

                    int fs = 0, fe = -1;
                    int.TryParse(frame.Attribute("stmtstart")?.Value, out fs);
                    if (int.TryParse(frame.Attribute("stmtend")?.Value, out var feParsed)) fe = feParsed;
                    frames.Add((handle!, fs, fe));
                }
            }

            return frames;
        }
        catch
        {
            return empty;
        }
    }

    private void SavePlanFile(string planXml, string defaultName)
    {
        var dialog = new SaveFileDialog
        {
            Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
            DefaultExt = ".sqlplan",
            FileName = $"{defaultName}_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, planXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save plan: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void DownloadDeadlockXml_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not DeadlockProcessDetail row || string.IsNullOrEmpty(row.DeadlockGraphXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "XML files (*.xml)|*.xml|All files (*.*)|*.*",
            DefaultExt = ".xml",
            FileName = $"deadlock_{row.DeadlockTime:yyyyMMdd_HHmmss}.xml"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, row.DeadlockGraphXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save deadlock XML: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void DownloadBlockedProcessXml_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not BlockedProcessReportRow row || string.IsNullOrEmpty(row.BlockedProcessReportXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "XML files (*.xml)|*.xml|All files (*.*)|*.*",
            DefaultExt = ".xml",
            FileName = $"blocked_process_{row.EventTime:yyyyMMdd_HHmmss}.xml"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, row.BlockedProcessReportXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save blocked process XML: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }
}
